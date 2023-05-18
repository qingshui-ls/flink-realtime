package com.ecust.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONAware;
import com.alibaba.fastjson.JSONObject;
import com.ecust.utils.DateFormatUtil;
import com.ecust.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
/**
 * 数据流 ： web/app -> nginx -> 日志服务器(.log) -> Flume -> Kafka(ODS) -> FlinkAPP(BaseLogAPP) -> Kafka(DWD) -> FlinkAPP -> Kafka (DWD)
 * 程序：Mock(模拟产生日志数据，lg.sh) -> Flume(f1.sh) ->  Kafka(ZK)-> FlinkAPP(BaseLogAPP) -> Kafka(DWD) -> DwdTrafficUniqueVisitorDetail -> Kafka (DWD)
 */
public class DwdTrafficUniqueVisitorDetail {
    /**
     * 1.获取执行环境
     * 2.读取kafka 页面日志主题创建流
     * 3.过滤掉上一跳页面不为null的数据并将每行数据转换为JSON对象
     * 4.按照mid分组
     * 5.使用状态编程实现按照Mid的去重
     * 6.写入kafka
     */
    public static void main(String[] args) throws Exception {
        // 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);// 生产环境设置为kafka的主题分区数

        // 1.1 开启checkPoint
       /* env.enableCheckpointing(5 * 6000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(10 * 6000L);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 5000L));
        // 1.2设置状态后端
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020//211126/ck");
        System.setProperty("HADOOP_USER_NAME", "ls");*/

        String topic = "dwd_traffic_page_log";
        String groupId = "Unique_Visitor_Detail";
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(topic, groupId));
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String s, Collector<JSONObject> collector) throws Exception {
                try {
                    JSONObject jsonObject = JSON.parseObject(s);
                    // 获取上一跳页面id
                    String lastPageId = jsonObject.getJSONObject("page").getString("last_page_id");
                    if (lastPageId == null) {
                        collector.collect(jsonObject);
                    }
                } catch (Exception e) {
                    System.out.println("脏数据：" + s);
                }

            }
        });
        KeyedStream<JSONObject, String> keyedStream = jsonObjDS.keyBy(jsonObject -> jsonObject.getJSONObject("common").getString("mid"));
        SingleOutputStreamOperator<JSONObject> UVDS = keyedStream.filter(new RichFilterFunction<JSONObject>() {
            private ValueState<String> lastVisitState;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<String> stateDescriptor = new ValueStateDescriptor<>("last-visit", String.class);
                // 设置状态的TTL
                stateDescriptor.enableTimeToLive(new StateTtlConfig.Builder(Time.days(1)).
                        setUpdateType(StateTtlConfig.UpdateType.OnReadAndWrite).
                        build());

                lastVisitState = getRuntimeContext().getState(stateDescriptor);
            }

            @Override
            public boolean filter(JSONObject jsonObject) throws Exception {
                // 获取状态数据 & 当前数据中的时间戳并转换为日期
                String lastDate = lastVisitState.value();
                Long ts = jsonObject.getLong("ts");
                String curDate = DateFormatUtil.toDate(ts);
                if (lastDate == null || !lastDate.equals(curDate)) {
                    lastVisitState.update(curDate);
                    return true;
                } else {
                    return false;
                }
            }
        });
        // 将数据写到kafka
        String targetTopic = "dwd_traffic_unique_visitor_detail";
        UVDS.print(">>>>");
        UVDS.map(JSONAware::toJSONString).addSink(MyKafkaUtil.getFlinkKafkaProducer(targetTopic));
        // 7. 执行
        env.execute("DwdTrafficUniqueVisitorDetail");
    }
}
