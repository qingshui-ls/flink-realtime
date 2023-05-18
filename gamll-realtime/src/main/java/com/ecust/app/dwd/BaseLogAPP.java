package com.ecust.app.dwd;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.ecust.utils.DateFormatUtil;
import com.ecust.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
/**
 * 数据流 ： web/app -> nginx -> 日志服务器(.log) -> Flume -> Kafka(ODS) -> FlinkAPP(BaseLogAPP) -> Kafka(DWD)
 * 程序：Mock(模拟产生日志数据，lg.sh) -> Flume(f1.sh) ->  Kafka(ZK)-> FlinkAPP(BaseLogAPP) -> Kafka(DWD)
 */
public class BaseLogAPP {
    /**
     * 1.获取执行环境
     * 2.消费kafka topic——log数据创建流
     * 3.过滤掉非JSON格式数据&每行数据转换为JSON对象
     * 4.按照MID分组
     * 5.使用状态编程做新老访客的标签校验
     * 6.使用侧输出流进行分流处理
     * 7.提取各个侧输出流数据
     * 8.将数据写入对应主题
     * 9.启动任务
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

        String topic = "topic_log";
        String groupId = "base_log_app";

        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(topic, groupId));
        OutputTag<String> dirtyTag = new OutputTag<String>("Dirty data") {
        };
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String s, ProcessFunction<String, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                try {
                    JSONObject jsonObject = JSONObject.parseObject(s);
                    collector.collect(jsonObject);
                } catch (Exception e) {
                    context.output(dirtyTag, s);
                }
            }
        });
        // 获取侧输出流脏数据并打印
        DataStream<String> dirtyDS = jsonObjDS.getSideOutput(dirtyTag);
        dirtyDS.print("Dirty Data >>>>>>>> ");

        // 按照Mid分组
        KeyedStream<JSONObject, String> keyedStream = jsonObjDS.keyBy(json -> json.getJSONObject("common").getString("mid"));
        // 使用状态编程做新老访客标记
        SingleOutputStreamOperator<JSONObject> jsonObjWithNewFlagDS = keyedStream.map(new RichMapFunction<JSONObject, JSONObject>() {
            private ValueState<String> lastVisitState;

            @Override
            public void open(Configuration parameters) throws Exception {
                lastVisitState = getRuntimeContext().getState(new ValueStateDescriptor<String>("last-visit", String.class));
            }

            @Override
            public JSONObject map(JSONObject jsonObject) throws Exception {
                // 获取is——new 标记和 ts
                String isNew = jsonObject.getJSONObject("common").getString("is_new");
                Long ts = jsonObject.getLong("ts");
                // 将ts转为年月日
                String curDate = DateFormatUtil.toDate(ts);
                // 获取状态中的日期
                String lastDate = lastVisitState.value();
                // 判断isNew 的值是否为1
                if ("1".equals(isNew)) {
                    if (lastDate == null) {
                        lastVisitState.update(curDate);
                    } else if (!lastDate.equals(curDate)) {
                        jsonObject.getJSONObject("common").put("is_new", 0);
                    }
                } else if (lastDate == null) {
                    lastVisitState.update(DateFormatUtil.toDate(ts - 24 * 3600 * 1000L));
                }
                return jsonObject;
            }
        });
        // 使用侧输出流进行分流处理
        // 页面日志放到主流 ，其他的 启动、曝光、动作、错误放到侧输出流
        OutputTag<String> startTag = new OutputTag<String>("start") {
        };
        OutputTag<String> displayTag = new OutputTag<String>("display") {
        };
        OutputTag<String> actionTag = new OutputTag<String>("action") {
        };
        OutputTag<String> errorTag = new OutputTag<String>("error") {
        };
        SingleOutputStreamOperator<String> pageDS = jsonObjWithNewFlagDS.process(new ProcessFunction<JSONObject, String>() {
            @Override
            public void processElement(JSONObject jsonObject, ProcessFunction<JSONObject, String>.Context context, Collector<String> collector) throws Exception {
                // 尝试获取错误信息
                String err = jsonObject.getString("err");
                if (err != null) {
                    // 将数据写到error侧输出流
                    context.output(errorTag, jsonObject.toJSONString());
                }
                // 移除错误信息
                jsonObject.remove("err");
                // 尝试获取启动信息
                String start = jsonObject.getString("start");
                if (start != null) {
                    // 将数据写到start侧输出流
                    context.output(startTag, jsonObject.toJSONString());
                } else {
                    // 获取公共信息 & 页面 & 时间戳
                    String common = jsonObject.getString("common");
                    String pageId = jsonObject.getJSONObject("page").getString("page_id");
                    Long ts = jsonObject.getLong("ts");
                    // 尝试获取曝光数据
                    JSONArray displays = jsonObject.getJSONArray("displays");
                    if (displays != null && displays.size() > 0) {
                        // 将每条数据写到display侧输出流
                        for (int i = 0; i < displays.size(); i++) {
                            JSONObject display = displays.getJSONObject(i);
                            // 加入其他详细信息
                            display.put("common", common);
                            display.put("page_id", pageId);
                            display.put("ts", ts);
                            context.output(displayTag, display.toJSONString());
                        }
                    }
                    // 尝试获取动作数据
                    JSONArray actions = jsonObject.getJSONArray("actions");
                    if (actions != null && actions.size() > 0) {
                        // 将每条数据写到display侧输出流
                        for (int i = 0; i < actions.size(); i++) {
                            JSONObject action = actions.getJSONObject(i);
                            // 加入其他详细信息
                            action.put("common", common);
                            action.put("page_id", pageId);
                            context.output(actionTag, action.toJSONString());
                        }
                    }

                    // 移除曝光和动作数据 & 写到页面日志主流
                    jsonObject.remove("displays");
                    jsonObject.remove("actions");
                    collector.collect(jsonObject.toJSONString());
                }
            }
        });
        // 提取各个侧输出流数据
        DataStream<String> startDS = pageDS.getSideOutput(startTag);
        DataStream<String> errorDS = pageDS.getSideOutput(errorTag);
        DataStream<String> displayDS = pageDS.getSideOutput(displayTag);
        DataStream<String> actionDS = pageDS.getSideOutput(actionTag);

        // 8.将数据打印并写入对应主题
//        pageDS.print("PAGE>>>>>");
//        startDS.print("START>>>>>");
//        errorDS.print("ERROR>>>>>");
//        displayDS.print("DISPLAY>>>>>");
//        actionDS.print("ACTION>>>>>");

        // 定义不同日志输出到 Kafka 的主题名称
        String page_topic = "dwd_traffic_page_log";
        String start_topic = "dwd_traffic_start_log";
        String display_topic = "dwd_traffic_display_log";
        String action_topic = "dwd_traffic_action_log";
        String error_topic = "dwd_traffic_error_log";

        pageDS.addSink(MyKafkaUtil.getFlinkKafkaProducer(page_topic));
        startDS.addSink(MyKafkaUtil.getFlinkKafkaProducer(start_topic));
        errorDS.addSink(MyKafkaUtil.getFlinkKafkaProducer(error_topic));
        displayDS.addSink(MyKafkaUtil.getFlinkKafkaProducer(display_topic));
        actionDS.addSink(MyKafkaUtil.getFlinkKafkaProducer(action_topic));

        // 9.启动任务
        env.execute("BaseLogAPP");
    }
}
