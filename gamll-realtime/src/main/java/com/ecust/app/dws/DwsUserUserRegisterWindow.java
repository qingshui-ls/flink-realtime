package com.ecust.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ecust.bean.UserRegisterBean;
import com.ecust.utils.ClickHouseUtil;
import com.ecust.utils.DateFormatUtil;
import com.ecust.utils.KafkaUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

/**
 * 从 DWD 层用户注册表中读取数据，统计各窗口注册用户数，写入 ClickHouse。
 * 1）读取 Kafka 用户注册主题数据
 * 2）转换数据结构 String 转换为 JSONObject。
 * 3）设置水位线
 * 4）开窗、聚合
 * 5）写入 ClickHouse
 * <p>
 * 数据流 ： web/app -> nginx -> 业务服务器(Mysql) -> Maxwell -> Kafka(ODS) -> FlinkAPP -> Kafka(DWD) -> FlinkAPP -> Clickhouse(DWS)
 * 程序：Mock(模拟产生业务数据) -> Mysql -> Maxwell ->   Kafka(ZK) -> FlinkAPP(DwdUserRegister) -> Kafka(DWD) -> DwsUserUserRegisterWindow -> Clickhouse(DWS)
 */

public class DwsUserUserRegisterWindow {
    public static void main(String[] args) throws Exception {
        // 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);// 生产环境设置为kafka的主题分区数

        // TODO 3. 读取 Kafka dwd_user_register 主题数据，封装为流
        String topic = "dwd_user_register";
        String groupId = "dws_user_user_register_window";
        FlinkKafkaConsumer<String> kafkaConsumer = KafkaUtil.getKafkaConsumer(topic, groupId);
        DataStreamSource<String> source = env.addSource(kafkaConsumer);

        // TODO 4. 转换数据结构
        SingleOutputStreamOperator<JSONObject> mappedStream = source.map(JSON::parseObject);

        // TODO 5. 设置水位线
        SingleOutputStreamOperator<JSONObject> withWatermarkDS = mappedStream.assignTimestampsAndWatermarks(
                WatermarkStrategy.<JSONObject>forMonotonousTimestamps()
                        .withTimestampAssigner((jsonObj, recordTimestamp) -> jsonObj.getLong("ts") * 1000L)
        );

        // TODO 6. 开窗
        AllWindowedStream<JSONObject, TimeWindow> windowDS = withWatermarkDS.windowAll(TumblingEventTimeWindows.of(
                org.apache.flink.streaming.api.windowing.time.Time.seconds(10L)));

        // TODO 7. 聚合
        SingleOutputStreamOperator<UserRegisterBean> aggregateDS = windowDS.aggregate(
                new AggregateFunction<JSONObject, Long, Long>() {
                    @Override
                    public Long createAccumulator() {
                        return 0L;
                    }

                    @Override
                    public Long add(JSONObject jsonObj, Long accumulator) {
                        accumulator += 1;
                        return accumulator;
                    }

                    @Override
                    public Long getResult(Long accumulator) {
                        return accumulator;
                    }

                    @Override
                    public Long merge(Long a, Long b) {
                        return null;
                    }
                }, new AllWindowFunction<Long, UserRegisterBean, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow timeWindow, Iterable<Long> iterable, Collector<UserRegisterBean> collector) throws Exception {
                        for (Long value : iterable) {
                            String stt = DateFormatUtil.toYmdHms(timeWindow.getStart());
                            String edt = DateFormatUtil.toYmdHms(timeWindow.getEnd());
                            UserRegisterBean userRegisterBean = new UserRegisterBean(
                                    stt,
                                    edt,
                                    value,
                                    System.currentTimeMillis()
                            );
                            collector.collect(userRegisterBean);
                        }
                    }
                });
        aggregateDS.print();
        // TODO 8. 写入到 OLAP 数据库
        SinkFunction<UserRegisterBean> sinkFunction = ClickHouseUtil.<UserRegisterBean>getJdbcSink(
                "insert into dws_user_user_register_window values(?,?,?,?)"
        );
        aggregateDS.addSink(sinkFunction);
        env.execute();
    }

}

