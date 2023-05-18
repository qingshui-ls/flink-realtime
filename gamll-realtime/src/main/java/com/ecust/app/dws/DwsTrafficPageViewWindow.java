package com.ecust.app.dws;

import com.alibaba.fastjson.JSONObject;
import com.ecust.bean.TrafficHomeDetailPageViewBean;
import com.ecust.bean.UserLoginBean;
import com.ecust.utils.ClickHouseUtil;
import com.ecust.utils.DateFormatUtil;
import com.ecust.utils.KafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * 1）读取 Kafka 页面主题数据
 * 2）转换数据结构
 * 3）过滤数据
 * 4）设置水位线
 * 5）按照 mid 分组
 * 6）统计首页和商品详情页独立访客数，转换数据结构
 * 7）开窗
 * 8）聚合
 * 9）将数据写出到 ClickHouse
 * 数据流 ： web/app -> nginx -> 日志服务器(.log) -> Flume -> Kafka(ODS) -> FlinkAPP(BaseLogAPP) -> Kafka(DWD) -> FlinkAPP -> Clickhouse(DWS)
 * 程序：Mock(模拟产生日志数据，lg.sh) -> Flume(f1.sh) ->  Kafka(ZK)-> FlinkAPP(BaseLogAPP) -> Kafka(DWD) -> DwsTrafficPageViewWindow -> Clickhouse(DWS)
 */
public class DwsTrafficPageViewWindow {
    public static void main(String[] args) throws Exception {
        // 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);// 生产环境设置为kafka的主题分区数

        // TODO 3. 读取 Kafka dwd_traffic_page_log 数据，封装为流
        String topic = "dwd_traffic_page_log";
        String groupId = "dws_traffic_page_view_window";

        FlinkKafkaConsumer<String> kafkaConsumer = KafkaUtil.getKafkaConsumer(topic, groupId);
        DataStreamSource<String> kafkaDS = env.addSource(kafkaConsumer);
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String s, Collector<JSONObject> collector) throws Exception {
                JSONObject jsonObject = JSONObject.parseObject(s);
                String pageId = jsonObject.getJSONObject("page").getString("page_id");
                if ("home".equals(pageId) || "good_detail".equals(pageId)) {
                    collector.collect(jsonObject);
                }
            }
        });
        // 提取事件时间生产水位线
        SingleOutputStreamOperator<JSONObject> jsonObjWithWmDS = jsonObjDS.assignTimestampsAndWatermarks(
                WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                            @Override
                            public long extractTimestamp(JSONObject jsonObject, long l) {
                                return jsonObject.getLongValue("ts");
                            }
                        }));
        // 按照mid分组
        KeyedStream<JSONObject, String> keyedStream = jsonObjWithWmDS.keyBy(x -> x.getJSONObject("common").getString("mid"));
        SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> trafficHomeDetailDS = keyedStream.flatMap(new RichFlatMapFunction<JSONObject, TrafficHomeDetailPageViewBean>() {
            private ValueState<String> homeLastState;
            private ValueState<String> detailLastState;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<String> homeDescriptor = new ValueStateDescriptor<>("home-state", String.class);
                ValueStateDescriptor<String> detailDescriptor = new ValueStateDescriptor<String>("detail-state", String.class);
                // 设置TTL
                StateTtlConfig ttlConfig = new StateTtlConfig.Builder(Time.days(1)).setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite).build();
                homeDescriptor.enableTimeToLive(ttlConfig);
                detailDescriptor.enableTimeToLive(ttlConfig);

                homeLastState = getRuntimeContext().getState(homeDescriptor);
                detailLastState = getRuntimeContext().getState(detailDescriptor);
            }

            @Override
            public void flatMap(JSONObject jsonObject, Collector<TrafficHomeDetailPageViewBean> collector) throws Exception {
                // 获取状态日期
                Long ts = jsonObject.getLong("ts");
                String homeLastDt = homeLastState.value();
                String curDt = DateFormatUtil.toDate(ts);
                String detailLastDt = detailLastState.value();

                // 定义访问首页或者详情页的数据
                long homeCt = 0L;
                long detailCt = 0L;
                // 如果状态为空或者状态时间与当前时间不同，则为需要的数据
                if ("home".equals(jsonObject.getJSONObject("page").getString("page_id"))) {
                    if (homeLastDt == null || !homeLastDt.equals(curDt)) {
                        homeCt = 1L;
                        homeLastState.update(curDt);
                    }
                } else {
                    if (detailLastDt == null || !detailLastDt.equals(curDt)) {
                        detailCt = 1L;
                        detailLastState.update(curDt);
                    }
                }

                // 满足任何一个数据不为0，则可以写出
                if (homeCt == 1L || detailCt == 1L) {
                    collector.collect(new TrafficHomeDetailPageViewBean("", "", homeCt, detailCt, ts));
                }
            }
        });
        SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> reduce = trafficHomeDetailDS.windowAll(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10)))
                .reduce(new ReduceFunction<TrafficHomeDetailPageViewBean>() {
                    @Override
                    public TrafficHomeDetailPageViewBean reduce(TrafficHomeDetailPageViewBean value1, TrafficHomeDetailPageViewBean t1) throws Exception {
                        value1.setHomeUvCt(value1.getHomeUvCt() + t1.getHomeUvCt());
                        value1.setGoodDetailUvCt(value1.getGoodDetailUvCt() + t1.getGoodDetailUvCt());
                        return value1;
                    }
                }, new AllWindowFunction<TrafficHomeDetailPageViewBean, TrafficHomeDetailPageViewBean, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow timeWindow, Iterable<TrafficHomeDetailPageViewBean> iterable, Collector<TrafficHomeDetailPageViewBean> collector) throws Exception {
                        // 获取数据
                        TrafficHomeDetailPageViewBean pageViewBean = iterable.iterator().next();
                        // 补充字段
                        pageViewBean.setEdt(DateFormatUtil.toYmdHms(timeWindow.getStart()));
                        pageViewBean.setStt(DateFormatUtil.toYmdHms(timeWindow.getEnd()));
                        pageViewBean.setTs(System.currentTimeMillis());
                        collector.collect(pageViewBean);
                    }
                });
        reduce.print(">>>>>>>>");
        reduce.addSink(ClickHouseUtil.<TrafficHomeDetailPageViewBean>getJdbcSink("insert into dws_traffic_page_view_window values(?,?,?,?,?)"));

        env.execute("DwsTrafficPageViewWindow");
    }
}
