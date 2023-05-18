package com.ecust.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ecust.utils.ClickHouseUtil;
import com.ecust.utils.DateFormatUtil;
import com.ecust.utils.KafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import com.ecust.bean.TrafficPageViewBean;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * （0）获取执行环境
 * （1）读取页面主题数据，封装为流
 * （2）统计页面浏览时长、页面浏览数、会话数，转换数据结构
 * （3）读取用户跳出明细数据
 * （4）转换用户跳出流数据结构
 * （5）读取独立访客明细数据
 * （6）转换独立访客流数据结构
 * （7）union 合并三条流
 * （8）设置水位线；
 * （9）按照维度字段分组；
 * （10）开窗
 * （11）聚合计算
 * （12）将数据写入 ClickHouse。
 * （13）启动任务
 *
 * 数据流Base ： web/app -> nginx -> 日志服务器(.log) -> Flume -> Kafka(ODS) -> FlinkAPP(BaseLogAPP) -> Kafka(DWD)
 * 数据流UV ： web/app -> nginx -> 日志服务器(.log) -> Flume -> Kafka(ODS) -> FlinkAPP(BaseLogAPP) -> Kafka(DWD) -> FlinkAPP -> Kafka (DWD)
 * 数据流UJ ： web/app -> nginx -> 日志服务器(.log) -> Flume -> Kafka(ODS) -> FlinkAPP(BaseLogAPP) -> Kafka(DWD) -> FlinkAPP -> Kafka (DWD)
 * ====> FlinkAPP -> ClickHouse(DWS)
 *
 * 程序Base：Mock(模拟产生日志数据，lg.sh) -> Flume(f1.sh) ->  Kafka(ZK)-> FlinkAPP(BaseLogAPP) -> Kafka(DWD)
 * 程序UV：Mock(模拟产生日志数据，lg.sh) -> Flume(f1.sh) ->  Kafka(ZK)-> FlinkAPP(BaseLogAPP) -> Kafka(DWD) -> DwdTrafficUniqueVisitorDetail -> Kafka (DWD)
 * 程序UJ：Mock(模拟产生日志数据，lg.sh) -> Flume(f1.sh) ->  Kafka(ZK)-> FlinkAPP(BaseLogAPP) -> Kafka(DWD) -> DwdTrafficUserJumpDetail -> Kafka (DWD)
 * ====> DwsTrafficVcChArIsNewPageViewWindow -> ClickHouse(DWS)
 */
public class DwsTrafficVcChArIsNewPageViewWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // TODO 3. 从 kafka dwd_traffic_page_log 主题读取页面数据，封装为流
        String topic = "dwd_traffic_page_log";
        String groupId = "dws_traffic_channel_page_view_window";
        FlinkKafkaConsumer<String> kafkaConsumer = KafkaUtil.getKafkaConsumer(topic, groupId);
        DataStreamSource<String> pageLogSource = env.addSource(kafkaConsumer);

        // TODO 4. 转换页面流数据结构
        SingleOutputStreamOperator<JSONObject> jsonObjStream = pageLogSource.map(JSON::parseObject);
        // TODO 5. 统计会话数、页面浏览数、页面访问时长，并封装为实体类
        SingleOutputStreamOperator<TrafficPageViewBean> mainStream = jsonObjStream.map(
                jsonObj -> {
                    JSONObject common = jsonObj.getJSONObject("common");
                    JSONObject page = jsonObj.getJSONObject("page");

                    // 获取 ts
                    Long ts = jsonObj.getLong("ts");

                    // 获取维度信息
                    String vc = common.getString("vc");
                    String ch = common.getString("ch");
                    String ar = common.getString("ar");
                    String isNew = common.getString("is_new");

                    // 获取页面访问时长
                    Long duringTime = page.getLong("during_time");

                    // 定义变量接受其它度量值
                    Long uvCt = 0L;
                    Long svCt = 0L;
                    Long pvCt = 1L;
                    Long ujCt = 0L;

                    // 判断本页面是否开启了一个新的会话
                    String lastPageId = page.getString("last_page_id");
                    if (lastPageId == null) {
                        svCt = 1L;
                    }

                    // 封装为实体类
                    return new TrafficPageViewBean(
                            "",
                            "",
                            vc,
                            ch,
                            ar,
                            isNew,
                            uvCt,
                            svCt,
                            pvCt,
                            duringTime,
                            ujCt,
                            ts
                    );
                });

        // TODO 6. 从 Kafka 读取跳出明细数据和独立访客数据，封装为流并转换数据结构，合并三条流
        // 6.1 从 Kafka dwd_traffic_user_jump_detail 读取跳出明细数据，封装为流
        String ujdTopic = "dwd_traffic_user_jump_detail";
        FlinkKafkaConsumer<String> ujdKafkaConsumer = KafkaUtil.getKafkaConsumer(ujdTopic, groupId);
        DataStreamSource<String> ujdSource = env.addSource(ujdKafkaConsumer);
        SingleOutputStreamOperator<TrafficPageViewBean> ujdMappedStream =
                ujdSource.map(jsonStr -> {
                    JSONObject jsonObj = JSONObject.parseObject(jsonStr);
                    JSONObject common = jsonObj.getJSONObject("common");
                    Long ts = jsonObj.getLong("ts") + 10 * 1000L;

                    // 获取维度信息
                    String vc = common.getString("vc");
                    String ch = common.getString("ch");
                    String ar = common.getString("ar");
                    String isNew = common.getString("is_new");

                    // 封装为实体类
                    return new TrafficPageViewBean(
                            "",
                            "",
                            vc,
                            ch,
                            ar,
                            isNew,
                            0L,
                            0L,
                            0L,
                            0L,
                            1L,
                            ts
                    );
                });


        // 6.2 从 Kafka dwd_traffic_unique_visitor_detail 主题读取独立访客数据，封装为流
        String uvTopic = "dwd_traffic_unique_visitor_detail";
        FlinkKafkaConsumer<String> uvKafkaConsumer = KafkaUtil.getKafkaConsumer(uvTopic, groupId);
        DataStreamSource<String> uvSource = env.addSource(uvKafkaConsumer);
        SingleOutputStreamOperator<TrafficPageViewBean> uvMappedStream =
                uvSource.map(jsonStr -> {
                    JSONObject jsonObj = JSON.parseObject(jsonStr);
                    JSONObject common = jsonObj.getJSONObject("common");
                    Long ts = jsonObj.getLong("ts");

                    // 获取维度信息
                    String vc = common.getString("vc");
                    String ch = common.getString("ch");
                    String ar = common.getString("ar");
                    String isNew = common.getString("is_new");

                    // 封装为实体类
                    return new TrafficPageViewBean(
                            "",
                            "",
                            vc,
                            ch,
                            ar,
                            isNew,
                            1L,
                            0L,
                            0L,
                            0L,
                            0L,
                            ts
                    );
                });
        // 6.3 合并三条流
        DataStream<TrafficPageViewBean> pageViewBeanDS = mainStream
                .union(ujdMappedStream)
                .union(uvMappedStream);
        // TODO 7. 设置水位线
        SingleOutputStreamOperator<TrafficPageViewBean> withWatermarkStream = pageViewBeanDS.assignTimestampsAndWatermarks(WatermarkStrategy.<TrafficPageViewBean>forMonotonousTimestamps()
                .withTimestampAssigner((SerializableTimestampAssigner<TrafficPageViewBean>) (trafficPageViewBean, l) -> trafficPageViewBean.getTs()));
        // TODO 8. 按照维度分组
        KeyedStream<TrafficPageViewBean, Tuple4<String, String, String, String>> keyedBeanStream = withWatermarkStream.keyBy(trafficPageViewBean ->
                        Tuple4.of(
                                trafficPageViewBean.getVc(),
                                trafficPageViewBean.getCh(),
                                trafficPageViewBean.getAr(),
                                trafficPageViewBean.getIsNew()
                        )
                , Types.TUPLE(Types.STRING, Types.STRING, Types.STRING, Types.STRING)
        );
        // TODO 9. 开窗
        WindowedStream<TrafficPageViewBean, Tuple4<String, String, String, String>, TimeWindow> windowStream = keyedBeanStream.window(
                        TumblingEventTimeWindows.of(Time.seconds(10L)))
                .allowedLateness(Time.seconds(10L));
        // TODO 10. 聚合计算
        SingleOutputStreamOperator<TrafficPageViewBean> reducedStream = windowStream.reduce(new ReduceFunction<TrafficPageViewBean>() {
            @Override
            public TrafficPageViewBean reduce(TrafficPageViewBean trafficPageViewBean, TrafficPageViewBean t1) throws Exception {
                trafficPageViewBean.setUvCt(trafficPageViewBean.getUvCt() + t1.getUvCt());
                trafficPageViewBean.setSvCt(trafficPageViewBean.getSvCt() + t1.getSvCt());
                trafficPageViewBean.setPvCt(trafficPageViewBean.getPvCt() + t1.getPvCt());
                trafficPageViewBean.setDurSum(trafficPageViewBean.getDurSum() + t1.getDurSum());
                trafficPageViewBean.setUjCt(trafficPageViewBean.getUjCt() + t1.getUjCt());
                return trafficPageViewBean;
            }
        }, new ProcessWindowFunction<TrafficPageViewBean, TrafficPageViewBean, Tuple4<String, String, String, String>, TimeWindow>() {

            @Override
            public void process(Tuple4<String, String, String, String> stringStringStringStringTuple4, ProcessWindowFunction<TrafficPageViewBean, TrafficPageViewBean, Tuple4<String, String, String, String>, TimeWindow>.Context context, Iterable<TrafficPageViewBean> iterable, Collector<TrafficPageViewBean> collector) throws Exception {
                // 补充窗口信息
                String stt = DateFormatUtil.toYmdHms(context.window().getStart());
                String edt = DateFormatUtil.toYmdHms(context.window().getEnd());
                for (TrafficPageViewBean element : iterable) {
                    element.setStt(stt);
                    element.setEdt(edt);
                    // 修改TS作为版本信息
                    element.setTs(System.currentTimeMillis());
                    collector.collect(element);
                }
            }
        });
        // TODO 11. 写入 OLAP 数据库
        reducedStream.addSink(ClickHouseUtil.<TrafficPageViewBean>getJdbcSink(
                "insert into dws_traffic_vc_ch_ar_is_new_page_view_window values(?,?,?,?,?,?,?,?,?,?,?,?)"
        ));
        env.execute("DwsTrafficVcChArIsNewPageViewWindow");
    }
}
