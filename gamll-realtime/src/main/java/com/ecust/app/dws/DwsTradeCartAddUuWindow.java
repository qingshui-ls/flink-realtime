package com.ecust.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ecust.bean.CartAddUuBean;
import com.ecust.utils.ClickHouseUtil;
import com.ecust.utils.DateFormatUtil;
import com.ecust.utils.KafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

/**
 * 从 Kafka 读取用户加购明细数据，统计每日各窗口加购独立用户数，写入 ClickHouse。
 * 1）从 Kafka 加购明细主题读取数据
 * 2）转换数据结构 将流中数据由 String 转换为 JSONObject。
 * 3）设置水位线
 * 4）按照用户 id 分组
 * 5）过滤独立用户加购记录
 * 运用 Flink 状态编程，将用户末次加购日期维护到状态中。
 * 如果末次登陆日期为 null 或者不等于当天日期，则保留数据并更新状态，否则丢弃，不做操作。
 * 6）开窗、聚合
 * 统计窗口中数据条数即为加购独立用户数，补充窗口起始时间、关闭时间，将时间戳字段置为当前系统时间，发送到下游。
 * 7）将数据写入 ClickHouse。
 * <p>
 * 数据流 ： web/app -> nginx -> 业务服务器(Mysql) -> Maxwell -> Kafka(ODS) -> FlinkAPP -> Kafka(DWD) -> FlinkAPP -> Clickhouse(DWS)
 * * 程序：Mock(模拟产生业务数据) -> Mysql -> Maxwell ->  Kafka(ZK) -> FlinkAPP(DwdTradeCartAdd) -> Kafka(DWD) -> DwsTradeCartAddUuWindow -> Clickhouse(DWS)
 */
public class DwsTradeCartAddUuWindow {
    public static void main(String[] args) throws Exception {
        // 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);// 生产环境设置为kafka的主题分区数
        // TODO 3. 从 Kafka dwd_trade_cart_add 主题读取数据
        String topic = "dwd_trade_cart_add";
        String groupId = "dws_trade_cart_add_uu_window";
        FlinkKafkaConsumer<String> kafkaConsumer = KafkaUtil.getKafkaConsumer(topic, groupId);
        DataStreamSource<String> source = env.addSource(kafkaConsumer);

        // TODO 4. 转换数据结构
        SingleOutputStreamOperator<JSONObject> mappedStream = source.map(JSON::parseObject);

        // TODO 5. 设置水位线
        SingleOutputStreamOperator<JSONObject> withWatermarkDS = mappedStream.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<JSONObject>forMonotonousTimestamps()
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<JSONObject>() {
                                    @Override
                                    public long extractTimestamp(JSONObject jsonObj, long recordTimestamp) {
                                        String operateTime = jsonObj.getString("operate_time");
                                        if (operateTime != null) {
                                            return DateFormatUtil.toTs(operateTime, true);
                                        } else {
                                            return DateFormatUtil.toTs(jsonObj.getString("create_time"), true);
                                        }
                                    }
                                }
                        )
        );

        // TODO 6. 按照用户 id 分组
        KeyedStream<JSONObject, String> keyedStream = withWatermarkDS.keyBy(r -> r.getString("user_id"));

        // TODO 7. 筛选加购独立用户
        SingleOutputStreamOperator<JSONObject> filteredStream = keyedStream.process(
                new KeyedProcessFunction<String, JSONObject, JSONObject>() {

                    private ValueState<String> lastCartAddDt;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        lastCartAddDt = getRuntimeContext().getState(
                                new ValueStateDescriptor<String>("last_cart_add_dt", String.class)
                        );
                    }

                    @Override
                    public void processElement(JSONObject jsonObj, Context ctx, Collector<JSONObject> out) throws Exception {
                        String lastCartAdd = lastCartAddDt.value();
                        String operateTime = jsonObj.getString("operate_time");
                        String curDt = null;
                        if (operateTime != null) {
                            curDt = operateTime.split(" ")[0];
                        }else {
                            String createTime = jsonObj.getString("create_time");
                            curDt = createTime.split(" ")[0];
                        }

                        if (lastCartAdd == null || !lastCartAdd.equals(curDt)) {
                            out.collect(jsonObj);
                        }
                    }
                }
        );

        // TODO 8. 开窗
        AllWindowedStream<JSONObject, TimeWindow> windowDS = filteredStream.windowAll(TumblingEventTimeWindows.of(
                org.apache.flink.streaming.api.windowing.time.Time.seconds(10L)));

        // TODO 9. 聚合
        SingleOutputStreamOperator<CartAddUuBean> aggregateDS = windowDS.aggregate(
                new AggregateFunction<JSONObject, Long, Long>() {

                    @Override
                    public Long createAccumulator() {
                        return 0L;
                    }

                    @Override
                    public Long add(JSONObject jsonObj, Long accumulator) {
                        return ++accumulator;
                    }

                    @Override
                    public Long getResult(Long accumulator) {
                        return accumulator;
                    }

                    @Override
                    public Long merge(Long a, Long b) {
                        return null;
                    }
                },
                new AllWindowFunction<Long, CartAddUuBean, TimeWindow>() {

                    @Override
                    public void apply(TimeWindow window, Iterable<Long> values, Collector<CartAddUuBean> out) throws Exception {
                        String stt = DateFormatUtil.toYmdHms(window.getStart());
                        String edt = DateFormatUtil.toYmdHms(window.getEnd());
                        for (Long value : values) {
                            CartAddUuBean cartAddUuBean = new CartAddUuBean(
                                    stt,
                                    edt,
                                    value,
                                    System.currentTimeMillis()
                            );
                            out.collect(cartAddUuBean);
                        }
                    }
                }
        );

        // TODO 10. 写入到 OLAP 数据库
        aggregateDS.print(">>>");

        SinkFunction<CartAddUuBean> jdbcSink = ClickHouseUtil.<CartAddUuBean>getJdbcSink(
                "insert into dws_trade_cart_add_uu_window values(?,?,?,?)"
        );
        aggregateDS.<CartAddUuBean>addSink(jdbcSink);

        env.execute();
    }

}
