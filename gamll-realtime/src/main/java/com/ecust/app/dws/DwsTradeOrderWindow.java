package com.ecust.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ecust.bean.TradeOrderBean;
import com.ecust.utils.ClickHouseUtil;
import com.ecust.utils.DateFormatUtil;
import com.ecust.utils.KafkaUtil;
import com.ecust.utils.TimestampLtz3CompareUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FilterFunction;
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
 * 交易域下单各窗口汇总表 :从 Kafka 订单明细主题读取数据，对数据去重，统计当日下单独立用户数和新增下单用户数，封装为实体类，写入 ClickHouse。
 * 1）从 Kafka订单明细主题读取数据
 * 2）转换数据结构
 * Kafka 订单明细主题的数据是通过 Kafka-Connector 从订单预处理主题读取后进行过滤获取的，Kafka-Connector 会过滤掉主题中的 null 数据，因此订单明细主题不存在为 null 的数据，直接转换数据结构即可。
 * 3）按照 order_detail_id 分组
 * order_detail_id 为数据唯一键。
 * 4）对 order_detail_id 相同的数据去重
 * 按照上文提到的方案对数据去重。
 * 5）设置水位线
 * 6）按照用户 id 分组
 * 7）计算度量字段的值
 * （1）当日下单独立用户数和新增下单用户数
 * 运用 Flink 状态编程，在状态中维护用户末次下单日期。
 * 若末次下单日期为 null，则将首次下单用户数和下单独立用户数均置为 1；否则首次下单用户数置为 0，判断末次下单日期是否为当日，如果不是当日则下单独立用户数置为 1，否则置为 0。最后将状态中的下单日期更新为当日。
 * （2）其余度量字段直接取流中数据的对应值即可。
 * 8）开窗、聚合
 * 度量字段求和，补充窗口起始时间和结束时间字段，ts 字段置为当前系统时间戳。
 * 9）写出到 ClickHouse。
 * <p>
 * 数据流 ： web/app -> nginx -> 业务服务器(Mysql) -> Maxwell -> Kafka(ODS)-> Kafka(ODS) -> FlinkAPP(BaseLogAPP) -> Kafka(DWD) -> FlinkAPP -> Kafka (DWD)-> FlinkAPP -> clickhouse(DWS)
 * 程序：Mock(模拟产生业务数据) -> Mysql -> Maxwell ->  Kafka(ZK) -> FlinkAPP(DwdTradeOrderPreProcess) -> Kafka(DWD) -> DwdTradeOrderDetail -> Kafka (DWD)-> DwsTradeOrderWindow -> clickhouse(DWS)
 * <p>
 * DwdTradeOrderPreProcess + DwdTradeOrderDetail + DwsTradeOrderWindow
 */
public class DwsTradeOrderWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // TODO 3. 从 Kafka dwd_trade_order_detail 读取订单明细数据
        String topic = "dwd_trade_order_detail";
        String groupId = "dws_trade_order_window";
        FlinkKafkaConsumer<String> kafkaConsumer = KafkaUtil.getKafkaConsumer(topic, groupId);
        DataStreamSource<String> source = env.addSource(kafkaConsumer);

        // TODO 4. 转换数据结构
        SingleOutputStreamOperator<String> filteredDS = source.filter(
                new FilterFunction<String>() {

                    @Override
                    public boolean filter(String jsonStr) throws Exception {
                        JSONObject jsonObj = JSON.parseObject(jsonStr);
                        String userId = jsonObj.getString("user_id");
                        String sourceTypeName = jsonObj.getString("source_type_name");
                        if (userId != null && sourceTypeName != null) {
                            return true;
                        }
                        return false;
                    }
                }
        );
        SingleOutputStreamOperator<JSONObject> mappedStream = filteredDS.map(JSON::parseObject);

        // TODO 5. 按照 order_detail_id 分组
        KeyedStream<JSONObject, String> keyedStream = mappedStream.keyBy(r -> r.getString("id"));

        // TODO 6. 对 order_detail_id 相同的数据去重
        SingleOutputStreamOperator<JSONObject> processedStream = keyedStream
                .process(
                        new KeyedProcessFunction<String, JSONObject, JSONObject>() {

                            private ValueState<JSONObject> filterState;

                            @Override
                            public void open(Configuration parameters) throws Exception {
                                super.open(parameters);
                                filterState = getRuntimeContext().getState(
                                        new ValueStateDescriptor<JSONObject>("filter_state", JSONObject.class)
                                );
                            }

                            @Override
                            public void processElement(JSONObject jsonObj, Context ctx, Collector<JSONObject> out) throws Exception {
                                JSONObject lastData = filterState.value();

                                if (lastData == null) {
                                    long currentProcessingTime = ctx.timerService().currentProcessingTime();
                                    ctx.timerService().registerProcessingTimeTimer(currentProcessingTime + 5000L);
                                    filterState.update(jsonObj);
                                } else {
                                    String lastRowOpTs = lastData.getString("row_op_ts");
                                    String rowOpTs = jsonObj.getString("row_op_ts");
                                    if (TimestampLtz3CompareUtil.compare(lastRowOpTs, rowOpTs) <= 0) {
                                        filterState.update(jsonObj);
                                    }
                                }

                            }

                            @Override
                            public void onTimer(long timestamp, OnTimerContext ctx, Collector<JSONObject> out) throws Exception {
                                super.onTimer(timestamp, ctx, out);
                                JSONObject currentValue = filterState.value();
                                if (currentValue != null) {
                                    out.collect(currentValue);
                                }
                                filterState.clear();
                            }
                        }
                );

        // TODO 7. 设置水位线
        SingleOutputStreamOperator<JSONObject> withWatermarkStream = processedStream.assignTimestampsAndWatermarks(
                WatermarkStrategy.<JSONObject>forMonotonousTimestamps()
                        .withTimestampAssigner(
                                (SerializableTimestampAssigner<JSONObject>) (jsonObj, recordTimestamp) -> DateFormatUtil.toTs(jsonObj.getString("create_time"), true)
                        )
        );

        // TODO 8. 按照用户 id 分组
        KeyedStream<JSONObject, String> keyedByUserIdStream = withWatermarkStream.keyBy(r -> r.getString("user_id"));

        // TODO 9. 统计当日下单独立用户数和新增下单用户数
        SingleOutputStreamOperator<TradeOrderBean> orderBeanStream = keyedByUserIdStream.process(
                new KeyedProcessFunction<String, JSONObject, TradeOrderBean>() {

                    private ValueState<String> lastOrderDtState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        lastOrderDtState = getRuntimeContext().getState(
                                new ValueStateDescriptor<String>("last_order_dt_state", String.class)
                        );
                    }

                    @Override
                    public void processElement(JSONObject jsonObj, Context ctx, Collector<TradeOrderBean> out) throws Exception {
                        String lastOrderDt = lastOrderDtState.value();
                        String orderDt = jsonObj.getString("date_id");

                        Long orderNewUserCount = 0L;
                        Long orderUniqueUserCount = 0L;
                        Double splitActivityAmount = jsonObj.getDouble("split_activity_amount");
                        Double splitCouponAmount = jsonObj.getDouble("split_coupon_amount");
                        Double splitOriginalAmount = jsonObj.getDouble("split_original_amount");
//                        Long ts = jsonObj.getLong("ts") * 1000L;

                        if (lastOrderDt == null) {
                            orderNewUserCount = 1L;
                            orderUniqueUserCount = 1L;
                        } else {
                            if (!lastOrderDt.equals(orderDt)) {
                                orderUniqueUserCount = 1L;
                            }
                        }
                        lastOrderDtState.update(orderDt);

                        TradeOrderBean tradeOrderBean = new TradeOrderBean(
                                "",
                                "",
                                orderUniqueUserCount,
                                orderNewUserCount,
                                splitActivityAmount,
                                splitCouponAmount,
                                splitOriginalAmount,
                                null
                        );

                        out.collect(tradeOrderBean);
                    }
                }
        );

        // TODO 10. 开窗
        AllWindowedStream<TradeOrderBean, TimeWindow> windowDS = orderBeanStream.windowAll(TumblingEventTimeWindows.of(
                org.apache.flink.streaming.api.windowing.time.Time.seconds(10L)));

        // TODO 11. 聚合
        SingleOutputStreamOperator<TradeOrderBean> aggregatedStream = windowDS.aggregate(
                new AggregateFunction<TradeOrderBean, TradeOrderBean, TradeOrderBean>() {

                    @Override
                    public TradeOrderBean createAccumulator() {
                        return new TradeOrderBean(
                                "",
                                "",
                                0L,
                                0L,
                                0.0,
                                0.0,
                                0.0,
                                0L
                        );
                    }

                    @Override
                    public TradeOrderBean add(TradeOrderBean value, TradeOrderBean accumulator) {
                        accumulator.setOrderUniqueUserCount(
                                accumulator.getOrderUniqueUserCount() + value.getOrderUniqueUserCount()
                        );
                        accumulator.setOrderNewUserCount(
                                accumulator.getOrderNewUserCount() + value.getOrderNewUserCount()
                        );
                        accumulator.setOrderActivityReduceAmount(
                                accumulator.getOrderActivityReduceAmount() +
                                        (value.getOrderActivityReduceAmount() == null
                                                ? 0.0 : value.getOrderActivityReduceAmount()
                                        )
                        );
                        accumulator.setOrderCouponReduceAmount(
                                accumulator.getOrderCouponReduceAmount() +
                                        (value.getOrderCouponReduceAmount() == null
                                                ? 0.0 : value.getOrderCouponReduceAmount()
                                        )
                        );
                        accumulator.setOrderOriginalTotalAmount(
                                accumulator.getOrderOriginalTotalAmount() +
                                        (value.getOrderOriginalTotalAmount() == null
                                                ? 0.0 : value.getOrderOriginalTotalAmount()
                                        )
                        );
                        return accumulator;
                    }

                    @Override
                    public TradeOrderBean getResult(TradeOrderBean accumulator) {
                        return accumulator;
                    }

                    @Override
                    public TradeOrderBean merge(TradeOrderBean a, TradeOrderBean b) {
                        return null;
                    }
                },
                new AllWindowFunction<TradeOrderBean, TradeOrderBean, TimeWindow>() {

                    @Override
                    public void apply(TimeWindow window, Iterable<TradeOrderBean> values, Collector<TradeOrderBean> out) throws Exception {
                        String stt = DateFormatUtil.toYmdHms(window.getStart());
                        String edt = DateFormatUtil.toYmdHms(window.getEnd());

                        for (TradeOrderBean value : values) {
                            value.setStt(stt);
                            value.setEdt(edt);
                            value.setTs(System.currentTimeMillis());
                            out.collect(value);
                        }
                    }
                }
        );

        aggregatedStream.print("aggregatedStream>>>>");
        // TODO 12. 写出到 OLAP 数据库
        SinkFunction<TradeOrderBean> jdbcSink = ClickHouseUtil.<TradeOrderBean>getJdbcSink(
                "insert into dws_trade_order_window values(?,?,?,?,?,?,?,?)"
        );
        aggregatedStream.<TradeOrderBean>addSink(jdbcSink);


        env.execute("DwsTradeOrderWindow");
    }
}
