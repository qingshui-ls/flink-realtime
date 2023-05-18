package com.ecust.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ecust.app.func.DimAsyncFunction;
import com.ecust.bean.TradeProvinceOrderWindow;
import com.ecust.utils.ClickHouseUtil;
import com.ecust.utils.DateFormatUtil;
import com.ecust.utils.KafkaUtil;
import com.ecust.utils.TimestampLtz3CompareUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;

/**
 * 从 Kafka 读取订单明细数据，过滤 null 数据并按照唯一键对数据去重，统计各省份各窗口订单数和订单金额，将数据写入 ClickHouse 交易域省份粒度下单各窗口汇总表。
 * 1）从 Kafka 订单明细主题读取数据
 * 2）转换数据结构
 * 3）按照唯一键去重
 * 4）转换数据结构
 * JSONObject 转换为实体类 TradeProvinceOrderWindow。
 * 5）设置水位线
 * 6）按照省份 ID 分组
 * provinceId 可以唯一标识数据。
 * 7）开窗
 * 8）聚合计算
 * 度量字段求和，并在窗口闭合后补充窗口起始时间和结束时间。将时间戳置为当前系统时间。
 * 9）关联省份信息
 * 补全省份名称字段。
 * 10）写出到 ClickHouse。
 * 数据流 ： web/app -> nginx -> 业务服务器(Mysql) -> Maxwell -> Kafka(ODS)-> Kafka(ODS) -> FlinkAPP(BaseLogAPP) -> Kafka(DWD) -> FlinkAPP -> Kafka (DWD) -> FlinkAPP -> clickhouse(DWS)
 * 程序：Mock(模拟产生业务数据) -> Mysql -> Maxwell ->  Kafka(ZK) -> FlinkAPP(DwdTradeOrderPreProcess) -> Kafka(DWD) -> DwdTradeOrderDetail -> Kafka (DWD) -> DwsTradeProvinceOrderWindow -> clickhouse(DWS)
 * HDFS + ZK + KF + MAXWELL + HBASE + REDIS + CLICKHOUSE
 * DimApp -> DwdTradeOrderPreProcess -> DwdTradeOrderDetail->  DwsTradeProvinceOrderWindow
 */
public class DwsTradeProvinceOrderWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // TODO 3. 从 Kafka dwd_trade_order_detail 主题读取订单明细数据
        String topic = "dwd_trade_order_detail";
        String groupId = "dws_trade_province_order_window";

        DataStreamSource<String> dataStreamSource = env.addSource(KafkaUtil.getKafkaConsumer(topic, groupId));
        SingleOutputStreamOperator<JSONObject> map = dataStreamSource.map(JSON::parseObject);
        KeyedStream<JSONObject, String> keyedStream = map.keyBy(x -> x.getString("id"));
        SingleOutputStreamOperator<JSONObject> process = keyedStream.process(new KeyedProcessFunction<String, JSONObject, JSONObject>() {
            private ValueState<JSONObject> lastValueState;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                lastValueState = getRuntimeContext().getState(new ValueStateDescriptor<JSONObject>("last_value_state", JSONObject.class));
            }

            @Override
            public void processElement(JSONObject jsonObject, KeyedProcessFunction<String, JSONObject, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                JSONObject lastValue = lastValueState.value();
                if (lastValue == null) {
                    long currentProcessingTime = context.timerService().currentProcessingTime();
                    context.timerService().registerProcessingTimeTimer(currentProcessingTime + 5000L);
                    lastValueState.update(jsonObject);
                } else {
                    String lastRowOpTs = lastValue.getString("row_op_ts");
                    String rowOpTs = jsonObject.getString("row_op_ts");
                    if (TimestampLtz3CompareUtil.compare(lastRowOpTs, rowOpTs) <= 0) {
                        lastValueState.update(jsonObject);
                    }
                }

            }

            @Override
            public void onTimer(long timestamp, OnTimerContext ctx, Collector<JSONObject> out) throws IOException {
                JSONObject lastValue = this.lastValueState.value();
                if (lastValue != null) {
                    out.collect(lastValue);
                }
                lastValueState.clear();
            }
        });
        SingleOutputStreamOperator<TradeProvinceOrderWindow> tradeProvinceDS = process.map(x -> {
            String provinceId = x.getString("province_id");
            String orderId = x.getString("order_id");
            Double orderAmount = x.getDouble("split_total_amount");
            Long ts = x.getLong("ts") * 1000L;

            return TradeProvinceOrderWindow.builder()
                    .provinceId(provinceId)
                    .orderIdSet(new HashSet<String>(Collections.singleton(orderId)))
                    .orderAmount(orderAmount)
                    .ts(ts)
                    .build();
        });
        SingleOutputStreamOperator<TradeProvinceOrderWindow> tradeProvinceWmDS = tradeProvinceDS.assignTimestampsAndWatermarks(WatermarkStrategy.<TradeProvinceOrderWindow>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner(new SerializableTimestampAssigner<TradeProvinceOrderWindow>() {
                    @Override
                    public long extractTimestamp(TradeProvinceOrderWindow tradeProvinceOrderWindow, long l) {
                        return tradeProvinceOrderWindow.getTs();
                    }
                }));
        // TODO 9. 按照省份 ID 分组
        KeyedStream<TradeProvinceOrderWindow, String> tradeProvinceKeyedStream = tradeProvinceWmDS.keyBy(x -> x.getProvinceId());
        SingleOutputStreamOperator<TradeProvinceOrderWindow> reduceDS = tradeProvinceKeyedStream.window(TumblingEventTimeWindows.of(Time.seconds(10L)))
                .reduce(new ReduceFunction<TradeProvinceOrderWindow>() {
                    @Override
                    public TradeProvinceOrderWindow reduce(TradeProvinceOrderWindow x1, TradeProvinceOrderWindow t1) throws Exception {
                        x1.getOrderIdSet().addAll(t1.getOrderIdSet());
                        x1.setOrderAmount(x1.getOrderAmount() + t1.getOrderAmount());
                        return x1;
                    }
                }, new WindowFunction<TradeProvinceOrderWindow, TradeProvinceOrderWindow, String, TimeWindow>() {
                    @Override
                    public void apply(String s, TimeWindow timeWindow, Iterable<TradeProvinceOrderWindow> iterable, Collector<TradeProvinceOrderWindow> collector) throws Exception {
                        TradeProvinceOrderWindow next = iterable.iterator().next();
                        next.setStt(DateFormatUtil.toYmdHms(timeWindow.getStart()));
                        next.setEdt(DateFormatUtil.toYmdHms(timeWindow.getEnd()));
//                        System.out.println("TimeWindow End" + DateFormatUtil.toYmdHms(timeWindow.getStart()));
                        next.setTs(System.currentTimeMillis());
                        next.setOrderCount((long) next.getOrderIdSet().size());
                        collector.collect(next);
                    }
                });
        // TODO 12. 关联省份信息
        SingleOutputStreamOperator<TradeProvinceOrderWindow> result = AsyncDataStream.unorderedWait(reduceDS, new DimAsyncFunction<TradeProvinceOrderWindow>("DIM_BASE_PROVINCE") {
                    @Override
                    public void join(TradeProvinceOrderWindow tradeProvinceOrderWindow, JSONObject dimInfo) {
                        tradeProvinceOrderWindow.setProvinceName(dimInfo.getString("name".toUpperCase()));
                    }

                    @Override
                    public String getkey(TradeProvinceOrderWindow tradeProvinceOrderWindow) {
                        return tradeProvinceOrderWindow.getProvinceId();
                    }
                },
                100, TimeUnit.SECONDS);
        // TODO 13. 写入到 OLAP 数据库
        SinkFunction<TradeProvinceOrderWindow> jdbcSink = ClickHouseUtil.<TradeProvinceOrderWindow>getJdbcSink(
                "insert into dws_trade_province_order_window values(?,?,?,?,?,?,?)"
        );
        result.<TradeProvinceOrderWindow>addSink(jdbcSink);

        env.execute();

    }
}
