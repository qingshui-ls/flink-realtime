package com.ecust.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ecust.bean.TradePaymentWindowBean;
import com.ecust.utils.ClickHouseUtil;
import com.ecust.utils.DateFormatUtil;
import com.ecust.utils.KafkaUtil;
import com.ecust.utils.TimestampLtz3CompareUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * 交易域支付各窗口汇总表
 * 10.7.1 主要任务
 * 从 Kafka 读取交易域支付成功主题数据，统计支付成功独立用户数和首次支付成功用户数。
 * （1）从 Kafka 支付成功明细主题读取数据
 * （2）转换数据结构 String 转换为 JSONObject。
 * （3）按照唯一键分组
 * （4）去重
 * （5）设置水位线，按照 user_id 分组
 * （6）统计独立支付人数和新增支付人数	 运用 Flink 状态编程，在状态中维护用户末次支付日期。
 * 若末次支付日期为 null，则将首次支付用户数和支付独立用户数均置为 1；否则首次支付用户数置为 0，判断末次支付日期是否为当日，如果不是当日则支付独立用户数置为 1，否则置为 0。最后将状态中的支付日期更新为当日。
 * （7）开窗、聚合 度量字段求和，补充窗口起始时间和结束时间字段，ts 字段置为当前系统时间戳。
 * （8）写出到 ClickHouse
 *
 *  数据流 ： web/app -> nginx -> 业务服务器(Mysql) -> Maxwell -> Kafka(ODS) -> FlinkAPP -> Kafka(DWD) -> FlinkAPP -> Kafka(DWD) -> FlinkAPP -> Clickhouse(DWS)
 *  程序：Mock(模拟产生业务数据) -> Mysql -> Maxwell ->  Kafka(ZK) -> FlinkAPP(DwdTradeOrderPreProcess) -> Kafka(DWD) -> FlinkAPP(DwdTradePayDetailSuc+DwdTradeOrderDetail) -> Kafka(DWD) -> DwsTradePaymentSucWindow -> Clickhouse(DWS)
 */
public class DwsTradePaymentSucWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // TODO 3. 从 Kafka dwd_trade_pay_detail_suc 主题读取支付成功明细数据，封装为流
        String topic = "dwd_trade_pay_detail_suc";
        String groupId = "dws_trade_payment_suc_window";
        DataStreamSource<String> dataStreamSource = env.addSource(KafkaUtil.getKafkaConsumer(topic, groupId));
        SingleOutputStreamOperator<JSONObject> jsonObjDS = dataStreamSource.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String s, Collector<JSONObject> collector) throws Exception {
                JSONObject jsonObject = null;
                try {
                    jsonObject = JSON.parseObject(s);
                    collector.collect(jsonObject);
                } catch (Exception e) {
                    System.out.println(">>>>>>>>>" + s);
                }
            }
        });

        KeyedStream<JSONObject, String> orderDetailId = jsonObjDS.keyBy(x -> x.getString("order_detail_id"));
        SingleOutputStreamOperator<JSONObject> process = orderDetailId.process(new KeyedProcessFunction<String, JSONObject, JSONObject>() {
            private ValueState<JSONObject> valueState;

            @Override
            public void open(Configuration parameters) throws Exception {
                valueState = getRuntimeContext().getState(new ValueStateDescriptor<JSONObject>("value-state", JSONObject.class));
            }

            @Override
            public void processElement(JSONObject jsonObject, KeyedProcessFunction<String, JSONObject, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                // 获取状态数据
                JSONObject state = valueState.value();
                if (state == null) {
                    valueState.update(jsonObject);
                    context.timerService().registerProcessingTimeTimer(context.timerService().currentProcessingTime() + 5000L);
                } else {
                    String stateRt = state.getString("row_op_ts");
                    String curRt = jsonObject.getString("row_op_ts");
                    int compare = TimestampLtz3CompareUtil.compare(stateRt, curRt);
                    if (compare != 1) {
                        valueState.update(jsonObject);
                    }
                }
            }

            @Override
            public void onTimer(long timestamp, KeyedProcessFunction<String, JSONObject, JSONObject>.OnTimerContext ctx, Collector<JSONObject> out) throws Exception {
                super.onTimer(timestamp, ctx, out);
                // 输出并清空状态数据
                JSONObject value = valueState.value();
                out.collect(value);
                valueState.clear();
            }
        });
        SingleOutputStreamOperator<JSONObject> watermarkDS = process.assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                    @Override
                    public long extractTimestamp(JSONObject jsonObject, long l) {
                        String cTime = jsonObject.getString("callback_time");
                        return DateFormatUtil.toTs(cTime, true);
                    }
                }));


        KeyedStream<JSONObject, String> keyedStream = watermarkDS.keyBy(x -> x.getString("user_id"));
        SingleOutputStreamOperator<TradePaymentWindowBean> tradePaymentDS = keyedStream.flatMap(new RichFlatMapFunction<JSONObject, TradePaymentWindowBean>() {
            private ValueState<String> lastDtState;

            @Override
            public void open(Configuration parameters) throws Exception {
                lastDtState = getRuntimeContext().getState(new ValueStateDescriptor<String>("last-dt", String.class));
            }

            @Override
            public void flatMap(JSONObject jsonObject, Collector<TradePaymentWindowBean> collector) throws Exception {
                // 取出当前日期和状态日期
                String lastDt = lastDtState.value();
                String curDt = jsonObject.getString("callback_time").split(" ")[0];
                // 定义当日支付人数和新增的付费用户数
                long paymentSucUniqueUserCount = 0L;
                long paymentSucNewUserCount = 0L;

                // 判断状态日期是否为null
                if (lastDt == null) {
                    paymentSucUniqueUserCount = 1L;
                    paymentSucNewUserCount = 1L;
                    lastDtState.update(curDt);
                } else if (!lastDt.equals(curDt)) {
                    paymentSucUniqueUserCount = 1L;
                    lastDtState.update(curDt);
                }
                // 返回数据
                if (paymentSucUniqueUserCount == 1) {
                    collector.collect(new TradePaymentWindowBean("", "", paymentSucUniqueUserCount, paymentSucNewUserCount, null));
                }
            }
        });
        SingleOutputStreamOperator<TradePaymentWindowBean> result = tradePaymentDS.windowAll(TumblingEventTimeWindows.of(Time.seconds(10)))
                .reduce(new ReduceFunction<TradePaymentWindowBean>() {
                    @Override
                    public TradePaymentWindowBean reduce(TradePaymentWindowBean tradePaymentWindowBean, TradePaymentWindowBean t1) throws Exception {
                        tradePaymentWindowBean.setPaymentSucNewUserCount(tradePaymentWindowBean.getPaymentSucNewUserCount() + t1.getPaymentSucNewUserCount());
                        tradePaymentWindowBean.setPaymentSucUniqueUserCount(tradePaymentWindowBean.getPaymentSucUniqueUserCount() + t1.getPaymentSucUniqueUserCount());
                        return tradePaymentWindowBean;
                    }
                }, new AllWindowFunction<TradePaymentWindowBean, TradePaymentWindowBean, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow timeWindow, Iterable<TradePaymentWindowBean> iterable, Collector<TradePaymentWindowBean> collector) throws Exception {
                        TradePaymentWindowBean next = iterable.iterator().next();
                        next.setTs(System.currentTimeMillis());
                        next.setEdt(DateFormatUtil.toYmdHms(timeWindow.getEnd()));
                        next.setStt(DateFormatUtil.toYmdHms(timeWindow.getStart()));
                        collector.collect(next);
                    }
                });
        result.print(">>>>>>");
        result.addSink(ClickHouseUtil.<TradePaymentWindowBean>getJdbcSink("insert into dws_trade_payment_suc_window values(?,?,?,?,?)"));
        env.execute("DwsTradePaymentSucWindow");
    }
}
