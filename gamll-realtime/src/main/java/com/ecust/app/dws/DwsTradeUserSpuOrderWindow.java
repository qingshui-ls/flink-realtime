package com.ecust.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ecust.app.func.DimAsyncFunction;
import com.ecust.bean.TradeUserSpuOrderBean;
import com.ecust.utils.ClickHouseUtil;
import com.ecust.utils.DateFormatUtil;
import com.ecust.utils.KafkaUtil;
import com.ecust.utils.TimestampLtz3CompareUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;


import java.time.Duration;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;

/**
 * 从 Kafka 订单明细主题读取数据，过滤 null 数据并按照唯一键对数据去重，关联维度信息，按照维度分组，统计各维度各窗口的订单数和订单金额，将数据写入 ClickHouse 交易域品牌-品类-用户-SPU粒度下单各窗口汇总表。
 * （1）从 Kafka 订单明细主题读取数据
 * （2）转换数据结构
 * （3）按照唯一键去重
 * （4）转换数据结构 JSONObject 转换为实体类 TradeTrademarkCategoryUserSpuOrderBean。
 * （5）补充与分组相关的维度信息
 * 关联 sku_info 表
 * 获取 tm_id，category3_id，spu_id。
 * （6）设置水位线
 * （7）分组、开窗、聚合
 * 按照维度信息分组，度量字段求和，并在窗口闭合后补充窗口起始时间和结束时间。将时间戳置为当前系统时间。
 * （8）维度关联，补充与分组无关的维度字段
 * ① 关联 spu_info 表
 * 获取 spu_name。
 * ② 关联 base_trademark 表
 * 获取 tm_name。
 * ③ 关联 base_category3 表
 * 获取 name（三级品类名称），获取 category2_id。
 * ④ 关联 base_categroy2 表
 * 获取 name（二级品类名称），category1_id。
 * ⑤ 关联 base_category1 表
 * 获取 name（一级品类名称）。
 * （9）写出到 ClickHouse。
 * <p>
 * 数据流 ： web/app -> nginx -> 业务服务器(Mysql) -> Maxwell -> Kafka(ODS)-> Kafka(ODS) -> FlinkAPP(BaseLogAPP) -> Kafka(DWD) -> FlinkAPP -> Kafka (DWD) -> FlinkAPP -> clickhouse(DWS)
 * 程序：Mock(模拟产生业务数据) -> Mysql -> Maxwell ->  Kafka(ZK) -> FlinkAPP(DwdTradeOrderPreProcess) -> Kafka(DWD) -> DwdTradeOrderDetail -> Kafka (DWD) -> DwsTradeUserSpuOrderWindow -> clickhouse(DWS)
 * HDFS + ZK + KF + MAXWELL + HBASE + REDIS + CLICKHOUSE
 * DimApp -> DwdTradeOrderPreProcess -> DwdTradeOrderDetail->  DwsTradeUserSpuOrderWindow
 */
public class DwsTradeUserSpuOrderWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // TODO 3. 从 Kafka dwd_trade_order_detail 主题读取下单明细数据
        String topic = "dwd_trade_order_detail";
        String groupId = "dws_trade_trademark_category_user_order_window";

        FlinkKafkaConsumer<String> kafkaConsumer = KafkaUtil.getKafkaConsumer(topic, groupId);
        DataStreamSource<String> dataStreamSource = env.addSource(kafkaConsumer);
        // TODO 4. 过滤字段不完整数据并转换数据结构
        SingleOutputStreamOperator<String> filteredDS = dataStreamSource.filter(
                new FilterFunction<String>() {
                    @Override
                    public boolean filter(String jsonStr) throws Exception {
                        JSONObject jsonObj = JSON.parseObject(jsonStr);
                        String userId = jsonObj.getString("user_id");
                        String sourceTypeName = jsonObj.getString("source_type_name");
                        return userId != null && sourceTypeName != null;
                    }
                }
        );
        SingleOutputStreamOperator<JSONObject> mappedStream = filteredDS.map(JSON::parseObject);

        // TODO 5. 按照 order_detail_id 分组
        KeyedStream<JSONObject, String> keyedDS = mappedStream.keyBy(r -> r.getString("id"));
        // TODO 6. 去重
        SingleOutputStreamOperator<JSONObject> processedStream = keyedDS.process(
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
        SingleOutputStreamOperator<TradeUserSpuOrderBean> tradeUserSpuDS = processedStream.map(x -> {
            HashSet<String> orderIds = new HashSet<>();
            orderIds.add(x.getString("order_id)"));
            return TradeUserSpuOrderBean.builder()
                    .skuId(x.getString("sku_id"))
                    .userId(x.getString("user_id"))
                    .orderAmount(x.getDouble("split_total_amount"))
                    .orderIdSet(orderIds)
                    .ts(DateFormatUtil.toTs(x.getString("create_time"), true))
                    .build();
        });

//        tradeUserSpuDS.print();

        //关联 sku_info 表获取 tm_id，category3_id，spu_id。
        SingleOutputStreamOperator<TradeUserSpuOrderBean> tradeUserSpuDSWithSkuDS = AsyncDataStream.unorderedWait(tradeUserSpuDS,
                new DimAsyncFunction<TradeUserSpuOrderBean>("dim_sku_info".toUpperCase()) {
                    @Override
                    public void join(TradeUserSpuOrderBean tradeUserSpuOrderBean, JSONObject dimInfo) {

                        tradeUserSpuOrderBean.setSpuId(dimInfo.getString("SPU_ID"));
                        tradeUserSpuOrderBean.setTrademarkId(dimInfo.getString("TM_ID"));
                        tradeUserSpuOrderBean.setCategory3Id(dimInfo.getString("CATEGORY3_ID"));
                    }

                    @Override
                    public String getkey(TradeUserSpuOrderBean tradeUserSpuOrderBean) {
                        return tradeUserSpuOrderBean.getSkuId();
                    }
                },
                100, TimeUnit.SECONDS);

//        tradeUserSpuDSWithSkuDS.print("维表关联成功>>>>>>");
        // 提取事件时间生产水位线
        SingleOutputStreamOperator<TradeUserSpuOrderBean> tradeUserSpuWithWmDS = tradeUserSpuDSWithSkuDS.assignTimestampsAndWatermarks(WatermarkStrategy.<TradeUserSpuOrderBean>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner((SerializableTimestampAssigner<TradeUserSpuOrderBean>) (tradeUserSpuOrderBean, l) -> tradeUserSpuOrderBean.getTs())
        );
        // 分组、开窗、聚合
        KeyedStream<TradeUserSpuOrderBean, Tuple4<String, String, String, String>> keyedStream = tradeUserSpuWithWmDS.keyBy(
                new KeySelector<TradeUserSpuOrderBean, Tuple4<String, String, String, String>>() {
                    @Override
                    public Tuple4<String, String, String, String> getKey(TradeUserSpuOrderBean x) throws Exception {
                        return Tuple4.of(
                                x.getUserId(),
                                x.getSpuId(),
                                x.getTrademarkId(),
                                x.getCategory3Id());
                    }
                });

        // 开窗
        SingleOutputStreamOperator<TradeUserSpuOrderBean> reduceDS = keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .reduce(new ReduceFunction<TradeUserSpuOrderBean>() {
                    @Override
                    public TradeUserSpuOrderBean reduce(TradeUserSpuOrderBean x1, TradeUserSpuOrderBean t1) throws Exception {
                        x1.getOrderIdSet().addAll(t1.getOrderIdSet());
                        x1.setOrderAmount(x1.getOrderAmount() + t1.getOrderAmount());
                        return x1;
                    }
                }, new WindowFunction<TradeUserSpuOrderBean, TradeUserSpuOrderBean, Tuple4<String, String, String, String>, TimeWindow>() {
                    @Override
                    public void apply(Tuple4<String, String, String, String> stringStringStringStringTuple4, TimeWindow timeWindow, Iterable<TradeUserSpuOrderBean> iterable, Collector<TradeUserSpuOrderBean> collector) throws Exception {
                        TradeUserSpuOrderBean userSpuOrderBean = iterable.iterator().next();
                        userSpuOrderBean.setTs(System.currentTimeMillis());
                        // 去重
                        userSpuOrderBean.setOrderCount((long) userSpuOrderBean.getOrderIdSet().size());
                        userSpuOrderBean.setStt(DateFormatUtil.toYmdHms(timeWindow.getStart()));
                        userSpuOrderBean.setEdt(DateFormatUtil.toYmdHms(timeWindow.getEnd()));
                        collector.collect(userSpuOrderBean);
                    }
                });


        // 关联spu、tm、category维表补充信息
        SingleOutputStreamOperator<TradeUserSpuOrderBean> SpuDS = AsyncDataStream.unorderedWait(reduceDS,
                new DimAsyncFunction<TradeUserSpuOrderBean>("DIM_SPU_INFO") {
                    @Override
                    public void join(TradeUserSpuOrderBean tradeUserSpuOrderBean, JSONObject dimInfo) {
                        tradeUserSpuOrderBean.setSpuName(dimInfo.getString("SPU_NAME"));
                    }

                    @Override
                    public String getkey(TradeUserSpuOrderBean tradeUserSpuOrderBean) {
                        return tradeUserSpuOrderBean.getSpuId();
                    }
                }, 100, TimeUnit.SECONDS);

        // 关联tm
        SingleOutputStreamOperator<TradeUserSpuOrderBean> tmDS = AsyncDataStream.unorderedWait(SpuDS,
                new DimAsyncFunction<TradeUserSpuOrderBean>("DIM_BASE_TRADEMARK") {
                    @Override
                    public void join(TradeUserSpuOrderBean tradeUserSpuOrderBean, JSONObject dimInfo) {
                        tradeUserSpuOrderBean.setTrademarkName(dimInfo.getString("TM_NAME"));
                    }

                    @Override
                    public String getkey(TradeUserSpuOrderBean tradeUserSpuOrderBean) {
                        return tradeUserSpuOrderBean.getTrademarkId();
                    }
                }, 100, TimeUnit.SECONDS);

        SingleOutputStreamOperator<TradeUserSpuOrderBean> category3DS = AsyncDataStream.unorderedWait(tmDS,
                new DimAsyncFunction<TradeUserSpuOrderBean>("DIM_BASE_CATEGORY3") {
                    @Override
                    public void join(TradeUserSpuOrderBean tradeUserSpuOrderBean, JSONObject dimInfo) {
                        tradeUserSpuOrderBean.setCategory3Name(dimInfo.getString("NAME"));
                        tradeUserSpuOrderBean.setCategory2Id(dimInfo.getString("CATEGORY2_ID"));
                    }

                    @Override
                    public String getkey(TradeUserSpuOrderBean tradeUserSpuOrderBean) {
                        return tradeUserSpuOrderBean.getCategory3Id();
                    }
                }, 100, TimeUnit.SECONDS);


        SingleOutputStreamOperator<TradeUserSpuOrderBean> category2DS = AsyncDataStream.unorderedWait(category3DS,
                new DimAsyncFunction<TradeUserSpuOrderBean>("DIM_BASE_CATEGORY2") {
                    @Override
                    public void join(TradeUserSpuOrderBean tradeUserSpuOrderBean, JSONObject dimInfo) {
                        tradeUserSpuOrderBean.setCategory2Name(dimInfo.getString("NAME"));
                        tradeUserSpuOrderBean.setCategory1Id(dimInfo.getString("CATEGORY1_ID"));
                    }

                    @Override
                    public String getkey(TradeUserSpuOrderBean tradeUserSpuOrderBean) {
                        return tradeUserSpuOrderBean.getCategory2Id();
                    }
                }, 100, TimeUnit.SECONDS);

        SingleOutputStreamOperator<TradeUserSpuOrderBean> category1DS = AsyncDataStream.unorderedWait(category2DS,
                new DimAsyncFunction<TradeUserSpuOrderBean>("DIM_BASE_CATEGORY1") {
                    @Override
                    public void join(TradeUserSpuOrderBean tradeUserSpuOrderBean, JSONObject dimInfo) {
                        tradeUserSpuOrderBean.setCategory1Name(dimInfo.getString("NAME"));
                    }

                    @Override
                    public String getkey(TradeUserSpuOrderBean tradeUserSpuOrderBean) {
                        return tradeUserSpuOrderBean.getCategory1Id();
                    }
                }, 100, TimeUnit.SECONDS);
        // 将数据写入clickhouse
        category1DS.print("result>>>>>>");
        category1DS.addSink(ClickHouseUtil.getJdbcSink("insert into  dws_trade_user_spu_order_window values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"));

        //TODO. 启动
        env.execute("DwsTradeUserSpuOrderWindow");
    }
}
