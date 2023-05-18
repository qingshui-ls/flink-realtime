package com.ecust.app.dws;

import com.alibaba.fastjson.JSONObject;
import com.ecust.app.func.DimAsyncFunction;
import com.ecust.bean.TradeTrademarkCategoryUserRefundBean;
import com.ecust.utils.ClickHouseUtil;
import com.ecust.utils.DateFormatUtil;
import com.ecust.utils.KafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;

/**
 * 1）从 Kafka 退单明细主题读取数据
 * 2）转换数据结构
 * JSONObject 转换为实体类 TradeTrademarkCategoryUserRefundBean。
 * 3）补充与分组相关的维度信息
 * 关联 sku_info 表
 * 获取 tm_id，category3_id。
 * 4）设置水位线
 * 5）分组、开窗、聚合
 * 按照维度信息分组，度量字段求和，并在窗口闭合后补充窗口起始时间和结束时间。将时间戳置为当前系统时间。
 * 6）补充与分组无关的维度信息
 * （1）关联 base_trademark 表
 * 获取 tm_name。
 * （2）关联 base_category3 表
 * 获取 name（三级品类名称），获取 category2_id。
 * （3）关联 base_categroy2 表
 * 获取 name（二级品类名称），category1_id。
 * （4）关联 base_category1 表
 * 获取 name（一级品类名称）。
 * 7）写出到 ClickHouse。
 *
 * 数据流 ： web/app -> nginx -> 业务服务器(Mysql) -> Maxwell -> Kafka(ODS)-> Kafka(ODS) -> FlinkAPP -> Kafka(DWD) -> FlinkAPP -> clickhouse(DWS)
 * 程序：Mock(模拟产生业务数据) -> Mysql -> Maxwell ->  Kafka(ZK) -> FlinkAPP(DwdTradeOrderRefund) -> Kafka(DWD) -> DwsTradeTrademarkCategoryUserRefundWindow  -> clickhouse(DWS)
 * HDFS + ZK + KF + MAXWELL + HBASE + REDIS + CLICKHOUSE
 *  DwdTradeOrderRefund + DwsTradeTrademarkCategoryUserRefundWindow
 */
public class DwsTradeTrademarkCategoryUserRefundWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // TODO 3. 从 Kafka dwd_trade_order_refund 主题读取退单明细数据
        String topic = "dwd_trade_order_refund";
        String groupId = "dws_trade_trademark_category_user_refund_window";
        FlinkKafkaConsumer<String> kafkaConsumer = KafkaUtil.getKafkaConsumer(topic, groupId);
        DataStreamSource<String> source = env.addSource(kafkaConsumer);

        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> map = source.map(new MapFunction<String, TradeTrademarkCategoryUserRefundBean>() {
            @Override
            public TradeTrademarkCategoryUserRefundBean map(String s) throws Exception {
                JSONObject jsonObj = JSONObject.parseObject(s);
                String orderId = jsonObj.getString("order_id");
                String userId = jsonObj.getString("user_id");
                String skuId = jsonObj.getString("sku_id");
                Long ts = jsonObj.getLong("ts") * 1000L;

                return TradeTrademarkCategoryUserRefundBean.builder()
                        .orderIdSet(new HashSet<String>(Collections.singleton(orderId)))
                        .userId(userId)
                        .skuId(skuId)
                        .ts(ts)
                        .build();
            }
        });

        // TODO 8. 维度关联，补充与分组相关的维度字段
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> tmDS = AsyncDataStream.unorderedWait(map,
                new DimAsyncFunction<TradeTrademarkCategoryUserRefundBean>("DIM_SKU_INFO") {
                    @Override
                    public void join(TradeTrademarkCategoryUserRefundBean tradeTrademarkCategoryUserRefundBean, JSONObject dimInfo) {
                        tradeTrademarkCategoryUserRefundBean.setTrademarkId(dimInfo.getString("TM_ID"));
                        tradeTrademarkCategoryUserRefundBean.setCategory3Id(dimInfo.getString("CATEGORY3_ID"));
                    }

                    @Override
                    public String getkey(TradeTrademarkCategoryUserRefundBean tradeTrademarkCategoryUserRefundBean) {
                        return tradeTrademarkCategoryUserRefundBean.getSkuId();
                    }
                }, 100, TimeUnit.SECONDS);
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> watermarks = tmDS.assignTimestampsAndWatermarks(WatermarkStrategy.<TradeTrademarkCategoryUserRefundBean>forBoundedOutOfOrderness(Duration.ZERO).withTimestampAssigner(
                new SerializableTimestampAssigner<TradeTrademarkCategoryUserRefundBean>() {
                    @Override
                    public long extractTimestamp(TradeTrademarkCategoryUserRefundBean tradeTrademarkCategoryUserRefundBean, long l) {
                        return tradeTrademarkCategoryUserRefundBean.getTs();
                    }
                }));

        // 分组开窗聚合
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> reduce = watermarks.keyBy(
                        new KeySelector<TradeTrademarkCategoryUserRefundBean, Tuple3<String, String, String>>() {
                            @Override
                            public Tuple3<String, String, String> getKey(TradeTrademarkCategoryUserRefundBean x) throws Exception {
                                return Tuple3.of(x.getUserId(), x.getTrademarkId(), x.getCategory3Id());
                            }
                        }
                ).window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .reduce(new ReduceFunction<TradeTrademarkCategoryUserRefundBean>() {
                            @Override
                            public TradeTrademarkCategoryUserRefundBean reduce(TradeTrademarkCategoryUserRefundBean x, TradeTrademarkCategoryUserRefundBean t1) throws Exception {
                                x.getOrderIdSet().addAll(t1.getOrderIdSet());
                                return x;
                            }
                        },
                        new WindowFunction<TradeTrademarkCategoryUserRefundBean, TradeTrademarkCategoryUserRefundBean, Tuple3<String, String, String>, TimeWindow>() {
                            @Override
                            public void apply(Tuple3<String, String, String> elements, TimeWindow timeWindow, Iterable<TradeTrademarkCategoryUserRefundBean> iterable, Collector<TradeTrademarkCategoryUserRefundBean> collector) throws Exception {
                                String stt = DateFormatUtil.toYmdHms(timeWindow.getStart());
                                String edt = DateFormatUtil.toYmdHms(timeWindow.getEnd());
                                TradeTrademarkCategoryUserRefundBean next = iterable.iterator().next();
                                next.setEdt(edt);
                                next.setStt(stt);
                                next.setRefundCount((long) next.getOrderIdSet().size());
                                next.setTs(System.currentTimeMillis());
                                collector.collect(next);
                            }

                        }
                );
        // TODO 13. 维度关联，补充与分组无关的维度字段

        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> tmNameDS = AsyncDataStream.unorderedWait(reduce,
                new DimAsyncFunction<TradeTrademarkCategoryUserRefundBean>("DIM_BASE_TRADEMARK") {
                    @Override
                    public void join(TradeTrademarkCategoryUserRefundBean tradeTrademarkCategoryUserRefundBean, JSONObject dimInfo) {
                        tradeTrademarkCategoryUserRefundBean.setTrademarkName(dimInfo.getString("TM_NAME"));

                    }

                    @Override
                    public String getkey(TradeTrademarkCategoryUserRefundBean tradeTrademarkCategoryUserRefundBean) {
                        return tradeTrademarkCategoryUserRefundBean.getTrademarkId();
                    }
                }, 100, TimeUnit.SECONDS);
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> category3DS = AsyncDataStream.unorderedWait(tmNameDS,
                new DimAsyncFunction<TradeTrademarkCategoryUserRefundBean>("DIM_BASE_CATEGORY3") {
                    @Override
                    public void join(TradeTrademarkCategoryUserRefundBean tradeTrademarkCategoryUserRefundBean, JSONObject dimInfo) {

                        tradeTrademarkCategoryUserRefundBean.setCategory3Name(dimInfo.getString("NAME"));
                        tradeTrademarkCategoryUserRefundBean.setCategory2Id(dimInfo.getString("CATEGORY3_ID"));
                    }

                    @Override
                    public String getkey(TradeTrademarkCategoryUserRefundBean tradeTrademarkCategoryUserRefundBean) {
                        return tradeTrademarkCategoryUserRefundBean.getCategory3Id();
                    }
                }, 100, TimeUnit.SECONDS);
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> category2DS = AsyncDataStream.unorderedWait(category3DS,
                new DimAsyncFunction<TradeTrademarkCategoryUserRefundBean>("DIM_BASE_CATEGORY2") {
                    @Override
                    public void join(TradeTrademarkCategoryUserRefundBean tradeTrademarkCategoryUserRefundBean, JSONObject dimInfo) {

                        tradeTrademarkCategoryUserRefundBean.setCategory3Name(dimInfo.getString("NAME"));
                        tradeTrademarkCategoryUserRefundBean.setCategory2Id(dimInfo.getString("CATEGORY2_ID"));
                    }

                    @Override
                    public String getkey(TradeTrademarkCategoryUserRefundBean tradeTrademarkCategoryUserRefundBean) {
                        return tradeTrademarkCategoryUserRefundBean.getCategory2Id();
                    }
                }, 100, TimeUnit.SECONDS);
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> category1DS = AsyncDataStream.unorderedWait(category2DS,
                new DimAsyncFunction<TradeTrademarkCategoryUserRefundBean>("DIM_BASE_CATEGORY1") {
                    @Override
                    public void join(TradeTrademarkCategoryUserRefundBean tradeTrademarkCategoryUserRefundBean, JSONObject dimInfo) {
                        tradeTrademarkCategoryUserRefundBean.setCategory3Name(dimInfo.getString("NAME"));
                    }

                    @Override
                    public String getkey(TradeTrademarkCategoryUserRefundBean tradeTrademarkCategoryUserRefundBean) {
                        return tradeTrademarkCategoryUserRefundBean.getCategory1Id();
                    }
                }, 100, TimeUnit.SECONDS);

        // TODO 14. 写出到 OLAP 数据库
        SinkFunction<TradeTrademarkCategoryUserRefundBean> jdbcSink =
                ClickHouseUtil.<TradeTrademarkCategoryUserRefundBean>getJdbcSink(
                        "insert into dws_trade_trademark_category_user_refund_window values(?,?,?,?,?,?,?,?,?,?,?,?,?)"
                );

        category1DS.print(">>>>>>>>");
        category1DS.<TradeTrademarkCategoryUserRefundBean>addSink(jdbcSink);

        env.execute("DwsTradeTrademarkCategoryUserRefundWindow");
    }
}
