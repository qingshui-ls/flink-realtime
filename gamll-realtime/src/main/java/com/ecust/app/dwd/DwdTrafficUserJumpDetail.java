package com.ecust.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONAware;
import com.alibaba.fastjson.JSONObject;
import com.ecust.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.List;
import java.util.Map;
/**
 * 数据流 ： web/app -> nginx -> 日志服务器(.log) -> Flume -> Kafka(ODS) -> FlinkAPP(BaseLogAPP) -> Kafka(DWD) -> FlinkAPP -> Kafka (DWD)
 * 程序：Mock(模拟产生日志数据，lg.sh) -> Flume(f1.sh) ->  Kafka(ZK)-> FlinkAPP(BaseLogAPP) -> Kafka(DWD) -> DwdTrafficUserJumpDetail -> Kafka (DWD)
 */
public class DwdTrafficUserJumpDetail {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        String topicId = "dwd_traffic_page_log";
        String groupId = "dwd_traffic_user_jump_detail";
        DataStreamSource<String> pageLog = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(topicId, groupId));
        // 测试数据
        /*DataStream<String> kafkaDS = env
            .fromElements(
                "{\"common\":{\"mid\":\"101\"},\"page\":{\"page_id\":\"home\"},\"ts\":10000} ",
                "{\"common\":{\"mid\":\"102\"},\"page\":{\"page_id\":\"home\"},\"ts\":12000}",
                "{\"common\":{\"mid\":\"102\"},\"page\":{\"page_id\":\"good_list\",\"last_page_id\":" +
                    "\"home\"},\"ts\":15000} ",
                "{\"common\":{\"mid\":\"102\"},\"page\":{\"page_id\":\"good_list\",\"last_page_id\":" +
                    "\"detail\"},\"ts\":30000} "
            );*/
        // TODO 4. 转换结构
        SingleOutputStreamOperator<JSONObject> jsonObjDS = pageLog.map(JSON::parseObject);
        // 提取事件事件
        // 按照mid分组
        KeyedStream<JSONObject, String> keyedStream = jsonObjDS
                .assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner((SerializableTimestampAssigner<JSONObject>) (jsonObject, l) -> jsonObject.getLongValue("ts")))
                .keyBy(x -> x.getJSONObject("common").getString("mid"));
        // CEP模式序列
        Pattern<JSONObject, JSONObject> pattern = Pattern.<JSONObject>begin("start").where(
                new SimpleCondition<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject jsonObject) throws Exception {
                        return jsonObject.getJSONObject("page").getString("last_page_id") == null;
                    }
                }
        ).next("next").where(
                new SimpleCondition<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject jsonObject) throws Exception {
                        return jsonObject.getJSONObject("page").getString("last_page_id") == null;
                    }
                }
        ).within(Time.seconds(10));

       /* Pattern<JSONObject, JSONObject> pattern1 = Pattern.<JSONObject>begin("start").where(
                new SimpleCondition<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject jsonObject) throws Exception {
                        return jsonObject.getJSONObject("page").getString("last_page_id") == null;
                    }
                }
        ).times(2).consecutive().within(Time.seconds(10));*/

        // 把 Pattern 应用到流上
        PatternStream<JSONObject> patternStream = CEP.pattern(keyedStream, pattern);
        OutputTag<String> timeoutTag = new OutputTag<String>("timeout") {
        };
        // 提取事件 ，正常输出
        SingleOutputStreamOperator<String> selectDS = patternStream.select(timeoutTag, new PatternTimeoutFunction<JSONObject, String>() {
                    @Override
                    public String timeout(Map<String, List<JSONObject>> map, long l) throws Exception {
                        return map.get("start").get(0).toJSONString();
                    }
                }, new PatternSelectFunction<JSONObject, String>() {
                    @Override
                    public String select(Map<String, List<JSONObject>> map) throws Exception {
                        return map.get("start").get(0).toJSONString();
                    }
                }
        );
        DataStream<String> timeoutDS = selectDS.getSideOutput(timeoutTag);
        // 合并两种事件
        DataStream<String> unionDS = selectDS.union(timeoutDS);


        selectDS.print("Select >>>>>");
        timeoutDS.print("Select >>>>>");
        // 输出到kafka
        String targetTopic = "dwd_traffic_user_jump_detail";
        FlinkKafkaProducer<String> flinkKafkaProducer = MyKafkaUtil.getFlinkKafkaProducer(targetTopic);
        unionDS.addSink(flinkKafkaProducer);

        env.execute("DwdTrafficUserJumpDetail");
    }
}
