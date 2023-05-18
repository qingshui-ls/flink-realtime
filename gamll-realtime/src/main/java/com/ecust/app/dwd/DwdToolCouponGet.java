package com.ecust.app.dwd;

import com.ecust.utils.KafkaUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DwdToolCouponGet {
    public static void main(String[] args) {
        // TODO 1. 环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // TODO 3. 从 Kafka 读取业务数据，封装为 Flink SQL 表
        tableEnv.executeSql("create table `topic_db`(\n" +
                "`database` string,\n" +
                "`table` string,\n" +
                "`data` map<string, string>,\n" +
                "`type` string,\n" +
                "`ts` string\n" +
                ")" + KafkaUtil.getKafkaDDL("topic_db", "dwd_tool_coupon_get"));

        // TODO 4. 读取优惠券领用数据，封装为表
        Table resultTable = tableEnv.sqlQuery("select\n" +
                "data['id'],\n" +
                "data['coupon_id'],\n" +
                "data['user_id'],\n" +
                "date_format(data['get_time'],'yyyy-MM-dd') date_id,\n" +
                "data['get_time'],\n" +
                "ts\n" +
                "from topic_db\n" +
                "where `table` = 'coupon_use'\n" +
                "and `type` = 'insert'\n");
        tableEnv.createTemporaryView("result_table", resultTable);

        // TODO 5. 建立 Kafka-Connector dwd_tool_coupon_get 表
        tableEnv.executeSql("create table dwd_tool_coupon_get (\n" +
                "id string,\n" +
                "coupon_id string,\n" +
                "user_id string,\n" +
                "date_id string,\n" +
                "get_time string,\n" +
                "ts string\n" +
                ")" + KafkaUtil.getKafkaSinkDDL("dwd_tool_coupon_get"));

        // TODO 6. 将数据写入 Kafka-Connector 表
        tableEnv.executeSql("" +
                "insert into dwd_tool_coupon_get select * from result_table");

    }
}
