package com.ecust.app.dwd;

import com.ecust.utils.KafkaUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DwdInteractionFavorAdd {
    public static void main(String[] args) throws Exception {

        // TODO 1. 环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);


        // TODO 3. 从 Kafka 读取业务数据，封装为 Flink SQL 表
        tableEnv.executeSql("create table topic_db(" +
                "`database` string,\n" +
                "`table` string,\n" +
                "`type` string,\n" +
                "`data` map<string, string>,\n" +
                "`ts` string\n" +
                ")" + KafkaUtil.getKafkaDDL("topic_db", "dwd_interaction_favor_add"));

        // TODO 4. 读取收藏表数据
        Table favorInfo = tableEnv.sqlQuery("select\n" +
                "data['id'] id,\n" +
                "data['user_id'] user_id,\n" +
                "data['sku_id'] sku_id,\n" +
                "date_format(data['create_time'],'yyyy-MM-dd') date_id,\n" +
                "data['create_time'] create_time,\n" +
                "ts\n" +
                "from topic_db\n" +
                "where `table` = 'favor_info'\n" +
                "and `type` = 'insert'\n");
        tableEnv.createTemporaryView("favor_info", favorInfo);

        // TODO 5. 创建 Kafka-Connector dwd_interaction_favor_add 表
        tableEnv.executeSql("create table dwd_interaction_favor_add (\n" +
                "id string,\n" +
                "user_id string,\n" +
                "sku_id string,\n" +
                "date_id string,\n" +
                "create_time string,\n" +
                "ts string\n" +
                ")" + KafkaUtil.getKafkaSinkDDL("dwd_interaction_favor_add"));

        // TODO 6. 将数据写入 Kafka-Connector 表
        tableEnv.executeSql("" +
                "insert into dwd_interaction_favor_add select * from favor_info");
    }

}
