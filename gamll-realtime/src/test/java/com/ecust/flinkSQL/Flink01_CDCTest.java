package com.ecust.flinkSQL;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Flink01_CDCTest {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .username("root")
                .password("000000")
                .databaseList("gmall-flink")
                .tableList("gmall-flink.base_trademark")
                .startupOptions(StartupOptions.initial())
                .deserializer(new JsonDebeziumDeserializationSchema())
                .build();
        DataStreamSource<String> mysqlSourceDS = env.fromSource(mySqlSource,
                WatermarkStrategy.noWatermarks(),
                "MysqlSource");

        /**
         *
         * 插入： {"before":null,"after":{"id":14,"tm_name":"a","logo_url":"/sadsd/s"},"source":{"version":"1.5.4.Final","connector":"mysql","
         * name":"mysql_binlog_source","ts_ms":1683622678000,"snapshot":"false","db":"gmall-flink","sequence":null,"table":"base_trademark","server_id":1,"gtid":null,
         * "file":"mysql-bin.000039","pos":368,"row":0,"thread":null,"query":null},"op":"c","ts_ms":1683622678733,"transaction":null}
         * 删除： {"before":{"id":14,"tm_name":"a","logo_url":"/sadsd/s"},"after":null,"source":{"version":"1.5.4.Final","connector":"mysql",
         * "name":"mysql_binlog_source","ts_ms":1683622687000,"snapshot":"false","db":"gmall-flink","sequence":null,"table":"base_trademark","server_id":1,
         * "gtid":null,"file":"mysql-bin.000039","pos":670,
         * "row":0,"thread":null,"query":null},"op":"d","ts_ms":1683622687578,"transaction":null}
         */
        mysqlSourceDS.print(">>>>>>");

        env.execute();
    }

}
