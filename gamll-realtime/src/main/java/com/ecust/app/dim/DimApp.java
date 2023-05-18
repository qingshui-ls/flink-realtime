package com.ecust.app.dim;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ecust.app.func.DIMSinkFunction;
import com.ecust.app.func.MyBroadcastFunction;
import com.ecust.app.func.TableProcessFunction;
import com.ecust.bean.TableProcess;
import com.ecust.utils.MyKafkaUtil;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 数据流 ： web/app -> nginx -> 业务服务器 -> Mysql(binlog) -> MaxWell -> Kafka(ODS) -> FlinkAPP -> Phoenix
 * 程序：Mock(模拟生产业务数据) -> Mysql(binlog) -> MaxWell -> Kafka + ZK(ODS) -> FlinkAPP -> Phoenix(HBase + zk + HDFS)
 */
public class DimApp {
    public static void main(String[] args) throws Exception {
        // 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);// 生产环境设置为kafka的主题分区数

        // 1.1 开启checkPoint
       /* env.enableCheckpointing(5 * 6000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(10 * 6000L);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 5000L));
        // 1.2设置状态后端
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020//211126/ck");
        System.setProperty("HADOOP_USER_NAME", "ls");*/

        // 2.读取kafka topic——db主题数据创建主流
        String topic = "topic_db";
        String groupId = "dim_app_211126";
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(topic, groupId));
        // 3.过滤掉非JSON数据&保留新增、变化以及初始化数据
        SingleOutputStreamOperator<JSONObject> filteredDS = kafkaDS.flatMap(new FlatMapFunction<String, JSONObject>() {

            @Override
            public void flatMap(String s, Collector<JSONObject> collector) throws Exception {
                try {
                    // 将数据转换为JSON格式
                    JSONObject jsonObject = JSON.parseObject(s);
                    // 获取数据中的操作类型字段
                    String type = jsonObject.getString("type");
                    // 保留新增、变化、以及初始化类型数据
//                    bootstrap-insert
                    if ("insert".equals(type) || "update".equals(type) || "bootstrap-insert".equals(type)) {
                        collector.collect(jsonObject);
                    }
                } catch (Exception e) {
                    System.out.println("发现脏数据" + s);
                }
            }
        });

        // 4.使用FlinkCDC 读取mysql配置信息表创建配置流
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .username("root")
                .password("000000")
                .databaseList("gmall-config")
                .tableList("gmall-config.table_process")
                .startupOptions(StartupOptions.initial())
                .deserializer(new JsonDebeziumDeserializationSchema())
                .build();
        DataStreamSource<String> mysqlSourceDS = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MysqlSource");

        // 5.将配置流处理为广播流
        MapStateDescriptor<String, TableProcess> mapStateDescriptor = new MapStateDescriptor<>("table-process-state", String.class, TableProcess.class);
        BroadcastStream<String> broadcastStream = mysqlSourceDS.broadcast(mapStateDescriptor);
        // 6.连接主流和广播流
        BroadcastConnectedStream<JSONObject, String> connectedDS = filteredDS.connect(broadcastStream);
        // 7.处理连接流，根据配置信息处理主流数据
        SingleOutputStreamOperator<JSONObject> dimDS = connectedDS.process(new TableProcessFunction(mapStateDescriptor));
        // 8.将数据写出到Phoenix
//        dimDS.print(">>>");
        dimDS.addSink(new DIMSinkFunction());
        // 9.启动任务
        env.execute("DimAPP");
    }
}
