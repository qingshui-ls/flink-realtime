package com.ecust.app.dws;

import com.ecust.app.func.KeywordUDTF;
import com.ecust.common.GmallConstant;
import com.ecust.utils.KafkaUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import com.ecust.bean.KeywordBean;
import com.ecust.utils.ClickHouseUtil;

/**
 * 1. 获取执行环境
 * 2.使用DDL方式读取kafka page_log主题的数据创建表并且提取时间戳生产WaterMark
 * 3.过滤出搜索数据
 * 4.注册UDTF & 分词
 * 5.分组、开窗、聚合
 * 6.将动态表转换为流
 * 7.将数据写入到Clickhouse
 * 8.启动任务
 * <p>
 * 数据流 ： web/app -> nginx -> 日志服务器(.log) -> Flume -> Kafka(ODS) -> FlinkAPP -> Kafka(DWD) -> FlinkAPP -> ClickHouse
 * 程序：Mock(模拟生产业务数据) -> Flume(f1) -> Kafka + ZK(ODS) -> BaseLogAPP -> Kafka(DWD) -> DwsTrafficSourceKeywordPageViewWindow -> ClickHouse + ZK
 */

public class DwsTrafficSourceKeywordPageViewWindow {
    public static void main(String[] args) throws Exception {
        // 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);// 生产环境设置为kafka的主题分区数
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        String topic = "dwd_traffic_page_log";
        String groupId = "dws_traffic_source_keyword_page_view_window";

        tableEnv.executeSql("create table page_log(\n" +
                "`common` map<string, string>,\n" +
                "`page` map<string, string>,\n" +
                "`ts` bigint,\n" +
                "row_time AS TO_TIMESTAMP(FROM_UNIXTIME(ts/1000, 'yyyy-MM-dd HH:mm:ss')),\n" +
                "WATERMARK FOR row_time AS row_time - INTERVAL '2' SECOND\n" +
                ")" + KafkaUtil.getKafkaDDL(topic, groupId));
        // TODO 4. 从表中过滤搜索行为
        Table searchTable = tableEnv.sqlQuery("select\n" +
                "page['item'] full_word,\n" +
                "row_time\n" +
                "from page_log\n" +
                "where page['item'] is not null\n" +
                "and page['last_page_id'] = 'search'\n" +
                "and page['item_type'] = 'keyword'");
        tableEnv.createTemporaryView("search_table", searchTable);

        //TODO 5.注册UDTF & 分词
        tableEnv.createTemporarySystemFunction("ik_analyze", KeywordUDTF.class);

        // TODO 6. 使用自定义的UDTF函数对搜索的内容进行分词
        Table splitTable = tableEnv.sqlQuery("select\n" +
                "keyword,\n" +
                "row_time \n" +
                "from search_table,\n" +
                "lateral table(ik_analyze(full_word))\n" +
                "as t(keyword)");
        tableEnv.createTemporaryView("split_table", splitTable);

        // TODO 6. 分组、开窗、聚合计算
        Table KeywordBeanSearch = tableEnv.sqlQuery("select\n" +
                "DATE_FORMAT(TUMBLE_START(row_time, INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') stt,\n" +
                "DATE_FORMAT(TUMBLE_END(row_time, INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') edt,\n'" +
                GmallConstant.KEYWORD_SEARCH + "' source,\n" +
                "keyword,\n" +
                "count(*) keyword_count,\n" +
                "UNIX_TIMESTAMP()*1000 ts\n" +
                "from split_table\n" +
                "GROUP BY TUMBLE(row_time, INTERVAL '10' SECOND),keyword");
        // TODO 7. 将动态表转换为流
        DataStream<KeywordBean> keywordBeanDS = tableEnv.toAppendStream(KeywordBeanSearch, KeywordBean.class);

        keywordBeanDS.print();
//        // TODO 8. 将流中的数据写到ClickHouse中
        SinkFunction<KeywordBean> jdbcSink = ClickHouseUtil.<KeywordBean>getJdbcSink(
                "insert into dws_traffic_source_keyword_page_view_window values(?,?,?,?,?,?)");
        keywordBeanDS.addSink(jdbcSink);

        env.execute();

    }
}
