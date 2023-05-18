package com.ecust.app.excer;

import com.ecust.utils.MyKafkaUtil;
import com.ecust.utils.MySQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * 1.获取执行环境
 * 2.创建topic——db表
 * 3.过滤出订单明细数据
 * 4.过滤出订单数据
 * 5.过滤出订单明细活动关联数据
 * 6.过滤出订单明细购物券关联数据
 * 7.创建base_dic LooUp表
 * 8.关联5张表
 * 9.创建 upsert-kafka表
 * 10.将数据写出
 * 11.启动任务
 */
public class DwdTradeOrderPreProcess {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        //TODO 1.创建topic——db表
        tableEnv.executeSql(MyKafkaUtil.getTopicDb("order_pre_process"));
        //TODO 3.过滤出订单明细数据
        Table orderDetailTable = tableEnv.sqlQuery("select  " +
                "    data['id'] id, " +
                "    data['order_id'] order_id, " +
                "    data['sku_id'] sku_id, " +
                "    data['sku_name'] sku_name, " +
                "    data['order_price'] order_price, " +
                "    data['sku_num'] sku_num, " +
                "    data['create_time'] create_time, " +
                "    data['source_type'] source_type, " +
                "    data['source_id'] source_id, " +
                "    data['split_total_amount'] split_total_amount, " +
                "    data['split_activity_amount'] split_activity_amount, " +
                "    data['split_coupon_amount'] split_coupon_amount, " +
                "    pt " +
                "from topic_db " +
                "where `database` = 'gmall-flink' " +
                "and `table` = 'order_detail' " +
                "and `type` = 'insert'");
        tableEnv.createTemporaryView("order_detail_table", orderDetailTable);
/*//         测试打印
        tableEnv.toDataStream(orderDetailTable).print("orderDetailTable);*/
        //TODO 4.过滤出订单数据
        Table orderInfoTable = tableEnv.sqlQuery("select  " +
                "    data['id'] id, " +
                "    data['consignee'] consignee, " +
                "    data['consignee_tel'] consignee_tel, " +
                "    data['total_amount'] total_amount, " +
                "    data['order_status'] order_status, " +
                "    data['user_id'] user_id, " +
                "    data['payment_way'] payment_way, " +
                "    data['delivery_address'] delivery_address, " +
                "    data['order_comment'] order_comment, " +
                "    data['out_trade_no'] out_trade_no, " +
                "    data['trade_body'] trade_body, " +
                "    data['create_time'] create_time, " +
                "    data['operate_time'] operate_time, " +
                "    data['expire_time'] expire_time, " +
                "    data['process_status'] process_status, " +
                "    data['tracking_no'] tracking_no, " +
                "    data['parent_order_id'] parent_order_id, " +
                "    data['province_id'] province_id, " +
                "    data['activity_reduce_amount'] activity_reduce_amount, " +
                "    data['coupon_reduce_amount'] coupon_reduce_amount, " +
                "    data['original_total_amount'] original_total_amount, " +
                "    data['feight_fee'] feight_fee, " +
                "    data['feight_fee_reduce'] feight_fee_reduce, " +
                "    data['refundable_time'] refundable_time, " +
                "    `type`, " +
                "    `old` " +
                "from topic_db " +
                "where `database` = 'gmall-flink' " +
                "and `table` = 'order_info' " +
                "and (`type` = 'insert' or `type` = 'update')");
        tableEnv.createTemporaryView("order_info_table", orderInfoTable);
        /*//         测试打印
        tableEnv.toDataStream(orderInfoTable, Row.class).print("orderInfoTable");*/
        //TODO 5.过滤出订单明细活动关联数据
        Table orderDetailActivityTable = tableEnv.sqlQuery("select  " +
                "    data['id'] id, " +
                "    data['order_id'] order_id, " +
                "    data['order_detail_id'] order_detail_id, " +
                "    data['activity_id'] activity_id, " +
                "    data['activity_rule_id'] activity_rule_id, " +
                "    data['sku_id'] sku_id, " +
                "    data['create_time'] create_time " +
                "from topic_db " +
                "where `database` = 'gmall-flink' " +
                "and `table` = 'order_detail_activity' " +
                "and `type` = 'insert'");
        tableEnv.createTemporaryView("order_activity_table", orderDetailActivityTable);
//        tableEnv.toDataStream(orderDetailActivityTable,Row.class).print("orderDetailActivityTable>>");
        //TODO 6.过滤出订单明细购物券关联数据
        Table orderDetailCouponTable = tableEnv.sqlQuery("select  " +
                "    data['id'] id, " +
                "    data['order_id'] order_id, " +
                "    data['order_detail_id'] order_detail_id, " +
                "    data['coupon_id'] coupon_id, " +
                "    data['coupon_use_id'] coupon_use_id, " +
                "    data['sku_id'] sku_id, " +
                "    data['create_time'] create_time " +
                "from topic_db " +
                "where `database` = 'gmall-flink' " +
                "and `table` = 'order_detail_coupon' " +
                "and `type` = 'insert'");
        tableEnv.createTemporaryView("order_coupon_table", orderDetailCouponTable);
//        tableEnv.toDataStream(orderDetailCouponTable,Row.class).print("orderDetailCouponTable>>");
        //TODO 7.创建base_dic LooUp表 base_dic
        tableEnv.executeSql(MySQLUtil.getBaseDicLookUpDDL());

        //TODO 8.关联5张表
        Table resultTable = tableEnv.sqlQuery("select  " +
                "od.id, " +
                "od.order_id, " +
                "oi.user_id, " +
                "oi.order_status, " +
                "od.sku_id, " +
                "od.sku_name, " +
                "oi.province_id, " +
                "act.activity_id, " +
                "act.activity_rule_id, " +
                "cou.coupon_id, " +
                "date_format(od.create_time, 'yyyy-MM-dd') date_id, " +
                "od.create_time, " +
                "date_format(oi.operate_time, 'yyyy-MM-dd') operate_date_id, " +
                "oi.operate_time, " +
                "od.source_id, " +
                "od.source_type, " +
                "dic.dic_name source_type_name, " +
                "od.sku_num, " +
//                "od.split_original_amount, " +
                "od.split_activity_amount, " +
                "od.split_coupon_amount, " +
                "od.split_total_amount, " +
                "oi.`type`, " +
                "oi.`old`, " +
//                "od.od_ts, " +
//                "oi.oi_ts, " +
                "current_row_timestamp() row_op_ts " +
                "from order_detail_table od  " +
                "join order_info_table oi " +
                "on od.order_id = oi.id " +
                "left join order_activity_table act " +
                "on od.id = act.order_detail_id " +
                "left join order_coupon_table cou " +
                "on od.id = cou.order_detail_id " +
                "join `base_dic` for system_time as of od.pt as dic " +
                "on od.source_type = dic.dic_code");
        tableEnv.createTemporaryView("result_table", resultTable);
        tableEnv.toChangelogStream(resultTable).print("result_table>>");

        env.execute("DwdTradeOrderPreProcess");
    }
}
