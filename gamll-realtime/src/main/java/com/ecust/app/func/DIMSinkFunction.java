package com.ecust.app.func;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;
import com.alibaba.fastjson.JSONObject;
import com.ecust.utils.DimUtil;
import com.ecust.utils.DruidDSUtil;
import com.ecust.utils.PhoenixUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.SQLException;
// {"sinkTable":"dim_base_trademark","database":"gmall","xid":1959,"data":{"tm_name":"SHANGHAI","id":122},"old":{"tm_name":"shanghai"},"commit":true,"type":"update","table":"base_trademark","ts":1592199920}
// {"sinkTable":"dim_user_info","database":"gmall-flink","data":{"birthday":"1993-01-16","login_name":"c43ohnpj6lq","create_time":"2023-05-16 17:08:21","name":"贺楠","user_level":"1","id":27},"type":"bootstrap-insert","table":"user_info","ts":1592217246}

public class DIMSinkFunction extends RichSinkFunction<JSONObject> {
    private DruidDataSource dataSource;

    @Override
    public void open(Configuration parameters) throws Exception {
        // 创建连接池
        dataSource = DruidDSUtil.createDataSource();
    }


    @Override
    public void invoke(JSONObject jsonObj, Context context) throws Exception {
        // 获取目标表表名
        String sinkTable = jsonObj.getString("sinkTable");
        // 获取操作类型
        String type = jsonObj.getString("type");
        // 获取 id 字段的值
        String id = jsonObj.getString("id");

        // 清除 JSON 对象中的 sinkTable 字段和 type 字段
        // 以便可将该对象直接用于 HBase 表的数据写入
        jsonObj.remove("sinkTable");
        jsonObj.remove("type");

        JSONObject data = jsonObj.getJSONObject("data");
        // 获取连接对象
        DruidPooledConnection conn = dataSource.getConnection();

        try {
            PhoenixUtil.upsertValues(conn, sinkTable, data);
        } catch (Exception e) {
            System.out.println("维度数据写入异常");
            e.printStackTrace();
        } finally {
            try {
                // 归还数据库连接对象
                conn.close();
            } catch (SQLException sqlException) {
                System.out.println("数据库连接对象归还异常");
                sqlException.printStackTrace();
            }
        }

        // 如果操作类型为 update，则清除 redis 中的缓存信息
        if ("update".equals(type)) {
            DimUtil.deleteCached(sinkTable, id);
        }

    }
}
