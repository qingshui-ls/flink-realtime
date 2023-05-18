package com.ecust.app.func;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ecust.bean.TableProcess;
import com.ecust.common.GmallConfig;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;


public class TableProcessFunction extends BroadcastProcessFunction<JSONObject, String, JSONObject> {
    private Connection connection;
    private MapStateDescriptor<String, TableProcess> mapStateDescriptor;

    public TableProcessFunction(MapStateDescriptor<String, TableProcess> mapStateDescriptor) {
        this.mapStateDescriptor = mapStateDescriptor;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
    }

    /**
     * 1.获取广播的配置数据
     * 2.过滤字段 filterColumn
     * 3.补充Sink Table字段输出
     */
    @Override
    public void processElement(JSONObject jsonObject, BroadcastProcessFunction<JSONObject, String, JSONObject>.ReadOnlyContext readOnlyContext, Collector<JSONObject> collector) throws Exception {
        ReadOnlyBroadcastState<String, TableProcess> broadcastState = readOnlyContext.getBroadcastState(mapStateDescriptor);
        String table = jsonObject.getString("table");
        TableProcess tableProcess = broadcastState.get(table);

        if (tableProcess != null) {
            // 过滤字段
            filterColum(jsonObject.getJSONObject("data"), tableProcess.getSinkColumns());
            // 补充SinkTable字段并写出到流中
            jsonObject.put("sinkTable", tableProcess.getSinkTable());
            collector.collect(jsonObject);
        }
//        } else {
//            System.out.println("找不到对应的key：" + table);
//        }


    }

    /**
     * 过滤字段
     *
     * @param data        {"id":13,"tm——name"：atguigu,"logo_url":"/bbb/as/"}
     * @param sinkColumns "id","tm_name"
     */
    private void filterColum(JSONObject data, String sinkColumns) {
        Set<Map.Entry<String, Object>> entries = data.entrySet();
        Iterator<Map.Entry<String, Object>> iterator = entries.iterator();
        // 切分sinkColumns
        String[] split = sinkColumns.split(",");
        List<String> columnList = Arrays.asList(split);
        entries.removeIf(x -> !columnList.contains(x.getKey()));
    }

    /**
     * 1.获取并解析数据，方便主流操作
     * 2.检验表是否存在，如果不存在则需要在Phoenix中建表 checkTable
     * 3.写入状态，广播
     */
    @Override
    public void processBroadcastElement(String s, BroadcastProcessFunction<JSONObject, String, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
        JSONObject jsonObject = JSONObject.parseObject(s);
        // 写入状态并广播
        BroadcastState<String, TableProcess> broadcastState = context.getBroadcastState(mapStateDescriptor);
        String op = jsonObject.getString("op");
        if ("d".equals(op)) {
            TableProcess before = jsonObject.getObject("before", TableProcess.class);
            String sourceTable = before.getSourceTable();
            broadcastState.remove(sourceTable);
        } else {
            TableProcess config = jsonObject.getObject("after", TableProcess.class);
            //检验并建表
            checkTable(config.getSinkTable(),
                    config.getSinkColumns(),
                    config.getSinkPk(),
                    config.getSinkExtend());
            broadcastState.put(config.getSourceTable(), config);
        }
    }

    /**
     * create table if not exists db.tn(id varchar primary key, x varchar,c varchar)xxx;
     *
     * @param sinkTable   Phoenix表名
     * @param sinkColumns Phoenix表字段
     * @param sinkPk      Phoenix表主键
     * @param sinkExtend  Phoenix表扩展字段
     */
    private void checkTable(String sinkTable, String sinkColumns, String sinkPk, String sinkExtend) {
        PreparedStatement preparedStatement = null;

        try {
            // 处理特殊字段
            if (sinkPk == null || "".equals(sinkPk)) {
                sinkPk = "id";
            }
            if (sinkExtend == null) {
                sinkExtend = "";
            }
            // 拼接SQL
            StringBuffer createTableSQL = new StringBuffer("create table if not exists ")
                    .append(GmallConfig.HBASE_SCHEMA)
                    .append(".")
                    .append(sinkTable)
                    .append("(");
            String[] columns = sinkColumns.split(",");
            for (int i = 0; i < columns.length; i++) {
                // 取出字段
                String column = columns[i];
                // 判断是否为主键
                if (sinkPk.equals(column)) {
                    createTableSQL.append(column).append(" varchar primary key");
                } else {
                    createTableSQL.append(column).append(" varchar");
                }
                // 判断是否为最后一个字段
                if (i < columns.length - 1) {
                    createTableSQL.append(",");
                }
            }
            createTableSQL.append(")").append(sinkExtend);
            // 编译SQL
            System.out.println("建表语句为：" + createTableSQL);
            preparedStatement = connection.prepareStatement(createTableSQL.toString());
            // 执行SQL
            preparedStatement.execute();

        } catch (SQLException e) {
            throw new RuntimeException("建表失败：" + sinkTable);
        } finally {
            if (preparedStatement != null) {
                // 释放资源
                try {
                    preparedStatement.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    @Override
    public void close() throws Exception {
        connection.close();
    }
}