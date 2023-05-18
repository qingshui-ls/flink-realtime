package com.ecust.utils;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;
import com.alibaba.fastjson.JSONObject;
import com.ecust.common.GmallConfig;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.lang.StringUtils;


import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.sql.*;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;

public class PhoenixUtil {
    /**
     * 1.拼接SQL
     * 2.预编译
     * 3.执行
     * 4.释放资源
     *
     * @param connection Phoenix 连接
     * @param sinkTable  表名
     * @param data       数据
     */
    public static void upsertValues(DruidPooledConnection connection, String sinkTable, JSONObject data) throws SQLException {
        //1.拼接SQL upsert into db.tn(id,name,sex) values(xxx,xx,xxx);

        Set<String> columns = data.keySet();
        Collection<Object> values = data.values();
        String sql = "upsert into " + GmallConfig.HBASE_SCHEMA + "." + sinkTable + "(" +
                StringUtils.join(columns, ",") + ") values ('" +
                StringUtils.join(values, "','") + "')";
//        System.out.println(sql);
        // 2.预编译SQL
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        preparedStatement.execute();
        connection.commit();
        preparedStatement.close();
    }

    /**
     * Phoenix 表查询方法
     *
     * @param conn 数据库连接对象
     * @param sql  查询数据的 SQL 语句
     * @param clz  返回的集合元素类型的 class 对象
     * @param <T>  返回的集合元素类型
     * @return 封装为 List<T> 的查询结果
     */
    public static <T> List<T> queryList(Connection conn, String sql, Class<T> clz) throws SQLException, InstantiationException, IllegalAccessException, InvocationTargetException {
        List<T> resList = new ArrayList<>();
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            //获取数据库操作对象
            ps = conn.prepareStatement(sql);
            //执行SQL语句
            rs = ps.executeQuery();
            ResultSetMetaData metaData = rs.getMetaData();
            while (rs.next()) {
                //通过反射，创建对象，用于封装查询结果
                T obj = clz.newInstance();
                for (int i = 1; i <= metaData.getColumnCount(); i++) {
                    String columnName = metaData.getColumnName(i);
                    Object columnValue = rs.getObject(i);
                    BeanUtils.setProperty(obj, columnName, columnValue);
                }
                resList.add(obj);
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("从phoenix数据库中查询数据发送异常了~~");
        } finally {
            //释放资源
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (ps != null) {
                try {
                    ps.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
        return resList;
    }

    public static void main(String[] args) throws SQLException, InvocationTargetException, InstantiationException, IllegalAccessException {
        DruidDataSource dataSource = DruidDSUtil.createDataSource();
        DruidPooledConnection connection = dataSource.getConnection();
//        String sql = "select * from GMALL2022_REALTIME.DIM_BASE_CATEGORY2";
//        System.out.println(PhoenixUtil.queryList(connection, sql, JSONObject.class));
        upsertValues(connection, "DIM_BASE_CATEGORY2", new JSONObject());
        connection.close();
    }
}
