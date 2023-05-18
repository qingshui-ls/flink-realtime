package com.ecust.app.func;


import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;
import com.alibaba.fastjson.JSONObject;
import com.ecust.utils.DimUtil;
import com.ecust.utils.DruidDSUtil;
import com.ecust.utils.ThreadPoolUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import java.util.Collections;
import java.util.concurrent.ThreadPoolExecutor;


public abstract class DimAsyncFunction<T> extends RichAsyncFunction<T, T> implements DimJoinFunction<T>{
    private static DruidDataSource dataSource = null;
    private static ThreadPoolExecutor threadPoolExecutor = null;

    private String tableName;

    public DimAsyncFunction(String tableName) {
        this.tableName = tableName;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        dataSource = DruidDSUtil.createDataSource();
        threadPoolExecutor = ThreadPoolUtil.getThreadPoolExecutor();
    }

    @Override
    public void asyncInvoke(T t, ResultFuture<T> resultFuture) throws Exception {

        threadPoolExecutor.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    // 获取连接
                    DruidPooledConnection connection = dataSource.getConnection();
                    // 查询维表获取维度信息
                    String key = getkey(t);
                    JSONObject dimInfo = DimUtil.getDimInfo(connection, tableName, key);
                    // 将维度信息补充至当前数据
                    if (dimInfo != null) {
                        join(t, dimInfo);
                    }
                    // 归还连接
                    connection.close();
                    // 将结果写出
                    resultFuture.complete(Collections.singletonList(t));
                } catch (Exception e) {
                    e.printStackTrace();
                    System.out.println("关联维表失败:" + t + ",Table:" + tableName);
//                    resultFuture.complete(Collections.singletonList(t));
                }
            }
        });
    }



    @Override
    public void timeout(T input, ResultFuture<T> resultFuture) throws Exception {
        System.out.println("TimeOut:" + input);
    }
}
