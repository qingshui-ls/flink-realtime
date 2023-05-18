package com.ecust.app.func;

import com.alibaba.fastjson.JSONObject;

public interface DimJoinFunction<T> {
    void join(T t, JSONObject dimInfo);

    String getkey(T t);
}
