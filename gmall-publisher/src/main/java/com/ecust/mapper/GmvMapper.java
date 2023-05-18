package com.ecust.mapper;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Select;

import java.math.BigDecimal;
@Mapper
public interface GmvMapper {
//    @Select("select sum(order_amount) from dws_trade_province_order_window where toYYYYMMDD(stt)=#{date};")
    @Select("select sum(order_amount) from dws_trade_province_order_window;")
    public BigDecimal selectGmv(int date);
}
