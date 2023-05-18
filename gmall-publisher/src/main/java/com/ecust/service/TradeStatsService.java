package com.ecust.service;

import com.ecust.bean.TradeProvinceOrderAmount;
import com.ecust.bean.TradeProvinceOrderCt;
import com.ecust.bean.TradeStats;

import java.util.List;

public interface TradeStatsService {
    Double getTotalAmount(Integer date);

    List<TradeStats> getTradeStats(Integer date);

    List<TradeProvinceOrderCt> getTradeProvinceOrderCt(Integer date);

    List<TradeProvinceOrderAmount> getTradeProvinceOrderAmount(Integer date);
}
