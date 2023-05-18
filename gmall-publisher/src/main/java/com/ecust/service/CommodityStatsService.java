package com.ecust.service;

import com.ecust.bean.CategoryCommodityStats;
import com.ecust.bean.SpuCommodityStats;
import com.ecust.bean.TrademarkCommodityStats;
import com.ecust.bean.TrademarkOrderAmountPieGraph;

import java.util.List;
import java.util.Map;

public interface CommodityStatsService {
    List<TrademarkCommodityStats> getTrademarkCommodityStatsService(Integer date);

    List<CategoryCommodityStats> getCategoryStatsService(Integer date);

    List<SpuCommodityStats> getSpuCommodityStats(Integer date);

    List<TrademarkOrderAmountPieGraph> getTmOrderAmtPieGra(Integer date);

    Map getGmvByTm(int date, int limit);
}
