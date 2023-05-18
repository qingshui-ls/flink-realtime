package com.ecust.service.impl;

import com.ecust.bean.CategoryCommodityStats;
import com.ecust.bean.SpuCommodityStats;
import com.ecust.bean.TrademarkCommodityStats;
import com.ecust.bean.TrademarkOrderAmountPieGraph;
import com.ecust.mapper.CommodityStatsMapper;
import com.ecust.service.CommodityStatsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class CommodityStatsServiceImpl implements CommodityStatsService {

    @Autowired
    private CommodityStatsMapper commodityStatsMapper;

    @Override
    public List<TrademarkCommodityStats> getTrademarkCommodityStatsService(Integer date) {
        return commodityStatsMapper.selectTrademarkStats(date);
    }

    @Override
    public List<TrademarkOrderAmountPieGraph> getTmOrderAmtPieGra(Integer date) {
        return commodityStatsMapper.selectTmOrderAmtPieGra(date);
    }

    @Override
    public Map getGmvByTm(int date, int limit) {
        //查询数据
        List<Map> mapList = commodityStatsMapper.selectGmvByTm(date, limit);

        //创建Map存放所需要的结果数据
        HashMap<String, BigDecimal> resultMap = new HashMap<>();

        //遍历集合取出数据放入Map中
        for (Map map : mapList) {
            resultMap.put((String) map.get("trademark_name"), (BigDecimal) map.get("order_amount"));
        }

        //返回结果
        return resultMap;
    }

    @Override
    public List<CategoryCommodityStats> getCategoryStatsService(Integer date) {
        return commodityStatsMapper.selectCategoryStats(date);
    }

    @Override
    public List<SpuCommodityStats> getSpuCommodityStats(Integer date) {
        return commodityStatsMapper.selectSpuStats(date);
    }
}
