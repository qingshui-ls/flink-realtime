package com.ecust.service.impl;

import com.ecust.bean.TrafficVisitorStatsPerHour;
import com.ecust.bean.TrafficVisitorTypeStats;
import com.ecust.mapper.TrafficVisitorStatsMapper;
import com.ecust.service.TrafficVisitorStatsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class TrafficVisitorStatsServiceImpl implements TrafficVisitorStatsService {

    @Autowired
    private TrafficVisitorStatsMapper trafficVisitorStatsMapper;

    @Override
    public List<TrafficVisitorTypeStats> getVisitorTypeStats(Integer date) {
        return trafficVisitorStatsMapper.selectVisitorTypeStats(date);
    }

    @Override
    public List<TrafficVisitorStatsPerHour> getVisitorPerHrStats(Integer date) {
        return trafficVisitorStatsMapper.selectVisitorStatsPerHr(date);
    }
}
