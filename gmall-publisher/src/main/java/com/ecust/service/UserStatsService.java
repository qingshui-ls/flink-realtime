package com.ecust.service;

import com.ecust.bean.UserChangeCtPerType;
import com.ecust.bean.UserPageCt;
import com.ecust.bean.UserTradeCt;

import java.util.List;

public interface UserStatsService {
    List<UserPageCt> getUvByPage(Integer date);

    List<UserChangeCtPerType> getUserChangeCt(Integer date);

    List<UserTradeCt> getTradeUserCt(Integer date);
}
