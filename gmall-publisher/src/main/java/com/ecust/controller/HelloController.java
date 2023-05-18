package com.ecust.controller;

import com.ecust.service.GmvService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.math.BigDecimal;
import java.text.SimpleDateFormat;

@RestController
public class HelloController {
    @Autowired
    private GmvService gmvService;

    @GetMapping("/hello")
    public String testHello() {
        return "hello world";
    }

    @GetMapping("/gmv")
    public String getGmv(@RequestParam(value = "date", defaultValue = "0") int date) {
        //select sum(order_amount) from dws_trade_province_order_window where toYYYYMMDD(stt)=20230518;
        if (date == 0) {
            date = getToday();
        }
        BigDecimal gmv = gmvService.getGmv(date);

        return "{" +
                "            \"status\": 0," +
                "                \"msg\": \"\"," +
                "                \"data\": " + gmv.toPlainString() + "" +
                "        }";
    }

    private int getToday() {
        long ts = System.currentTimeMillis();
        SimpleDateFormat yyyyMMdd = new SimpleDateFormat("yyyyMMdd");
        String format = yyyyMMdd.format(ts);
        return Integer.parseInt(format);
    }

    /**
     * sugar 柱状图json数据
     * {
     *   "status": 0,
     *   "msg": "",
     *   "data": {
     *     "categories": [
     *       "苹果",
     *       "三星",
     *       "华为",
     *       "oppo",
     *       "vivo",
     *       "小米79"
     *     ],
     *     "series": [
     *       {
     *         "name": "手机品牌",
     *         "data": [
     *           9754,
     *           9113,
     *           8955,
     *           5941,
     *           6954,
     *           6879
     *         ]
     *       }
     *     ]
     *   }
     * }
     */


}
