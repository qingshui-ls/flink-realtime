package com.ecust.mapper;

import org.apache.ibatis.annotations.Mapper;

import java.util.List;
import java.util.Map;

@Mapper
public interface UvMapper {

    public List<Map> selectUvByCh(int date);
}
