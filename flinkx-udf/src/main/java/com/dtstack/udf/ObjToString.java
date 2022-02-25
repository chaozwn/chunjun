package com.dtstack.udf;

import org.apache.flink.table.functions.ScalarFunction;

import com.alibaba.fastjson.JSON;

/**
 * @author jayce
 * @version 1.0
 * @date 2022/2/15 4:27 下午
 */
public class ObjToString extends ScalarFunction {
    public String eval(Object obj) {
        return JSON.toJSONString(obj);
    }
}
