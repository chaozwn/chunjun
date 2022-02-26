package com.dtstack.udf;

import org.apache.flink.table.functions.ScalarFunction;

import org.apache.commons.lang3.StringUtils;

import java.sql.Timestamp;

/**
 * @author jayce
 * @version 1.0
 * @date 2022/2/15 5:59 下午
 */
public class StringToTimestamp extends ScalarFunction {
    public Timestamp eval(String timestamp) {
        if (StringUtils.isBlank(timestamp)) {
            return null;
        }
        return new Timestamp(Long.parseLong(timestamp));
    }
}
