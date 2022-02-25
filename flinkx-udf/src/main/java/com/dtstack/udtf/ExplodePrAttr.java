package com.dtstack.udtf;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.api.*;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * 用来打散map里面的函数获取数据
 *
 * @author jayce
 * @version 1.0
 * @date 2022/2/15 6:23 下午
 */
@FunctionHint(
        output =
                @DataTypeHint(
                        "ROW<attr_id string,attr_name string,attr_data_type string,attr_value string>"))
public class ExplodePrAttr extends TableFunction<Row> {
    public void eval(Map<String, String> pr) {
        List<String> params =
                pr.keySet().stream()
                        .filter(s -> s.startsWith("$zg_epid#"))
                        .map(s -> s.substring("$zg_epid#".length()))
                        .collect(Collectors.toList());
        params.forEach(
                param -> {
                    String attr_name = param.substring(1);
                    String attr_value = pr.getOrDefault(param, "");
                    String attr_id = pr.getOrDefault("$zg_epid#" + param, "");
                    String attr_data_type = pr.getOrDefault("$zg_eptp#" + param, "");
                    collect(Row.of(attr_id, attr_name, attr_data_type, attr_value));
                });
    }
}
