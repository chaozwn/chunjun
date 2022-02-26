package com.dtstack.udtf;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
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
                        "ROW<property_id string,property_name string,property_date_type string,property_value string>"))
public class ExplodePropertyAttr extends TableFunction<Row> {
    public void eval(Map<String, String> pr) {
        if (pr == null || pr.isEmpty()) {
            collect(Row.of("", null, null, null));
            return;
        }
        List<String> params =
                pr.keySet().stream()
                        .filter(s -> s.startsWith("$zg_upid#"))
                        .map(s -> s.substring("$zg_upid#".length()))
                        .collect(Collectors.toList());
        params.forEach(
                param -> {
                    String property_name = param.substring(1);
                    String property_value = pr.getOrDefault(param, "");
                    String property_id = pr.getOrDefault("$zg_upid#" + param, "");
                    String property_date_type = pr.getOrDefault("$zg_uptp#" + param, "");
                    collect(Row.of(property_id, property_name, property_date_type, property_value));
                });
    }
}
