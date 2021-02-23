package com.weshare.udf;

import com.weshare.utils.EmptyUtil;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;


/**
 * @author ximing.wei
 */
@Description(
        name = "is_empty",
        value = "_FUNC_(Object object [, String string || Integer int])\n{'',null,'null','nil','na','n/a','\\n'}",
        extended = "" +
                "Example:\n" +
                "  SELECT _FUNC_('null', '') as t;\n" +
                "    null"
)
public class IsEmpty extends UDF {
    public String evaluate(String string) {
        if (EmptyUtil.isEmpty(string)) return null;
        return String.valueOf(string);
    }

    public String evaluate(String string, String defaultValue) {
        if (EmptyUtil.isEmpty(string)) return evaluate(defaultValue);
        return String.valueOf(string);
    }

    public String evaluate(String string, int defaultValue) {
        if (EmptyUtil.isEmpty(string)) return String.valueOf(defaultValue);
        return String.valueOf(string);
    }

    public String evaluate(String string, Integer defaultValue) {
        if (EmptyUtil.isEmpty(string)) return String.valueOf(defaultValue);
        return String.valueOf(string);
    }
}
