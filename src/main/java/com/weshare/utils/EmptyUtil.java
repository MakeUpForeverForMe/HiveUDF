package com.weshare.utils;

import java.util.HashMap;
import java.util.Map;

/**
 * @author ximing.wei
 */
public class EmptyUtil {
    private final static int defV = 0;
    private final static int mapV = 1;
    private static Map<String, Integer> map = new HashMap<>();

    static {
        map.put("", mapV);
        map.put("null", mapV);
        map.put("nil", mapV);
        map.put("na", mapV);
        map.put("n/a", mapV);
        map.put("\\n", mapV);
    }

    public static boolean isEmpty(Object object) {
        if (object == null || "".equals(object)) return true;
        if ("String".equals(object.getClass().getSimpleName())) return map.getOrDefault(((String) object).trim().toLowerCase(), defV) == mapV;
        return false;
    }
}
