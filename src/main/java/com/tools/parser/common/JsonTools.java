package com.tools.parser.common;

import com.alibaba.fastjson.JSONObject;

public class JsonTools {
    public JsonTools() {
    }
    /**
     * @param key
     *            表示json字符串的头信息
     * @param value
     *            是对解析的集合的类型
     * @return
     */

//将数据转换为Json
    public static String createJsonString(String key, Object value) {

        JSONObject jsonObject = new JSONObject();
        jsonObject.put(key, value);
        return jsonObject.toString();

    }
}
