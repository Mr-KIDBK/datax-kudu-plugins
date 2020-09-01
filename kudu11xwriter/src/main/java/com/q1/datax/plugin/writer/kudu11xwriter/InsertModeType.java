package com.q1.datax.plugin.writer.kudu11xwriter;

import com.alibaba.datax.common.exception.DataXException;

import java.util.Arrays;

/**
 * @author daizihao
 * @create 2020-08-31 14:47
 **/
public enum InsertModeType {
    Insert("insert"),
    Upsert("upsert"),
    Update("update");
    private String mode;

    InsertModeType(String mode) {
        this.mode = mode.toLowerCase();
    }

    public String getMode() {
        return mode;
    }

    public static InsertModeType getByTypeName(String modeName) {
        for (InsertModeType modeType : values()) {
            if (modeType.mode.equalsIgnoreCase(modeName)) {
                return modeType;
            }
        }
        throw DataXException.asDataXException(Kudu11xWriterErrorcode.ILLEGAL_VALUE,
                String.format("Kuduwriter 不支持该 mode 类型:%s, 目前支持的 mode 类型是:%s", modeName, Arrays.asList(values())));
    }
}
