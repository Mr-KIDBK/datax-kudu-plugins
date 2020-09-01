package com.q1.datax.plugin.writer.kudu11xwriter;

import com.alibaba.datax.common.spi.ErrorCode;

/**
 * @author daizihao
 * @create 2020-08-27 19:25
 **/
public enum Kudu11xWriterErrorcode implements ErrorCode {
    REQUIRED_VALUE("Kuduwriter-00", "您缺失了必须填写的参数值."),
    ILLEGAL_VALUE("Kuduwriter-01", "您填写的参数值不合法."),
    GET_KUDU_CONNECTION_ERROR("Kuduwriter-02", "获取Kudu连接时出错."),
    GET_KUDU_TABLE_ERROR("Kuduwriter-03", "获取 Kudu  table时出错."),
    CLOSE_KUDU_CONNECTION_ERROR("Kuduwriter-04", "关闭Kudu连接时出错."),
    CLOSE_KUDU_SESSION_ERROR("Kuduwriter-06", "关闭Kudu table时时出错."),
    PUT_KUDU_ERROR("Kuduwriter-07", "写入Kudu时发生IO异常."),
    DELETE_KUDU_ERROR("Kuduwriter-08", "delete Kudu表时发生异常."),
    CONSTRUCT_ROWKEY_ERROR("Kuduwriter-10", "构建rowkey时发生异常."),
    CONSTRUCT_VERSION_ERROR("Kuduwriter-11", "构建version时发生异常."),
    GET_KUDU_BUFFEREDMUTATOR_ERROR("Kuduwriter-12", "获取Kudu BufferedMutator 时出错."),
    CLOSE_KUDU_BUFFEREDMUTATOR_ERROR("Kuduwriter-13", "关闭 Kudu BufferedMutator时出错."),
    GREATE_KUDU_TABLE_ERROR("Kuduwriter-14", "创建 Kudu  table时出错."),
    PARAMETER_NUM_ERROR("Kuduwriter-15","参数个数不匹配")
    ;

    private final String code;
    private final String description;


    private Kudu11xWriterErrorcode(String code, String description) {
        this.code = code;
        this.description = description;
    }
    @Override
    public String getCode() {
        return null;
    }

    @Override
    public String getDescription() {
        return null;
    }
}
