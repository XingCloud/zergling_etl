package com.elex.bigdata.zergling.etl.hbase;

import org.apache.hadoop.hbase.client.Put;

/**
 * Author: liqiang
 * Date: 14-3-17
 * Time: 下午2:09
 * 用于将一条日志转换为put对象
 */
public interface HBaseBuilder {
    public Put buildPut(String line) throws Exception;
    public void cleanup() throws Exception;
}
