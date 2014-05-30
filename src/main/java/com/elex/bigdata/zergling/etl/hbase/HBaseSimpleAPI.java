package com.elex.bigdata.zergling.etl.hbase;

import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;

import java.util.List;

/**
 * Author: liqiang
 * Date: 14-5-30
 * Time: 下午3:31
 */
public class HBaseSimpleAPI {

    public static void put(String tableName,List<Put> puts) throws Exception{
        HTableInterface hTable = null;
        try {
            hTable = HBaseResourceManager.getHTable(tableName);
            hTable.put(puts);
        } finally {
            HBaseResourceManager.closeHTable(hTable);
        }
    }

}
