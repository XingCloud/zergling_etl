package com.elex.bigdata.zergling.etl.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;

/**
 * User: Z J Wu Date: 14-1-6 Time: 下午2:20 Package: com.elex.bigdata.historytracer
 */
public class HBaseResourceManager {

    private static HConnection conn = null;

    public static void init() throws IOException{
        try {
            conn = HConnectionManager.createConnection(HBaseConfiguration.create());
        } catch (Exception e) {
            throw new IOException("Cannot create hbase connection.", e);
        }
    }

    public static synchronized HTableInterface getHTable(String tableName) throws IOException {
        try {
            if(conn == null){
                init();
            }
            return conn.getTable(tableName);
        } catch (Exception e) {
            throw new IOException("Cannot get htable from hbase(" + tableName + ").", e);
        }
    }

    // This is a return operation.
    public static void closeHTable(HTableInterface hTableInterface) {
        if (hTableInterface == null) {
            return;
        }
        try {
            hTableInterface.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // This is a return operation.
    public static void closeResultScanner(ResultScanner resultScanner) {
        if (resultScanner == null) {
            return;
        }
        resultScanner.close();
    }

    // This is a shutdown operation.
    public static void close() throws IOException {
        if(conn != null){
            conn.close();
        }
    }

}
