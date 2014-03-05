package com.elex.bigdata.zergling.etl.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.ResultScanner;

import java.io.IOException;

/**
 * User: Z J Wu Date: 14-1-6 Time: 下午2:20 Package: com.elex.bigdata.historytracer
 */
public class HBaseResourceManager {

  private HTablePool pool;

  public HBaseResourceManager(int poolSize) {
    Configuration conf = HBaseConfiguration.create();
    this.pool = new HTablePool(conf, poolSize);
  }

  public HTableInterface getHTable(String tableName) throws IOException {
    try {
      return pool.getTable(tableName);
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
  public void close() throws IOException {
    this.pool.close();
  }

}
