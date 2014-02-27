package com.elex.bigdata.zergling.etl.model;

import org.apache.hadoop.hbase.util.Bytes;

/**
 * User: Z J Wu Date: 14-2-26 Time: 上午10:27 Package: com.elex.bigdata.zergling.etl.model
 */
public class ColumnInfo {

  private String columnFamily;

  private String qualifier;

  private byte[] byteVal;

  public ColumnInfo(String columnFamily, String qualifier, byte[] byteVal) {
    this.columnFamily = columnFamily;
    this.qualifier = qualifier;
    this.byteVal = byteVal;
  }

  public byte[] getColumnFamilyBytes() {
    return Bytes.toBytes(columnFamily);
  }

  public byte[] getQualifierBytes() {
    return Bytes.toBytes(qualifier);
  }

  public byte[] getValueBytes() {
    return byteVal;
  }
}
