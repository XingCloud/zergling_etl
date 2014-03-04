package com.elex.bigdata.zergling.etl.hbase;

/**
 * User: Z J Wu Date: 14-3-4 Time: 下午4:12 Package: com.elex.bigdata.zergling.etl.hbase
 */
public class PutterCounter {

  private long val;

  public PutterCounter() {
    this.val = 0;
  }

  public PutterCounter(long val) {
    this.val = val;
  }

  public synchronized void incVal(long v) {
    this.val += v;
  }

  public long getVal() {
    return val;
  }
}
