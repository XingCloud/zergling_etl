package com.elex.bigdata.zergling.etl.model;

import org.apache.hadoop.hbase.client.Put;

/**
 * User: Z J Wu Date: 14-3-27 Time: 下午4:30 Package: com.elex.bigdata.zergling.etl.model
 */
public abstract class GenericLog {

  protected String rawLine;

  public abstract Put toPut(int outerVersion, int innerVersion) throws Exception;

  public abstract String toLine();

  public String getRawLine() {
    return rawLine;
  }

  public void setRawLine(String rawLine) {
    this.rawLine = rawLine;
  }
}
