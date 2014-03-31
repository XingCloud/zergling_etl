package com.elex.bigdata.zergling.etl.model;

/**
 * User: Z J Wu Date: 14-3-27 Time: 下午3:54 Package: com.elex.bigdata.zergling.etl.model
 */
public abstract class BasicNavigatorLog extends GenericLog {

  // Rowkey
  protected String dateString;
  protected String uid;
  protected String url;

  protected long ip;

  protected BasicNavigatorLog(String dateString, String uid, long ip, String url) {
    this.dateString = dateString;
    this.uid = uid;
    this.ip = ip;
    this.url = url;
  }

  public String getDateString() {
    return dateString;
  }

  public void setDateString(String dateString) {
    this.dateString = dateString;
  }

  public String getUid() {
    return uid;
  }

  public void setUid(String uid) {
    this.uid = uid;
  }

  public long getIp() {
    return ip;
  }

  public void setIp(long ip) {
    this.ip = ip;
  }

  public String getUrl() {
    return url;
  }

  public void setUrl(String url) {
    this.url = url;
  }
}
