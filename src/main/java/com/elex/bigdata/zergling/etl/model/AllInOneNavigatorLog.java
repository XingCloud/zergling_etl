package com.elex.bigdata.zergling.etl.model;

import static com.elex.bigdata.zergling.etl.ETLConstants.LOG_LINE_SEPERATOR;

import com.elex.bigdata.zergling.etl.ETLUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * User: Z J Wu Date: 14-3-27 Time: 下午4:10 Package: com.elex.bigdata.zergling.etl.model
 */
public class AllInOneNavigatorLog extends BasicNavigatorLog {

  private static final byte[] CF = Bytes.toBytes("basis");

  private static final byte[] Q_IP = Bytes.toBytes("ip");
  private static final byte[] Q_URL = Bytes.toBytes("url");
  private static final byte[] Q_PROJECT_ID = Bytes.toBytes("project_id");
  private static final byte[] Q_NATION = Bytes.toBytes("nation");
  private static final byte[] Q_USERDATE_STRING = Bytes.toBytes("user_date");

  private String projectId;
  private String nation;
  private String userDateString;

  public AllInOneNavigatorLog(String dateString, String uid, long ip, String url, String projectId, String nation,
                              String userDateString) {
    super(dateString, uid, ip, url);
    this.projectId = projectId;
    this.nation = nation;
    this.userDateString = userDateString;
  }

  @Override
  public Put toPut(int outerVersion, int innerVersion) throws Exception {
    long version = ETLUtils.makeVersion(outerVersion, innerVersion);
    byte[] rowkeyBytes = Bytes.toBytes(dateString.concat(uid));
    Put put = new Put(rowkeyBytes);
    if (StringUtils.isBlank(this.url)) {
      throw new Exception("URL is necessary.");
    }
    put.add(CF, Q_URL, version, Bytes.toBytes(url));

    if (StringUtils.isNotBlank(this.projectId)) {
      put.add(CF, Q_PROJECT_ID, version, Bytes.toBytes(projectId));
    }
    if (StringUtils.isNotBlank(this.nation)) {
      put.add(CF, Q_NATION, version, Bytes.toBytes(nation));
    }
    if (StringUtils.isNotBlank(this.userDateString)) {
      put.add(CF, Q_USERDATE_STRING, version, Bytes.toBytes(userDateString));
    }
    if (this.ip > 0) {
      put.add(CF, Q_IP, version, Bytes.toBytes(ip));
    }
    return put;
  }

  @Override
  public String toLine() {
    StringBuilder sb = new StringBuilder();
    sb.append(dateString);
    sb.append(LOG_LINE_SEPERATOR);
    sb.append(uid);
    sb.append(LOG_LINE_SEPERATOR);
    sb.append(url);
    sb.append(LOG_LINE_SEPERATOR);
    sb.append(projectId);
    sb.append(LOG_LINE_SEPERATOR);
    sb.append(nation);
    sb.append(LOG_LINE_SEPERATOR);
    sb.append(userDateString);
    return sb.toString();
  }

  public String getProjectId() {
    return projectId;
  }

  public void setProjectId(String projectId) {
    this.projectId = projectId;
  }

  public String getNation() {
    return nation;
  }

  public void setNation(String nation) {
    this.nation = nation;
  }

  public String getUserDateString() {
    return userDateString;
  }

  public void setUserDateString(String userDateString) {
    this.userDateString = userDateString;
  }
}
