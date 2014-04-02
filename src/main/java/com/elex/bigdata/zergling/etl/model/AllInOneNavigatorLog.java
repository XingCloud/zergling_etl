package com.elex.bigdata.zergling.etl.model;

import static com.elex.bigdata.zergling.etl.ETLConstants.LOG_LINE_SEPERATOR;

import com.elex.bigdata.zergling.etl.ETLConstants;
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
    if (StringUtils.isBlank(this.projectId) || StringUtils.isBlank(this.nation) || StringUtils
      .isBlank(this.url) || StringUtils.isBlank(this.uid)) {
      return null;
    }
    Byte projectByte = ETLConstants.METRIC_MAPPING.getProjectURLByte(this.projectId);
    if (projectByte == null) {
      throw new Exception("Unknown project(" + this.projectId + ")");
    }
    if (StringUtils.isBlank(this.url)) {
      throw new Exception("URL is necessary.");
    }

    // Make version
    long version = ETLUtils.makeVersion(outerVersion, innerVersion);
    byte[] b = Bytes.toBytes(nation.toLowerCase().concat(dateString.concat(uid)));
    byte[] rowkeyBytes = new byte[b.length + 1];
    rowkeyBytes[0] = projectByte.byteValue();
    System.arraycopy(b, 0, rowkeyBytes, 1, b.length);
    Put put = new Put(rowkeyBytes);

    put.add(CF, Q_IP, version, Bytes.toBytes(ip));
    put.add(CF, Q_URL, version, Bytes.toBytes(url));

    if (StringUtils.isNotBlank(this.userDateString)) {
      put.add(CF, Q_USERDATE_STRING, version, Bytes.toBytes(userDateString));
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
