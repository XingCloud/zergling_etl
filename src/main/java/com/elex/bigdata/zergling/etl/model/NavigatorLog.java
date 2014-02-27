package com.elex.bigdata.zergling.etl.model;

import static com.elex.bigdata.zergling.etl.ETLConstants.LOG_LINE_SEPERATOR;
import static com.elex.bigdata.zergling.etl.ExceptedContent.ERROR_EMPTY_CONTENT;
import static com.elex.bigdata.zergling.etl.model.HbaseDataType.NUM;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.util.Bytes;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

/**
 * User: Z J Wu Date: 14-2-25 Time: 下午3:17 Package: com.elex.bigdata.zergling.etl
 */
public class NavigatorLog {

  private static final String CF1 = "basis";
  private static final String CF2 = "extend";

  private String dateString;
  private String nation;
  private String uid;

  @BigDataColumn(cf = CF1, q = "title")
  private String title;
  @BigDataColumn(cf = CF1, q = "url")
  private String url;
  @BigDataColumn(cf = CF1, q = "ip", type = NUM)
  private long ip;

  @BigDataColumn(cf = CF2, q = "content")
  private String rawContent;

  public NavigatorLog(String dateString, String uid, String nation, String title, long ip) {
    this.dateString = dateString;
    this.uid = uid;
    this.nation = nation;
    this.title = title;
    this.ip = ip;
  }

  public byte[] getRowkey() {
    return Bytes.toBytes(dateString.concat(nation).concat(uid));
  }

  public List<ColumnInfo> getColumnInfos() throws IllegalAccessException {
    Class<NavigatorLog> clz = NavigatorLog.class;
    Field[] fields = clz.getDeclaredFields();
    BigDataColumn bigDataColumnAnno;
    List<ColumnInfo> columnInfos = new ArrayList<>(3);
    String cf, q, stringVal;
    byte[] byteVal;
    Object fieldVal;
    HbaseDataType type;
    for (Field field : fields) {
      field.setAccessible(true);
      bigDataColumnAnno = field.getAnnotation(BigDataColumn.class);
      if (bigDataColumnAnno == null) {
        continue;
      }
      fieldVal = field.get(this);
      if (fieldVal == null) {
        continue;
      }
      stringVal = fieldVal.toString();
      if (StringUtils.isBlank(stringVal)) {
        continue;
      }

      cf = bigDataColumnAnno.cf();
      q = bigDataColumnAnno.q();
      type = bigDataColumnAnno.type();

      if (NUM.equals(type)) {
        byteVal = Bytes.toBytes(Long.parseLong(stringVal));
      } else {
        byteVal = Bytes.toBytes(stringVal);
      }
      columnInfos.add(new ColumnInfo(cf, q, byteVal));
    }
    return columnInfos;
  }

  public String toLine() {
    StringBuilder sb = new StringBuilder();
    sb.append(dateString);
    sb.append(LOG_LINE_SEPERATOR);
    sb.append(uid);
    sb.append(LOG_LINE_SEPERATOR);
    sb.append(nation);
    sb.append(LOG_LINE_SEPERATOR);
    sb.append(ip);
    sb.append(LOG_LINE_SEPERATOR);
    if (StringUtils.isBlank(title)) {
      sb.append(ERROR_EMPTY_CONTENT.getId());
    } else {
      sb.append(title);
    }
    sb.append(LOG_LINE_SEPERATOR);
    if (StringUtils.isBlank(url)) {
      sb.append(ERROR_EMPTY_CONTENT.getId());
    } else {
      sb.append(url);
    }
    return sb.toString();
  }

  public long getIp() {
    return ip;
  }

  public void setIp(long ip) {
    this.ip = ip;
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

  public String getNation() {
    return nation;
  }

  public void setNation(String nation) {
    this.nation = nation;
  }

  public String getTitle() {
    return title;
  }

  public void setTitle(String title) {
    this.title = title;
  }

  public String getUrl() {
    return url;
  }

  public void setUrl(String url) {
    this.url = url;
  }

  public String getRawContent() {
    return rawContent;
  }

  public void setRawContent(String rawContent) {
    this.rawContent = rawContent;
  }
}
