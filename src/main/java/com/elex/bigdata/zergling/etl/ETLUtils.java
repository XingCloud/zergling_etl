package com.elex.bigdata.zergling.etl;

import static com.elex.bigdata.zergling.etl.ETLConstants.HTTPS_START;
import static com.elex.bigdata.zergling.etl.ETLConstants.HTTPS_START_LENGTH;
import static com.elex.bigdata.zergling.etl.ETLConstants.HTTP_START;
import static com.elex.bigdata.zergling.etl.ETLConstants.HTTP_START_LENGTH;
import static com.elex.bigdata.zergling.etl.ETLConstants.IP_SEPERATOR;
import static com.elex.bigdata.zergling.etl.ETLConstants.URL_END;
import static com.elex.bigdata.zergling.etl.model.HbaseDataType.NUM;

import com.elex.bigdata.zergling.etl.model.BigDataColumn;
import com.elex.bigdata.zergling.etl.model.ColumnInfo;
import com.elex.bigdata.zergling.etl.model.HbaseDataType;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.HttpClients;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Field;
import java.net.URI;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

/**
 * User: Z J Wu Date: 14-2-24 Time: 上午10:52 Package: com.elex.bigdata.zergling.etl
 */
public class ETLUtils {
  public static final Map<String, SimpleDateFormat> SDF_MAP = new HashMap<>();

  static {
    SimpleDateFormat brSDF = new SimpleDateFormat("yyyyMMddHHmmss");
    brSDF.setTimeZone(TimeZone.getTimeZone("-06:00"));
    SimpleDateFormat frSDF = new SimpleDateFormat("yyyyMMddHHmmss");
    frSDF.setTimeZone(TimeZone.getTimeZone("+01:00"));
    SimpleDateFormat bjSDF = new SimpleDateFormat("yyyyMMddHHmmss");
    frSDF.setTimeZone(TimeZone.getTimeZone("+08:00"));
    SDF_MAP.put("br", brSDF);
    SDF_MAP.put("fr", frSDF);
    SDF_MAP.put("bj", bjSDF);
  }

  private static final String HTTP_SCHEMA = "http";
  private static final String HOST = "restore.url";
  private static final int PORT = 8080;
  private static final String PATH = "/ur/r";
  private static final HttpClient CLIENT = HttpClients.createDefault();

  public static String truncateURL(String url) {
    if (StringUtils.isBlank(url)) {
      return null;
    }
    int from = 0, to = url.length();
    if (url.startsWith(HTTP_START)) {
      from = HTTP_START_LENGTH;
    } else if (url.startsWith(HTTPS_START)) {
      from = HTTPS_START_LENGTH;
    }
    if (url.endsWith(URL_END)) {
      --to;
    }
    return url.substring(from, to);
  }

  public static String long2Ip(long ip) {
    StringBuilder sb = new StringBuilder(15);

    for (int i = 0; i < 4; i++) {
      sb.insert(0, Long.toString(ip & 0xff));
      if (i < 3) {
        sb.insert(0, '.');
      }
      ip >>= 8;
    }

    return sb.toString();
  }

  public static long ip2Long(String ip) {
    int a = 0, b = ip.indexOf(IP_SEPERATOR);
    long i1 = Long.valueOf(ip.substring(a, b)) << 24;
    a = b + 1;
    b = ip.indexOf(IP_SEPERATOR, a);
    long i2 = Long.valueOf(ip.substring(a, b)) << 16;
    a = b + 1;
    b = ip.indexOf(IP_SEPERATOR, a);
    long i3 = Long.valueOf(ip.substring(a, b)) << 8;
    a = b + 1;
    long i4 = Long.valueOf(ip.substring(a));
    return i1 + i2 + i3 + i4;
  }

  private static String readResponse(HttpEntity responseEntity) throws IOException {
    if (responseEntity == null) {
      return null;
    }
    StringBuilder responseString = new StringBuilder();
    String line;
    try (BufferedReader reader = new BufferedReader(new InputStreamReader(responseEntity.getContent()));) {
      while ((line = reader.readLine()) != null) {
        line = StringUtils.trimToNull(line);
        if (StringUtils.isNotEmpty(line)) {
          responseString.append(line);
        }
      }
    }
    return responseString.toString();
  }

  public static String restoreShortenedURL(String shortURL) throws Exception {
    URIBuilder builder = new URIBuilder();
    builder.setScheme(HTTP_SCHEMA);
    builder.setHost(HOST);
    builder.setPort(PORT);
    builder.setPath(PATH);
    builder.addParameter("url", shortURL);

    URI uri = builder.build();
    HttpGet httpGet = new HttpGet(uri);
    HttpResponse response = CLIENT.execute(httpGet);
    StatusLine statusLine = response.getStatusLine();
    int c = statusLine.getStatusCode();
    String result;
    if (200 == c) {
      result = StringUtils.trimToNull(readResponse(response.getEntity()));
      if (StringUtils.isBlank(result) || "err".equals(result)) {
        throw new Exception("Invalid url result - " + shortURL);
      } else {
        return result;
      }
    } else {
      throw new Exception("Error return code: " + String.valueOf(c));
    }
  }

  public static long makeVersion(int outerVersion, int innerVersion) {
    byte[] longBytes = new byte[8];
    byte[] outerBytes = Bytes.toBytes(outerVersion);
    byte[] innerBytes = Bytes.toBytes(innerVersion);
    System.arraycopy(outerBytes, 0, longBytes, 0, outerBytes.length);
    System.arraycopy(innerBytes, 0, longBytes, 4, innerBytes.length);
    return Bytes.toLong(longBytes);
  }

  public static List<ColumnInfo> getColumnInfos(Object log) throws IllegalAccessException {
    Field[] fields = log.getClass().getDeclaredFields();
    BigDataColumn bigDataColumnAnno;
    List<ColumnInfo> columnInfos = new ArrayList<>();
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
      fieldVal = field.get(log);
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

  public static String current5Min() throws ParseException {
    DecimalFormat df = new DecimalFormat("000");
    Date d = new Date();
    SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
    Date d2 = sdf.parse(sdf.format(d));
    return df.format((d.getTime() - d2.getTime()) / 300000);
  }

}
