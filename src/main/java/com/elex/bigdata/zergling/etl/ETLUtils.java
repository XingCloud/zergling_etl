package com.elex.bigdata.zergling.etl;

import static com.elex.bigdata.zergling.etl.ETLConstants.HTTPS_START;
import static com.elex.bigdata.zergling.etl.ETLConstants.HTTPS_START_LENGTH;
import static com.elex.bigdata.zergling.etl.ETLConstants.HTTP_START;
import static com.elex.bigdata.zergling.etl.ETLConstants.HTTP_START_LENGTH;
import static com.elex.bigdata.zergling.etl.ETLConstants.IP_SEPERATOR;
import static com.elex.bigdata.zergling.etl.ETLConstants.URL_END;

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
import java.net.URI;
import java.util.Arrays;

/**
 * User: Z J Wu Date: 14-2-24 Time: 上午10:52 Package: com.elex.bigdata.zergling.etl
 */
public class ETLUtils {
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

  public static void main(String[] args) throws Exception {
    for (int i = 0; i < 1000; i++) {
      for (int j = 0; j < 100; j++) {
        long l = makeVersion(i, j);
        System.out.println(l + " - " + Arrays.toString(Bytes.toBytes(l)));
      }
    }

  }

}
