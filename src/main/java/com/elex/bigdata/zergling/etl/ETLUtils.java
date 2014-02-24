package com.elex.bigdata.zergling.etl;

import static com.elex.bigdata.zergling.etl.ETLConstants.HTTPS_START;
import static com.elex.bigdata.zergling.etl.ETLConstants.HTTPS_START_LENGTH;
import static com.elex.bigdata.zergling.etl.ETLConstants.HTTP_START;
import static com.elex.bigdata.zergling.etl.ETLConstants.HTTP_START_LENGTH;
import static com.elex.bigdata.zergling.etl.ETLConstants.IP_SEPERATOR;
import static com.elex.bigdata.zergling.etl.ETLConstants.URL_END;

import org.apache.commons.lang3.StringUtils;

/**
 * User: Z J Wu Date: 14-2-24 Time: 上午10:52 Package: com.elex.bigdata.zergling.etl
 */
public class ETLUtils {

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

  public static void main(String[] args) {
    System.out.println(ip2Long("187.24.162.236"));
    System.out.println(long2Ip(ip2Long("187.24.162.236")));
  }

}
