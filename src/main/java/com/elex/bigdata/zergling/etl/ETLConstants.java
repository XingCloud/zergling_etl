package com.elex.bigdata.zergling.etl;

import java.text.SimpleDateFormat;
import java.util.TimeZone;

/**
 * User: Z J Wu Date: 14-2-24 Time: 上午10:19 Package: com.elex.bigdata.zergling.etl
 */
public class ETLConstants {
  public static final char LOG_LINE_SEPERATOR = '\001';
  public static final String UNKNOWN_NATION = "NA";
  public static final String NULL_STRING = "null";
  public static final char IP_SEPERATOR = '.';
  public static final String REGEX_IP_ADDRESS = "((?:(?:25[0-5]|2[0-4]\\d|((1\\d{2})|([1-9]?\\d)))\\.){3}(?:25[0-5]|2[0-4]\\d|((1\\d{2})|([1-9]?\\d))))";
  public static final String HTTP_START = "http://";
  public static final int HTTP_START_LENGTH = HTTP_START.length();
  public static final String HTTPS_START = "https://";
  public static final int HTTPS_START_LENGTH = HTTPS_START.length();

  public static final String URL_END = "/";

  public static final SimpleDateFormat STANDARD_OUTPUT_SDF = new SimpleDateFormat("yyyyMMddHHmmss");

  static {
    STANDARD_OUTPUT_SDF.setTimeZone(TimeZone.getTimeZone("GMT+08:00"));
//    STANDARD_OUTPUT_SDF.setTimeZone(TimeZone.getTimeZone("GMT-06:00"));
  }
}
