package com.elex.bigdata.logging;

import org.apache.log4j.Logger;

/**
 * User: Z J Wu Date: 14-2-24 Time: 下午3:59 Package: com.elex.bigdata.zergling.etl.logger
 */
public class WrongURLLogger extends BaseLogger {
  private static final Logger LOGGER = Logger.getLogger(WrongURLLogger.class);

  public static void log(String line) {
    LOGGER.info(line);
  }
}
