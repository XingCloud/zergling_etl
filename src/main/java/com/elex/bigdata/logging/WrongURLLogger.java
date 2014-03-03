package com.elex.bigdata.logging;

import org.apache.log4j.Logger;

/**
 * User: Z J Wu Date: 14-2-24 Time: 下午3:59 Package: com.elex.bigdata.zergling.etl.logger
 */
public class WrongURLLogger extends BaseLogger {
  private static WrongURLLogger instance;

  public WrongURLLogger() {
    this.LOGGER = Logger.getLogger(WrongURLLogger.class);
  }

  public synchronized static WrongURLLogger getInstance() {
    if (instance == null) {
      instance = new WrongURLLogger();
    }
    return instance;
  }
}
