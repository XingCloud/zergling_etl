package com.elex.bigdata.logging;

import org.apache.log4j.Logger;

/**
 * User: Z J Wu Date: 14-2-24 Time: 下午3:59 Package: com.elex.bigdata.zergling.etl.logger
 */
public class WrongTitleLogger extends BaseLogger {
  private static WrongTitleLogger instance;

  public WrongTitleLogger() {
    this.LOGGER = Logger.getLogger(WrongTitleLogger.class);
  }

  public synchronized static WrongTitleLogger getInstance() {
    if (instance == null) {
      instance = new WrongTitleLogger();
    }
    return instance;
  }
}
