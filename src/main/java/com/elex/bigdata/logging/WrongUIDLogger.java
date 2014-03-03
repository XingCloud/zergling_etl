package com.elex.bigdata.logging;

import org.apache.log4j.Logger;

/**
 * User: Z J Wu Date: 14-2-24 Time: 下午3:59 Package: com.elex.bigdata.zergling.etl.logger
 */
public class WrongUIDLogger extends BaseLogger {
  private static WrongUIDLogger instance;

  public WrongUIDLogger() {
    this.LOGGER = Logger.getLogger(WrongUIDLogger.class);
  }

  public synchronized static WrongUIDLogger getInstance() {
    if (instance == null) {
      instance = new WrongUIDLogger();
    }
    return instance;
  }

}
