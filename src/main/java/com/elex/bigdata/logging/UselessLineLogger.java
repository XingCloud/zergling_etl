package com.elex.bigdata.logging;

import org.apache.log4j.Logger;

/**
 * User: Z J Wu Date: 14-2-24 Time: 下午3:59 Package: com.elex.bigdata.zergling.etl.logger
 */
public class UselessLineLogger extends BaseLogger {
  private static UselessLineLogger instance;

  public UselessLineLogger() {
    this.LOGGER = Logger.getLogger(UselessLineLogger.class);
  }

  public synchronized static UselessLineLogger getInstance() {
    if (instance == null) {
      instance = new UselessLineLogger();
    }
    return instance;
  }
}
