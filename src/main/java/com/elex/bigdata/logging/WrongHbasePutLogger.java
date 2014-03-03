package com.elex.bigdata.logging;

import org.apache.log4j.Logger;

/**
 * User: Z J Wu Date: 14-3-3 Time: 上午11:34 Package: com.elex.bigdata.logging
 */
public class WrongHbasePutLogger extends BaseLogger {
  private static WrongHbasePutLogger instance;

  public WrongHbasePutLogger() {
    this.LOGGER = Logger.getLogger(WrongHbasePutLogger.class);
  }

  public synchronized static WrongHbasePutLogger getInstance() {
    if (instance == null) {
      instance = new WrongHbasePutLogger();
    }
    return instance;
  }

}
