package com.elex.bigdata.logging;

import static com.elex.bigdata.zergling.etl.ETLConstants.LOG_LINE_SEPERATOR;

import com.elex.bigdata.zergling.etl.model.GenericLog;
import org.apache.commons.collections.CollectionUtils;
import org.apache.log4j.Logger;

import java.util.Collection;

/**
 * User: Z J Wu Date: 14-3-3 Time: 上午11:34 Package: com.elex.bigdata.logging
 */
public class WrongHbasePutLogger {
  private static final Logger LOGGER = Logger.getLogger(WrongHbasePutLogger.class);

  public static void logNavigatorLog(String className, Collection<GenericLog> logs) {
    if (CollectionUtils.isEmpty(logs)) {
      return;
    }
    for (GenericLog log : logs) {
      LOGGER.error(className + LOG_LINE_SEPERATOR + log.toLine());
    }
  }
}
