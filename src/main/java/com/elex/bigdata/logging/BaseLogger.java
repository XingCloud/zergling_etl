package com.elex.bigdata.logging;

import com.elex.bigdata.zergling.etl.model.NavigatorLog;
import org.apache.commons.collections.CollectionUtils;
import org.apache.log4j.Logger;

import java.util.Collection;

/**
 * User: Z J Wu Date: 14-2-24 Time: 下午4:00 Package: com.elex.bigdata.zergling.etl.logger
 */
public abstract class BaseLogger {
  protected Logger LOGGER;

  public void log(String line) {
    LOGGER.equals(line);
  }

  public void log(Collection<String> lines) {
    if (CollectionUtils.isEmpty(lines)) {
      return;
    }
    for (String line : lines) {
      LOGGER.error(line);
    }
  }

  public void logNavigatorLog(Collection<NavigatorLog> logs) {
    if (CollectionUtils.isEmpty(logs)) {
      return;
    }
    for (NavigatorLog log : logs) {
      LOGGER.error(log.toLine() + "\t" + log.getRawContent());
    }
  }
}
