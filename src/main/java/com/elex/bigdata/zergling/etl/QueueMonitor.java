package com.elex.bigdata.zergling.etl;

import org.apache.log4j.Logger;

import java.util.concurrent.TimeUnit;

/**
 * User: Z J Wu Date: 14-3-28 Time: 下午6:30 Package: com.elex.bigdata.zergling.etl
 */
public class QueueMonitor implements Runnable {
  private static final Logger LOGGER = Logger.getLogger(QueueMonitor.class);
  private volatile boolean run = true;
  private InternalQueue urlRestoreQueue;
  private InternalQueue logStoreQueue;

  public QueueMonitor(InternalQueue urlRestoreQueue, InternalQueue logStoreQueue) {
    this.urlRestoreQueue = urlRestoreQueue;
    this.logStoreQueue = logStoreQueue;
  }

  public void stop() {
    this.run = false;
  }

  @Override
  public void run() {
    while (run) {
      LOGGER.info("Heartbeat(UrlRestoreQueueSize=" + urlRestoreQueue.size() + ", LogStoreQueueSize=" + logStoreQueue
        .size() + ")");
      try {
        TimeUnit.SECONDS.sleep(1);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }
}
