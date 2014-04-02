package com.elex.bigdata.zergling.etl;

import static com.elex.bigdata.zergling.etl.ETLUtils.truncateURL;

import com.elex.bigdata.zergling.etl.model.BasicNavigatorLog;
import com.elex.bigdata.zergling.etl.model.LogBatch;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

import java.net.URLDecoder;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * User: Z J Wu Date: 14-3-27 Time: 下午5:52 Package: com.elex.bigdata.zergling.etl
 */
public class UrlShorterWorker<T extends BasicNavigatorLog> implements Runnable {
  private static final Logger LOGGER = Logger.getLogger(UrlShorterWorker.class);
  private InternalQueue<LogBatch<T>> source;
  private InternalQueue<LogBatch<T>> sink;
  private CountDownLatch signal;
  private boolean enabled;

  public UrlShorterWorker(InternalQueue<LogBatch<T>> source, InternalQueue<LogBatch<T>> sink, CountDownLatch signal,
                          boolean enabled) {
    this.source = source;
    this.sink = sink;
    this.signal = signal;
    this.enabled = enabled;
  }

  private void restoreURL(LogBatch<T> batch) {
    List<T> content;
    String originalURL;
    content = batch.getContent();
    if (!enabled) {
      for (BasicNavigatorLog log : content) {
        originalURL = truncateURL(log.getUrl());
        log.setUrl(originalURL);
      }
      return;
    }

    for (BasicNavigatorLog log : content) {
      try {
        originalURL = UrlShortener.getInstance().toOriginalURL(log.getUrl());
      } catch (Exception e) {
        originalURL = "Unknown";
      }
      if (StringUtils.isBlank(originalURL)) {
        originalURL = "Unknown";
      }

      try {
        if (originalURL.startsWith("http://goo.mx")) {
          originalURL = ETLUtils.restoreShortenedURL(originalURL);
        }
        originalURL = URLDecoder.decode(originalURL, "utf8");
      } catch (Exception e) {
        originalURL = "Unknown";
      }

      originalURL = truncateURL(originalURL);
      log.setUrl(originalURL);
    }
  }

  @Override
  public void run() {
    LogBatch<T> batch;
    try {
      while (true) {
        batch = source.take();
        if (batch == null || batch.isEmpty()) {
          continue;
        }
        if (batch.isPill()) {
          break;
        }
        restoreURL(batch);
        sink.put(batch);
      }
    } catch (InterruptedException e) {
      LOGGER.warn(Thread.currentThread().getName() + " is interrupt.");
      Thread.currentThread().interrupt();
    } finally {
      LOGGER.info(Thread.currentThread().getName() + " received a pill and finished restore shortened URL.");
      signal.countDown();
    }
  }
}
