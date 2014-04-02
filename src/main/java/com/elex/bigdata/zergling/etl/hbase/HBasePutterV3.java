package com.elex.bigdata.zergling.etl.hbase;

import com.elex.bigdata.zergling.etl.InternalQueue;
import com.elex.bigdata.zergling.etl.model.GenericLog;
import com.elex.bigdata.zergling.etl.model.LogBatch;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Vector;
import java.util.concurrent.CountDownLatch;

/**
 * User: Z J Wu Date: 14-2-26 Time: 上午9:48 Package: com.elex.bigdata.zergling.etl.hbase
 */
public class HBasePutterV3<T extends GenericLog> implements Runnable {

  private static final Logger LOGGER = Logger.getLogger(HBasePutterV3.class);

  private InternalQueue<LogBatch<T>> queue;

  private CountDownLatch signal;

  private HTableInterface hTable;

  private boolean enableHbasePut;

  private PutterCounter success;

  private Vector<LogBatch<T>> failedBatches;

  public HBasePutterV3(InternalQueue<LogBatch<T>> queue, CountDownLatch signal, HTableInterface hTable,
                       boolean enableHbasePut, PutterCounter success, Vector<LogBatch<T>> failedBatches) {
    this.queue = queue;
    this.signal = signal;
    this.hTable = hTable;
    this.enableHbasePut = enableHbasePut;
    this.success = success;
    this.failedBatches = failedBatches;
  }

  private String purePut(List<Put> puts) {
    try {
      hTable.put(puts);
      return null;
    } catch (Exception e) {
      return e.getClass().getName();
    }
  }

  @Override
  public void run() {
    LogBatch<T> batch;
    Put put;
    List<T> content;
    List<Put> puts;

    try {
      while (true) {
        batch = queue.take();
        if (batch == null || batch.isEmpty()) {
          continue;
        }
        if (batch.isPill()) {
          break;
        }
        content = batch.getContent();
        int outerVersion = batch.getVersion();
        puts = new ArrayList<>(content.size());
        int innerVersion = 0;
        for (GenericLog log : content) {
          try {
            put = log.toPut(outerVersion, innerVersion);
          } catch (Exception e) {
            LOGGER.warn(Thread.currentThread().getName() + " Put is not valid, ignore this log(" + log.toLine() + ")");
            continue;
          }
          puts.add(put);
          ++innerVersion;
        }

        long t1 = System.currentTimeMillis();
        String returnResult;
        if (enableHbasePut) {
          returnResult = purePut(puts);
        } else {
          for (GenericLog log : content) {
            LOGGER.info("[LINE] - " + log.toLine());
          }
          returnResult = null;
        }
        boolean ok = StringUtils.isBlank(returnResult);
        long t2 = System.currentTimeMillis();
        long thisRoundTime = t2 - t1;
        if (ok) {
          success.incVal(innerVersion);
          if (thisRoundTime > 50) {
            LOGGER.info(
              "Put hbase ok but too slow, " + Thread.currentThread().getName() + " used " + thisRoundTime + " millis.");
          }
        } else {
          LOGGER.info("Put hbase error(" + Thread.currentThread().getName() + " in " + thisRoundTime + " millis.");
          failedBatches.add(batch);
        }
      }
    } catch (InterruptedException e) {
      LOGGER.warn(Thread.currentThread().getName() + " is interrupt.");
      Thread.currentThread().interrupt();
    } finally {
      LOGGER.info(Thread.currentThread().getName() + " received a pill and finished store log to hbbase.");
      HBaseResourceManager.closeHTable(hTable);
      signal.countDown();
    }
  }
}
