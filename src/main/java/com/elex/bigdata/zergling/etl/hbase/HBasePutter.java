package com.elex.bigdata.zergling.etl.hbase;

import com.elex.bigdata.logging.WrongHbasePutLogger;
import com.elex.bigdata.zergling.etl.InternalQueue;
import com.elex.bigdata.zergling.etl.model.ColumnInfo;
import com.elex.bigdata.zergling.etl.model.LogBatch;
import com.elex.bigdata.zergling.etl.model.NavigatorLog;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * User: Z J Wu Date: 14-2-26 Time: 上午9:48 Package: com.elex.bigdata.zergling.etl.hbase
 */
public class HBasePutter implements Runnable {

  private static final Logger LOGGER = Logger.getLogger(HBasePutter.class);

  private InternalQueue<LogBatch<NavigatorLog>> queue;

  private CountDownLatch signal;

  private HTableInterface hTable;

  private boolean onlyShow;

  public HBasePutter(InternalQueue<LogBatch<NavigatorLog>> queue, CountDownLatch signal, HTableInterface hTable,
                     boolean onlyShow) {
    this.queue = queue;
    this.signal = signal;
    this.hTable = hTable;
    this.onlyShow = onlyShow;
  }

  @Override
  public void run() {
    LogBatch<NavigatorLog> batch;
    Put put;
    List<NavigatorLog> content;
    byte[] rowkeyBytes;
    List<ColumnInfo> columnInfos;
    List<Put> puts;
    try {
      while (true) {
        batch = queue.take();
        if (batch == null || batch.isEmpty()) {
          continue;
        }
        if (batch.isPill()) {
          LOGGER.warn(Thread.currentThread().getName() + " pill received, quit now.");
          break;
        }
        content = batch.getContent();
        puts = new ArrayList<>(content.size());
        for (NavigatorLog log : content) {
          rowkeyBytes = log.getRowkey();
          try {
            columnInfos = log.getColumnInfos();
          } catch (IllegalAccessException e) {
            continue;
          }
          put = new Put(rowkeyBytes);
          for (ColumnInfo ci : columnInfos) {
            put.add(ci.getColumnFamilyBytes(), ci.getQualifierBytes(), ci.getValueBytes());
          }
          puts.add(put);
        }
        if (onlyShow) {
          continue;
        }
        long t1 = System.currentTimeMillis();
        boolean successful = true;
        try {
          hTable.put(puts);
        } catch (IOException e) {
          successful = false;
          WrongHbasePutLogger.getInstance().logNavigatorLog(e.getClass().getName(), content);
          e.printStackTrace();
        } finally {
          long t2 = System.currentTimeMillis();
          LOGGER.info("Status=[" + (successful ? "ok" : "with-err"
          ) + "], CostInMilliseconds(" + Thread.currentThread().getName() + ")=" + (t2 - t1));
        }
      }
    } catch (InterruptedException e) {
      LOGGER.warn(Thread.currentThread().getName() + " is interrupt.");
      Thread.currentThread().interrupt();
    } finally {
      HBaseResourceManager.closeHTable(hTable);
      signal.countDown();
    }
  }
}
