package com.elex.bigdata.zergling.etl;

import static com.elex.bigdata.zergling.etl.ETLUtils.ip2Long;

import com.elex.bigdata.zergling.etl.hbase.HBasePutterV3;
import com.elex.bigdata.zergling.etl.hbase.HBaseResourceManager;
import com.elex.bigdata.zergling.etl.hbase.PutterCounter;
import com.elex.bigdata.zergling.etl.model.AllInOneNavigatorLog;
import com.elex.bigdata.zergling.etl.model.LogBatch;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * User: Z J Wu Date: 14-3-27 Time: 下午5:14 Package: com.elex.bigdata.zergling.etl
 */
public class AllInOneNavigatorETL extends ETLBase {
  private static final Logger LOGGER = Logger.getLogger(AllInOneNavigatorETL.class);

  private void fillDate(char[] dateChars, String dateString) {
    dateChars[0] = dateString.charAt(0);
    dateChars[1] = dateString.charAt(1);
    dateChars[2] = dateString.charAt(2);
    dateChars[3] = dateString.charAt(3);
    dateChars[4] = dateString.charAt(5);
    dateChars[5] = dateString.charAt(6);
    dateChars[6] = dateString.charAt(8);
    dateChars[7] = dateString.charAt(9);
    dateChars[8] = dateString.charAt(11);
    dateChars[9] = dateString.charAt(12);
    dateChars[10] = dateString.charAt(14);
    dateChars[11] = dateString.charAt(15);
    dateChars[12] = dateString.charAt(17);
    dateChars[13] = dateString.charAt(18);
  }

  private void putQueue(int batchSize, File input, InternalQueue<LogBatch<AllInOneNavigatorLog>> urlRestoreQueue) throws
    IOException, InterruptedException {
    String line;
    int from, to, version = 1;
    char blank = ' ', stop = '&';
    long ipLong;
    String requestURISep = "/nav.png?", projectSep = "p=", nationSep = "nation=", uidSep = "uid=", urlSep = "url=";
    String ipString, dateString, userLocalTime, requestURI, projectId, nation, uid, url;
    char[] dateChars = new char[14];
    AllInOneNavigatorLog singleLog;
    LogBatch<AllInOneNavigatorLog> batch = new LogBatch<>(batchSize, 0);
    int cnt = 0;
    try (BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(input)));) {
      while ((line = br.readLine()) != null) {
        line = StringUtils.trimToNull(line);
        if (StringUtils.isBlank(line)) {
          continue;
        }

        // Filter other invalid request.
        if (requestURISep.indexOf(requestURISep) < 0) {
          continue;
        }

        // Parse ip
        to = line.indexOf(blank);
        if (to < 0) {
          continue;
        }
        from = 0;
        ipString = line.substring(from, to);
        if (!ipString.matches(ETLConstants.REGEX_IP_ADDRESS)) {
          ipLong = 0;
        } else {
          ipLong = ip2Long(ipString);
        }

        from = to + 1;
        to = line.indexOf(blank, from);
        if (from < 0 || to < 0) {
          continue;
        }
        dateString = line.substring(from, to);
        fillDate(dateChars, dateString);
        dateString = new String(dateChars);

        from = line.indexOf(requestURISep);
        requestURI = line.substring(from);
        projectId = extractContent(requestURI, projectSep, stop);
        nation = extractContent(requestURI, nationSep, stop);
        uid = extractContent(requestURI, uidSep, stop);
        url = extractContent(requestURI, urlSep);
        userLocalTime = toLocalTime(dateString, nation);
        singleLog = new AllInOneNavigatorLog(dateString, uid, ipLong, url, projectId, nation, userLocalTime);
        ++cnt;
        if (batch.isFull()) {
          urlRestoreQueue.put(batch);
          batch = new LogBatch<>(batchSize, version);
          ++version;
        }
        batch.add(singleLog);
      }
      if (!batch.isEmpty()) {
        urlRestoreQueue.put(batch);
      }
      LOGGER.info("All file content read(" + cnt + " lines).");
    }
  }

  private void extractAndRun(String fileInput, String hTableName, int batchSize, int urlRestoreWorkerCount,
                             int logStoreWorkerCount, boolean enableURLRestore, boolean enableHbasePut) throws
    IOException, InterruptedException {
    File input = new File(fileInput);
    if (!input.exists()) {
      throw new IOException("File not found - " + fileInput);
    }
    System.out.println("haha");
    LOGGER.info(
      "Generic navigator log putter(File=" + fileInput + ", HTable=" + hTableName + ", BatchSize=" + batchSize + ", URLRestoreWorker=" + urlRestoreWorkerCount + ", LogStoreWorker=" + logStoreWorkerCount + ") begin working.");
    final InternalQueue<LogBatch<AllInOneNavigatorLog>> urlRestoreQueue = new InternalQueue<>(), logStoreQueue = new InternalQueue<>();
    CountDownLatch urlRestoreSignal = new CountDownLatch(urlRestoreWorkerCount), logStoreSignal = new CountDownLatch(
      logStoreWorkerCount);
    PutterCounter pc = new PutterCounter();
    HBaseResourceManager manager;
    if (!enableHbasePut) {
      manager = null;
    } else {
      manager = new HBaseResourceManager((int) (logStoreWorkerCount * 1.5));
    }
    List<UrlShorterWorker<AllInOneNavigatorLog>> urlRestoreWorkers = new ArrayList<>(urlRestoreWorkerCount);
    for (int i = 0; i < urlRestoreWorkerCount; i++) {
      urlRestoreWorkers.add(new UrlShorterWorker<>(urlRestoreQueue, logStoreQueue, urlRestoreSignal, enableURLRestore));
    }
    LOGGER.info("URL restore workers inited.");

    List<HBasePutterV3<AllInOneNavigatorLog>> logStoreWorkers = new ArrayList<>(urlRestoreWorkerCount);
    HTableInterface htable;
    for (int i = 0; i < logStoreWorkerCount; i++) {
      if (!enableHbasePut) {
        htable = null;
      } else {
        htable = manager.getHTable(hTableName);
      }
      logStoreWorkers.add(new HBasePutterV3<>(logStoreQueue, logStoreSignal, htable, enableHbasePut, pc));
    }
    LOGGER.info("Log store workers inited.");
    LOGGER.info("Begin to put queue.");

    DecimalFormat df = new DecimalFormat("00");

    // Start url restore threads.
    for (int i = 0; i < urlRestoreWorkerCount; i++) {
      new Thread(urlRestoreWorkers.get(i), "UrlRestoreThread[" + df.format(i) + "]").start();
    }

    // Start log save threads.
    for (int i = 0; i < logStoreWorkerCount; i++) {
      new Thread(logStoreWorkers.get(i), "LogStoreThread[" + df.format(i) + "]").start();
    }
    QueueMonitor queueMonitor = new QueueMonitor(urlRestoreQueue, logStoreQueue);
    new Thread(queueMonitor, "MonitorThread").start();

    long t1 = System.currentTimeMillis();
    try {
      putQueue(batchSize, input, urlRestoreQueue);
      LOGGER.info("All lines have sent to restore queue. Begin to send url-restore pills.");
    } finally {
      for (int i = 0; i < urlRestoreWorkerCount; i++) {
        urlRestoreQueue.put(new LogBatch<AllInOneNavigatorLog>(true));
      }
    }
    LOGGER.info("All url-restore pill sent.");

    urlRestoreSignal.await();

    LOGGER.info("All url-restore workers finish their work. Begin to send log-store pills.");
    for (int i = 0; i < logStoreWorkerCount; i++) {
      logStoreQueue.put(new LogBatch<AllInOneNavigatorLog>(true));
    }
    logStoreSignal.await();
    LOGGER.info("All log-store workers finish their work. Closing resources.");
    queueMonitor.stop();
    long t2 = System.currentTimeMillis();
    if (enableHbasePut) {
      manager.close();
    }
    LOGGER
      .info("File processed successfully. " + pc.getVal() + " rows is processed in " + (t2 - t1) + " milliseconds.");
  }

  public static void main(String[] args) throws Exception {
    if (ArrayUtils.isEmpty(args) || args.length != 7) {
      throw new Exception("Parameters are invalid.");
    }
    String inputFilePath = StringUtils.trimToNull(args[0]);
    if (StringUtils.isBlank(inputFilePath)) {
      throw new Exception("Input file path is necessary.");
    }
    String hTableName = StringUtils.trimToNull(args[1]);
    if (StringUtils.isBlank(hTableName)) {
      throw new Exception("HTable name is necessary.");
    }

    int batchSize = Integer.parseInt(args[2]), urlRestoreWorkerCount = Integer
      .parseInt(args[3]), logStoreWorkerCount = Integer.parseInt(args[4]);
    boolean enabledURLRestore = Boolean.parseBoolean(args[5]), enableHbasePut = Boolean.parseBoolean(args[6]);
    AllInOneNavigatorETL etl = new AllInOneNavigatorETL();
    etl.extractAndRun(inputFilePath, hTableName, batchSize, urlRestoreWorkerCount, logStoreWorkerCount,
                      enabledURLRestore, enableHbasePut);
  }
}
