package com.elex.bigdata.zergling.etl;

import static com.elex.bigdata.zergling.etl.ETLConstants.STANDARD_OUTPUT_FORMAT;
import static com.elex.bigdata.zergling.etl.ETLUtils.ip2Long;
import static com.elex.bigdata.zergling.etl.ETLUtils.truncateURL;

import com.elex.bigdata.zergling.etl.hbase.HBasePutter;
import com.elex.bigdata.zergling.etl.hbase.HBaseResourceManager;
import com.elex.bigdata.zergling.etl.hbase.PutterCounter;
import com.elex.bigdata.zergling.etl.model.LogBatch;
import com.elex.bigdata.zergling.etl.model.NavigatorLog;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.URLDecoder;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.TimeZone;
import java.util.concurrent.CountDownLatch;

/**
 * User: Z J Wu Date: 14-2-20 Time: 下午4:15 Package: com.elex.bigdata.zergling.etl
 */
public class NavigatorETL extends ETLBase {

  private static final Logger LOGGER = Logger.getLogger(NavigatorETL.class);

  private String projectId;
  private String rawFilePath;
  private String output;
  private SimpleDateFormat sdf;
  private SimpleDateFormat outputSDF;

  public NavigatorETL(String projectId, String rawFilePath, String output, SimpleDateFormat sdf,
                      String userDefinedOutputTimezone) {
    this.projectId = projectId;
    this.rawFilePath = rawFilePath;
    this.output = output;
    this.sdf = sdf;
    this.outputSDF = new SimpleDateFormat(STANDARD_OUTPUT_FORMAT);
    this.outputSDF.setTimeZone(TimeZone.getTimeZone(userDefinedOutputTimezone));
  }

  public void run(int batchSize, InternalQueue<LogBatch<NavigatorLog>> queue, int workerCount) throws IOException,
    ParseException, InterruptedException {
    File input = new File(rawFilePath);
    char blank = ' ', urlStop = '&';
    String sep1 = " - - ", sep2 = "/img.gif?", sep3 = "HTTP/1.1 ";
    int a, b;
    Date d;
    String line, ip, dateString, urlParameters, uid, uidKey = "uid=", nationKey = "nation=", nation, titleKey = "title=", title, urlKey = "url=", url, rawContent;
    int maxUID = 0;
    long ipLong;

    LOGGER.info("Begin to read.");
    long t1 = System.currentTimeMillis();

    LogBatch<NavigatorLog> batch = new LogBatch<>(batchSize, 0);

    NavigatorLog nl;
    int version = 1;
    try (BufferedReader br = new BufferedReader(
      new InputStreamReader(new FileInputStream(input))); PrintWriter pw = new PrintWriter(
      new OutputStreamWriter(new FileOutputStream(new File(output))))) {
      while ((line = br.readLine()) != null) {
        line = StringUtils.trimToNull(line);
        if (StringUtils.isBlank(line)) {
          continue;
        }
        a = line.indexOf(sep1);
        if (a < 0) {
          continue;
        }
        ip = line.substring(0, a);
        if (!ip.matches(ETLConstants.REGEX_IP_ADDRESS)) {
          continue;
        }
        ipLong = ip2Long(ip);

        a = line.indexOf('[');
        b = line.indexOf(']');
        if (a < 0 || b < 0) {
          continue;
        }
        dateString = line.substring(a + 1, b);
        d = sdf.parse(dateString);
        dateString = outputSDF.format(d);

        a = line.indexOf(sep2);
        b = line.indexOf(blank, a + sep2.length());
        urlParameters = line.substring(a + sep2.length(), b);

        uid = StringUtils.trimToNull(extractContent(urlParameters, uidKey, urlStop));
        if (StringUtils.isBlank(uid) || ETLConstants.NULL_STRING.equalsIgnoreCase(uid)) {
//          WrongUIDLogger.getInstance().log(line);
          continue;
        }
        if (uid.length() > maxUID) {
          maxUID = uid.length();
        }
        nation = StringUtils.trimToNull(extractContent(urlParameters, nationKey, urlStop));
        if (StringUtils.isBlank(nation)) {
          nation = ETLConstants.UNKNOWN_NATION;
        }

        title = extractContent(urlParameters, titleKey, urlStop);
        if (StringUtils.isNotBlank(title)) {
          try {
            title = URLDecoder.decode(title.toLowerCase(), "utf8");
          } catch (Exception e) {
//            WrongTitleLogger.getInstance().log(line);
          }
        }

        url = StringUtils.trimToNull(extractContent(urlParameters, urlKey));

        if (StringUtils.isNotBlank(url)) {
          try {
            if (url.startsWith("http://goo.mx")) {
              url = ETLUtils.restoreShortenedURL(url);
            }
            url = URLDecoder.decode(url, "utf8");
          } catch (Exception e) {
//            WrongURLLogger.getInstance().log(line);
          }
          url = truncateURL(url);
        }

        if (StringUtils.isBlank(title) && StringUtils.isBlank(url)) {
          continue;
        }

        a = line.indexOf(sep3);
        if (a < 0) {
          continue;
        }
        rawContent = line.substring(a);

        nl = new NavigatorLog(dateString, uid, nation, title, ipLong);
        nl.setUrl(url);
        nl.setRawContent(rawContent);
        pw.write(nl.toLine());
        pw.write('\n');

        if (batch.isFull()) {
          queue.put(batch);
          batch = new LogBatch<>(batchSize, version);
          ++version;
        }
        batch.add(nl);
      }
      if (!batch.isEmpty()) {
        queue.put(batch);
      }
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      for (int i = 0; i < workerCount; i++) {
        queue.put(new LogBatch<NavigatorLog>(true));
//        LOGGER.info("Pill(" + i + ") putted.");
      }
    }
    long t2 = System.currentTimeMillis();
    LOGGER.info("All done in " + (t2 - t1) + " millis.");
  }

  public static void main(String[] args) throws IOException, ParseException, InterruptedException {
    if (args == null || args.length < 8) {
      LOGGER.info("Wrong parameter number.");
      System.exit(1);
    }
    long t1 = System.currentTimeMillis();
    String projectId = args[0];
    String input = args[1];
    String output = args[2];
    String hTableName = args[3];
    boolean onlyShow = Boolean.parseBoolean(args[4]);
    int workerCount = Integer.parseInt(args[5]);
    int batchSize = Integer.parseInt(args[6]);
    String userDefinedOutputTimezone = args[7];

    InternalQueue<LogBatch<NavigatorLog>> queue = new InternalQueue<>();
    CountDownLatch signal = new CountDownLatch(workerCount);
    List<HBasePutter> putters = new ArrayList<>(workerCount);
    HBaseResourceManager manager = new HBaseResourceManager((int) (workerCount * 1.5));
    PutterCounter pc = new PutterCounter();

    for (int i = 0; i < workerCount; i++) {
      putters.add(new HBasePutter(queue, signal, manager.getHTable(hTableName), onlyShow, pc));
    }
    LOGGER.info("Hbase putter created successfully(" + workerCount + ").");

    DecimalFormat df = new DecimalFormat("00");
    for (int i = 0; i < putters.size(); i++) {
      new Thread(putters.get(i), "HbasePutter[" + df.format(i) + "]").start();
    }

    NavigatorETL navigatorETL = new NavigatorETL(projectId, input, output,
                                                 new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss Z", Locale.ENGLISH),
                                                 userDefinedOutputTimezone);
    navigatorETL.run(batchSize, queue, workerCount);
    signal.await();
    long t2 = System.currentTimeMillis();
    LOGGER.info(
      pc.getVal() + " lines putted to hbase successfully(" + workerCount + ") in " + (t2 - t1) + " milliseconds.");
  }
}
