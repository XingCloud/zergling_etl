package com.elex.bigdata.zergling.etl;

import com.elex.bigdata.zergling.etl.hbase.HBasePutter;
import com.elex.bigdata.zergling.etl.hbase.HBaseResourceManager;
import com.elex.bigdata.zergling.etl.model.LogBatch;
import com.elex.bigdata.zergling.etl.model.NavigatorLog;
import org.junit.Test;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CountDownLatch;

/**
 * User: Z J Wu Date: 14-2-26 Time: 下午5:56 Package: com.elex.bigdata.zergling.etl
 */

public class TestLoadFromFile {

  @Test
  public void test() throws ParseException, InterruptedException, IOException {
    String projectId = "22find";
    String input = "D:/22find/22find.2014-02-20.347.log";
    String output = "D:/22find/22find.2014-02-20.347.out.log";
    String hTableName = "nav_" + projectId;

    int workerCount = 3;
    InternalQueue<LogBatch<NavigatorLog>> queue = new InternalQueue<>();
    CountDownLatch signal = new CountDownLatch(workerCount);
    List<HBasePutter> putters = new ArrayList<>(workerCount);
    HBaseResourceManager manager = new HBaseResourceManager(20);

    for (int i = 0; i < workerCount; i++) {
      putters.add(new HBasePutter(queue, signal, manager.getHTable(hTableName), true));
    }
    for (int i = 0; i < putters.size(); i++) {
      new Thread(putters.get(i), "HbasePutter" + i).start();
    }

    NavigatorETL navigatorETL = new NavigatorETL(projectId, input, output,
                                                 new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss Z", Locale.ENGLISH), 100);
    navigatorETL.run(queue, workerCount);
    signal.await();
  }
}
