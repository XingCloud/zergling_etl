package com.elex.bigdata.zergling.etl;

import com.elex.bigdata.zergling.etl.hbase.HBaseResourceManager;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

/**
 * User: Z J Wu Date: 14-3-5 Time: 下午3:40 Package: com.elex.bigdata.zergling.etl
 */
public class RowkeyRangeDeleter {
  private static final Logger LOGGER = Logger.getLogger(RowkeyRangeDeleter.class);

  public static void main(String[] args) throws IOException {
    if (args == null || args.length < 4) {
      LOGGER.error("Parameter is not enough");
      System.exit(1);
    }
    String table = args[0];
    String rowkeyStart = args[1];
    String rowkeyStop = args[2];
    String output = args[3];

    HBaseResourceManager manager = new HBaseResourceManager(20);

    File f = new File(output);
    HTableInterface hTableInterface = null;
    ResultScanner rs = null;

    int batch = 100, count = 0;
    List<Delete> deletes = new ArrayList<>(batch);
    try (PrintWriter pw = new PrintWriter(
      new OutputStreamWriter(new FileOutputStream(f))); BufferedReader br = new BufferedReader(
      new InputStreamReader(new FileInputStream(f)));) {
      hTableInterface = manager.getHTable(table);
      Scan scan = new Scan(Bytes.toBytes(rowkeyStart), Bytes.toBytes(rowkeyStop));

      rs = hTableInterface.getScanner(scan);

      for (Result r : rs) {
        pw.write(Bytes.toStringBinary(r.getRow()));
        pw.write("\n");
      }

      String line;
      Delete delete;
      while ((line = br.readLine()) != null) {
        if (count >= batch) {
          hTableInterface.delete(deletes);
          deletes = new ArrayList<>(batch);
          count = 0;
        }
        delete = new Delete(Bytes.toBytes(line));
        deletes.add(delete);
        ++count;
      }
      if (!deletes.isEmpty()) {
        hTableInterface.delete(deletes);
      }
    } finally {
      HBaseResourceManager.closeResultScanner(rs);
      HBaseResourceManager.closeHTable(hTableInterface);
    }
  }
}
