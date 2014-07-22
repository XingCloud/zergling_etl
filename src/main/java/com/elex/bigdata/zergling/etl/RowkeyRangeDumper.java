package com.elex.bigdata.zergling.etl;

import com.elex.bigdata.zergling.etl.hbase.HBaseResourceManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;

/**
 * User: Z J Wu Date: 14-3-5 Time: 下午3:40 Package: com.elex.bigdata.zergling.etl
 */
public class RowkeyRangeDumper {
  private static final Logger LOGGER = Logger.getLogger(RowkeyRangeDumper.class);

  public static void main(String[] args) throws IOException {
    if (args == null || args.length < 4) {
      LOGGER.error("Parameter is not enough");
      System.exit(1);
    }
    String table = args[0];
    String rowkeyStart = args[1];
    String rowkeyStop = args[2];
    String output = args[3];

//    HBaseResourceManager manager = new HBaseResourceManager(20);

    File f = new File(output);
    HTableInterface hTableInterface = null;
    try (PrintWriter pw = new PrintWriter(new OutputStreamWriter(new FileOutputStream(f)))) {
      hTableInterface = HBaseResourceManager.getHTable(table);
      Scan scan = new Scan(Bytes.toBytes(rowkeyStart), Bytes.toBytes(rowkeyStop));

      ResultScanner rs = hTableInterface.getScanner(scan);

      for (Result r : rs) {
        pw.write(Bytes.toStringBinary(r.getRow()));
        pw.write("\n");
      }
    } finally {
      HBaseResourceManager.closeHTable(hTableInterface);
    }
  }
}
