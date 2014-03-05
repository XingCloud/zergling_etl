package com.elex.bigdata.zergling.etl;

import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * User: Z J Wu Date: 14-3-5 Time: 上午10:03 Package: com.elex.bigdata.zergling.etl
 */
public class TestFileDifference {

  @Test
  public void test() throws IOException {
    long l1 = 1000000000l, l2 = 500;
    byte[] b1= Bytes.toBytes(l1);
    byte[] b2= Bytes.toBytes(l2);
    System.out.println(Arrays.toString(b1));
    System.out.println(Arrays.toString(b2));

    for (int i = 0; i < 500; i++) {
      System.out.println(Arrays.toString(Bytes.toBytes(Long.valueOf(i))));
    }

//    String file = "D:/securecrt_files/22find_test.2014-03-03.nav.log";
//    File f = new File(file);
//    String line;
//    String[] arr;
//    Set<String> s = new HashSet<>(500);
//    String rowkey;
//    try (BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(f)))) {
//      while ((line = br.readLine()) != null) {
//        arr = line.split("\001");
//        rowkey = arr[0] + arr[2] + arr[1];
//        if (s.contains(rowkey)) {
//          System.out.println(rowkey);
//        } else {
//          s.add(rowkey);
//        }
//      }
//    }
  }
}
