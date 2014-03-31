package com.elex.bigdata.zergling.etl;

import java.text.ParseException;

/**
 * User: Z J Wu Date: 14-3-31 Time: 下午4:23 Package: com.elex.bigdata.zergling.etl.model
 */
public class LastMin5 {
  public static void main(String[] args) throws ParseException {
    System.out.println(ETLUtils.last5Min());
  }
}
