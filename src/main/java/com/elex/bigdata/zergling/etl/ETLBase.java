package com.elex.bigdata.zergling.etl;

/**
 * User: Z J Wu Date: 14-2-24 Time: 上午10:23 Package: com.elex.bigdata.zergling.etl
 */
public abstract class ETLBase {
  protected String extractContent(String line, String key, char stop) {
    int a, b, keyLength = key.length();
    a = line.indexOf(key);
    b = line.indexOf(stop, a + keyLength);
    if (a < 0) {
      return null;
    }
    if (b >= 0) {
      return line.substring(a + keyLength, b);
    } else {
      return line.substring(a + keyLength);
    }
  }

  protected String extractContent(String line, String key) {
    int a = line.indexOf(key);
    return (a < 0) ? null : line.substring(a + key.length());
  }
}
