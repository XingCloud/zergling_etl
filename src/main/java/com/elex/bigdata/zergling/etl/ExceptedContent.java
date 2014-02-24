package com.elex.bigdata.zergling.etl;

/**
 * User: Z J Wu Date: 14-2-24 Time: 下午3:44 Package: com.elex.bigdata.zergling.etl
 */
public enum ExceptedContent {
  ERROR_WRONG_URL("BD_ERR_01");

  private String id;

  ExceptedContent(String id) {
    this.id = id;
  }

  public String getId() {
    return id;
  }
}
