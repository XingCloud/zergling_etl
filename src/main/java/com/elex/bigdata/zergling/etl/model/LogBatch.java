package com.elex.bigdata.zergling.etl.model;

import org.apache.commons.collections.CollectionUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * User: Z J Wu Date: 14-2-26 Time: 上午9:53 Package: com.elex.bigdata.zergling.etl.model
 */
public class LogBatch<T> {

  public static final LogBatch PILL = new LogBatch<>(true);

  private int capacity;

  private final boolean pill;

  private List<T> content;

  private int count = 0;

  public LogBatch() {
    this.content = new ArrayList<>(30);
    this.capacity = 30;
    this.pill = false;
  }

  public LogBatch(int initCapacity) {
    this.content = new ArrayList<>(initCapacity);
    this.pill = false;
    this.capacity = initCapacity;
  }

  public LogBatch(boolean pill) {
    this.pill = pill;
  }

  public boolean isPill() {
    return pill;
  }

  public List<T> getContent() {
    return content;
  }

  public void add(T t) {
    this.content.add(t);
    ++count;
  }

  public boolean isFull() {
    return isPill() ? true : (count == capacity);
  }

  public int size() {
    return isPill() ? 0 : this.content.size();
  }

  public boolean isEmpty() {
    return !isPill() && CollectionUtils.isEmpty(content);
  }

}
