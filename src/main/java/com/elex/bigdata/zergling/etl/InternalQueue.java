package com.elex.bigdata.zergling.etl;

import java.util.concurrent.LinkedBlockingQueue;

/**
 * User: Z J Wu Date: 14-2-26 Time: 上午9:41 Package: com.elex.bigdata.zergling.etl
 */
public class InternalQueue<T> {

  private final LinkedBlockingQueue<T> queue;

  public InternalQueue() {
    this.queue = new LinkedBlockingQueue<>();
  }

  public InternalQueue(int capacity) {
    this.queue = new LinkedBlockingQueue<>(capacity);
  }

  public T take() throws InterruptedException {
    return queue.take();
  }

  public void put(T t) throws InterruptedException {
    queue.put(t);
  }

}
