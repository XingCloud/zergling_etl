package com.elex.bigdata.zergling.etl;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * User: Z J Wu Date: 14-3-17 Time: 上午11:07 Package: com.elex.bigdata.etl
 */
public class TestQueuePut {
  private static class InnerRunner implements Runnable {
    private LinkedBlockingQueue<Integer> queue;
    private CountDownLatch signal;

    private InnerRunner(LinkedBlockingQueue<Integer> queue, CountDownLatch signal) {
      this.queue = queue;
      this.signal = signal;
    }

    private void doJob(Integer i) {
      if (i > 4) {
        throw new RuntimeException("Val is too big.");
      }
      System.out.println("Value is " + i);
    }

    @Override
    public void run() {
      Integer v;
      try {
        while (true) {
          v = queue.take();
          if (v == null) {
            continue;
          }
          if (v <= 0) {
            break;
          }
          boolean successful = false;
          try {
            doJob(v);
            successful = true;
          } catch (Exception e) {
            System.out.println("Exception caught: " + e.getMessage());
          } finally {
            if (successful) {
//              System.out.println("This round is ok.");
            } else {
//              System.out.println("This round is wrong.");
            }
          }
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      } finally {
        signal.countDown();
      }
    }
  }

  public static void main(String[] args) throws InterruptedException {
    int size = 2;
    CountDownLatch signal = new CountDownLatch(size);
    LinkedBlockingQueue<Integer> queue = new LinkedBlockingQueue<>();
    List<InnerRunner> runners = new ArrayList<>(size);

    for (int i = 0; i < size; i++) {
      runners.add(new InnerRunner(queue, signal));
    }

    for (InnerRunner ir : runners) {
      new Thread(ir).start();
    }

    Random random = new Random();
    for (int i = 0; i < 10; i++) {
      queue.put(random.nextInt(6) + 1);
    }

    for (int i = 0; i < size; i++) {
      queue.put(-1);
    }
    signal.await();
    System.out.println("All done.");
  }
}
