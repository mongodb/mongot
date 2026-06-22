package com.xgen.testing;

import com.google.errorprone.annotations.concurrent.GuardedBy;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * A Fake implementation of {@link BlockingQueue} for testing that allows tests to force actions to
 * be interleaved between taking an item off a queue and processing it. For example:
 *
 * <pre>{@code
 * @Test
 * public void processItemAfterShutdown() {
 *   FakeBlockingQueue<Item> queue = FakeBlockingQueue.paused();
 *   publisher = new Publisher(queue).start();
 *   consumer = new Consumer(queue).start(); // dequeues next item and then pauses thread
 *
 *   server.shutdown();
 *   queue.resume(); // resume processing of previously dequeued item
 *   consumer.close()
 * }
 * }</pre>
 */
public class ControlledBlockingQueue<T> extends LinkedBlockingQueue<T> {

  private final Object pauseMonitor = new Object();

  private final Object takeCountMonitor = new Object();

  /** Number of items that have been removed from the queue. */
  @GuardedBy("takeCountMonitor")
  private int takeCount = 0;

  @GuardedBy("pauseMonitor")
  private boolean isPaused = true;

  public static <T> ControlledBlockingQueue<T> paused() {
    return new ControlledBlockingQueue<T>().pause();
  }

  public static <T> ControlledBlockingQueue<T> ready() {
    return new ControlledBlockingQueue<T>().resume();
  }

  /**
   * Causes subsequent calls to take/poll to block after removing the next element from the queue
   * until another thread calls {@link #resume()}.
   */
  public ControlledBlockingQueue<T> pause() {
    synchronized (this.pauseMonitor) {
      this.isPaused = true;
    }
    return this;
  }

  /**
   * If the queue is currently in the blocked state, all blocked threads resume their execution and
   * future calls to take/poll will not block until {@link #pause()} is called again.
   */
  public ControlledBlockingQueue<T> resume() {
    synchronized (this.pauseMonitor) {
      this.isPaused = false;
      this.pauseMonitor.notifyAll();
    }
    return this;
  }

  /** If paused, blocks until {@link #resume()} is called, otherwise returns immediately. */
  private void waitForReady() throws InterruptedException {
    synchronized (this.pauseMonitor) {
      while (this.isPaused) {
        this.pauseMonitor.wait();
      }
    }
  }

  /** Increments the take counter and notifies all waiting threads. */
  private void notifyTake() {
    synchronized (this.takeCountMonitor) {
      ++this.takeCount;
      this.takeCountMonitor.notifyAll();
    }
  }

  /**
   * Waits until at least n items have been dequeued from this {@link BlockingQueue}.
   *
   * @param n - The number of items to wait for, counted from the time that the queue is created
   */
  public void waitForItemsDequeued(int n) throws InterruptedException {
    synchronized (this.takeCountMonitor) {
      while (this.takeCount < n) {
        this.takeCountMonitor.wait();
      }
    }
  }

  /**
   * Take an item off this queue.
   *
   * <p>If this queue is paused, an item is taken off the queue as soon as it becomes available, any
   * threads listening are notified of the take event, and then the calling thread blocks until
   * {@link #resume()} is called. If this queue is not paused, this function acts normally and
   * notifies any threads listening for a take event.
   */
  @Override
  public T take() throws InterruptedException {
    T next = super.take();
    notifyTake();
    waitForReady();
    return next;
  }

  @Override
  public T poll() {
    T next = super.poll();
    try {
      if (next != null) {
        notifyTake();
      }
      waitForReady();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
    return next;
  }

  @Override
  public T peek() {
    T next = super.peek();
    try {
      waitForReady();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
    return next;
  }
}
