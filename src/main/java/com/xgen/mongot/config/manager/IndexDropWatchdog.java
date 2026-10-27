package com.xgen.mongot.config.manager;

import com.google.errorprone.annotations.concurrent.GuardedBy;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reports overdue drops, including calls that block before returning a future.
 *
 * <p>A short-lived, independent daemon thread avoids both shared executor starvation and a
 * permanent executor lifecycle. The watchdog never acquires a config, lifecycle or index lock.
 */
final class IndexDropWatchdog implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(IndexDropWatchdog.class);

  private enum State {
    RUNNING,
    COMPLETED,
    OVERDUE
  }

  @GuardedBy("this")
  private State state = State.RUNNING;

  private final CountDownLatch done = new CountDownLatch(1);
  private final long alertDeadlineNanos;
  private final AtomicLong overdueBatches;
  private final Runnable onOverdue;

  IndexDropWatchdog(Duration alertAfter, AtomicLong overdueBatches, Runnable onOverdue) {
    if (alertAfter.isNegative() || alertAfter.isZero()) {
      throw new IllegalArgumentException("Index drop alert threshold must be positive");
    }
    this.alertDeadlineNanos = System.nanoTime() + alertAfter.toNanos();
    this.overdueBatches = overdueBatches;
    this.onOverdue = onOverdue;
    Thread.ofPlatform().daemon(true).name("index-drop-watchdog").start(this::watch);
  }

  private void watch() {
    try {
      if (!this.done.await(
          Math.max(0, this.alertDeadlineNanos - System.nanoTime()), TimeUnit.NANOSECONDS)) {
        reportOverdue();
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      // This private watchdog is never interrupted as a means of cancelling a drop.
      LOG.atError().setCause(e).log("Index drop watchdog was unexpectedly interrupted");
      reportOverdue();
    }
  }

  private void reportOverdue() {
    synchronized (this) {
      if (this.state != State.RUNNING) {
        return;
      }
      this.state = State.OVERDUE;
      this.overdueBatches.incrementAndGet();
    }
    // Diagnostics must not hold even the watchdog's lock: a slow log sink must not block
    // completion.
    this.onOverdue.run();
  }

  @Override
  public synchronized void close() {
    if (this.state == State.OVERDUE) {
      this.overdueBatches.decrementAndGet();
    }
    this.state = State.COMPLETED;
    this.done.countDown();
  }
}
