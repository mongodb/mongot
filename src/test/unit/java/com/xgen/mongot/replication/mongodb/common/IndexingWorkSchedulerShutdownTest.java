package com.xgen.mongot.replication.mongodb.common;

import static com.google.common.truth.Truth.assertThat;

import com.xgen.mongot.replication.mongodb.common.IndexingWorkSchedulerFactory.IndexingStrategy;
import com.xgen.mongot.util.concurrent.Executors;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Test;

/**
 * Covers {@link IndexingWorkScheduler#shutdown()} being safe to call more than once.
 *
 * <p>This is not hypothetical: {@link IndexingWorkSchedulerFactory} maps several {@link
 * IndexingStrategy} keys onto a single scheduler instance, so a caller walking those entries
 * reaches the same instance repeatedly. A second {@code interrupt()} lands while the run loop is in
 * {@code Executors.shutdownOrFail}, whose {@code awaitTermination} treats {@code
 * InterruptedException} as fatal and halts the JVM -- which surfaces in CI as a bazel {@code FAIL
 * (Exit 1)} on a test that actually passed.
 */
public class IndexingWorkSchedulerShutdownTest {

  /** Counts interrupts so the test can assert the extra shutdown calls did not deliver one. */
  private static final class InterruptCountingScheduler extends IndexingWorkScheduler {

    private final AtomicInteger interrupts = new AtomicInteger();

    InterruptCountingScheduler() {
      super(
          Executors.fixedSizeThreadPool("indexing", 1, new SimpleMeterRegistry()),
          IndexingStrategy.EMBEDDING_MATERIALIZED_VIEW);
    }

    @Override
    public void interrupt() {
      this.interrupts.incrementAndGet();
      super.interrupt();
    }

    @Override
    CompletableFuture<Void> getBatchTasksFuture(IndexingSchedulerBatch batch) {
      return CompletableFuture.completedFuture(null);
    }

    @Override
    void handleBatchException(IndexingSchedulerBatch batch, Throwable throwable) {}
  }

  @Test
  public void repeatedShutdownInterruptsOnceAndReturnsTheSameFuture() throws Exception {
    var scheduler = new InterruptCountingScheduler();
    scheduler.start();

    CompletableFuture<Void> first = scheduler.shutdown();
    CompletableFuture<Void> second = scheduler.shutdown();
    CompletableFuture<Void> third = scheduler.shutdown();

    // One instance is reachable under four IndexingStrategy keys, so a caller iterating the factory
    // map calls shutdown four times; only the first may interrupt.
    CompletableFuture<Void> fourth = scheduler.shutdown();

    assertThat(second).isSameInstanceAs(first);
    assertThat(third).isSameInstanceAs(first);
    assertThat(fourth).isSameInstanceAs(first);

    // Completing at all is part of the assertion: a redundant interrupt would have halted the JVM
    // from inside the run loop's shutdownOrFail instead.
    first.get(10, TimeUnit.SECONDS);

    assertThat(scheduler.interrupts.get()).isEqualTo(1);
  }

  @Test
  public void shutdownCompletesWhenCalledOnce() throws Exception {
    var scheduler = new InterruptCountingScheduler();
    scheduler.start();

    scheduler.shutdown().get(10, TimeUnit.SECONDS);

    assertThat(scheduler.interrupts.get()).isEqualTo(1);
  }
}
