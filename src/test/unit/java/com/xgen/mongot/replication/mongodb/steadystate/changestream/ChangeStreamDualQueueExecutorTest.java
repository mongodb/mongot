package com.xgen.mongot.replication.mongodb.steadystate.changestream;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.xgen.mongot.replication.mongodb.common.ChangeStreamBatch;
import com.xgen.mongot.replication.mongodb.common.MongoDbReplicationConfig;
import com.xgen.testing.mongot.index.definition.SearchIndexDefinitionBuilder;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.commons.codec.binary.Hex;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.BsonTimestamp;
import org.junit.Test;

public class ChangeStreamDualQueueExecutorTest {

  private static EnqueueContext contextWithNoPreviousBatch() {
    return new EnqueueContext(SearchIndexDefinitionBuilder.VALID_INDEX, Optional.empty(), () -> {});
  }

  /** Previous batch that puts the index well above the lag threshold (should be high priority). */
  private static EnqueueContext contextWithHighOpTimeLag() {
    int nowSeconds = (int) (System.currentTimeMillis() / 1000);
    BsonTimestamp commandOperationTime = new BsonTimestamp(nowSeconds, 0);
    BsonTimestamp resumeTokenTime = new BsonTimestamp(nowSeconds - 60, 0);
    return new EnqueueContext(
        SearchIndexDefinitionBuilder.VALID_INDEX,
        Optional.of(
            new ChangeStreamBatch(
                Collections.emptyList(),
                resumeTokenForTime(resumeTokenTime),
                commandOperationTime)),
        () -> {});
  }

  /** A previous batch where the index is caught up (should be low priority). */
  private static EnqueueContext contextWithCaughtUpPreviousBatch() {
    int nowSeconds = (int) (System.currentTimeMillis() / 1000);
    BsonTimestamp ts = new BsonTimestamp(nowSeconds, 0);
    return new EnqueueContext(
        SearchIndexDefinitionBuilder.VALID_INDEX,
        Optional.of(new ChangeStreamBatch(Collections.emptyList(), resumeTokenForTime(ts), ts)),
        () -> {});
  }

  /**
   * Previous batch with 2s of optime lag — below both a 5s threshold and the default 60s threshold.
   */
  private static EnqueueContext contextWithLowOpTimeLag() {
    int nowSeconds = (int) (System.currentTimeMillis() / 1000);
    BsonTimestamp commandOperationTime = new BsonTimestamp(nowSeconds, 0);
    BsonTimestamp resumeTokenTime = new BsonTimestamp(nowSeconds - 2, 0);
    return new EnqueueContext(
        SearchIndexDefinitionBuilder.VALID_INDEX,
        Optional.of(
            new ChangeStreamBatch(
                Collections.emptyList(),
                resumeTokenForTime(resumeTokenTime),
                commandOperationTime)),
        () -> {});
  }

  private static EnqueueContext contextWithOnDropped(Runnable onDropped) {
    return new EnqueueContext(
        SearchIndexDefinitionBuilder.VALID_INDEX, Optional.empty(), onDropped);
  }

  private static final int PREFERRED_TASKS_PER_REGULAR_TASK = 4;

  private static ChangeStreamDualQueueExecutor createExecutor(int numThreads) {
    return new ChangeStreamDualQueueExecutor(
        "test-executor",
        numThreads,
        new SimpleMeterRegistry(),
        new OptimeLagPriorityStrategy(
            MongoDbReplicationConfig.DEFAULT_STEADY_STATE_PRIORITY_OPTIME_THRESHOLD_SECONDS),
        PREFERRED_TASKS_PER_REGULAR_TASK);
  }

  /** Executor with a lower threshold so we can test the high-priority path. */
  private static ChangeStreamDualQueueExecutor createExecutorWithThreshold(
      int numThreads, int thresholdSeconds) {
    return new ChangeStreamDualQueueExecutor(
        "test-executor",
        numThreads,
        new SimpleMeterRegistry(),
        new OptimeLagPriorityStrategy(thresholdSeconds),
        PREFERRED_TASKS_PER_REGULAR_TASK);
  }

  /** Builds a PBRT whose decoded optime is {@code ts}. */
  private static BsonDocument resumeTokenForTime(BsonTimestamp ts) {
    ByteBuffer buf = ByteBuffer.allocate(9);
    buf.put((byte) 0x82);
    buf.putLong(ts.getValue());
    return new BsonDocument("_data", new BsonString(Hex.encodeHexString(buf.array())));
  }

  @Test
  public void enqueue_taskExecutes() throws InterruptedException {
    ChangeStreamDualQueueExecutor executor = createExecutor(1);
    CountDownLatch latch = new CountDownLatch(1);

    assertThat(executor.enqueue(latch::countDown, contextWithNoPreviousBatch())).isTrue();

    assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();
    executor.shutdown();
  }

  @Test
  public void enqueue_withoutPreviousBatch_prioritizesTaskAheadOfRegularPriorityTasks()
      throws InterruptedException {
    ChangeStreamDualQueueExecutor executor = createExecutor(1);
    CountDownLatch blockThread = new CountDownLatch(1);
    CountDownLatch threadBlocked = new CountDownLatch(1);
    List<String> executionOrder = new ArrayList<>();

    executor.enqueue(
        () -> {
          threadBlocked.countDown();
          try {
            blockThread.await();
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
        },
        contextWithNoPreviousBatch());

    assertThat(threadBlocked.await(5, TimeUnit.SECONDS)).isTrue();

    executor.enqueue(() -> executionOrder.add("low"), contextWithCaughtUpPreviousBatch());
    executor.enqueue(() -> executionOrder.add("first-submission"), contextWithNoPreviousBatch());

    CountDownLatch done = new CountDownLatch(2);
    executor.enqueue(
        () -> {
          executionOrder.add("low-2");
          done.countDown();
        },
        contextWithCaughtUpPreviousBatch());
    executor.enqueue(
        () -> {
          executionOrder.add("first-submission-2");
          done.countDown();
        },
        contextWithNoPreviousBatch());

    blockThread.countDown();
    assertThat(done.await(5, TimeUnit.SECONDS)).isTrue();

    assertThat(executionOrder.indexOf("first-submission"))
        .isLessThan(executionOrder.indexOf("low"));
    executor.shutdown();
  }

  @Test
  public void enqueue_optimeLagAboveThreshold_isHighPriority() throws InterruptedException {
    // Use a low threshold (5s) so contextWithHighOpTimeLag (60s lag) triggers high priority.
    ChangeStreamDualQueueExecutor executor = createExecutorWithThreshold(1, 5);
    CountDownLatch blockThread = new CountDownLatch(1);
    CountDownLatch threadBlocked = new CountDownLatch(1);
    List<String> executionOrder = new ArrayList<>();

    executor.enqueue(
        () -> {
          threadBlocked.countDown();
          try {
            blockThread.await();
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
        },
        contextWithNoPreviousBatch());

    assertThat(threadBlocked.await(5, TimeUnit.SECONDS)).isTrue();

    CountDownLatch done = new CountDownLatch(2);
    executor.enqueue(
        () -> {
          executionOrder.add("low");
          done.countDown();
        },
        contextWithCaughtUpPreviousBatch());
    executor.enqueue(
        () -> {
          executionOrder.add("high");
          done.countDown();
        },
        contextWithHighOpTimeLag());

    blockThread.countDown();
    assertThat(done.await(5, TimeUnit.SECONDS)).isTrue();

    assertThat(executionOrder.indexOf("high")).isLessThan(executionOrder.indexOf("low"));
    executor.shutdown();
  }

  @Test
  public void enqueue_optimeLagBelowThreshold_isLowPriority() throws InterruptedException {
    ChangeStreamDualQueueExecutor executor = createExecutorWithThreshold(1, 5);
    CountDownLatch blockThread = new CountDownLatch(1);
    CountDownLatch threadBlocked = new CountDownLatch(1);
    List<String> executionOrder = new ArrayList<>();

    executor.enqueue(
        () -> {
          threadBlocked.countDown();
          try {
            blockThread.await();
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
        },
        contextWithNoPreviousBatch());

    assertThat(threadBlocked.await(5, TimeUnit.SECONDS)).isTrue();

    CountDownLatch done = new CountDownLatch(2);
    executor.enqueue(
        () -> {
          executionOrder.add("below-threshold");
          done.countDown();
        },
        contextWithLowOpTimeLag());
    executor.enqueue(
        () -> {
          executionOrder.add("high");
          done.countDown();
        },
        contextWithNoPreviousBatch());

    blockThread.countDown();
    assertThat(done.await(5, TimeUnit.SECONDS)).isTrue();

    assertThat(executionOrder.indexOf("high"))
        .isLessThan(executionOrder.indexOf("below-threshold"));
    executor.shutdown();
  }

  @Test
  public void shutdown_completesInFlightTasks() throws InterruptedException {
    ChangeStreamDualQueueExecutor executor = createExecutor(1);
    CountDownLatch latch = new CountDownLatch(3);

    executor.enqueue(latch::countDown, contextWithNoPreviousBatch());
    executor.enqueue(latch::countDown, contextWithHighOpTimeLag());
    executor.enqueue(latch::countDown, contextWithCaughtUpPreviousBatch());

    assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();
    executor.shutdown();
  }

  @Test
  public void enqueue_afterShutdown_returnsFalseAndCallsOnDropped() {
    ChangeStreamDualQueueExecutor executor = createExecutor(1);
    executor.shutdown();

    AtomicBoolean dropped = new AtomicBoolean(false);
    boolean accepted = executor.enqueue(() -> {}, contextWithOnDropped(() -> dropped.set(true)));

    assertThat(accepted).isFalse();
    assertThat(dropped.get()).isTrue();
  }

  @Test
  public void constructor_rejectsNullStrategy() {
    assertThrows(
        NullPointerException.class,
        () ->
            new ChangeStreamDualQueueExecutor(
                "test-executor",
                1,
                new SimpleMeterRegistry(),
                null,
                PREFERRED_TASKS_PER_REGULAR_TASK));
  }
}
