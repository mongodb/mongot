package com.xgen.mongot.replication.mongodb.steadystate.changestream;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.xgen.testing.mongot.index.definition.SearchIndexDefinitionBuilder;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.Test;

public class DualQueueExecutorTest {

  private static EnqueueContext contextWithNoPreviousBatch() {
    return new EnqueueContext(
        SearchIndexDefinitionBuilder.VALID_INDEX, Optional.empty(), () -> {});
  }

  private static EnqueueContext contextWithPreviousBatch(boolean hadDocuments) {
    return new EnqueueContext(
        SearchIndexDefinitionBuilder.VALID_INDEX,
        Optional.of(new PreviousBatchInfo(hadDocuments)),
        () -> {});
  }

  private static EnqueueContext contextWithOnDropped(Runnable onDropped) {
    return new EnqueueContext(
        SearchIndexDefinitionBuilder.VALID_INDEX, Optional.empty(), onDropped);
  }

  private static DualQueueExecutor createExecutor(int numThreads) {
    return new DualQueueExecutor("test-executor", numThreads, new SimpleMeterRegistry());
  }

  @Test
  public void enqueue_taskExecutes() throws InterruptedException {
    DualQueueExecutor executor = createExecutor(1);
    CountDownLatch latch = new CountDownLatch(1);

    assertThat(executor.enqueue(latch::countDown, contextWithNoPreviousBatch())).isTrue();

    assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();
    executor.shutdown();
  }

  @Test
  public void enqueue_withoutPreviousBatch_prioritizesTaskAheadOfRegularPriorityTasks()
      throws InterruptedException {
    DualQueueExecutor executor = createExecutor(1);
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

    executor.enqueue(() -> executionOrder.add("low"), contextWithPreviousBatch(false));
    executor.enqueue(() -> executionOrder.add("first-submission"), contextWithNoPreviousBatch());

    CountDownLatch done = new CountDownLatch(2);
    executor.enqueue(
        () -> {
          executionOrder.add("low-2");
          done.countDown();
        },
        contextWithPreviousBatch(false));
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
  public void enqueue_previousBatchHadDocuments_prioritizesTaskAheadOfRegularPriorityTasks()
      throws InterruptedException {
    DualQueueExecutor executor = createExecutor(1);
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
        contextWithPreviousBatch(false));
    executor.enqueue(
        () -> {
          executionOrder.add("high");
          done.countDown();
        },
        contextWithPreviousBatch(true));

    blockThread.countDown();
    assertThat(done.await(5, TimeUnit.SECONDS)).isTrue();

    assertThat(executionOrder.indexOf("high")).isLessThan(executionOrder.indexOf("low"));
    executor.shutdown();
  }

  @Test
  public void enqueue_withPreviousBatch_servesFourPrioritizedTasksBeforeRegularTasks()
      throws InterruptedException {
    // Verifies PREFERRED_TASKS_PER_REGULAR_TASK=4: queue 1 low then 4 high; all 4 run before low.
    DualQueueExecutor executor = createExecutor(1);
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

    CountDownLatch done = new CountDownLatch(5);
    executor.enqueue(
        () -> {
          executionOrder.add("low");
          done.countDown();
        },
        contextWithPreviousBatch(false));
    for (int i = 0; i < 4; i++) {
      int idx = i;
      executor.enqueue(
          () -> {
            executionOrder.add("high-" + idx);
            done.countDown();
          },
          contextWithPreviousBatch(true));
    }

    blockThread.countDown();
    assertThat(done.await(5, TimeUnit.SECONDS)).isTrue();

    int lowIndex = executionOrder.indexOf("low");
    for (int i = 0; i < 4; i++) {
      assertThat(executionOrder.indexOf("high-" + i)).isLessThan(lowIndex);
    }
    executor.shutdown();
  }

  @Test
  public void shutdown_completesInFlightTasks() throws InterruptedException {
    DualQueueExecutor executor = createExecutor(1);
    CountDownLatch latch = new CountDownLatch(3);

    executor.enqueue(latch::countDown, contextWithNoPreviousBatch());
    executor.enqueue(latch::countDown, contextWithPreviousBatch(true));
    executor.enqueue(latch::countDown, contextWithPreviousBatch(false));

    assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();
    executor.shutdown();
  }

  @Test
  public void enqueue_afterShutdown_returnsFalseAndCallsOnDropped() {
    DualQueueExecutor executor = createExecutor(1);
    executor.shutdown();

    AtomicBoolean dropped = new AtomicBoolean(false);
    boolean accepted = executor.enqueue(() -> {}, contextWithOnDropped(() -> dropped.set(true)));

    assertThat(accepted).isFalse();
    assertThat(dropped.get()).isTrue();
  }

  @Test
  public void constructor_rejectsNonPositiveThreads() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new DualQueueExecutor("test-executor", 0, new SimpleMeterRegistry()));
  }
}
