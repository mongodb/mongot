package com.xgen.mongot.util.concurrent;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Test;

public class DualQueueExecutorTest {

  private static final int PREFERRED_TASKS_PER_REGULAR_TASK = 4;

  private static DualQueueExecutor createExecutor(int numThreads) {
    return new DualQueueExecutor(
        "test-executor", numThreads, new SimpleMeterRegistry(), PREFERRED_TASKS_PER_REGULAR_TASK);
  }

  @Test
  public void enqueue_taskExecutes() throws InterruptedException {
    DualQueueExecutor executor = createExecutor(1);
    CountDownLatch latch = new CountDownLatch(1);

    assertThat(executor.enqueue(latch::countDown, true, () -> {})).isTrue();

    assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();
    executor.shutdown();
  }

  @Test
  public void enqueue_servesFourPrioritizedTasksBeforeRegularTasks() throws InterruptedException {
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
        true,
        () -> {});

    assertThat(threadBlocked.await(5, TimeUnit.SECONDS)).isTrue();

    CountDownLatch done = new CountDownLatch(5);
    executor.enqueue(
        () -> {
          executionOrder.add("low");
          done.countDown();
        },
        false,
        () -> {});
    for (int i = 0; i < 4; i++) {
      int idx = i;
      executor.enqueue(
          () -> {
            executionOrder.add("high-" + idx);
            done.countDown();
          },
          true,
          () -> {});
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
  public void enqueue_loneHighPriorityTask_neverStarvedByManyIdleIndexes()
      throws InterruptedException {
    DualQueueExecutor executor = createExecutor(1);
    int numIdleIndexes = 20;
    int roundsPerIdleIndex = 20;
    int totalRegularTasks = numIdleIndexes * roundsPerIdleIndex;

    List<String> executionOrder = Collections.synchronizedList(new ArrayList<>());
    CountDownLatch idleDone = new CountDownLatch(totalRegularTasks);
    AtomicBoolean keepHighPriorityGoing = new AtomicBoolean(true);

    Runnable[] highPriorityTask = new Runnable[1];
    highPriorityTask[0] =
        () -> {
          executionOrder.add("high");
          if (keepHighPriorityGoing.get()) {
            executor.enqueue(highPriorityTask[0], true, () -> {});
          }
        };
    executor.enqueue(highPriorityTask[0], true, () -> {});

    for (int i = 0; i < numIdleIndexes; i++) {
      AtomicInteger remainingRounds = new AtomicInteger(roundsPerIdleIndex);
      Runnable[] idleTask = new Runnable[1];
      idleTask[0] =
          () -> {
            executionOrder.add("regular");
            idleDone.countDown();
            if (remainingRounds.decrementAndGet() > 0) {
              executor.enqueue(idleTask[0], false, () -> {});
            }
          };
      executor.enqueue(idleTask[0], false, () -> {});
    }

    assertThat(idleDone.await(30, TimeUnit.SECONDS)).isTrue();
    keepHighPriorityGoing.set(false);
    executor.shutdown();

    List<String> snapshot = new ArrayList<>(executionOrder);
    for (int i = 1; i < snapshot.size(); i++) {
      if (snapshot.get(i).equals("regular")) {
        assertThat(snapshot.get(i - 1)).isNotEqualTo("regular");
      }
    }
  }

  @Test
  public void enqueue_fromManyConcurrentThreadsWithMultipleWorkers_runsEveryTaskExactlyOnce()
      throws InterruptedException {
    // All other tests here use a single worker thread, so routing is exercised against a single
    // consumer. This drives concurrent producers AND multiple worker threads at once, to catch
    // routing bugs (dropped/duplicated tasks, races in DualBlockingQueue's selectNext) that only
    // surface with real multi-threaded contention on both ends.
    DualQueueExecutor executor = createExecutor(4);
    int totalTasks = 500;
    CountDownLatch done = new CountDownLatch(totalTasks);
    AtomicInteger executedCount = new AtomicInteger();

    ExecutorService submitters = java.util.concurrent.Executors.newFixedThreadPool(8);
    try {
      for (int i = 0; i < totalTasks; i++) {
        boolean isHighPriority = i % 2 == 0;
        submitters.execute(
            () ->
                executor.enqueue(
                    () -> {
                      executedCount.incrementAndGet();
                      done.countDown();
                    },
                    isHighPriority,
                    () -> {}));
      }

      assertThat(done.await(10, TimeUnit.SECONDS)).isTrue();
    } finally {
      submitters.shutdown();
    }
    assertThat(executedCount.get()).isEqualTo(totalTasks);
    executor.shutdown();
  }

  @Test
  public void shutdown_completesInFlightTasks() throws InterruptedException {
    DualQueueExecutor executor = createExecutor(1);
    CountDownLatch latch = new CountDownLatch(3);

    executor.enqueue(latch::countDown, true, () -> {});
    executor.enqueue(latch::countDown, true, () -> {});
    executor.enqueue(latch::countDown, false, () -> {});

    assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();
    executor.shutdown();
  }

  @Test
  public void enqueue_afterShutdown_returnsFalseAndCallsOnDropped() {
    DualQueueExecutor executor = createExecutor(1);
    executor.shutdown();

    AtomicBoolean dropped = new AtomicBoolean(false);
    boolean accepted = executor.enqueue(() -> {}, true, () -> dropped.set(true));

    assertThat(accepted).isFalse();
    assertThat(dropped.get()).isTrue();
  }

  @Test
  public void constructor_rejectsNonPositiveThreads() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new DualQueueExecutor(
                "test-executor", 0, new SimpleMeterRegistry(), PREFERRED_TASKS_PER_REGULAR_TASK));
  }

  @Test
  public void constructor_rejectsNonPositiveRatio() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new DualQueueExecutor("test-executor", 1, new SimpleMeterRegistry(), 0));
  }
}
