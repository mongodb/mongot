package com.xgen.mongot.replication.mongodb.steadystate.changestream;

import static com.google.common.truth.Truth.assertThat;
import static com.xgen.testing.mongot.mock.index.VectorIndex.mockAutoEmbeddingVectorDefinition;

import com.xgen.mongot.util.concurrent.Executors;
import com.xgen.mongot.util.concurrent.NamedScheduledExecutorService;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.bson.types.ObjectId;
import org.junit.Test;

public class FifoExecutorTest {

  private static EnqueueContext contextWithOnDropped(Runnable onDropped) {
    return new EnqueueContext(
        mockAutoEmbeddingVectorDefinition(new ObjectId()), Optional.empty(), onDropped);
  }

  private static FifoExecutor createExecutor(int numThreads) {
    NamedScheduledExecutorService delegate =
        Executors.fixedSizeThreadScheduledExecutor(
            "test-executor", numThreads, new SimpleMeterRegistry());
    return new FifoExecutor(delegate);
  }

  @Test
  public void enqueue_taskExecutes() throws InterruptedException {
    FifoExecutor executor = createExecutor(1);
    CountDownLatch latch = new CountDownLatch(1);

    assertThat(executor.enqueue(latch::countDown, contextWithOnDropped(() -> {}))).isTrue();

    assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();
    executor.shutdown();
  }

  @Test
  public void enqueue_afterShutdown_returnsFalseAndCallsOnDropped() {
    FifoExecutor executor = createExecutor(1);
    executor.shutdown();

    AtomicBoolean dropped = new AtomicBoolean(false);
    boolean accepted = executor.enqueue(() -> {}, contextWithOnDropped(() -> dropped.set(true)));

    assertThat(accepted).isFalse();
    assertThat(dropped.get()).isTrue();
  }

  @Test
  public void enqueue_doesNotThrottleAutoEmbeddingIndexes() throws InterruptedException {
    FifoExecutor executor = createExecutor(2);
    CountDownLatch bothRunning = new CountDownLatch(2);
    CountDownLatch release = new CountDownLatch(1);

    Runnable blockingTask =
        () -> {
          bothRunning.countDown();
          try {
            release.await();
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
        };

    executor.enqueue(blockingTask, contextWithOnDropped(() -> {}));
    executor.enqueue(blockingTask, contextWithOnDropped(() -> {}));

    assertThat(bothRunning.await(5, TimeUnit.SECONDS)).isTrue();
    release.countDown();
    executor.shutdown();
  }
}
