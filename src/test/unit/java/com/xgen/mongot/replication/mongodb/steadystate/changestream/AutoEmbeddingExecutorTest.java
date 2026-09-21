package com.xgen.mongot.replication.mongodb.steadystate.changestream;

import static com.google.common.truth.Truth.assertThat;
import static com.xgen.testing.mongot.mock.index.VectorIndex.mockAutoEmbeddingVectorDefinition;

import com.xgen.mongot.replication.mongodb.common.ChangeStreamBatch;
import com.xgen.mongot.util.concurrent.Executors;
import com.xgen.mongot.util.concurrent.NamedScheduledExecutorService;
import com.xgen.testing.mongot.index.definition.SearchIndexDefinitionBuilder;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.lang.ref.WeakReference;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.bson.BsonDocument;
import org.bson.BsonTimestamp;
import org.bson.types.ObjectId;
import org.junit.Test;

public class AutoEmbeddingExecutorTest {

  private static EnqueueContext embeddingContextWithOnDropped(Runnable onDropped) {
    return new EnqueueContext(
        mockAutoEmbeddingVectorDefinition(new ObjectId()), Optional.empty(), onDropped);
  }

  private static EnqueueContext searchContextWithOnDropped(Runnable onDropped) {
    return new EnqueueContext(
        SearchIndexDefinitionBuilder.VALID_INDEX, Optional.empty(), onDropped);
  }

  private static AutoEmbeddingExecutor createExecutor(int numThreads, Optional<Integer> cap) {
    NamedScheduledExecutorService delegate =
        Executors.fixedSizeThreadScheduledExecutor(
            "test-executor", numThreads, new SimpleMeterRegistry());
    return new AutoEmbeddingExecutor(new SimpleMeterRegistry(), delegate, cap);
  }

  @Test
  public void enqueue_taskExecutes() throws InterruptedException {
    AutoEmbeddingExecutor executor = createExecutor(1, Optional.of(1));
    CountDownLatch latch = new CountDownLatch(1);

    assertThat(executor.enqueue(latch::countDown, embeddingContextWithOnDropped(() -> {})))
        .isTrue();

    assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();
    executor.shutdown();
  }

  @Test
  public void enqueue_nonEmbeddingIndex_isNotThrottled() throws InterruptedException {
    // The cap only applies to auto-embedding indexes; other submissions run unthrottled even when
    // the single permit is held.
    AutoEmbeddingExecutor executor = createExecutor(2, Optional.of(1));
    CountDownLatch holdPermit = new CountDownLatch(1);
    CountDownLatch permitHeld = new CountDownLatch(1);
    CountDownLatch searchTaskDone = new CountDownLatch(1);

    executor.enqueue(
        () -> {
          permitHeld.countDown();
          try {
            holdPermit.await();
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
        },
        embeddingContextWithOnDropped(() -> {}));
    assertThat(permitHeld.await(5, TimeUnit.SECONDS)).isTrue();
    assertThat(executor.getEmbeddingAvailablePermits()).hasValue(0);

    executor.enqueue(searchTaskDone::countDown, searchContextWithOnDropped(() -> {}));

    assertThat(searchTaskDone.await(5, TimeUnit.SECONDS)).isTrue();
    holdPermit.countDown();
    executor.shutdown();
  }

  @Test
  public void enqueue_embeddingIndexAboveCap_isRescheduledUntilPermitAvailable()
      throws InterruptedException {
    AutoEmbeddingExecutor executor = createExecutor(2, Optional.of(1));
    CountDownLatch holdPermit = new CountDownLatch(1);
    CountDownLatch permitHeld = new CountDownLatch(1);
    CountDownLatch secondTaskDone = new CountDownLatch(1);

    executor.enqueue(
        () -> {
          permitHeld.countDown();
          try {
            holdPermit.await();
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
        },
        embeddingContextWithOnDropped(() -> {}));
    assertThat(permitHeld.await(5, TimeUnit.SECONDS)).isTrue();

    executor.enqueue(secondTaskDone::countDown, embeddingContextWithOnDropped(() -> {}));

    holdPermit.countDown();

    assertThat(secondTaskDone.await(5, TimeUnit.SECONDS)).isTrue();
    executor.shutdown();
  }

  @Test
  public void enqueue_embeddingIndexAboveCap_doesNotRetainPreviousBatch() throws Exception {
    AutoEmbeddingExecutor executor = createExecutor(2, Optional.of(1));
    CountDownLatch holdPermit = new CountDownLatch(1);
    CountDownLatch permitHeld = new CountDownLatch(1);
    executor.enqueue(
        () -> {
          permitHeld.countDown();
          try {
            holdPermit.await();
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
        },
        embeddingContextWithOnDropped(() -> {}));
    assertThat(permitHeld.await(5, TimeUnit.SECONDS)).isTrue();

    WeakReference<ChangeStreamBatch> previousBatch = enqueueCarryingPreviousBatch(executor);

    assertThat(awaitCollected(previousBatch)).isTrue();

    holdPermit.countDown();
    executor.shutdown();
  }

  @Test
  public void enqueue_afterShutdown_returnsFalseAndCallsOnDropped() {
    AutoEmbeddingExecutor executor = createExecutor(1, Optional.of(1));
    executor.shutdown();

    AtomicBoolean dropped = new AtomicBoolean(false);
    boolean accepted =
        executor.enqueue(() -> {}, embeddingContextWithOnDropped(() -> dropped.set(true)));

    assertThat(accepted).isFalse();
    assertThat(dropped.get()).isTrue();
  }

  @Test
  public void getEmbeddingAvailablePermits_withoutCap_isEmpty() {
    AutoEmbeddingExecutor executor = createExecutor(1, Optional.empty());

    assertThat(executor.getEmbeddingAvailablePermits()).isEmpty();

    executor.shutdown();
  }

  @Test
  public void getEmbeddingAvailablePermits_withCap_reflectsInitialPermits() {
    AutoEmbeddingExecutor executor = createExecutor(1, Optional.of(3));

    assertThat(executor.getEmbeddingAvailablePermits()).hasValue(3);

    executor.shutdown();
  }

  // Helpers

  /**
   * Enqueues an auto-embedding submission whose context carries a previous batch, and returns a
   * weak reference to that batch. The batch is only ever strongly held by this method's frame, so
   * once it returns the batch is reachable exactly when the executor retained it.
   */
  private static WeakReference<ChangeStreamBatch> enqueueCarryingPreviousBatch(
      AutoEmbeddingExecutor executor) {
    ChangeStreamBatch previousBatch =
        new ChangeStreamBatch(
            Collections.emptyList(), new BsonDocument(), Optional.of(new BsonTimestamp()));

    executor.enqueue(
        () -> {},
        new EnqueueContext(
            mockAutoEmbeddingVectorDefinition(new ObjectId()),
            Optional.of(previousBatch),
            () -> {}));

    return new WeakReference<>(previousBatch);
  }

  /** Polls, prompting collection, until {@code ref} clears or the attempts are exhausted. */
  private static boolean awaitCollected(WeakReference<?> ref) throws InterruptedException {
    for (int attempt = 0; attempt < 50; attempt++) {
      if (ref.get() == null) {
        return true;
      }
      System.gc();
      Thread.sleep(20);
    }
    return ref.get() == null;
  }
}
