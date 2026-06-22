package com.xgen.mongot.replication.mongodb.common;

import static com.xgen.mongot.util.Check.checkState;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Stopwatch;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import com.xgen.mongot.index.version.GenerationId;
import com.xgen.mongot.metrics.MetricsFactory;
import io.micrometer.core.instrument.Tags;
import java.time.Duration;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.WeakHashMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import org.bson.types.ObjectId;
import org.jetbrains.annotations.TestOnly;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This queue accepts {@link SchedulerBatch} of indexing work to be processed. It organizes the
 * batches by index in {@link IndexBatches} objects. Removing from the queue removes the {@link
 * IndexBatches} object with the least recently enqueued batch with the highest priority from the
 * batchQueue, returning that batch.
 *
 * <p>Once an {@link IndexBatches} has been removed from the queue, it will not be placed back in
 * the batchQueue again until finalizeBatch is called for the first batch in that object. This
 * indicates that that batch has been processed, can be removed from the {@link IndexBatches}
 * object, and that {@link IndexBatches} can be re-added to the batchQueue if it still contains
 * batches, so it's next batch can be available for removal.
 *
 * <p>The queue uses a map of {@link GenerationId} to {@link IndexBatches} and removes those {@link
 * IndexBatches} from the map when they are empty. This means that the queue has no work when the
 * indexBatchesMap is empty.
 *
 * <p>If a batch fails to be processed, cancel should be called so further work for that index can
 * be stopped by removing the {@link IndexBatches} from the map and not re-adding it to the
 * batchQueue.
 */
public class SchedulerQueue<T extends SchedulerQueue.SchedulerBatch> {

  static final AtomicLong batchCounter = new AtomicLong(0);
  private static final Logger LOG = LoggerFactory.getLogger(SchedulerQueue.class);

  // Priority of a batch.  Order of priority is:
  // INITIAL_SYNC_CHANGE_STREAM > STEADY_STATE_CHANGE_STREAM > INITIAL_SYNC_COLLECTION_SCAN
  public enum Priority {
    INITIAL_SYNC_CHANGE_STREAM(0),
    STEADY_STATE_CHANGE_STREAM(1),
    INITIAL_SYNC_COLLECTION_SCAN(2);

    private final int value;

    Priority(int value) {
      this.value = value;
    }

    public int getValue() {
      return this.value;
    }
  }

  /**
   * Represents a batch that has been scheduled to be indexed. It will be removed from this Queue in
   * priority first, then FIFO order, behind any other batches for the same index. Each batch has a
   * sequenceNumber that is used to determine the order they came in and a priority. Batches with
   * higher priority will be removed before batches with lower priority.
   */
  abstract static class SchedulerBatch {
    public final Priority priority;
    public final CompletableFuture<Void> future;
    public final GenerationId generationId;
    public final Optional<ObjectId> attemptId;
    public final long sequenceNumber;
    public final Stopwatch schedulingTimer;

    protected SchedulerBatch(
        Priority priority,
        CompletableFuture<Void> future,
        GenerationId generationId,
        Optional<ObjectId> attemptId) {
      this.future = future;
      this.generationId = generationId;
      this.priority = priority;
      this.attemptId = attemptId;
      this.sequenceNumber = batchCounter.getAndIncrement();
      this.schedulingTimer = Stopwatch.createStarted();
    }

    public Duration elapsed() {
      if (this.schedulingTimer.isRunning()) {
        this.schedulingTimer.stop();
      }
      return this.schedulingTimer.elapsed();
    }

    protected abstract int size();
  }

  private final PriorityBlockingQueue<IndexBatches<T>> batchQueue;

  @GuardedBy("this")
  private final Map<GenerationId, IndexBatches<T>> indexBatchesMap;

  /**
   * Cache of the in-flight batches. Used to guarantee idempotent calls to {@link #cancel}, making
   * sure we return the same in-flight batch Future even if cancellation happened multiple times
   * concurrently.
   */
  @GuardedBy("this")
  private final Map<GenerationId, T> inFlightBatchesMap;

  @GuardedBy("this")
  private final Map<ObjectId, Throwable> failedIndexingAttempts;

  private final AtomicLong queuedBatchesTotal;
  private final AtomicLong queuedEventsTotal;

  SchedulerQueue(CommonReplicationConfig.Type type, MetricsFactory metricsFactory) {
    this(new PriorityBlockingQueue<>(), type, metricsFactory);
  }

  SchedulerQueue(MetricsFactory metricsFactory) {
    this(new PriorityBlockingQueue<>(), CommonReplicationConfig.Type.DEFAULT, metricsFactory);
  }

  @VisibleForTesting
  SchedulerQueue(
      PriorityBlockingQueue<IndexBatches<T>> batchQueue,
      CommonReplicationConfig.Type type,
      MetricsFactory metricsFactory) {
    this.batchQueue = batchQueue;
    this.indexBatchesMap = new HashMap<>();
    this.inFlightBatchesMap = new HashMap<>();
    this.queuedBatchesTotal =
        metricsFactory.numGauge("queuedBatchesTotal", Tags.of("replicationType", type.name()));
    this.queuedEventsTotal =
        metricsFactory.numGauge("queuedEventsTotal", Tags.of("replicationType", type.name()));
    this.failedIndexingAttempts = new WeakHashMap<>();
  }

  /**
   * Adds a batch to the queue in it's corresponding {@link IndexBatches} object, behind any other
   * batches previously enqueued for this index.
   *
   * @param batch to be added to the queue
   */
  synchronized void add(T batch) {
    // prevent the batch from adding into the batchQueue if there's exception processing other
    // batches of the same index attempt
    if (batch.attemptId.isPresent()
        && this.failedIndexingAttempts.containsKey(batch.attemptId.get())) {
      Throwable throwable = this.failedIndexingAttempts.get(batch.attemptId.get());
      LOG.atWarn()
          .addKeyValue("generationId", batch.generationId)
          .addKeyValue("reason", throwable)
          .log("Skipping batch for scheduler queue for generation");
      batch.future.completeExceptionally(throwable);
      return;
    }

    // update metrics when batch is enqueued.
    this.queuedBatchesTotal.incrementAndGet();
    this.queuedEventsTotal.getAndAdd(batch.size());

    if (this.indexBatchesMap.containsKey(batch.generationId)) {
      this.indexBatchesMap.get(batch.generationId).add(batch);
    } else {
      IndexBatches<T> batches = new IndexBatches<>(batch);
      this.indexBatchesMap.put(batch.generationId, batches);
      this.batchQueue.offer(batches);
    }
  }

  /**
   * Removes and returns the next batch to be processed from the queue, blocking until one is
   * available. After that batch has been processed, either cancel or finalizeBatch should be
   * called, but not both.
   *
   * @return the next batch to be processed from the queue.
   * @throws InterruptedException if interrupted while waiting
   */
  T remove() throws InterruptedException {
    IndexBatches<T> nextBatches = this.batchQueue.take();
    synchronized (this) {
      if (nextBatches.next().isPresent()) {
        T batch = nextBatches.next().get();
        this.inFlightBatchesMap.put(batch.generationId, batch);
        batch.future.exceptionally(
            throwable -> {
              // schedule cleanup for the case when the batch might not be finalized
              this.clearInFlightBatch(batch.generationId, batch);
              return null;
            });
        return batch;
      }
    }
    // If the IndexBatches is empty, it has been cancelled, move to the next batch.
    return remove();
  }

  /**
   * Finalized a batch's processing, removing that batch front of it's index's {@link IndexBatches}
   * object and re-adding that {@link IndexBatches} back to the batchQueue if it has remaining
   * batches. This way, the next batch for this index can be eligible for scheduling.
   *
   * <p>If the {@link IndexBatches} is empty, we will remove it from our indexBatchesMap.
   *
   * <p>This should only be called when there is an outstanding batch for an index that has been
   * finished.
   *
   * @param batch to be finalized
   */
  synchronized void finalizeBatch(T batch) {
    // update metrics when batch is dequeued.
    this.queuedBatchesTotal.decrementAndGet();
    this.queuedEventsTotal.getAndAdd(-batch.size());

    GenerationId generationId = batch.generationId;
    this.inFlightBatchesMap.remove(generationId);

    if (!this.indexBatchesMap.containsKey(generationId)) {
      // batches for this index have been cancelled
      return;
    }

    IndexBatches<T> indexBatches = this.indexBatchesMap.get(generationId);

    checkState(
        !this.batchQueue.contains(indexBatches),
        "batchQueue already contains the index being finalized");

    checkState(
        indexBatches.next().isPresent() && batch.equals(indexBatches.next().get()),
        "cannot finalize a batch that was not running: %s, queue head batch: %s",
        batch,
        indexBatches.next().get());

    indexBatches.remove();

    if (!indexBatches.isEmpty()) {
      this.batchQueue.offer(indexBatches);
    } else {
      this.indexBatchesMap.remove(generationId);
    }
  }

  /**
   * Cancels further processing for this index, removing the {@link IndexBatches} from our map as
   * well as the batchQueue, if it is present. We also complete the futures for all batches that
   * have not been scheduled with the reason. Batches that have already been scheduled will still
   * complete.
   *
   * <p>Returns a future that completes once any already scheduled work finishes
   *
   * @param generationId of the index to cancel processing for
   * @param reason for the cancellation
   */
  synchronized CompletableFuture<Void> cancel(
      GenerationId generationId, Optional<ObjectId> attemptId, Throwable reason) {
    // Add the attempt id to failedIndexingAttempts
    attemptId.ifPresent(
        objectId -> {
          LOG.atWarn()
              .addKeyValue("generationId", generationId)
              .addKeyValue("reason", reason)
              .log("Adding batch attempt id to the map", reason);
          this.failedIndexingAttempts.put(objectId, reason);
        });

    LOG.atWarn()
        .addKeyValue("indexId", generationId.indexId)
        .addKeyValue("generationId", generationId)
        .log("cancelling queue batches for index");
    Optional<SchedulerBatch> inFlightBatch =
        Optional.ofNullable(this.inFlightBatchesMap.get(generationId));

    if (this.indexBatchesMap.containsKey(generationId)) {
      IndexBatches<T> indexBatches = this.indexBatchesMap.get(generationId);
      this.batchQueue.remove(indexBatches);

      if (inFlightBatch.isPresent() && !indexBatches.isEmpty()) {
        // remove the first batch which is in-flight, so we don't cancel it below
        indexBatches.remove();
      }
      // Update the metrics for all batches in indexBatches, excluding the in-flight batch
      this.updateMetricsForCancelBatches(indexBatches);
      indexBatches.cancel(reason);
      this.indexBatchesMap.remove(generationId);
    }

    // returned a cached in-flight batch future, if present. that will guarantee that all threads
    // which might be calling this method concurrently could wait on the same future
    return inFlightBatch.map(b -> b.future).orElse(CompletableFuture.completedFuture(null));
  }

  void updateMetricsForCancelBatches(IndexBatches<T> indexBatches) {
    synchronized (indexBatches) {
      indexBatches.batches.forEach(
          batch -> {
            this.queuedBatchesTotal.decrementAndGet();
            this.queuedEventsTotal.getAndAdd(-batch.size());
          });
    }
  }

  synchronized void clearInFlightBatch(GenerationId generationId, SchedulerBatch batch) {
    if (this.inFlightBatchesMap.get(generationId) == batch) {
      this.inFlightBatchesMap.remove(generationId);
    }
  }

  /** Returns true if the queue has no work in it. */
  synchronized boolean isEmpty() {
    return this.indexBatchesMap.isEmpty();
  }

  /** Returns true if the queue contains a batch for the given generationId. */
  @TestOnly
  @VisibleForTesting
  synchronized boolean contains(GenerationId generationId) {
    return this.indexBatchesMap.containsKey(generationId);
  }

  @TestOnly
  @VisibleForTesting
  public synchronized int getFailedIndexingAttemptsSize() {
    return this.failedIndexingAttempts.size();
  }

  /**
   * {@link IndexBatches} is a queue that represents batches for a single index in FIFO order. It is
   * synchronized and comparable with other {@link IndexBatches} based on the sequence number of the
   * first {@link SchedulerBatch} in the queue and the priority of the {@link IndexBatches}. An
   * {@link IndexBatches} will only ever hold batches for an index of the same priority, as one
   * indexGeneration is never in multiple priority states at once.
   */
  static class IndexBatches<T extends SchedulerBatch> implements Comparable<IndexBatches<T>> {

    private volatile long currentSequence;
    private final Priority priority;

    @GuardedBy("this")
    private final Queue<T> batches = new LinkedList<>();

    IndexBatches(T batch) {
      this.batches.add(batch);
      this.priority = batch.priority;
      this.currentSequence = batch.sequenceNumber;
    }

    synchronized void add(T batch) {
      checkState(batch.priority == this.priority, "batch has mismatched priority");
      checkState(
          batch.sequenceNumber > this.currentSequence,
          "batch cannot be added to IndexBatches with a lower sequence number than already added.");
      this.batches.add(batch);
      if (this.batches.size() == 1) {
        this.currentSequence = batch.sequenceNumber;
      }
    }

    synchronized T remove() {
      checkState(!this.batches.isEmpty(), "cannot remove from empty batches");

      T batch = this.batches.remove();
      if (!this.batches.isEmpty()) {
        this.currentSequence = this.batches.peek().sequenceNumber;
      }
      return batch;
    }

    public synchronized Optional<T> next() {
      return this.batches.isEmpty() ? Optional.empty() : Optional.of(this.batches.peek());
    }

    @Override
    public int compareTo(IndexBatches<T> other) {

      // Should only be called from our batchQueue, which
      // should never have empty IndexBatches
      checkState(
          !this.isEmpty() && !other.isEmpty(), "Cannot compare IndexBatches with an empty queue.");

      if (this.priority != other.priority) {
        return Integer.compare(this.priority.getValue(), other.priority.getValue());
      }

      return Long.compare(this.currentSequence, other.currentSequence);
    }

    // only used to be synchronized on the assertion in compareTo
    synchronized boolean isEmpty() {
      return this.batches.isEmpty();
    }

    synchronized void cancel(Throwable reason) {
      while (!this.batches.isEmpty()) {
        this.batches.remove().future.completeExceptionally(reason);
      }
    }
  }
}
