package com.xgen.mongot.replication.mongodb.common;

import com.google.common.annotations.VisibleForTesting;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import com.xgen.mongot.embedding.exceptions.MaterializedViewNonTransientException;
import com.xgen.mongot.embedding.exceptions.MaterializedViewTransientException;
import com.xgen.mongot.index.DocumentEvent;
import com.xgen.mongot.index.ExceededLimitsException;
import com.xgen.mongot.index.FieldExceededLimitsException;
import com.xgen.mongot.index.IndexMetricsUpdater.IndexingMetricsUpdater;
import com.xgen.mongot.index.version.GenerationId;
import com.xgen.mongot.metrics.MetricsFactory;
import com.xgen.mongot.metrics.ServerStatusDataExtractor;
import com.xgen.mongot.metrics.ServerStatusDataExtractor.ReplicationMeterData.IndexingMeterData;
import com.xgen.mongot.replication.mongodb.common.IndexingWorkSchedulerFactory.IndexingStrategy;
import com.xgen.mongot.replication.mongodb.common.SchedulerQueue.Priority;
import com.xgen.mongot.util.Crash;
import com.xgen.mongot.util.FutureUtils;
import com.xgen.mongot.util.concurrent.Executors;
import com.xgen.mongot.util.concurrent.NamedExecutorService;
import com.xgen.mongot.util.functionalinterfaces.CheckedRunnable;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import org.bson.types.ObjectId;
import org.jetbrains.annotations.TestOnly;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstract base class for indexing work schedulers.
 *
 * <p>This class is designed to manage and schedule batches of indexing tasks, ensuring that only a
 * limited number of batches are processed concurrently. Subclasses must implement the {@link
 * #getBatchTasksFuture(IndexingSchedulerBatch)} method to define the specific indexing task for
 * each indexing strategy.
 */
public abstract class IndexingWorkScheduler extends Thread {

  private static final Logger LOG = LoggerFactory.getLogger(IndexingWorkScheduler.class);

  private static final int NUM_CONCURRENT_INDEXING_BATCHES = 2;

  final NamedExecutorService executor;

  private final CompletableFuture<Void> shutdownFuture;

  // Controls how many batches the scheduler will schedule in its executor at a time. This
  // limit allows the scheduler to more readily respond to incoming batches, that may have
  // higher priority than batches in its current queues.
  private final Semaphore concurrentIndexingBatches;

  private final SchedulerQueue<IndexingSchedulerBatch> schedulerQueue;

  private final Counter enqueueCounter;

  private final Counter dequeueCounter;

  private final DistributionSummary batchEventDistribution;

  final Timer batchTimer;

  private final Timer batchSchedulingTimer;

  private final IndexingStrategy indexingStrategy;

  @GuardedBy("this")
  private boolean shutdown;

  protected IndexingWorkScheduler(
      NamedExecutorService executor, IndexingStrategy indexingStrategy) {
    super(indexingStrategy.getThreadName());
    this.executor = executor;
    this.indexingStrategy = indexingStrategy;
    this.concurrentIndexingBatches = new Semaphore(NUM_CONCURRENT_INDEXING_BATCHES);
    this.shutdownFuture = new CompletableFuture<>();
    this.shutdown = false;

    // TODO(CLOUDP-319970): differentiate metrics for each strategy.
    MetricsFactory metricsFactory =
        new MetricsFactory(
            "indexingWorkScheduler",
            executor.getMeterRegistry());
    var replicationTag = ServerStatusDataExtractor.Scope.REPLICATION.getTag();

    this.schedulerQueue = new SchedulerQueue<>(metricsFactory);
    this.enqueueCounter = metricsFactory.counter("enqueueCalls");
    this.dequeueCounter = metricsFactory.counter("dequeueCalls");

    // ServerStatusDataExtractor enforces that there are no duplicate meters. The following meters
    // may have been created by another IndexingWorkScheduler and should therefore be reused.

    /* Tracks the number of documents in each batch */
    this.batchEventDistribution =
        metricsFactory.summary(
            IndexingMeterData.INDEXING_BATCH_DISTRIBUTION, Tags.of(replicationTag));
    // TODO(CLOUDP-289914): Remove this batchTimer after switching to new one by IndexMetricsUpdater
    /* Tracks duration of indexing a batch. This includes the time it takes to generate embeddings
    for auto-embedding indexes */
    this.batchTimer =
        metricsFactory.timer(IndexingMeterData.INDEXING_BATCH_DURATIONS, Tags.of(replicationTag));
    /* Tracks duration that a batch is queued for before it's indexed */
    this.batchSchedulingTimer =
        metricsFactory.timer(
            IndexingMeterData.INDEXING_BATCH_SCHEDULING_DURATIONS, Tags.of(replicationTag));
  }

  /**
   * Defines the indexing work to be done for each batch according to the scheduler's specific
   * indexing strategy.
   *
   * <p>This method is invoked within the scheduler's run loop and returns a {@link
   * CompletableFuture} that represents the completion of all indexing tasks for the given {@link
   * IndexingSchedulerBatch}. Each subclass must implement this method to define how batches are
   * processed according to their indexing strategy.
   *
   * @param batch the batch of indexing tasks to be processed
   * @return a {@link CompletableFuture} that completes when all tasks in the batch are finished
   */
  abstract CompletableFuture<Void> getBatchTasksFuture(IndexingSchedulerBatch batch);

  /**
   * Handles exceptions that occur during the indexing process for a given batch.
   *
   * <p>This method is invoked within the scheduler's run loop when an exception is thrown while
   * processing an indexing batch. Each subclass must implement this method to define how exceptions
   * are handled according to their indexing strategy.
   *
   * @param batch the batch of indexing tasks that was processed.
   * @param throwable the throwable that occurred during processing.
   */
  abstract void handleBatchException(IndexingSchedulerBatch batch, Throwable throwable);

  /**
   * Decides whether we should commit when finalizing a batch. This is useful for auto-embedding mat
   * view collections, where we want to commit after every batch instead of periodically like with
   * Lucene indexes.
   *
   * @return true if we should commit, false otherwise. Defaults to false.
   */
  protected boolean shouldCommitOnFinalize() {
    return false;
  }

  @Override
  public void run() {
    while (true) {
      IndexingSchedulerBatch batch;
      try {
        this.concurrentIndexingBatches.acquire();

        batch = this.schedulerQueue.remove();
        this.dequeueCounter.increment();
        this.batchSchedulingTimer.record(batch.elapsed());
      } catch (InterruptedException ex) {
        Executors.shutdownOrFail(this.executor);
        this.shutdownFuture.complete(null);
        return;
      }

      Timer.Sample sample = Timer.start();
      // complete the batch's future when the tasks finish running.
      getBatchTasksFuture(batch)
          .whenComplete((result, throwable) -> this.concurrentIndexingBatches.release())
          .whenComplete(
              (result, throwable) -> {
                long durationNs = sample.stop(this.batchTimer);
                batch
                    .indexingMetricsUpdater
                    .getBatchIndexingTimer()
                    .record(durationNs, TimeUnit.NANOSECONDS);
              })
          .thenComposeAsync(
              (ignoredNull) -> {
                // since ExceededLimitsException is a checked exception we can't throw it in here,
                // instead we use compose to replace the current stage with a failed future. Or a
                // successful one if we did not exceed limits.
                return failedFutureIfExceededLimits(batch);
              },
              this.executor)
          .thenRunAsync(finalize(batch), this.executor)
          .exceptionally(
              throwable -> {
                LOG.atError()
                    .addKeyValue("size", batch.size())
                    .addKeyValue("priority", batch.priority)
                    .addKeyValue("indexId", batch.generationId.indexId)
                    .addKeyValue("generationId", batch.generationId)
                    .addKeyValue(
                        "commitUserData", batch.commitUserData.map(e -> e.toBson().toString()))
                    .addKeyValue("sequenceNumber", batch.sequenceNumber)
                    .addKeyValue("indexingStrategy", this.indexingStrategy.name())
                    .log("Failed to process a scheduler batch");
                handleBatchException(batch, throwable);
                return null;
              });
    }
  }

  protected CompletableFuture<Void> failedFutureIfExceededLimits(IndexingSchedulerBatch batch) {
    return batch
        .indexer
        .exceededLimits()
        .<CompletableFuture<Void>>map(CompletableFuture::failedFuture)
        .orElse(FutureUtils.COMPLETED_FUTURE);
  }

  private Runnable finalize(IndexingSchedulerBatch batch) {
    return () -> {
      this.schedulerQueue.finalizeBatch(batch);
      if (batch.commitUserData.isPresent()) {
        batch.indexer.updateCommitUserData(batch.commitUserData.get());
        if (shouldCommitOnFinalize()) {
          try {
            batch.indexer.commit();
          } catch (MaterializedViewNonTransientException unrecoverable) {
            LOG.atError()
                .addKeyValue("indexId", batch.generationId.indexId)
                .addKeyValue("generationId", batch.generationId)
                .addKeyValue("indexingStrategy", this.indexingStrategy.name())
                .setCause(unrecoverable)
                .log("Failed the index due to unrecoverable error.");
            // rethrow MaterializedViewNonTransientException, this should fail the index.
            throw unrecoverable;
          } catch (Exception e) {
            LOG.atError()
                .addKeyValue("indexId", batch.generationId.indexId)
                .addKeyValue("generationId", batch.generationId)
                .addKeyValue("indexingStrategy", this.indexingStrategy.name())
                .setCause(e)
                .log("Failed to commit index");
            throw new MaterializedViewTransientException("Failed to commit index");
          }
        }
      }
      this.batchEventDistribution.record(batch.events.size());
      batch.future.complete(null);
    };
  }

  /**
   * Shuts down the IndexingWorkScheduler, asserting that no work is left in the scheduler first. We
   * shut down the IndexingWorkScheduler after all managers that submit work to it have been shut
   * down, so we expect that the queue is drained and expect no more work to be scheduled. The run
   * loop, if waiting, will be awoken by a signal, see the shutdown, and return.
   */
  public CompletableFuture<Void> shutdown() {
    synchronized (this) { // https://github.com/mockito/mockito/issues/2970
      this.shutdown = true;

      if (!this.schedulerQueue.isEmpty()) {
        String message =
            String.format(
                "%s was shut down while it still has outstanding batches",
                this.indexingStrategy.getThreadName());
        Crash.because(message).withThreadDump().now();
      }

      this.interrupt();

      return this.shutdownFuture;
    }
  }

  /**
   * Cancels any batches for the given GenerationId that have not yet been dispatched to the
   * executor.
   *
   * <p>Returns a future that completes when all unscheduled work is cancelled and all in-progress
   * work is completed. If there was any in-progress work, this future will complete with the result
   * of that work (success unless there is a {@link ExceededLimitsException} or runtime exception)
   *
   * <p>This does not prevent further work from being scheduled for this generationId.
   */
  public CompletableFuture<Void> cancel(
      GenerationId generationId, Optional<ObjectId> attemptId, Throwable reason) {
    return this.schedulerQueue.cancel(generationId, attemptId, reason);
  }

  /**
   * Schedules a batch of {@link DocumentEvent} to be indexed, with priority based on the given
   * priority.
   *
   * <p>If {@code commitUserData} is non-empty, {@code indexer::updateCommitUserData} will be
   * triggered after indexing.
   *
   * <p>The returned future may complete exceptionally with {@link ExceededLimitsException} or
   * runtime exceptions that happened during indexing or committing.
   */
  public CompletableFuture<Void> schedule(
      List<DocumentEvent> batch,
      Priority priority,
      DocumentIndexer indexer,
      GenerationId generationId,
      Optional<ObjectId> attemptId,
      Optional<IndexCommitUserData> commitUserData,
      IndexingMetricsUpdater indexingMetricsUpdater) {
    synchronized (this) { // https://github.com/mockito/mockito/issues/2970
      if (this.shutdown) {
        String message =
            String.format(
                "cannot schedule work on the %s after it has been shut down",
                this.indexingStrategy.getThreadName());
        Crash.because(message).now();
      }

      CompletableFuture<Void> future = new CompletableFuture<>();

      this.schedulerQueue.add(
          new IndexingSchedulerBatch(
              batch,
              priority,
              indexer,
              future,
              generationId,
              attemptId,
              commitUserData,
              indexingMetricsUpdater));
      this.enqueueCounter.increment();

      return future;
    }
  }

  @TestOnly
  @VisibleForTesting
  public double getEnqueueCount() {
    return this.enqueueCounter.count();
  }

  @TestOnly
  @VisibleForTesting
  public boolean schedulerQueueContains(GenerationId generationId) {
    return this.schedulerQueue.contains(generationId);
  }

  protected static class IndexingTask implements CheckedRunnable<FieldExceededLimitsException> {

    private final DocumentEvent event;
    private final DocumentIndexer indexer;

    IndexingTask(DocumentIndexer indexer, DocumentEvent event) {
      this.event = event;
      this.indexer = indexer;
    }

    @Override
    public void run() throws FieldExceededLimitsException {
      this.indexer.indexDocumentEvent(this.event);
    }
  }
}
