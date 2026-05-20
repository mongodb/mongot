package com.xgen.mongot.replication.mongodb.common;

import static com.xgen.mongot.replication.mongodb.common.CommonReplicationConfig.Type.DEFAULT;

import com.google.common.annotations.VisibleForTesting;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import com.xgen.mongot.index.IndexMetricsUpdater.ReplicationMetricsUpdater;
import com.xgen.mongot.index.version.GenerationId;
import com.xgen.mongot.metrics.MetricsFactory;
import com.xgen.mongot.metrics.ServerStatusDataExtractor;
import com.xgen.mongot.metrics.ThreadPoolResourceMetrics;
import com.xgen.mongot.replication.mongodb.common.SchedulerQueue.Priority;
import com.xgen.mongot.util.Crash;
import com.xgen.mongot.util.concurrent.Executors;
import com.xgen.mongot.util.concurrent.NamedExecutorService;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import org.bson.RawBsonDocument;
import org.bson.types.ObjectId;
import org.jetbrains.annotations.TestOnly;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The decoding work scheduler accepts batch decoding work and schedules it to be completed on an
 * executor.
 */
public class DecodingWorkScheduler extends Thread {

  private static final Logger LOG = LoggerFactory.getLogger(DecodingWorkScheduler.class);

  private final NamedExecutorService executor;

  // Controls how many batches the scheduler will schedule in it's executor at a time. This
  // limit allows the scheduler to more readily respond to incoming batches, that may have
  // higher priority than batches in it's current queues.
  private final Semaphore concurrentDecodingBatches;

  private final CompletableFuture<Void> shutdownFuture;

  private final SchedulerQueue<DecodingSchedulerBatch> schedulerQueue;

  private final Counter enqueueCounter;

  private final Counter dequeueCounter;

  private final DistributionSummary batchEventDistribution;

  private final Timer batchTimer;

  private final Timer batchSchedulingTimer;

  @GuardedBy("this")
  private boolean shutdown;

  DecodingWorkScheduler(
      int numConcurrentDecoding,
      CommonReplicationConfig.Type type,
      NamedExecutorService executor,
      MetricsFactory metricsFactory) {
    super("DecodingWorkScheduler");
    this.executor = executor;
    this.concurrentDecodingBatches = new Semaphore(numConcurrentDecoding);

    this.shutdownFuture = new CompletableFuture<>();
    this.shutdown = false;
    
    Tag replicationTag = ServerStatusDataExtractor.Scope.REPLICATION.getTag();

    this.schedulerQueue = new SchedulerQueue<>(type, metricsFactory);

    this.enqueueCounter = metricsFactory.counter("enqueueCalls");
    this.dequeueCounter = metricsFactory.counter("dequeueCalls");
    /* Tracks the number of documents in each batch */
    this.batchEventDistribution =
        metricsFactory.summary("decodingBatchDistribution", Tags.of(replicationTag));
    // TODO(CLOUDP-289914): Remove this batchTimer after switching to new one by IndexMetricsUpdater
    /* Tracks duration of decoding a batch */
    this.batchTimer = metricsFactory.timer("decodingBatchDurations", Tags.of(replicationTag));
    /* Tracks duration that a batch is queued for before it's decoded */
    this.batchSchedulingTimer =
        metricsFactory.timer("decodingBatchSchedulingDurations", Tags.of(replicationTag));
  }

  // TODO(CLOUDP-405327): remove test-only overloads once LIFECYCLE_ATTRIBUTION_METRICS rolls out.
  public static DecodingWorkScheduler create(int numDecodingThreads, MeterRegistry registry) {
    return create(numDecodingThreads, DEFAULT, registry, false);
  }

  public static DecodingWorkScheduler create(
      int numDecodingThreads, CommonReplicationConfig.Type type, MeterRegistry registry) {
    return create(numDecodingThreads, type, registry, false);
  }

  /**
   * Creates and starts a new DecodingWorkScheduler.
   *
   * @return an DecodingWorkScheduler with metricsNamespacePrefix added to the beginning of all
   *     metrics.
   */
  public static DecodingWorkScheduler create(
      int numDecodingThreads,
      CommonReplicationConfig.Type type,
      MeterRegistry registry,
      boolean enableLifecycleAttributionMetrics) {
    var executor =
        Executors.fixedSizeThreadPool(
            type.metricsNamespacePrefix + "decoding", numDecodingThreads, registry);
    if (enableLifecycleAttributionMetrics) {
      ThreadPoolResourceMetrics.create(type.resourceAttributionSubsystem)
          .register(executor, registry);
    }
    var scheduler =
        new DecodingWorkScheduler(
            numDecodingThreads,
            type,
            executor,
            new MetricsFactory("decodingWorkScheduler", registry));
    scheduler.start();
    return scheduler;
  }

  @Override
  public void run() {
    while (true) {
      DecodingSchedulerBatch batch;

      try {
        this.concurrentDecodingBatches.acquire();

        batch = this.schedulerQueue.remove();
        this.dequeueCounter.increment();
        this.batchSchedulingTimer.record(batch.elapsed());
      } catch (InterruptedException ex) {
        Executors.shutdownOrFail(this.executor);
        this.shutdownFuture.complete(null);
        return;
      }

      Timer.Sample sample = Timer.start();
      CompletableFuture.runAsync(() -> batch.decoder.decode(batch.events), this.executor)
          .whenComplete((result, throwable) -> this.concurrentDecodingBatches.release())
          .whenComplete(
              (result, throwable) -> {
                long durationNs = sample.stop(this.batchTimer);
                batch
                    .replicationMetricsUpdater
                    .getSteadyStateMetrics()
                    .getBatchDecodingTimer()
                    .record(durationNs, TimeUnit.NANOSECONDS);
              })
          .thenRunAsync(finalize(batch), this.executor)
          .exceptionally(
              throwable -> {
                LOG.atError()
                    .addKeyValue("size", batch.size())
                    .addKeyValue("priority", batch.priority)
                    .addKeyValue("indexId", batch.generationId.indexId)
                    .addKeyValue("generationId", batch.generationId)
                    .addKeyValue("sequenceNumber", batch.sequenceNumber)
                    .log("Failed to process a scheduler batch");
                batch.future.completeExceptionally(throwable);
                return null;
              });
    }
  }

  private Runnable finalize(DecodingSchedulerBatch batch) {
    return () -> {
      this.schedulerQueue.finalizeBatch(batch);
      this.batchEventDistribution.record(batch.events.size());
      batch.future.complete(null);
    };
  }

  /**
   * Shuts down the DecodingWorkScheduler, asserting that no work is left in the scheduler first. We
   * shut down the DecodingWorkScheduler after all managers that submit work to it have been shut
   * down, so we expect that the queue is drained and expect no more work to be scheduled. The run
   * loop, if waiting, will be awoken by a signal, see the shutdown, and return.
   */
  public synchronized CompletableFuture<Void> shutdown() {
    this.shutdown = true;

    if (!this.schedulerQueue.isEmpty()) {
      Crash.because("DecodingWorkScheduler was shut down while it still has outstanding batches")
          .now();
    }

    this.interrupt();

    return this.shutdownFuture;
  }

  /**
   * Cancels any batches for the given GenerationId that have not yet been dispatched to the
   * executor.
   *
   * <p>Returns a future that completes when all unscheduled work is cancelled and all in-progress
   * work is completed. If there was any in-progress work, this future will complete with the result
   * of that work or runtime exception.
   *
   * <p>This does not prevent further work from being scheduled for this generationId.
   */
  public CompletableFuture<Void> cancel(
      GenerationId generationId, Optional<ObjectId> attemptId, Throwable reason) {
    return this.schedulerQueue.cancel(generationId, attemptId, reason);
  }

  /**
   * Schedules a batch of {@link org.bson.RawBsonDocument} for decoding, with priority based on the
   * given priority.
   *
   * <p>The returned future may complete exceptionally.
   */
  public synchronized CompletableFuture<Void> schedule(
      GenerationId generationId,
      Optional<ObjectId> attemptId,
      List<RawBsonDocument> batch,
      Priority priority,
      DocumentBatchDecoder decoder,
      ReplicationMetricsUpdater replicationMetricsUpdater) {

    if (this.shutdown) {
      Crash.because("cannot schedule work on the DecodingWorkScheduler after it has been shut down")
          .now();
    }

    CompletableFuture<Void> future = new CompletableFuture<>();

    this.schedulerQueue.add(
        new DecodingSchedulerBatch(
            batch, priority, decoder, future, generationId, attemptId, replicationMetricsUpdater));
    this.enqueueCounter.increment();

    return future;
  }

  @TestOnly
  @VisibleForTesting
  public SchedulerQueue<DecodingSchedulerBatch> getSchedulerQueue() {
    return this.schedulerQueue;
  }
}
