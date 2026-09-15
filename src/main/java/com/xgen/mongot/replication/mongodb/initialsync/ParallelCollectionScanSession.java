package com.xgen.mongot.replication.mongodb.initialsync;

import com.google.common.annotations.VisibleForTesting;
import com.xgen.mongot.logging.DefaultKeyValueLogger;
import com.xgen.mongot.metrics.MetricsFactory;
import com.xgen.mongot.replication.mongodb.common.InitialSyncException;
import io.micrometer.core.instrument.MeterRegistry;
import java.io.Closeable;
import java.time.Clock;
import java.time.Duration;
import java.util.HashMap;

/**
 * One parallel natural-order collection scan, from the plan that partitioned the collection through
 * however many windows it takes to consume every partition.
 *
 * <p>Holds what outlives a single window: the session's {@link ParallelCollectionScanResumeState},
 * the thread pool sized to its partitions, and the {@link ParallelCollectionScanDispatcher} that
 * fans a window out across them.
 *
 * <p>The manager drives this a window at a time, applying change stream events in between, and
 * closes it once the scan is done.
 */
class ParallelCollectionScanSession implements Closeable {

  private static final String POOL_NAME_PREFIX = "parallelCollectionScan";

  private final DefaultKeyValueLogger logger;
  private final ParallelCollectionScanResumeState resumeState;
  private final ParallelCollectionScanDispatcher dispatcher;

  @VisibleForTesting
  ParallelCollectionScanSession(
      InitialSyncContext context,
      ParallelCollectionScanResumeState resumeState,
      ParallelCollectionScanDispatcher dispatcher) {
    HashMap<String, Object> defaultKeyValues = new HashMap<>();
    defaultKeyValues.put("indexId", context.getIndexId());
    defaultKeyValues.put("generationId", context.getGenerationId());
    this.logger =
        DefaultKeyValueLogger.getLogger(ParallelCollectionScanSession.class, defaultKeyValues);

    this.resumeState = resumeState;
    this.dispatcher = dispatcher;

    this.logger
        .atInfo()
        .addKeyValue("numPartitions", resumeState.getPartitions().size())
        .addKeyValue("initialSyncId", resumeState.getInitialSyncId())
        .addKeyValue("highWaterMark", resumeState.getHighWaterMark())
        .log("Beginning a parallel collection scan.");
  }

  /**
   * Assembles a session for the given state, whether it was just planned or rebuilt from a commit
   * blob, and makes that state the index's durable position.
   */
  static ParallelCollectionScanSession create(
      Clock clock,
      InitialSyncContext context,
      InitialSyncMongoClient mongoClient,
      ParallelCollectionScanResumeState resumeState,
      Duration collectionScanTime,
      MetricsFactory metricsFactory,
      MeterRegistry meterRegistry) {

    CollectionScanTaskExecutor taskExecutor =
        new PartitionPoolExecutor(
            String.format("%s-%s", POOL_NAME_PREFIX, context.uniqueString()),
            resumeState.getPartitions().size(),
            meterRegistry);

    ParallelCollectionScanDispatcher dispatcher =
        new ParallelCollectionScanDispatcher(
            clock,
            context,
            collectionScanTime,
            scannerFactory(clock, context, mongoClient, resumeState, metricsFactory),
            taskExecutor);

    context.indexer.updateCommitUserDataSupplier(resumeState::snapshot);

    return new ParallelCollectionScanSession(context, resumeState, dispatcher);
  }

  @VisibleForTesting
  static ParallelCollectionScannerFactory scannerFactory(
      Clock clock,
      InitialSyncContext context,
      InitialSyncMongoClient mongoClient,
      ParallelCollectionScanResumeState resumeState,
      MetricsFactory metricsFactory) {
    return (highWaterMark, partitionState, abortRequested) ->
        new ParallelCollectionScanner(
            clock,
            context.withProgress(highWaterMark),
            mongoClient,
            partitionState,
            resumeState.getInitialSyncId(),
            abortRequested,
            metricsFactory);
  }

  ParallelCollectionScanResumeState getResumeState() {
    return this.resumeState;
  }

  /**
   * Scans every unfinished partition for one window, returning whether the collection still has
   * range left to scan.
   */
  boolean scanWindow() throws InitialSyncException {
    return this.dispatcher.dispatch(
        this.resumeState.getHighWaterMark(), this.resumeState.getPartitions());
  }

  /** Asks the running window to stop. Callable from another thread. */
  void signalShutdown() {
    this.dispatcher.signalShutdown();
  }

  /** Releases the partition pool. No further window can be scanned afterwards. */
  @Override
  public void close() {
    this.dispatcher.close();
  }
}
