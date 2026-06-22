package com.xgen.mongot.replication.mongodb.initialsync;

import static com.xgen.mongot.index.definition.MaterializedViewIndexDefinitionGeneration.isMaterializedViewBasedIndex;
import static com.xgen.mongot.replication.mongodb.initialsync.InitialSyncManager.awaitShutdown;
import static com.xgen.mongot.replication.mongodb.initialsync.InitialSyncManager.getResultOrThrow;
import static com.xgen.mongot.replication.mongodb.initialsync.InitialSyncManager.supplyAsync;
import static com.xgen.mongot.util.Check.checkState;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Stopwatch;
import com.google.errorprone.annotations.Var;
import com.mongodb.MongoCommandException;
import com.mongodb.MongoNamespace;
import com.xgen.mongot.embedding.config.MaterializedViewCollectionMetadataCatalog;
import com.xgen.mongot.logging.DefaultKeyValueLogger;
import com.xgen.mongot.metrics.MetricsFactory;
import com.xgen.mongot.replication.mongodb.common.ChangeStreamResumeInfo;
import com.xgen.mongot.replication.mongodb.common.IndexCommitUserData;
import com.xgen.mongot.replication.mongodb.common.InitialSyncException;
import com.xgen.mongot.replication.mongodb.common.InitialSyncResumeInfo;
import com.xgen.mongot.util.BsonUtils;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.Crash;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import java.time.Clock;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.bson.BsonDocument;
import org.bson.BsonTimestamp;
import org.bson.BsonValue;

public class BufferlessInitialSyncManager implements InitialSyncManager {

  private final DefaultKeyValueLogger logger;
  private final InitialSyncContext context;
  private final BufferlessCollectionScannerFactory collectionScannerFactory;
  private final BufferlessChangeStreamApplierFactory changeStreamApplierFactory;
  private final ServerClusterTimeProvider clusterTimeProvider;
  private final Duration collectionScanTime;
  private final Optional<InitialSyncResumeInfo> resumeInfo;

  private static final long _100_MB = 100L * 1024 * 1024;
  private static final long _1_GB = 1024L * 1024 * 1024;

  /* Tracks the duration of a collection scan */
  private final Timer collectionScanTimer;
  /* Tracks the duration of applying change stream events */
  private final Timer changeStreamTimer;
  private final InitialSyncMongoClient mongoClient;
  private final Timer fsyncSuccessDurationTimer;
  private final Timer fsyncFailureDurationTimer;
  private final MetricsFactory metricsFactory;

  @VisibleForTesting
  BufferlessInitialSyncManager(
      InitialSyncContext context,
      BufferlessCollectionScannerFactory collectionScannerFactory,
      BufferlessChangeStreamApplierFactory changeStreamApplierFactory,
      ServerClusterTimeProvider clusterTimeProvider,
      Duration collectionScanTime,
      Optional<InitialSyncResumeInfo> resumeInfo,
      MetricsFactory metricsFactory,
      InitialSyncMongoClient mongoClient) {
    HashMap<String, Object> defaultKeyValues = new HashMap<>();
    defaultKeyValues.put("indexId", context.getIndexId());
    defaultKeyValues.put("generationId", context.getGenerationId());
    this.logger =
        DefaultKeyValueLogger.getLogger(BufferlessInitialSyncManager.class, defaultKeyValues);

    this.context = context;
    this.collectionScannerFactory = collectionScannerFactory;
    this.changeStreamApplierFactory = changeStreamApplierFactory;
    this.clusterTimeProvider = clusterTimeProvider;
    this.collectionScanTime = collectionScanTime;
    this.resumeInfo = resumeInfo;
    this.collectionScanTimer = metricsFactory.timer("collectionScanTime");
    this.changeStreamTimer = metricsFactory.timer("changeStreamTime");
    this.mongoClient = mongoClient;
    this.fsyncSuccessDurationTimer =
        metricsFactory.timer("fsyncDuration", Tags.of("outcome", "success"));
    this.fsyncFailureDurationTimer =
        metricsFactory.timer("fsyncDuration", Tags.of("outcome", "failure"));
    this.metricsFactory = metricsFactory;
  }

  static InitialSyncManagerFactory factory(
      Duration collectionScanTime,
      Duration changeStreamCatchupTimeout,
      Duration changeStreamLagTime,
      boolean avoidNaturalOrderScanSyncSourceChangeResync,
      List<String> excludedChangestreamFields,
      boolean matchCollectionUuidForUpdateLookup,
      boolean splitLargeChangeStreamEvents,
      Optional<MaterializedViewCollectionMetadataCatalog> mvMetadataCatalog,
      MetricsFactory metricsFactory) {
    return (initialSyncContext, mongoClient, namespace, resumeInfo) ->
        create(
            initialSyncContext,
            mongoClient,
            namespace,
            collectionScanTime,
            changeStreamCatchupTimeout,
            changeStreamLagTime,
            excludedChangestreamFields,
            matchCollectionUuidForUpdateLookup,
            splitLargeChangeStreamEvents,
            resumeInfo,
            mvMetadataCatalog,
            metricsFactory,
            avoidNaturalOrderScanSyncSourceChangeResync);
  }

  static String getSizeBucket(long totalBytes) {
    if (totalBytes < _100_MB) {
      return "lt_100MB";
    } else if (totalBytes < _1_GB) {
      return "100MB_1GB";
    } else {
      return "gt_1GB";
    }
  }

  private static BufferlessInitialSyncManager create(
      InitialSyncContext initialSyncContext,
      InitialSyncMongoClient mongoClient,
      MongoNamespace namespace,
      Duration collectionScanTime,
      Duration changeStreamCatchupTimeout,
      Duration changeStreamLagTime,
      List<String> excludedChangestreamFields,
      boolean matchCollectionUuidForUpdateLookup,
      boolean splitLargeChangeStreamEvents,
      Optional<InitialSyncResumeInfo> resumeInfo,
      Optional<MaterializedViewCollectionMetadataCatalog> mvMetadataCatalog,
      MetricsFactory metricsFactory,
      boolean avoidNaturalOrderScanSyncSourceChangeResync) {
    BufferlessCollectionScannerFactory collectionScannerFactory =
        (context, lastId) -> {
          if (isMaterializedViewBasedIndex(
              context.indexDefinitionGeneration.getIndexDefinition())) {
            return new AutoEmbeddingSortedIdCollectionScanner(
                Clock.systemUTC(),
                context,
                mongoClient,
                lastId,
                Check.isPresent(mvMetadataCatalog, "mvMetadataCatalog"),
                metricsFactory);
          } else {
            return new BufferlessCollectionScanner(
                Clock.systemUTC(),
                context,
                mongoClient,
                lastId,
                metricsFactory,
                avoidNaturalOrderScanSyncSourceChangeResync);
          }
        };
    BufferlessChangeStreamApplierFactory changeStreamApplierFactory =
        (BsonTimestamp highWaterMark, boolean isFreshStart) ->
            new BufferlessChangeStreamApplier(
                Clock.systemUTC(),
                changeStreamCatchupTimeout,
                changeStreamLagTime,
                excludedChangestreamFields,
                matchCollectionUuidForUpdateLookup,
                splitLargeChangeStreamEvents,
                initialSyncContext,
                mongoClient,
                namespace,
                highWaterMark,
                metricsFactory,
                avoidNaturalOrderScanSyncSourceChangeResync,
                isFreshStart);

    return new BufferlessInitialSyncManager(
        initialSyncContext,
        collectionScannerFactory,
        changeStreamApplierFactory,
        mongoClient::getMaxValidMajorityReadOptime,
        collectionScanTime,
        resumeInfo,
        metricsFactory,
        mongoClient);
  }

  /**
   * Runs the initial sync, returning after the index has been committed. Returns a
   * ChangeStreamResumeInfo indicating the position in a change stream where the sync has applied
   * to.
   */
  @Override
  public ChangeStreamResumeInfo sync() throws InitialSyncException {
    // If we're resuming a sync, read the change stream high water mark and the id of the last doc
    // indexed during collection scan from the resume info.
    @Var BsonTimestamp highWaterMark;
    @Var BsonValue lastScannedToken;
    if (this.resumeInfo.isPresent()) {
      // This should never happen because useNaturalOrderScan should be set properly when requests
      // are enqueued into initial sync queue.
      if ((this.resumeInfo.get().isBufferlessIdOrderInitialSyncResumeInfo()
              && this.context.useNaturalOrderScan())
          || (this.resumeInfo.get().isBufferlessNaturalOrderInitialSyncResumeInfo()
              && !this.context.useNaturalOrderScan())) {
        this.logger
            .atWarn()
            .addKeyValue("useNaturalOrderScan", this.context.useNaturalOrderScan())
            .log("resumeInfo does not match with request context, retry the initial sync");
        throw InitialSyncException.createInvalidated(this.resumeInfo.get());
      }
      highWaterMark = this.resumeInfo.get().getResumeOperationTime();
      lastScannedToken = this.resumeInfo.get().getResumeToken();
      this.logger
          .atInfo()
          .addKeyValue("lastScannedToken", lastScannedToken)
          .log("Collection scan will resume from the last doc indexed during collection scan.");
    } else {
      highWaterMark = this.clusterTimeProvider.getCurrentClusterTime();
      lastScannedToken =
          this.context.useNaturalOrderScan() ? new BsonDocument() : BsonUtils.MIN_KEY;
    }
    // isFreshStart is true when we're not resuming from a crash (resumeInfo is empty).
    // For fresh starts, we can safely use highWaterMark + 1 since the collection scan already
    // captured everything at highWaterMark. For resumes, we must use highWaterMark (inclusive)
    // to avoid missing events from multi-document transactions.
    boolean isFreshStart = this.resumeInfo.isEmpty();
    BufferlessChangeStreamApplier changeStreamApplier =
        this.changeStreamApplierFactory.create(highWaterMark, isFreshStart);

    Stopwatch stopwatch = Stopwatch.createStarted();
    this.logger
        .atInfo()
        .addKeyValue("useNaturalOrderScan", this.context.useNaturalOrderScan())
        .addKeyValue("startOpTime", highWaterMark)
        .addKeyValue(
            "resumeOpTime", this.resumeInfo.map(InitialSyncResumeInfo::getResumeOperationTime))
        .addKeyValue("indexId", this.context.getIndexId())
        .addKeyValue("generationId", this.context.getGenerationId())
        .addKeyValue("lastScannedToken", this.resumeInfo.map(InitialSyncResumeInfo::getResumeToken))
        .addKeyValue("hostName", this.resumeInfo.map(InitialSyncResumeInfo::getSyncSourceHost))
        .log("Beginning initial sync.");

    InitialSyncException.wrapIfThrows(
        () -> {
          // run fsync to force a disk write to create a newer checkpoint
          // lastStableRecoveryTimestamp,
          // which ensures record id correctness for natural order scan
          if (this.context.useNaturalOrderScan()) {
            var fsyncStopwatch = Stopwatch.createStarted();
            try {
              var response = this.mongoClient.fsync();
              this.fsyncSuccessDurationTimer.record(fsyncStopwatch.stop().elapsed());
              this.logger
                  .atInfo()
                  .addKeyValue("response", response.toJson())
                  .log("finish fsync successfully");
            } catch (MongoCommandException e) {
              this.fsyncFailureDurationTimer.record(fsyncStopwatch.stop().elapsed());
              String errorCode = e.getErrorCodeName() != null ? e.getErrorCodeName() : "unknown";
              this.metricsFactory
                  .counter(
                      "fsyncError",
                      Tags.of("errorCode", errorCode, "errorType", "MongoCommandException"))
                  .increment();
              this.logger
                  .atInfo()
                  .addKeyValue("errorCode", e.getErrorCode())
                  .addKeyValue("errorCodeName", e.getErrorCodeName())
                  .addKeyValue("message", e.getErrorMessage())
                  .addKeyValue("response", e.getResponse().toJson())
                  .log("Failed to run fsync during initial sync, ignore error and continue");
            } catch (Exception e) {
              this.fsyncFailureDurationTimer.record(fsyncStopwatch.stop().elapsed());
              this.metricsFactory
                  .counter("fsyncError", Tags.of("errorType", e.getClass().getSimpleName()))
                  .increment();
              throw e;
            }
          }
        });

    double bytesBeforeSync =
        this.context.getInitialSyncMetricsUpdater().getTotalApplicableBytes().count();

    try (changeStreamApplier) {
      // Continue the initial sync until the entire collection has been scanned.
      @Var boolean continueSync = true;
      while (continueSync) {
        // Update the last scanned id and whether we need to continue scanning. If we're done
        // scanning, catch up with the change stream one last time before finishing the initial
        // sync.

        var collectionScanTimer = Stopwatch.createStarted();
        BufferlessCollectionScanner.Result scanResult =
            scanCollection(highWaterMark, lastScannedToken);
        this.collectionScanTimer.record(collectionScanTimer.stop().elapsed());

        continueSync = scanResult.getContinueSync();
        lastScannedToken = scanResult.getLastScannedToken();

        // Apply change stream events and update the high water mark.
        var changeStreamTimer = Stopwatch.createStarted();
        highWaterMark =
            applyChangeStreamEvents(changeStreamApplier, lastScannedToken, continueSync);
        this.changeStreamTimer.record(changeStreamTimer.stop().elapsed());
      }
    }

    Optional<ChangeStreamResumeInfo> changeStreamResumeInfo = changeStreamApplier.getResumeInfo();

    long totalBytesProcessed =
        (long)
            (this.context.getInitialSyncMetricsUpdater().getTotalApplicableBytes().count()
                - bytesBeforeSync);
    String sizeBucket = getSizeBucket(totalBytesProcessed);
    Tags sizeCategoryTag = Tags.of("sizeCategory", sizeBucket);

    double elapsedSeconds = stopwatch.elapsed().toMillis() / 1000.0;
    if (elapsedSeconds > 0) {
      // Record the throughput in a histogram
      // Specify the buckets in the histogram so we don't have an explosion in the data.
      // Right now, with the size CategoryTag, we will have around 7*3 buckets.
      this.metricsFactory
          .histogram(
              "completedSyncThroughputBytesPerSec",
              sizeCategoryTag,
              1_000_000, // 1 MB/s
              3_000_000, // 3 MB/s
              10_000_000, // 10 MB/s
              30_000_000, // 30 MB/s
              100_000_000, // 100 MB/s
              209_715_200, // 200 MiB/s
              314_572_800 // 300 MiB/s
              )
          .record(totalBytesProcessed / elapsedSeconds);
    }

    this.logger
        .atInfo()
        .addKeyValue("useNaturalOrderScan", this.context.useNaturalOrderScan())
        .addKeyValue("duration", stopwatch)
        .addKeyValue("totalBytesProcessed", totalBytesProcessed)
        .addKeyValue("sizeCategory", sizeBucket)
        .addKeyValue(
            "throughputBytesPerSec", elapsedSeconds > 0 ? totalBytesProcessed / elapsedSeconds : 0)
        .addKeyValue("indexId", this.context.getIndexId())
        .addKeyValue("generationId", this.context.getGenerationId())
        .log("Completed initial sync. Beginning first commit.");
    checkState(
        changeStreamResumeInfo.isPresent(),
        "Change stream application completed without setting ChangeStreamResumeInfo");

    IndexCommitUserData commitUserData =
        IndexCommitUserData.createChangeStreamResume(
            changeStreamResumeInfo.get(), this.context.getIndexFormatVersion());
    doFirstCommit(commitUserData);
    this.logger
        .atInfo()
        .addKeyValue("indexId", this.context.getIndexId())
        .addKeyValue("generationId", this.context.getGenerationId())
        .log("Completed first commit.");
    return changeStreamResumeInfo.get();
  }

  /**
   * Returns the shutdown timeout based on index type. Auto-embedding indexes use a longer timeout
   * to accommodate slow external embedding API calls.
   */
  private Duration getShutdownTimeout() {
    return isMaterializedViewBasedIndex(this.context.indexDefinitionGeneration.getIndexDefinition())
        ? InitialSyncManager.AUTO_EMBEDDING_SHUTDOWN_TIMEOUT
        : InitialSyncManager.SHUTDOWN_TIMEOUT;
  }

  /**
   * Issues a new collection scan cursor with the change stream high water mark as the
   * readConcern.afterClusterTime, ensuring the scan represents a view of the collection from an
   * opTime after the last event applied. Scan for some time, limited by collectionScanTime.
   */
  private BufferlessCollectionScanner.Result scanCollection(
      BsonTimestamp highWaterMark, BsonValue lastScannedToken) throws InitialSyncException {
    BufferlessCollectionScanner collectionScanner =
        this.collectionScannerFactory.create(
            this.context.withProgress(highWaterMark), lastScannedToken);

    CompletableFuture<BufferlessCollectionScanner.Result> scanFuture =
        supplyAsync(
            () -> collectionScanner.scanWithTimeLimit(this.collectionScanTime),
            String.format("%s %s", this.context.uniqueString(), "CollectionScanner"));

    Duration shutdownTimeout = getShutdownTimeout();

    Runnable shutDown =
        () -> {
          collectionScanner.signalShutdown();
          awaitShutdown(shutdownTimeout, scanFuture);
        };

    return getResultOrThrow(scanFuture, "waiting for collection scan", shutDown, this.logger);
  }

  /**
   * Apply events until we have 1) encountered a batch that is past stopAfterOpTime, signalling that
   * we have caught up to where we ended the most recent collection scan phase, and 2) spent some
   * time trying to further "catch up" with the change stream.
   */
  private BsonTimestamp applyChangeStreamEvents(
      BufferlessChangeStreamApplier changeStreamApplier,
      BsonValue lastScannedToken,
      boolean continueSync)
      throws InitialSyncException {
    // Use the opTime after a collection scan phase to signal the change stream event application
    // phase to switch back after witnessing a batch that is past this opTime.
    BsonTimestamp stopAfterOpTime = this.clusterTimeProvider.getCurrentClusterTime();

    CompletableFuture<BsonTimestamp> applierFuture =
        supplyAsync(
            () -> changeStreamApplier.applyEvents(lastScannedToken, stopAfterOpTime, continueSync),
            String.format(
                "%s %s",
                this.context.uniqueString(), BufferlessChangeStreamApplier.class.getSimpleName()));

    Duration shutdownTimeout = getShutdownTimeout();

    Runnable changeStreamShutdown =
        () -> {
          changeStreamApplier.signalShutdown();
          awaitShutdown(shutdownTimeout, applierFuture);
        };

    return getResultOrThrow(
        applierFuture,
        "waiting for change stream events to be applied",
        changeStreamShutdown,
        this.logger);
  }

  private void doFirstCommit(IndexCommitUserData commitUserData) throws InitialSyncException {
    this.context.indexer.updateCommitUserData(commitUserData);
    CompletableFuture<Void> commitFuture =
        InitialSyncManager.runAsync(
            () ->
                // This needs to be run in a separate thread to avoid being interrupted.
                // Interrupting the commit operation will cause the index writer closed.
                Crash.because("failed to do first commit after initial sync")
                    .ifThrowsExceptionOrError(this.context.indexer::commit),
            String.format("%s %s", this.context.uniqueString(), "FirstCommit"));
    Duration shutdownTimeout = getShutdownTimeout();

    // When shutdown is triggered, crash JVM if the commit operation doesn't complete in time.
    Runnable firstCommitShutdown = () -> awaitShutdown(shutdownTimeout, commitFuture);

    getResultOrThrow(commitFuture, "waiting for first commit", firstCommitShutdown, this.logger);
  }
}
