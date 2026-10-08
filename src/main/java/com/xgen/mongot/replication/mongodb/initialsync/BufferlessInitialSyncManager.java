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
import com.xgen.mongot.embedding.exceptions.MaterializedViewNonTransientException;
import com.xgen.mongot.logging.DefaultKeyValueLogger;
import com.xgen.mongot.metrics.MetricsFactory;
import com.xgen.mongot.replication.mongodb.common.BufferlessParallelNaturalOrderInitialSyncResumeInfo;
import com.xgen.mongot.replication.mongodb.common.ChangeStreamResumeInfo;
import com.xgen.mongot.replication.mongodb.common.IndexCommitUserData;
import com.xgen.mongot.replication.mongodb.common.InitialSyncException;
import com.xgen.mongot.replication.mongodb.common.InitialSyncResumeInfo;
import com.xgen.mongot.util.BsonUtils;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.Crash;
import com.xgen.mongot.util.mongodb.serialization.MongoDbCollectionInfo;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import java.io.IOException;
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
  private final ParallelCollectionScanSessionFactory parallelSessionFactory;
  private final InitialSyncPartitionPlannerFactory partitionPlannerFactory;
  private final MongoDbCollectionInfo collectionInfo;
  private final ServerClusterTimeProvider clusterTimeProvider;
  private final Duration collectionScanTime;
  private final Optional<InitialSyncResumeInfo> resumeInfo;

  private static final long _100_MB = 100L * 1024 * 1024;
  private static final long _1_GB = 1024L * 1024 * 1024;
  private static final long _5_GB = 5L * _1_GB;
  private static final long _10_GB = 10L * _1_GB;

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
      ParallelCollectionScanSessionFactory parallelSessionFactory,
      InitialSyncPartitionPlannerFactory partitionPlannerFactory,
      MongoDbCollectionInfo collectionInfo,
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
    this.parallelSessionFactory = parallelSessionFactory;
    this.partitionPlannerFactory = partitionPlannerFactory;
    this.collectionInfo = collectionInfo;
    this.clusterTimeProvider = clusterTimeProvider;
    this.collectionScanTime = collectionScanTime;
    this.resumeInfo = resumeInfo;
    this.collectionScanTimer = metricsFactory.timerSec("collectionScanTime");
    this.changeStreamTimer = metricsFactory.timerSec("changeStreamTime");
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
      MetricsFactory metricsFactory,
      MeterRegistry meterRegistry) {
    return (initialSyncContext, mongoClient, namespace, collectionInfo, resumeInfo) ->
        create(
            initialSyncContext,
            mongoClient,
            namespace,
            collectionInfo,
            collectionScanTime,
            changeStreamCatchupTimeout,
            changeStreamLagTime,
            excludedChangestreamFields,
            matchCollectionUuidForUpdateLookup,
            splitLargeChangeStreamEvents,
            resumeInfo,
            mvMetadataCatalog,
            metricsFactory,
            meterRegistry,
            avoidNaturalOrderScanSyncSourceChangeResync);
  }

  static String getSizeBucket(long totalBytes) {
    if (totalBytes < _100_MB) {
      return "lt_100MB";
    } else if (totalBytes < _1_GB) {
      return "100MB_1GB";
    } else if (totalBytes < _5_GB) {
      return "1GB_5GB";
    } else if (totalBytes < _10_GB) {
      return "5GB_10GB";
    } else {
      return "gt_10GB";
    }
  }

  private static BufferlessInitialSyncManager create(
      InitialSyncContext initialSyncContext,
      InitialSyncMongoClient mongoClient,
      MongoNamespace namespace,
      MongoDbCollectionInfo collectionInfo,
      Duration collectionScanTime,
      Duration changeStreamCatchupTimeout,
      Duration changeStreamLagTime,
      List<String> excludedChangestreamFields,
      boolean matchCollectionUuidForUpdateLookup,
      boolean splitLargeChangeStreamEvents,
      Optional<InitialSyncResumeInfo> resumeInfo,
      Optional<MaterializedViewCollectionMetadataCatalog> mvMetadataCatalog,
      MetricsFactory metricsFactory,
      MeterRegistry meterRegistry,
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
        (BsonTimestamp highWaterMark,
            boolean isFreshStart,
            Optional<ParallelCollectionScanResumeState> parallelResumeState) ->
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
                isFreshStart,
                parallelResumeState);

    ParallelCollectionScanSessionFactory parallelSessionFactory =
        resumeState ->
            ParallelCollectionScanSession.create(
                Clock.systemUTC(),
                initialSyncContext,
                mongoClient,
                resumeState,
                collectionScanTime,
                metricsFactory,
                meterRegistry);

    InitialSyncPartitionPlannerFactory partitionPlannerFactory =
        contextWithProgress ->
            new InitialSyncPartitionPlanner(
                Clock.systemUTC(),
                contextWithProgress,
                mongoClient.getCollectionStatsMongoClient(),
                mongoClient.getPartitionPlanningMongoClient(),
                mongoClient.getCollectionSamplingMongoClient(),
                metricsFactory);

    return new BufferlessInitialSyncManager(
        initialSyncContext,
        collectionScannerFactory,
        changeStreamApplierFactory,
        parallelSessionFactory,
        partitionPlannerFactory,
        collectionInfo,
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
    if (parallelResumeInfo().isPresent()) {
      BufferlessParallelNaturalOrderInitialSyncResumeInfo parallel = parallelResumeInfo().get();
      // The partitions are only scannable by a parallel run, and the blob holds no single token to
      // fall back on, so with either switch off the sync starts over rather than reading a token
      // that does not exist.
      if (!this.context.useNaturalOrderScan()
          || !this.context.isParallelNaturalOrderScanEnabled()) {
        this.logger
            .atWarn()
            .addKeyValue("useNaturalOrderScan", this.context.useNaturalOrderScan())
            .addKeyValue("parallelScanEnabled", this.context.isParallelNaturalOrderScanEnabled())
            .addKeyValue("resumeOpTime", parallel.getResumeOperationTime())
            .log("Cannot resume a parallel natural-order scan, restarting the initial sync");
        throw InitialSyncException.createRequiresResync(
            "cannot resume a parallel natural-order collection scan");
      }
      highWaterMark = parallel.getResumeOperationTime();
      // Each partition carries its own position, so there is nothing to thread through the loop.
      lastScannedToken = new BsonDocument();
      this.logger
          .atInfo()
          .addKeyValue("resumeOpTime", highWaterMark)
          .addKeyValue("numPartitions", parallel.getPartitions().size())
          .log("Collection scan will resume from the partitions the last attempt committed.");
    } else if (this.resumeInfo.isPresent()) {
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

    Stopwatch stopwatch = Stopwatch.createStarted();
    this.logger
        .atInfo()
        .addKeyValue("useNaturalOrderScan", this.context.useNaturalOrderScan())
        .addKeyValue("parallelScanEnabled", this.context.isParallelNaturalOrderScanEnabled())
        .addKeyValue("startOpTime", highWaterMark)
        .addKeyValue(
            "resumeOpTime", this.resumeInfo.map(InitialSyncResumeInfo::getResumeOperationTime))
        .addKeyValue("indexId", this.context.getIndexId())
        .addKeyValue("generationId", this.context.getGenerationId())
        // The token in use rather than the blob's: a parallel resume has none to read.
        .addKeyValue("lastScannedToken", lastScannedToken)
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

    // A resume rebuilds its partitions from the blob it was handed. Planning fresh ones reads
    // $recordId boundaries off the sync source, so it runs after the fsync above, which is what
    // makes those ids stable for a natural-order scan.
    Optional<ParallelCollectionScanResumeState> parallelResumeState =
        resolveParallelScan(highWaterMark);

    BufferlessChangeStreamApplier changeStreamApplier =
        this.changeStreamApplierFactory.create(highWaterMark, isFreshStart, parallelResumeState);

    double bytesBeforeSync =
        this.context.getInitialSyncMetricsUpdater().getTotalApplicableBytes().count();

    try (changeStreamApplier) {
      Optional<ParallelCollectionScanSession> parallelSession =
          parallelResumeState.map(this.parallelSessionFactory::create);

      // Continue the initial sync until the entire collection has been scanned.
      @Var boolean continueSync = true;
      try {
        // Only a freshly planned scan has boundaries that are not durable yet. Ordered after the
        // session, which installs the supplier a commit reads.
        if (parallelSession.isPresent() && parallelResumeInfo().isEmpty()) {
          commitPartitionPlan();
        }

        while (continueSync) {
          // Update the last scanned id and whether we need to continue scanning. If we're done
          // scanning, catch up with the change stream one last time before finishing the initial
          // sync.

          var collectionScanTimer = Stopwatch.createStarted();
          if (parallelSession.isPresent()) {
            // Partitions carry their own positions in the session state, so there is no token to
            // thread from one window to the next.
            continueSync = scanPartitions(parallelSession.get());
          } else {
            BufferlessCollectionScanner.Result scanResult =
                scanCollection(highWaterMark, lastScannedToken);
            continueSync = scanResult.getContinueSync();
            lastScannedToken = scanResult.getLastScannedToken();
          }
          this.collectionScanTimer.record(collectionScanTimer.stop().elapsed());

          // Apply change stream events and update the high water mark. On the parallel path the
          // applier advances the session's mark instead, and ignores the token.
          var changeStreamTimer = Stopwatch.createStarted();
          highWaterMark =
              applyChangeStreamEvents(changeStreamApplier, lastScannedToken, continueSync);
          this.changeStreamTimer.record(changeStreamTimer.stop().elapsed());
        }
      } finally {
        parallelSession.ifPresent(ParallelCollectionScanSession::close);
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
      // Right now, with the size CategoryTag, we will have around 7*5 buckets.
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
            changeStreamResumeInfo.get(),
            this.context.getGenerationId().generation.formatVersion.versionNumber());
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

  /** The parallel scan this sync resumes, if it was handed a blob describing one. */
  private Optional<BufferlessParallelNaturalOrderInitialSyncResumeInfo> parallelResumeInfo() {
    return this.resumeInfo.flatMap(
        info ->
            info instanceof BufferlessParallelNaturalOrderInitialSyncResumeInfo parallel
                ? Optional.of(parallel)
                : Optional.empty());
  }

  /**
   * The parallel scan this sync will run, or empty to scan the collection with a single cursor.
   *
   * <p>A parallel resume rebuilds the partitions the last attempt committed, so each picks up from
   * the record id it had reached and the ones that finished are not read again. Only a fresh sync
   * asks the planner whether this collection can be split at all.
   *
   * <p>Empty covers every route to the sequential scan: natural order is off, the parallel feature
   * is off, a sequential scan is resuming, or the planner declined to partition this collection.
   */
  private Optional<ParallelCollectionScanResumeState> resolveParallelScan(
      BsonTimestamp highWaterMark) {
    // Either switch off means no partitions are scanned, whether they were planned here or read
    // back from a blob. sync() turns a resume away before this, so the sequential scan reached
    // from here always has a token to start from.
    if (!this.context.useNaturalOrderScan() || !this.context.isParallelNaturalOrderScanEnabled()) {
      return Optional.empty();
    }

    checkState(
        !isMaterializedViewBasedIndex(this.context.indexDefinitionGeneration.getIndexDefinition()),
        "parallel natural-order scan does not support materialized-view based indexes");

    // The scanners send the plan's $initialSyncId with every window, so a sync source that
    // reincarnated its data since the blob was written fails the first window rather than
    // scanning rewritten record ids.
    Optional<BufferlessParallelNaturalOrderInitialSyncResumeInfo> resumed = parallelResumeInfo();
    if (resumed.isPresent()) {
      return Optional.of(
          ParallelCollectionScanResumeState.fromPersisted(
              this.context.getGenerationId().generation.formatVersion.versionNumber(),
              resumed.get()));
    }

    // Any other resume is a sequential natural-order scan with a token and everything indexed
    // behind it. Partitioning now would restart from freshly planned boundaries and re-scan all of
    // that, so only a fresh sync is ever partitioned. A resume that had indexed nothing yet loses
    // no indexed work by staying sequential, which is not worth reading its token to tell apart.
    //
    // The cost is that one restart, however early, leaves this generation sequential to the end.
    // Partitioning the remainder above the resume record id would recover that.
    if (this.resumeInfo.isPresent()) {
      return Optional.empty();
    }

    // Namespace resolution matches on info.uuid, which only collections report, so a view cannot
    // reach here. Declining rather than casting keeps planning from failing a sync if that changes.
    if (!(this.collectionInfo instanceof MongoDbCollectionInfo.Collection collection)) {
      return Optional.empty();
    }

    // The planner reads the sync source as of the high water mark, the same snapshot the first
    // scan window reads, so the record ids it returns belong to the data that will be scanned.
    InitialSyncPartitionPlan plan =
        this.partitionPlannerFactory
            .create(this.context.withProgress(highWaterMark))
            .plan(collection);
    if (plan
        instanceof
        InitialSyncPartitionPlan.SequentialFallback(
            InitialSyncPartitionPlan.ShortCircuitReason reason)) {
      this.logger
          .atInfo()
          .addKeyValue("reason", reason)
          .log("Not partitioning the collection scan, falling back to a single cursor.");
      return Optional.empty();
    }

    var partitioned = (InitialSyncPartitionPlan.PartitionedResult) plan;
    return Optional.of(
        ParallelCollectionScanResumeState.fromPlan(
            this.context.getGenerationId().generation.formatVersion.versionNumber(),
            partitioned,
            highWaterMark,
            Optional.of(this.mongoClient.getSyncSourceHost())));
  }

  /**
   * Publishes the partition boundaries before the first window runs. The periodic committer is on a
   * fixed delay, so without this a restart in the window before its first run would plan again.
   *
   * <p>Runs on its own thread, like the first commit does, because interrupting a commit closes the
   * index writer. A failure is logged rather than thrown: the commit only saves the planner's
   * reads, and without it the next attempt plans afresh, which is what it would have done anyway.
   */
  private void commitPartitionPlan() throws InitialSyncException {
    CompletableFuture<Void> commitFuture =
        InitialSyncManager.runAsync(
            () -> {
              try {
                this.context.indexer.commit();
              } catch (IOException e) {
                this.logger
                    .atWarn()
                    .setCause(e)
                    .log("Could not commit the partition plan, continuing with the scan.");
              }
            },
            String.format("%s %s", this.context.uniqueString(), "PartitionPlanCommit"));

    Duration shutdownTimeout = getShutdownTimeout();

    Runnable shutDown = () -> awaitShutdown(shutdownTimeout, commitFuture);

    getResultOrThrow(commitFuture, "waiting for the partition plan commit", shutDown, this.logger);
  }

  /**
   * Scans every unfinished partition for one window. Mirrors {@link #scanCollection}: the work runs
   * off this thread so that a shutdown can stop it and still be waited on.
   */
  private boolean scanPartitions(ParallelCollectionScanSession session)
      throws InitialSyncException {
    CompletableFuture<Boolean> scanFuture =
        supplyAsync(
            session::scanWindow,
            String.format("%s %s", this.context.uniqueString(), "ParallelCollectionScan"));

    Duration shutdownTimeout = getShutdownTimeout();

    Runnable shutDown =
        () -> {
          session.signalShutdown();
          awaitShutdown(shutdownTimeout, scanFuture);
        };

    return getResultOrThrow(
        scanFuture, "waiting for parallel collection scan", shutDown, this.logger);
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

  void doFirstCommit(IndexCommitUserData commitUserData) throws InitialSyncException {
    try {
      this.context.indexer.updateCommitUserData(commitUserData);
    } catch (MaterializedViewNonTransientException e) {
      throw translateMaterializedViewRejection(e);
    }
    // This needs to be run in a separate thread to avoid being interrupted. Interrupting the
    // commit operation will cause the index writer closed.
    CompletableFuture<Void> commitFuture =
        InitialSyncManager.runAsync(
            this::doFirstCommitAfterInitialSync,
            String.format("%s %s", this.context.uniqueString(), "FirstCommit"));
    Duration shutdownTimeout = getShutdownTimeout();

    // When shutdown is triggered, crash JVM if the commit operation doesn't complete in time.
    Runnable firstCommitShutdown = () -> awaitShutdown(shutdownTimeout, commitFuture);

    getResultOrThrow(commitFuture, "waiting for first commit", firstCommitShutdown, this.logger);
  }

  // Mirrors EmbeddingIndexingWorkScheduler#handleBatchException's routing of a materialized-view
  // commit rejection, so an MV first-commit rejection is handled as an ordinary demotion instead
  // of halting the JVM (CLOUDP-434414).
  void doFirstCommitAfterInitialSync() throws InitialSyncException {
    try {
      this.context.indexer.commit();
    } catch (MaterializedViewNonTransientException e) {
      throw translateMaterializedViewRejection(e);
    } catch (Exception | Error e) {
      // Any other first-commit failure is genuinely fatal -- preserve the original halt.
      Crash.because("failed to do first commit after initial sync").withThrowable(e).now();
      throw Check.unreachableError();
    }
  }

  static InitialSyncException translateMaterializedViewRejection(
      MaterializedViewNonTransientException e) {
    return e.getReason() == MaterializedViewNonTransientException.Reason.INVALID_LEASE
        ? InitialSyncException.createShutDown()
        : InitialSyncException.createFailed(e);
  }
}
