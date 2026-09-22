package com.xgen.mongot.replication.mongodb;

import static com.xgen.mongot.replication.mongodb.IndexManager.State;
import static com.xgen.mongot.util.Check.checkState;
import static com.xgen.mongot.util.mongodb.Errors.isMatchCollectionUuidUnsupportedException;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Streams;
import com.google.errorprone.annotations.Var;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import com.mongodb.MongoClientException;
import com.mongodb.MongoException;
import com.xgen.mongot.cursor.MongotCursorManager;
import com.xgen.mongot.featureflag.Feature;
import com.xgen.mongot.featureflag.FeatureFlags;
import com.xgen.mongot.index.EncodedUserData;
import com.xgen.mongot.index.Index;
import com.xgen.mongot.index.IndexGeneration;
import com.xgen.mongot.index.InitializedIndex;
import com.xgen.mongot.index.ReplicationOpTimeInfo;
import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.definition.VectorIndexDefinition;
import com.xgen.mongot.index.status.IndexStatus;
import com.xgen.mongot.index.status.IndexStatus.Reason;
import com.xgen.mongot.index.status.IndexStatus.StatusCode;
import com.xgen.mongot.index.status.StaleStatusReason;
import com.xgen.mongot.index.version.GenerationId;
import com.xgen.mongot.logging.DefaultKeyValueLogger;
import com.xgen.mongot.metrics.MetricsFactory;
import com.xgen.mongot.replication.mongodb.common.ChangeStreamResumeInfo;
import com.xgen.mongot.replication.mongodb.common.DocumentIndexer;
import com.xgen.mongot.replication.mongodb.common.IndexCommitUserData;
import com.xgen.mongot.replication.mongodb.common.IndexStateInfo;
import com.xgen.mongot.replication.mongodb.common.InitialSyncException;
import com.xgen.mongot.replication.mongodb.common.InitialSyncResumeInfo;
import com.xgen.mongot.replication.mongodb.common.MongoViewExceptionUtils;
import com.xgen.mongot.replication.mongodb.common.PeriodicIndexCommitter;
import com.xgen.mongot.replication.mongodb.common.QueryRejectedBySettingsExceptionUtils;
import com.xgen.mongot.replication.mongodb.common.ResumeTokenUtils;
import com.xgen.mongot.replication.mongodb.common.StaleStateInfo;
import com.xgen.mongot.replication.mongodb.common.SteadyStateException;
import com.xgen.mongot.replication.mongodb.initialsync.InitialSyncQueue;
import com.xgen.mongot.replication.mongodb.steadystate.SteadyStateManager;
import com.xgen.mongot.replication.mongodb.synonyms.SynonymManager;
import com.xgen.mongot.replication.mongodb.synonyms.SynonymMappingManager;
import com.xgen.mongot.replication.mongodb.synonyms.SynonymMappingManagerFactory;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.Crash;
import com.xgen.mongot.util.DurationUtils;
import com.xgen.mongot.util.FutureUtils;
import com.xgen.mongot.util.functionalinterfaces.CheckedRunnable;
import com.xgen.mongot.util.mongodb.Errors;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import java.io.IOException;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.bson.BsonTimestamp;

/** Handles replication for a single index generation. */
public class ReplicationIndexManager implements IndexManager {
  // TODO(CLOUDP-365528): Visibility of some of these fields have been raised for re-use in
  // MaterializedViewGenerator. Need to revisit these fields/values for auto-embedding.
  /** Default resync backoff when replication config does not specify {@code resyncBackoffMs}. */
  public static final Duration DEFAULT_RESYNC_BACKOFF = Duration.ofSeconds(30);

  /**
   * Default transient backoff when replication config does not specify {@code transientBackoffMs}.
   */
  public static final Duration DEFAULT_TRANSIENT_BACKOFF = Duration.ofSeconds(30);

  /**
   * System property specifying the perf-test force-merge-after-initial-sync target segment count.
   * It is baked into the mongot binary the perf framework builds (via the {@code upload_artifacts}
   * job), which passes {@code -D<this property>=<n>} on the JVM command line, so perf runs get an
   * index merged down to {@code n} segments.
   */
  static final String FORCE_MERGE_MAX_NUM_SEGMENTS_PROPERTY =
      "mongot.perf.forceMergeMaxNumSegments";

  private static final String EMPTY_EXCEPTION_METRIC_FIELD = "None";
  private static final String INDEX_DROPPED_COUNTER_UNKNOWN_TAG = "unknown";
  // Base name of the auto-embedding internal-search database. Kept in sync with
  // InternalDatabaseResolver.DEFAULT_BASE_NAME (duplicated here to avoid coupling the core
  // replication manager to the embedding package for a single constant). On multi-tenant clusters
  // the resolved database is "<tenant>___mdb_internal_search"; on dedicated it is the base name.
  private static final String INTERNAL_SEARCH_DB_BASE_NAME = "__mdb_internal_search";
  @VisibleForTesting static final String EXCEEDED_LIMIT_REASON_PREFIX = "Exceeded max limit: ";

  /** The action taken when an index fails. */
  enum FailedIndexAction {
    DROP,
    CLEAR,
    CLOSE,
    RETAIN
  }

  /**
   * The next step to be performed during the replication init process, determined by processing the
   * IndexCommitUserData (or lack thereof).
   */
  private enum InitAction {
    RUN_INITIAL_SYNC,
    RESUME_INITIAL_SYNC,
    RESUME_STEADY_STATE,
  }

  protected final DefaultKeyValueLogger logger;

  /** A lifecycle executor to schedule tasks onto. */
  private final Executor lifecycleExecutor;

  /** The cursor manager that can be used for killing cursors when an index is dropped. */
  private final MongotCursorManager cursorManager;

  /** The initial sync queue to enqueue resyncs onto. */
  private final InitialSyncQueue initialSyncQueue;

  /** The steady state manager used to create SteadyStateIndexManagers for the Index. */
  private final SteadyStateManager steadyStateManager;

  private final Collection<SynonymMappingManager> synonymMappingManagers;

  /** The index whose lifecycle the ReplicationIndexManager owns. */
  protected final IndexGeneration indexGeneration;

  private final InitializedIndex index;

  /** The indexer that indexes documents into the owned Index. */
  private final DocumentIndexer documentIndexer;

  /** The periodic committer, which triggers commits with a configured frequency. */
  private final PeriodicIndexCommitter periodicCommitter;

  /** Resync backoff duration on initial-sync error */
  private final Duration resyncBackoff;

  /** Backoff duration on steady-state transient error */
  private final Duration transientBackoff;

  /** Backoff duration on server ingress request rate limit */
  private final Duration requestRateLimitBackoff;

  /** The current state of the ReplicationIndexManager. */
  @GuardedBy("this")
  private State state;

  /** The future representing the work to initialize the ReplicationIndexManager. */
  @GuardedBy("this")
  protected CompletableFuture<Void> initFuture;

  /** The future representing the work to shut down steady state indexing. */
  @GuardedBy("this")
  private CompletableFuture<Void> steadyStateShutDownFuture;

  private final MetricsFactory metricsFactory;

  /** The feature flags defined by mms config. */
  private final FeatureFlags featureFlags;

  /**
   * A flag indicating if the matchCollectionUUIDForUpdateLookup change stream parameter should be
   * removed because it is unsupported by the MongoDB version.
   */
  private boolean removeMatchCollectionUuid;

  /**
   * A flag indicating if the natural order scan is supported. It would be turned off due to
   * unsupported MongoDB version or a false feature flag.
   */
  private boolean isNaturalOrderScanSupported;

  /** A separate executor used to initialize indexes (to not block lifecycle execution). */
  protected ReplicationIndexManager(
      Executor lifecycleExecutor,
      MongotCursorManager cursorManager,
      InitialSyncQueue initialSyncQueue,
      SteadyStateManager steadyStateManager,
      Collection<SynonymMappingManager> synonymMappingManagers,
      IndexGeneration indexGeneration,
      InitializedIndex initializedIndex,
      DocumentIndexer documentIndexer,
      PeriodicIndexCommitter periodicCommitter,
      MetricsFactory metricsFactory,
      FeatureFlags featureFlags,
      Duration resyncBackoff,
      Duration transientBackoff,
      Duration requestRateLimitBackoff,
      boolean enableNaturalOrderScan) {
    HashMap<String, Object> defaultKeyValues = new HashMap<>();
    defaultKeyValues.put("indexId", indexGeneration.getGenerationId().indexId);
    defaultKeyValues.put("generationId", indexGeneration.getGenerationId());
    this.logger = DefaultKeyValueLogger.getLogger(ReplicationIndexManager.class, defaultKeyValues);

    this.lifecycleExecutor = lifecycleExecutor;
    this.cursorManager = cursorManager;
    this.initialSyncQueue = initialSyncQueue;
    this.steadyStateManager = steadyStateManager;
    this.synonymMappingManagers = synonymMappingManagers;
    this.indexGeneration = indexGeneration;
    this.index = initializedIndex;
    this.documentIndexer = documentIndexer;
    this.periodicCommitter = periodicCommitter;
    this.resyncBackoff = resyncBackoff;
    this.transientBackoff = transientBackoff;
    this.requestRateLimitBackoff = requestRateLimitBackoff;
    this.state = State.INITIALIZING;
    this.initFuture = FutureUtils.COMPLETED_FUTURE;
    this.steadyStateShutDownFuture = FutureUtils.COMPLETED_FUTURE;
    this.metricsFactory = metricsFactory;
    this.featureFlags = featureFlags;
    this.removeMatchCollectionUuid = false;
    this.isNaturalOrderScanSupported = enableNaturalOrderScan;
  }

  /**
   * Creates and initializes an ReplicationIndexManager for the supplied index.
   *
   * @param lifecycleExecutor the Executor to use to run all miscellaneous work on
   * @param cursorManager the cursor manager that can be used for killing cursors when an index is
   *     dropped
   * @param initialSyncQueue the initial sync queue to enqueue resyncs onto
   * @param steadyStateManager the steady state manager used to create SteadyStateIndexManagers for
   *     the Index
   * @param synonymManagerOptional the optional synonym manager to enqueue synonym collection sync
   *     tasks onto
   * @param indexGeneration the index whose lifecycle the created ReplicationIndexManager should own
   * @param documentIndexer the indexer used to process document events and commit the index
   * @return an ReplicationIndexManager that owns the supplied index
   */
  public static ReplicationIndexManager create(
      Executor lifecycleExecutor,
      MongotCursorManager cursorManager,
      InitialSyncQueue initialSyncQueue,
      SteadyStateManager steadyStateManager,
      Optional<SynonymManager> synonymManagerOptional,
      IndexGeneration indexGeneration,
      InitializedIndex initializedIndex,
      DocumentIndexer documentIndexer,
      ScheduledExecutorService commitExecutor,
      Duration commitInterval,
      Duration requestRateLimitBackoff,
      MeterRegistry meterRegistry,
      FeatureFlags featureFlags,
      boolean enableNaturalOrderScan) {
    Index index = indexGeneration.getIndex();
    PeriodicIndexCommitter periodicCommitter =
        createPeriodicCommitter(index, documentIndexer, commitExecutor, commitInterval);
    return create(
        lifecycleExecutor,
        cursorManager,
        initialSyncQueue,
        steadyStateManager,
        synonymManagerOptional,
        indexGeneration,
        initializedIndex,
        documentIndexer,
        periodicCommitter,
        SynonymMappingManager::create,
        meterRegistry,
        featureFlags,
        DEFAULT_RESYNC_BACKOFF,
        DEFAULT_TRANSIENT_BACKOFF,
        requestRateLimitBackoff,
        enableNaturalOrderScan);
  }

  @VisibleForTesting
  static ReplicationIndexManager create(
      Executor lifecycleExecutor,
      MongotCursorManager cursorManager,
      InitialSyncQueue initialSyncQueue,
      SteadyStateManager steadyStateManager,
      Optional<SynonymManager> synonymManagerOptional,
      IndexGeneration indexGeneration,
      InitializedIndex initializedIndex,
      DocumentIndexer documentIndexer,
      PeriodicIndexCommitter periodicCommitter,
      SynonymMappingManagerFactory synonymMappingManagerFactory,
      MeterRegistry meterRegistry,
      FeatureFlags featureFlags,
      Duration resyncBackoff,
      Duration transientBackoff,
      Duration requestRateLimitBackoff,
      boolean enableNaturalScan) {
    Collection<SynonymMappingManager> synonymMappingManagers =
        synonymManagerOptional.isPresent()
            ? synonymMappingManagerFactory.create(
                synonymManagerOptional.get(), lifecycleExecutor, indexGeneration)
            : Collections.emptyList();

    ReplicationIndexManager manager =
        new ReplicationIndexManager(
            lifecycleExecutor,
            cursorManager,
            initialSyncQueue,
            steadyStateManager,
            synonymMappingManagers,
            indexGeneration,
            initializedIndex,
            documentIndexer,
            periodicCommitter,
            new MetricsFactory("replicationIndexManager", meterRegistry),
            featureFlags,
            resyncBackoff,
            transientBackoff,
            requestRateLimitBackoff,
            enableNaturalScan);

    // Schedule the manager to be initialized on the lifecycleExecutor, and fail the Index
    // if initialization fails.
    synchronized (manager) {
      manager.initFuture =
          Crash.because("failed initializing ReplicationIndexManager")
              .ifCompletesExceptionally(
                  CompletableFuture.runAsync(manager::init, lifecycleExecutor)
                      .handleAsync(
                          (ignored, throwable) -> {
                            if (throwable != null) {
                              if (featureFlags.isEnabled(
                                  Feature.RETAIN_FAILED_INDEX_DATA_ON_DISK)) {
                                manager.failAndCloseIndex(
                                    throwable, IndexStatus.Reason.INITIALIZATION_FAILED);
                              } else {
                                manager.failAndDropIndex(
                                    throwable, IndexStatus.Reason.INITIALIZATION_FAILED);
                              }
                            }

                            return null;
                          },
                          lifecycleExecutor));
    }

    return manager;
  }

  /**
   * A custom vector engine index needs no Java commit cycle: each completed replication batch
   * stages its resume position with the writer and the engine schedules its own commits. It gets an
   * inactive committer so the shutdown path stays uniform; every other index gets periodic commits.
   */
  @VisibleForTesting
  static PeriodicIndexCommitter createPeriodicCommitter(
      Index index,
      DocumentIndexer documentIndexer,
      ScheduledExecutorService commitExecutor,
      Duration commitInterval) {
    boolean engineSchedulesCommits =
        index.getDefinition() instanceof VectorIndexDefinition vectorDef
            && vectorDef.isCustomVectorEngineIndex();
    return engineSchedulesCommits
        ? PeriodicIndexCommitter.createInactive(index, documentIndexer)
        : PeriodicIndexCommitter.create(index, documentIndexer, commitExecutor, commitInterval);
  }

  /**
   * Gracefully shuts down the ReplicationIndexManager.
   *
   * @return a future that completes when the ReplicationIndexManager has completed shutting down.
   *     The future will only ever complete successfully.
   */
  @Override
  public synchronized CompletableFuture<Void> shutdown() {
    // Set our state to SHUT_DOWN so no further events will be scheduled.
    State currentState = this.state;
    transitionState(State.SHUT_DOWN);

    // Stop periodic commits for the index
    this.periodicCommitter.close();

    return FutureUtils.allOf(
        Streams.concat(
                Stream.of(shutdownReplication(currentState)),
                this.synonymMappingManagers.stream().map(SynonymMappingManager::shutdown))
            .collect(Collectors.toList()));
  }

  @Override
  public synchronized CompletableFuture<Void> getInitFuture() {
    return this.initFuture;
  }

  @Override
  public synchronized State getState() {
    return this.state;
  }

  private synchronized CompletableFuture<Void> shutdownReplication(State currentState) {
    GenerationId generationId = this.indexGeneration.getGenerationId();
    return switch (currentState) {
      case INITIALIZING -> this.initFuture;

      // No need to wait on anything if we already failed.
      // We can also end up calling shutdown() when already in SHUT_DOWN if, for example, the
      // collection is dropped, then later the user deletes the index. Seeing the collection be
      // dropped will transition us to SHUT_DOWN, and deleting the index will call shutdown().
      // Additionally, if we're waiting to put the index back into the initial sync queue, but no
      // outstanding work is currently scheduled, there's nothing to shut down.
      case FAILED, FAILED_EXCEEDED, SHUT_DOWN, INITIAL_SYNC_BACKOFF -> FutureUtils.COMPLETED_FUTURE;
      case INITIAL_SYNC -> this.initialSyncQueue.cancel(generationId);
      case STEADY_STATE -> FutureUtils.voidFuture(this.steadyStateManager.stop(generationId));
      case STEADY_STATE_SHUT_DOWN -> this.steadyStateShutDownFuture;
    };
  }

  /**
   * Drops the owned Index.
   *
   * @return a future that completes when the ReplicationIndexManager has been dropped. The future
   *     will only ever complete successfully.
   */
  @Override
  public synchronized CompletableFuture<Void> drop() {
    return this.shutdown().thenRunAsync(this::dropIndex, this.lifecycleExecutor);
  }

  /**
   * Index InitAction will be determined from IndexCommitUserData same as add() inside {@link
   * MongoDbNoOpReplicationManager}, any changes there should be reflected here too
   */
  // TODO(CLOUDP-280897): Make InitAction logic in a single place
  protected synchronized void init() {
    if (isShutDown()) {
      return;
    }

    checkState(
        this.state.equals(State.INITIALIZING), "cannot call init() when not in INITIALIZING");
    checkState(
        this.indexGeneration.getDefinitionGeneration().hasSupportedFormatVersion(),
        "index format version is not supported for replication");

    IndexCommitUserData userData = getIndexCommitUserData();
    Optional<String> exceededReason = userData.getExceededLimitsReason();
    if (exceededReason.isPresent()) {
      exceededIndex(exceededReason.get());
      return;
    }
    Optional<StaleStateInfo> staleStateInfo = userData.getStaleStateInfo();
    if (staleStateInfo.isPresent()) {
      // Do not start replication for indexes that were marked as stale on previous runs
      shutDownReplicationOnStaleIndex(staleStateInfo.get());
      return;
    }

    if (this.featureFlags.isEnabled(Feature.SHUT_DOWN_REPLICATION_WHEN_COLLECTION_NOT_FOUND)) {
      Optional<IndexStateInfo> indexStateInfo = userData.getIndexStateInfo();
      if (indexStateInfo.isPresent() && indexStateInfo.get().isCollectionNotFound()) {
        // Do not start replication for indexes that cannot resolve collections on mongod.
        // This typically occurs when a collection is missing on a shard.
        shutDownReplicationBecauseCollectionDoesNotExist();
        return;
      }
    }

    InitAction initAction = determineInitAction(userData);
    switch (initAction) {
      case RUN_INITIAL_SYNC -> enqueueInitialSync(IndexStatus.initialSync());
      case RESUME_INITIAL_SYNC ->
          enqueueInitialSyncResume(
              userData
                  .getInitialSyncResumeInfo()
                  .orElseThrow(
                      () ->
                          new IllegalStateException(
                              "initial sync resume info should be present "
                                  + "to resume initial sync")));
      case RESUME_STEADY_STATE ->
          resumeSteadyState(
              userData
                  .getResumeInfo()
                  .orElseThrow(
                      () ->
                          new IllegalStateException(
                              "resume info should be present to resume steady state.")));
    }
  }

  private InitAction determineInitAction(IndexCommitUserData userData) {
    if (userData.isEmpty()) {
      this.logger.info(
          "No index metadata found (likely due to never having been committed), resyncing.");
      return InitAction.RUN_INITIAL_SYNC;
    }

    // This shouldn't happen.
    if (userData.getBackendIndexVersion().isEmpty()) {
      this.logger.error("No index version found, resyncing.");
      return InitAction.RUN_INITIAL_SYNC;
    }

    int committedFormatVersion = userData.getBackendIndexVersion().get();
    int formatVersion =
        this.indexGeneration.getDefinitionGeneration().generation().formatVersion.versionNumber();
    if (committedFormatVersion != formatVersion) {
      Crash.because(
              String.format(
                  "index [%s] commit metadata index format version mismatch "
                      + "(expecting %s, found on disk %s)",
                  this.indexGeneration.getGenerationId(), formatVersion, committedFormatVersion))
          .now();
    }

    if (userData.getInitialSyncResumeInfo().isPresent()) {
      this.logger.info("Found initial sync resume data, resuming.");
      return InitAction.RESUME_INITIAL_SYNC;
    }

    // This shouldn't happen since we commit resume info while an initial sync is in progress; it's
    // only omitted after an initial sync has completed, at which point we should have this
    // information.
    Optional<ChangeStreamResumeInfo> resumeInfo = userData.getResumeInfo();
    if (resumeInfo.isEmpty()) {
      this.logger.error(
          "No resumeToken or initial sync minValidOpTime found for index, resyncing.");
      return InitAction.RUN_INITIAL_SYNC;
    }

    // TODO(CLOUDP-420697): This is a one-time healing shim, not steady-state logic. mongot 1.69.1
    // stopped new bad checkpoints from being created, and this guard rewrites each wedged
    // checkpoint on a single resume/restart. Remove it (and INTERNAL_SEARCH_DB_BASE_NAME, the
    // RESET_STALE_INTERNAL_SEARCH_CHECKPOINT feature flag, the staleInternalSearchNamespaceResync
    // counter, and the associated tests) once the release carrying this guard has fully rolled out
    // and the counter is zero fleet-wide.
    //
    // Guard against a stale internal-search namespace baked into the checkpoint. An auto-embedding
    // derived index tails its materialized-view collection in the internal-search database. On a
    // pre-fix mongot (CLOUDP-407569), a failed initial conf call defaulted isDedicatedCluster=true,
    // so the resolver produced the un-prefixed base name "__mdb_internal_search" instead of the
    // tenant-prefixed "<tenant>___mdb_internal_search". The derived index initial-synced against
    // that wrong collection and persisted its namespace in the resume info. Because the resume path
    // trusts the persisted namespace and never re-resolves, such an index keeps tailing the wrong
    // (never-dropped) collection indefinitely -- surviving the upgrade that fixed resolution. If
    // the
    // committed namespace's database no longer matches the (now correctly resolved) definition
    // database, the checkpoint is stale: discard it and resync against the correct namespace.
    // Scoped
    // to internal-search databases so non-auto-embedding indexes are unaffected.
    String resolvedDatabase = this.index.getDefinition().getDatabase();
    String committedDatabase = resumeInfo.get().getNamespace().getDatabaseName();
    if (this.featureFlags.isEnabled(Feature.RESET_STALE_INTERNAL_SEARCH_CHECKPOINT)
        && resolvedDatabase.endsWith(INTERNAL_SEARCH_DB_BASE_NAME)
        && !committedDatabase.equals(resolvedDatabase)) {
      this.logger.warn(
          "Committed change-stream namespace database [{}] does not match the resolved index "
              + "database [{}]; checkpoint was written against a stale internal-search namespace "
              + "(CLOUDP-416232). Discarding checkpoint and resyncing.",
          committedDatabase,
          resolvedDatabase);
      this.metricsFactory.counter("staleInternalSearchNamespaceResync").increment();
      return InitAction.RUN_INITIAL_SYNC;
    }

    return InitAction.RESUME_STEADY_STATE;
  }

  protected synchronized void enqueueInitialSync(IndexStatus status) {
    Check.checkState(
        status.getStatusCode() == IndexStatus.StatusCode.INITIAL_SYNC,
        "initial sync cannot be started with %s status",
        status);

    if (isShutDown()) {
      return;
    }

    transitionState(State.INITIAL_SYNC);
    // clears the index as well as the commit user data:
    clearIndex(IndexStatus.notStarted(), IndexCommitUserData.EMPTY);

    this.logger.info("Enqueueing initial sync.");
    try {
      Crash.because("failed enqueueing fresh initial sync")
          .ifCompletesExceptionally(
              this.initialSyncQueue
                  .enqueue(
                      this.documentIndexer,
                      this.indexGeneration.getDefinitionGeneration(),
                      () -> this.index.setStatus(status),
                      this.index.getMetricsUpdater(),
                      this.removeMatchCollectionUuid,
                      this.isNaturalOrderScanSupported)
                  .handleAsync(this::handleInitialSyncResult, this.lifecycleExecutor));
    } catch (InitialSyncException e) {
      handleInitialSyncException(e);
    }
  }

  private synchronized void enqueueInitialSyncResume(InitialSyncResumeInfo initialSyncResumeInfo) {
    if (isShutDown()) {
      return;
    }

    killCursors(IndexStatus.notStarted());
    transitionState(State.INITIAL_SYNC);

    this.logger.info("Enqueueing initial sync resume.");

    try {
      Crash.because("failed enqueueing initial sync resume")
          .ifCompletesExceptionally(
              this.initialSyncQueue
                  .enqueueResume(
                      this.documentIndexer,
                      this.indexGeneration.getDefinitionGeneration(),
                      () -> this.index.setStatus(IndexStatus.initialSync()),
                      initialSyncResumeInfo,
                      this.index.getMetricsUpdater(),
                      this.removeMatchCollectionUuid,
                      this.isNaturalOrderScanSupported)
                  .handleAsync(this::handleInitialSyncResult, this.lifecycleExecutor));
    } catch (InitialSyncException e) {
      handleInitialSyncException(e);
    }
  }

  private synchronized void scheduleInitialSyncRetry() {
    if (isShutDown()) {
      return;
    }
    checkState(
        this.state.equals(State.INITIAL_SYNC),
        "%s should already be in %s if retrying",
        getClass().getSimpleName(),
        State.INITIAL_SYNC);

    this.index.setStatus(IndexStatus.doesNotExist(IndexStatus.Reason.COLLECTION_NOT_FOUND));

    Crash.because("failed scheduling initial sync retry")
        .ifCompletesExceptionally(
            CompletableFuture.runAsync(
                this::enqueueInitialSyncRetry,
                CompletableFuture.delayedExecutor(
                    this.resyncBackoff.toMillis(), TimeUnit.MILLISECONDS, this.lifecycleExecutor)));
  }

  private synchronized void enqueueInitialSyncRetry() {
    if (isShutDown()) {
      return;
    }
    try {
      Crash.because("failed enqueueing initial sync retry")
          .ifCompletesExceptionally(
              this.initialSyncQueue
                  .enqueue(
                      this.documentIndexer,
                      this.indexGeneration.getDefinitionGeneration(),
                      () -> this.index.setStatus(IndexStatus.initialSync()),
                      this.index.getMetricsUpdater(),
                      false,
                      this.isNaturalOrderScanSupported)
                  .handleAsync(this::handleInitialSyncResult, this.lifecycleExecutor));
    } catch (InitialSyncException e) {
      handleInitialSyncException(e);
    }
  }

  private synchronized Void handleInitialSyncResult(
      ChangeStreamResumeInfo resumeInfo, Throwable throwable) {
    if (throwable != null) {
      handleInitialSyncException(throwable);
      return null;
    }

    // maybeForceMergeAfterInitialSync() can take a long time to complete, and since this method is
    // synchronized, it holds the manager's monitor for that whole duration
    // -- e.g. shutdown() will block until the merge finishes.
    maybeForceMergeAfterInitialSync();
    resumeSteadyState(resumeInfo);
    return null;
  }

  private synchronized void handleInitialSyncException(Throwable throwable) {
    this.recordReplicationExceptionMetric(throwable);

    if (isShutDown()) {
      if (!InitialSyncException.isShutDown(throwable)) {
        this.logger.info(
            "Experienced error during initial sync after shut down. Not scheduling more work.",
            throwable);
      }

      return;
    }

    // If the exception was not an InitialSyncException (i.e. was unexpected), fail the index.
    if (!(throwable instanceof InitialSyncException)) {
      maybeCrashOnUnexpectedThrowable(throwable);
      if (this.featureFlags.isEnabled(Feature.RETAIN_FAILED_INITIAL_SYNC_DATA_ON_DISK)) {
        failAndCloseIndex(throwable, IndexStatus.Reason.INITIAL_SYNC_REPLICATION_FAILED);
      } else {
        failAndDropIndex(throwable, IndexStatus.Reason.INITIAL_SYNC_REPLICATION_FAILED);
      }
      return;
    }

    InitialSyncException initialSyncException = (InitialSyncException) throwable;
    this.index
        .getMetricsUpdater()
        .getIndexingMetricsUpdater()
        .getInitialSyncExceptionCounter()
        .increment();
    switch (initialSyncException.getType()) {
      case DROPPED -> {
        this.logger.info(
            "Collection was dropped during initial sync. Stopping replication and dropping "
                + "index: {}",
            initialSyncException.getMessage());
        handleCollectionDropped();
        return;
      }
      case FAILED -> {
        handleInitialSyncFailed(initialSyncException);
        return;
      }
      case FIELD_EXCEEDED, DOCS_EXCEEDED ->
          exceededIndex(String.valueOf(initialSyncException.getMessage()));
      case SHUT_DOWN -> {
        // If the initial sync was shut down, there are 3 possible reasons:
        // - The InitialSyncQueue is being shut down
        // - This ReplicationIndexManager is being shut down
        // - Initial sync is paused on current index
        // We shouldn't schedule any more work.
        this.logger.info("Initial sync was shut down. Not scheduling any more work.");
        transitionState(State.SHUT_DOWN);
        return;
      }
      case REQUIRES_RESYNC -> {
        if (InitialSyncException.isInitialSyncIdMismatched(throwable.getCause())) {
          Tags tags = Tags.of("reason", "initialSyncIdMismatched", "useNaturalOrderScan", "true");
          this.metricsFactory.counter("naturalOrderScanRetry", tags).increment();
          failRetainOrResync(throwable, IndexStatus.initialSync());
          return;
        }

        if (InitialSyncException.isNaturalOrderScanFailed(throwable.getCause())) {
          this.isNaturalOrderScanSupported = false;
          Tags tags = Tags.of("reason", "ServerQueryFailed", "useNaturalOrderScan", "false");
          this.metricsFactory.counter("naturalOrderScanRetry", tags).increment();
          failRetainOrResync(throwable, IndexStatus.initialSync());
          return;
        }

        if (InitialSyncException.isBsonTooLargeError(throwable.getCause())) {
          failClearOrResync(throwable, IndexStatus.initialSync());
          return;
        }

        Optional<Throwable> cause = Optional.ofNullable(initialSyncException.getCause());
        if (QueryRejectedBySettingsExceptionUtils.isQueryRejectedBySettingsError(cause)) {
          this.metricsFactory.counter("queryRejectedBySettingsInitialSyncRetry").increment();

          IndexStatus status =
              IndexStatus.initialSync(
                  QueryRejectedBySettingsExceptionUtils.getQueryRejectedBySettingsErrorMessage(
                      Check.isPresent(cause, "cause")));

          // Set the index status so the error message is exposed to the user during backoff. The
          // index keeps resyncing (message is re-set on each retry) until the operation rejection
          // filter is removed, at which point the error clears on the next successful sync.
          this.index.setStatus(status);
          scheduleResync(throwable, status);
          return;
        }

        if (this.indexGeneration.getDefinition().getView().isPresent()) {
          if (MongoViewExceptionUtils.isViewPipelineRelated(cause)) {
            this.metricsFactory.counter("viewPipelineRelatedInitialSyncRetry").increment();

            String viewPipelineErrorMessage =
                MongoViewExceptionUtils.getViewPipelineErrorMessage(
                    Check.isPresent(cause, "cause"));

            if (this.featureFlags.isEnabled(Feature.RETAIN_FAILED_INITIAL_SYNC_DATA_ON_DISK)) {
              // Close the index with the view pipeline error message preserved in the
              // FAILED status so the user can see what went wrong.
              this.logger.error("Failing due to unexpected error.", throwable);
              incrementFailedIndexCounter(FailedIndexAction.CLOSE, getState(), throwable);
              transitionState(State.FAILED);
              closeIndex(
                  IndexStatus.failed(
                      viewPipelineErrorMessage, Reason.INITIAL_SYNC_REPLICATION_FAILED));
            } else {
              IndexStatus status = IndexStatus.initialSync(viewPipelineErrorMessage);
              // Set index status so that the error message is exposed to the user during
              // backoff. The error will be cleared when we transition to STEADY after a
              // successful resync.
              this.index.setStatus(status);
              scheduleResync(throwable, status);
            }
            return;
          }
        }

        if (InitialSyncException.isNoQueryExecutionPlansError(throwable.getCause())) {
          this.metricsFactory.counter("retriedInitialSyncDueToNoQueryExecutionPlans").increment();
          failCloseOrResync(throwable, IndexStatus.initialSync());
          return;
        }
        failRetainOrResync(throwable, IndexStatus.initialSync());
        return;
      }
      case RESUMABLE_TRANSIENT -> {
        Optional<Throwable> cause = Optional.ofNullable(initialSyncException.getCause());
        Duration operationBackoff = getTransientBackoffDuration(cause, this.resyncBackoff);
        if (cause.isPresent() && isMatchCollectionUuidUnsupportedException(cause.get())) {
          this.metricsFactory.counter("matchCollectionUUIDForUpdateLookupNotSupported").increment();
          this.removeMatchCollectionUuid = true;
        }
        scheduleResumableResync(throwable, operationBackoff);
        return;
      }
      case INVALIDATED -> {
        // this.isNaturalOrderScanSupported should always be paired with correct resumeInfo when
        // requests are enqueued. This ideally should never happen, but adding this here to cover
        // unforeseen corner cases
        if ((initialSyncException.getResumeInfo().isBufferlessIdOrderInitialSyncResumeInfo()
                && this.isNaturalOrderScanSupported)
            || (initialSyncException.getResumeInfo().isBufferlessNaturalOrderInitialSyncResumeInfo()
                && !this.isNaturalOrderScanSupported)) {
          this.isNaturalOrderScanSupported =
              !initialSyncException.getResumeInfo().isBufferlessIdOrderInitialSyncResumeInfo();
          Tags tags =
              Tags.of(
                  "reason",
                  "IncompatibleResumeInfo",
                  "useNaturalOrderScan",
                  String.valueOf(this.isNaturalOrderScanSupported));
          this.metricsFactory.counter("naturalOrderScanRetry", tags).increment();
        }
        scheduleResumeSync(initialSyncException.getResumeInfo());
        return;
      }
      case DOES_NOT_EXIST -> {
        if (this.featureFlags.isEnabled(Feature.SHUT_DOWN_REPLICATION_WHEN_COLLECTION_NOT_FOUND)) {
          shutDownReplicationBecauseCollectionDoesNotExist();
        } else {
          scheduleInitialSyncRetry();
        }
      }
    }
  }

  private synchronized void resumeSteadyState(ChangeStreamResumeInfo resumeInfo) {
    if (isShutDown()) {
      return;
    }

    // Before marking the index as steady, ensure all readers are up-to-date.
    Crash.because("failed to refresh reader").ifThrows(() -> this.index.getReader().refresh());

    this.logger.info("Resuming steady state replication.");

    // A resume token optime will have an inc that is one greater than the "actual" optime of the
    // last event in the change stream. (see: https://tinyurl.com/ydbjkv8k)
    // We subtract one from the inc value of the resume token to get the operation time of the
    // last indexed event, to match the optime that would be set by the change stream batch
    // application process.
    BsonTimestamp resumeTokenOpTime =
        Crash.because("failed to parse resume token")
            .ifThrows(() -> ResumeTokenUtils.opTimeFromResumeToken(resumeInfo.getResumeToken()));
    BsonTimestamp lastIndexedOpTime =
        new BsonTimestamp(resumeTokenOpTime.getTime(), Math.max(0, resumeTokenOpTime.getInc() - 1));
    ReplicationOpTimeInfo opTimeInfo =
        this.index.getMetricsUpdater().getIndexingMetricsUpdater().getReplicationOpTimeInfo();

    // Update optime info metric before setting index to STEADY to ensure metrics consistency.
    // Acquire unset lock to ensure another thread does not unset the metric while we update it.
    synchronized (opTimeInfo.getUnsetLock()) {
      opTimeInfo.update(lastIndexedOpTime.getValue(), lastIndexedOpTime.getValue());

      transitionState(State.STEADY_STATE);
      this.index.setStatus(IndexStatus.steady());
    }

    try {
      Crash.because("failed during steady state indexing")
          .ifCompletesExceptionally(
              this.steadyStateManager
                  .add(
                      this.indexGeneration.getGenerationId(),
                      this.documentIndexer,
                      this.indexGeneration.getDefinition(),
                      this.index.getMetricsUpdater(),
                      resumeInfo,
                      this.removeMatchCollectionUuid)
                  .handleAsync(
                      (ignored, throwable) -> {
                        checkState(
                            throwable != null, "steady state indexing completed unexceptionally");

                        handleSteadyStateException(throwable);
                        return null;
                      },
                      this.lifecycleExecutor));
    } catch (SteadyStateException e) {
      this.recordReplicationExceptionMetric(e);
      this.handleSteadyStateExceptionAfterManagerShutdown(e, Optional.empty());
    }
  }

  protected void maybeForceMergeAfterInitialSync() {
    String maxNumSegmentsProperty = System.getProperty(FORCE_MERGE_MAX_NUM_SEGMENTS_PROPERTY);
    if (isShutDown() || maxNumSegmentsProperty == null) {
      return;
    }
    int maxNumSegments = Integer.parseInt(maxNumSegmentsProperty);

    this.logger.info("Force-merging index to {} segment(s) after initial sync.", maxNumSegments);
    Crash.because("failed to force-merge index after initial sync")
        .ifThrowsExceptionOrError(
            () -> {
              this.index.getWriter().forceMerge(maxNumSegments);
              this.documentIndexer.commit();
              this.index.getReader().refresh();
            });
  }

  private synchronized void handleSteadyStateException(Throwable throwable) {
    this.recordReplicationExceptionMetric(throwable);

    if (isShutDown()) {
      if (!SteadyStateException.isShutDown(throwable)) {
        this.logger.info(
            "Experienced error during steady state indexing after shut down."
                + " Not scheduling more work.",
            throwable);
      }

      return;
    }

    this.logger.info(
        "Witnessed exception during steady state indexing. "
            + "Shutting down steady state indexing.");
    transitionState(State.STEADY_STATE_SHUT_DOWN);

    this.steadyStateShutDownFuture =
        Crash.because("failed stopping steady state")
            .ifCompletesExceptionally(
                this.steadyStateManager
                    .stop(this.indexGeneration.getGenerationId())
                    .thenAcceptAsync(
                        resumeInfo ->
                            handleSteadyStateExceptionAfterManagerShutdown(
                                throwable, Optional.of(resumeInfo)),
                        this.lifecycleExecutor));
  }

  private void recordReplicationExceptionMetric(Throwable throwable) {
    String className = exceptionClassWithErrorCode(throwable);

    Optional<Throwable> causeThrowable =
        Optional.of(throwable)
            .filter(e -> e instanceof InitialSyncException || e instanceof SteadyStateException)
            .flatMap(e -> Optional.ofNullable(e.getCause()));

    String cause =
        causeThrowable
            .map(ReplicationIndexManager::exceptionClassWithErrorCode)
            .orElse(EMPTY_EXCEPTION_METRIC_FIELD);

    String causeCategory;
    if (causeThrowable.isEmpty()) {
      causeCategory = EMPTY_EXCEPTION_METRIC_FIELD;
    } else if (causeThrowable.get() instanceof MongoClientException) {
      causeCategory = "mongo-driver";
    } else if (causeThrowable.get() instanceof MongoException) {
      causeCategory = "mongod";
    } else {
      causeCategory = EMPTY_EXCEPTION_METRIC_FIELD;
    }

    String type;
    if (throwable instanceof SteadyStateException steadyStateException) {
      type = steadyStateException.getType().name();
    } else if (throwable instanceof InitialSyncException initialSyncException) {
      type = initialSyncException.getType().name();
    } else {
      type = EMPTY_EXCEPTION_METRIC_FIELD;
    }

    this.metricsFactory
        .counter(
            "exceptions",
            Tags.of(
                "clazz", className, "type", type, "cause", cause, "causeCategory", causeCategory))
        .increment();
  }

  private synchronized void handleSteadyStateExceptionAfterManagerShutdown(
      Throwable throwable, Optional<ChangeStreamResumeInfo> resumeInfo) {
    this.logger.info("SteadyStateIndexManager shut down. Handling exception.");

    // It's possible that while shutting down the SteadyStateIndexManager we were shut down. If so
    // don't do any more work.
    if (this.isShutDown()) {
      return;
    }

    // If the exception was not an SteadyStateException (i.e. was unexpected),
    if (!(throwable instanceof SteadyStateException steadyStateException)) {
      maybeCrashOnUnexpectedThrowable(throwable);
      if (this.featureFlags.isEnabled(Feature.STALE_STATE_TRANSITION)) {
        staleIndex(StaleStatusReason.UNEXPECTED_ERROR, Optional.ofNullable(throwable.getMessage()));
      } else if (this.featureFlags.isEnabled(Feature.RETAIN_FAILED_INDEX_DATA_ON_DISK)) {
        failAndCloseIndex(throwable, IndexStatus.Reason.STEADY_STATE_REPLICATION_FAILED);
      } else {
        failAndDropIndex(throwable, IndexStatus.Reason.STEADY_STATE_REPLICATION_FAILED);
      }
      return;
    }

    this.index
        .getMetricsUpdater()
        .getIndexingMetricsUpdater()
        .getSteadyStateExceptionCounter()
        .increment();
    switch (steadyStateException.getType()) {
      case DROPPED -> {
        this.logger.info("Collection was dropped. Stopping replication and dropping index.");
        handleCollectionDropped();
        return;
      }
      case REQUIRES_RESYNC -> {
        this.logger.info(
            "Exception requiring resync occurred during steady state replication.",
            steadyStateException);
        enqueueInitialSync(IndexStatus.initialSync());
        return;
      }
      case TRANSIENT -> {
        Optional<Throwable> cause = Optional.ofNullable(steadyStateException.getCause());
        Duration operationBackoff = getTransientBackoffDuration(cause, this.transientBackoff);

        this.logger.info(
            "Witnessed transient error. Trying to resume in {} milliseconds.",
            operationBackoff.toMillis(),
            steadyStateException);

        checkState(
            resumeInfo.isPresent(),
            "TRANSIENT SteadyStateException thrown but no ChangeStreamResumeInfo present");

        if (this.indexGeneration.getDefinition().getView().isPresent()) {
          if (MongoViewExceptionUtils.isViewPipelineRelated(cause)) {
            this.metricsFactory.counter("viewPipelineRelatedSteadyStateRetry").increment();

            IndexStatus status =
                IndexStatus.recoveringTransient(
                    MongoViewExceptionUtils.getViewPipelineErrorMessage(
                        Check.isPresent(cause, "cause")));

            // Set index status so that the error message is exposed to the user during the backoff
            this.index.setStatus(status);
          }
        }

        if (QueryRejectedBySettingsExceptionUtils.isQueryRejectedBySettingsError(cause)) {
          this.metricsFactory.counter("queryRejectedBySettingsSteadyStateRetry").increment();

          IndexStatus status =
              IndexStatus.recoveringTransient(
                  QueryRejectedBySettingsExceptionUtils.getQueryRejectedBySettingsErrorMessage(
                      Check.isPresent(cause, "cause")));

          // Set index status so that the error message is exposed to the user during the backoff.
          this.index.setStatus(status);
        }

        // By default, we assume that the matchCollectionUuidForUpdateLookup is supported. If the
        // matchCollectionUuidForUpdateLookup parameter is unsupported, remove it when we resume
        // steady state.
        if (cause.isPresent() && isMatchCollectionUuidUnsupportedException(cause.get())) {
          this.metricsFactory.counter("matchCollectionUUIDForUpdateLookupNotSupported").increment();
          this.removeMatchCollectionUuid = true;
        }

        scheduleResumeSteadyState(resumeInfo.get(), operationBackoff);
        return;
      }
      case NON_INVALIDATING_RESYNC -> {
        handleSteadyStateNonInvalidatingResync(steadyStateException);
        return;
      }
      case FIELD_EXCEEDED -> exceededIndex(String.valueOf(steadyStateException.getMessage()));
      case DOCS_EXCEEDED -> staleIndex(StaleStatusReason.DOCS_EXCEEDED, Optional.empty());
      case RENAMED -> {
        this.logger.info("Witnessed rename event. Resuming immediately.");
        ChangeStreamResumeInfo renameResumeInfo = steadyStateException.getResumeInfo();
        IndexDefinition indexDefinition = this.index.getDefinition();
        this.logger.info(
            "Collection name in the definition is changed from {} to {}.",
            indexDefinition.getLastObservedCollectionName(),
            renameResumeInfo.getNamespace().getCollectionName());
        indexDefinition.setLastObservedCollectionName(
            renameResumeInfo.getNamespace().getCollectionName());
        resumeSteadyState(steadyStateException.getResumeInfo());
        return;
      }
      case INVALIDATED -> {
        this.logger.info("Witnessed invalidate event. Opening new change stream.");
        resumeSteadyState(steadyStateException.getResumeInfo());
        return;
      }
      case SHUT_DOWN -> {
        this.logger.info("Steady state was shut down. Not scheduling any more work.");
        transitionState(State.SHUT_DOWN);
        return;
      }
    }
  }

  /**
   * Returns a backoff duration to wait before resuming steady state replication on TRANSIENT error.
   */
  private Duration getTransientBackoffDuration(Optional<Throwable> cause, Duration defaultBackoff) {
    return cause.isPresent() && Errors.isIngressRequestRateLimitError(cause.get())
        ? DurationUtils.getRandomFullDistributionDuration(this.requestRateLimitBackoff)
        : defaultBackoff;
  }

  protected void handleSteadyStateNonInvalidatingResync(SteadyStateException steadyStateException) {
    this.logger.info(
        "Witnessed exception requiring a new index. "
            + "Committing and marking current index as recovering.",
        steadyStateException);
    Crash.because("failed to commit index").ifThrows(this.documentIndexer::commit);
    this.index.setStatus(IndexStatus.recoveringNonTransient(getLastOptime()));
  }

  protected void handleInitialSyncFailed(InitialSyncException initialSyncException) {
    if (InitialSyncException.isNotablescanError(initialSyncException.getCause())) {
      this.metricsFactory.counter("failedInitialSyncDueToNotablescan").increment();
    }
    if (this.featureFlags.isEnabled(Feature.RETAIN_FAILED_INITIAL_SYNC_DATA_ON_DISK)) {
      failAndCloseIndex(initialSyncException, Reason.INITIAL_SYNC_REPLICATION_FAILED);
    } else {
      failAndDropIndex(initialSyncException, Reason.INITIAL_SYNC_REPLICATION_FAILED);
    }
  }

  private synchronized boolean isShutDown() {
    return this.state.equals(State.SHUT_DOWN);
  }

  private synchronized void transitionState(State destination) {
    if (destination.equals(this.state)) {
      return;
    }

    if (this.state.equals(State.INITIAL_SYNC)
        && (destination.equals(State.STEADY_STATE) || destination.equals(State.SHUT_DOWN))) {
      // Reset if we've successfully completed initial sync or initial sync has shut down.
      this.index
          .getMetricsUpdater()
          .getIndexingMetricsUpdater()
          .getConsecutiveInitialSyncResyncExceptions()
          .set(0);
    }

    // Reset the per-phase _id field type gauge when leaving a replication phase so it reflects only
    // the _id type of indexes currently active in that phase (not completed / discarded ones).
    if (this.state.equals(State.INITIAL_SYNC)) {
      this.index
          .getMetricsUpdater()
          .getReplicationMetricsUpdater()
          .getInitialSyncMetrics()
          .resetIdKeyFieldType();
    } else if (this.state.equals(State.STEADY_STATE)) {
      this.index
          .getMetricsUpdater()
          .getReplicationMetricsUpdater()
          .getSteadyStateMetrics()
          .resetIdKeyFieldType();
    }

    this.metricsFactory
        .counter(
            "transitionState",
            Tags.of(
                "fromState", this.state.name(),
                "toState", destination.name()))
        .increment();

    this.logger.info("Transitioning from {} to {}.", this.state, destination);
    this.state = destination;
  }

  private void scheduleResumeSteadyState(
      ChangeStreamResumeInfo resumeInfo, Duration operationBackoff) {
    Crash.because("failed resuming steady state")
        .ifCompletesExceptionally(
            CompletableFuture.runAsync(
                () -> this.resumeSteadyState(resumeInfo),
                CompletableFuture.delayedExecutor(
                    operationBackoff.toMillis(), TimeUnit.MILLISECONDS, this.lifecycleExecutor)));
  }

  private synchronized void scheduleResync(Throwable cause, IndexStatus indexStatus) {
    Check.checkState(
        indexStatus.getStatusCode() == IndexStatus.StatusCode.INITIAL_SYNC,
        "sync cannot be re-scheduled with %s status",
        indexStatus);

    this.logger.info(
        "Error while trying to sync {}. Retrying in {} seconds",
        this.indexGeneration.getDefinition(),
        this.resyncBackoff.toSeconds(),
        cause);

    this.transitionState(State.INITIAL_SYNC_BACKOFF);

    this.index
        .getMetricsUpdater()
        .getIndexingMetricsUpdater()
        .getConsecutiveInitialSyncResyncExceptions()
        .incrementAndGet();

    Crash.because("failed enqueueing initial sync")
        .ifCompletesExceptionally(
            CompletableFuture.runAsync(
                () -> enqueueInitialSync(indexStatus),
                CompletableFuture.delayedExecutor(
                    this.resyncBackoff.toMillis(), TimeUnit.MILLISECONDS, this.lifecycleExecutor)));
  }

  private synchronized void scheduleResumableResync(Throwable cause, Duration operationBackoff) {
    this.logger.info(
        "Transient error while trying to sync {}. Retrying in {} milliseconds",
        this.indexGeneration.getDefinition(),
        operationBackoff.toMillis(),
        cause);

    this.transitionState(State.INITIAL_SYNC_BACKOFF);

    Crash.because("failed resuming resync")
        .ifCompletesExceptionally(
            CompletableFuture.runAsync(
                this::resumeResync,
                CompletableFuture.delayedExecutor(
                    operationBackoff.toMillis(), TimeUnit.MILLISECONDS, this.lifecycleExecutor)));
  }

  private synchronized void resumeResync() {
    if (isShutDown()) {
      return;
    }

    // Parse the last commit user data to determine whether we should enqueue a resume.
    IndexCommitUserData userData = getIndexCommitUserData();
    if (userData.getInitialSyncResumeInfo().isPresent()) {
      enqueueInitialSyncResume(userData.getInitialSyncResumeInfo().get());
    } else {
      enqueueInitialSync(IndexStatus.initialSync());
    }
  }

  private void scheduleResumeSync(InitialSyncResumeInfo resumeInfo) {
    this.logger.info(
        "Encountered invalidate event while trying to sync {}. Resuming sync now.",
        this.indexGeneration.getDefinition());

    Crash.because("failed resuming initial sync")
        .ifCompletesExceptionally(
            CompletableFuture.runAsync(
                () -> {
                  if (isShutDown()) {
                    return;
                  }
                  enqueueInitialSyncResume(resumeInfo);
                },
                this.lifecycleExecutor));
  }

  private void exceededIndex(String reason) {
    this.logger.info("Failing index that exceeded limits: {}", reason);

    transitionState(State.FAILED_EXCEEDED);
    clearIndex(
        IndexStatus.failed(
            EXCEEDED_LIMIT_REASON_PREFIX + reason, IndexStatus.Reason.EXCEED_MAX_LIMIT),
        IndexCommitUserData.createExceeded(reason));
  }

  private void shutDownReplicationBecauseCollectionDoesNotExist() {
    this.logger.info("Collection not found, shutting down replication for this index");
    IndexStateInfo indexStateInfo =
        IndexStateInfo.create(StatusCode.DOES_NOT_EXIST, Reason.COLLECTION_NOT_FOUND);
    shutdownReplicationAndCommitStatus(indexStateInfo);
  }

  private synchronized void shutdownReplicationAndCommitStatus(IndexStateInfo indexStateInfo) {
    // Already shut down (e.g. this generator's own shutdown() already ran for an unrelated
    // reason): skip the redundant write.
    if (isShutDown()) {
      return;
    }
    transitionState(State.SHUT_DOWN);
    this.index.setStatus(indexStateInfo.toIndexStatus());

    // This assumes that the periodic index committer is already shut down or cannot overwrite this
    // data
    IndexCommitUserData userData = IndexCommitUserData.createFromIndexStateInfo(indexStateInfo);
    this.documentIndexer.updateCommitUserData(userData);
    Crash.because("failed to commit index status: " + indexStateInfo.getStatus())
        .ifThrows(this::commitIndexer);
  }

  private void shutDownReplicationOnStaleIndex(StaleStateInfo staleStateInfo) {
    BsonTimestamp lastOptime = staleStateInfo.getLastOptime();
    this.logger.warn(
        "Stopping replication and serving queries with stale data. Last optime {}. Reason: {}",
        lastOptime,
        staleStateInfo.getMessage());

    // Update optime info metric before setting index to STALE to ensure metrics consistency.
    // Acquire unset lock to ensure another thread does not unset the metric while we update it.
    ReplicationOpTimeInfo opTimeInfo =
        this.index.getMetricsUpdater().getIndexingMetricsUpdater().getReplicationOpTimeInfo();
    synchronized (opTimeInfo.getUnsetLock()) {
      opTimeInfo.update(lastOptime.getValue(), lastOptime.getValue());

      transitionState(State.SHUT_DOWN);
      this.index.setStatus(IndexStatus.stale(staleStateInfo.getMessage(), lastOptime));
    }
  }

  private void staleIndex(StaleStatusReason staleStatusReason, Optional<String> errorMessage) {
    // Already shut down: skip the redundant write. Every current caller already holds the
    // generator monitor and checks isShutDown() before reaching here, but this guard makes the
    // method correct on its own rather than relying on that caller discipline.
    if (isShutDown()) {
      return;
    }
    IndexCommitUserData userData = getIndexCommitUserData();

    String message = staleStatusReason.formatMessage(errorMessage.orElse(""));

    this.logger.warn(
        "Transitioning to STALE index. Reason: {}. Last optime: {}",
        message,
        getLastOptimeFromUserData(userData));

    StaleStateInfo staleStateInfo = StaleStateInfo.create(staleStatusReason, message, userData);

    shutDownReplicationOnStaleIndex(staleStateInfo);

    // commit user data to set STALE status for this index again on restart
    this.documentIndexer.updateCommitUserData(IndexCommitUserData.createStale(staleStateInfo));
    Crash.because("failed to commit stale status").ifThrows(this::commitIndexer);
  }

  private BsonTimestamp getLastOptime() {
    return getLastOptimeFromUserData(getIndexCommitUserData());
  }

  private BsonTimestamp getLastOptimeFromUserData(IndexCommitUserData userData) {
    var resumeInfo =
        userData
            .getResumeInfo()
            .orElseGet(
                () -> {
                  Crash.because("Failed to get change stream resume info from user data").now();
                  return Check.unreachable("Crash.now() should have halted the JVM");
                });
    return Crash.because("failed to parse resume token")
        .ifThrowsExceptionOrError(
            () -> ResumeTokenUtils.opTimeFromResumeToken(resumeInfo.getResumeToken()));
  }

  private void clearIndex(IndexStatus status, IndexCommitUserData indexCommitUserData) {
    // Already shut down: skip the redundant write. Every current caller already holds the
    // generator monitor and checks isShutDown() before reaching here, but this guard makes the
    // method correct on its own rather than relying on that caller discipline.
    if (isShutDown()) {
      return;
    }
    // In order to fully clear the index, we need to:
    //   1. kill all cursors so no more IndexReaders are open against it
    //   2. delete all the documents from the index (commits the map)
    //   3. refresh the IndexReader's view of the index, so any leftover handles to the old view of
    //      the index are removed and data files can be cleaned up
    this.logger.info("Clearing index.");
    killCursors(status);
    Crash.because("failed to clear index").ifThrows(() -> clearIndexer(indexCommitUserData));
  }

  private void commitIndexer() throws IOException {
    runIndexerOperation(this.documentIndexer::commit);
  }

  private void clearIndexer(IndexCommitUserData indexCommitUserData) throws IOException {
    runIndexerOperation(() -> this.documentIndexer.clearIndex(indexCommitUserData));
  }

  /**
   * Runs a commit/clearIndex operation against the document indexer, called from {@link
   * #shutdownReplicationAndCommitStatus}, {@link #staleIndex}, and {@link #clearIndex}. Base
   * implementation just runs it; a subclass whose {@link DocumentIndexer} can reject a write for a
   * reason specific to it may override this to intercept that rejection instead of letting it reach
   * the {@code Crash.ifThrows()} caller and halt the JVM.
   */
  protected void runIndexerOperation(CheckedRunnable<IOException> operation) throws IOException {
    operation.run();
  }

  protected void failAndDropIndex(Throwable throwable, IndexStatus.Reason reason) {
    this.logger.error("Failing due to unexpected error.", throwable);
    incrementFailedIndexCounter(FailedIndexAction.DROP, getState(), throwable);
    transitionState(State.FAILED);
    dropIndex(IndexStatus.failed(failureMessage(FailedIndexAction.DROP, throwable), reason));
  }

  protected void failAndCloseIndex(Throwable throwable, IndexStatus.Reason reason) {
    this.logger.error("Failing due to unexpected error.", throwable);
    incrementFailedIndexCounter(FailedIndexAction.CLOSE, getState(), throwable);
    transitionState(State.FAILED);
    closeIndex(IndexStatus.failed(failureMessage(FailedIndexAction.CLOSE, throwable), reason));
  }

  /**
   * If RETAIN_FAILED_INITIAL_SYNC_DATA_ON_DISK is enabled, retain the failed index data. Otherwise,
   * attempt to resync the index.
   *
   * @param throwable the throwable that caused the failure
   * @param indexStatus the index status to set on failure
   */
  void failRetainOrResync(Throwable throwable, IndexStatus indexStatus) {
    if (this.featureFlags.isEnabled(Feature.RETAIN_FAILED_INITIAL_SYNC_DATA_ON_DISK)) {
      failAndRetainIndex(throwable);
    } else {
      // Old resync behavior.
      scheduleResync(throwable, indexStatus);
    }
  }

  /**
   * If RETAIN_FAILED_INITIAL_SYNC_DATA_ON_DISK is enabled, clear the failed index data. Otherwise,
   * attempt to resync the index.
   *
   * @param throwable the throwable that caused the failure
   * @param indexStatus the index status to set on failure
   */
  void failClearOrResync(Throwable throwable, IndexStatus indexStatus) {
    if (this.featureFlags.isEnabled(Feature.RETAIN_FAILED_INITIAL_SYNC_DATA_ON_DISK)) {
      failAndClearIndex(throwable);
    } else {
      // Old resync behavior.
      scheduleResync(throwable, indexStatus);
    }
  }

  /**
   * If RETAIN_FAILED_INITIAL_SYNC_DATA_ON_DISK is enabled, close the failed index. Otherwise,
   * attempt to resync the index.
   *
   * @param throwable the throwable that caused the failure
   * @param indexStatus the index status to set on failure
   */
  void failCloseOrResync(Throwable throwable, IndexStatus indexStatus) {
    if (this.featureFlags.isEnabled(Feature.RETAIN_FAILED_INITIAL_SYNC_DATA_ON_DISK)) {
      failAndCloseIndex(throwable, Reason.INITIAL_SYNC_REPLICATION_FAILED);
    } else {
      scheduleResync(throwable, indexStatus);
    }
  }

  /**
   * Clear the index data and mark the index as failed. The index data is removed but the index
   * entry is preserved so recovery can create a new initial sync attempt.
   *
   * @param throwable the throwable that caused the failure
   */
  void failAndClearIndex(Throwable throwable) {
    this.logger.error("Failing due to unexpected error.", throwable);
    incrementFailedIndexCounter(FailedIndexAction.CLEAR, getState(), throwable);
    transitionState(State.FAILED);
    clearIndex(
        IndexStatus.failed(
            failureMessage(FailedIndexAction.CLEAR, throwable),
            Reason.INITIAL_SYNC_REPLICATION_FAILED_RETRY),
        IndexCommitUserData.EMPTY);
  }

  /**
   * Retain the data for the failed index and mark the failed index as recoverable.
   *
   * @param throwable the throwable that caused the failure
   */
  void failAndRetainIndex(Throwable throwable) {
    this.logger.error("Failing due to unexpected error.", throwable);
    incrementFailedIndexCounter(FailedIndexAction.RETAIN, getState(), throwable);
    transitionState(State.FAILED);
    // Close keeps the current generation's index data on disk so a rebuild can reuse it.
    closeIndex(
        IndexStatus.failed(
            failureMessage(FailedIndexAction.RETAIN, throwable),
            Reason.INITIAL_SYNC_REPLICATION_FAILED_RETRY));
  }

  private String failureMessage(FailedIndexAction action, Throwable throwable) {
    return String.format(
        "replication failed (action=%s)%s",
        action.name().toLowerCase(), throwable == null ? "" : ": " + throwable.getMessage());
  }

  private void incrementFailedIndexCounter(
      FailedIndexAction action, State state, Throwable throwable) {
    if (throwable == null) {
      return;
    }
    this.metricsFactory
        .counter(
            "failed_index",
            Tags.of(
                "action",
                action.name(),
                "state",
                state.name(),
                "error",
                exceptionClassWithErrorCode(throwable)))
        .increment();
  }

  private void closeIndex(IndexStatus status) {
    Crash.because("failed to close index")
        .ifThrows(
            () -> {
              killCursors(status);
              this.index.close();
              this.synonymMappingManagers.forEach(SynonymMappingManager::shutdown);
            });
  }

  /**
   * Handles the tailed collection being dropped out from under this generation — a change-stream
   * drop event during steady state, or the collection no longer resolving mid-initial-sync after it
   * had been resolved. Base behavior shuts the generation down and drops the local index.
   * Subclasses that own shared resources keyed to the dropped collection can override to clean
   * those up first, while the generation — and any leadership tied to it — is still live; once
   * {@link #dropIndex()} runs, the state is terminal.
   *
   * <p>A collection that is merely not visible yet (start-of-sync resolution failure) never reaches
   * this: it is classified {@code DOES_NOT_EXIST} and retried.
   */
  protected void handleCollectionDropped() {
    dropIndex();
  }

  private void dropIndex() {
    transitionState(State.SHUT_DOWN);
    dropIndex(IndexStatus.doesNotExist(IndexStatus.Reason.INDEX_DROPPED));
  }

  private void dropIndex(IndexStatus status) {
    if (status.getReason().isPresent()) {
      this.metricsFactory
          .counter(
              "indexesDroppedCounter",
              Tags.of("reason", status.getReason().get().name().toLowerCase()))
          .increment();
    } else {
      this.metricsFactory
          .counter("indexesDroppedCounter", Tags.of("reason", INDEX_DROPPED_COUNTER_UNKNOWN_TAG))
          .increment();
    }
    Crash.because("failed to drop index")
        .ifThrows(
            () -> {
              killCursors(status);
              this.index.close();
              this.index.drop();
              this.synonymMappingManagers.forEach(SynonymMappingManager::shutdown);
            });
  }

  private void killCursors(IndexStatus status) {
    Crash.because("failed to kill index cursors")
        .ifThrows(
            () -> {
              // First set the index to its closed status so that no new cursors can be created.
              this.index.setStatus(status);

              // Then kill existing cursors so no searches are run on the Index after it's closed.
              this.cursorManager.killIndexCursors(this.indexGeneration.getGenerationId());
            });
  }

  @VisibleForTesting
  static void maybeCrashOnUnexpectedThrowable(Throwable throwable) {
    @Var Optional<Throwable> reason = Optional.of(throwable);
    while (reason.isPresent()) {
      // Crash instead of fail/stale an index when OOM during replication.
      if (reason.get() instanceof OutOfMemoryError) {
        // Log the complete stack trace for debugging.
        Crash.because("OOM during replication").withThrowable(throwable).now();
      }
      // The OOM error may appear in the cause. Check the cause recursively.
      reason = Optional.ofNullable(reason.get().getCause());
    }
  }

  private IndexCommitUserData getIndexCommitUserData() {
    EncodedUserData encodedUserData = this.index.getWriter().getCommitUserData();
    return IndexCommitUserData.fromEncodedData(
        encodedUserData, Optional.of(this.indexGeneration.getGenerationId()));
  }

  @VisibleForTesting
  protected static String exceptionClassWithErrorCode(Throwable throwable) {
    if (throwable instanceof MongoException) {
      return throwable.getClass().getSimpleName() + ":" + ((MongoException) throwable).getCode();
    }

    return throwable.getClass().getSimpleName();
  }
}
