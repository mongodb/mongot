package com.xgen.mongot.replication.mongodb;

import static com.xgen.mongot.replication.mongodb.ReplicationIndexManager.EXCEEDED_LIMIT_REASON_PREFIX;

import com.google.errorprone.annotations.concurrent.GuardedBy;
import com.xgen.mongot.catalog.IndexCatalog;
import com.xgen.mongot.catalog.InitializedIndexCatalog;
import com.xgen.mongot.cursor.MongotCursorManager;
import com.xgen.mongot.featureflag.Feature;
import com.xgen.mongot.featureflag.FeatureFlags;
import com.xgen.mongot.index.EncodedUserData;
import com.xgen.mongot.index.IndexGeneration;
import com.xgen.mongot.index.InitializedIndex;
import com.xgen.mongot.index.ReplicationOpTimeInfo;
import com.xgen.mongot.index.status.IndexStatus;
import com.xgen.mongot.index.version.GenerationId;
import com.xgen.mongot.metrics.MetricsFactory;
import com.xgen.mongot.replication.ReplicationManager;
import com.xgen.mongot.replication.mongodb.common.ChangeStreamResumeInfo;
import com.xgen.mongot.replication.mongodb.common.IndexCommitUserData;
import com.xgen.mongot.replication.mongodb.common.IndexStateInfo;
import com.xgen.mongot.replication.mongodb.common.ReplicationOptimeUpdater;
import com.xgen.mongot.replication.mongodb.common.ResumeTokenUtils;
import com.xgen.mongot.replication.mongodb.common.StaleStateInfo;
import com.xgen.mongot.util.Crash;
import com.xgen.mongot.util.concurrent.Executors;
import com.xgen.mongot.util.concurrent.NamedExecutorService;
import com.xgen.mongot.util.mongodb.SyncSourceConfig;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import org.bson.BsonTimestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * MongoDbNoOpReplicationManager is the no-op version of {@link MongoDbReplicationManager} This
 * class will be created when replication is shutdown so no replication could be performed It only
 * kill cursors when dropping indexes
 */
public class MongoDbNoOpReplicationManager implements ReplicationManager {
  private static final Logger LOG = LoggerFactory.getLogger(MongoDbNoOpReplicationManager.class);
  private final MetricsFactory metricsFactory;
  private final Optional<SyncSourceConfig> syncSourceConfig;
  private final MongotCursorManager cursorManager;
  private final InitializedIndexCatalog initializedIndexCatalog;
  private final NamedExecutorService lifeCycleExecutor;
  private final FeatureFlags featureFlags;
  private final MeterRegistry meterRegistry;
  private final ReplicationOptimeUpdater replicationOptimeUpdater;
  private final AtomicLong noOpManagerUp;

  @GuardedBy("this")
  private final List<CompletableFuture<Void>> initFutures;

  private MongoDbNoOpReplicationManager(
      Optional<SyncSourceConfig> syncSourceConfig,
      MongotCursorManager cursorManager,
      InitializedIndexCatalog initializedIndexCatalog,
      ReplicationOptimeUpdater replicationOptimeUpdater,
      FeatureFlags featureFlags,
      MeterRegistry meterRegistry) {
    LOG.info("creating MongoDbNoOpReplicationManager");
    this.syncSourceConfig = syncSourceConfig;
    this.cursorManager = cursorManager;
    this.initializedIndexCatalog = initializedIndexCatalog;
    this.replicationOptimeUpdater = replicationOptimeUpdater;
    this.initFutures = new ArrayList<>();
    this.meterRegistry = meterRegistry;
    this.metricsFactory = new MetricsFactory("replication.mongodb", meterRegistry);
    this.lifeCycleExecutor =
        Executors.fixedSizeThreadPool("no-op-indexing-lifecycle", 1, meterRegistry);
    this.noOpManagerUp =
        this.metricsFactory.numGauge(
            "manager", Tags.of("type", syncSourceConfig.isEmpty() ? "noOpMmsDown" : "noOp"));
    this.noOpManagerUp.incrementAndGet();
    this.featureFlags = featureFlags;
  }

  public static MongoDbNoOpReplicationManager create(
      Optional<SyncSourceConfig> syncSourceConfig,
      MongotCursorManager cursorManager,
      IndexCatalog indexCatalog,
      InitializedIndexCatalog initializedIndexCatalog,
      FeatureFlags featureFlags,
      MeterRegistry meterRegistry) {

    var replicationOptimeMetricUpdater =
        ReplicationOptimeUpdater.create(
            indexCatalog,
            initializedIndexCatalog,
            syncSourceConfig,
            ReplicationOptimeUpdater.DEFAULT_UPDATE_INTERVAL,
            meterRegistry);

    return new MongoDbNoOpReplicationManager(
        syncSourceConfig,
        cursorManager,
        initializedIndexCatalog,
        replicationOptimeMetricUpdater,
        featureFlags,
        meterRegistry);
  }

  @Override
  public boolean isReplicationSupported() {
    return false;
  }

  @Override
  public synchronized void add(IndexGeneration indexGeneration) {
    this.initializedIndexCatalog
        .getIndex(indexGeneration.getGenerationId())
        .ifPresent(
            initializedIndex -> {
              this.initFutures.add(
                  CompletableFuture.runAsync(
                      () -> resetStatus(initializedIndex), this.lifeCycleExecutor));
            });
  }

  @Override
  public synchronized CompletableFuture<Void> dropIndex(GenerationId generationId) {
    return this.initializedIndexCatalog
        .getIndex(generationId)
        .map(
            index -> {
              index.setStatus(IndexStatus.doesNotExist(IndexStatus.Reason.INDEX_DROPPED));
              return CompletableFuture.runAsync(
                  () -> killCursors(generationId), this.lifeCycleExecutor);
            })
        .orElseGet(() -> CompletableFuture.completedFuture(null));
  }

  private void killCursors(GenerationId generationId) {
    Crash.because("failed to kill index cursors")
        .ifThrows(
            () -> {
              // Kill existing cursors so no searches are run on the Index after it's closed.
              // Note: We do NOT close or drop the index here because IndexActions.dropIndex()
              // handles closing and dropping the index after this future completes.
              this.cursorManager.killIndexCursors(generationId);
            });
  }

  /**
   * Index status will be recovered same as init() inside{@link ReplicationIndexManager}, any
   * changes there should be reflected here too
   *
   * <p>Indexes would be set as fail(exceededLimit), stale(staleInfo), initial_sync, steady and
   * notStarted.
   */
  // TODO(CLOUDP-280897): Make the status recovery logic in a single place
  private void resetStatus(InitializedIndex index) {
    EncodedUserData encodedUserData = index.getWriter().getCommitUserData();
    IndexCommitUserData userData =
        IndexCommitUserData.fromEncodedData(encodedUserData, Optional.empty());
    Optional<String> exceededReason = userData.getExceededLimitsReason();
    if (exceededReason.isPresent()) {
      index.setStatus(IndexStatus.failed(EXCEEDED_LIMIT_REASON_PREFIX + exceededReason.get()));
      return;
    }

    Optional<StaleStateInfo> staleStateInfo = userData.getStaleStateInfo();
    if (staleStateInfo.isPresent()) {
      // Update optime info metric before setting index to STALE to ensure metrics consistency.
      // Acquire unset lock to ensure another thread does not unset the metric while we update it.
      ReplicationOpTimeInfo opTimeInfo =
          index.getMetricsUpdater().getIndexingMetricsUpdater().getReplicationOpTimeInfo();
      BsonTimestamp lastOptime = staleStateInfo.get().getLastOptime();
      synchronized (opTimeInfo.getUnsetLock()) {
        opTimeInfo.update(lastOptime.getValue(), lastOptime.getValue());
        index.setStatus(
            IndexStatus.stale(
                staleStateInfo.get().getMessage(), staleStateInfo.get().getLastOptime()));
      }
      return;
    }

    if (this.featureFlags.isEnabled(Feature.SHUT_DOWN_REPLICATION_WHEN_COLLECTION_NOT_FOUND)) {
      Optional<IndexStateInfo> indexStateInfo = userData.getIndexStateInfo();
      if (indexStateInfo.isPresent() && indexStateInfo.get().isCollectionNotFound()) {
        index.setStatus(IndexStatus.doesNotExist(IndexStatus.Reason.COLLECTION_NOT_FOUND));
        return;
      }
    }

    if (userData.getInitialSyncResumeInfo().isPresent()) {
      index.setStatus(IndexStatus.initialSync());
      return;
    }

    if (userData.getResumeInfo().isPresent()) {
      ChangeStreamResumeInfo resumeInfo = userData.getResumeInfo().get();
      // A resume token optime will have an inc that is one greater than the "actual" optime of the
      // last event in the change stream. (see: https://tinyurl.com/ydbjkv8k)
      // We subtract one from the inc value of the resume token to get the operation time of the
      // last indexed event, to match the optime that would be set by the change stream batch
      // application process.
      BsonTimestamp resumeTokenOpTime =
          Crash.because("failed to parse resume token")
              .ifThrows(() -> ResumeTokenUtils.opTimeFromResumeToken(resumeInfo.getResumeToken()));
      BsonTimestamp lastIndexedOpTime =
          new BsonTimestamp(
              resumeTokenOpTime.getTime(), Math.max(0, resumeTokenOpTime.getInc() - 1));
      ReplicationOpTimeInfo opTimeInfo =
          index.getMetricsUpdater().getIndexingMetricsUpdater().getReplicationOpTimeInfo();

      // Update optime info metric before setting index to STEADY to ensure metrics consistency.
      // Acquire unset lock to ensure another thread does not unset the metric while we update it.
      synchronized (opTimeInfo.getUnsetLock()) {
        opTimeInfo.update(lastIndexedOpTime.getValue(), lastIndexedOpTime.getValue());
        index.setStatus(IndexStatus.steady());
      }

      return;
    }

    index.setStatus(IndexStatus.notStarted());
  }

  @Override
  public Optional<SyncSourceConfig> getSyncSourceConfig() {
    return this.syncSourceConfig;
  }

  @Override
  public synchronized boolean isInitialized() {
    boolean isInitialized =
        this.initFutures.stream()
            .allMatch(initFuture -> initFuture.isDone() && !initFuture.isCompletedExceptionally());
    if (isInitialized) {
      this.initFutures.clear();
    }
    return isInitialized;
  }

  /**
   * There is no need to wait for completable future from dropping index as this has been waited
   * inside {@link com.xgen.mongot.config.manager.IndexActions}.
   */
  @Override
  public CompletableFuture<Void> shutdown() {
    var shutdownExecutor =
        Executors.fixedSizeThreadPool("replication-manager-shutdown", 1, this.meterRegistry);

    this.noOpManagerUp.decrementAndGet();
    return CompletableFuture.runAsync(this.replicationOptimeUpdater::close, shutdownExecutor)
        .thenRunAsync(shutdownExecutor::shutdown, shutdownExecutor);
  }
}
