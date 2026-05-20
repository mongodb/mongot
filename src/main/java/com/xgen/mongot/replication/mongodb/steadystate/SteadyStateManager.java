package com.xgen.mongot.replication.mongodb.steadystate;

import static com.xgen.mongot.util.Check.checkState;

import com.google.common.annotations.VisibleForTesting;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import com.xgen.mongot.index.IndexMetricsUpdater;
import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.version.GenerationId;
import com.xgen.mongot.metrics.MeterAndFtdcRegistry;
import com.xgen.mongot.replication.mongodb.common.ChangeStreamResumeInfo;
import com.xgen.mongot.replication.mongodb.common.DecodingWorkScheduler;
import com.xgen.mongot.replication.mongodb.common.DocumentIndexer;
import com.xgen.mongot.replication.mongodb.common.IdTypeObservingDocumentIndexer;
import com.xgen.mongot.replication.mongodb.common.IndexingWorkSchedulerFactory;
import com.xgen.mongot.replication.mongodb.common.SessionRefresher;
import com.xgen.mongot.replication.mongodb.common.SteadyStateException;
import com.xgen.mongot.replication.mongodb.steadystate.changestream.ChangeStreamManager;
import com.xgen.mongot.replication.mongodb.steadystate.changestream.SteadyStateReplicationConfig;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.Crash;
import com.xgen.mongot.util.mongodb.BatchMongoClient;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The SteadyStateManager manages the lifecycle of the steady state indexing process. In particular
 * it manages the lifecycle of SteadyStateIndexManagers.
 *
 * <p>At run time there is intended to only be a single SteadyStateManager.
 */
public class SteadyStateManager {

  private static final Logger LOG = LoggerFactory.getLogger(SteadyStateManager.class);

  private final ChangeStreamManager changeStreamManager;

  @GuardedBy("this")
  private final Map<GenerationId, SteadyStateIndexManager> indexManagers;

  @GuardedBy("this")
  private boolean shutdown;

  @VisibleForTesting
  SteadyStateManager(
      ChangeStreamManager changeStreamManager,
      Map<GenerationId, SteadyStateIndexManager> indexManagers) {
    this.changeStreamManager = changeStreamManager;
    this.indexManagers = indexManagers;

    this.shutdown = false;
  }

  /** Creates a new SteadyStateManager. */
  public static SteadyStateManager create(
      MeterAndFtdcRegistry meterAndFtdcRegistry,
      SessionRefresher sessionRefresher,
      IndexingWorkSchedulerFactory indexingWorkSchedulerFactory,
      com.mongodb.client.MongoClient syncMongoClient,
      BatchMongoClient syncBatchMongoClient,
      DecodingWorkScheduler decodingWorkScheduler,
      SteadyStateReplicationConfig replicationConfig,
      boolean enableLifecycleAttributionMetrics) {
    Check.argIsPositive(
        replicationConfig.getNumConcurrentChangeStreams(), "numConcurrentChangeStreams");
    Check.argIsPositive(
        replicationConfig.getChangeStreamQueryMaxTimeMs(), "changeStreamQueryMaxTimeMs");
    Check.argIsPositive(
        replicationConfig.getChangeStreamCursorMaxTimeSec(), "changeStreamCursorMaxTimeSec");

    return new SteadyStateManager(
        ChangeStreamManager.create(
            meterAndFtdcRegistry,
            sessionRefresher,
            syncMongoClient,
            syncBatchMongoClient,
            indexingWorkSchedulerFactory,
            decodingWorkScheduler,
            replicationConfig,
            enableLifecycleAttributionMetrics),
        new HashMap<>());
  }

  /**
   * Gracefully shuts down the SteadyStateManager.
   *
   * @return a future that completes when the SteadyStateManager has completed shutting down. The
   *     future will only ever complete successfully.
   */
  public synchronized CompletableFuture<Void> shutdown() {
    LOG.info("Shutting down.");

    this.shutdown = true;

    return this.changeStreamManager.shutdown();
  }

  /**
   * Creates a new SteadyStateIndexManager for the supplied index, which in turn begins the steady
   * state indexing process.
   *
   * <p>Returns a CompletableFuture that completes exceptionally when an exception occurs during
   * steady state indexing. This CompletableFuture should never complete successfully.
   */
  public CompletableFuture<Void> add(
      GenerationId generationId,
      DocumentIndexer documentIndexer,
      IndexDefinition indexDefinition,
      IndexMetricsUpdater indexMetricsUpdater,
      ChangeStreamResumeInfo resumeInfo,
      boolean removeMatchCollectionUuid)
      throws SteadyStateException {
    synchronized (this) { // https://github.com/mockito/mockito/issues/2970
      checkState(
          !this.indexManagers.containsKey(generationId),
          "SteadyStateManager already contains index %s",
          generationId);

      if (this.shutdown) {
        throw SteadyStateException.createShutDown();
      }

      AtomicReference<ChangeStreamResumeInfo> resumeInfoReference =
          new AtomicReference<>(resumeInfo);
      Consumer<ChangeStreamResumeInfo> resumeInfoUpdater = resumeInfoReference::set;
      Supplier<ChangeStreamResumeInfo> resumeInfoSupplier = resumeInfoReference::get;

      DocumentIndexer observingIndexer =
          IdTypeObservingDocumentIndexer.wrap(
              documentIndexer,
              typeName ->
                  indexMetricsUpdater
                      .getReplicationMetricsUpdater()
                      .getSteadyStateMetrics()
                      .reportIdKeyFieldType(typeName));

      CompletableFuture<Void> changeStreamLifecycleFuture =
          this.changeStreamManager.add(
              generationId,
              observingIndexer,
              indexDefinition,
              resumeInfo,
              resumeInfoUpdater,
              indexMetricsUpdater,
              removeMatchCollectionUuid);

      SteadyStateIndexManager indexManager =
          SteadyStateIndexManager.create(resumeInfoSupplier, changeStreamLifecycleFuture);
      this.indexManagers.put(generationId, indexManager);

      return indexManager.getFuture();
    }
  }

  /**
   * Gracefully shuts down the steady state indexing of the index with the supplied id.
   *
   * @param generationId the id of the index to stop steady state
   * @return a future that completes when the scheduled work for the index has completed. The future
   *     will only ever complete successfully.
   */
  public synchronized CompletableFuture<ChangeStreamResumeInfo> stop(GenerationId generationId) {
    checkState(
        this.indexManagers.containsKey(generationId),
        "SteadyStateManager does not contain index %s",
        generationId);

    SteadyStateIndexManager indexManager = this.indexManagers.remove(generationId);
    return Crash.because("failed stopping change stream")
        .ifCompletesExceptionally(
            this.changeStreamManager
                .stop(generationId)
                .thenApply(ignored -> indexManager.getResumeInfo()));
  }
}
