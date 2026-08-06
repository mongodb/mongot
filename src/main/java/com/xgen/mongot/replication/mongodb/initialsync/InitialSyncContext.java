package com.xgen.mongot.replication.mongodb.initialsync;

import com.xgen.mongot.index.DocumentEvent;
import com.xgen.mongot.index.IndexMetricsUpdater;
import com.xgen.mongot.index.IndexMetricsUpdater.ReplicationMetricsUpdater.InitialSyncMetrics;
import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.definition.IndexDefinitionGeneration;
import com.xgen.mongot.index.version.GenerationId;
import com.xgen.mongot.index.version.IndexFormatVersion;
import com.xgen.mongot.monitor.Gate;
import com.xgen.mongot.replication.mongodb.common.AutoEmbeddingMaterializedViewConfig;
import com.xgen.mongot.replication.mongodb.common.DocumentIndexer;
import com.xgen.mongot.replication.mongodb.common.IndexCommitUserData;
import com.xgen.mongot.replication.mongodb.common.IndexingWorkScheduler;
import com.xgen.mongot.replication.mongodb.common.InitialSyncException;
import com.xgen.mongot.replication.mongodb.common.PauseInitialSyncException;
import com.xgen.mongot.replication.mongodb.common.SchedulerQueue;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.bson.BsonTimestamp;
import org.bson.types.ObjectId;

/** InitialSyncContext consolidates data and interfaces used throughout initial sync. */
public class InitialSyncContext {

  public final IndexDefinitionGeneration indexDefinitionGeneration;
  public final IndexingWorkScheduler indexingWorkScheduler;
  public final DocumentIndexer indexer;
  public final IndexMetricsUpdater indexMetricsUpdater;

  // Refers to the change stream buffer start time for buffered initial syncs, or the change stream
  // high water mark for bufferless initial syncs.
  private final Optional<BsonTimestamp> changeStreamResumeOperationTime;
  private final Optional<BsonTimestamp> changeStreamBufferEndOperationTime;
  private final Optional<Integer> embeddingGetMoreBatchSize;
  private final boolean useNaturalOrderScan;
  private final boolean enableParallelNaturalOrderScan;
  private final int parallelPartitionCount;
  private final boolean removeMatchCollectionUuid;
  private final Gate initialSyncGate;
  private final Optional<AutoEmbeddingMaterializedViewConfig> autoEmbeddingMaterializedViewConfig;

  private InitialSyncContext(
      IndexDefinitionGeneration indexDefinitionGeneration,
      IndexingWorkScheduler indexingWorkScheduler,
      DocumentIndexer indexer,
      Optional<BsonTimestamp> changeStreamResumeOperationTime,
      Optional<BsonTimestamp> changeStreamBufferEndOperationTime,
      IndexMetricsUpdater indexMetricsUpdater,
      Optional<Integer> embeddingGetMoreBatchSize,
      boolean removeMatchCollectionUuid,
      boolean useNaturalOrderScan,
      boolean enableParallelNaturalOrderScan,
      int parallelPartitionCount,
      Gate initialSyncGate,
      Optional<AutoEmbeddingMaterializedViewConfig> autoEmbeddingMaterializedViewConfig) {

    this.indexDefinitionGeneration = indexDefinitionGeneration;
    this.indexingWorkScheduler = indexingWorkScheduler;
    this.indexer = indexer;
    this.changeStreamResumeOperationTime = changeStreamResumeOperationTime;
    this.changeStreamBufferEndOperationTime = changeStreamBufferEndOperationTime;
    this.indexMetricsUpdater = indexMetricsUpdater;
    this.embeddingGetMoreBatchSize =
        this.getIndexDefinition().isAutoEmbeddingIndex()
            ? embeddingGetMoreBatchSize
            : Optional.empty();
    this.useNaturalOrderScan = useNaturalOrderScan;
    this.enableParallelNaturalOrderScan = enableParallelNaturalOrderScan;
    this.parallelPartitionCount = parallelPartitionCount;
    this.removeMatchCollectionUuid = removeMatchCollectionUuid;
    this.initialSyncGate = initialSyncGate;
    this.autoEmbeddingMaterializedViewConfig = autoEmbeddingMaterializedViewConfig;
  }

  static InitialSyncContext create(
      IndexDefinitionGeneration indexDefinitionGeneration,
      IndexingWorkScheduler indexingWorkScheduler,
      DocumentIndexer indexer,
      IndexMetricsUpdater indexMetricsUpdater,
      Optional<Integer> embeddingGetMoreBatchSize,
      boolean removeMatchCollectionUuid,
      boolean useNaturalOrderScan,
      boolean enableParallelNaturalOrderScan,
      int parallelPartitionCount,
      Gate initialSyncGate) {
    return create(
        indexDefinitionGeneration,
        indexingWorkScheduler,
        indexer,
        indexMetricsUpdater,
        embeddingGetMoreBatchSize,
        removeMatchCollectionUuid,
        useNaturalOrderScan,
        enableParallelNaturalOrderScan,
        parallelPartitionCount,
        initialSyncGate,
        Optional.empty());
  }

  static InitialSyncContext create(
      IndexDefinitionGeneration indexDefinitionGeneration,
      IndexingWorkScheduler indexingWorkScheduler,
      DocumentIndexer indexer,
      IndexMetricsUpdater indexMetricsUpdater,
      Optional<Integer> embeddingGetMoreBatchSize,
      boolean removeMatchCollectionUuid,
      boolean useNaturalOrderScan,
      boolean enableParallelNaturalOrderScan,
      int parallelPartitionCount,
      Gate initialSyncGate,
      Optional<AutoEmbeddingMaterializedViewConfig> autoEmbeddingMaterializedViewConfig) {

    return new InitialSyncContext(
        indexDefinitionGeneration,
        indexingWorkScheduler,
        indexer,
        Optional.empty(),
        Optional.empty(),
        indexMetricsUpdater,
        embeddingGetMoreBatchSize,
        removeMatchCollectionUuid,
        useNaturalOrderScan,
        enableParallelNaturalOrderScan,
        parallelPartitionCount,
        initialSyncGate,
        autoEmbeddingMaterializedViewConfig);
  }

  public GenerationId getGenerationId() {
    return this.indexDefinitionGeneration.getGenerationId();
  }

  public IndexDefinition getIndexDefinition() {
    return this.indexDefinitionGeneration.getIndexDefinition();
  }

  public ObjectId getIndexId() {
    return this.indexDefinitionGeneration.getIndexId();
  }

  public IndexFormatVersion getIndexFormatVersion() {
    return this.indexDefinitionGeneration.generation().indexFormatVersion;
  }

  public InitialSyncMetrics getInitialSyncMetricsUpdater() {
    return this.indexMetricsUpdater.getReplicationMetricsUpdater().getInitialSyncMetrics();
  }

  public IndexMetricsUpdater getIndexMetricsUpdater() {
    return this.indexMetricsUpdater;
  }

  public Optional<AutoEmbeddingMaterializedViewConfig> getAutoEmbeddingMaterializedViewConfig() {
    return this.autoEmbeddingMaterializedViewConfig;
  }

  /**
   * Gets the operation time at which collection change stream buffering began or the change stream
   * high water mark.
   *
   * <p>Throws if called on an {@code InitialSyncContext} without this information.
   */
  public BsonTimestamp getChangeStreamResumeOperationTime() {
    return this.changeStreamResumeOperationTime.orElseThrow();
  }

  /**
   * Gets the operation time at which collection change stream buffering ended.
   *
   * <p>Throws if called on an {@code InitialSyncContext} without this information.
   */
  public BsonTimestamp getChangeStreamBufferEndOperationTime() {
    return this.changeStreamBufferEndOperationTime.orElseThrow();
  }

  /**
   * Gets the batch size used for collection scan and change stream operations.
   *
   * <p>Batch size is only used for auto-embedding indexes to reduce memory pressure during
   * embedding retrieval and control the granularity of indexing progress updates. It will always be
   * empty if the index does not have vector auto-embedding fields.
   */
  public Optional<Integer> getEmbeddingGetMoreBatchSize() {
    return this.embeddingGetMoreBatchSize;
  }

  /**
   * Returns true if the matchCollectionUuidForUpdateLookup change stream parameter should be
   * removed because is unsupported.
   */
  public boolean isRemoveMatchCollectionUuid() {
    return this.removeMatchCollectionUuid;
  }

  public boolean useNaturalOrderScan() {
    return this.useNaturalOrderScan;
  }

  /**
   * Returns true if the natural-order collection scan may be split into disjoint {@code $recordId}
   * partitions scanned in parallel. Only meaningful alongside {@link #useNaturalOrderScan()}.
   */
  public boolean isParallelNaturalOrderScanEnabled() {
    return this.enableParallelNaturalOrderScan;
  }

  /** Returns the number of partitions to use when parallel natural-order scan is enabled. */
  public int getParallelPartitionCount() {
    return this.parallelPartitionCount;
  }

  /**
   * Schedules a batch of document events to be indexed. Shortcut for {@link
   * IndexingWorkScheduler#schedule}.
   *
   * @param batch Documents to be indexed.
   * @param priority Index priority for documents.
   * @param commitUserData User index data to commit.
   * @return Future to index document events.
   * @throws InitialSyncException if initial syncs are paused.
   */
  public CompletableFuture<Void> schedule(
      List<DocumentEvent> batch,
      SchedulerQueue.Priority priority,
      IndexCommitUserData commitUserData) {

    if (this.initialSyncGate.isClosed()) {
      return CompletableFuture.failedFuture(
          InitialSyncException.createResumableTransient(
              new PauseInitialSyncException("Initial syncs are paused")));
    }
    return this.indexingWorkScheduler.schedule(
        batch,
        priority,
        this.indexer,
        getGenerationId(),
        Optional.empty(),
        Optional.of(commitUserData),
        this.indexMetricsUpdater.getIndexingMetricsUpdater());
  }

  /**
   * Cancels all batches for this context's GenerationId.
   *
   * <p>Shortcut for {@link IndexingWorkScheduler#cancel}
   */
  public CompletableFuture<Void> cancel(Throwable reason) {
    return this.indexingWorkScheduler.cancel(getGenerationId(), Optional.empty(), reason);
  }

  /** A string used to uniquely identify InitialSyncContext. */
  public String uniqueString() {
    return this.indexDefinitionGeneration.getGenerationId().uniqueString();
  }

  /**
   * Creates a new {@code InitialSyncContext} with additional context from an in-progress change
   * stream buffer operation or an in-progress change stream event application.
   */
  InitialSyncContext withProgress(BsonTimestamp changeStreamResumeOperationTime) {
    return withProgress(Optional.of(changeStreamResumeOperationTime), Optional.empty());
  }

  /**
   * Creates a new {@code InitialSyncContext} with additional context from a completed change stream
   * buffer operation.
   */
  InitialSyncContext withProgress(
      BsonTimestamp changeStreamResumeOperationTime,
      BsonTimestamp changeStreamBufferEndOperationTime) {
    return withProgress(
        Optional.of(changeStreamResumeOperationTime),
        Optional.of(changeStreamBufferEndOperationTime));
  }

  private InitialSyncContext withProgress(
      Optional<BsonTimestamp> changeStreamResumeOperationTime,
      Optional<BsonTimestamp> changeStreamBufferEndOperationTime) {
    return new InitialSyncContext(
        this.indexDefinitionGeneration,
        this.indexingWorkScheduler,
        this.indexer,
        changeStreamResumeOperationTime,
        changeStreamBufferEndOperationTime,
        this.indexMetricsUpdater,
        this.embeddingGetMoreBatchSize,
        this.removeMatchCollectionUuid,
        this.useNaturalOrderScan,
        this.enableParallelNaturalOrderScan,
        this.parallelPartitionCount,
        this.initialSyncGate,
        this.autoEmbeddingMaterializedViewConfig);
  }
}
