package com.xgen.mongot.index.autoembedding;

import static com.xgen.mongot.embedding.mongodb.leasing.LeaseManager.DEFAULT_INDEX_DEFINITION_VERSION;
import static com.xgen.mongot.index.definition.IndexDefinitionGeneration.Type.AUTO_EMBEDDING;
import static com.xgen.mongot.index.definition.MaterializedViewIndexDefinitionGeneration.MIN_VERSION_FOR_MATERIALIZED_VIEW_EMBEDDING;

import com.xgen.mongot.cursor.batch.BatchSizeStrategy;
import com.xgen.mongot.cursor.batch.QueryCursorOptions;
import com.xgen.mongot.embedding.config.MaterializedViewCollectionMetadata;
import com.xgen.mongot.embedding.config.MaterializedViewCollectionMetadata.MaterializedViewSchemaMetadata;
import com.xgen.mongot.embedding.exceptions.MaterializedViewNonTransientException;
import com.xgen.mongot.embedding.exceptions.MaterializedViewTransientException;
import com.xgen.mongot.embedding.mongodb.leasing.LeaseManager;
import com.xgen.mongot.index.EncodedUserData;
import com.xgen.mongot.index.IndexMetricValuesSupplier;
import com.xgen.mongot.index.IndexMetrics;
import com.xgen.mongot.index.IndexMetricsUpdater;
import com.xgen.mongot.index.IndexTypeData;
import com.xgen.mongot.index.IndexUnavailableException;
import com.xgen.mongot.index.InitializedAutoEmbedIndex;
import com.xgen.mongot.index.MetaResults;
import com.xgen.mongot.index.ReaderClosedException;
import com.xgen.mongot.index.VectorIndexReader;
import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.definition.IndexDefinitionGeneration;
import com.xgen.mongot.index.definition.MaterializedViewIndexDefinitionGeneration;
import com.xgen.mongot.index.lucene.EmptySearchBatchProducer;
import com.xgen.mongot.index.mongodb.MaterializedViewWriter;
import com.xgen.mongot.index.query.InvalidQueryException;
import com.xgen.mongot.index.query.MaterializedVectorSearchQuery;
import com.xgen.mongot.index.query.QueryExecutionContext;
import com.xgen.mongot.index.query.QueryOptimizationFlags;
import com.xgen.mongot.index.status.IndexStatus;
import com.xgen.mongot.index.version.MaterializedViewGenerationId;
import io.micrometer.core.instrument.Tags;
import java.io.IOException;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.bson.BsonArray;
import org.bson.BsonTimestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InitializedMaterializedViewIndex implements InitializedAutoEmbedIndex {
  private static final Logger LOG = LoggerFactory.getLogger(InitializedMaterializedViewIndex.class);
  private static final NoOpIndexReader NO_OP_INDEX_READER = new NoOpIndexReader();
  private final IndexDefinition indexDefinition;
  private final MaterializedViewGenerationId generationId;
  private final IndexMetricsUpdater indexMetricsUpdater;
  private final MaterializedViewWriter materializedViewWriter;
  private final AtomicReference<IndexStatus> statusRef;
  private final AtomicBoolean wasQueryable;
  private final LeaseManager leaseManager;
  private final MaterializedViewSchemaMetadata schemaMetadata;
  private final String matViewDatabaseName;
  private final AtomicLong leaderStatusGauge;

  public InitializedMaterializedViewIndex(
      MaterializedViewIndexDefinitionGeneration matViewDefinitionGeneration,
      MaterializedViewWriter materializedViewWriter,
      IndexMetricsUpdater indexMetricsUpdater,
      AtomicReference<IndexStatus> statusRef,
      LeaseManager leaseManager,
      MaterializedViewSchemaMetadata schemaMetadata,
      String matViewDatabaseName) {
    this.indexDefinition = matViewDefinitionGeneration.getIndexDefinition();
    this.generationId = matViewDefinitionGeneration.getGenerationId();
    this.indexMetricsUpdater = indexMetricsUpdater;
    this.materializedViewWriter = materializedViewWriter;
    this.statusRef = statusRef;
    this.leaseManager = leaseManager;
    this.wasQueryable =
        new AtomicBoolean(
            statusRef.get().canServiceQueries() || isCurrentVersionQueryablePerLease());
    this.schemaMetadata = schemaMetadata;
    this.matViewDatabaseName = matViewDatabaseName;
    this.leaderStatusGauge =
        indexMetricsUpdater.getMetricsFactory().perIndexNumGauge("leaderStatus");
  }

  /**
   * Registers one-hot status gauges for on-call RCA.
   *
   * <p>These gauges are not a canonical source of truth. Multiple index instances can share one
   * materialized-view generation and gauge identity, so the latest registration wins.
   */
  void registerStatusGauges() {
    Tags indexTypeTags =
        Tags.of(
            IndexTypeData.INDEX_TYPE_TAG_NAME,
            IndexTypeData.getIndexTypeTag(this.indexDefinition).tagValue);
    for (IndexStatus.StatusCode statusCode : IndexStatus.StatusCode.values()) {
      this.indexMetricsUpdater
          .getMetricsFactory()
          .perIndexObjectValueGauge(
              IndexMetricValuesSupplier.MetricNames.INDEX_STATUS_CODE,
              this,
              index -> index.getStatus().getStatusCode() == statusCode ? 1 : 0,
              Tags.of("status", statusCode.name()).and(indexTypeTags));
    }
  }

  @Override
  public VectorIndexReader getReader() {
    return NO_OP_INDEX_READER;
  }

  @Override
  public MaterializedViewWriter getWriter() {
    return this.materializedViewWriter;
  }

  @Override
  public IndexMetricsUpdater getMetricsUpdater() {
    return this.indexMetricsUpdater;
  }

  /**
   * Updates the leader-status gauge for this index.
   *
   * @param isLeader true if this index is currently being replicated by a leader generator, false
   *     otherwise
   */
  public void setLeaderMode(boolean isLeader) {
    this.leaderStatusGauge.set(isLeader ? 1 : 0);
    LOG.atInfo()
        .addKeyValue("generationId", this.generationId)
        .addKeyValue("isLeader", isLeader)
        .addKeyValue("stateObjectIdentity", System.identityHashCode(this.leaderStatusGauge))
        .log("Set leaderStatus gauge");
  }

  /**
   * Re-registers the per-index {@code leaderStatus} gauge if it has been evicted from the registry
   * while this generation is still live. Invoked from the leader heartbeat so a healthy leader's
   * gauge self-heals within one beat instead of staying absent until process restart.
   */
  public void ensureLeaderStatusGaugeRegistered() {
    boolean reRegistered =
        this.indexMetricsUpdater
            .getMetricsFactory()
            .ensurePerIndexObjectValueGaugeRegistered(
                "leaderStatus", this.leaderStatusGauge, Number::doubleValue);
    if (reRegistered) {
      LOG.atWarn()
          .addKeyValue("generationId", this.generationId)
          .addKeyValue("stateObjectIdentity", System.identityHashCode(this.leaderStatusGauge))
          .log("Re-registered missing leaderStatus gauge");
    }
  }

  @Override
  public IndexMetrics getMetrics() {
    return this.indexMetricsUpdater.getMetrics();
  }

  @Override
  public long getIndexSize() {
    return this.indexMetricsUpdater.getIndexSize();
  }

  @Override
  public void clear(EncodedUserData dropUserData) {}

  @Override
  public MaterializedViewGenerationId getGenerationId() {
    return this.generationId;
  }

  @Override
  public IndexDefinitionGeneration.Type getType() {
    return AUTO_EMBEDDING;
  }

  @Override
  public IndexDefinition getDefinition() {
    return this.indexDefinition;
  }

  @Override
  public void setStatus(IndexStatus status) {
    setStatus(status, Runnable::run);
  }

  /**
   * {@code publishGate} runs the write to the status reference, which generations sharing a
   * materialized view collection share. Callers that can race a generator replacement pass a gate
   * that runs the write only while their generator is still the installed one, so a displaced
   * generator cannot overwrite its successor's status. The lease update stays outside the gate: it
   * is a remote call, and the gate serializes against generator installation.
   */
  public void setStatus(IndexStatus status, Consumer<Runnable> publishGate) {
    try {
      this.leaseManager.updateReplicationStatus(
          this.generationId,
          this.indexDefinition.getDefinitionVersion().orElse(DEFAULT_INDEX_DEFINITION_VERSION),
          status);
    } catch (MaterializedViewNonTransientException e) {
      // Raw message; AutoEmbeddingCompositeIndex adds the failure prefix and stage
      // label when it surfaces this component's status.
      publishGate.accept(
          () ->
              this.statusRef.set(IndexStatus.failed("Corrupted Lease documents for this index.")));
      return;
    } catch (MaterializedViewTransientException ignored) {
      // TODO(CLOUDP-371153): Revisit this to decide how to handle committing error.
    }
    if (status.canServiceQueries()) {
      this.wasQueryable.set(true);
    }
    publishGate.accept(() -> this.statusRef.set(status));
  }

  @Override
  public IndexStatus getStatus() {
    IndexStatus status = this.statusRef.get();
    // If the index was previously queryable but is now rebuilding (e.g. fell off the oplog),
    // report RECOVERING_TRANSIENT (reported externally as STALE) so Atlas reflects "queryable but
    // potentially out of date" while the resync is in progress. Include NOT_STARTED: replication
    // clears the index to NOT_STARTED before the async initial-sync callback sets INITIAL_SYNC.
    if (this.wasQueryable.get()
        && (status.getStatusCode() == IndexStatus.StatusCode.INITIAL_SYNC
            || status.getStatusCode() == IndexStatus.StatusCode.NOT_STARTED)) {
      return IndexStatus.recoveringTransient("Recovering via resync");
    }
    return status;
  }

  @Override
  public boolean isCompatibleWith(IndexDefinition indexDefinition) {
    // TODO(CLOUDP-360523): Implement compatibility check for redefined index.
    return indexDefinition.isAutoEmbeddingIndex()
        && indexDefinition.getParsedAutoEmbeddingFeatureVersion()
            >= MIN_VERSION_FOR_MATERIALIZED_VIEW_EMBEDDING;
  }

  @Override
  public void drop() throws IOException {}

  @Override
  public boolean isClosed() {
    return false;
  }

  @Override
  public void throwIfUnavailableForQuerying() throws IndexUnavailableException {}

  @Override
  public void close() throws IOException {
    this.indexMetricsUpdater.close();
  }

  /** Returns the oplog position at which the MV first became STEADY. */
  public Optional<BsonTimestamp> getSteadyAsOfOplogPosition() {
    return this.leaseManager.getSteadyAsOfOplogPosition(this.generationId);
  }

  /**
   * Returns true if the current definition version has ever been queryable, as persisted in the
   * lease. Used to seed both this index's in-memory {@code wasQueryable} ratchet and the {@code
   * AutoEmbeddingCompositeIndex} ratchet across restarts so the leader's effective status matches
   * followers (which read the lease directly via {@code StatusResolutionUtils}).
   */
  public boolean isCurrentVersionQueryablePerLease() {
    return this.leaseManager.isCurrentVersionQueryable(
        this.generationId,
        this.indexDefinition.getDefinitionVersion().orElse(DEFAULT_INDEX_DEFINITION_VERSION));
  }

  public UUID getMaterializedViewCollectionUuid() {
    return this.materializedViewWriter.getCollectionUuid();
  }

  public MaterializedViewSchemaMetadata getSchemaMetadata() {
    return this.schemaMetadata;
  }

  /**
   * The materialized view's immutable collection metadata (UUID, collection name, schema).
   * Persisted into the derived Lucene commit so the derived index definition can be rebuilt when
   * mongod is unreachable; the mongod-assigned collection UUID and MV-sourced schema in particular
   * cannot be recovered offline any other way.
   */
  public MaterializedViewCollectionMetadata getMaterializedViewCollectionMetadata() {
    return new MaterializedViewCollectionMetadata(
        this.schemaMetadata,
        this.materializedViewWriter.getCollectionUuid(),
        this.materializedViewWriter.getNamespace().getCollectionName());
  }

  public String getMaterializedViewDatabaseName() {
    return this.matViewDatabaseName;
  }

  static class NoOpIndexReader implements VectorIndexReader {
    @Override
    public BsonArray query(
        MaterializedVectorSearchQuery materializedQuery, QueryExecutionContext context)
        throws ReaderClosedException, IOException, InvalidQueryException {
      return new BsonArray();
    }

    @Override
    public VectorProducerAndMetaResults query(
        MaterializedVectorSearchQuery materializedVectorSearchQuery,
        QueryExecutionContext context,
        QueryCursorOptions queryCursorOptions,
        BatchSizeStrategy batchSizeStrategy,
        QueryOptimizationFlags queryOptimizationFlags)
        throws IOException, InvalidQueryException, InterruptedException, ReaderClosedException {
      return new VectorProducerAndMetaResults(new EmptySearchBatchProducer(), MetaResults.EMPTY);
    }

    @Override
    public void refresh() throws IOException, ReaderClosedException {}

    @Override
    public void open() {}

    @Override
    public void close() {}
  }
}
