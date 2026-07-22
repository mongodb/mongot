package com.xgen.mongot.index.autoembedding;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.xgen.mongot.embedding.config.MaterializedViewCollectionMetadata.MaterializedViewSchemaMetadata;
import com.xgen.mongot.embedding.exceptions.MaterializedViewNonTransientException;
import com.xgen.mongot.embedding.mongodb.leasing.LeaseManager;
import com.xgen.mongot.index.IndexMetricValuesSupplier;
import com.xgen.mongot.index.IndexMetricsUpdater;
import com.xgen.mongot.index.ReplicationOpTimeInfo;
import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.definition.MaterializedViewIndexDefinitionGeneration;
import com.xgen.mongot.index.definition.SearchIndexDefinition;
import com.xgen.mongot.index.definition.VectorIndexDefinition;
import com.xgen.mongot.index.mongodb.MaterializedViewWriter;
import com.xgen.mongot.index.status.IndexStatus;
import com.xgen.mongot.index.version.MaterializedViewGenerationId;
import com.xgen.mongot.metrics.MeterAndFtdcRegistry;
import com.xgen.mongot.metrics.PerIndexMetricsFactory;
import com.xgen.testing.mongot.mock.index.IndexMetricsSupplier;
import com.xgen.testing.mongot.mock.index.MaterializedViewIndex;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import org.bson.BsonTimestamp;
import org.bson.types.ObjectId;
import org.junit.Test;

/**
 * Tests for {@link InitializedMaterializedViewIndex}, including that closing the index unregisters
 * the leader-status gauge (Bug: gauge remained after auto-embedding index drop).
 */
public class InitializedMaterializedViewIndexTest {

  private static final ObjectId INDEX_ID = new ObjectId();
  private static final String NAMESPACE = "embedding.materializedView.stats";

  @Test
  public void getStatus_returnsRecoveringTransient_whenPreviouslyQueryableAndNowInitialSync()
      throws Exception {
    InitializedMaterializedViewIndex index = createIndex(IndexStatus.unknown());

    // Transition to STEADY (queryable)
    index.setStatus(IndexStatus.steady());
    assertEquals(IndexStatus.StatusCode.STEADY, index.getStatus().getStatusCode());

    // Fall off the oplog -> INITIAL_SYNC: should report RECOVERING_TRANSIENT (STALE), not BUILDING
    index.setStatus(IndexStatus.initialSync());
    assertEquals(IndexStatus.StatusCode.RECOVERING_TRANSIENT, index.getStatus().getStatusCode());
  }

  @Test
  public void getStatus_returnsRecoveringTransient_whenPreviouslyQueryableAndNowNotStarted()
      throws Exception {
    InitializedMaterializedViewIndex index = createIndex(IndexStatus.unknown());

    index.setStatus(IndexStatus.steady());
    assertEquals(IndexStatus.StatusCode.STEADY, index.getStatus().getStatusCode());

    // ReplicationManager clears to NOT_STARTED before enqueueing initial sync
    index.setStatus(IndexStatus.notStarted());
    assertEquals(IndexStatus.StatusCode.RECOVERING_TRANSIENT, index.getStatus().getStatusCode());
  }

  @Test
  public void getStatus_returnsInitialSync_whenNeverQueryableAndNowInitialSync() throws Exception {
    InitializedMaterializedViewIndex index = createIndex(IndexStatus.unknown());

    index.setStatus(IndexStatus.initialSync());
    assertEquals(IndexStatus.StatusCode.INITIAL_SYNC, index.getStatus().getStatusCode());
  }

  @Test
  public void getStatus_returnsNotStarted_whenNeverQueryableAndNowNotStarted() throws Exception {
    InitializedMaterializedViewIndex index = createIndex(IndexStatus.unknown());

    index.setStatus(IndexStatus.notStarted());
    assertEquals(IndexStatus.StatusCode.NOT_STARTED, index.getStatus().getStatusCode());
  }

  @Test
  public void getStatus_returnsRecoveringTransient_whenInitializedAsQueryable() throws Exception {
    // Index rehydrated from a previous STEADY state
    InitializedMaterializedViewIndex index = createIndex(IndexStatus.steady());

    index.setStatus(IndexStatus.initialSync());
    assertEquals(IndexStatus.StatusCode.RECOVERING_TRANSIENT, index.getStatus().getStatusCode());
  }

  @Test
  public void getStatus_returnsRecoveringTransient_whenLeaseQueryableAndNowInitialSync()
      throws Exception {
    // Process restart while MV is mid-resync: statusRef is fresh IndexStatus.unknown(), but
    // the persisted lease records isQueryable=true. The constructor must seed wasQueryable
    // from the lease so the leader reports RECOVERING_TRANSIENT (STALE), matching followers,
    // not raw INITIAL_SYNC (BUILDING).
    InitializedMaterializedViewIndex index =
        createIndex(IndexStatus.unknown(), /* leaseQueryable= */ true);

    index.setStatus(IndexStatus.initialSync());
    assertEquals(IndexStatus.StatusCode.RECOVERING_TRANSIENT, index.getStatus().getStatusCode());
  }

  @Test
  public void setStatus_corruptedLease_setsRawFailedMessage() throws Exception {
    // The MV component carries a raw message; AutoEmbeddingCompositeIndex adds the failure prefix
    // and stage label when it surfaces this status.
    LeaseManager leaseManager = mock(LeaseManager.class);
    doThrow(new MaterializedViewNonTransientException("simulated corrupted lease"))
        .when(leaseManager)
        .updateReplicationStatus(any(), anyLong(), any());
    InitializedMaterializedViewIndex index = createIndexWithLeaseManager(leaseManager);

    index.setStatus(IndexStatus.steady());

    IndexStatus status = index.getStatus();
    assertEquals(IndexStatus.StatusCode.FAILED, status.getStatusCode());
    assertEquals(Optional.of("Corrupted Lease documents for this index."), status.getMessage());
  }

  @Test
  public void getDefinition_vectorIndex_returnsVectorIndexDefinition() throws Exception {
    MaterializedViewIndexDefinitionGeneration defGen =
        MaterializedViewIndex.mockMatViewDefinitionGeneration(INDEX_ID);
    InitializedMaterializedViewIndex index = createIndexFor(defGen, IndexStatus.unknown());

    IndexDefinition definition = index.getDefinition();

    assertTrue(
        "Vector input must round-trip as VectorIndexDefinition",
        definition instanceof VectorIndexDefinition);
    assertEquals(defGen.getIndexDefinition(), definition);
  }

  @Test
  public void getDefinition_searchIndex_returnsSearchIndexDefinition() throws Exception {
    MaterializedViewIndexDefinitionGeneration defGen =
        MaterializedViewIndex.mockMatViewSearchDefinitionGeneration(INDEX_ID);
    InitializedMaterializedViewIndex index = createIndexFor(defGen, IndexStatus.unknown());

    IndexDefinition definition = index.getDefinition();

    assertTrue(
        "Search input must round-trip as SearchIndexDefinition",
        definition instanceof SearchIndexDefinition);
    assertEquals(defGen.getIndexDefinition(), definition);
  }

  private InitializedMaterializedViewIndex createIndex(IndexStatus initialStatus) throws Exception {
    return createIndex(initialStatus, /* leaseQueryable= */ false);
  }

  private InitializedMaterializedViewIndex createIndex(
      IndexStatus initialStatus, boolean leaseQueryable) throws Exception {
    return createIndexFor(
        MaterializedViewIndex.mockMatViewDefinitionGeneration(INDEX_ID),
        initialStatus,
        leaseQueryable);
  }

  private InitializedMaterializedViewIndex createIndexFor(
      MaterializedViewIndexDefinitionGeneration defGen, IndexStatus initialStatus)
      throws Exception {
    return createIndexFor(defGen, initialStatus, /* leaseQueryable= */ false);
  }

  private InitializedMaterializedViewIndex createIndexFor(
      MaterializedViewIndexDefinitionGeneration defGen,
      IndexStatus initialStatus,
      boolean leaseQueryable)
      throws Exception {
    MeterAndFtdcRegistry meterAndFtdcRegistry = MeterAndFtdcRegistry.createWithSimpleRegistries();
    MaterializedViewGenerationId generationId = defGen.getGenerationId();
    String uniqueString = generationId.uniqueString();
    String collectionName = "matview-" + INDEX_ID.toHexString();
    PerIndexMetricsFactory metricsFactory =
        new PerIndexMetricsFactory(NAMESPACE, meterAndFtdcRegistry, uniqueString, collectionName);
    IndexMetricValuesSupplier metricValuesSupplier =
        IndexMetricsSupplier.mockEmptyIndexMetricsSupplier();
    IndexMetricsUpdater indexMetricsUpdater =
        new IndexMetricsUpdater(defGen.getIndexDefinition(), metricValuesSupplier, metricsFactory);
    MaterializedViewWriter writer = mock(MaterializedViewWriter.class);
    LeaseManager leaseManager = mock(LeaseManager.class);
    doNothing().when(leaseManager).updateReplicationStatus(any(), anyLong(), any());
    when(leaseManager.isCurrentVersionQueryable(any(), anyLong())).thenReturn(leaseQueryable);
    AtomicReference<IndexStatus> statusRef = new AtomicReference<>(initialStatus);
    MaterializedViewSchemaMetadata schemaMetadata =
        new MaterializedViewSchemaMetadata(0, java.util.Map.of());
    return new InitializedMaterializedViewIndex(
        defGen,
        writer,
        indexMetricsUpdater,
        statusRef,
        leaseManager,
        schemaMetadata,
        "__mdb_internal_search");
  }

  private InitializedMaterializedViewIndex createIndexWithLeaseManager(LeaseManager leaseManager)
      throws Exception {
    MaterializedViewIndexDefinitionGeneration defGen =
        MaterializedViewIndex.mockMatViewDefinitionGeneration(INDEX_ID);
    MeterAndFtdcRegistry meterAndFtdcRegistry = MeterAndFtdcRegistry.createWithSimpleRegistries();
    MaterializedViewGenerationId generationId = defGen.getGenerationId();
    String uniqueString = generationId.uniqueString();
    String collectionName = "matview-" + INDEX_ID.toHexString();
    PerIndexMetricsFactory metricsFactory =
        new PerIndexMetricsFactory(NAMESPACE, meterAndFtdcRegistry, uniqueString, collectionName);
    IndexMetricValuesSupplier metricValuesSupplier =
        IndexMetricsSupplier.mockEmptyIndexMetricsSupplier();
    IndexMetricsUpdater indexMetricsUpdater =
        new IndexMetricsUpdater(defGen.getIndexDefinition(), metricValuesSupplier, metricsFactory);
    MaterializedViewWriter writer = mock(MaterializedViewWriter.class);
    AtomicReference<IndexStatus> statusRef = new AtomicReference<>(IndexStatus.unknown());
    MaterializedViewSchemaMetadata schemaMetadata =
        new MaterializedViewSchemaMetadata(0, java.util.Map.of());
    return new InitializedMaterializedViewIndex(
        defGen,
        writer,
        indexMetricsUpdater,
        statusRef,
        leaseManager,
        schemaMetadata,
        "__mdb_internal_search");
  }

  @Test
  public void close_whenCalled_unregistersLeaderStatusGauge() throws Exception {
    MeterAndFtdcRegistry meterAndFtdcRegistry = MeterAndFtdcRegistry.createWithSimpleRegistries();
    MeterRegistry meterRegistry = meterAndFtdcRegistry.meterRegistry();

    MaterializedViewIndexDefinitionGeneration defGen =
        MaterializedViewIndex.mockMatViewDefinitionGeneration(INDEX_ID);
    MaterializedViewGenerationId generationId = defGen.getGenerationId();
    String uniqueString = generationId.uniqueString();
    String collectionName = "matview-" + INDEX_ID.toHexString();

    PerIndexMetricsFactory metricsFactory =
        new PerIndexMetricsFactory(NAMESPACE, meterAndFtdcRegistry, uniqueString, collectionName);
    IndexMetricValuesSupplier metricValuesSupplier =
        IndexMetricsSupplier.mockEmptyIndexMetricsSupplier();
    IndexMetricsUpdater indexMetricsUpdater =
        new IndexMetricsUpdater(defGen.getIndexDefinition(), metricValuesSupplier, metricsFactory);

    MaterializedViewWriter writer = mock(MaterializedViewWriter.class);
    LeaseManager leaseManager = mock(LeaseManager.class);
    AtomicReference<IndexStatus> statusRef = new AtomicReference<>(IndexStatus.unknown());
    MaterializedViewSchemaMetadata schemaMetadata =
        new MaterializedViewSchemaMetadata(0, java.util.Map.of());

    InitializedMaterializedViewIndex index =
        new InitializedMaterializedViewIndex(
            defGen,
            writer,
            indexMetricsUpdater,
            statusRef,
            leaseManager,
            schemaMetadata,
            "__mdb_internal_search");

    long leaderStatusGaugesBefore =
        meterRegistry.getMeters().stream()
            .filter(m -> m.getId().getName().contains("leaderStatus"))
            .count();
    assertTrue(
        "leaderStatus gauge should be registered after construction",
        leaderStatusGaugesBefore >= 1);

    index.close();

    long leaderStatusGaugesAfter =
        meterRegistry.getMeters().stream()
            .filter(m -> m.getId().getName().contains("leaderStatus"))
            .count();
    assertEquals(
        "leaderStatus gauge should be unregistered after close()", 0, leaderStatusGaugesAfter);
  }

  @Test
  public void close_replicationOpTimeCanStillBeUpdated() throws Exception {
    InitializedMaterializedViewIndex index = createIndex(IndexStatus.steady());
    BsonTimestamp replicationOpTime = new BsonTimestamp(100, 1);
    BsonTimestamp maxPossibleReplicationOpTime = new BsonTimestamp(120, 1);

    index.close();
    ReplicationOpTimeInfo opTimeInfo =
        index.getMetricsUpdater().getIndexingMetricsUpdater().getReplicationOpTimeInfo();
    opTimeInfo.update(replicationOpTime.getValue(), maxPossibleReplicationOpTime.getValue());

    assertEquals(replicationOpTime.getValue(), opTimeInfo.snapshotOrThrow().replicationOpTime());
    assertEquals(
        maxPossibleReplicationOpTime.getValue(),
        opTimeInfo.snapshotOrThrow().maxPossibleReplicationOpTime());
  }

  @Test
  public void close_replicationOpTimeCanStillBeUnset() throws Exception {
    InitializedMaterializedViewIndex index = createIndex(IndexStatus.steady());
    ReplicationOpTimeInfo opTimeInfo =
        index.getMetricsUpdater().getIndexingMetricsUpdater().getReplicationOpTimeInfo();
    opTimeInfo.update(new BsonTimestamp(100, 1).getValue(), new BsonTimestamp(120, 1).getValue());

    index.close();
    ReplicationOpTimeInfo closedIndexOpTimeInfo =
        index.getMetricsUpdater().getIndexingMetricsUpdater().getReplicationOpTimeInfo();

    assertTrue(closedIndexOpTimeInfo.unset(() -> true));
    assertTrue(closedIndexOpTimeInfo.snapshot().isEmpty());
  }
}
