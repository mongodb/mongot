package com.xgen.mongot.index.autoembedding;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
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
import com.xgen.mongot.index.IndexTypeData;
import com.xgen.mongot.index.ReplicationOpTimeInfo;
import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.definition.MaterializedViewIndexDefinitionGeneration;
import com.xgen.mongot.index.definition.SearchIndexDefinition;
import com.xgen.mongot.index.definition.VectorIndexDefinition;
import com.xgen.mongot.index.mongodb.MaterializedViewWriter;
import com.xgen.mongot.index.status.IndexStatus;
import com.xgen.mongot.index.version.Generation;
import com.xgen.mongot.index.version.GenerationId;
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
 * Tests for {@link InitializedMaterializedViewIndex}, including metric lifecycle behavior.
 *
 * <p>HELP-97792 exposed a host-local materialized-view status divergence that existing Lucene
 * status metrics could not show.
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

  @Test
  public void statusGauges_followUnknownToSteadyTransition() throws Exception {
    IndexFixture fixture =
        createStatusMetricIndex(
            MeterAndFtdcRegistry.createWithSimpleRegistries(), IndexStatus.unknown());

    assertStatusGaugeValue(
        fixture.meterRegistry(), IndexStatus.StatusCode.UNKNOWN, 1, fixture.indexType());
    assertStatusGaugeValue(
        fixture.meterRegistry(), IndexStatus.StatusCode.STEADY, 0, fixture.indexType());

    fixture.index().setStatus(IndexStatus.steady());

    assertStatusGaugeValue(
        fixture.meterRegistry(), IndexStatus.StatusCode.UNKNOWN, 0, fixture.indexType());
    assertStatusGaugeValue(
        fixture.meterRegistry(), IndexStatus.StatusCode.STEADY, 1, fixture.indexType());
  }

  @Test
  public void statusGauges_reportEffectiveRecoveringStatus_notRawInitialSync() throws Exception {
    IndexFixture fixture =
        createStatusMetricIndex(
            MeterAndFtdcRegistry.createWithSimpleRegistries(), IndexStatus.unknown());
    fixture.index().setStatus(IndexStatus.steady());

    fixture.index().setStatus(IndexStatus.initialSync());

    assertStatusGaugeValue(
        fixture.meterRegistry(),
        IndexStatus.StatusCode.RECOVERING_TRANSIENT,
        1,
        fixture.indexType());
    assertStatusGaugeValue(
        fixture.meterRegistry(), IndexStatus.StatusCode.INITIAL_SYNC, 0, fixture.indexType());
  }

  @Test
  public void statusGauges_attemptReplacementOverwritesPredecessorAndSurvivesItsClose()
      throws Exception {
    MeterAndFtdcRegistry meterAndFtdcRegistry = MeterAndFtdcRegistry.createWithSimpleRegistries();
    GenerationId a0 = Generation.CURRENT.generationId(INDEX_ID);
    GenerationId a1 = Generation.CURRENT.nextAttempt().generationId(INDEX_ID);
    MaterializedViewGenerationId matViewA0 = MaterializedViewGenerationId.from(a0);
    MaterializedViewGenerationId matViewA1 = MaterializedViewGenerationId.from(a1);
    assertNotEquals(a0.uniqueString(), a1.uniqueString());
    assertEquals(matViewA0, matViewA1);

    IndexFixture predecessor =
        createStatusMetricIndex(
            meterAndFtdcRegistry,
            MaterializedViewIndex.mockMatViewDefinitionGeneration(matViewA0),
            IndexStatus.steady());
    String vectorAutoEmbeddingIndexType =
        IndexTypeData.IndexTypeTag.TAG_VECTOR_SEARCH_AUTO_EMBEDDING.tagValue;
    assertStatusGaugeValue(
        meterAndFtdcRegistry.meterRegistry(),
        IndexStatus.StatusCode.STEADY,
        1,
        vectorAutoEmbeddingIndexType);

    IndexFixture successor =
        createStatusMetricIndex(
            meterAndFtdcRegistry,
            MaterializedViewIndex.mockMatViewDefinitionGeneration(matViewA1),
            IndexStatus.unknown());
    assertStatusGaugeValue(
        meterAndFtdcRegistry.meterRegistry(),
        IndexStatus.StatusCode.UNKNOWN,
        1,
        vectorAutoEmbeddingIndexType);
    assertStatusGaugeValue(
        meterAndFtdcRegistry.meterRegistry(),
        IndexStatus.StatusCode.STEADY,
        0,
        vectorAutoEmbeddingIndexType);
    successor.index().setStatus(IndexStatus.steady());

    predecessor.index().close();

    assertEquals(
        IndexStatus.StatusCode.values().length,
        countStatusGauges(meterAndFtdcRegistry.meterRegistry()));
    assertStatusGaugeValue(
        meterAndFtdcRegistry.meterRegistry(),
        IndexStatus.StatusCode.STEADY,
        1,
        vectorAutoEmbeddingIndexType);
    assertStatusGaugeValue(
        meterAndFtdcRegistry.meterRegistry(),
        IndexStatus.StatusCode.UNKNOWN,
        0,
        vectorAutoEmbeddingIndexType);
  }

  @Test
  public void close_finalIndexUnregistersStatusGauges() throws Exception {
    IndexFixture fixture =
        createStatusMetricIndex(
            MeterAndFtdcRegistry.createWithSimpleRegistries(), IndexStatus.unknown());

    assertEquals(
        IndexStatus.StatusCode.values().length, countStatusGauges(fixture.meterRegistry()));

    fixture.index().close();

    assertEquals(0, countStatusGauges(fixture.meterRegistry()));
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

  private IndexFixture createStatusMetricIndex(
      MeterAndFtdcRegistry meterAndFtdcRegistry, IndexStatus initialStatus) throws Exception {
    MaterializedViewIndexDefinitionGeneration defGen =
        MaterializedViewIndex.mockMatViewDefinitionGeneration(INDEX_ID);
    return createStatusMetricIndex(meterAndFtdcRegistry, defGen, initialStatus);
  }

  private IndexFixture createStatusMetricIndex(
      MeterAndFtdcRegistry meterAndFtdcRegistry,
      MaterializedViewIndexDefinitionGeneration defGen,
      IndexStatus initialStatus)
      throws Exception {
    MaterializedViewGenerationId generationId = defGen.getGenerationId();
    String uniqueString = generationId.uniqueString();
    String collectionName = "matview-" + INDEX_ID.toHexString();
    PerIndexMetricsFactory metricsFactory =
        new PerIndexMetricsFactory(
            NAMESPACE,
            meterAndFtdcRegistry,
            uniqueString,
            collectionName,
            /* identitySafeClose= */ true);
    IndexMetricValuesSupplier metricValuesSupplier =
        IndexMetricsSupplier.mockEmptyIndexMetricsSupplier();
    IndexMetricsUpdater indexMetricsUpdater =
        new IndexMetricsUpdater(defGen.getIndexDefinition(), metricValuesSupplier, metricsFactory);
    MaterializedViewWriter writer = mock(MaterializedViewWriter.class);
    LeaseManager leaseManager = mock(LeaseManager.class);
    doNothing().when(leaseManager).updateReplicationStatus(any(), anyLong(), any());
    AtomicReference<IndexStatus> statusRef = new AtomicReference<>(initialStatus);
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
    index.registerStatusGauges();
    String indexType = IndexTypeData.getIndexTypeTag(defGen.getIndexDefinition()).tagValue;
    return new IndexFixture(index, meterAndFtdcRegistry.meterRegistry(), indexType);
  }

  @Test
  public void statusGauges_includeSearchAutoEmbeddingIndexType() throws Exception {
    MaterializedViewIndexDefinitionGeneration defGen =
        MaterializedViewIndex.mockMatViewSearchDefinitionGeneration(INDEX_ID);
    IndexFixture fixture =
        createStatusMetricIndex(
            MeterAndFtdcRegistry.createWithSimpleRegistries(), defGen, IndexStatus.steady());

    assertEquals(
        IndexTypeData.IndexTypeTag.TAG_SEARCH_AUTO_EMBEDDING.tagValue, fixture.indexType());
    assertStatusGaugeValue(
        fixture.meterRegistry(), IndexStatus.StatusCode.STEADY, 1, fixture.indexType());
  }

  private static void assertStatusGaugeValue(
      MeterRegistry meterRegistry,
      IndexStatus.StatusCode statusCode,
      long expectedValue,
      String indexType) {
    double value =
        meterRegistry
            .get(NAMESPACE + "." + IndexMetricValuesSupplier.MetricNames.INDEX_STATUS_CODE)
            .tag("status", statusCode.name())
            .tag(IndexTypeData.INDEX_TYPE_TAG_NAME, indexType)
            .gauge()
            .value();
    assertEquals(expectedValue, value, 0);
  }

  private static long countStatusGauges(MeterRegistry meterRegistry) {
    String metricName = NAMESPACE + "." + IndexMetricValuesSupplier.MetricNames.INDEX_STATUS_CODE;
    return meterRegistry.getMeters().stream()
        .filter(meter -> meter.getId().getName().equals(metricName))
        .count();
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

  private record IndexFixture(
      InitializedMaterializedViewIndex index, MeterRegistry meterRegistry, String indexType) {}
}
