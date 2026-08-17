package com.xgen.mongot.cursor;

import static com.google.common.truth.Truth.assertThat;
import static com.xgen.testing.mongot.mock.index.IndexGeneration.mockIndexGeneration;
import static com.xgen.testing.mongot.mock.index.SearchIndex.MOCK_INDEX_COLLECTION_UUID;
import static com.xgen.testing.mongot.mock.index.SearchIndex.MOCK_INDEX_DATABASE_NAME;
import static com.xgen.testing.mongot.mock.index.SearchIndex.MOCK_INDEX_DEFINITION_GENERATION;
import static com.xgen.testing.mongot.mock.index.SearchIndex.MOCK_INDEX_GENERATION_ID;
import static com.xgen.testing.mongot.mock.index.SearchIndex.MOCK_INDEX_LAST_OBSERVED_COLLECTION_NAME;
import static com.xgen.testing.mongot.mock.index.SearchIndex.mockIndex;
import static com.xgen.testing.mongot.mock.index.SearchIndex.mockInitializedIndex;
import static com.xgen.testing.mongot.mock.index.SearchIndex.mockInitializedIndexWithMetaProducer;
import static com.xgen.testing.mongot.mock.index.query.Query.mockBadIndexNameQuery;
import static com.xgen.testing.mongot.mock.index.query.Query.mockQuery;
import static com.xgen.testing.mongot.mock.index.query.Query.simpleQueryBuilder;
import static org.mockito.Mockito.mock;

import com.google.errorprone.annotations.Var;
import com.xgen.mongot.catalog.DefaultIndexCatalog;
import com.xgen.mongot.catalog.InitializedIndexCatalog;
import com.xgen.mongot.cursor.batch.QueryCursorOptions;
import com.xgen.mongot.index.IndexGeneration;
import com.xgen.mongot.index.InitializedIndex;
import com.xgen.mongot.index.autoembedding.UnresolvedAutoEmbeddingIndexGeneration;
import com.xgen.mongot.index.definition.VectorIndexDefinition;
import com.xgen.mongot.index.definition.VectorIndexDefinitionGeneration;
import com.xgen.mongot.index.query.InvalidQueryException;
import com.xgen.mongot.index.query.Query;
import com.xgen.mongot.index.query.QueryOptimizationFlags;
import com.xgen.mongot.index.status.IndexStatus;
import com.xgen.mongot.index.status.StaleStatusReason;
import com.xgen.mongot.index.version.GenerationId;
import com.xgen.mongot.index.version.IndexFormatVersion;
import com.xgen.mongot.metrics.MetricsFactory;
import com.xgen.mongot.searchenvoy.grpc.SearchEnvoyMetadata;
import com.xgen.mongot.util.Bytes;
import com.xgen.mongot.util.concurrent.NamedScheduledExecutorService;
import com.xgen.mongot.util.functionalinterfaces.CheckedConsumer;
import com.xgen.testing.TestUtils;
import com.xgen.testing.mongot.index.definition.VectorIndexDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.VectorIndexDefinitionGenerationBuilder;
import com.xgen.testing.mongot.index.version.GenerationIdBuilder;
import com.xgen.testing.mongot.mock.index.BatchProducer;
import com.xgen.testing.mongot.mock.index.VectorIndex;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.bson.BsonTimestamp;
import org.bson.types.ObjectId;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class MongotCursorManagerImplTest {

  private static final int INTERMEDIATE_PROTOCOL_VERSION = 1;
  private static final String VECTOR_INDEX_NAME = "vector";

  private static final VectorIndexDefinitionGeneration VECTOR_INDEX =
      VectorIndexDefinitionGenerationBuilder.builder()
          .definition(
              VectorIndexDefinitionBuilder.builder()
                  .name(VECTOR_INDEX_NAME)
                  .withCosineVectorField("embedding", 128)
                  .build())
          .generation(1, IndexFormatVersion.CURRENT.versionNumber)
          .build();

  private static MongotCursorManagerImpl getCursorManager() {
    DefaultIndexCatalog indexCatalog = new DefaultIndexCatalog();
    IndexGeneration indexGeneration = mockIndexGeneration();
    indexCatalog.addIndex(indexGeneration);
    indexCatalog.addIndex(mockIndexGeneration(VECTOR_INDEX));
    InitializedIndexCatalog initializedIndexCatalog = new InitializedIndexCatalog();
    initializedIndexCatalog.addIndex(mockInitializedIndex(indexGeneration));
    initializedIndexCatalog.addIndex(VectorIndex.mockInitializedIndex(VECTOR_INDEX));
    var metrics = new MetricsFactory("factory", new SimpleMeterRegistry());
    return new MongotCursorManagerImpl(
        indexCatalog,
        initializedIndexCatalog,
        mock(NamedScheduledExecutorService.class),
        metrics,
        CursorIdSupplier.createDefault());
  }

  private static MongotCursorManagerImpl getCursorManager(int numFullBatches) {
    DefaultIndexCatalog indexCatalog = new DefaultIndexCatalog();
    IndexGeneration indexGeneration = mockIndexGeneration();
    indexCatalog.addIndex(indexGeneration);
    InitializedIndexCatalog initializedIndexCatalog = new InitializedIndexCatalog();
    initializedIndexCatalog.addIndex(mockInitializedIndex(indexGeneration, numFullBatches));

    var metrics = new MetricsFactory("factory", new SimpleMeterRegistry());
    return new MongotCursorManagerImpl(
        indexCatalog,
        initializedIndexCatalog,
        mock(NamedScheduledExecutorService.class),
        metrics,
        CursorIdSupplier.createDefault());
  }

  private MongotCursorManagerImpl getCursorManager(
      Duration idleCursorHandlingRate,
      Duration cursorIdleTime,
      Bytes messageSizeLimit,
      Bytes bsonSizeSoftLimit,
      Bytes bsonSizeHardLimit) {
    DefaultIndexCatalog indexCatalog = new DefaultIndexCatalog();
    IndexGeneration generation = mockIndexGeneration();
    indexCatalog.addIndex(generation);
    InitializedIndexCatalog initializedIndexCatalog = new InitializedIndexCatalog();
    initializedIndexCatalog.addIndex(mockInitializedIndex(generation, Integer.MAX_VALUE));
    CursorConfig config =
        CursorConfig.create(
            Optional.of(idleCursorHandlingRate),
            Optional.of(cursorIdleTime),
            Optional.of(messageSizeLimit),
            Optional.of(bsonSizeSoftLimit),
            Optional.of(bsonSizeHardLimit),
            Optional.empty());
    return MongotCursorManagerImpl.fromConfig(
        config, new SimpleMeterRegistry(), indexCatalog, initializedIndexCatalog);
  }

  private static MongotCursorManagerImpl getCursorManagerWithMetaProducer(
      int numFullSearchBatches, int numFullMetaBatches) {
    DefaultIndexCatalog indexCatalog = new DefaultIndexCatalog();
    IndexGeneration generation = mockIndexGeneration();
    indexCatalog.addIndex(generation);
    InitializedIndexCatalog initializedIndexCatalog = new InitializedIndexCatalog();
    initializedIndexCatalog.addIndex(
        mockInitializedIndexWithMetaProducer(generation, numFullSearchBatches, numFullMetaBatches));

    return new MongotCursorManagerImpl(
        indexCatalog,
        initializedIndexCatalog,
        mock(NamedScheduledExecutorService.class),
        new MetricsFactory("test", new SimpleMeterRegistry()),
        CursorIdSupplier.createDefault());
  }

  private static MongotCursorManagerImpl getCursorManagerWithIndexStatus(IndexStatus status) {
    DefaultIndexCatalog indexCatalog = new DefaultIndexCatalog();
    var index = mockIndex();
    index.setStatus(status);
    IndexGeneration generation = mockIndexGeneration(index);
    indexCatalog.addIndex(generation);
    InitializedIndexCatalog initializedIndexCatalog = new InitializedIndexCatalog();
    initializedIndexCatalog.addIndex(mockInitializedIndex(generation));

    var metrics = new MetricsFactory("factory", new SimpleMeterRegistry());
    return new MongotCursorManagerImpl(
        indexCatalog,
        initializedIndexCatalog,
        mock(NamedScheduledExecutorService.class),
        metrics,
        CursorIdSupplier.createDefault());
  }

  private static MongotCursorManagerImpl getCursorManagerWithUninitializedIndex(
      IndexStatus status, MetricsFactory metricsFactory) {
    DefaultIndexCatalog indexCatalog = new DefaultIndexCatalog();
    var index = mockIndex();
    index.setStatus(status);
    IndexGeneration generation = mockIndexGeneration(index);
    indexCatalog.addIndex(generation);
    InitializedIndexCatalog initializedIndexCatalog = new InitializedIndexCatalog();

    return new MongotCursorManagerImpl(
        indexCatalog,
        initializedIndexCatalog,
        mock(NamedScheduledExecutorService.class),
        metricsFactory,
        CursorIdSupplier.createDefault());
  }

  @Test
  public void testValidNewCursor() throws Exception {
    MongotCursorManagerImpl cursorManager = getCursorManager();
    long cursorId =
        cursorManager.newCursor(
                MOCK_INDEX_DATABASE_NAME,
                MOCK_INDEX_LAST_OBSERVED_COLLECTION_NAME,
                MOCK_INDEX_COLLECTION_UUID,
                Optional.empty(),
                mockQuery(),
                QueryCursorOptions.empty(),
                QueryOptimizationFlags.DEFAULT_OPTIONS,
                Optional.empty())
            .cursorId;

    // Shouldn't throw a MongotCursorNotFoundException
    cursorManager.getNextBatch(
        cursorId, CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT, QueryCursorOptions.empty());
  }

  @Test
  public void testValidNewIntermediateCursors() throws Exception {
    MongotCursorManagerImpl cursorManager = getCursorManager();
    var cursors =
        cursorManager.newIntermediateCursors(
            MOCK_INDEX_DATABASE_NAME,
            MOCK_INDEX_LAST_OBSERVED_COLLECTION_NAME,
            MOCK_INDEX_COLLECTION_UUID,
            Optional.empty(),
            mockQuery(),
            INTERMEDIATE_PROTOCOL_VERSION,
            QueryCursorOptions.empty(),
            QueryOptimizationFlags.DEFAULT_OPTIONS,
            Optional.empty());

    cursorManager.getNextBatch(
        cursors.searchCursorId,
        CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT,
        QueryCursorOptions.empty());
    cursorManager.getNextBatch(
        cursors.metaCursorId,
        CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT,
        QueryCursorOptions.empty());
  }

  @Test
  public void testInvalidDatabaseNewCursor() throws Exception {
    MongotCursorManagerImpl cursorManager = getCursorManager();

    long cursorId =
        cursorManager.newCursor(
                "invalid",
                MOCK_INDEX_LAST_OBSERVED_COLLECTION_NAME,
                MOCK_INDEX_COLLECTION_UUID,
                Optional.empty(),
                mockQuery(),
                QueryCursorOptions.empty(),
                QueryOptimizationFlags.DEFAULT_OPTIONS,
                Optional.empty())
            .cursorId;

    assertEmptyCursor(cursorManager, cursorId);
  }

  @Test
  public void testInvalidDatabaseNewIntermediateCursors() throws Exception {
    MongotCursorManagerImpl cursorManager = getCursorManager();

    var cursors =
        cursorManager.newIntermediateCursors(
            "invalid",
            MOCK_INDEX_LAST_OBSERVED_COLLECTION_NAME,
            MOCK_INDEX_COLLECTION_UUID,
            Optional.empty(),
            mockQuery(),
            INTERMEDIATE_PROTOCOL_VERSION,
            QueryCursorOptions.empty(),
            QueryOptimizationFlags.DEFAULT_OPTIONS,
            Optional.empty());

    assertEmptyCursor(cursorManager, cursors.searchCursorId);
    assertMetaCursorCount(cursorManager, cursors.metaCursorId, 0);
  }

  @Test
  public void testInvalidCollectionUuidNewCursor() throws Exception {
    MongotCursorManagerImpl cursorManager = getCursorManager();

    long cursorId =
        cursorManager.newCursor(
                MOCK_INDEX_DATABASE_NAME,
                MOCK_INDEX_LAST_OBSERVED_COLLECTION_NAME,
                UUID.randomUUID(),
                Optional.empty(),
                mockQuery(),
                QueryCursorOptions.empty(),
                QueryOptimizationFlags.DEFAULT_OPTIONS,
                Optional.empty())
            .cursorId;

    assertEmptyCursor(cursorManager, cursorId);
  }

  @Test
  public void testInvalidCollectionUuidNewIntermediateCursors() throws Exception {
    MongotCursorManagerImpl cursorManager = getCursorManager();

    var cursors =
        cursorManager.newIntermediateCursors(
            MOCK_INDEX_DATABASE_NAME,
            MOCK_INDEX_LAST_OBSERVED_COLLECTION_NAME,
            UUID.randomUUID(),
            Optional.empty(),
            mockQuery(),
            INTERMEDIATE_PROTOCOL_VERSION,
            QueryCursorOptions.empty(),
            QueryOptimizationFlags.DEFAULT_OPTIONS,
            Optional.empty());

    assertEmptyCursor(cursorManager, cursors.searchCursorId);
    assertMetaCursorCount(cursorManager, cursors.metaCursorId, 0);
  }

  @Test
  public void testInvalidIndexNameNewCursor() throws Exception {
    MongotCursorManagerImpl cursorManager = getCursorManager();

    long cursorId =
        cursorManager.newCursor(
                MOCK_INDEX_DATABASE_NAME,
                MOCK_INDEX_LAST_OBSERVED_COLLECTION_NAME,
                MOCK_INDEX_COLLECTION_UUID,
                Optional.empty(),
                mockBadIndexNameQuery(),
                QueryCursorOptions.empty(),
                QueryOptimizationFlags.DEFAULT_OPTIONS,
                Optional.empty())
            .cursorId;

    assertEmptyCursor(cursorManager, cursorId);
  }

  @Test
  public void newCursor_vectorIndexName_throwsUserError() {
    MongotCursorManagerImpl cursorManager = getCursorManager();
    VectorIndexDefinition definition = VECTOR_INDEX.definition();
    Query query = simpleQueryBuilder().index(VECTOR_INDEX_NAME).build();

    Exception ex =
        Assert.assertThrows(
            InvalidQueryException.class,
            () ->
                cursorManager.newCursor(
                    definition.getDatabase(),
                    definition.getLastObservedCollectionName(),
                    definition.getCollectionUuid(),
                    Optional.empty(),
                    query,
                    QueryCursorOptions.empty(),
                    QueryOptimizationFlags.DEFAULT_OPTIONS,
                    Optional.empty()));

    assertThat(ex)
        .hasMessageThat()
        .isEqualTo("Cannot execute $search over vectorSearch index 'vector'");
  }

  @Test
  public void testUninitializedIndexNewCursor() throws Exception {
    var metrics = new MetricsFactory("factory", new SimpleMeterRegistry());
    MongotCursorManagerImpl cursorManager =
        getCursorManagerWithUninitializedIndex(
            IndexStatus.stale(
                StaleStatusReason.UNEXPECTED_ERROR.formatMessage("replication error"),
                new BsonTimestamp()),
            metrics);
    long cursorId =
        cursorManager.newCursor(
                MOCK_INDEX_DATABASE_NAME,
                MOCK_INDEX_LAST_OBSERVED_COLLECTION_NAME,
                MOCK_INDEX_COLLECTION_UUID,
                Optional.empty(),
                mockQuery(),
                QueryCursorOptions.empty(),
                QueryOptimizationFlags.DEFAULT_OPTIONS,
                Optional.empty())
            .cursorId;
    assertEmptyCursor(cursorManager, cursorId);
    Assert.assertEquals(
        1.0,
        metrics
            .counter(
                "uninitializedIndexCursorRequests",
                Tags.of("indexStatus", IndexStatus.StatusCode.STALE.name()))
            .count(),
        0.01);
  }

  @Test
  public void
      newCursor_unresolvedAutoEmbeddingIndexServingOffline_returnsReconstructedIndexResults()
          throws Exception {
    // An unresolved auto-embedding generation serving offline registers the index it reconstructed
    // from its last commit under the raw generation id, so its cursors read that index rather than
    // the empty cursors used for indexes that do not exist.
    DefaultIndexCatalog indexCatalog = new DefaultIndexCatalog();
    InitializedIndex reconstructedIndex = mockInitializedIndex(mockIndexGeneration(), 1);
    indexCatalog.addIndex(
        UnresolvedAutoEmbeddingIndexGeneration.createStale(
            MOCK_INDEX_DEFINITION_GENERATION, reconstructedIndex, Map.of()));
    InitializedIndexCatalog initializedIndexCatalog = new InitializedIndexCatalog();
    initializedIndexCatalog.addIndex(reconstructedIndex);
    MongotCursorManagerImpl cursorManager =
        new MongotCursorManagerImpl(
            indexCatalog,
            initializedIndexCatalog,
            mock(NamedScheduledExecutorService.class),
            new MetricsFactory("factory", new SimpleMeterRegistry()),
            CursorIdSupplier.createDefault());

    long cursorId =
        cursorManager.newCursor(
                MOCK_INDEX_DATABASE_NAME,
                MOCK_INDEX_LAST_OBSERVED_COLLECTION_NAME,
                MOCK_INDEX_COLLECTION_UUID,
                Optional.empty(),
                mockQuery(),
                QueryCursorOptions.empty(),
                QueryOptimizationFlags.DEFAULT_OPTIONS,
                Optional.empty())
            .cursorId;

    MongotCursorResultInfo batch =
        cursorManager.getNextBatch(
            cursorId, CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT, QueryCursorOptions.empty());
    Assert.assertEquals(BatchProducer.MOCK_BATCH_SIZE, batch.batch.asArray().size());
  }

  @Test
  public void hasOpenCursors_unresolvedAutoEmbeddingIndexServingOffline_seesOfflineCursor()
      throws Exception {
    // Phasing out a generation is gated on hasOpenCursors, so an offline cursor has to be visible
    // there. Otherwise recovery closes and drops the reconstructed reader while the cursor is still
    // reading from it, and killIndexCursors cannot reach the cursor either.
    DefaultIndexCatalog indexCatalog = new DefaultIndexCatalog();
    InitializedIndex reconstructedIndex =
        mockInitializedIndex(mockIndexGeneration(), Integer.MAX_VALUE);
    indexCatalog.addIndex(
        UnresolvedAutoEmbeddingIndexGeneration.createStale(
            MOCK_INDEX_DEFINITION_GENERATION, reconstructedIndex, Map.of()));
    InitializedIndexCatalog initializedIndexCatalog = new InitializedIndexCatalog();
    initializedIndexCatalog.addIndex(reconstructedIndex);
    MongotCursorManagerImpl cursorManager =
        new MongotCursorManagerImpl(
            indexCatalog,
            initializedIndexCatalog,
            mock(NamedScheduledExecutorService.class),
            new MetricsFactory("factory", new SimpleMeterRegistry()),
            CursorIdSupplier.createDefault());

    cursorManager.newCursor(
        MOCK_INDEX_DATABASE_NAME,
        MOCK_INDEX_LAST_OBSERVED_COLLECTION_NAME,
        MOCK_INDEX_COLLECTION_UUID,
        Optional.empty(),
        mockQuery(),
        QueryCursorOptions.empty(),
        QueryOptimizationFlags.DEFAULT_OPTIONS,
        Optional.empty());

    Assert.assertTrue(cursorManager.hasOpenCursors(MOCK_INDEX_GENERATION_ID));

    cursorManager.killIndexCursors(MOCK_INDEX_GENERATION_ID);
    Assert.assertFalse(cursorManager.hasOpenCursors(MOCK_INDEX_GENERATION_ID));
  }

  @Test
  public void testInvalidIndexNameNewIntermediateCursors() throws Exception {
    MongotCursorManagerImpl cursorManager = getCursorManager();

    var cursors =
        cursorManager.newIntermediateCursors(
            MOCK_INDEX_DATABASE_NAME,
            MOCK_INDEX_LAST_OBSERVED_COLLECTION_NAME,
            MOCK_INDEX_COLLECTION_UUID,
            Optional.empty(),
            mockBadIndexNameQuery(),
            INTERMEDIATE_PROTOCOL_VERSION,
            QueryCursorOptions.empty(),
            QueryOptimizationFlags.DEFAULT_OPTIONS,
            Optional.empty());

    assertEmptyCursor(cursorManager, cursors.searchCursorId);
    assertMetaCursorCount(cursorManager, cursors.metaCursorId, 0);
  }

  @Test
  public void testIndexStatusDoesNotExistNewCursorIsEmpty() throws Exception {
    MongotCursorManagerImpl cursorManager =
        getCursorManagerWithIndexStatus(
            IndexStatus.doesNotExist(IndexStatus.Reason.COLLECTION_NOT_FOUND));

    long cursorId =
        cursorManager.newCursor(
                MOCK_INDEX_DATABASE_NAME,
                MOCK_INDEX_LAST_OBSERVED_COLLECTION_NAME,
                MOCK_INDEX_COLLECTION_UUID,
                Optional.empty(),
                mockQuery(),
                QueryCursorOptions.empty(),
                QueryOptimizationFlags.DEFAULT_OPTIONS,
                Optional.empty())
            .cursorId;

    assertEmptyCursor(cursorManager, cursorId);
  }

  @Test
  public void testRoutedFromAnotherShardNewCursorIsEmpty() throws Exception {
    MongotCursorManagerImpl cursorManager = getCursorManager();

    long cursorId =
        cursorManager.newCursor(
                MOCK_INDEX_DATABASE_NAME,
                MOCK_INDEX_LAST_OBSERVED_COLLECTION_NAME,
                MOCK_INDEX_COLLECTION_UUID,
                Optional.empty(),
                mockQuery(),
                QueryCursorOptions.empty(),
                QueryOptimizationFlags.DEFAULT_OPTIONS,
                Optional.of(
                    SearchEnvoyMetadata.newBuilder().setRoutedFromAnotherShard(true).build()))
            .cursorId;

    assertEmptyCursor(cursorManager, cursorId);
  }

  @Test
  public void testIndexStatusDoesNotExistNewIntermediateCursorsAreEmpty() throws Exception {
    MongotCursorManagerImpl cursorManager =
        getCursorManagerWithIndexStatus(
            IndexStatus.doesNotExist(IndexStatus.Reason.COLLECTION_NOT_FOUND));

    var cursors =
        cursorManager.newIntermediateCursors(
            MOCK_INDEX_DATABASE_NAME,
            MOCK_INDEX_LAST_OBSERVED_COLLECTION_NAME,
            MOCK_INDEX_COLLECTION_UUID,
            Optional.empty(),
            mockQuery(),
            INTERMEDIATE_PROTOCOL_VERSION,
            QueryCursorOptions.empty(),
            QueryOptimizationFlags.DEFAULT_OPTIONS,
            Optional.empty());

    assertEmptyCursor(cursorManager, cursors.searchCursorId);
    assertMetaCursorCount(cursorManager, cursors.metaCursorId, 0);
  }

  @Test
  public void testRoutedFromAnotherShardIntermediateCursorsAreEmpty() throws Exception {
    MongotCursorManagerImpl cursorManager = getCursorManager();

    var cursors =
        cursorManager.newIntermediateCursors(
            MOCK_INDEX_DATABASE_NAME,
            MOCK_INDEX_LAST_OBSERVED_COLLECTION_NAME,
            MOCK_INDEX_COLLECTION_UUID,
            Optional.empty(),
            mockQuery(),
            INTERMEDIATE_PROTOCOL_VERSION,
            QueryCursorOptions.empty(),
            QueryOptimizationFlags.DEFAULT_OPTIONS,
            Optional.of(SearchEnvoyMetadata.newBuilder().setRoutedFromAnotherShard(true).build()));

    assertEmptyCursor(cursorManager, cursors.searchCursorId);
    assertMetaCursorCount(cursorManager, cursors.metaCursorId, 0);
  }

  @Test
  public void testIndexSteadyNewCursor() throws Exception {
    MongotCursorManagerImpl cursorManager = getCursorManagerWithIndexStatus(IndexStatus.steady());
    long cursorId =
        cursorManager.newCursor(
                MOCK_INDEX_DATABASE_NAME,
                MOCK_INDEX_LAST_OBSERVED_COLLECTION_NAME,
                MOCK_INDEX_COLLECTION_UUID,
                Optional.empty(),
                mockQuery(),
                QueryCursorOptions.empty(),
                QueryOptimizationFlags.DEFAULT_OPTIONS,
                Optional.empty())
            .cursorId;

    // Shouldn't throw a MongotCursorNotFoundException.
    cursorManager.getNextBatch(
        cursorId, CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT, QueryCursorOptions.empty());
  }

  @Test
  public void testIndexSteadyNewIntermediateCursors() throws Exception {
    MongotCursorManagerImpl cursorManager = getCursorManagerWithIndexStatus(IndexStatus.steady());
    var cursors =
        cursorManager.newIntermediateCursors(
            MOCK_INDEX_DATABASE_NAME,
            MOCK_INDEX_LAST_OBSERVED_COLLECTION_NAME,
            MOCK_INDEX_COLLECTION_UUID,
            Optional.empty(),
            mockQuery(),
            INTERMEDIATE_PROTOCOL_VERSION,
            QueryCursorOptions.empty(),
            QueryOptimizationFlags.DEFAULT_OPTIONS,
            Optional.empty());

    // Shouldn't throw a MongotCursorNotFoundException.
    cursorManager.getNextBatch(
        cursors.searchCursorId,
        CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT,
        QueryCursorOptions.empty());
    cursorManager.getNextBatch(
        cursors.metaCursorId,
        CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT,
        QueryCursorOptions.empty());
  }

  @Test(expected = MongotCursorNotFoundException.class)
  public void testInvalidCursorIdGetNextBatch() throws Exception {
    MongotCursorManagerImpl cursorManager = getCursorManager();
    cursorManager.getNextBatch(
        42L, CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT, QueryCursorOptions.empty());
  }

  @Test
  public void testExhaustedCursor() throws Exception {
    MongotCursorManagerImpl cursorManager = getCursorManager(2);

    long cursorId =
        cursorManager.newCursor(
                MOCK_INDEX_DATABASE_NAME,
                MOCK_INDEX_LAST_OBSERVED_COLLECTION_NAME,
                MOCK_INDEX_COLLECTION_UUID,
                Optional.empty(),
                mockQuery(),
                QueryCursorOptions.empty(),
                QueryOptimizationFlags.DEFAULT_OPTIONS,
                Optional.empty())
            .cursorId;

    // The mock BatchProducer returns two batches, then a batch with only
    // 2 results. That signifies that the cursor is exhausted and does not have any more results.
    Assert.assertEquals(
        BatchProducer.MOCK_BATCH_SIZE,
        cursorManager
            .getNextBatch(
                cursorId, CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT, QueryCursorOptions.empty())
            .batch
            .asArray()
            .size());

    Assert.assertEquals(
        BatchProducer.MOCK_BATCH_SIZE,
        cursorManager
            .getNextBatch(
                cursorId, CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT, QueryCursorOptions.empty())
            .batch
            .asArray()
            .size());

    MongotCursorResultInfo lastBatch =
        cursorManager.getNextBatch(
            cursorId, CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT, QueryCursorOptions.empty());
    Assert.assertEquals(BatchProducer.REMAINING_DOCS_LAST_BATCH, lastBatch.batch.asArray().size());

    Assert.assertTrue(lastBatch.exhausted);

    Assert.assertThrows(
        MongotCursorNotFoundException.class,
        () ->
            cursorManager.getNextBatch(
                cursorId, CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT, QueryCursorOptions.empty()));
  }

  @Test
  public void testExhaustedIntermediateSearchCursor() throws Exception {
    MongotCursorManagerImpl cursorManager = getCursorManager(2);

    var cursors =
        cursorManager.newIntermediateCursors(
            MOCK_INDEX_DATABASE_NAME,
            MOCK_INDEX_LAST_OBSERVED_COLLECTION_NAME,
            MOCK_INDEX_COLLECTION_UUID,
            Optional.empty(),
            mockQuery(),
            INTERMEDIATE_PROTOCOL_VERSION,
            QueryCursorOptions.empty(),
            QueryOptimizationFlags.DEFAULT_OPTIONS,
            Optional.empty());

    // The mock BatchProducer returns two batches, then a batch with only
    // 2 results. That signifies that the cursor is exhausted and does not have any more results.
    Assert.assertEquals(
        BatchProducer.MOCK_BATCH_SIZE,
        cursorManager
            .getNextBatch(
                cursors.searchCursorId,
                CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT,
                QueryCursorOptions.empty())
            .batch
            .asArray()
            .size());

    Assert.assertEquals(
        BatchProducer.MOCK_BATCH_SIZE,
        cursorManager
            .getNextBatch(
                cursors.searchCursorId,
                CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT,
                QueryCursorOptions.empty())
            .batch
            .asArray()
            .size());

    MongotCursorResultInfo lastBatch =
        cursorManager.getNextBatch(
            cursors.searchCursorId,
            CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT,
            QueryCursorOptions.empty());
    Assert.assertEquals(BatchProducer.REMAINING_DOCS_LAST_BATCH, lastBatch.batch.asArray().size());

    Assert.assertTrue(lastBatch.exhausted);

    Assert.assertThrows(
        MongotCursorNotFoundException.class,
        () ->
            cursorManager.getNextBatch(
                cursors.searchCursorId,
                CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT,
                QueryCursorOptions.empty()));
  }

  @Test
  public void testMetaResults() throws Exception {
    MongotCursorManagerImpl cursorManager = getCursorManager();
    SearchCursorInfo cursorInfo =
        cursorManager.newCursor(
            MOCK_INDEX_DATABASE_NAME,
            MOCK_INDEX_LAST_OBSERVED_COLLECTION_NAME,
            MOCK_INDEX_COLLECTION_UUID,
            Optional.empty(),
            mockQuery(),
            QueryCursorOptions.empty(),
            QueryOptimizationFlags.DEFAULT_OPTIONS,
            Optional.empty());

    @Var long docCount = 0;
    @Var boolean exhausted = false;
    while (!exhausted) {
      MongotCursorResultInfo getMoreInfo =
          cursorManager.getNextBatch(
              cursorInfo.cursorId,
              CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT,
              QueryCursorOptions.empty());
      docCount += getMoreInfo.batch.asArray().size();
      exhausted = getMoreInfo.exhausted;
    }

    Assert.assertEquals(docCount, cursorInfo.metaResults.count().getTotalOrLower().longValue());
  }

  @Test
  public void testIntermediateMetaResults() throws Exception {
    MongotCursorManagerImpl cursorManager = getCursorManager();
    var cursors =
        cursorManager.newIntermediateCursors(
            MOCK_INDEX_DATABASE_NAME,
            MOCK_INDEX_LAST_OBSERVED_COLLECTION_NAME,
            MOCK_INDEX_COLLECTION_UUID,
            Optional.empty(),
            mockQuery(),
            INTERMEDIATE_PROTOCOL_VERSION,
            QueryCursorOptions.empty(),
            QueryOptimizationFlags.DEFAULT_OPTIONS,
            Optional.empty());

    @Var long docCount = 0;
    @Var boolean exhausted = false;
    while (!exhausted) {
      MongotCursorResultInfo getMoreInfo =
          cursorManager.getNextBatch(
              cursors.searchCursorId,
              CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT,
              QueryCursorOptions.empty());
      docCount += getMoreInfo.batch.asArray().size();
      exhausted = getMoreInfo.exhausted;
    }

    assertMetaCursorCount(cursorManager, cursors.metaCursorId, docCount);
  }

  @Test
  public void testKillCursor() throws Exception {
    MongotCursorManagerImpl cursorManager = getCursorManager();
    long cursorId =
        cursorManager.newCursor(
                MOCK_INDEX_DATABASE_NAME,
                MOCK_INDEX_LAST_OBSERVED_COLLECTION_NAME,
                MOCK_INDEX_COLLECTION_UUID,
                Optional.empty(),
                mockQuery(),
                QueryCursorOptions.empty(),
                QueryOptimizationFlags.DEFAULT_OPTIONS,
                Optional.empty())
            .cursorId;

    // Shouldn't throw a MongotCursorNotFoundException.
    cursorManager.getNextBatch(
        cursorId, CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT, QueryCursorOptions.empty());

    cursorManager.killCursor(cursorId);

    try {
      cursorManager.getNextBatch(
          cursorId, CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT, QueryCursorOptions.empty());
      Assert.fail("did not throw MongotCursorNotFoundException after killCursor");
    } catch (MongotCursorNotFoundException ignored) {
      // expected
    }
  }

  @Test
  public void testKillIntermediateCursors() throws Exception {
    MongotCursorManagerImpl cursorManager = getCursorManager();
    var cursors =
        cursorManager.newIntermediateCursors(
            MOCK_INDEX_DATABASE_NAME,
            MOCK_INDEX_LAST_OBSERVED_COLLECTION_NAME,
            MOCK_INDEX_COLLECTION_UUID,
            Optional.empty(),
            mockQuery(),
            INTERMEDIATE_PROTOCOL_VERSION,
            QueryCursorOptions.empty(),
            QueryOptimizationFlags.DEFAULT_OPTIONS,
            Optional.empty());

    // Shouldn't throw a MongotCursorNotFoundException.
    cursorManager.getNextBatch(
        cursors.searchCursorId,
        CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT,
        QueryCursorOptions.empty());
    cursorManager.getNextBatch(
        cursors.metaCursorId,
        CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT,
        QueryCursorOptions.empty());

    cursorManager.killCursor(cursors.searchCursorId);
    cursorManager.killCursor(cursors.metaCursorId);

    try {
      cursorManager.getNextBatch(
          cursors.searchCursorId,
          CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT,
          QueryCursorOptions.empty());
      Assert.fail("did not throw MongotCursorNotFoundException after killCursor");
    } catch (MongotCursorNotFoundException ignored) {
      // expected
    }

    try {
      cursorManager.getNextBatch(
          cursors.metaCursorId,
          CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT,
          QueryCursorOptions.empty());
      Assert.fail("did not throw MongotCursorNotFoundException after killCursor");
    } catch (MongotCursorNotFoundException ignored) {
      // expected
    }
  }

  @Test
  public void testKillCursorNoCursor() {
    MongotCursorManagerImpl cursorManager = getCursorManager();
    // Shouldn't throw any unchecked exceptions.
    cursorManager.killCursor(13L);
  }

  @Test
  public void testKillIndexCursors() throws Exception {
    MongotCursorManagerImpl cursorManager = getCursorManager();
    long cursorId1 =
        cursorManager.newCursor(
                MOCK_INDEX_DATABASE_NAME,
                MOCK_INDEX_LAST_OBSERVED_COLLECTION_NAME,
                MOCK_INDEX_COLLECTION_UUID,
                Optional.empty(),
                mockQuery(),
                QueryCursorOptions.empty(),
                QueryOptimizationFlags.DEFAULT_OPTIONS,
                Optional.empty())
            .cursorId;

    long cursorId2 =
        cursorManager.newCursor(
                MOCK_INDEX_DATABASE_NAME,
                MOCK_INDEX_LAST_OBSERVED_COLLECTION_NAME,
                MOCK_INDEX_COLLECTION_UUID,
                Optional.empty(),
                mockQuery(),
                QueryCursorOptions.empty(),
                QueryOptimizationFlags.DEFAULT_OPTIONS,
                Optional.empty())
            .cursorId;

    // Shouldn't throw a MongotCursorNotFoundException.
    cursorManager.getNextBatch(
        cursorId1, CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT, QueryCursorOptions.empty());
    cursorManager.getNextBatch(
        cursorId2, CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT, QueryCursorOptions.empty());

    cursorManager.killIndexCursors(MOCK_INDEX_GENERATION_ID);
    Assert.assertThrows(
        MongotCursorNotFoundException.class,
        () ->
            cursorManager.getNextBatch(
                cursorId1, CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT, QueryCursorOptions.empty()));
    Assert.assertThrows(
        MongotCursorNotFoundException.class,
        () ->
            cursorManager.getNextBatch(
                cursorId2, CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT, QueryCursorOptions.empty()));
  }

  @Test
  public void testKillIntermediateIndexCursors() throws Exception {
    MongotCursorManagerImpl cursorManager = getCursorManager();
    var cursors1 =
        cursorManager.newIntermediateCursors(
            MOCK_INDEX_DATABASE_NAME,
            MOCK_INDEX_LAST_OBSERVED_COLLECTION_NAME,
            MOCK_INDEX_COLLECTION_UUID,
            Optional.empty(),
            mockQuery(),
            INTERMEDIATE_PROTOCOL_VERSION,
            QueryCursorOptions.empty(),
            QueryOptimizationFlags.DEFAULT_OPTIONS,
            Optional.empty());

    var cursors2 =
        cursorManager.newIntermediateCursors(
            MOCK_INDEX_DATABASE_NAME,
            MOCK_INDEX_LAST_OBSERVED_COLLECTION_NAME,
            MOCK_INDEX_COLLECTION_UUID,
            Optional.empty(),
            mockQuery(),
            INTERMEDIATE_PROTOCOL_VERSION,
            QueryCursorOptions.empty(),
            QueryOptimizationFlags.DEFAULT_OPTIONS,
            Optional.empty());

    // Shouldn't throw a MongotCursorNotFoundException.
    cursorManager.getNextBatch(
        cursors1.searchCursorId,
        CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT,
        QueryCursorOptions.empty());
    cursorManager.getNextBatch(
        cursors1.metaCursorId,
        CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT,
        QueryCursorOptions.empty());
    cursorManager.getNextBatch(
        cursors2.searchCursorId,
        CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT,
        QueryCursorOptions.empty());
    cursorManager.getNextBatch(
        cursors2.metaCursorId,
        CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT,
        QueryCursorOptions.empty());

    cursorManager.killIndexCursors(MOCK_INDEX_GENERATION_ID);
    Assert.assertThrows(
        MongotCursorNotFoundException.class,
        () ->
            cursorManager.getNextBatch(
                cursors1.searchCursorId,
                CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT,
                QueryCursorOptions.empty()));
    Assert.assertThrows(
        MongotCursorNotFoundException.class,
        () ->
            cursorManager.getNextBatch(
                cursors1.metaCursorId,
                CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT,
                QueryCursorOptions.empty()));
    Assert.assertThrows(
        MongotCursorNotFoundException.class,
        () ->
            cursorManager.getNextBatch(
                cursors2.searchCursorId,
                CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT,
                QueryCursorOptions.empty()));
    Assert.assertThrows(
        MongotCursorNotFoundException.class,
        () ->
            cursorManager.getNextBatch(
                cursors2.metaCursorId,
                CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT,
                QueryCursorOptions.empty()));
  }

  @Test
  public void testKillIntermediateAndNormalIndexCursors() throws Exception {
    MongotCursorManagerImpl cursorManager = getCursorManager();
    var cursors1 =
        cursorManager.newIntermediateCursors(
            MOCK_INDEX_DATABASE_NAME,
            MOCK_INDEX_LAST_OBSERVED_COLLECTION_NAME,
            MOCK_INDEX_COLLECTION_UUID,
            Optional.empty(),
            mockQuery(),
            INTERMEDIATE_PROTOCOL_VERSION,
            QueryCursorOptions.empty(),
            QueryOptimizationFlags.DEFAULT_OPTIONS,
            Optional.empty());

    var cursor2 =
        cursorManager.newCursor(
                MOCK_INDEX_DATABASE_NAME,
                MOCK_INDEX_LAST_OBSERVED_COLLECTION_NAME,
                MOCK_INDEX_COLLECTION_UUID,
                Optional.empty(),
                mockQuery(),
                QueryCursorOptions.empty(),
                QueryOptimizationFlags.DEFAULT_OPTIONS,
                Optional.empty())
            .cursorId;

    // Shouldn't throw a MongotCursorNotFoundException.
    cursorManager.getNextBatch(
        cursors1.searchCursorId,
        CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT,
        QueryCursorOptions.empty());
    cursorManager.getNextBatch(
        cursors1.metaCursorId,
        CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT,
        QueryCursorOptions.empty());
    cursorManager.getNextBatch(
        cursor2, CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT, QueryCursorOptions.empty());

    cursorManager.killIndexCursors(MOCK_INDEX_GENERATION_ID);
    Assert.assertThrows(
        MongotCursorNotFoundException.class,
        () ->
            cursorManager.getNextBatch(
                cursors1.searchCursorId,
                CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT,
                QueryCursorOptions.empty()));
    Assert.assertThrows(
        MongotCursorNotFoundException.class,
        () ->
            cursorManager.getNextBatch(
                cursors1.metaCursorId,
                CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT,
                QueryCursorOptions.empty()));
    Assert.assertThrows(
        MongotCursorNotFoundException.class,
        () ->
            cursorManager.getNextBatch(
                cursor2, CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT, QueryCursorOptions.empty()));
  }

  @Test
  public void testClosedCursorManagerDoesNotAcceptNewOperations() throws Exception {
    MongotCursorManagerImpl cursorManager =
        getCursorManager(
            Duration.ofMillis(5),
            Duration.ofMillis(500),
            Bytes.ofMebi(48),
            Bytes.ofMebi(16),
            Bytes.ofMebi(17));

    long cursorId =
        cursorManager.newCursor(
                MOCK_INDEX_DATABASE_NAME,
                MOCK_INDEX_LAST_OBSERVED_COLLECTION_NAME,
                MOCK_INDEX_COLLECTION_UUID,
                Optional.empty(),
                mockQuery(),
                QueryCursorOptions.empty(),
                QueryOptimizationFlags.DEFAULT_OPTIONS,
                Optional.empty())
            .cursorId;

    var cursors =
        cursorManager.newIntermediateCursors(
            MOCK_INDEX_DATABASE_NAME,
            MOCK_INDEX_LAST_OBSERVED_COLLECTION_NAME,
            MOCK_INDEX_COLLECTION_UUID,
            Optional.empty(),
            mockQuery(),
            INTERMEDIATE_PROTOCOL_VERSION,
            QueryCursorOptions.empty(),
            QueryOptimizationFlags.DEFAULT_OPTIONS,
            Optional.empty());

    cursorManager.close();

    TestUtils.assertThrows(
        "is closed",
        IllegalStateException.class,
        () ->
            cursorManager.getNextBatch(
                cursorId, CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT, QueryCursorOptions.empty()));
    TestUtils.assertThrows(
        "is closed",
        IllegalStateException.class,
        () ->
            cursorManager.getNextBatch(
                cursors.searchCursorId,
                CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT,
                QueryCursorOptions.empty()));
    TestUtils.assertThrows(
        "is closed",
        IllegalStateException.class,
        () ->
            cursorManager.getNextBatch(
                cursors.metaCursorId,
                CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT,
                QueryCursorOptions.empty()));
    TestUtils.assertThrows(
        "is closed",
        IllegalStateException.class,
        () ->
            cursorManager.newCursor(
                MOCK_INDEX_DATABASE_NAME,
                MOCK_INDEX_LAST_OBSERVED_COLLECTION_NAME,
                MOCK_INDEX_COLLECTION_UUID,
                Optional.empty(),
                mockQuery(),
                QueryCursorOptions.empty(),
                QueryOptimizationFlags.DEFAULT_OPTIONS,
                Optional.empty()));
    TestUtils.assertThrows(
        "is closed",
        IllegalStateException.class,
        () ->
            cursorManager.newIntermediateCursors(
                MOCK_INDEX_DATABASE_NAME,
                MOCK_INDEX_LAST_OBSERVED_COLLECTION_NAME,
                MOCK_INDEX_COLLECTION_UUID,
                Optional.empty(),
                mockQuery(),
                INTERMEDIATE_PROTOCOL_VERSION,
                QueryCursorOptions.empty(),
                QueryOptimizationFlags.DEFAULT_OPTIONS,
                Optional.empty()));

    TestUtils.assertThrows(
        "is closed",
        IllegalStateException.class,
        () -> cursorManager.killIdleCursorsSince(Instant.now()));
  }

  @Test
  public void testKillIndexCursorsForTwoIndexesWithSameObjectId() throws Exception {
    DefaultIndexCatalog indexCatalog = new DefaultIndexCatalog();
    InitializedIndexCatalog initializedIndexCatalog = new InitializedIndexCatalog();
    var metrics = new MetricsFactory("factory", new SimpleMeterRegistry());
    MongotCursorManagerImpl cursorManager =
        new MongotCursorManagerImpl(
            indexCatalog,
            initializedIndexCatalog,
            mock(NamedScheduledExecutorService.class),
            metrics,
            CursorIdSupplier.createDefault());
    ObjectId indexId = new ObjectId();

    IndexGeneration indexGen1 = mockIndexGeneration(indexId);
    GenerationId id1 = indexGen1.getGenerationId();

    IndexGeneration indexGen2 = mockIndexGeneration(GenerationIdBuilder.incrementUser(id1));
    GenerationId id2 = indexGen2.getGenerationId();

    // Cursor on the first index:
    indexCatalog.addIndex(indexGen1);
    initializedIndexCatalog.addIndex(mockInitializedIndex(indexGen1));
    long cursorId1 =
        cursorManager.newCursor(
                MOCK_INDEX_DATABASE_NAME,
                MOCK_INDEX_LAST_OBSERVED_COLLECTION_NAME,
                MOCK_INDEX_COLLECTION_UUID,
                Optional.empty(),
                mockQuery(),
                QueryCursorOptions.empty(),
                QueryOptimizationFlags.DEFAULT_OPTIONS,
                Optional.empty())
            .cursorId;

    // replace an old index with a new one.
    indexCatalog.addIndex(indexGen2);
    initializedIndexCatalog.addIndex(mockInitializedIndex(indexGen2));
    // Cursor on the second index:
    long cursorId2 =
        cursorManager.newCursor(
                MOCK_INDEX_DATABASE_NAME,
                MOCK_INDEX_LAST_OBSERVED_COLLECTION_NAME,
                MOCK_INDEX_COLLECTION_UUID,
                Optional.empty(),
                mockQuery(),
                QueryCursorOptions.empty(),
                QueryOptimizationFlags.DEFAULT_OPTIONS,
                Optional.empty())
            .cursorId;

    // index1 is not in the catalog. Still, it's cursor should be working
    Assert.assertTrue(cursorManager.hasOpenCursors(id1));
    cursorManager.getNextBatch(
        cursorId1, CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT, QueryCursorOptions.empty());
    Assert.assertTrue(cursorManager.hasOpenCursors(id2));

    // kill for the first index, make sure the cursor on the second one is still valid
    cursorManager.killIndexCursors(id1);
    Assert.assertThrows(
        MongotCursorNotFoundException.class,
        () ->
            cursorManager.getNextBatch(
                cursorId1, CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT, QueryCursorOptions.empty()));
    Assert.assertFalse(cursorManager.hasOpenCursors(id1));

    // Should still be valid
    cursorManager.getNextBatch(
        cursorId2, CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT, QueryCursorOptions.empty());
    Assert.assertTrue(cursorManager.hasOpenCursors(id2));
  }

  @Test
  public void testKillIndexCursorsNoOpenCursors() {
    MongotCursorManagerImpl cursorManager = getCursorManager();
    cursorManager.killIndexCursors(MOCK_INDEX_GENERATION_ID);
  }

  @Test
  public void testKillIndexCursorsNonExistentIndex() {
    MongotCursorManagerImpl cursorManager = getCursorManager();
    cursorManager.killIndexCursors(GenerationIdBuilder.create());
  }

  @Test
  public void testEmptyCursorsDoNotAffectHasOpenCursors() throws Exception {
    var cursorManager = getCursorManager();
    long cursorId =
        cursorManager.newCursor(
                "invalid",
                "invalid",
                MOCK_INDEX_COLLECTION_UUID,
                Optional.empty(),
                mockQuery(),
                QueryCursorOptions.empty(),
                QueryOptimizationFlags.DEFAULT_OPTIONS,
                Optional.empty())
            .cursorId;
    Assert.assertFalse(cursorManager.hasOpenCursors(MOCK_INDEX_GENERATION_ID));
    assertEmptyCursor(cursorManager, cursorId);
  }

  @Test
  public void testEmptyIntermediateCursorsDoNotAffectHasOpenCursors() throws Exception {
    var cursorManager = getCursorManager();
    var cursors =
        cursorManager.newIntermediateCursors(
            "invalid",
            "invalid",
            MOCK_INDEX_COLLECTION_UUID,
            Optional.empty(),
            mockQuery(),
            INTERMEDIATE_PROTOCOL_VERSION,
            QueryCursorOptions.empty(),
            QueryOptimizationFlags.DEFAULT_OPTIONS,
            Optional.empty());

    Assert.assertFalse(cursorManager.hasOpenCursors(MOCK_INDEX_GENERATION_ID));
    assertEmptyCursor(cursorManager, cursors.searchCursorId);
    assertMetaCursorCount(cursorManager, cursors.metaCursorId, 0);
  }

  @Test
  public void testHasOpenCursorsForAvailableIndex() throws Exception {
    var cursorManager = getCursorManager();
    long cursorId =
        cursorManager.newCursor(
                MOCK_INDEX_DATABASE_NAME,
                MOCK_INDEX_LAST_OBSERVED_COLLECTION_NAME,
                MOCK_INDEX_COLLECTION_UUID,
                Optional.empty(),
                mockQuery(),
                QueryCursorOptions.empty(),
                QueryOptimizationFlags.DEFAULT_OPTIONS,
                Optional.empty())
            .cursorId;
    Assert.assertTrue(cursorManager.hasOpenCursors(MOCK_INDEX_GENERATION_ID));
    cursorManager.killCursor(cursorId);
    Assert.assertFalse(cursorManager.hasOpenCursors(MOCK_INDEX_GENERATION_ID));
  }

  @Test
  public void testHasOpenIntermediateCursorsForAvailableIndex() throws Exception {
    var cursorManager = getCursorManager();
    var cursors =
        cursorManager.newIntermediateCursors(
            MOCK_INDEX_DATABASE_NAME,
            MOCK_INDEX_LAST_OBSERVED_COLLECTION_NAME,
            MOCK_INDEX_COLLECTION_UUID,
            Optional.empty(),
            mockQuery(),
            INTERMEDIATE_PROTOCOL_VERSION,
            QueryCursorOptions.empty(),
            QueryOptimizationFlags.DEFAULT_OPTIONS,
            Optional.empty());

    Assert.assertTrue(cursorManager.hasOpenCursors(MOCK_INDEX_GENERATION_ID));
    cursorManager.killCursor(cursors.searchCursorId);
    Assert.assertTrue(cursorManager.hasOpenCursors(MOCK_INDEX_GENERATION_ID));
    cursorManager.killCursor(cursors.metaCursorId);
    Assert.assertFalse(cursorManager.hasOpenCursors(MOCK_INDEX_GENERATION_ID));
  }

  @Test
  public void testHasOpenCursorsReturnsFalseForExhaustedCursor() throws Exception {
    var cursorManager = getCursorManager(1);
    long cursorId =
        cursorManager.newCursor(
                MOCK_INDEX_DATABASE_NAME,
                MOCK_INDEX_LAST_OBSERVED_COLLECTION_NAME,
                MOCK_INDEX_COLLECTION_UUID,
                Optional.empty(),
                mockQuery(),
                QueryCursorOptions.empty(),
                QueryOptimizationFlags.DEFAULT_OPTIONS,
                Optional.empty())
            .cursorId;
    Assert.assertTrue(cursorManager.hasOpenCursors(MOCK_INDEX_GENERATION_ID));
    cursorManager.getNextBatch(
        cursorId, CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT, QueryCursorOptions.empty());

    // should be killed here:
    cursorManager.getNextBatch(
        cursorId, CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT, QueryCursorOptions.empty());
    Assert.assertFalse(cursorManager.hasOpenCursors(MOCK_INDEX_GENERATION_ID));
  }

  @Test
  public void testHasOpenCursorsReturnsFalseForExhaustedIntermediateCursors() throws Exception {
    var cursorManager = getCursorManager(1);
    var cursors =
        cursorManager.newIntermediateCursors(
            MOCK_INDEX_DATABASE_NAME,
            MOCK_INDEX_LAST_OBSERVED_COLLECTION_NAME,
            MOCK_INDEX_COLLECTION_UUID,
            Optional.empty(),
            mockQuery(),
            INTERMEDIATE_PROTOCOL_VERSION,
            QueryCursorOptions.empty(),
            QueryOptimizationFlags.DEFAULT_OPTIONS,
            Optional.empty());

    Assert.assertTrue(cursorManager.hasOpenCursors(MOCK_INDEX_GENERATION_ID));

    @Var boolean exhausted = false;
    while (!exhausted) {
      var batchInfo =
          cursorManager.getNextBatch(
              cursors.searchCursorId,
              CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT,
              QueryCursorOptions.empty());
      exhausted = batchInfo.exhausted;
    }

    // Should still have the meta cursor
    Assert.assertTrue(cursorManager.hasOpenCursors(MOCK_INDEX_GENERATION_ID));

    exhausted = false;
    while (!exhausted) {
      var batchInfo =
          cursorManager.getNextBatch(
              cursors.metaCursorId,
              CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT,
              QueryCursorOptions.empty());
      exhausted = batchInfo.exhausted;
    }

    Assert.assertFalse(cursorManager.hasOpenCursors(MOCK_INDEX_GENERATION_ID));
  }

  @Test
  public void testConcurrentNewCursors() throws Exception {
    MongotCursorManagerImpl cursorManager = getCursorManager();
    Set<Long> cursorIds = Collections.synchronizedSet(new HashSet<>());

    // Create 100 tasks that create a regular cursor and two intermediate cursors
    // and adds them to cursorIds
    int numCursors = 100;
    Collection<Callable<Void>> tasks =
        IntStream.range(0, numCursors)
            .mapToObj(
                i ->
                    (Callable<Void>)
                        () -> {
                          long cursorId =
                              cursorManager.newCursor(
                                      MOCK_INDEX_DATABASE_NAME,
                                      MOCK_INDEX_LAST_OBSERVED_COLLECTION_NAME,
                                      MOCK_INDEX_COLLECTION_UUID,
                                      Optional.empty(),
                                      mockQuery(),
                                      QueryCursorOptions.empty(),
                                      QueryOptimizationFlags.DEFAULT_OPTIONS,
                                      Optional.empty())
                                  .cursorId;
                          cursorIds.add(cursorId);

                          var cursors =
                              cursorManager.newIntermediateCursors(
                                  MOCK_INDEX_DATABASE_NAME,
                                  MOCK_INDEX_LAST_OBSERVED_COLLECTION_NAME,
                                  MOCK_INDEX_COLLECTION_UUID,
                                  Optional.empty(),
                                  mockQuery(),
                                  INTERMEDIATE_PROTOCOL_VERSION,
                                  QueryCursorOptions.empty(),
                                  QueryOptimizationFlags.DEFAULT_OPTIONS,
                                  Optional.empty());
                          cursorIds.add(cursors.searchCursorId);
                          cursorIds.add(cursors.metaCursorId);
                          return null;
                        })
            .collect(Collectors.toList());

    // Invoke all of the tasks and let them finish.
    ExecutorService executor = Executors.newCachedThreadPool();
    executor.invokeAll(tasks, 5, TimeUnit.SECONDS);

    // Ensure that each task was allocated its own unique cursor id.
    Assert.assertEquals(
        "duplicate cursor id allocation detected", numCursors * 3, cursorIds.size());

    // Ensure that each of the cursor ids are backed by a cursor.
    for (long cursorId : cursorIds) {
      // Shouldn't throw any exception.
      cursorManager.getNextBatch(
          cursorId, CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT, QueryCursorOptions.empty());
    }
  }

  @Test
  public void testConcurrentKillCursor() throws Exception {
    // Want to keep producing cursor batches until we've dropped the cursors.
    MongotCursorManagerImpl cursorManager = getCursorManager(Integer.MAX_VALUE);

    long cursorId =
        cursorManager.newCursor(
                MOCK_INDEX_DATABASE_NAME,
                MOCK_INDEX_LAST_OBSERVED_COLLECTION_NAME,
                MOCK_INDEX_COLLECTION_UUID,
                Optional.empty(),
                mockQuery(),
                QueryCursorOptions.empty(),
                QueryOptimizationFlags.DEFAULT_OPTIONS,
                Optional.empty())
            .cursorId;

    // Start a task that will get the next batch until it throws an exception, and ensure that it
    // was due to it being killed.
    AtomicBoolean killed = new AtomicBoolean(false);
    Runnable task = waitForCursorKillGetNextBatch(cursorManager, cursorId, killed);
    ExecutorService executorService = Executors.newSingleThreadExecutor();
    Future<?> future = executorService.submit(task);

    // Wait to help make sure the task is running, then drop the index cursors.
    Thread.sleep(500);

    // NOTE: this in theory could result in a false positive if a cursor for some reason threw a
    // MongotCursorNotFoundException after dropped was set to true but prior to killCursor being
    // called. All of the workarounds I can presently think of involve either potentially
    // flakey results (e.g. move dropped to after killIndexCursors) or would themselves add
    // synchronization that would invalidate the whole point of this test.
    killed.set(true);
    cursorManager.killCursor(cursorId);

    // Let the task finish and ensure it didn't throw an Exception.
    future.get(500, TimeUnit.MILLISECONDS);
  }

  @Test
  public void testConcurrentKillIndexCursorsGetExplain() throws Exception {
    // Want to keep getting explain until we've dropped the cursors.
    MongotCursorManagerImpl cursorManager = getCursorManager(Integer.MAX_VALUE);

    // Get numCursors cursor ids.
    int numCursors = 4;
    Set<Long> cursorIds =
        IntStream.range(0, numCursors)
            .mapToObj(
                i -> {
                  try {
                    return cursorManager.newCursor(
                            MOCK_INDEX_DATABASE_NAME,
                            MOCK_INDEX_LAST_OBSERVED_COLLECTION_NAME,
                            MOCK_INDEX_COLLECTION_UUID,
                            Optional.empty(),
                            mockQuery(),
                            QueryCursorOptions.empty(),
                            QueryOptimizationFlags.DEFAULT_OPTIONS,
                            Optional.empty())
                        .cursorId;
                  } catch (Exception e) {
                    throw new RuntimeException(e);
                  }
                })
            .collect(Collectors.toSet());

    // Create tasks that get the next cursor batch until a MongotCursorNotFoundException is thrown,
    // at which point they ensure that it was supposed to have been thrown.
    AtomicBoolean dropped = new AtomicBoolean(false);
    Collection<Runnable> tasks =
        cursorIds.stream()
            .map(
                cursorId ->
                    waitForCursorKillGeneric(
                        cursorManager::getExplainQueryState, cursorId, dropped))
            .collect(Collectors.toList());

    // Invoke all of the tasks and let them start running.
    ExecutorService executor = Executors.newCachedThreadPool();
    List<Future<?>> futures = tasks.stream().map(executor::submit).collect(Collectors.toList());

    // Wait to help make sure the threads are running, then drop the index cursors.
    Thread.sleep(500);

    // NOTE: this in theory could result in a false positive if a cursor for some reason threw a
    // MongotCursorNotFoundException after dropped was set to true but prior to killIndexCursors
    // being called. All of the workarounds I can presently think of involve either potentially
    // flakey results (e.g. move dropped to after killIndexCursors) or would themselves add
    // synchronization that would invalidate the whole point of this test.
    dropped.set(true);
    cursorManager.killIndexCursors(MOCK_INDEX_GENERATION_ID);

    // Let the tasks finish and ensure they didn't throw an Exception.
    for (Future<?> future : futures) {
      future.get(1, TimeUnit.SECONDS);
    }
  }

  @Test
  public void testConcurrentKillIndexCursors() throws Exception {
    // Want to keep producing cursor batches until we've dropped the cursors.
    MongotCursorManagerImpl cursorManager = getCursorManager(Integer.MAX_VALUE);

    // Get numCursors cursor ids.
    int numCursors = 4;
    Set<Long> cursorIds =
        IntStream.range(0, numCursors)
            .mapToObj(
                i -> {
                  try {
                    return cursorManager.newCursor(
                            MOCK_INDEX_DATABASE_NAME,
                            MOCK_INDEX_LAST_OBSERVED_COLLECTION_NAME,
                            MOCK_INDEX_COLLECTION_UUID,
                            Optional.empty(),
                            mockQuery(),
                            QueryCursorOptions.empty(),
                            QueryOptimizationFlags.DEFAULT_OPTIONS,
                            Optional.empty())
                        .cursorId;
                  } catch (Exception e) {
                    throw new RuntimeException(e);
                  }
                })
            .collect(Collectors.toSet());

    // Create tasks that get the next cursor batch until a MongotCursorNotFoundException is thrown,
    // at which point they ensure that it was supposed to have been thrown.
    AtomicBoolean dropped = new AtomicBoolean(false);
    Collection<Runnable> tasks =
        cursorIds.stream()
            .map(cursorId -> waitForCursorKillGetNextBatch(cursorManager, cursorId, dropped))
            .collect(Collectors.toList());

    // Invoke all of the tasks and let them start running.
    ExecutorService executor = Executors.newCachedThreadPool();
    List<Future<?>> futures = tasks.stream().map(executor::submit).collect(Collectors.toList());

    // Wait to help make sure the threads are running, then drop the index cursors.
    Thread.sleep(500);

    // NOTE: this in theory could result in a false positive if a cursor for some reason threw a
    // MongotCursorNotFoundException after dropped was set to true but prior to killIndexCursors
    // being called. All of the workarounds I can presently think of involve either potentially
    // flakey results (e.g. move dropped to after killIndexCursors) or would themselves add
    // synchronization that would invalidate the whole point of this test.
    dropped.set(true);
    cursorManager.killIndexCursors(MOCK_INDEX_GENERATION_ID);

    // Let the tasks finish and ensure they didn't throw an Exception.
    for (Future<?> future : futures) {
      future.get(1, TimeUnit.SECONDS);
    }
  }

  @Test
  public void testConcurrentKillIntermediateIndexCursors() throws Exception {
    // Want to keep producing cursor batches until we've dropped the cursors.
    MongotCursorManagerImpl cursorManager =
        getCursorManagerWithMetaProducer(Integer.MAX_VALUE, Integer.MAX_VALUE);

    // Get numCursors cursor ids.
    int numCursors = 4;
    Set<Long> cursorIds = new HashSet<>();
    IntStream.range(0, numCursors)
        .forEach(
            i -> {
              try {
                var cursors =
                    cursorManager.newIntermediateCursors(
                        MOCK_INDEX_DATABASE_NAME,
                        MOCK_INDEX_LAST_OBSERVED_COLLECTION_NAME,
                        MOCK_INDEX_COLLECTION_UUID,
                        Optional.empty(),
                        mockQuery(),
                        INTERMEDIATE_PROTOCOL_VERSION,
                        QueryCursorOptions.empty(),
                        QueryOptimizationFlags.DEFAULT_OPTIONS,
                        Optional.empty());
                cursorIds.add(cursors.searchCursorId);
                cursorIds.add(cursors.metaCursorId);
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
            });

    // Create tasks that get the next cursor batch until a MongotCursorNotFoundException is thrown,
    // at which point they ensure that it was supposed to have been thrown.
    AtomicBoolean dropped = new AtomicBoolean(false);
    Collection<Runnable> tasks =
        cursorIds.stream()
            .map(cursorId -> waitForCursorKillGetNextBatch(cursorManager, cursorId, dropped))
            .collect(Collectors.toList());

    // Invoke all of the tasks and let them start running.
    ExecutorService executor = Executors.newCachedThreadPool();
    List<Future<?>> futures = tasks.stream().map(executor::submit).collect(Collectors.toList());

    // Wait to help make sure the threads are running, then drop the index cursors.
    Thread.sleep(500);

    // NOTE: this in theory could result in a false positive if a cursor for some reason threw a
    // MongotCursorNotFoundException after dropped was set to true but prior to killIndexCursors
    // being called. All of the workarounds I can presently think of involve either potentially
    // flakey results (e.g. move dropped to after killIndexCursors) or would themselves add
    // synchronization that would invalidate the whole point of this test.
    dropped.set(true);
    cursorManager.killIndexCursors(MOCK_INDEX_GENERATION_ID);

    // Let the tasks finish and ensure they didn't throw an Exception.
    for (Future<?> future : futures) {
      future.get(1, TimeUnit.SECONDS);
    }
  }

  private static Runnable waitForCursorKillGetNextBatch(
      MongotCursorManagerImpl cursorManager, long cursorId, AtomicBoolean killed) {
    CheckedConsumer<Long, MongotCursorNotFoundException> getNextBatchFunc =
        (Long id) -> {
          try {
            cursorManager.getNextBatch(
                id, CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT, QueryCursorOptions.empty());
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        };

    return waitForCursorKillGeneric(getNextBatchFunc, cursorId, killed);
  }

  private static Runnable waitForCursorKillGeneric(
      CheckedConsumer<Long, MongotCursorNotFoundException> toRun,
      long cursorId,
      AtomicBoolean killed) {
    return () -> {
      while (true) {
        try {
          toRun.accept(cursorId);
        } catch (MongotCursorNotFoundException ignored) {
          Assert.assertTrue(
              "MongotCursorNotFoundException thrown prior to cursor being killed", killed.get());
          break;
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    };
  }

  @Test
  public void testIdleCursorKilledAfterIdleTimeDuration() throws Exception {
    MongotCursorManagerImpl cursorManager = getCursorManager();

    long idleCursorId =
        cursorManager.newCursor(
                MOCK_INDEX_DATABASE_NAME,
                MOCK_INDEX_LAST_OBSERVED_COLLECTION_NAME,
                MOCK_INDEX_COLLECTION_UUID,
                Optional.empty(),
                mockQuery(),
                QueryCursorOptions.empty(),
                QueryOptimizationFlags.DEFAULT_OPTIONS,
                Optional.empty())
            .cursorId;

    var idleTime = Instant.now();

    long nonIdleCursorId =
        cursorManager.newCursor(
                MOCK_INDEX_DATABASE_NAME,
                MOCK_INDEX_LAST_OBSERVED_COLLECTION_NAME,
                MOCK_INDEX_COLLECTION_UUID,
                Optional.empty(),
                mockQuery(),
                QueryCursorOptions.empty(),
                QueryOptimizationFlags.DEFAULT_OPTIONS,
                Optional.empty())
            .cursorId;
    cursorManager.killIdleCursorsSince(idleTime);

    // nonIdleCursor was active since idleTime, so it should still exist
    assertCursorExists(cursorManager, nonIdleCursorId);

    // idleCursor was not used since idleTime, so it is idle
    assertCursorDoesNotExist(cursorManager, idleCursorId);
  }

  @Test
  public void testIdleIntermediateCursorsKilledAfterIdleTimeDuration() throws Exception {
    MongotCursorManagerImpl cursorManager = getCursorManager();

    var idleCursors =
        cursorManager.newIntermediateCursors(
            MOCK_INDEX_DATABASE_NAME,
            MOCK_INDEX_LAST_OBSERVED_COLLECTION_NAME,
            MOCK_INDEX_COLLECTION_UUID,
            Optional.empty(),
            mockQuery(),
            INTERMEDIATE_PROTOCOL_VERSION,
            QueryCursorOptions.empty(),
            QueryOptimizationFlags.DEFAULT_OPTIONS,
            Optional.empty());

    var idleTime = Instant.now();

    var nonIdleCursors =
        cursorManager.newIntermediateCursors(
            MOCK_INDEX_DATABASE_NAME,
            MOCK_INDEX_LAST_OBSERVED_COLLECTION_NAME,
            MOCK_INDEX_COLLECTION_UUID,
            Optional.empty(),
            mockQuery(),
            INTERMEDIATE_PROTOCOL_VERSION,
            QueryCursorOptions.empty(),
            QueryOptimizationFlags.DEFAULT_OPTIONS,
            Optional.empty());
    cursorManager.killIdleCursorsSince(idleTime);

    // nonIdleCursors were active since idleTime, so they should still exist
    assertCursorExists(cursorManager, nonIdleCursors.searchCursorId);
    assertCursorExists(cursorManager, nonIdleCursors.metaCursorId);

    // idleCursors were not used since idleTime, so they are idle
    assertCursorDoesNotExist(cursorManager, idleCursors.searchCursorId);
    assertCursorDoesNotExist(cursorManager, idleCursors.metaCursorId);
  }

  @Test
  public void testGetMoreMakesCursorNonIdle() throws Exception {
    MongotCursorManagerImpl cursorManager = getCursorManager();

    // create two cursors, let one become idle and call getMore on the other
    long idleCursorId =
        cursorManager.newCursor(
                MOCK_INDEX_DATABASE_NAME,
                MOCK_INDEX_LAST_OBSERVED_COLLECTION_NAME,
                MOCK_INDEX_COLLECTION_UUID,
                Optional.empty(),
                mockQuery(),
                QueryCursorOptions.empty(),
                QueryOptimizationFlags.DEFAULT_OPTIONS,
                Optional.empty())
            .cursorId;
    long nonIdleCursorId =
        cursorManager.newCursor(
                MOCK_INDEX_DATABASE_NAME,
                MOCK_INDEX_LAST_OBSERVED_COLLECTION_NAME,
                MOCK_INDEX_COLLECTION_UUID,
                Optional.empty(),
                mockQuery(),
                QueryCursorOptions.empty(),
                QueryOptimizationFlags.DEFAULT_OPTIONS,
                Optional.empty())
            .cursorId;

    // Calling get more on nonIdleCursor should make it non idle
    var idleTime = Instant.now();

    cursorManager.getNextBatch(
        nonIdleCursorId, CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT, QueryCursorOptions.empty());

    cursorManager.killIdleCursorsSince(idleTime);

    // nonIdleCursor was active since idleTime, so it should still exist
    assertCursorExists(cursorManager, nonIdleCursorId);

    // idleCursor was not used since idleTime, so it is idle
    assertCursorDoesNotExist(cursorManager, idleCursorId);
  }

  @Test
  public void testGetMoreMakesIntermediateCursorsNonIdle() throws Exception {
    // Use a meta cursor with 3 batches so it isn't immediately killed
    MongotCursorManagerImpl cursorManager = getCursorManagerWithMetaProducer(3, 3);

    // create two sets of cursors, let one become idle and call getMore on the other
    var idleCursors =
        cursorManager.newIntermediateCursors(
            MOCK_INDEX_DATABASE_NAME,
            MOCK_INDEX_LAST_OBSERVED_COLLECTION_NAME,
            MOCK_INDEX_COLLECTION_UUID,
            Optional.empty(),
            mockQuery(),
            INTERMEDIATE_PROTOCOL_VERSION,
            QueryCursorOptions.empty(),
            QueryOptimizationFlags.DEFAULT_OPTIONS,
            Optional.empty());
    var nonIdleCursors =
        cursorManager.newIntermediateCursors(
            MOCK_INDEX_DATABASE_NAME,
            MOCK_INDEX_LAST_OBSERVED_COLLECTION_NAME,
            MOCK_INDEX_COLLECTION_UUID,
            Optional.empty(),
            mockQuery(),
            INTERMEDIATE_PROTOCOL_VERSION,
            QueryCursorOptions.empty(),
            QueryOptimizationFlags.DEFAULT_OPTIONS,
            Optional.empty());

    // Calling get more on nonIdleCursor should make it non idle
    var idleTime = Instant.now();

    cursorManager.getNextBatch(
        nonIdleCursors.searchCursorId,
        CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT,
        QueryCursorOptions.empty());
    cursorManager.getNextBatch(
        nonIdleCursors.metaCursorId,
        CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT,
        QueryCursorOptions.empty());

    cursorManager.killIdleCursorsSince(idleTime);

    // nonIdleCursor was active since idleTime, so it should still exist
    assertCursorExists(cursorManager, nonIdleCursors.searchCursorId);
    assertCursorExists(cursorManager, nonIdleCursors.metaCursorId);

    // idleCursor was not used since idleTime, so it is idle
    assertCursorDoesNotExist(cursorManager, idleCursors.searchCursorId);
    assertCursorDoesNotExist(cursorManager, idleCursors.metaCursorId);
  }

  @Test
  public void testCursorManagerTracksOpenCursorMetric() throws Exception {
    // Use a meta cursor with 3 batches so it isn't immediately killed
    DefaultIndexCatalog indexCatalog = new DefaultIndexCatalog();
    IndexGeneration indexGeneration = mockIndexGeneration();
    indexCatalog.addIndex(indexGeneration);
    InitializedIndexCatalog initializedIndexCatalog = new InitializedIndexCatalog();
    initializedIndexCatalog.addIndex(mockInitializedIndex(indexGeneration));

    var metrics =
        new MetricsFactory("testCursorManagerTracksOpenCursorMetric", new SimpleMeterRegistry());
    MongotCursorManagerImpl cursorManager =
        new MongotCursorManagerImpl(
            indexCatalog,
            initializedIndexCatalog,
            mock(NamedScheduledExecutorService.class),
            metrics,
            CursorIdSupplier.createDefault());

    Gauge thisMetric = metrics.get("trackedCursors").gauge();
    Assert.assertEquals(0, thisMetric.value(), 0.1);

    var cursor1 =
        cursorManager.newCursor(
            MOCK_INDEX_DATABASE_NAME,
            MOCK_INDEX_LAST_OBSERVED_COLLECTION_NAME,
            MOCK_INDEX_COLLECTION_UUID,
            Optional.empty(),
            mockQuery(),
            QueryCursorOptions.empty(),
            QueryOptimizationFlags.DEFAULT_OPTIONS,
            Optional.empty());
    Assert.assertEquals(1, thisMetric.value(), 0.1);

    cursorManager.newCursor(
        MOCK_INDEX_DATABASE_NAME,
        MOCK_INDEX_LAST_OBSERVED_COLLECTION_NAME,
        MOCK_INDEX_COLLECTION_UUID,
        Optional.empty(),
        mockQuery(),
        QueryCursorOptions.empty(),
        QueryOptimizationFlags.DEFAULT_OPTIONS,
        Optional.empty());
    Assert.assertEquals(2, thisMetric.value(), 0.1);

    cursorManager.killCursor(cursor1.cursorId);
    // should decrement to 1 cursor
    Assert.assertEquals(1, thisMetric.value(), 0.1);
  }

  /**
   * Tests that the cursor manager kills appropriate idle cursors while other thread is calling
   * getMore.
   */
  @Test
  public void testPeriodicIdleCursorReaperConcurrentGetMores() throws Exception {
    Duration cycleInterval = Duration.ofMillis(1); // basically checks idle cursors all the time.
    Duration idleTime = Duration.ofMillis(500);

    var cursorManager =
        getCursorManager(
            cycleInterval, idleTime, Bytes.ofMebi(48), Bytes.ofMebi(16), Bytes.ofMebi(17));
    // create two cursors, let one become idle and call getMore on the other
    long idleCursorId =
        cursorManager.newCursor(
                MOCK_INDEX_DATABASE_NAME,
                MOCK_INDEX_LAST_OBSERVED_COLLECTION_NAME,
                MOCK_INDEX_COLLECTION_UUID,
                Optional.empty(),
                mockQuery(),
                QueryCursorOptions.empty(),
                QueryOptimizationFlags.DEFAULT_OPTIONS,
                Optional.empty())
            .cursorId;
    long activeCursorId =
        cursorManager.newCursor(
                MOCK_INDEX_DATABASE_NAME,
                MOCK_INDEX_LAST_OBSERVED_COLLECTION_NAME,
                MOCK_INDEX_COLLECTION_UUID,
                Optional.empty(),
                mockQuery(),
                QueryCursorOptions.empty(),
                QueryOptimizationFlags.DEFAULT_OPTIONS,
                Optional.empty())
            .cursorId;

    assertCursorExists(cursorManager, activeCursorId);

    // start using activeCursor in the background
    var killed = new AtomicBoolean();
    var executor = Executors.newSingleThreadExecutor();
    executor.submit(waitForCursorKillGetNextBatch(cursorManager, activeCursorId, killed));

    assertCursorExists(cursorManager, activeCursorId);

    Duration idleCursorTimeToLive = idleTime.plus(cycleInterval);
    Thread.sleep(idleCursorTimeToLive.multipliedBy(2).toMillis());

    assertCursorExists(cursorManager, activeCursorId);
    assertCursorDoesNotExist(cursorManager, idleCursorId);

    // stop using activeCursor, and stop the getMores
    killed.set(true);
    cursorManager.killCursor(activeCursorId);

    executor.shutdown();
  }

  private void assertCursorExists(MongotCursorManagerImpl cursorManager, long cursorId)
      throws IOException, MongotCursorNotFoundException {
    // should not throw:
    cursorManager.getNextBatch(
        cursorId, CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT, QueryCursorOptions.empty());
  }

  private void assertCursorDoesNotExist(MongotCursorManagerImpl cursorManager, long cursorId) {
    Assert.assertThrows(
        MongotCursorNotFoundException.class,
        () ->
            cursorManager.getNextBatch(
                cursorId, CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT, QueryCursorOptions.empty()));
  }

  private static void assertEmptyCursor(MongotCursorManagerImpl cursorManager, long cursorId)
      throws Exception {
    // Should return an empty batch
    MongotCursorResultInfo batch =
        cursorManager.getNextBatch(
            cursorId, CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT, QueryCursorOptions.empty());
    Assert.assertEquals(0, batch.batch.asArray().size());
    Assert.assertTrue(batch.exhausted);

    try {
      cursorManager.getNextBatch(
          cursorId, CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT, QueryCursorOptions.empty());
      Assert.fail();
    } catch (MongotCursorNotFoundException expected) {
      // expected
    }
  }

  private void assertMetaCursorCount(
      MongotCursorManagerImpl cursorManager, long cursorId, long expectedCount) throws Exception {
    MongotCursorResultInfo metaResultInfo =
        cursorManager.getNextBatch(
            cursorId, CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT, QueryCursorOptions.empty());

    Assert.assertTrue(metaResultInfo.exhausted);
    Assert.assertEquals(1, metaResultInfo.batch.asArray().getValues().size());
    Assert.assertEquals(
        expectedCount,
        metaResultInfo.batch.asArray().get(0).asDocument().get("count").asInt64().getValue());
  }
}
