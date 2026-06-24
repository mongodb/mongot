package com.xgen.mongot.server.command.search;

import static com.google.common.truth.Truth.assertThat;
import static com.xgen.mongot.index.definition.IndexDefinition.Fields.NUM_PARTITIONS;
import static com.xgen.mongot.util.bson.FloatVector.OriginalType.NATIVE;
import static com.xgen.testing.mongot.mock.cursor.MongotCursorBatches.mockInitialMongotCursorBatchForVectorSearch;
import static com.xgen.testing.mongot.mock.index.IndexGeneration.mockDefinitionGeneration;
import static com.xgen.testing.mongot.mock.index.VectorIndex.MOCK_INDEX_DEFINITION_GENERATION;
import static com.xgen.testing.mongot.mock.index.VectorIndex.MOCK_INDEX_GENERATION_ID;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.base.Supplier;
import com.xgen.mongot.catalog.DefaultIndexCatalog;
import com.xgen.mongot.catalog.IndexCatalog;
import com.xgen.mongot.catalog.InitializedIndexCatalog;
import com.xgen.mongot.cursor.CursorConfig;
import com.xgen.mongot.cursor.CursorQuery;
import com.xgen.mongot.cursor.MongotCursorManager;
import com.xgen.mongot.cursor.MongotCursorResultInfo;
import com.xgen.mongot.cursor.SearchCursorInfo;
import com.xgen.mongot.cursor.serialization.MongotCursorBatch;
import com.xgen.mongot.embedding.providers.EmbeddingServiceManager;
import com.xgen.mongot.featureflag.Feature;
import com.xgen.mongot.featureflag.FeatureFlags;
import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlagConfig;
import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlagRegistry;
import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlags;
import com.xgen.mongot.index.EmptyExplainInformation;
import com.xgen.mongot.index.IndexGeneration;
import com.xgen.mongot.index.IndexMetricValuesSupplier;
import com.xgen.mongot.index.IndexMetricsUpdater;
import com.xgen.mongot.index.IndexUnavailableException;
import com.xgen.mongot.index.InitializedVectorIndex;
import com.xgen.mongot.index.MetaResults;
import com.xgen.mongot.index.ReaderClosedException;
import com.xgen.mongot.index.definition.StoredSourceDefinition;
import com.xgen.mongot.index.definition.VectorIndexDefinition;
import com.xgen.mongot.index.lucene.LuceneVectorIndex;
import com.xgen.mongot.index.lucene.LuceneVectorIndexReader;
import com.xgen.mongot.index.lucene.explain.information.LuceneQuerySpecification;
import com.xgen.mongot.index.lucene.explain.information.MetadataExplainInformation;
import com.xgen.mongot.index.lucene.explain.information.SearchExplainInformation;
import com.xgen.mongot.index.lucene.explain.information.SearchExplainInformationBuilder;
import com.xgen.mongot.index.lucene.explain.tracing.Explain;
import com.xgen.mongot.index.query.DeadlineExceededException;
import com.xgen.mongot.index.query.InvalidQueryException;
import com.xgen.mongot.index.query.MaterializedVectorSearchQuery;
import com.xgen.mongot.index.query.QueryExecutionContext;
import com.xgen.mongot.index.query.operators.VectorSearchCriteria;
import com.xgen.mongot.index.query.operators.VectorSearchFilter;
import com.xgen.mongot.index.query.operators.VectorSearchQueryInput;
import com.xgen.mongot.index.query.operators.mql.Clause;
import com.xgen.mongot.index.status.IndexStatus;
import com.xgen.mongot.metrics.MetricsFactory;
import com.xgen.mongot.searchenvoy.grpc.SearchEnvoyMetadata;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.ErrorType;
import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.bson.FloatVector;
import com.xgen.mongot.util.bson.Vector;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.mongodb.MongoDbServerInfo;
import com.xgen.mongot.util.mongodb.MongoDbVersion;
import com.xgen.testing.mongot.index.IndexMetricsUpdaterBuilder;
import com.xgen.testing.mongot.index.definition.VectorIndexDefinitionBuilder;
import com.xgen.testing.mongot.index.lucene.explain.information.DefaultQueryBuilder;
import com.xgen.testing.mongot.index.lucene.explain.information.QueryExplainInformationBuilder;
import com.xgen.testing.mongot.index.lucene.explain.tracing.FakeExplain;
import com.xgen.testing.mongot.index.query.ApproximateVectorQueryCriteriaBuilder;
import com.xgen.testing.mongot.index.query.ExactVectorCriteriaBuilder;
import com.xgen.testing.mongot.index.query.VectorQueryBuilder;
import com.xgen.testing.mongot.index.query.operators.mql.ClauseBuilder;
import com.xgen.testing.mongot.index.query.operators.mql.MqlFilterOperatorBuilder;
import com.xgen.testing.mongot.index.query.operators.mql.ValueBuilder;
import com.xgen.testing.mongot.mock.index.SearchIndex;
import com.xgen.testing.mongot.mock.index.VectorIndex;
import com.xgen.testing.mongot.mock.index.VectorSearchResultBatch;
import com.xgen.testing.mongot.server.command.search.definition.request.CursorOptionsDefinitionBuilder;
import com.xgen.testing.mongot.server.command.search.definition.request.ExplainDefinitionBuilder;
import com.xgen.testing.mongot.server.command.search.definition.request.VectorSearchCommandDefinitionBuilder;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.types.ObjectId;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

@RunWith(Parameterized.class)
public class VectorSearchCommandTest {
  private static final String DATABASE_NAME = "testDb";
  private static final String COLLECTION_NAME = "testCollection";
  private static final String NAMESPACE = String.format("%s.%s", DATABASE_NAME, COLLECTION_NAME);
  private static final UUID COLLECTION_UUID = UUID.randomUUID();
  private static final String INDEX_NAME = "default";
  private static final int NUM_CANDIDATES = 5000;
  private static final int LIMIT = 1000;
  private static final Vector QUERY_VECTOR = Vector.fromFloats(new float[] {1f, 2f, 3f}, NATIVE);
  private static final FieldPath PATH = FieldPath.parse("testPath");

  private static final MongoDbVersion MIN_STORED_SOURCE_VERSION = new MongoDbVersion(8, 2, 0);

  @Rule public MockitoRule mockitoRule = MockitoJUnit.rule();

  @Parameterized.Parameter public MongoDbVersion mongoDbVersion;

  @Parameterized.Parameters(name = "mongod-{0}")
  public static Iterable<MongoDbVersion> versions() {
    return Arrays.asList(new MongoDbVersion(8, 0, 0), new MongoDbVersion(8, 2, 0));
  }

  private SearchCommandsRegister.BootstrapperMetadata bootstrapperMetadata() {
    return bootstrapperMetadata(registryWithVectorSearchTimeoutEnabled());
  }

  private SearchCommandsRegister.BootstrapperMetadata bootstrapperMetadata(
      DynamicFeatureFlagRegistry dynamicFeatureFlagRegistry) {
    return new SearchCommandsRegister.BootstrapperMetadata(
        "testVersion",
        "localhost",
        () -> new MongoDbServerInfo(Optional.of(this.mongoDbVersion), Optional.of("testRs")),
        FeatureFlags.getDefault(),
        dynamicFeatureFlagRegistry);
  }

  private static DynamicFeatureFlagRegistry registryWithVectorSearchTimeoutEnabled() {
    return new DynamicFeatureFlagRegistry(
        Optional.of(
            List.of(
                new DynamicFeatureFlagConfig(
                    DynamicFeatureFlags.VECTOR_SEARCH_QUERY_TIMEOUT.getName(),
                    DynamicFeatureFlagConfig.Phase.ENABLED,
                    List.of(),
                    List.of(),
                    0,
                    DynamicFeatureFlagConfig.Scope.MONGOT_CLUSTER))),
        Optional.empty(),
        Optional.empty(),
        Optional.of(new ObjectId()));
  }

  // Simple mock - embedding service is never invoked for queryVector-based tests
  private static final Supplier<EmbeddingServiceManager> MOCK_EMBEDDING_SERVICE =
      VectorSearchCommandTest::mockEmbeddingService;

  private static EmbeddingServiceManager mockEmbeddingService() {
    return mock(EmbeddingServiceManager.class);
  }

  private static VectorSearchFilter getFilter() throws BsonParseException {
    Clause filter =
        ClauseBuilder.orClause()
            .addClause(
                ClauseBuilder.simpleClause()
                    .path(FieldPath.parse("cost"))
                    .addOperator(
                        MqlFilterOperatorBuilder.eq().value(ValueBuilder.intNumber(100)).build())
                    .build())
            .addClause(
                ClauseBuilder.simpleClause()
                    .path(FieldPath.parse("cost"))
                    .addOperator(
                        MqlFilterOperatorBuilder.lt().value(ValueBuilder.intNumber(50)).build())
                    .build())
            .build();
    return new VectorSearchFilter.ClauseFilter(filter);
  }

  @Test
  public void testValidIndex() throws Exception {

    var mocks = new Mocks();
    var query =
        VectorQueryBuilder.builder()
            .index(INDEX_NAME)
            .criteria(
                ApproximateVectorQueryCriteriaBuilder.builder()
                    .limit(LIMIT)
                    .numCandidates(NUM_CANDIDATES)
                    .queryVector(QUERY_VECTOR)
                    .path(PATH)
                    .filter(getFilter())
                    .build())
            .build();
    var command =
        new VectorSearchCommand(
            VectorSearchCommandDefinitionBuilder.builder()
                .db(DATABASE_NAME)
                .collectionName(COLLECTION_NAME)
                .collectionUuid(COLLECTION_UUID)
                .vectorSearchQuery(query)
                .build(),
            mocks.catalog,
            mocks.initializedIndexCatalog,
            bootstrapperMetadata(),
            MOCK_EMBEDDING_SERVICE,
            new VectorSearchCommand.Metrics(mockMetricsFactory()),
            mocks.cursorManager,
            CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT);

    var result = command.run();

    // ensure that reader is being called with the exact same query as in command
    Mockito.verify(mocks.reader, times(1))
        .query(
            new MaterializedVectorSearchQuery(query, query.criteria().queryVector().get()),
            QueryExecutionContext.empty());
    var expected =
        mockInitialMongotCursorBatchForVectorSearch(0, new VectorSearchResultBatch(4), NAMESPACE)
            .toBson();
    Assert.assertEquals(expected, result);
    Assert.assertTrue(command.getCreatedCursorIds().isEmpty());
    Assert.assertEquals(
        1, mocks.metricsUpdater.getQueryingMetricsUpdater().getVectorCommandCounter().count(), 0);
  }

  @Test
  public void testDeadlineTimestampIsThreadedToQuery() throws Exception {
    var mocks = new Mocks();
    long deadline = System.currentTimeMillis();
    var query =
        VectorQueryBuilder.builder()
            .index(INDEX_NAME)
            .criteria(
                ApproximateVectorQueryCriteriaBuilder.builder()
                    .limit(LIMIT)
                    .numCandidates(NUM_CANDIDATES)
                    .queryVector(QUERY_VECTOR)
                    .path(PATH)
                    .build())
            .build();
    var command =
        new VectorSearchCommand(
            VectorSearchCommandDefinitionBuilder.builder()
                .db(DATABASE_NAME)
                .collectionName(COLLECTION_NAME)
                .collectionUuid(COLLECTION_UUID)
                .vectorSearchQuery(query)
                .deadlineTimestampMs(deadline)
                .build(),
            mocks.catalog,
            mocks.initializedIndexCatalog,
            bootstrapperMetadata(),
            MOCK_EMBEDDING_SERVICE,
            new VectorSearchCommand.Metrics(mockMetricsFactory()),
            mocks.cursorManager,
            CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT);

    command.run();

    var contextCaptor = org.mockito.ArgumentCaptor.forClass(QueryExecutionContext.class);
    Mockito.verify(mocks.reader, times(1)).query(any(), contextCaptor.capture());
    assertThat(contextCaptor.getValue().deadlineTimestampMs()).isEqualTo(Optional.of(deadline));
  }

  @Test
  public void testDeadlineExceededIsReturnedAsErrorToUser() throws Exception {
    var mocks = new Mocks();

    when(mocks.reader.query(any(), any())).thenThrow(new DeadlineExceededException());

    VectorSearchCommand command =
        new VectorSearchCommand(
            VectorSearchCommandDefinitionBuilder.builder()
                .db(DATABASE_NAME)
                .collectionName(COLLECTION_NAME)
                .collectionUuid(COLLECTION_UUID)
                .vectorSearchQuery(
                    VectorQueryBuilder.builder()
                        .index(INDEX_NAME)
                        .criteria(
                            ApproximateVectorQueryCriteriaBuilder.builder()
                                .limit(LIMIT)
                                .numCandidates(NUM_CANDIDATES)
                                .queryVector(QUERY_VECTOR)
                                .path(PATH)
                                .build())
                        .build())
                .deadlineTimestampMs(System.currentTimeMillis() - 1000)
                .build(),
            mocks.catalog,
            mocks.initializedIndexCatalog,
            bootstrapperMetadata(),
            MOCK_EMBEDDING_SERVICE,
            new VectorSearchCommand.Metrics(mockMetricsFactory()),
            mocks.cursorManager,
            CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT);

    var result = command.run();

    Assert.assertEquals(
        new BsonDocument()
            .append("ok", new BsonInt32(0))
            .append("code", new BsonInt32(50))
            .append("codeName", new BsonString("MaxTimeMSExpired"))
            .append("errmsg", new BsonString("operation exceeded time limit")),
        result);
  }

  @Test
  public void testDeadlineIgnoredWhenFeatureFlagDisabled() throws Exception {
    var mocks = new Mocks();
    var query =
        VectorQueryBuilder.builder()
            .index(INDEX_NAME)
            .criteria(
                ApproximateVectorQueryCriteriaBuilder.builder()
                    .limit(LIMIT)
                    .numCandidates(NUM_CANDIDATES)
                    .queryVector(QUERY_VECTOR)
                    .path(PATH)
                    .build())
            .build();
    var command =
        new VectorSearchCommand(
            VectorSearchCommandDefinitionBuilder.builder()
                .db(DATABASE_NAME)
                .collectionName(COLLECTION_NAME)
                .collectionUuid(COLLECTION_UUID)
                .vectorSearchQuery(query)
                .deadlineTimestampMs(System.currentTimeMillis() - 1000)
                .build(),
            mocks.catalog,
            mocks.initializedIndexCatalog,
            bootstrapperMetadata(DynamicFeatureFlagRegistry.empty()),
            MOCK_EMBEDDING_SERVICE,
            new VectorSearchCommand.Metrics(mockMetricsFactory()),
            mocks.cursorManager,
            CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT);

    command.run();

    // With the feature flag disabled the deadline is dropped, so the context carries no deadline
    // and downstream timeout checks become no-ops.
    var contextCaptor = org.mockito.ArgumentCaptor.forClass(QueryExecutionContext.class);
    Mockito.verify(mocks.reader, times(1)).query(any(), contextCaptor.capture());
    assertThat(contextCaptor.getValue().deadlineTimestampMs()).isEqualTo(Optional.empty());
  }

  @Test
  public void testVectorSearchWithEnvoyMetadataReturnsEmptyBatch() throws Exception {
    var mocks = new Mocks();

    VectorSearchCommand command =
        new VectorSearchCommand(
            VectorSearchCommandDefinitionBuilder.builder()
                .db(DATABASE_NAME)
                .collectionName(COLLECTION_NAME)
                .collectionUuid(COLLECTION_UUID)
                .vectorSearchQuery(
                    VectorQueryBuilder.builder()
                        .index(INDEX_NAME)
                        .criteria(
                            ApproximateVectorQueryCriteriaBuilder.builder()
                                .limit(LIMIT)
                                .numCandidates(NUM_CANDIDATES)
                                .queryVector(QUERY_VECTOR)
                                .path(PATH)
                                .filter(getFilter())
                                .build())
                        .build())
                .build(),
            mocks.catalog,
            mocks.initializedIndexCatalog,
            bootstrapperMetadata(),
            MOCK_EMBEDDING_SERVICE,
            new VectorSearchCommand.Metrics(mockMetricsFactory()),
            mocks.cursorManager,
            CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT);

    command.handleSearchEnvoyMetadata(
        SearchEnvoyMetadata.newBuilder().setRoutedFromAnotherShard(true).build());

    var result = command.run();
    mocks.verifyEmptyBatchReturned(result, command);
  }

  @Test
  public void testInvalidQuery() throws Exception {
    var mocks = new Mocks();
    VectorSearchCommand command =
        new VectorSearchCommand(
            VectorSearchCommandDefinitionBuilder.builder()
                .db(DATABASE_NAME)
                .collectionName(COLLECTION_NAME)
                .collectionUuid(COLLECTION_UUID)
                .vectorSearchQuery(
                    new BsonParseException(
                        "limit should be less than or equal to numCandidates", Optional.of(PATH)))
                .build(),
            new DefaultIndexCatalog(),
            new InitializedIndexCatalog(),
            bootstrapperMetadata(),
            MOCK_EMBEDDING_SERVICE,
            new VectorSearchCommand.Metrics(mockMetricsFactory()),
            mocks.cursorManager,
            CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT);

    var result = command.run();
    Assert.assertEquals(
        new BsonDocument()
            .append("ok", new BsonInt32(0))
            .append(
                "errmsg",
                new BsonString(
                    "\"" + PATH + "\" " + "limit should be less than or equal to numCandidates")),
        result);
  }

  @Test
  public void maybeLoadShed_regularVectorQuery_returnsTrue() throws Exception {
    var mocks = new Mocks();
    var command =
        new VectorSearchCommand(
            VectorSearchCommandDefinitionBuilder.builder()
                .db(DATABASE_NAME)
                .collectionName(COLLECTION_NAME)
                .collectionUuid(COLLECTION_UUID)
                .vectorSearchQuery(
                    VectorQueryBuilder.builder()
                        .index(INDEX_NAME)
                        .criteria(
                            ApproximateVectorQueryCriteriaBuilder.builder()
                                .limit(LIMIT)
                                .numCandidates(NUM_CANDIDATES)
                                .queryVector(QUERY_VECTOR)
                                .path(PATH)
                                .build())
                        .build())
                .build(),
            mocks.catalog,
            mocks.initializedIndexCatalog,
            bootstrapperMetadata(),
            MOCK_EMBEDDING_SERVICE,
            new VectorSearchCommand.Metrics(mockMetricsFactory()),
            mocks.cursorManager,
            CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT);

    Assert.assertTrue(command.maybeLoadShed());
  }

  @Test
  public void maybeLoadShed_autoEmbeddingQuery_returnsFalse() throws Exception {
    var mocks = new Mocks();
    var command =
        new VectorSearchCommand(
            VectorSearchCommandDefinitionBuilder.builder()
                .db(DATABASE_NAME)
                .collectionName(COLLECTION_NAME)
                .collectionUuid(COLLECTION_UUID)
                .vectorSearchQuery(
                    VectorQueryBuilder.builder()
                        .index(INDEX_NAME)
                        .criteria(
                            ApproximateVectorQueryCriteriaBuilder.builder()
                                .limit(LIMIT)
                                .numCandidates(NUM_CANDIDATES)
                                .query(new VectorSearchQueryInput.Text("test query"))
                                .path(PATH)
                                .build())
                        .build())
                .build(),
            mocks.catalog,
            mocks.initializedIndexCatalog,
            bootstrapperMetadata(),
            MOCK_EMBEDDING_SERVICE,
            new VectorSearchCommand.Metrics(mockMetricsFactory()),
            mocks.cursorManager,
            CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT);

    Assert.assertFalse(command.maybeLoadShed());
  }

  @Test
  public void maybeLoadShed_invalidQuery_returnsTrue() throws Exception {
    var mocks = new Mocks();
    var command =
        new VectorSearchCommand(
            VectorSearchCommandDefinitionBuilder.builder()
                .db(DATABASE_NAME)
                .collectionName(COLLECTION_NAME)
                .collectionUuid(COLLECTION_UUID)
                .vectorSearchQuery(new BsonParseException("invalid query", Optional.of(PATH)))
                .build(),
            new DefaultIndexCatalog(),
            new InitializedIndexCatalog(),
            bootstrapperMetadata(),
            MOCK_EMBEDDING_SERVICE,
            new VectorSearchCommand.Metrics(mockMetricsFactory()),
            mocks.cursorManager,
            CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT);

    Assert.assertTrue(command.maybeLoadShed());
  }

  @Test
  public void testCollectionUuidThatDoesNotExistReturnsEmptyBatch() throws Exception {
    var mocks = new Mocks();
    var command =
        new VectorSearchCommand(
            VectorSearchCommandDefinitionBuilder.builder()
                .db(DATABASE_NAME)
                .collectionName(COLLECTION_NAME)
                .collectionUuid(UUID.randomUUID())
                .vectorSearchQuery(
                    VectorQueryBuilder.builder()
                        .index(INDEX_NAME)
                        .criteria(
                            ApproximateVectorQueryCriteriaBuilder.builder()
                                .limit(LIMIT)
                                .numCandidates(NUM_CANDIDATES)
                                .queryVector(QUERY_VECTOR)
                                .path(PATH)
                                .filter(getFilter())
                                .build())
                        .build())
                .build(),
            mocks.catalog,
            mocks.initializedIndexCatalog,
            bootstrapperMetadata(),
            MOCK_EMBEDDING_SERVICE,
            new VectorSearchCommand.Metrics(mockMetricsFactory()),
            mocks.cursorManager,
            CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT);

    var result = command.run();
    mocks.verifyEmptyBatchReturned(result, command);
  }

  @Test
  public void testIndexNameThatDoesNotExistReturnsEmptyBatch() throws Exception {
    var mocks = new Mocks();
    var command =
        new VectorSearchCommand(
            VectorSearchCommandDefinitionBuilder.builder()
                .db(DATABASE_NAME)
                .collectionName(COLLECTION_NAME)
                .collectionUuid(UUID.randomUUID())
                .vectorSearchQuery(
                    VectorQueryBuilder.builder()
                        .index("nonexistent")
                        .criteria(
                            ApproximateVectorQueryCriteriaBuilder.builder()
                                .limit(LIMIT)
                                .numCandidates(NUM_CANDIDATES)
                                .queryVector(QUERY_VECTOR)
                                .path(PATH)
                                .filter(getFilter())
                                .build())
                        .build())
                .build(),
            mocks.catalog,
            mocks.initializedIndexCatalog,
            bootstrapperMetadata(),
            MOCK_EMBEDDING_SERVICE,
            new VectorSearchCommand.Metrics(mockMetricsFactory()),
            mocks.cursorManager,
            CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT);

    var result = command.run();
    mocks.verifyEmptyBatchReturned(result, command);
  }

  @Test
  public void testIndexNotInitializedReturnsErrorResponse() throws Exception {
    var catalog = Mockito.mock(IndexCatalog.class);
    var initializedIndexCatalog = Mockito.mock(InitializedIndexCatalog.class);

    var vectorIndexDefinition =
        VectorIndexDefinitionBuilder.builder()
            .name(INDEX_NAME)
            .withDotProductVectorField(PATH.toString(), 1024)
            .build();
    var definitionGeneration = mockDefinitionGeneration(vectorIndexDefinition);
    com.xgen.mongot.index.VectorIndex mockVectorIndex = VectorIndex.mockIndex(definitionGeneration);
    // Set status to STEADY so throwIfUnavailableForQuerying() won't throw
    mockVectorIndex.setStatus(IndexStatus.steady());

    IndexGeneration indexGeneration = new IndexGeneration(mockVectorIndex, definitionGeneration);

    when(catalog.getIndex(DATABASE_NAME, COLLECTION_UUID, Optional.empty(), INDEX_NAME))
        .thenReturn(Optional.of(indexGeneration));
    when(initializedIndexCatalog.getIndex(indexGeneration.getGenerationId()))
        .thenReturn(Optional.empty());

    var mocks = new Mocks();
    var command =
        new VectorSearchCommand(
            VectorSearchCommandDefinitionBuilder.builder()
                .db(DATABASE_NAME)
                .collectionName(COLLECTION_NAME)
                .collectionUuid(COLLECTION_UUID)
                .vectorSearchQuery(
                    VectorQueryBuilder.builder()
                        .index(INDEX_NAME)
                        .criteria(
                            ApproximateVectorQueryCriteriaBuilder.builder()
                                .limit(LIMIT)
                                .numCandidates(NUM_CANDIDATES)
                                .queryVector(QUERY_VECTOR)
                                .path(PATH)
                                .filter(getFilter())
                                .build())
                        .build())
                .build(),
            catalog,
            initializedIndexCatalog,
            bootstrapperMetadata(),
            MOCK_EMBEDDING_SERVICE,
            new VectorSearchCommand.Metrics(mockMetricsFactory()),
            mocks.cursorManager,
            CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT);

    var result = command.run();
    Assert.assertEquals(0, result.getInt32("ok").getValue());
    Assert.assertTrue(result.containsKey("errmsg"));
    Assert.assertEquals(
        String.format("Index %s not initialized", INDEX_NAME),
        result.getString("errmsg").getValue());
    // Verify that throwIfUnavailableForQuerying() was called before the initialization check
    verify(mockVectorIndex).throwIfUnavailableForQuerying();
  }

  @Test
  public void testIoExceptionVectorSearchCommand() throws Exception {
    var mocks = new Mocks();

    when(mocks.reader.query(any(), any()))
        .thenAnswer(
            invocation -> {
              throw new IOException("IO error");
            });

    VectorSearchCommand command =
        new VectorSearchCommand(
            VectorSearchCommandDefinitionBuilder.builder()
                .db(DATABASE_NAME)
                .collectionName(COLLECTION_NAME)
                .collectionUuid(COLLECTION_UUID)
                .vectorSearchQuery(
                    VectorQueryBuilder.builder()
                        .index(INDEX_NAME)
                        .criteria(
                            ApproximateVectorQueryCriteriaBuilder.builder()
                                .limit(LIMIT)
                                .numCandidates(NUM_CANDIDATES)
                                .queryVector(QUERY_VECTOR)
                                .path(PATH)
                                .filter(getFilter())
                                .build())
                        .build())
                .build(),
            mocks.catalog,
            mocks.initializedIndexCatalog,
            bootstrapperMetadata(),
            MOCK_EMBEDDING_SERVICE,
            new VectorSearchCommand.Metrics(mockMetricsFactory()),
            mocks.cursorManager,
            CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT);

    var result = command.run();
    Assert.assertEquals(
        new BsonDocument()
            .append("ok", new BsonInt32(0))
            .append("errmsg", new BsonString("IO error")),
        result);
  }

  @Test
  public void testVectorSearchCommandExplain() throws Exception {
    Assume.assumeTrue(
        this.mongoDbVersion.compareTo(Explain.FIRST_VERSION_KILLS_CURSORS_EXPLAIN) >= 0);
    var mocks = new Mocks();
    var explainInformation =
        SearchExplainInformationBuilder.newBuilder()
            .queryExplainInfos(
                List.of(
                    QueryExplainInformationBuilder.builder()
                        .type(LuceneQuerySpecification.Type.DEFAULT_QUERY)
                        .args(DefaultQueryBuilder.builder().queryType("DocAndScoreQuery").build())
                        .build()))
            .build();

    VectorSearchCommand command =
        new VectorSearchCommand(
            VectorSearchCommandDefinitionBuilder.builder()
                .db(DATABASE_NAME)
                .collectionName(COLLECTION_NAME)
                .collectionUuid(COLLECTION_UUID)
                .vectorSearchQuery(
                    VectorQueryBuilder.builder()
                        .index(INDEX_NAME)
                        .criteria(
                            ApproximateVectorQueryCriteriaBuilder.builder()
                                .limit(LIMIT)
                                .numCandidates(NUM_CANDIDATES)
                                .queryVector(QUERY_VECTOR)
                                .path(PATH)
                                .filter(getFilter())
                                .build())
                        .build())
                .explain(
                    ExplainDefinitionBuilder.builder()
                        .verbosity(Explain.Verbosity.ALL_PLANS_EXECUTION)
                        .build())
                .build(),
            mocks.catalog,
            mocks.initializedIndexCatalog,
            bootstrapperMetadata(),
            MOCK_EMBEDDING_SERVICE,
            new VectorSearchCommand.Metrics(mockMetricsFactory()),
            mocks.cursorManager,
            CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT);

    try (var ignored =
        FakeExplain.setup(
            Explain.Verbosity.EXECUTION_STATS,
            NUM_PARTITIONS.getDefaultValue(),
            explainInformation)) {
      BsonDocument result = command.run();
      MongotCursorBatch batch = MongotCursorBatch.fromBson(result);
      Assert.assertTrue(batch.explain().isPresent());
      Assert.assertTrue(batch.cursor().isPresent());
      Assert.assertEquals(batch.explain().get(), explainInformation);
    }
    Assert.assertTrue(command.getCreatedCursorIds().isEmpty());
  }

  @Test
  public void testExplainQueryPlannerNoCursor() throws Exception {
    var mocks = new Mocks();
    var explainInformation =
        SearchExplainInformationBuilder.newBuilder()
            .queryExplainInfos(
                List.of(
                    QueryExplainInformationBuilder.builder()
                        .type(LuceneQuerySpecification.Type.DEFAULT_QUERY)
                        .args(DefaultQueryBuilder.builder().queryType("DocAndScoreQuery").build())
                        .build()))
            .build();

    VectorSearchCommand command =
        new VectorSearchCommand(
            VectorSearchCommandDefinitionBuilder.builder()
                .db(DATABASE_NAME)
                .collectionName(COLLECTION_NAME)
                .collectionUuid(COLLECTION_UUID)
                .vectorSearchQuery(
                    VectorQueryBuilder.builder()
                        .index(INDEX_NAME)
                        .criteria(
                            ApproximateVectorQueryCriteriaBuilder.builder()
                                .limit(LIMIT)
                                .numCandidates(NUM_CANDIDATES)
                                .queryVector(QUERY_VECTOR)
                                .path(PATH)
                                .filter(getFilter())
                                .build())
                        .build())
                .explain(
                    ExplainDefinitionBuilder.builder()
                        .verbosity(Explain.Verbosity.ALL_PLANS_EXECUTION)
                        .build())
                .build(),
            mocks.catalog,
            mocks.initializedIndexCatalog,
            bootstrapperMetadata(),
            MOCK_EMBEDDING_SERVICE,
            new VectorSearchCommand.Metrics(mockMetricsFactory()),
            mocks.cursorManager,
            CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT);

    try (var ignored =
        FakeExplain.setup(
            Explain.Verbosity.QUERY_PLANNER,
            NUM_PARTITIONS.getDefaultValue(),
            explainInformation)) {
      BsonDocument result = command.run();
      MongotCursorBatch batch = MongotCursorBatch.fromBson(result);
      Assert.assertTrue(batch.explain().isPresent());
      Assert.assertTrue(batch.cursor().isEmpty());
      Assert.assertEquals(batch.explain().get(), explainInformation);
    }
    Assert.assertTrue(command.getCreatedCursorIds().isEmpty());
  }

  @Test
  public void testExplainRecordsMetadata() throws Exception {
    var mocks = new Mocks();

    VectorSearchCommand command =
        new VectorSearchCommand(
            VectorSearchCommandDefinitionBuilder.builder()
                .db(DATABASE_NAME)
                .collectionName(COLLECTION_NAME)
                .collectionUuid(COLLECTION_UUID)
                .vectorSearchQuery(
                    VectorQueryBuilder.builder()
                        .index(INDEX_NAME)
                        .criteria(
                            ApproximateVectorQueryCriteriaBuilder.builder()
                                .limit(LIMIT)
                                .numCandidates(NUM_CANDIDATES)
                                .queryVector(QUERY_VECTOR)
                                .path(PATH)
                                .filter(getFilter())
                                .build())
                        .build())
                .explain(
                    ExplainDefinitionBuilder.builder()
                        .verbosity(Explain.Verbosity.ALL_PLANS_EXECUTION)
                        .build())
                .build(),
            mocks.catalog,
            mocks.initializedIndexCatalog,
            bootstrapperMetadata(),
            MOCK_EMBEDDING_SERVICE,
            new VectorSearchCommand.Metrics(mockMetricsFactory()),
            mocks.cursorManager,
            CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT);

    try (var unused =
        Explain.setup(
            Optional.of(Explain.Verbosity.QUERY_PLANNER),
            Optional.of(NUM_PARTITIONS.getDefaultValue()))) {
      command.run();

      var metadata = Explain.collect().get().metadata().get();
      assertThat(metadata)
          .isEqualTo(
              new MetadataExplainInformation(
                  Optional.of("testVersion"),
                  Optional.of("localhost"),
                  Optional.of(INDEX_NAME),
                  Optional.empty(),
                  Optional.empty(),
                  Optional.empty()));
    }
    Assert.assertTrue(command.getCreatedCursorIds().isEmpty());
  }

  private SearchExplainInformation makeLargeExplanation(int byteSize) {
    return new SearchExplainInformation(
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.of(
            new MetadataExplainInformation(
                Optional.empty(),
                Optional.of("x".repeat(byteSize)),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty())),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty());
  }

  @Test
  public void testVectorSearchCommandExplainOversized() throws Exception {
    var mocks = new Mocks();
    SearchExplainInformation explainInformation = makeLargeExplanation(17_000_000);

    VectorSearchCommand command =
        new VectorSearchCommand(
            VectorSearchCommandDefinitionBuilder.builder()
                .db(DATABASE_NAME)
                .collectionName(COLLECTION_NAME)
                .collectionUuid(COLLECTION_UUID)
                .vectorSearchQuery(
                    VectorQueryBuilder.builder()
                        .index(INDEX_NAME)
                        .criteria(
                            ApproximateVectorQueryCriteriaBuilder.builder()
                                .limit(LIMIT)
                                .numCandidates(NUM_CANDIDATES)
                                .queryVector(QUERY_VECTOR)
                                .path(PATH)
                                .filter(getFilter())
                                .build())
                        .build())
                .explain(
                    ExplainDefinitionBuilder.builder()
                        .verbosity(Explain.Verbosity.ALL_PLANS_EXECUTION)
                        .build())
                .build(),
            mocks.catalog,
            mocks.initializedIndexCatalog,
            bootstrapperMetadata(),
            MOCK_EMBEDDING_SERVICE,
            new VectorSearchCommand.Metrics(mockMetricsFactory()),
            mocks.cursorManager,
            CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT);

    try (var ignored =
        FakeExplain.setup(
            Explain.Verbosity.EXECUTION_STATS,
            NUM_PARTITIONS.getDefaultValue(),
            explainInformation)) {
      BsonDocument result = command.run();
      Assert.assertEquals(0, result.getInt32("ok").getValue());
      Assert.assertTrue(result.containsKey("errmsg"));
    }
  }

  @Test
  public void testVectorSearchCommandExplainPlusResultsOversized() throws Exception {
    Assume.assumeTrue(
        this.mongoDbVersion.compareTo(Explain.FIRST_VERSION_KILLS_CURSORS_EXPLAIN) >= 0);
    var mocks = new Mocks();
    SearchExplainInformation explainInformation = makeLargeExplanation(12_000_000);

    // This loop creates a large BsonArray < 16 Mib
    BsonArray largeArray = new BsonArray();
    for (int i = 0; i < 1_000_000; i++) {
      largeArray.add(new BsonInt32(i));
    }
    when(mocks.reader.query(any(), any())).thenReturn(largeArray);

    VectorSearchCommand command =
        new VectorSearchCommand(
            VectorSearchCommandDefinitionBuilder.builder()
                .db(DATABASE_NAME)
                .collectionName(COLLECTION_NAME)
                .collectionUuid(COLLECTION_UUID)
                .vectorSearchQuery(
                    VectorQueryBuilder.builder()
                        .index(INDEX_NAME)
                        .criteria(
                            ApproximateVectorQueryCriteriaBuilder.builder()
                                .limit(LIMIT)
                                .numCandidates(NUM_CANDIDATES)
                                .queryVector(QUERY_VECTOR)
                                .path(PATH)
                                .filter(getFilter())
                                .build())
                        .build())
                .explain(
                    ExplainDefinitionBuilder.builder()
                        .verbosity(Explain.Verbosity.ALL_PLANS_EXECUTION)
                        .build())
                .build(),
            mocks.catalog,
            mocks.initializedIndexCatalog,
            bootstrapperMetadata(),
            MOCK_EMBEDDING_SERVICE,
            new VectorSearchCommand.Metrics(mockMetricsFactory()),
            mocks.cursorManager,
            CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT);

    try (var ignored =
        FakeExplain.setup(
            Explain.Verbosity.EXECUTION_STATS,
            NUM_PARTITIONS.getDefaultValue(),
            explainInformation)) {
      BsonDocument result = command.run();
      Assert.assertEquals(0, result.getInt32("ok").getValue());
      Assert.assertTrue(result.containsKey("errmsg"));
      Assert.assertTrue(
          result
              .get("errmsg")
              .asString()
              .toString()
              .contains("Explain is too large in vector search query response"));
    }
  }

  @Test
  public void testExplainWithSearchEnvoyMetadataReturnsEmptyExplainInfo() throws Exception {
    var mocks = new Mocks();

    VectorSearchCommand command =
        new VectorSearchCommand(
            VectorSearchCommandDefinitionBuilder.builder()
                .db(DATABASE_NAME)
                .collectionName(COLLECTION_NAME)
                .collectionUuid(COLLECTION_UUID)
                .vectorSearchQuery(
                    VectorQueryBuilder.builder()
                        .index(INDEX_NAME)
                        .criteria(
                            ApproximateVectorQueryCriteriaBuilder.builder()
                                .limit(LIMIT)
                                .numCandidates(NUM_CANDIDATES)
                                .queryVector(QUERY_VECTOR)
                                .path(PATH)
                                .filter(getFilter())
                                .build())
                        .build())
                .explain(
                    ExplainDefinitionBuilder.builder()
                        .verbosity(Explain.Verbosity.ALL_PLANS_EXECUTION)
                        .build())
                .build(),
            mocks.catalog,
            mocks.initializedIndexCatalog,
            bootstrapperMetadata(),
            MOCK_EMBEDDING_SERVICE,
            new VectorSearchCommand.Metrics(mockMetricsFactory()),
            mocks.cursorManager,
            CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT);

    command.handleSearchEnvoyMetadata(
        SearchEnvoyMetadata.newBuilder().setRoutedFromAnotherShard(true).build());

    var result = command.run();
    Assert.assertEquals(new EmptyExplainInformation().toBson(), result);
    Assert.assertTrue(command.getCreatedCursorIds().isEmpty());
    Assert.assertEquals(
        0, mocks.metricsUpdater.getQueryingMetricsUpdater().getVectorCommandCounter().count(), 0);
  }

  @Test
  public void testCommandRetriesUponReaderClosedException() throws Exception {
    var mocks = new Mocks();

    when(mocks.reader.query(any(), any()))
        .thenThrow(ReaderClosedException.class)
        .thenReturn(mocks.bsonResults);

    var command =
        new VectorSearchCommand(
            VectorSearchCommandDefinitionBuilder.builder()
                .db(DATABASE_NAME)
                .collectionName(COLLECTION_NAME)
                .collectionUuid(COLLECTION_UUID)
                .vectorSearchQuery(
                    VectorQueryBuilder.builder()
                        .index(INDEX_NAME)
                        .criteria(
                            ApproximateVectorQueryCriteriaBuilder.builder()
                                .limit(LIMIT)
                                .numCandidates(NUM_CANDIDATES)
                                .queryVector(QUERY_VECTOR)
                                .path(PATH)
                                .filter(getFilter())
                                .build())
                        .build())
                .build(),
            mocks.catalog,
            mocks.initializedIndexCatalog,
            bootstrapperMetadata(),
            MOCK_EMBEDDING_SERVICE,
            new VectorSearchCommand.Metrics(mockMetricsFactory()),
            mocks.cursorManager,
            CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT);

    var result = command.run();
    var expected =
        mockInitialMongotCursorBatchForVectorSearch(0, new VectorSearchResultBatch(4), NAMESPACE)
            .toBson();
    Assert.assertEquals(expected, result);
    Assert.assertTrue(command.getCreatedCursorIds().isEmpty());
    Assert.assertEquals(
        1, mocks.metricsUpdater.getQueryingMetricsUpdater().getVectorCommandCounter().count(), 0);
  }

  @Test
  public void testStoredSourceCursorPathReturnsErrorWhenBatchCannotFit() throws Exception {
    Assume.assumeTrue(this.mongoDbVersion.compareTo(MIN_STORED_SOURCE_VERSION) >= 0);
    var mocks = new Mocks();
    long cursorId = 123L;
    when(mocks.cursorManager.newCursor(
            any(), any(), any(), any(), any(CursorQuery.class), any(), any(), any()))
        .thenReturn(new SearchCursorInfo(cursorId, MetaResults.EMPTY));
    when(mocks.cursorManager.getNextBatch(Mockito.eq(cursorId), any(), any()))
        .thenThrow(new IllegalStateException("Search result output exceeds BSON size limit"));

    var query =
        VectorQueryBuilder.builder()
            .index(INDEX_NAME)
            .criteria(
                ApproximateVectorQueryCriteriaBuilder.builder()
                    .limit(LIMIT)
                    .numCandidates(NUM_CANDIDATES)
                    .queryVector(QUERY_VECTOR)
                    .path(PATH)
                    .filter(getFilter())
                    .build())
            .returnStoredSource(true)
            .build();

    var command =
        new VectorSearchCommand(
            VectorSearchCommandDefinitionBuilder.builder()
                .db(DATABASE_NAME)
                .collectionName(COLLECTION_NAME)
                .collectionUuid(COLLECTION_UUID)
                .vectorSearchQuery(query)
                .build(),
            mocks.catalog,
            mocks.initializedIndexCatalog,
            bootstrapperMetadata(),
            MOCK_EMBEDDING_SERVICE,
            new VectorSearchCommand.Metrics(mockMetricsFactory()),
            mocks.cursorManager,
            CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT);

    var result = command.run();

    Assert.assertEquals(0, result.getInt32("ok").getValue());
    Assert.assertTrue(
        result
            .getString("errmsg")
            .getValue()
            .contains("Search result output exceeds BSON size limit"));
    assertThat(command.getCreatedCursorIds()).containsExactly(cursorId);
    verify(mocks.cursorManager, times(1)).getNextBatch(Mockito.eq(cursorId), any(), any());
    verify(mocks.reader, times(0)).query(any(), any());
  }

  @Test
  public void testStoredSourceCursorPathRetriesUponReaderClosedException() throws Exception {
    Assume.assumeTrue(this.mongoDbVersion.compareTo(MIN_STORED_SOURCE_VERSION) >= 0);
    var mocks = new Mocks();
    long firstCursorId = 501L;
    long secondCursorId = 502L;

    when(mocks.cursorManager.newCursor(
            any(), any(), any(), any(), any(CursorQuery.class), any(), any(), any()))
        .thenReturn(new SearchCursorInfo(firstCursorId, MetaResults.EMPTY))
        .thenReturn(new SearchCursorInfo(secondCursorId, MetaResults.EMPTY));
    when(mocks.cursorManager.getIndexQueryBatchTimerRecorder(firstCursorId))
        .thenReturn(sample -> {});
    when(mocks.cursorManager.getIndexQueryBatchTimerRecorder(secondCursorId))
        .thenReturn(sample -> {});
    Mockito.doAnswer(
            ignored -> {
              throw ReaderClosedException.create("getNextBatch");
            })
        .when(mocks.cursorManager)
        .getNextBatch(Mockito.eq(firstCursorId), any(), any());
    when(mocks.cursorManager.getNextBatch(Mockito.eq(secondCursorId), any(), any()))
        .thenReturn(new MongotCursorResultInfo(true, new BsonArray(), NAMESPACE));

    var query =
        VectorQueryBuilder.builder()
            .index(INDEX_NAME)
            .criteria(
                ApproximateVectorQueryCriteriaBuilder.builder()
                    .limit(LIMIT)
                    .numCandidates(NUM_CANDIDATES)
                    .queryVector(QUERY_VECTOR)
                    .path(PATH)
                    .filter(getFilter())
                    .build())
            .returnStoredSource(true)
            .build();

    var command =
        new VectorSearchCommand(
            VectorSearchCommandDefinitionBuilder.builder()
                .db(DATABASE_NAME)
                .collectionName(COLLECTION_NAME)
                .collectionUuid(COLLECTION_UUID)
                .vectorSearchQuery(query)
                .build(),
            mocks.catalog,
            mocks.initializedIndexCatalog,
            bootstrapperMetadata(),
            MOCK_EMBEDDING_SERVICE,
            new VectorSearchCommand.Metrics(mockMetricsFactory()),
            mocks.cursorManager,
            CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT);

    var result = command.run();

    Assert.assertEquals(1, result.getInt32("ok").getValue());
    assertThat(command.getCreatedCursorIds()).containsExactly(firstCursorId, secondCursorId);
    verify(mocks.cursorManager, times(2))
        .newCursor(any(), any(), any(), any(), any(CursorQuery.class), any(), any(), any());
    verify(mocks.cursorManager, times(1)).getNextBatch(Mockito.eq(firstCursorId), any(), any());
    verify(mocks.cursorManager, times(1)).getNextBatch(Mockito.eq(secondCursorId), any(), any());
    verify(mocks.reader, times(0)).query(any(), any());
  }

  @Test
  public void testStoredSourceCursorPathRetryCleansUpFailedAttemptCursor() throws Exception {
    Assume.assumeTrue(this.mongoDbVersion.compareTo(MIN_STORED_SOURCE_VERSION) >= 0);
    var mocks = new Mocks();
    long firstCursorId = 601L;
    long secondCursorId = 602L;

    when(mocks.cursorManager.newCursor(
            any(), any(), any(), any(), any(CursorQuery.class), any(), any(), any()))
        .thenReturn(new SearchCursorInfo(firstCursorId, MetaResults.EMPTY))
        .thenReturn(new SearchCursorInfo(secondCursorId, MetaResults.EMPTY));
    when(mocks.cursorManager.getIndexQueryBatchTimerRecorder(firstCursorId))
        .thenReturn(sample -> {});
    when(mocks.cursorManager.getIndexQueryBatchTimerRecorder(secondCursorId))
        .thenReturn(sample -> {});
    Mockito.doAnswer(
            ignored -> {
              throw ReaderClosedException.create("getNextBatch");
            })
        .when(mocks.cursorManager)
        .getNextBatch(Mockito.eq(firstCursorId), any(), any());
    when(mocks.cursorManager.getNextBatch(Mockito.eq(secondCursorId), any(), any()))
        .thenReturn(new MongotCursorResultInfo(true, new BsonArray(), NAMESPACE));

    var query =
        VectorQueryBuilder.builder()
            .index(INDEX_NAME)
            .criteria(
                ApproximateVectorQueryCriteriaBuilder.builder()
                    .limit(LIMIT)
                    .numCandidates(NUM_CANDIDATES)
                    .queryVector(QUERY_VECTOR)
                    .path(PATH)
                    .filter(getFilter())
                    .build())
            .returnStoredSource(true)
            .build();

    var command =
        new VectorSearchCommand(
            VectorSearchCommandDefinitionBuilder.builder()
                .db(DATABASE_NAME)
                .collectionName(COLLECTION_NAME)
                .collectionUuid(COLLECTION_UUID)
                .vectorSearchQuery(query)
                .build(),
            mocks.catalog,
            mocks.initializedIndexCatalog,
            bootstrapperMetadata(),
            MOCK_EMBEDDING_SERVICE,
            new VectorSearchCommand.Metrics(mockMetricsFactory()),
            mocks.cursorManager,
            CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT);

    var result = command.run();

    Assert.assertEquals(1, result.getInt32("ok").getValue());
    verify(mocks.cursorManager, times(1)).killCursor(firstCursorId);
    verify(mocks.cursorManager, times(0)).killCursor(secondCursorId);
  }

  @Test
  public void testStoredSourceExplainQueryPlannerReturnsNoCursor() throws Exception {
    Assume.assumeTrue(this.mongoDbVersion.compareTo(MIN_STORED_SOURCE_VERSION) >= 0);
    var mocks = new Mocks();
    long cursorId = 701L;
    var explainInformation =
        SearchExplainInformationBuilder.newBuilder()
            .queryExplainInfos(
                List.of(
                    QueryExplainInformationBuilder.builder()
                        .type(LuceneQuerySpecification.Type.DEFAULT_QUERY)
                        .args(DefaultQueryBuilder.builder().queryType("DocAndScoreQuery").build())
                        .build()))
            .build();

    when(mocks.cursorManager.newCursor(
            any(), any(), any(), any(), any(CursorQuery.class), any(), any(), any()))
        .thenReturn(new SearchCursorInfo(cursorId, MetaResults.EMPTY));
    when(mocks.cursorManager.getIndexQueryBatchTimerRecorder(cursorId)).thenReturn(sample -> {});
    when(mocks.cursorManager.getNextBatch(Mockito.eq(cursorId), any(), any()))
        .thenReturn(
            new MongotCursorResultInfo(
                true, new BsonArray(), Optional.of(explainInformation), NAMESPACE));

    var query =
        VectorQueryBuilder.builder()
            .index(INDEX_NAME)
            .criteria(
                ApproximateVectorQueryCriteriaBuilder.builder()
                    .limit(LIMIT)
                    .numCandidates(NUM_CANDIDATES)
                    .queryVector(QUERY_VECTOR)
                    .path(PATH)
                    .filter(getFilter())
                    .build())
            .returnStoredSource(true)
            .build();

    var command =
        new VectorSearchCommand(
            VectorSearchCommandDefinitionBuilder.builder()
                .db(DATABASE_NAME)
                .collectionName(COLLECTION_NAME)
                .collectionUuid(COLLECTION_UUID)
                .vectorSearchQuery(query)
                .explain(
                    ExplainDefinitionBuilder.builder()
                        .verbosity(Explain.Verbosity.QUERY_PLANNER)
                        .build())
                .build(),
            mocks.catalog,
            mocks.initializedIndexCatalog,
            bootstrapperMetadata(),
            MOCK_EMBEDDING_SERVICE,
            new VectorSearchCommand.Metrics(mockMetricsFactory()),
            mocks.cursorManager,
            CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT);

    try (var ignored =
        FakeExplain.setup(
            Explain.Verbosity.QUERY_PLANNER,
            NUM_PARTITIONS.getDefaultValue(),
            explainInformation)) {
      BsonDocument result = command.run();
      MongotCursorBatch batch = MongotCursorBatch.fromBson(result);
      Assert.assertTrue(batch.explain().isPresent());
      Assert.assertTrue(batch.cursor().isEmpty());
      Assert.assertEquals(batch.explain().get(), explainInformation);
    }
    verify(mocks.cursorManager, times(1)).killCursor(cursorId);
  }

  @Test
  public void testStoredSourceCursorPathRecordsVectorMetrics() throws Exception {
    Assume.assumeTrue(this.mongoDbVersion.compareTo(MIN_STORED_SOURCE_VERSION) >= 0);
    var mocks = new Mocks();
    long cursorId = 456L;
    when(mocks.cursorManager.newCursor(
            any(), any(), any(), any(), any(CursorQuery.class), any(), any(), any()))
        .thenReturn(new SearchCursorInfo(cursorId, MetaResults.EMPTY));
    when(mocks.cursorManager.getIndexQueryBatchTimerRecorder(cursorId)).thenReturn(sample -> {});
    when(mocks.cursorManager.getNextBatch(Mockito.eq(cursorId), any(), any()))
        .thenReturn(new MongotCursorResultInfo(true, new BsonArray(), NAMESPACE));

    var query =
        VectorQueryBuilder.builder()
            .index(INDEX_NAME)
            .criteria(
                ApproximateVectorQueryCriteriaBuilder.builder()
                    .limit(LIMIT)
                    .numCandidates(NUM_CANDIDATES)
                    .queryVector(QUERY_VECTOR)
                    .path(PATH)
                    .filter(getFilter())
                    .build())
            .returnStoredSource(true)
            .build();

    var command =
        new VectorSearchCommand(
            VectorSearchCommandDefinitionBuilder.builder()
                .db(DATABASE_NAME)
                .collectionName(COLLECTION_NAME)
                .collectionUuid(COLLECTION_UUID)
                .vectorSearchQuery(query)
                .build(),
            mocks.catalog,
            mocks.initializedIndexCatalog,
            bootstrapperMetadata(),
            MOCK_EMBEDDING_SERVICE,
            new VectorSearchCommand.Metrics(mockMetricsFactory()),
            mocks.cursorManager,
            CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT);

    var result = command.run();

    Assert.assertEquals(1, result.getInt32("ok").getValue());
    assertThat(command.getCreatedCursorIds()).containsExactly(cursorId);
    verify(mocks.cursorManager, times(1)).getNextBatch(Mockito.eq(cursorId), any(), any());
    verify(mocks.reader, times(0)).query(any(), any());
    Assert.assertEquals(
        1, mocks.metricsUpdater.getQueryingMetricsUpdater().getVectorCommandCounter().count(), 0);
    Assert.assertEquals(
        1, mocks.metricsUpdater.getQueryingMetricsUpdater().getVectorResultLatencyTimer().count());
  }

  @Test
  public void testVectorSearchCommandRejectsRequiresSequenceToken() throws Exception {
    var mocks = new Mocks();
    var query =
        VectorQueryBuilder.builder()
            .index(INDEX_NAME)
            .criteria(
                ApproximateVectorQueryCriteriaBuilder.builder()
                    .limit(LIMIT)
                    .numCandidates(NUM_CANDIDATES)
                    .queryVector(QUERY_VECTOR)
                    .path(PATH)
                    .filter(getFilter())
                    .build())
            .build();

    var command =
        new VectorSearchCommand(
            VectorSearchCommandDefinitionBuilder.builder()
                .db(DATABASE_NAME)
                .collectionName(COLLECTION_NAME)
                .collectionUuid(COLLECTION_UUID)
                .vectorSearchQuery(query)
                .cursorOptions(
                    CursorOptionsDefinitionBuilder.builder().requireSequenceTokens(true).build())
                .build(),
            mocks.catalog,
            mocks.initializedIndexCatalog,
            bootstrapperMetadata(),
            MOCK_EMBEDDING_SERVICE,
            new VectorSearchCommand.Metrics(mockMetricsFactory()),
            mocks.cursorManager,
            CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT);

    var result = command.run();

    Assert.assertEquals(0, result.getInt32("ok").getValue());
    Assert.assertTrue(
        result
            .getString("errmsg")
            .getValue()
            .contains("Pagination is not supported with the 'vectorSearch' command."));
  }

  @Test
  public void testVectorSearchCommandRejectsCursorOptionsWithoutStoredSource() throws Exception {
    var mocks = new Mocks();
    var query =
        VectorQueryBuilder.builder()
            .index(INDEX_NAME)
            .criteria(
                ApproximateVectorQueryCriteriaBuilder.builder()
                    .limit(LIMIT)
                    .numCandidates(NUM_CANDIDATES)
                    .queryVector(QUERY_VECTOR)
                    .path(PATH)
                    .filter(getFilter())
                    .build())
            .returnStoredSource(false)
            .build();

    var command =
        new VectorSearchCommand(
            VectorSearchCommandDefinitionBuilder.builder()
                .db(DATABASE_NAME)
                .collectionName(COLLECTION_NAME)
                .collectionUuid(COLLECTION_UUID)
                .vectorSearchQuery(query)
                .cursorOptions(CursorOptionsDefinitionBuilder.builder().batchSize(5).build())
                .build(),
            mocks.catalog,
            mocks.initializedIndexCatalog,
            bootstrapperMetadata(),
            MOCK_EMBEDDING_SERVICE,
            new VectorSearchCommand.Metrics(mockMetricsFactory()),
            mocks.cursorManager,
            CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT);

    var result = command.run();

    Assert.assertEquals(0, result.getInt32("ok").getValue());
    Assert.assertTrue(
        result
            .getString("errmsg")
            .getValue()
            .contains(
                "cursor options are only supported for "
                    + "vector search when returnStoredSource is true"));
  }

  @Test
  public void testBsonFloat32QueryCounter() throws Exception {
    var mocks = new Mocks();
    var query =
        VectorQueryBuilder.builder()
            .index(INDEX_NAME)
            .criteria(
                ApproximateVectorQueryCriteriaBuilder.builder()
                    .limit(LIMIT)
                    .numCandidates(NUM_CANDIDATES)
                    .queryVector(
                        Vector.fromFloats(new float[] {1f, 2f, 3f}, FloatVector.OriginalType.BSON))
                    .path(PATH)
                    .filter(getFilter())
                    .build())
            .build();
    var command =
        new VectorSearchCommand(
            VectorSearchCommandDefinitionBuilder.builder()
                .db(DATABASE_NAME)
                .collectionName(COLLECTION_NAME)
                .collectionUuid(COLLECTION_UUID)
                .vectorSearchQuery(query)
                .build(),
            mocks.catalog,
            mocks.initializedIndexCatalog,
            bootstrapperMetadata(),
            MOCK_EMBEDDING_SERVICE,
            new VectorSearchCommand.Metrics(mockMetricsFactory()),
            mocks.cursorManager,
            CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT);

    var result = command.run();

    var expected =
        mockInitialMongotCursorBatchForVectorSearch(0, new VectorSearchResultBatch(4), NAMESPACE)
            .toBson();
    Assert.assertEquals(expected, result);

    // Check counters
    Assert.assertEquals(1, command.getBsonFloatVectorQueries().count(), 0);
    Assert.assertEquals(0, command.getBsonByteVectorQueries().count(), 0);
    Assert.assertEquals(0, command.getBsonBitVectorQueries().count(), 0);

    command.run();
    // Check counters
    Assert.assertEquals(2, command.getBsonFloatVectorQueries().count(), 0);
    Assert.assertEquals(0, command.getBsonByteVectorQueries().count(), 0);
    Assert.assertEquals(0, command.getBsonBitVectorQueries().count(), 0);
  }

  @Test
  public void testBsonByteQueryCounter() throws Exception {
    var mocks = new Mocks();
    var query =
        VectorQueryBuilder.builder()
            .index(INDEX_NAME)
            .criteria(
                ApproximateVectorQueryCriteriaBuilder.builder()
                    .limit(LIMIT)
                    .numCandidates(NUM_CANDIDATES)
                    .queryVector(Vector.fromBytes(new byte[] {0x01, 0x02, 0x03}))
                    .path(PATH)
                    .filter(getFilter())
                    .build())
            .build();
    var command =
        new VectorSearchCommand(
            VectorSearchCommandDefinitionBuilder.builder()
                .db(DATABASE_NAME)
                .collectionName(COLLECTION_NAME)
                .collectionUuid(COLLECTION_UUID)
                .vectorSearchQuery(query)
                .build(),
            mocks.catalog,
            mocks.initializedIndexCatalog,
            bootstrapperMetadata(),
            MOCK_EMBEDDING_SERVICE,
            new VectorSearchCommand.Metrics(mockMetricsFactory()),
            mocks.cursorManager,
            CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT);

    var result = command.run();

    var expected =
        mockInitialMongotCursorBatchForVectorSearch(0, new VectorSearchResultBatch(4), NAMESPACE)
            .toBson();
    Assert.assertEquals(expected, result);

    // Check counters
    Assert.assertEquals(0, command.getBsonFloatVectorQueries().count(), 0);
    Assert.assertEquals(1, command.getBsonByteVectorQueries().count(), 0);
    Assert.assertEquals(0, command.getBsonBitVectorQueries().count(), 0);

    command.run();
    // Check counters
    Assert.assertEquals(0, command.getBsonFloatVectorQueries().count(), 0);
    Assert.assertEquals(2, command.getBsonByteVectorQueries().count(), 0);
    Assert.assertEquals(0, command.getBsonBitVectorQueries().count(), 0);
  }

  @Test
  public void testBsonBitQueryCounter() throws Exception {
    var mocks = new Mocks();
    var query =
        VectorQueryBuilder.builder()
            .index(INDEX_NAME)
            .criteria(
                ApproximateVectorQueryCriteriaBuilder.builder()
                    .limit(LIMIT)
                    .numCandidates(NUM_CANDIDATES)
                    .queryVector(Vector.fromBits(new byte[] {0x01, 0x02, 0x03}))
                    .path(PATH)
                    .filter(getFilter())
                    .build())
            .build();
    var command =
        new VectorSearchCommand(
            VectorSearchCommandDefinitionBuilder.builder()
                .db(DATABASE_NAME)
                .collectionName(COLLECTION_NAME)
                .collectionUuid(COLLECTION_UUID)
                .vectorSearchQuery(query)
                .build(),
            mocks.catalog,
            mocks.initializedIndexCatalog,
            bootstrapperMetadata(),
            MOCK_EMBEDDING_SERVICE,
            new VectorSearchCommand.Metrics(mockMetricsFactory()),
            mocks.cursorManager,
            CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT);

    var result = command.run();

    var expected =
        mockInitialMongotCursorBatchForVectorSearch(0, new VectorSearchResultBatch(4), NAMESPACE)
            .toBson();
    Assert.assertEquals(expected, result);

    // Check counters
    Assert.assertEquals(0, command.getBsonFloatVectorQueries().count(), 0);
    Assert.assertEquals(0, command.getBsonByteVectorQueries().count(), 0);
    Assert.assertEquals(1, command.getBsonBitVectorQueries().count(), 0);

    command.run();
    // Check counters
    Assert.assertEquals(0, command.getBsonFloatVectorQueries().count(), 0);
    Assert.assertEquals(0, command.getBsonByteVectorQueries().count(), 0);
    Assert.assertEquals(2, command.getBsonBitVectorQueries().count(), 0);
  }

  @Test
  public void testNativeFloat32QueryCounter() throws Exception {
    var mocks = new Mocks();
    var query =
        VectorQueryBuilder.builder()
            .index(INDEX_NAME)
            .criteria(
                ApproximateVectorQueryCriteriaBuilder.builder()
                    .limit(LIMIT)
                    .numCandidates(NUM_CANDIDATES)
                    .queryVector(Vector.fromFloats(new float[] {1f, 2f, 3f}, NATIVE))
                    .path(PATH)
                    .filter(getFilter())
                    .build())
            .build();
    var command =
        new VectorSearchCommand(
            VectorSearchCommandDefinitionBuilder.builder()
                .db(DATABASE_NAME)
                .collectionName(COLLECTION_NAME)
                .collectionUuid(COLLECTION_UUID)
                .vectorSearchQuery(query)
                .build(),
            mocks.catalog,
            mocks.initializedIndexCatalog,
            bootstrapperMetadata(),
            MOCK_EMBEDDING_SERVICE,
            new VectorSearchCommand.Metrics(mockMetricsFactory()),
            mocks.cursorManager,
            CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT);

    var result = command.run();

    var expected =
        mockInitialMongotCursorBatchForVectorSearch(0, new VectorSearchResultBatch(4), NAMESPACE)
            .toBson();
    Assert.assertEquals(expected, result);

    // Check counters
    Assert.assertEquals(0, command.getBsonFloatVectorQueries().count(), 0);
    Assert.assertEquals(0, command.getBsonByteVectorQueries().count(), 0);
    Assert.assertEquals(0, command.getBsonBitVectorQueries().count(), 0);

    command.run();
    // Check counters
    Assert.assertEquals(0, command.getBsonFloatVectorQueries().count(), 0);
    Assert.assertEquals(0, command.getBsonByteVectorQueries().count(), 0);
    Assert.assertEquals(0, command.getBsonBitVectorQueries().count(), 0);
  }

  @Test
  public void testExactVectorSearchQueryCounter() throws Exception {
    var mocks = new Mocks();
    var query =
        VectorQueryBuilder.builder()
            .index(INDEX_NAME)
            .criteria(
                ExactVectorCriteriaBuilder.builder()
                    .limit(LIMIT)
                    .queryVector(QUERY_VECTOR)
                    .path(PATH)
                    .filter(getFilter())
                    .build())
            .build();
    var command =
        new VectorSearchCommand(
            VectorSearchCommandDefinitionBuilder.builder()
                .db(DATABASE_NAME)
                .collectionName(COLLECTION_NAME)
                .collectionUuid(COLLECTION_UUID)
                .vectorSearchQuery(query)
                .build(),
            mocks.catalog,
            mocks.initializedIndexCatalog,
            bootstrapperMetadata(),
            MOCK_EMBEDDING_SERVICE,
            new VectorSearchCommand.Metrics(mockMetricsFactory()),
            mocks.cursorManager,
            CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT);

    var result = command.run();

    var expected =
        mockInitialMongotCursorBatchForVectorSearch(0, new VectorSearchResultBatch(4), NAMESPACE)
            .toBson();
    Assert.assertEquals(expected, result);

    // Check counter
    Assert.assertEquals(1, command.getExactVectorSearchQueries().count(), 0);

    command.run();
    // Check counter
    Assert.assertEquals(2, command.getExactVectorSearchQueries().count(), 0);
  }

  @Test
  public void testApproximateVectorSearchQueryCounter() throws Exception {
    var mocks = new Mocks();
    var query =
        VectorQueryBuilder.builder()
            .index(INDEX_NAME)
            .criteria(
                ApproximateVectorQueryCriteriaBuilder.builder()
                    .limit(LIMIT)
                    .numCandidates(NUM_CANDIDATES)
                    .queryVector(QUERY_VECTOR)
                    .path(PATH)
                    .filter(getFilter())
                    .build())
            .build();
    var mockMetricsFactory = mockMetricsFactory();
    var command =
        new VectorSearchCommand(
            VectorSearchCommandDefinitionBuilder.builder()
                .db(DATABASE_NAME)
                .collectionName(COLLECTION_NAME)
                .collectionUuid(COLLECTION_UUID)
                .vectorSearchQuery(query)
                .build(),
            mocks.catalog,
            mocks.initializedIndexCatalog,
            bootstrapperMetadata(),
            MOCK_EMBEDDING_SERVICE,
            new VectorSearchCommand.Metrics(mockMetricsFactory),
            mocks.cursorManager,
            CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT);

    var result = command.run();

    var expected =
        mockInitialMongotCursorBatchForVectorSearch(0, new VectorSearchResultBatch(4), NAMESPACE);
    Assert.assertEquals(expected.toBson(), result);

    // Check counter
    Assert.assertEquals(1, command.getApproximateVectorSearchQueries().count(), 0);

    command.run();
    // Check counter
    Assert.assertEquals(2, command.getApproximateVectorSearchQueries().count(), 0);
  }

  // Helper to test running count for both exact and approximate vector search queries.
  private void testVectorSearchRunningCountHelper(
      VectorSearchCriteria.Type queryType, boolean throwException) throws Exception {
    var mocks = new Mocks();

    var queryCountDown = new CountDownLatch(1);
    var testCountDown = new CountDownLatch(1);

    when(mocks.reader.query(any(), any()))
        .thenAnswer(
            invocation -> {
              // Signal the test thread proceed.
              testCountDown.countDown();
              // Wait test thread signal
              queryCountDown.await();
              if (throwException) {
                throw new IOException("IO error");
              }
              return mocks.bsonResults;
            });

    var query =
        switch (queryType) {
          case EXACT ->
              VectorQueryBuilder.builder()
                  .index(INDEX_NAME)
                  .criteria(
                      ExactVectorCriteriaBuilder.builder()
                          .limit(LIMIT)
                          .queryVector(QUERY_VECTOR)
                          .path(PATH)
                          .filter(getFilter())
                          .build())
                  .build();
          case APPROXIMATE ->
              VectorQueryBuilder.builder()
                  .index(INDEX_NAME)
                  .criteria(
                      ApproximateVectorQueryCriteriaBuilder.builder()
                          .limit(LIMIT)
                          .numCandidates(NUM_CANDIDATES)
                          .queryVector(QUERY_VECTOR)
                          .path(PATH)
                          .filter(getFilter())
                          .build())
                  .build();
          case AUTO_EMBEDDING ->
              throw new IllegalArgumentException("Unsupported query type: " + queryType);
        };
    var mockMetricsFactory = mockMetricsFactory();
    var command =
        new VectorSearchCommand(
            VectorSearchCommandDefinitionBuilder.builder()
                .db(DATABASE_NAME)
                .collectionName(COLLECTION_NAME)
                .collectionUuid(COLLECTION_UUID)
                .vectorSearchQuery(query)
                .build(),
            mocks.catalog,
            mocks.initializedIndexCatalog,
            bootstrapperMetadata(),
            MOCK_EMBEDDING_SERVICE,
            new VectorSearchCommand.Metrics(mockMetricsFactory),
            mocks.cursorManager,
            CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT);

    var result = CompletableFuture.supplyAsync(command::run, Executors.newSingleThreadExecutor());

    var gaugeName =
        switch (queryType) {
          case EXACT -> "concurrentExactQueries";
          case APPROXIMATE -> "concurrentApproximateQueries";
          case AUTO_EMBEDDING ->
              throw new IllegalArgumentException("Unsupported query type: " + queryType);
        };
    // wait for the query thread reaching synchronization point.
    testCountDown.await();
    // validate the running count
    Assert.assertEquals(1, mockMetricsFactory.get(gaugeName).gauge().value(), 0);
    // Signal the query thread proceed.
    queryCountDown.countDown();
    result.get();
    Assert.assertEquals(0, mockMetricsFactory.get(gaugeName).gauge().value(), 0);
  }

  @Test
  public void testExactVectorSearchQueryRunningCount() throws Exception {
    testVectorSearchRunningCountHelper(VectorSearchCriteria.Type.EXACT, true);
    testVectorSearchRunningCountHelper(VectorSearchCriteria.Type.EXACT, false);
    testVectorSearchRunningCountHelper(VectorSearchCriteria.Type.APPROXIMATE, true);
    testVectorSearchRunningCountHelper(VectorSearchCriteria.Type.APPROXIMATE, false);
  }

  @Test
  public void getBatch_unreachableErrorThrown_metricIncremented()
      throws ReaderClosedException,
          IOException,
          InvalidQueryException,
          BsonParseException,
          IndexUnavailableException {
    var mocks = new Mocks();
    when(mocks.catalog.getIndex(any(), any(), any(), any())).thenThrow(Check.unreachableError());

    var query =
        VectorQueryBuilder.builder()
            .index(INDEX_NAME)
            .criteria(
                ApproximateVectorQueryCriteriaBuilder.builder()
                    .limit(LIMIT)
                    .numCandidates(NUM_CANDIDATES)
                    .queryVector(QUERY_VECTOR)
                    .path(PATH)
                    .filter(getFilter())
                    .build())
            .build();
    var metricsFactory = mockMetricsFactory();
    var command =
        new VectorSearchCommand(
            VectorSearchCommandDefinitionBuilder.builder()
                .db(DATABASE_NAME)
                .collectionName(COLLECTION_NAME)
                .collectionUuid(COLLECTION_UUID)
                .vectorSearchQuery(query)
                .build(),
            mocks.catalog,
            mocks.initializedIndexCatalog,
            bootstrapperMetadata(),
            MOCK_EMBEDDING_SERVICE,
            new VectorSearchCommand.Metrics(metricsFactory),
            mocks.cursorManager,
            CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT);

    Assert.assertThrows(AssertionError.class, command::run);
    assertThat(
            metricsFactory
                .counter(
                    "vectorSearchCommandInternalFailures",
                    Tags.of(
                        "exceptionName",
                        AssertionError.class.getSimpleName(),
                        ErrorType.METRIC_TAG_KEY,
                        ErrorType.INTERNAL_ERROR.toString()))
                .count())
        .isEqualTo(1.0);
  }

  // Test to prove that a RunTimeException that is wrapping an InvalidQueryException results in
  // the system treating it as an invalid query (user error) and not an internal failure
  // (system failure).
  @Test
  public void testWrappedInvalidQueryException() throws Exception {
    var mocks = new Mocks();

    // Force the reader to throw a RuntimeException that wraps an InvalidQueryException.
    when(mocks.reader.query(any(), any()))
        .thenThrow(new RuntimeException(new InvalidQueryException("wrapped user error")));

    var query =
        VectorQueryBuilder.builder()
            .index(INDEX_NAME)
            .criteria(
                ApproximateVectorQueryCriteriaBuilder.builder()
                    .limit(LIMIT)
                    .numCandidates(NUM_CANDIDATES)
                    .queryVector(QUERY_VECTOR)
                    .path(PATH)
                    .filter(getFilter())
                    .build())
            .build();
    var metricsFactory = mockMetricsFactory();

    var command =
        new VectorSearchCommand(
            VectorSearchCommandDefinitionBuilder.builder()
                .db(DATABASE_NAME)
                .collectionName(COLLECTION_NAME)
                .collectionUuid(COLLECTION_UUID)
                .vectorSearchQuery(query)
                .build(),
            mocks.catalog,
            mocks.initializedIndexCatalog,
            bootstrapperMetadata(),
            MOCK_EMBEDDING_SERVICE,
            new VectorSearchCommand.Metrics(metricsFactory),
            mocks.cursorManager,
            CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT);

    var result = command.run();

    // Should be an error response (ok: 0)
    Assert.assertEquals(0, result.getInt32("ok").getValue());
    // The message should be unwrapped (contain the inner message)
    Assert.assertTrue(result.getString("errmsg").getValue().contains("wrapped user error"));

    // Should count as a User Error (Invalid Query)
    Assert.assertEquals(
        1.0, metricsFactory.counter("vectorSearchCommandInvalidQueries").count(), 0.0);

    // Should NOT count as an Internal System Failure
    Assert.assertEquals(
        0.0, metricsFactory.counter("vectorSearchCommandInternalFailures").count(), 0.0);

    // Should count as a wrapped InvalidQueryException
    Assert.assertEquals(
        1.0,
        metricsFactory.counter("vectorSearchCommandWrappedInvalidQueryException").count(),
        0.0);
  }

  @Test
  public void testCheckSupportRejectsWhenStoredSourceUndefined() throws Exception {
    Assume.assumeTrue(this.mongoDbVersion.compareTo(MIN_STORED_SOURCE_VERSION) >= 0);
    var query =
        VectorQueryBuilder.builder()
            .index(INDEX_NAME)
            .criteria(
                ApproximateVectorQueryCriteriaBuilder.builder()
                    .limit(LIMIT)
                    .numCandidates(NUM_CANDIDATES)
                    .queryVector(QUERY_VECTOR)
                    .path(PATH)
                    .build())
            .returnStoredSource(true)
            .build();

    InitializedVectorIndex mockIndex = mock(InitializedVectorIndex.class);
    when(mockIndex.getDefinition())
        .thenReturn(
            VectorIndexDefinitionBuilder.builder()
                .withCosineVectorField(PATH.toString(), 3)
                .build());

    var ex =
        Assert.assertThrows(
            InvalidQueryException.class,
            () ->
                VectorSearchCommand.checkSupportForVectorStoredSource(
                    bootstrapperMetadata(), query, Optional.of(mockIndex)));
    Assert.assertTrue(ex.getMessage().contains("storedSource is not configured for this index"));
  }

  @Test
  public void testCheckSupportRejectsOldMongoDbVersion() throws Exception {
    Assume.assumeTrue(this.mongoDbVersion.compareTo(MIN_STORED_SOURCE_VERSION) < 0);
    var query =
        VectorQueryBuilder.builder()
            .index(INDEX_NAME)
            .criteria(
                ApproximateVectorQueryCriteriaBuilder.builder()
                    .limit(LIMIT)
                    .numCandidates(NUM_CANDIDATES)
                    .queryVector(QUERY_VECTOR)
                    .path(PATH)
                    .build())
            .returnStoredSource(true)
            .build();

    InitializedVectorIndex mockIndex = mock(InitializedVectorIndex.class);
    when(mockIndex.getDefinition())
        .thenReturn(
            VectorIndexDefinitionBuilder.builder()
                .withCosineVectorField(PATH.toString(), 3)
                .storedSource(
                    StoredSourceDefinition.create(
                        StoredSourceDefinition.Mode.INCLUSION, List.of("_id")))
                .build());

    var metadata =
        new SearchCommandsRegister.BootstrapperMetadata(
            "testVersion",
            "localhost",
            () -> new MongoDbServerInfo(Optional.of(this.mongoDbVersion), Optional.of("testRs")),
            FeatureFlags.withDefaults()
                .enable(Feature.ENABLE_VALIDATION_OF_RETURN_STORED_SOURCE)
                .build(),
            DynamicFeatureFlagRegistry.empty());

    var ex =
        Assert.assertThrows(
            InvalidQueryException.class,
            () ->
                VectorSearchCommand.checkSupportForVectorStoredSource(
                    metadata, query, Optional.of(mockIndex)));
    Assert.assertTrue(ex.getMessage().contains("requires MongoDB server version"));
  }

  private static class Mocks {

    private final IndexCatalog catalog;
    private final InitializedIndexCatalog initializedIndexCatalog;
    private final IndexMetricsUpdater metricsUpdater;
    final LuceneVectorIndexReader reader;
    final BsonArray bsonResults;
    final MongotCursorManager cursorManager;

    public Mocks()
        throws ReaderClosedException,
            IOException,
            InvalidQueryException,
            IndexUnavailableException {
      var index = Mockito.mock(LuceneVectorIndex.class);

      InitializedVectorIndex initializedIndex = Mockito.mock(InitializedVectorIndex.class);
      lenient()
          .when(initializedIndex.getDefinition())
          .thenReturn(
              VectorIndexDefinitionBuilder.builder()
                  .withTextField(PATH.toString())
                  .storedSource(
                      StoredSourceDefinition.create(
                          StoredSourceDefinition.Mode.INCLUSION, List.of("_id")))
                  .build());
      lenient().when(index.isCompatibleWith(any(VectorIndexDefinition.class))).thenReturn(true);
      lenient().when(index.asVectorIndex()).thenCallRealMethod();
      lenient().when(initializedIndex.asVectorIndex()).thenCallRealMethod();
      this.catalog = Mockito.mock(IndexCatalog.class);
      this.initializedIndexCatalog = Mockito.mock(InitializedIndexCatalog.class);
      IndexGeneration indexGeneration1 = mock(IndexGeneration.class);
      when(indexGeneration1.getGenerationId())
          .thenReturn(MOCK_INDEX_DEFINITION_GENERATION.getGenerationId());
      lenient()
          .when(indexGeneration1.getDefinition())
          .thenReturn(
              VectorIndexDefinitionBuilder.builder()
                  .withDotProductVectorField(PATH.toString(), 1024)
                  .build());
      lenient().when(indexGeneration1.getIndex()).thenReturn(index);
      // Change to use initialized index.
      when(this.catalog.getIndex(DATABASE_NAME, COLLECTION_UUID, Optional.empty(), INDEX_NAME))
          .thenReturn(Optional.of(indexGeneration1));

      when(this.initializedIndexCatalog.getIndex(MOCK_INDEX_GENERATION_ID))
          .thenReturn(Optional.of(initializedIndex));
      this.reader = Mockito.mock(LuceneVectorIndexReader.class);
      when(initializedIndex.getReader()).thenReturn(this.reader);

      this.bsonResults = new VectorSearchResultBatch(4).getBsonResults();
      when(this.reader.query(any(), any())).thenReturn(this.bsonResults);

      this.metricsUpdater =
          IndexMetricsUpdaterBuilder.builder()
              .metricsFactory(SearchIndex.mockMetricsFactory())
              .indexMetricsSupplier(Mockito.mock(IndexMetricValuesSupplier.class))
              .build();
      when(initializedIndex.getMetricsUpdater()).thenReturn(this.metricsUpdater);

      this.cursorManager = mock(MongotCursorManager.class);
    }

    void verifyEmptyBatchReturned(BsonDocument result, VectorSearchCommand command)
        throws Exception {
      var expected =
          mockInitialMongotCursorBatchForVectorSearch(0, new VectorSearchResultBatch(0), NAMESPACE)
              .toBson();
      Assert.assertEquals(expected, result);
      Assert.assertTrue(command.getCreatedCursorIds().isEmpty());
      Assert.assertEquals(
          0, this.metricsUpdater.getQueryingMetricsUpdater().getVectorCommandCounter().count(), 0);
    }
  }

  private static MetricsFactory mockMetricsFactory() {
    return new MetricsFactory("mockNamespace", new SimpleMeterRegistry());
  }
}
