package com.xgen.mongot.server.command.search;

import static com.google.common.truth.Truth.assertThat;
import static com.xgen.mongot.util.bson.FloatVector.OriginalType.NATIVE;
import static com.xgen.testing.mongot.mock.cursor.MongotCursorBatches.mockInitialMongotCursorBatchForVectorSearch;
import static com.xgen.testing.mongot.mock.index.VectorIndex.MOCK_INDEX_DEFINITION_GENERATION;
import static com.xgen.testing.mongot.mock.index.VectorIndex.MOCK_INDEX_GENERATION_ID;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.xgen.mongot.catalog.IndexCatalog;
import com.xgen.mongot.catalog.InitializedIndexCatalog;
import com.xgen.mongot.cursor.CursorConfig;
import com.xgen.mongot.cursor.MongotCursorManager;
import com.xgen.mongot.embedding.EmbeddingRequestContext;
import com.xgen.mongot.embedding.VectorOrError;
import com.xgen.mongot.embedding.config.MaterializedViewCollectionMetadata;
import com.xgen.mongot.embedding.providers.EmbeddingServiceManager;
import com.xgen.mongot.embedding.providers.configs.EmbeddingModelCatalog;
import com.xgen.mongot.embedding.providers.configs.EmbeddingModelConfig;
import com.xgen.mongot.embedding.providers.configs.EmbeddingServiceConfig;
import com.xgen.mongot.featureflag.FeatureFlags;
import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlagRegistry;
import com.xgen.mongot.index.IndexGeneration;
import com.xgen.mongot.index.IndexMetricValuesSupplier;
import com.xgen.mongot.index.IndexMetricsUpdater;
import com.xgen.mongot.index.IndexUnavailableException;
import com.xgen.mongot.index.InitializedVectorIndex;
import com.xgen.mongot.index.ReaderClosedException;
import com.xgen.mongot.index.autoembedding.AutoEmbeddingIndexGeneration;
import com.xgen.mongot.index.autoembedding.InitializedMaterializedViewIndex;
import com.xgen.mongot.index.autoembedding.MaterializedViewIndexGeneration;
import com.xgen.mongot.index.definition.VectorIndexDefinition;
import com.xgen.mongot.index.lucene.LuceneVectorIndex;
import com.xgen.mongot.index.lucene.LuceneVectorIndexReader;
import com.xgen.mongot.index.query.InvalidQueryException;
import com.xgen.mongot.index.query.MaterializedVectorSearchQuery;
import com.xgen.mongot.index.query.VectorSearchQuery;
import com.xgen.mongot.index.query.operators.VectorSearchFilter;
import com.xgen.mongot.index.query.operators.VectorSearchQueryInput;
import com.xgen.mongot.index.query.operators.mql.Clause;
import com.xgen.mongot.index.version.Generation;
import com.xgen.mongot.index.version.MaterializedViewGeneration;
import com.xgen.mongot.index.version.MaterializedViewGenerationId;
import com.xgen.mongot.metrics.MetricsFactory;
import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.bson.Vector;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.mongodb.MongoDbServerInfo;
import com.xgen.testing.mongot.index.IndexMetricsUpdaterBuilder;
import com.xgen.testing.mongot.index.definition.VectorIndexDefinitionBuilder;
import com.xgen.testing.mongot.index.query.ApproximateVectorQueryCriteriaBuilder;
import com.xgen.testing.mongot.index.query.VectorQueryBuilder;
import com.xgen.testing.mongot.index.query.operators.mql.ClauseBuilder;
import com.xgen.testing.mongot.index.query.operators.mql.MqlFilterOperatorBuilder;
import com.xgen.testing.mongot.index.query.operators.mql.ValueBuilder;
import com.xgen.testing.mongot.mock.index.SearchIndex;
import com.xgen.testing.mongot.mock.index.VectorSearchResultBatch;
import com.xgen.testing.mongot.server.command.search.definition.request.VectorSearchCommandDefinitionBuilder;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

/**
 * Tests for VectorSearchCommand with auto-embedding (query text) functionality. These tests use
 * vectorSearch.query instead of vectorSearch.queryVector.
 */
@RunWith(MockitoJUnitRunner.class)
public class VectorSearchCommandAutoEmbedTest {
  private static final String DATABASE_NAME = "testDb";
  private static final String COLLECTION_NAME = "testCollection";
  private static final String NAMESPACE = String.format("%s.%s", DATABASE_NAME, COLLECTION_NAME);
  private static final UUID COLLECTION_UUID = UUID.randomUUID();
  private static final String INDEX_NAME = "default";
  private static final String MAT_VIEW_INDEX_NAME = "matviewDefault";
  private static final int NUM_CANDIDATES = 5000;
  private static final int LIMIT = 1000;
  private static final Vector QUERY_VECTOR = Vector.fromFloats(new float[] {1f, 2f, 3f}, NATIVE);
  private static final FieldPath PATH = FieldPath.parse("testPath");
  private static final SearchCommandsRegister.BootstrapperMetadata BOOTSTRAPPER_METADATA =
      new SearchCommandsRegister.BootstrapperMetadata(
          "testVersion",
          "localhost",
          () -> MongoDbServerInfo.EMPTY,
          FeatureFlags.getDefault(),
          DynamicFeatureFlagRegistry.empty());
  private static final Supplier<EmbeddingServiceManager> MOCK_EMBEDDING_SERVICE =
      mockEmbeddingServiceManager(new VectorOrError(QUERY_VECTOR));

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
  public void testValidQueryFromTextWithDefaultModel() throws Exception {
    var mocks = new Mocks();
    var command =
        buildAutoEmbeddingVectorSearchCommandWithMocks(
            mocks,
            VectorQueryBuilder.builder()
                .index(INDEX_NAME)
                .criteria(
                    ApproximateVectorQueryCriteriaBuilder.builder()
                        .limit(LIMIT)
                        .numCandidates(NUM_CANDIDATES)
                        .query(new VectorSearchQueryInput.Text("test query"))
                        .path(PATH)
                        .filter(getFilter())
                        .build())
                .build());
    var result = command.run();

    // ensure that reader is being called with QUERY_VECTOR
    ArgumentCaptor<MaterializedVectorSearchQuery> captor =
        ArgumentCaptor.forClass(MaterializedVectorSearchQuery.class);
    Mockito.verify(mocks.reader, times(1)).query(captor.capture());
    MaterializedVectorSearchQuery ranQuery = captor.getValue();
    Assert.assertEquals(QUERY_VECTOR, ranQuery.queryVector().get());

    var expected =
        mockInitialMongotCursorBatchForVectorSearch(0, new VectorSearchResultBatch(4), NAMESPACE)
            .toBson();
    Assert.assertEquals(expected, result);
    Assert.assertTrue(command.getCreatedCursorIds().isEmpty());
    Assert.assertEquals(
        1, mocks.metricsUpdater.getQueryingMetricsUpdater().getVectorCommandCounter().count(), 0);
    // Check counter
    Assert.assertEquals(1, command.getAutoEmbeddingVectorSearchQueries().count(), 0);
    Assert.assertEquals(1, command.getAutoEmbeddingTextQueries().count(), 0);
    Assert.assertEquals(0, command.getAutoEmbeddingMultiModalQueries().count(), 0);
  }

  @Test
  public void testValidQueryFromTextWithDefaultModel_withMultiModalQuery() throws Exception {
    var mocks = new Mocks();
    var command = buildAutoEmbeddingVectorSearchCommandWithMocksWithMultiModalQuery(mocks);
    var result = command.run();

    // ensure that reader is being called with QUERY_VECTOR
    ArgumentCaptor<MaterializedVectorSearchQuery> captor =
        ArgumentCaptor.forClass(MaterializedVectorSearchQuery.class);
    Mockito.verify(mocks.reader, times(1)).query(captor.capture());
    MaterializedVectorSearchQuery ranQuery = captor.getValue();
    Assert.assertEquals(QUERY_VECTOR, ranQuery.queryVector().get());

    var expected =
        mockInitialMongotCursorBatchForVectorSearch(0, new VectorSearchResultBatch(4), NAMESPACE)
            .toBson();
    Assert.assertEquals(expected, result);
    Assert.assertTrue(command.getCreatedCursorIds().isEmpty());
    Assert.assertEquals(
        1, mocks.metricsUpdater.getQueryingMetricsUpdater().getVectorCommandCounter().count(), 0);
    // Check counter
    Assert.assertEquals(1, command.getAutoEmbeddingVectorSearchQueries().count(), 0);
    Assert.assertEquals(0, command.getAutoEmbeddingTextQueries().count(), 0);
    Assert.assertEquals(1, command.getAutoEmbeddingMultiModalQueries().count(), 0);
  }

  @Test
  public void testAutoEmbeddingIndex_throwInvalidQueryException() throws Exception {
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
                                .path(PATH.newChild("invalid_child"))
                                .filter(getFilter())
                                .build())
                        .build())
                .build(),
            mocks.catalog,
            mocks.initializedIndexCatalog,
            BOOTSTRAPPER_METADATA,
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
                    "Vector index for this text based query is invalid due to missing model")),
        result);
  }

  @Test
  public void extractModelNamesFromMaterializedViewAutoEmbeddingIndexDefinition_returnsModelNames()
      throws Exception {
    var mocks = new Mocks();
    var matViewVectorSearchQuery =
        VectorQueryBuilder.builder()
            .index(MAT_VIEW_INDEX_NAME)
            .criteria(
                ApproximateVectorQueryCriteriaBuilder.builder()
                    .limit(LIMIT)
                    .numCandidates(NUM_CANDIDATES)
                    .query(new VectorSearchQueryInput.Text("test query"))
                    .path(PATH)
                    .filter(getFilter())
                    .build())
            .build();
    var command = buildAutoEmbeddingVectorSearchCommandWithMocks(mocks, matViewVectorSearchQuery);
    Optional<String> modelName =
        command.findEmbeddingModelName(
            VectorIndexDefinitionBuilder.builder()
                .withDotProductVectorField(PATH.toString(), 1024)
                .build(),
            matViewVectorSearchQuery);
    assertThat(modelName).isPresent();
    Assert.assertEquals("voyage-3-large", modelName.get());
  }

  @Test
  public void extractModelNamesFromRegularVectorIndexDefinition_returnsNoModelName()
      throws Exception {
    var mocks = new Mocks();
    var matViewVectorSearchQuery =
        VectorQueryBuilder.builder()
            .index(INDEX_NAME)
            .criteria(
                ApproximateVectorQueryCriteriaBuilder.builder()
                    .limit(LIMIT)
                    .numCandidates(NUM_CANDIDATES)
                    .query(new VectorSearchQueryInput.Text("test query"))
                    .path(PATH)
                    .filter(getFilter())
                    .build())
            .build();
    var command = buildAutoEmbeddingVectorSearchCommandWithMocks(mocks, matViewVectorSearchQuery);
    Optional<String> modelName =
        command.findEmbeddingModelName(
            VectorIndexDefinitionBuilder.builder()
                .withDotProductVectorField(PATH.toString(), 1024)
                .build(),
            matViewVectorSearchQuery);
    assertThat(modelName).isEmpty();
  }

  @Test
  public void findEmbeddingModelName_throwsException_whenQueryModelNotAllowed() throws Exception {
    var mocks = new Mocks();
    // Query specifies a model that is not in the allowed list
    var queryWithInvalidModel =
        VectorQueryBuilder.builder()
            .index(MAT_VIEW_INDEX_NAME)
            .criteria(
                ApproximateVectorQueryCriteriaBuilder.builder()
                    .limit(LIMIT)
                    .numCandidates(NUM_CANDIDATES)
                    .query(
                        new VectorSearchQueryInput.Text("test query", Optional.of("invalid-model")))
                    .path(PATH)
                    .filter(getFilter())
                    .build())
            .build();
    var command = buildAutoEmbeddingVectorSearchCommandWithMocks(mocks, queryWithInvalidModel);
    // Should throw InvalidQueryException when query model is not allowed
    var exception =
        Assert.assertThrows(
            InvalidQueryException.class,
            () ->
                command.findEmbeddingModelName(
                    VectorIndexDefinitionBuilder.builder()
                        .withDotProductVectorField(PATH.toString(), 1024)
                        .build(),
                    queryWithInvalidModel));
    assertThat(exception.getMessage()).contains("model 'invalid-model' is not allowed");
  }

  @Test
  public void findEmbeddingModelName_fallsBackToIndexModel_whenQueryModelNotSpecified()
      throws Exception {
    var mocks = new Mocks();
    // Query does not specify a model
    var queryWithoutModel =
        VectorQueryBuilder.builder()
            .index(MAT_VIEW_INDEX_NAME)
            .criteria(
                ApproximateVectorQueryCriteriaBuilder.builder()
                    .limit(LIMIT)
                    .numCandidates(NUM_CANDIDATES)
                    .query(new VectorSearchQueryInput.Text("test query"))
                    .path(PATH)
                    .filter(getFilter())
                    .build())
            .build();
    var command = buildAutoEmbeddingVectorSearchCommandWithMocks(mocks, queryWithoutModel);
    Optional<String> modelName =
        command.findEmbeddingModelName(
            VectorIndexDefinitionBuilder.builder()
                .withDotProductVectorField(PATH.toString(), 1024)
                .build(),
            queryWithoutModel);
    // Should fall back to the index model (voyage-3-large from the mock)
    assertThat(modelName).isPresent();
    Assert.assertEquals("voyage-3-large", modelName.get());
  }

  @Test
  public void findEmbeddingModelName_returnsEmpty_whenQueryVectorUsed() throws Exception {
    var mocks = new Mocks();
    // Query uses queryVector instead of query text
    var queryWithVector =
        VectorQueryBuilder.builder()
            .index(MAT_VIEW_INDEX_NAME)
            .criteria(
                ApproximateVectorQueryCriteriaBuilder.builder()
                    .limit(LIMIT)
                    .numCandidates(NUM_CANDIDATES)
                    .queryVector(QUERY_VECTOR)
                    .path(PATH)
                    .filter(getFilter())
                    .build())
            .build();
    var command = buildAutoEmbeddingVectorSearchCommandWithMocks(mocks, queryWithVector);
    Optional<String> modelName =
        command.findEmbeddingModelName(
            VectorIndexDefinitionBuilder.builder()
                .withDotProductVectorField(PATH.toString(), 1024)
                .build(),
            queryWithVector);
    // Should return empty since queryVector is used, not query text
    assertThat(modelName).isEmpty();
  }

  @Test
  public void findEmbeddingModelName_voyage4ModelsAreInterchangeable_queryVoyage4WithIndexVoyage4()
      throws Exception {
    // Test that voyage-4 query model works with voyage-4 index model
    var mocks = new Mocks();
    var queryWithVoyage4 =
        VectorQueryBuilder.builder()
            .index(MAT_VIEW_INDEX_NAME)
            .criteria(
                ApproximateVectorQueryCriteriaBuilder.builder()
                    .limit(LIMIT)
                    .numCandidates(NUM_CANDIDATES)
                    .query(new VectorSearchQueryInput.Text("test query", Optional.of("voyage-4")))
                    .path(PATH)
                    .filter(getFilter())
                    .build())
            .build();
    var command = buildAutoEmbeddingVectorSearchCommandWithMocks(mocks, queryWithVoyage4);
    Optional<String> modelName =
        command.findEmbeddingModelName(
            VectorIndexDefinitionBuilder.builder()
                .withAutoEmbedField(PATH.toString(), "voyage-4")
                .build(),
            queryWithVoyage4);
    assertThat(modelName).isPresent();
    Assert.assertEquals("voyage-4", modelName.get());
  }

  @Test
  public void
      findEmbeddingModelName_voyage4ModelsAreInterchangeable_queryVoyage4LargeWithIndexVoyage4()
          throws Exception {
    // Test that voyage-4-large query model works with voyage-4 index model
    var mocks = new Mocks();
    var queryWithVoyage4Large =
        VectorQueryBuilder.builder()
            .index(MAT_VIEW_INDEX_NAME)
            .criteria(
                ApproximateVectorQueryCriteriaBuilder.builder()
                    .limit(LIMIT)
                    .numCandidates(NUM_CANDIDATES)
                    .query(
                        new VectorSearchQueryInput.Text(
                            "test query", Optional.of("voyage-4-large")))
                    .path(PATH)
                    .filter(getFilter())
                    .build())
            .build();
    var command = buildAutoEmbeddingVectorSearchCommandWithMocks(mocks, queryWithVoyage4Large);
    Optional<String> modelName =
        command.findEmbeddingModelName(
            VectorIndexDefinitionBuilder.builder()
                .withAutoEmbedField(PATH.toString(), "voyage-4")
                .build(),
            queryWithVoyage4Large);
    // Should use the query-specified model (voyage-4-large) since it's compatible with voyage-4
    assertThat(modelName).isPresent();
    Assert.assertEquals("voyage-4-large", modelName.get());
  }

  @Test
  public void
      findEmbeddingModelName_voyage4ModelsAreInterchangeable_queryVoyage4LiteWithIndexVoyage4Large()
          throws Exception {
    // Test that voyage-4-lite query model works with voyage-4-large index model
    var mocks = new Mocks();
    var queryWithVoyage4Lite =
        VectorQueryBuilder.builder()
            .index(MAT_VIEW_INDEX_NAME)
            .criteria(
                ApproximateVectorQueryCriteriaBuilder.builder()
                    .limit(LIMIT)
                    .numCandidates(NUM_CANDIDATES)
                    .query(
                        new VectorSearchQueryInput.Text("test query", Optional.of("voyage-4-lite")))
                    .path(PATH)
                    .filter(getFilter())
                    .build())
            .build();
    var command = buildAutoEmbeddingVectorSearchCommandWithMocks(mocks, queryWithVoyage4Lite);
    Optional<String> modelName =
        command.findEmbeddingModelName(
            VectorIndexDefinitionBuilder.builder()
                .withAutoEmbedField(PATH.toString(), "voyage-4-large")
                .build(),
            queryWithVoyage4Lite);
    // voyage-4-lite is compatible with voyage-4-large, so query model is used
    assertThat(modelName).isPresent();
    Assert.assertEquals("voyage-4-lite", modelName.get());
  }

  @Test
  public void
      findEmbeddingModelName_voyage4ModelsAreInterchangeable_queryVoyage4WithIndexVoyage4Lite()
          throws Exception {
    // Test that voyage-4 query model works with voyage-4-lite index model
    var mocks = new Mocks();
    var queryWithVoyage4 =
        VectorQueryBuilder.builder()
            .index(MAT_VIEW_INDEX_NAME)
            .criteria(
                ApproximateVectorQueryCriteriaBuilder.builder()
                    .limit(LIMIT)
                    .numCandidates(NUM_CANDIDATES)
                    .query(new VectorSearchQueryInput.Text("test query", Optional.of("voyage-4")))
                    .path(PATH)
                    .filter(getFilter())
                    .build())
            .build();
    var command = buildAutoEmbeddingVectorSearchCommandWithMocks(mocks, queryWithVoyage4);
    Optional<String> modelName =
        command.findEmbeddingModelName(
            VectorIndexDefinitionBuilder.builder()
                .withAutoEmbedField(PATH.toString(), "voyage-4-lite")
                .build(),
            queryWithVoyage4);
    // Should use the query-specified model (voyage-4) since it's compatible with voyage-4-lite
    assertThat(modelName).isPresent();
    Assert.assertEquals("voyage-4", modelName.get());
  }

  @Test
  public void findEmbeddingModelName_voyage4NotCompatibleWithVoyage3() throws Exception {
    // Test that voyage-4 query model is NOT compatible with voyage-3 index model
    var mocks = new Mocks();
    var queryWithVoyage4 =
        VectorQueryBuilder.builder()
            .index(MAT_VIEW_INDEX_NAME)
            .criteria(
                ApproximateVectorQueryCriteriaBuilder.builder()
                    .limit(LIMIT)
                    .numCandidates(NUM_CANDIDATES)
                    .query(new VectorSearchQueryInput.Text("test query", Optional.of("voyage-4")))
                    .path(PATH)
                    .filter(getFilter())
                    .build())
            .build();
    var command = buildAutoEmbeddingVectorSearchCommandWithMocks(mocks, queryWithVoyage4);
    var exception =
        Assert.assertThrows(
            InvalidQueryException.class,
            () ->
                command.findEmbeddingModelName(
                    VectorIndexDefinitionBuilder.builder()
                        .withAutoEmbedField(PATH.toString(), "voyage-3")
                        .build(),
                    queryWithVoyage4));
    assertThat(exception.getMessage())
        .contains("query model 'voyage-4' is not compatible with the index model 'voyage-3'");
  }

  @Test
  public void findEmbeddingModelName_voyage4NotCompatibleWithVoyageCode3() throws Exception {
    // Test that voyage-4 query model is NOT compatible with voyage-code-3 index model
    var mocks = new Mocks();
    var queryWithVoyage4 =
        VectorQueryBuilder.builder()
            .index(MAT_VIEW_INDEX_NAME)
            .criteria(
                ApproximateVectorQueryCriteriaBuilder.builder()
                    .limit(LIMIT)
                    .numCandidates(NUM_CANDIDATES)
                    .query(new VectorSearchQueryInput.Text("test query", Optional.of("voyage-4")))
                    .path(PATH)
                    .filter(getFilter())
                    .build())
            .build();
    var command = buildAutoEmbeddingVectorSearchCommandWithMocks(mocks, queryWithVoyage4);
    var exception =
        Assert.assertThrows(
            InvalidQueryException.class,
            () ->
                command.findEmbeddingModelName(
                    VectorIndexDefinitionBuilder.builder()
                        .withAutoEmbedField(PATH.toString(), "voyage-code-3")
                        .build(),
                    queryWithVoyage4));
    assertThat(exception.getMessage())
        .contains("query model 'voyage-4' is not compatible with the index model 'voyage-code-3'");
  }

  @Test
  public void findEmbeddingModelName_voyageCode3NotCompatibleWithVoyage4() throws Exception {
    // Test that voyage-code-3 query model is NOT compatible with voyage-4 index model
    var mocks = new Mocks();
    var queryWithVoyageCode3 =
        VectorQueryBuilder.builder()
            .index(MAT_VIEW_INDEX_NAME)
            .criteria(
                ApproximateVectorQueryCriteriaBuilder.builder()
                    .limit(LIMIT)
                    .numCandidates(NUM_CANDIDATES)
                    .query(
                        new VectorSearchQueryInput.Text("test query", Optional.of("voyage-code-3")))
                    .path(PATH)
                    .filter(getFilter())
                    .build())
            .build();
    var command = buildAutoEmbeddingVectorSearchCommandWithMocks(mocks, queryWithVoyageCode3);
    var exception =
        Assert.assertThrows(
            InvalidQueryException.class,
            () ->
                command.findEmbeddingModelName(
                    VectorIndexDefinitionBuilder.builder()
                        .withAutoEmbedField(PATH.toString(), "voyage-4")
                        .build(),
                    queryWithVoyageCode3));
    assertThat(exception.getMessage())
        .contains("query model 'voyage-code-3' is not compatible with the index model 'voyage-4'");
  }

  @Test
  public void findEmbeddingModelName_voyageCode3CompatibleWithItself() throws Exception {
    // Test that voyage-code-3 query model IS compatible with voyage-code-3 index model
    var mocks = new Mocks();
    var queryWithVoyageCode3 =
        VectorQueryBuilder.builder()
            .index(MAT_VIEW_INDEX_NAME)
            .criteria(
                ApproximateVectorQueryCriteriaBuilder.builder()
                    .limit(LIMIT)
                    .numCandidates(NUM_CANDIDATES)
                    .query(
                        new VectorSearchQueryInput.Text("test query", Optional.of("voyage-code-3")))
                    .path(PATH)
                    .filter(getFilter())
                    .build())
            .build();
    var command = buildAutoEmbeddingVectorSearchCommandWithMocks(mocks, queryWithVoyageCode3);
    var modelName =
        command.findEmbeddingModelName(
            VectorIndexDefinitionBuilder.builder()
                .withAutoEmbedField(PATH.toString(), "voyage-code-3")
                .build(),
            queryWithVoyageCode3);
    // Should use the query-specified model (voyage-code-3) since it's compatible with itself
    assertThat(modelName).isPresent();
    Assert.assertEquals("voyage-code-3", modelName.get());
  }

  @Test
  public void findEmbeddingModelName_caseInsensitiveQueryModel() throws Exception {
    // Test that model names are case-insensitive (e.g., "VoyagE-4-Large" should work)
    var mocks = new Mocks();
    var queryWithMixedCase =
        VectorQueryBuilder.builder()
            .index(MAT_VIEW_INDEX_NAME)
            .criteria(
                ApproximateVectorQueryCriteriaBuilder.builder()
                    .limit(LIMIT)
                    .numCandidates(NUM_CANDIDATES)
                    .query(
                        new VectorSearchQueryInput.Text(
                            "test query", Optional.of("VoyagE-4-Large")))
                    .path(PATH)
                    .filter(getFilter())
                    .build())
            .build();
    var command = buildAutoEmbeddingVectorSearchCommandWithMocks(mocks, queryWithMixedCase);
    var modelName =
        command.findEmbeddingModelName(
            VectorIndexDefinitionBuilder.builder()
                .withAutoEmbedField(PATH.toString(), "voyage-4")
                .build(),
            queryWithMixedCase);
    // Should use the query-specified model (case preserved) since it's compatible
    assertThat(modelName).isPresent();
    Assert.assertEquals("VoyagE-4-Large", modelName.get());
  }

  @Test
  public void testAutoEmbeddingIndex_throwInvalidQueryException_withMultiModalQuery()
      throws Exception {
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
                                .query(
                                    new VectorSearchQueryInput.MultiModal(
                                        Optional.of("test query")))
                                .path(PATH.newChild("invalid_child"))
                                .filter(getFilter())
                                .build())
                        .build())
                .build(),
            mocks.catalog,
            mocks.initializedIndexCatalog,
            BOOTSTRAPPER_METADATA,
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
                    "Vector index for this text based query is invalid due to missing model")),
        result);
  }

  @Test
  public void testEmptyQueryText_throwsInvalidQueryException() throws Exception {
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
                                .query(new VectorSearchQueryInput.Text(""))
                                .path(PATH)
                                .filter(getFilter())
                                .build())
                        .build())
                .build(),
            mocks.catalog,
            mocks.initializedIndexCatalog,
            BOOTSTRAPPER_METADATA,
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
                new BsonString("'query' field cannot be empty for auto-embedding vector search")),
        result);
  }

  @Test
  public void testEmptyQueryText_withMultiModalQuery_throwsInvalidQueryException()
      throws Exception {
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
                                .query(new VectorSearchQueryInput.MultiModal(Optional.of("")))
                                .path(PATH)
                                .filter(getFilter())
                                .build())
                        .build())
                .build(),
            mocks.catalog,
            mocks.initializedIndexCatalog,
            BOOTSTRAPPER_METADATA,
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
                new BsonString("'query' field cannot be empty for auto-embedding vector search")),
        result);
  }

  @Test
  public void testEmptyQueryDocument_withMultiModalQuery_throwsInvalidQueryException()
      throws Exception {
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
                                .query(new VectorSearchQueryInput.MultiModal(Optional.empty()))
                                .path(PATH)
                                .filter(getFilter())
                                .build())
                        .build())
                .build(),
            mocks.catalog,
            mocks.initializedIndexCatalog,
            BOOTSTRAPPER_METADATA,
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
                new BsonString("'query' field cannot be empty for auto-embedding vector search")),
        result);
  }

  @Test
  public void testFailedEmbeddingInQueryText_throwsIllegalStateException() throws Exception {
    var mocks = new Mocks();
    var command =
        buildAutoEmbeddingVectorSearchCommandWithMocks(
            mocks,
            VectorQueryBuilder.builder()
                .index(INDEX_NAME)
                .criteria(
                    ApproximateVectorQueryCriteriaBuilder.builder()
                        .limit(LIMIT)
                        .numCandidates(NUM_CANDIDATES)
                        .query(new VectorSearchQueryInput.Text("test query"))
                        .path(PATH)
                        .filter(getFilter())
                        .build())
                .build(),
            mockEmbeddingServiceManager(new VectorOrError("Got bad request error")));

    var result = command.run();
    Assert.assertEquals(
        new BsonDocument()
            .append("ok", new BsonInt32(0))
            .append(
                "errmsg",
                new BsonString("Got error when embedding query: Optional[Got bad request error]")),
        result);
  }

  @Test
  public void testMaybeEmbed_withTextQuery_includesAutoEmbeddingFieldsMapping() throws Exception {
    // Verifies that maybeEmbed() includes autoEmbeddingFieldsMapping when using text query
    var mocks = new Mocks();
    var internalPath = FieldPath.parse("_autoEmbed.testPath");
    configureWithAutoEmbeddingFieldsMapping(mocks, Map.of(PATH, internalPath));

    var command =
        buildAutoEmbeddingVectorSearchCommandWithMocks(
            mocks,
            VectorQueryBuilder.builder()
                .index(INDEX_NAME)
                .criteria(
                    ApproximateVectorQueryCriteriaBuilder.builder()
                        .limit(LIMIT)
                        .numCandidates(NUM_CANDIDATES)
                        .query(new VectorSearchQueryInput.Text("test query"))
                        .path(PATH)
                        .filter(getFilter())
                        .build())
                .build());
    command.run();

    MaterializedVectorSearchQuery materializedQuery = captureMaterializedQuery(mocks.reader);
    assertThat(materializedQuery.autoEmbeddingFieldsMapping()).containsExactly(PATH, internalPath);
    // Verify materializedCriteria() has internal path and queryVector.
    var criteria = materializedQuery.materializedCriteria();
    assertThat(criteria.path()).isEqualTo(internalPath);
    assertThat(criteria.queryVector()).hasValue(QUERY_VECTOR);
    assertThat(criteria.query()).isEmpty();
  }

  @Test
  public void testMaybeEmbed_withQueryVector_includesAutoEmbeddingFieldsMapping() throws Exception {
    // Verifies that maybeEmbed() includes autoEmbeddingFieldsMapping when using queryVector
    var mocks = new Mocks();
    var internalPath = FieldPath.parse("_autoEmbed.testPath");
    configureWithAutoEmbeddingFieldsMapping(mocks, Map.of(PATH, internalPath));

    var command =
        buildAutoEmbeddingVectorSearchCommandWithMocks(
            mocks,
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
                .build());
    command.run();

    MaterializedVectorSearchQuery materializedQuery = captureMaterializedQuery(mocks.reader);
    assertThat(materializedQuery.autoEmbeddingFieldsMapping()).containsExactly(PATH, internalPath);
    // Verify materializedCriteria() has internal path, queryVector, and no text query
    var criteria = materializedQuery.materializedCriteria();
    assertThat(criteria.path()).isEqualTo(internalPath);
    assertThat(criteria.queryVector()).hasValue(QUERY_VECTOR);
    assertThat(criteria.query()).isEmpty();
  }

  /**
   * Configures a Mocks instance with an AutoEmbeddingIndexGeneration that has the given
   * autoEmbeddingFieldsMapping. This overrides the catalog stubs to return the auto-embedding
   * index.
   */
  private static void configureWithAutoEmbeddingFieldsMapping(
      Mocks mocks, Map<FieldPath, FieldPath> autoEmbeddingFieldsMapping) {
    var schemaMetadata =
        new MaterializedViewCollectionMetadata.MaterializedViewSchemaMetadata(
            1, autoEmbeddingFieldsMapping);
    var matViewGenId =
        new MaterializedViewGenerationId(
            MOCK_INDEX_GENERATION_ID.indexId, new MaterializedViewGeneration(Generation.CURRENT));
    var vectorDef =
        VectorIndexDefinitionBuilder.builder().withAutoEmbedField(PATH.toString()).build();

    // InitializedMaterializedViewIndex - the actual index that gets queried
    var matViewIndex = Mockito.mock(InitializedMaterializedViewIndex.class);
    when(matViewIndex.getSchemaMetadata()).thenReturn(schemaMetadata);
    when(matViewIndex.getReader()).thenReturn(mocks.reader);
    when(matViewIndex.getDefinition()).thenReturn(vectorDef);

    // MaterializedViewIndexGeneration - wraps the initialized index
    var matViewIndexGeneration = Mockito.mock(MaterializedViewIndexGeneration.class);
    when(matViewIndexGeneration.getIndex()).thenReturn(matViewIndex);

    // AutoEmbeddingIndexGeneration - the composite index returned by catalog
    var autoEmbeddingIndexGeneration = Mockito.mock(AutoEmbeddingIndexGeneration.class);
    when(autoEmbeddingIndexGeneration.getType()).thenCallRealMethod();
    when(autoEmbeddingIndexGeneration.getMaterializedViewIndexGeneration())
        .thenReturn(matViewIndexGeneration);
    when(autoEmbeddingIndexGeneration.getGenerationId()).thenReturn(matViewGenId);
    when(autoEmbeddingIndexGeneration.getIndex()).thenReturn(matViewIndex);

    // Override catalog stubs
    when(mocks.catalog.getIndex(DATABASE_NAME, COLLECTION_UUID, Optional.empty(), INDEX_NAME))
        .thenReturn(Optional.of(autoEmbeddingIndexGeneration));
    when(mocks.initializedIndexCatalog.getIndex(matViewGenId))
        .thenReturn(Optional.of(matViewIndex));
  }

  private static MaterializedVectorSearchQuery captureMaterializedQuery(
      LuceneVectorIndexReader reader) throws Exception {
    ArgumentCaptor<MaterializedVectorSearchQuery> captor =
        ArgumentCaptor.forClass(MaterializedVectorSearchQuery.class);
    Mockito.verify(reader).query(captor.capture());
    return captor.getValue();
  }

  private static VectorSearchCommand buildAutoEmbeddingVectorSearchCommandWithMocks(
      Mocks mocks, VectorSearchQuery vectorSearchQuery) {
    return buildAutoEmbeddingVectorSearchCommandWithMocks(
        mocks, vectorSearchQuery, MOCK_EMBEDDING_SERVICE);
  }

  private static VectorSearchCommand buildAutoEmbeddingVectorSearchCommandWithMocks(
      Mocks mocks,
      VectorSearchQuery vectorSearchQuery,
      Supplier<EmbeddingServiceManager> embeddingServiceManagerSupplier) {
    return new VectorSearchCommand(
        VectorSearchCommandDefinitionBuilder.builder()
            .db(DATABASE_NAME)
            .collectionName(COLLECTION_NAME)
            .collectionUuid(COLLECTION_UUID)
            .vectorSearchQuery(vectorSearchQuery)
            .build(),
        mocks.catalog,
        mocks.initializedIndexCatalog,
        BOOTSTRAPPER_METADATA,
        embeddingServiceManagerSupplier,
        new VectorSearchCommand.Metrics(mockMetricsFactory()),
        mocks.cursorManager,
        CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT);
  }

  private static VectorSearchCommand
      buildAutoEmbeddingVectorSearchCommandWithMocksWithMultiModalQuery(Mocks mocks)
          throws BsonParseException {
    return new VectorSearchCommand(
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
                            .query(new VectorSearchQueryInput.MultiModal(Optional.of("test query")))
                            .path(PATH)
                            .filter(getFilter())
                            .build())
                    .build())
            .build(),
        mocks.catalog,
        mocks.initializedIndexCatalog,
        BOOTSTRAPPER_METADATA,
        MOCK_EMBEDDING_SERVICE,
        new VectorSearchCommand.Metrics(mockMetricsFactory()),
        mocks.cursorManager,
        CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT);
  }

  private static Supplier<EmbeddingServiceManager> mockEmbeddingServiceManager(
      VectorOrError vectorOrError) {
    setupRegistry();
    EmbeddingServiceManager manager = mock(EmbeddingServiceManager.class);
    doReturn(List.of(vectorOrError))
        .when(manager)
        .embed(
            Mockito.anyList(),
            any(EmbeddingModelConfig.class),
            any(EmbeddingServiceConfig.ServiceTier.class),
            any(EmbeddingRequestContext.class));
    return Suppliers.ofInstance(manager);
  }

  private static void setupRegistry() {
    EmbeddingModelCatalog.registerModelConfig(
        "voyage-3-large",
        EmbeddingModelConfig.create(
            "voyage-3-large",
            EmbeddingServiceConfig.EmbeddingProvider.VOYAGE,
            new EmbeddingServiceConfig.EmbeddingConfig(
                Optional.of("us-east-1"),
                new EmbeddingServiceConfig.VoyageModelConfig(
                    Optional.of(512),
                    Optional.of(EmbeddingServiceConfig.TruncationOption.START),
                    Optional.of(100),
                    Optional.of(1000)),
                new EmbeddingServiceConfig.ErrorHandlingConfig(50, 50L, 10L, 0.1),
                new EmbeddingServiceConfig.VoyageEmbeddingCredentials(
                    "token123", "2024-10-15T22:32:20.925Z"),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                true,
                Optional.empty(),
                false,
                Optional.empty())));

    // Register compatible models for voyage-4 family (all interchangeable)
    // Note: Each model is automatically compatible with itself, so we only list OTHER compatible
    // models
    Set<String> voyage4OtherCompatibleModels = Set.of("voyage-4-large", "voyage-4-lite");
    EmbeddingModelCatalog.registerCompatibleModels("voyage-4", voyage4OtherCompatibleModels);
    EmbeddingModelCatalog.registerCompatibleModels(
        "voyage-4-large", Set.of("voyage-4", "voyage-4-lite"));
    EmbeddingModelCatalog.registerCompatibleModels(
        "voyage-4-lite", Set.of("voyage-4", "voyage-4-large"));

    // Register compatible models for voyage-code-3 (only compatible with itself - empty set)
    EmbeddingModelCatalog.registerCompatibleModels("voyage-code-3", Set.of());

    // Register compatible models for voyage-3 (only compatible with itself - empty set, for
    // testing)
    EmbeddingModelCatalog.registerCompatibleModels("voyage-3", Set.of());
  }

  private static class Mocks {

    final IndexCatalog catalog;
    final InitializedIndexCatalog initializedIndexCatalog;
    final IndexMetricsUpdater metricsUpdater;
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
              VectorIndexDefinitionBuilder.builder().withTextField(PATH.toString()).build());
      lenient().when(index.isCompatibleWith(any(VectorIndexDefinition.class))).thenReturn(true);
      lenient().when(index.asVectorIndex()).thenCallRealMethod();
      lenient().when(initializedIndex.asVectorIndex()).thenCallRealMethod();
      this.catalog = Mockito.mock(IndexCatalog.class);
      this.initializedIndexCatalog = Mockito.mock(InitializedIndexCatalog.class);
      IndexGeneration indexGeneration1 = mock(IndexGeneration.class);
      when(indexGeneration1.getGenerationId())
          .thenReturn(MOCK_INDEX_DEFINITION_GENERATION.getGenerationId());
      when(indexGeneration1.getDefinition())
          .thenReturn(
              VectorIndexDefinitionBuilder.builder()
                  .withDotProductVectorField(PATH.toString(), 1024)
                  .build());
      lenient().when(indexGeneration1.getIndex()).thenReturn(index);
      // Change to use initialized index.
      when(this.catalog.getIndex(DATABASE_NAME, COLLECTION_UUID, Optional.empty(), INDEX_NAME))
          .thenReturn(Optional.of(indexGeneration1));

      IndexGeneration indexGeneration2 = mock(IndexGeneration.class);
      when(indexGeneration2.getDefinition())
          .thenReturn(
              VectorIndexDefinitionBuilder.builder().withAutoEmbedField(PATH.toString()).build());
      lenient().when(indexGeneration2.getIndex()).thenReturn(index);
      // throwIfUnavailableForQuerying() is called - we don't mock it to test real behavior
      when(this.catalog.getIndex(
              DATABASE_NAME, COLLECTION_UUID, Optional.empty(), MAT_VIEW_INDEX_NAME))
          .thenReturn(Optional.of(indexGeneration2));

      when(this.initializedIndexCatalog.getIndex(MOCK_INDEX_GENERATION_ID))
          .thenReturn(Optional.of(initializedIndex));
      this.reader = Mockito.mock(LuceneVectorIndexReader.class);
      when(initializedIndex.getReader()).thenReturn(this.reader);

      this.bsonResults = new VectorSearchResultBatch(4).getBsonResults();
      when(this.reader.query(any())).thenReturn(this.bsonResults);

      this.metricsUpdater =
          IndexMetricsUpdaterBuilder.builder()
              .metricsFactory(SearchIndex.mockMetricsFactory())
              .indexMetricsSupplier(Mockito.mock(IndexMetricValuesSupplier.class))
              .build();
      when(initializedIndex.getMetricsUpdater()).thenReturn(this.metricsUpdater);

      this.cursorManager = mock(MongotCursorManager.class);
    }
  }

  @Test
  public void testAutoEmbeddingVectorSearchQueryRunningCount() throws Exception {
    testAutoEmbeddingVectorSearchRunningCountHelper(false);
    testAutoEmbeddingVectorSearchRunningCountHelper(true);
  }

  @Test
  public void testAutoEmbeddingVectorSearchQueryRunningCount_withMultiModalQuery()
      throws Exception {
    testAutoEmbeddingVectorSearchRunningCountHelperWithMultiModalQuery(false);
    testAutoEmbeddingVectorSearchRunningCountHelperWithMultiModalQuery(true);
  }

  private void testAutoEmbeddingVectorSearchRunningCountHelper(boolean throwException)
      throws Exception {
    var mocks = new Mocks();

    var queryCountDown = new CountDownLatch(1);
    var testCountDown = new CountDownLatch(1);

    when(mocks.reader.query(any()))
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
        VectorQueryBuilder.builder()
            .index(INDEX_NAME)
            .criteria(
                ApproximateVectorQueryCriteriaBuilder.builder()
                    .limit(LIMIT)
                    .numCandidates(NUM_CANDIDATES)
                    .query(new VectorSearchQueryInput.Text("test query"))
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
            BOOTSTRAPPER_METADATA,
            MOCK_EMBEDDING_SERVICE,
            new VectorSearchCommand.Metrics(mockMetricsFactory),
            mocks.cursorManager,
            CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT);

    var result =
        CompletableFuture.supplyAsync(() -> command.run(), Executors.newSingleThreadExecutor());

    // wait for the query thread reaching synchronization point.
    testCountDown.await();
    // validate the running count
    Assert.assertEquals(
        1, mockMetricsFactory.get("concurrentAutoEmbeddingQueries").gauge().value(), 0);
    // Signal the query thread proceed.
    queryCountDown.countDown();
    result.get();
    Assert.assertEquals(
        0, mockMetricsFactory.get("concurrentAutoEmbeddingQueries").gauge().value(), 0);
  }

  private void testAutoEmbeddingVectorSearchRunningCountHelperWithMultiModalQuery(
      boolean throwException) throws Exception {
    var mocks = new Mocks();

    var queryCountDown = new CountDownLatch(1);
    var testCountDown = new CountDownLatch(1);

    when(mocks.reader.query(any()))
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
        VectorQueryBuilder.builder()
            .index(INDEX_NAME)
            .criteria(
                ApproximateVectorQueryCriteriaBuilder.builder()
                    .limit(LIMIT)
                    .numCandidates(NUM_CANDIDATES)
                    .query(new VectorSearchQueryInput.MultiModal(Optional.of("test query")))
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
            BOOTSTRAPPER_METADATA,
            MOCK_EMBEDDING_SERVICE,
            new VectorSearchCommand.Metrics(mockMetricsFactory),
            mocks.cursorManager,
            CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT);

    var result =
        CompletableFuture.supplyAsync(() -> command.run(), Executors.newSingleThreadExecutor());

    // wait for the query thread reaching synchronization point.
    testCountDown.await();
    // validate the running count
    Assert.assertEquals(
        1, mockMetricsFactory.get("concurrentAutoEmbeddingQueries").gauge().value(), 0);
    // Signal the query thread proceed.
    queryCountDown.countDown();
    result.get();
    Assert.assertEquals(
        0, mockMetricsFactory.get("concurrentAutoEmbeddingQueries").gauge().value(), 0);
  }

  private static MetricsFactory mockMetricsFactory() {
    return new MetricsFactory("mockNamespace", new SimpleMeterRegistry());
  }
}
