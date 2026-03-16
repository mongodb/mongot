package com.xgen.mongot.server.command.search;

import static com.xgen.mongot.index.definition.IndexDefinition.Fields.NUM_PARTITIONS;
import static com.xgen.mongot.util.Check.checkState;
import static com.xgen.mongot.util.bson.FloatVector.OriginalType.NATIVE;
import static com.xgen.testing.mongot.mock.cursor.MongotCursorBatches.MOCK_SEARCH_CURSOR_ID;
import static com.xgen.testing.mongot.mock.cursor.MongotCursorBatches.mockInitialMongotCursorBatchForVectorSearch;
import static com.xgen.testing.mongot.mock.cursor.MongotCursorBatches.mockMongotCursorBatchForVectorSearch;
import static com.xgen.testing.mongot.mock.index.IndexGeneration.mockDefinitionGeneration;
import static com.xgen.testing.mongot.mock.index.IndexGeneration.mockVectorIndexGeneration;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.truth.Truth;
import com.xgen.mongot.catalog.DefaultIndexCatalog;
import com.xgen.mongot.catalog.IndexCatalog;
import com.xgen.mongot.catalog.InitializedIndexCatalog;
import com.xgen.mongot.cursor.CursorConfig;
import com.xgen.mongot.cursor.MongotCursorManager;
import com.xgen.mongot.cursor.MongotCursorNotFoundException;
import com.xgen.mongot.cursor.MongotCursorResultInfo;
import com.xgen.mongot.cursor.QueryBatchTimerRecorder;
import com.xgen.mongot.cursor.SearchCursorInfo;
import com.xgen.mongot.cursor.serialization.MongotCursorBatch;
import com.xgen.mongot.cursor.serialization.MongotCursorResult;
import com.xgen.mongot.featureflag.FeatureFlags;
import com.xgen.mongot.index.CountResult;
import com.xgen.mongot.index.IndexGeneration;
import com.xgen.mongot.index.IndexMetricValuesSupplier;
import com.xgen.mongot.index.IndexMetricsUpdater;
import com.xgen.mongot.index.InitializedSearchIndex;
import com.xgen.mongot.index.InitializedVectorIndex;
import com.xgen.mongot.index.MetaResults;
import com.xgen.mongot.index.definition.IndexDefinitionGeneration;
import com.xgen.mongot.index.lucene.explain.information.LuceneQuerySpecification;
import com.xgen.mongot.index.lucene.explain.information.MetadataExplainInformation;
import com.xgen.mongot.index.lucene.explain.information.SearchExplainInformation;
import com.xgen.mongot.index.lucene.explain.information.SearchExplainInformationBuilder;
import com.xgen.mongot.index.lucene.explain.tracing.Explain;
import com.xgen.mongot.index.query.operators.VectorSearchFilter;
import com.xgen.mongot.index.query.operators.mql.Clause;
import com.xgen.mongot.index.status.IndexStatus;
import com.xgen.mongot.metrics.MeterAndFtdcRegistry;
import com.xgen.mongot.metrics.PerIndexMetricsFactory;
import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.bson.Vector;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.mongodb.MongoDbServerInfo;
import com.xgen.mongot.util.mongodb.MongoDbVersion;
import com.xgen.testing.mongot.index.definition.SearchIndexDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.VectorIndexDefinitionBuilder;
import com.xgen.testing.mongot.index.lucene.explain.information.DefaultQueryBuilder;
import com.xgen.testing.mongot.index.lucene.explain.information.QueryExplainInformationBuilder;
import com.xgen.testing.mongot.index.lucene.explain.tracing.FakeExplain;
import com.xgen.testing.mongot.index.query.ApproximateVectorQueryCriteriaBuilder;
import com.xgen.testing.mongot.index.query.VectorQueryBuilder;
import com.xgen.testing.mongot.index.query.operators.mql.ClauseBuilder;
import com.xgen.testing.mongot.index.query.operators.mql.MqlFilterOperatorBuilder;
import com.xgen.testing.mongot.index.query.operators.mql.ValueBuilder;
import com.xgen.testing.mongot.mock.index.VectorIndex;
import com.xgen.testing.mongot.server.command.search.definition.request.ExplainDefinitionBuilder;
import com.xgen.testing.mongot.server.command.search.definition.request.VectorSearchCommandDefinitionBuilder;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.theories.DataPoints;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.runner.RunWith;
import org.mockito.Mockito;

@RunWith(Theories.class)
public class DeprecatedVectorSearchCommandTest {
  private static final String DATABASE_NAME = "testDb";
  private static final String COLLECTION_NAME = "testCollection";
  private static final UUID COLLECTION_UUID = UUID.randomUUID();
  private static final String INDEX_NAME = "default";
  private static final int NUM_CANDIDATES = 5000;
  private static final int LIMIT = 1000;
  private static final Vector QUERY_VECTOR = Vector.fromFloats(new float[] {1f, 2f, 3f}, NATIVE);
  private static final FieldPath PATH = FieldPath.parse("testPath");
  private static final SearchCommandsRegister.BootstrapperMetadata BOOTSTRAPPER_METADATA =
      new SearchCommandsRegister.BootstrapperMetadata(
          "testVersion", "localhost", () -> MongoDbServerInfo.EMPTY, FeatureFlags.getDefault());
  private static final String RSID = "atlas-xyz";

  private static final double COUNTER_TOL = 1e-5;
  private static final BsonDocument INVALID_QUERY_RESPONSE =
      new BsonDocument()
          .append("ok", new BsonInt32(0))
          .append(
              "errmsg",
              new BsonString(
                  String.format(
                      "\""
                          + PATH.toString()
                          + "\" "
                          + "limit should be less than or equal to numCandidates")));
  private static final BsonDocument IO_ERROR_RESPONSE =
      new BsonDocument()
          .append("ok", new BsonInt32(0))
          .append("errmsg", new BsonString("IO error"));

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

  @DataPoints
  public static final List<MongoDbVersion> mdbVersions =
      List.of(new MongoDbVersion(8, 1, 0), new MongoDbVersion(7, 0, 0));

  @Test
  public void testValidIndex() throws Exception {
    DeprecatedVectorSearchCommand command =
        new DeprecatedVectorSearchCommand(
            getCursorManager(),
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
            CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT,
            new DefaultIndexCatalog(),
            getInitializedIndexCatalog(),
            mock(VectorSearchCommand.Factory.class),
            BOOTSTRAPPER_METADATA);

    BsonDocument result = command.run();
    BsonDocument expected = mockInitialMongotCursorBatchForVectorSearch().toBson();
    Assert.assertEquals(expected, result);
    Assert.assertEquals(Arrays.asList(MOCK_SEARCH_CURSOR_ID), command.getCreatedCursorIds());
  }

  @Test
  public void testRegistersMetrics() throws Exception {
    AtomicInteger metricUpdateCount = new AtomicInteger(0);
    MongotCursorManager manager =
        getCursorManager((cursorId) -> metricUpdateCount.incrementAndGet());
    DeprecatedVectorSearchCommand command =
        new DeprecatedVectorSearchCommand(
            manager,
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
            CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT,
            new DefaultIndexCatalog(),
            getInitializedIndexCatalog(),
            mock(VectorSearchCommand.Factory.class),
            BOOTSTRAPPER_METADATA);

    command.run();
    checkState(
        metricUpdateCount.get() > 0,
        "vectorSearchCommand should have called accept on the metrics updater.");
  }

  @Test
  public void testInvalidQuery() throws Exception {
    DeprecatedVectorSearchCommand command =
        new DeprecatedVectorSearchCommand(
            getCursorManager(),
            VectorSearchCommandDefinitionBuilder.builder()
                .db(DATABASE_NAME)
                .collectionName(COLLECTION_NAME)
                .collectionUuid(COLLECTION_UUID)
                .vectorSearchQuery(
                    new BsonParseException(
                        "limit should be less than or equal to numCandidates", Optional.of(PATH)))
                .build(),
            CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT,
            new DefaultIndexCatalog(),
            getInitializedIndexCatalog(),
            mock(VectorSearchCommand.Factory.class),
            BOOTSTRAPPER_METADATA);

    BsonDocument result = command.run();
    Assert.assertEquals(INVALID_QUERY_RESPONSE, result);
  }

  @Test
  public void testIoExceptionVectorSearchCommand() throws Exception {
    DeprecatedVectorSearchCommand command =
        new DeprecatedVectorSearchCommand(
            getIoExceptionCursorManager(),
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
            CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT,
            new DefaultIndexCatalog(),
            new InitializedIndexCatalog(),
            mock(VectorSearchCommand.Factory.class),
            BOOTSTRAPPER_METADATA);

    BsonDocument result = command.run();
    Assert.assertEquals(IO_ERROR_RESPONSE, result);
  }

  @Theory
  public void searchCommand_executeExplain_passes(MongoDbVersion mdbVersion) throws Exception {
    var explainInformation =
        SearchExplainInformationBuilder.newBuilder()
            .queryExplainInfos(
                List.of(
                    QueryExplainInformationBuilder.builder()
                        .type(LuceneQuerySpecification.Type.DEFAULT_QUERY)
                        .args(DefaultQueryBuilder.builder().queryType("DocAndScoreQuery").build())
                        .build()))
            .build();

    var cursorManager = getCursorManager(explainInformation);

    DeprecatedVectorSearchCommand command =
        new DeprecatedVectorSearchCommand(
            cursorManager,
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
            CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT,
            new DefaultIndexCatalog(),
            getInitializedIndexCatalog(),
            mock(VectorSearchCommand.Factory.class),
            new SearchCommandsRegister.BootstrapperMetadata(
                "local",
                "localhost",
                () -> new MongoDbServerInfo(Optional.of(mdbVersion), Optional.of(RSID)),
                FeatureFlags.getDefault()));

    try (var unused =
        FakeExplain.setup(
            Explain.Verbosity.EXECUTION_STATS,
            NUM_PARTITIONS.getDefaultValue(),
            explainInformation)) {
      BsonDocument result = command.run();
      MongotCursorBatch batch = MongotCursorBatch.fromBson(result);
      Assert.assertTrue(batch.explain().isPresent());
      Assert.assertEquals(batch.explain().get(), explainInformation);

      if (mdbVersion.compareTo(Explain.FIRST_VERSION_KILLS_CURSORS_EXPLAIN) < 0) {
        Assert.assertTrue(batch.cursor().isEmpty());
        verify(cursorManager, times(1)).killCursor(anyLong());
      } else {
        Assert.assertTrue(batch.cursor().isPresent());
        verify(cursorManager, never()).killCursor(anyLong());
      }
    }
  }

  @Test
  public void testExplainQueryPlannerNoCursor() throws Exception {
    var explainInformation =
        SearchExplainInformationBuilder.newBuilder()
            .queryExplainInfos(
                List.of(
                    QueryExplainInformationBuilder.builder()
                        .type(LuceneQuerySpecification.Type.DEFAULT_QUERY)
                        .args(DefaultQueryBuilder.builder().queryType("DocAndScoreQuery").build())
                        .build()))
            .build();

    var cursorManager = getCursorManager(explainInformation);

    DeprecatedVectorSearchCommand command =
        new DeprecatedVectorSearchCommand(
            cursorManager,
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
            CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT,
            new DefaultIndexCatalog(),
            getInitializedIndexCatalog(),
            mock(VectorSearchCommand.Factory.class),
            BOOTSTRAPPER_METADATA);

    try (var unused =
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

    verify(cursorManager, times(1)).killCursor(MOCK_SEARCH_CURSOR_ID);
  }

  @Test
  public void testExplainRecordsMetadata() throws Exception {
    var cursorManager = getCursorManager();

    DeprecatedVectorSearchCommand command =
        new DeprecatedVectorSearchCommand(
            cursorManager,
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
            CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT,
            new DefaultIndexCatalog(),
            getInitializedIndexCatalog(),
            mock(VectorSearchCommand.Factory.class),
            new SearchCommandsRegister.BootstrapperMetadata(
                "1.0", "foo", () -> MongoDbServerInfo.EMPTY, FeatureFlags.getDefault()));

    try (var unused =
        Explain.setup(
            Optional.of(Explain.Verbosity.QUERY_PLANNER),
            Optional.of(NUM_PARTITIONS.getDefaultValue()))) {
      command.run();

      var metadata = Explain.collect().get().metadata().get();
      Truth.assertThat(metadata)
          .isEqualTo(
              new MetadataExplainInformation(
                  Optional.of("1.0"),
                  Optional.of("foo"),
                  Optional.of(INDEX_NAME),
                  Optional.empty(),
                  Optional.empty(),
                  Optional.empty()));
    }
  }

  @Test
  public void testQueryAgainstVectorIndexIsDelegatedToAnotherCommand() throws Exception {
    var catalog = Mockito.mock(IndexCatalog.class);
    IndexGeneration indexGeneration = mockVectorIndexGeneration();
    when(catalog.getIndex(DATABASE_NAME, COLLECTION_UUID, Optional.empty(), INDEX_NAME))
        .thenReturn(Optional.of(indexGeneration));
    InitializedIndexCatalog initializedIndexCatalog = getInitializedIndexCatalog();
    var anotherCommandFactory = mock(VectorSearchCommand.Factory.class);
    var definition =
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
            .build();

    DeprecatedVectorSearchCommand command =
        new DeprecatedVectorSearchCommand(
            getCursorManager(),
            definition,
            CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT,
            catalog,
            initializedIndexCatalog,
            anotherCommandFactory,
            BOOTSTRAPPER_METADATA);

    IndexMetricsUpdater.QueryingMetricsUpdater queryingMetricsUpdater =
        initializedIndexCatalog
            .getIndex(indexGeneration.getGenerationId())
            .get()
            .getMetricsUpdater()
            .getQueryingMetricsUpdater();
    Assert.assertEquals(
        0, queryingMetricsUpdater.getVectorSearchQueriesOverSearchIndexes().count(), COUNTER_TOL);

    command.run();

    Assert.assertEquals(
        0, queryingMetricsUpdater.getVectorSearchQueriesOverSearchIndexes().count(), COUNTER_TOL);

    Mockito.verify(anotherCommandFactory).create(definition);
  }

  @Test
  public void testValidQueryAgainstSearchIndexIfNoVectorIndex() throws Exception {
    var catalog = Mockito.mock(IndexCatalog.class);
    var initializedIndexCatalog = Mockito.mock(InitializedIndexCatalog.class);
    IndexGeneration indexGeneration = mockVectorIndexGeneration();
    var initializedIndex = Mockito.mock(InitializedSearchIndex.class);
    when(initializedIndex.getType()).thenReturn(IndexDefinitionGeneration.Type.SEARCH);
    when(initializedIndex.getDefinition()).thenReturn(SearchIndexDefinitionBuilder.VALID_INDEX);
    var meterAndFtdcRegistry = MeterAndFtdcRegistry.createWithSimpleRegistries();
    PerIndexMetricsFactory metricsFactory =
        new PerIndexMetricsFactory(
            "testNamespace", meterAndFtdcRegistry, "testGenerationId", "testId");
    var indexMetricsUpdaterBuilder =
        new IndexMetricsUpdater.Builder(indexGeneration.getDefinition(), metricsFactory, true);
    var indexMetricsSupplier = Mockito.mock(IndexMetricValuesSupplier.class);
    IndexMetricsUpdater indexMetricsUpdater =
        indexMetricsUpdaterBuilder.build(indexMetricsSupplier);
    when(initializedIndex.getMetricsUpdater()).thenReturn(indexMetricsUpdater);
    var indexDefinition = indexGeneration.getDefinition();
    when(catalog.getIndex(
            indexDefinition.getDatabase(),
            indexDefinition.getCollectionUuid(),
            Optional.empty(),
            indexDefinition.getName()))
        .thenReturn(Optional.of(indexGeneration));
    when(initializedIndexCatalog.getIndex(indexGeneration.getGenerationId()))
        .thenReturn(Optional.of(initializedIndex));

    var definition =
        VectorSearchCommandDefinitionBuilder.builder()
            .db(indexDefinition.getDatabase())
            .collectionName(indexDefinition.getLastObservedCollectionName())
            .collectionUuid(indexDefinition.getCollectionUuid())
            .vectorSearchQuery(
                VectorQueryBuilder.builder()
                    .index(indexDefinition.getName())
                    .criteria(
                        ApproximateVectorQueryCriteriaBuilder.builder()
                            .limit(LIMIT)
                            .numCandidates(NUM_CANDIDATES)
                            .queryVector(QUERY_VECTOR)
                            .path(PATH)
                            .filter(getFilter())
                            .build())
                    .build())
            .build();

    DeprecatedVectorSearchCommand command =
        new DeprecatedVectorSearchCommand(
            getCursorManager(),
            definition,
            CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT,
            catalog,
            initializedIndexCatalog,
            mock(VectorSearchCommand.Factory.class),
            BOOTSTRAPPER_METADATA);

    BsonDocument result = command.run();
    BsonDocument expected = mockInitialMongotCursorBatchForVectorSearch().toBson();
    Assert.assertEquals(expected, result);
    Assert.assertEquals(List.of(MOCK_SEARCH_CURSOR_ID), command.getCreatedCursorIds());
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

    var definition =
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
            .build();

    DeprecatedVectorSearchCommand command =
        new DeprecatedVectorSearchCommand(
            getCursorManager(),
            definition,
            CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT,
            catalog,
            initializedIndexCatalog,
            mock(VectorSearchCommand.Factory.class),
            BOOTSTRAPPER_METADATA);

    BsonDocument result = command.run();
    Assert.assertEquals(0, result.getInt32("ok").getValue());
    Assert.assertTrue(result.containsKey("errmsg"));
    Assert.assertEquals(
        String.format("Index %s not initialized", INDEX_NAME),
        result.getString("errmsg").getValue());
  }

  private static MongotCursorManager getCursorManager() throws Exception {
    return getCursorManager((timer) -> {});
  }

  private static MongotCursorManager getCursorManager(SearchExplainInformation explain)
      throws Exception {
    return getCursorManager((timer) -> {}, Optional.of(explain));
  }

  private static MongotCursorManager getCursorManager(QueryBatchTimerRecorder metricRecorder)
      throws Exception {
    return getCursorManager(metricRecorder, Optional.empty());
  }

  /**
   * Creates a MongotCursorManager that behaves in a way that can illicit the desired behavior from
   * VectorSearchCommand.
   */
  private static MongotCursorManager getCursorManager(
      QueryBatchTimerRecorder metricRecorder, Optional<SearchExplainInformation> explainInformation)
      throws Exception {
    MongotCursorManager cursorManager = mock(MongotCursorManager.class);

    MongotCursorBatch batch = mockMongotCursorBatchForVectorSearch();

    // Only return a cursor batch for the correct cursor id.
    when(cursorManager.newCursor(any(), any(), any(), any(), any(), any(), any(), any()))
        .then(
            invocation ->
                new SearchCursorInfo(
                    MOCK_SEARCH_CURSOR_ID, new MetaResults(CountResult.lowerBoundCount(1000))));

    when(cursorManager.getNextBatch(anyLong(), any(), any()))
        .then(
            invocation -> {
              long cursorId = invocation.getArgument(0);
              if (cursorId != MOCK_SEARCH_CURSOR_ID) {
                throw new MongotCursorNotFoundException(cursorId);
              }

              return new MongotCursorResultInfo(
                  batch.getCursorExpected().getCursorId() == MongotCursorResult.EXHAUSTED_CURSOR_ID,
                  batch.getCursorExpected().getBatch(),
                  explainInformation,
                  batch.getCursorExpected().getNamespace());
            });

    when(cursorManager.getIndexQueryBatchTimerRecorder(anyLong())).thenReturn(metricRecorder);

    return cursorManager;
  }

  /** Creates a MongotCursorManager that throws an IOException when getting the batch. */
  private MongotCursorManager getIoExceptionCursorManager() throws Exception {
    MongotCursorManager cursorManager = mock(MongotCursorManager.class);
    when(cursorManager.newCursor(any(), any(), any(), any(), any(), any(), any(), any()))
        .thenReturn(
            new SearchCursorInfo(
                MOCK_SEARCH_CURSOR_ID, new MetaResults(CountResult.lowerBoundCount(1000))));

    when(cursorManager.getNextBatch(anyLong(), any(), any()))
        .thenThrow(new IOException("IO error"));
    when(cursorManager.getIndexQueryBatchTimerRecorder(anyLong()))
        .thenReturn(
            (timer) -> {
              // no-op QueryBatchTimerRecorder
            });

    return cursorManager;
  }

  private InitializedIndexCatalog getInitializedIndexCatalog() {
    var index = Mockito.mock(InitializedVectorIndex.class);
    var meterAndFtdcRegistry = MeterAndFtdcRegistry.createWithSimpleRegistries();
    PerIndexMetricsFactory metricsFactory =
        new PerIndexMetricsFactory(
            "testNamespace", meterAndFtdcRegistry, "testGenerationId", "testId");
    var indexMetricsUpdaterBuilder =
        new IndexMetricsUpdater.Builder(
            mockVectorIndexGeneration().getDefinition(), metricsFactory, true);
    var indexMetricsSupplier = Mockito.mock(IndexMetricValuesSupplier.class);
    IndexMetricsUpdater indexMetricsUpdater =
        indexMetricsUpdaterBuilder.build(indexMetricsSupplier);
    when(index.getType()).thenReturn(IndexDefinitionGeneration.Type.VECTOR);
    when(index.getMetricsUpdater()).thenReturn(indexMetricsUpdater);

    InitializedIndexCatalog initializedIndexCatalog = mock(InitializedIndexCatalog.class);
    when(initializedIndexCatalog.getIndex(any())).thenReturn(Optional.of(index));
    return initializedIndexCatalog;
  }
}
