package com.xgen.mongot.server.command.search;

import static com.xgen.mongot.index.definition.IndexDefinition.Fields.NUM_PARTITIONS;
import static com.xgen.mongot.util.Check.checkState;
import static com.xgen.testing.mongot.mock.cursor.MongotCursorBatches.MOCK_META_CURSOR_ID;
import static com.xgen.testing.mongot.mock.cursor.MongotCursorBatches.MOCK_SEARCH_CURSOR_ID;
import static com.xgen.testing.mongot.mock.cursor.MongotCursorBatches.mockInitialMongotCursorBatch;
import static com.xgen.testing.mongot.mock.cursor.MongotCursorBatches.mockIntermediateMongotCursorBatch;
import static com.xgen.testing.mongot.mock.index.IndexGeneration.mockDefinitionGeneration;
import static com.xgen.testing.mongot.mock.index.IndexGeneration.mockIndexGeneration;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.truth.Correspondence;
import com.google.common.truth.Truth;
import com.xgen.mongot.catalog.DefaultIndexCatalog;
import com.xgen.mongot.catalog.IndexCatalog;
import com.xgen.mongot.catalog.InitializedIndexCatalog;
import com.xgen.mongot.config.backup.JournalEntryIdentity;
import com.xgen.mongot.config.manager.IncompatibleIndexes;
import com.xgen.mongot.config.manager.QuarantinedIndex;
import com.xgen.mongot.cursor.CursorConfig;
import com.xgen.mongot.cursor.IntermediateSearchCursorInfo;
import com.xgen.mongot.cursor.MongotCursorManager;
import com.xgen.mongot.cursor.MongotCursorNotFoundException;
import com.xgen.mongot.cursor.MongotCursorResultInfo;
import com.xgen.mongot.cursor.QueryBatchTimerRecorder;
import com.xgen.mongot.cursor.SearchCursorInfo;
import com.xgen.mongot.cursor.serialization.MongotCursorBatch;
import com.xgen.mongot.cursor.serialization.MongotCursorResult;
import com.xgen.mongot.cursor.serialization.MongotIntermediateCursorBatch;
import com.xgen.mongot.featureflag.Feature;
import com.xgen.mongot.featureflag.FeatureFlags;
import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlagConfig;
import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlagRegistry;
import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlags;
import com.xgen.mongot.index.CountResult;
import com.xgen.mongot.index.IndexGeneration;
import com.xgen.mongot.index.IndexUnavailableException;
import com.xgen.mongot.index.InitialSyncIndexUnavailableException;
import com.xgen.mongot.index.InitializedIndex;
import com.xgen.mongot.index.InitializedSearchIndex;
import com.xgen.mongot.index.MetaResults;
import com.xgen.mongot.index.definition.IndexDefinitionGeneration;
import com.xgen.mongot.index.definition.SearchIndexCapabilities;
import com.xgen.mongot.index.definition.SearchIndexDefinition;
import com.xgen.mongot.index.lucene.explain.explainers.AggregationFeatureExplainer;
import com.xgen.mongot.index.lucene.explain.information.LuceneQuerySpecification;
import com.xgen.mongot.index.lucene.explain.information.MetadataExplainInformation;
import com.xgen.mongot.index.lucene.explain.information.SearchExplainInformation;
import com.xgen.mongot.index.lucene.explain.information.SearchExplainInformationBuilder;
import com.xgen.mongot.index.lucene.explain.tracing.Explain;
import com.xgen.mongot.index.query.CollectorQuery;
import com.xgen.mongot.index.query.InvalidQueryException;
import com.xgen.mongot.index.query.OperatorQuery;
import com.xgen.mongot.index.query.Query;
import com.xgen.mongot.index.query.SearchQuery;
import com.xgen.mongot.index.query.aggregation.AggregationNodeStats;
import com.xgen.mongot.index.query.collectors.FacetCollector;
import com.xgen.mongot.index.query.collectors.FacetDefinition;
import com.xgen.mongot.index.query.counts.Count;
import com.xgen.mongot.index.query.operators.VectorSearchCriteria;
import com.xgen.mongot.index.query.operators.VectorSearchOperator;
import com.xgen.mongot.index.query.operators.VectorSearchQueryInput;
import com.xgen.mongot.index.status.IndexStatus;
import com.xgen.mongot.index.version.Generation;
import com.xgen.mongot.index.version.IndexFormatVersion;
import com.xgen.mongot.index.version.UserIndexVersion;
import com.xgen.mongot.metrics.MetricsFactory;
import com.xgen.mongot.searchenvoy.grpc.SearchEnvoyMetadata;
import com.xgen.mongot.util.BsonUtils;
import com.xgen.mongot.util.Bytes;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.ErrorType;
import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.mongodb.MongoDbServerInfo;
import com.xgen.mongot.util.mongodb.MongoDbVersion;
import com.xgen.mongot.util.regex.RegexInterruptedException;
import com.xgen.mongot.util.regex.RegexLimitExceededException;
import com.xgen.testing.mongot.index.definition.SearchIndexDefinitionBuilder;
import com.xgen.testing.mongot.index.lucene.explain.information.QueryExplainInformationBuilder;
import com.xgen.testing.mongot.index.lucene.explain.information.TermQueryBuilder;
import com.xgen.testing.mongot.index.lucene.explain.tracing.FakeExplain;
import com.xgen.testing.mongot.index.query.ApproximateVectorQueryCriteriaBuilder;
import com.xgen.testing.mongot.metrics.TestMeterRegistry;
import com.xgen.testing.mongot.mock.index.SearchIndex;
import com.xgen.testing.mongot.server.command.search.definition.request.CursorOptionsDefinitionBuilder;
import com.xgen.testing.mongot.server.command.search.definition.request.ExplainDefinitionBuilder;
import com.xgen.testing.mongot.server.command.search.definition.request.SearchCommandDefinitionBuilder;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import org.bson.BsonArray;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.types.ObjectId;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.theories.DataPoints;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.runner.RunWith;
import org.mockito.Mockito;

@RunWith(Theories.class)
public class SearchCommandTest {

  private static final String DATABASE_NAME = "testDb";
  private static final String COLLECTION_NAME = "testCollection";
  private static final UUID COLLECTION_UUID = UUID.randomUUID();
  private static final String INDEX_NAME = "default";
  private static final SearchCommandsRegister.BootstrapperMetadata BOOTSTRAPPER_METADATA =
      new SearchCommandsRegister.BootstrapperMetadata(
          "testVersion",
          "localhost",
          () -> MongoDbServerInfo.EMPTY,
          FeatureFlags.getDefault(),
          DynamicFeatureFlagRegistry.empty());
  private static final String RSID = "atlas-xyz";

  private static final BsonDocument VALID_OPERATOR_QUERY =
      new BsonDocument()
          .append(
              "text",
              new BsonDocument()
                  .append("path", new BsonString("a"))
                  .append("query", new BsonString("b")));

  private static final BsonDocument QUERY_WITH_VALID_COUNT =
      new BsonDocument()
          .append(
              "count",
              new BsonDocument()
                  .append("type", new BsonString("lowerBound"))
                  .append("threshold", new BsonInt32(5000)))
          .append(
              "text",
              new BsonDocument()
                  .append("path", new BsonString("a"))
                  .append("query", new BsonString("b")));

  private static final BsonDocument QUERY_WITH_INVALID_COUNT =
      new BsonDocument()
          .append(
              "count",
              new BsonDocument()
                  .append("type", new BsonString("lowerBound"))
                  .append("threshold", new BsonInt32(-1)))
          .append(
              "text",
              new BsonDocument()
                  .append("path", new BsonString("a"))
                  .append("query", new BsonString("b")));

  private static final BsonDocument INVALID_OPERATOR_QUERY =
      new BsonDocument().append("text", new BsonDocument().append("query", new BsonString("b")));

  private static final BsonDocument INVALID_INDEX_QUERY =
      new BsonDocument()
          .append("index", new BsonString("invalid"))
          .append(
              "text",
              new BsonDocument()
                  .append("path", new BsonString("a"))
                  .append("query", new BsonString("b")));

  private static final BsonDocument UNKNOWN_QUERY =
      new BsonDocument()
          .append(
              "invalid",
              new BsonDocument()
                  .append("path", new BsonString("a"))
                  .append("query", new BsonString("b")));

  private static final BsonDocument VALID_COLLECTOR_QUERY =
      new BsonDocument()
          .append(
              "facet",
              new BsonDocument()
                  .append("operator", VALID_OPERATOR_QUERY)
                  .append(
                      "facets",
                      new BsonDocument()
                          .append(
                              "directorFacet",
                              new BsonDocument()
                                  .append("type", new BsonString("string"))
                                  .append("path", new BsonString("director")))));

  private static final BsonDocument INVALID_COLLECTOR_QUERY =
      new BsonDocument()
          .append(
              "facet",
              new BsonDocument()
                  .append("operator", VALID_OPERATOR_QUERY)
                  .append(
                      "facets",
                      new BsonDocument()
                          .append(
                              "testFacet",
                              new BsonDocument().append("path", new BsonString("test")))));

  /** Collector query with string facet numBuckets 2000 (above 1k limit when DFF off). */
  private static final BsonDocument COLLECTOR_QUERY_NUM_BUCKETS_2000 =
      new BsonDocument()
          .append(
              "facet",
              new BsonDocument()
                  .append("operator", VALID_OPERATOR_QUERY)
                  .append(
                      "facets",
                      new BsonDocument()
                          .append(
                              "directorFacet",
                              new BsonDocument()
                                  .append("type", new BsonString("string"))
                                  .append("path", new BsonString("director"))
                                  .append("numBuckets", new BsonInt32(2000)))));

  private static final BsonDocument INVALID_INDEX_RESPONSE =
      new BsonDocument()
          .append("ok", new BsonInt32(0))
          .append("errmsg", new BsonString("invalid index"));

  private static final BsonDocument IO_ERROR_RESPONSE =
      new BsonDocument()
          .append("ok", new BsonInt32(0))
          .append("errmsg", new BsonString("IO error"));

  private static MetricsFactory mockMetricsFactory() {
    return new MetricsFactory("mockNamespace", new SimpleMeterRegistry());
  }

  private static double internalFailureCount(
      MetricsFactory metricsFactory, String exceptionName, ErrorType errorType) {
    return metricsFactory
        .counter(
            "searchCommandInternalFailures",
            Tags.of("exceptionName", exceptionName, ErrorType.METRIC_TAG_KEY, errorType.toString()))
        .count();
  }

  @Test
  public void internalFailureMetric_hasPrometheusCompatibleTagKeys() {
    var registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
    var metrics = new SearchCommand.Metrics(new MetricsFactory("search_metrics", registry));

    metrics.recordInternalFailure(new IOException("handled exception"));
    metrics.recordInternalFailure(new AssertionError("raw throwable"));

    Assert.assertEquals(
        1.0,
        registry
            .find("search_metrics.searchCommandInternalFailures")
            .tags(
                "exceptionName",
                IOException.class.getSimpleName(),
                ErrorType.METRIC_TAG_KEY,
                ErrorType.INTERNAL_ERROR.toString())
            .counter()
            .count(),
        0.0);
    Assert.assertEquals(
        1.0,
        registry
            .find("search_metrics.searchCommandInternalFailures")
            .tags(
                "exceptionName",
                AssertionError.class.getSimpleName(),
                ErrorType.METRIC_TAG_KEY,
                ErrorType.INTERNAL_ERROR.toString())
            .counter()
            .count(),
        0.0);
  }

  @DataPoints
  public static final List<MongoDbVersion> mdbVersions =
      List.of(new MongoDbVersion(8, 1, 0), new MongoDbVersion(7, 0, 0));

  @Test
  public void testValidIndex() throws Exception {
    var metricsFactory = mockMetricsFactory();
    SearchCommand command =
        new SearchCommand(
            new SearchCommand.Metrics(metricsFactory),
            getCursorManager(),
            new DefaultIndexCatalog(),
            new InitializedIndexCatalog(),
            new IncompatibleIndexes(TestMeterRegistry.get()),
            SearchCommandDefinitionBuilder.builder()
                .db(DATABASE_NAME)
                .collectionName(COLLECTION_NAME)
                .collectionUuid(COLLECTION_UUID)
                .query(VALID_OPERATOR_QUERY)
                .build(),
            BOOTSTRAPPER_METADATA,
            CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT,
            () -> null);

    BsonDocument result = command.run();
    BsonDocument expected = mockInitialMongotCursorBatch().toBson();
    Assert.assertEquals(expected, result);
    Assert.assertEquals(List.of(MOCK_SEARCH_CURSOR_ID), command.getCreatedCursorIds());
    Truth.assertThat(
            metricsFactory
                .counter("regexAnalysisLimitExceeded", Tags.of("command", "search"))
                .count())
        .isEqualTo(0.0);
  }

  @Test
  public void getBatch_searchAggregationEnabled_recordsCursorId() throws Exception {
    runSearchAndAssertCursorId(
        FeatureFlags.withDefaults().enable(Feature.SEARCH_AGGREGATION).build(),
        Optional.of(MOCK_SEARCH_CURSOR_ID));
  }

  @Test
  public void getBatch_searchAggregationDisabled_doesNotRecordCursorId() throws Exception {
    runSearchAndAssertCursorId(
        FeatureFlags.withDefaults().disable(Feature.SEARCH_AGGREGATION).build(), Optional.empty());
  }

  @Test
  public void testValidIndexIntermediate() throws Exception {
    SearchCommand command =
        new SearchCommand(
            new SearchCommand.Metrics(mockMetricsFactory()),
            getCursorManager(),
            new DefaultIndexCatalog(),
            new InitializedIndexCatalog(),
            new IncompatibleIndexes(TestMeterRegistry.get()),
            SearchCommandDefinitionBuilder.builder()
                .db(DATABASE_NAME)
                .collectionName(COLLECTION_NAME)
                .collectionUuid(COLLECTION_UUID)
                .query(VALID_OPERATOR_QUERY)
                .intermediate(1)
                .build(),
            BOOTSTRAPPER_METADATA,
            CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT,
            () -> null);

    BsonDocument result = command.run();
    BsonDocument expected = mockIntermediateMongotCursorBatch().toBson();
    Assert.assertEquals(expected, result);
    Assert.assertEquals(
        Arrays.asList(MOCK_SEARCH_CURSOR_ID, MOCK_META_CURSOR_ID), command.getCreatedCursorIds());
  }

  /** Builds a BsonArray containing one document whose serialized size is exactly {@code size}. */
  private static BsonArray buildArrayOfSize(Bytes size) {
    long target = size.toBytes();
    // An empty {"pad": ""} document's serialized size, to compute the padding needed.
    long baseSize =
        BsonUtils.bsonValueSerializedBytes(
                new BsonArray(List.of(new BsonDocument("pad", new BsonString("")))))
            .toBytes();
    Check.checkArg(
        target >= baseSize,
        "requested size %s is smaller than the minimum encodable array size %s",
        target,
        baseSize);
    long padLength = target - baseSize;
    String padding = "x".repeat((int) padLength);
    return new BsonArray(List.of(new BsonDocument("pad", new BsonString(padding))));
  }

  /**
   * Regression test: {@code getIntermediateBatch} computes {@code bytesRemaining} (the total budget
   * minus the *combined* worst-case envelope size for both cursors), then reserves the meta
   * cursor's own worst-case envelope out of that before handing it its raw-content limit, so that
   * wrapping the meta content into an actual {@link MongotCursorBatch} (adding cursor id,
   * namespace, etc.) never pushes the wrapped size past {@code bytesRemaining}.
   *
   * <p>This mock's meta cursor genuinely respects whatever size limit {@code getIntermediateBatch}
   * passes to {@code getNextBatch} (mirroring {@link
   * com.xgen.mongot.util.bson.BsonArrayBuilder#withLimit}, which the real {@code
   * LuceneFacetCollectorMetaBatchProducer} uses and which never lets its output exceed the given
   * limit), so this exercises the same greedy-producer edge case a real cursor would hit.
   */
  @Test
  public void testIntermediateSearchCommand_limitRespectingMetaProducer_reservationAloneSuffices()
      throws Exception {
    MongotCursorManager cursorManager = mock(MongotCursorManager.class);
    MongotIntermediateCursorBatch intermediateCursorBatch = mockIntermediateMongotCursorBatch();
    MongotCursorBatch searchBatch = intermediateCursorBatch.cursors().get(1);

    when(cursorManager.newIntermediateCursors(
            any(), any(), any(), any(), any(), anyInt(), any(), any(), any()))
        .thenReturn(new IntermediateSearchCursorInfo(MOCK_SEARCH_CURSOR_ID, MOCK_META_CURSOR_ID));

    when(cursorManager.getNextBatch(anyLong(), any(), any()))
        .then(
            invocation -> {
              long cursorId = invocation.getArgument(0);
              if (cursorId != MOCK_SEARCH_CURSOR_ID && cursorId != MOCK_META_CURSOR_ID) {
                throw new MongotCursorNotFoundException(cursorId);
              }
              if (cursorId == MOCK_SEARCH_CURSOR_ID) {
                return new MongotCursorResultInfo(
                    false,
                    searchBatch.getCursorExpected().getBatch(),
                    Optional.empty(),
                    searchBatch.getCursorExpected().getNamespace());
              }
              // Meta cursor: genuinely fill content up to (but not exceeding) whatever limit was
              // actually passed in, like the real batch producer does.
              Bytes limit = invocation.getArgument(1);
              return new MongotCursorResultInfo(
                  false, buildArrayOfSize(limit), Optional.empty(), "foo.bar");
            });

    when(cursorManager.getIndexQueryBatchTimerRecorder(anyLong())).thenReturn((timer) -> {});

    Bytes emptyIntermediateBatchSize = MongotIntermediateCursorBatch.calculateEmptyBatchSize();
    // A small but nonzero budget so the meta cursor's content genuinely fills all the way to its
    // (correctly reserved, smaller-than-bytesRemaining) limit -- the greedy/worst case. Must be
    // large enough to hold the meta cursor's own worst-case envelope (namespace, cursor id, etc.)
    // on top of the combined envelope already reserved by calculateEmptyBatchSize() above.
    Bytes bsonSizeSoftLimit = emptyIntermediateBatchSize.add(Bytes.ofBytes(2000));

    SearchCommand command =
        new SearchCommand(
            new SearchCommand.Metrics(mockMetricsFactory()),
            cursorManager,
            new DefaultIndexCatalog(),
            new InitializedIndexCatalog(),
            new IncompatibleIndexes(TestMeterRegistry.get()),
            SearchCommandDefinitionBuilder.builder()
                .db(DATABASE_NAME)
                .collectionName(COLLECTION_NAME)
                .collectionUuid(COLLECTION_UUID)
                .query(VALID_OPERATOR_QUERY)
                .intermediate(1)
                .build(),
            BOOTSTRAPPER_METADATA,
            bsonSizeSoftLimit,
            () -> null);

    // SearchCommand.run() catches IllegalArgumentException internally and returns an error body
    // (ok: 0, errmsg: ...) rather than propagating it, so we must check the response shape, not
    // just that a non-null result came back.
    BsonDocument result = command.run();
    Assert.assertEquals(
        "expected a successful response (ok: 1), but got: " + result,
        1,
        result.getInt32("ok").getValue());
  }

  @Theory
  public void intermediateSearchCommand_explain_valid(MongoDbVersion mdbVersion) throws Exception {
    SearchExplainInformation explain =
        SearchExplainInformationBuilder.newBuilder()
            .queryExplainInfos(
                List.of(
                    QueryExplainInformationBuilder.builder()
                        .type(LuceneQuerySpecification.Type.TERM_QUERY)
                        .args(TermQueryBuilder.builder().path("a").value("hello").build())
                        .build()))
            .build();

    var cursorManager = getCursorManager(explain);

    SearchCommand command =
        new SearchCommand(
            new SearchCommand.Metrics(mockMetricsFactory()),
            cursorManager,
            new DefaultIndexCatalog(),
            new InitializedIndexCatalog(),
            new IncompatibleIndexes(TestMeterRegistry.get()),
            SearchCommandDefinitionBuilder.builder()
                .db(DATABASE_NAME)
                .collectionName(COLLECTION_NAME)
                .collectionUuid(COLLECTION_UUID)
                .query(VALID_OPERATOR_QUERY)
                .intermediate(1)
                .explain(
                    ExplainDefinitionBuilder.builder()
                        .verbosity(Explain.Verbosity.ALL_PLANS_EXECUTION)
                        .build())
                .build(),
            new SearchCommandsRegister.BootstrapperMetadata(
                "local",
                "localhost",
                () -> new MongoDbServerInfo(Optional.of(mdbVersion), Optional.of(RSID)),
                FeatureFlags.getDefault(),
                DynamicFeatureFlagRegistry.empty()),
            CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT,
            () -> null);

    try (var unused =
        FakeExplain.setup(
            Explain.Verbosity.EXECUTION_STATS, NUM_PARTITIONS.getDefaultValue(), explain)) {
      BsonDocument result = command.run();
      MongotIntermediateCursorBatch batch = MongotIntermediateCursorBatch.fromBson(result);
      Truth.assertThat(batch.cursors().stream().map(MongotCursorBatch::explain).toList())
          .comparingElementsUsing(
              Correspondence.from(
                  (first, unusedExplain) -> first.equals(explain),
                  "explain must be present in both batches"));
      if (mdbVersion.compareTo(Explain.FIRST_VERSION_KILLS_CURSORS_EXPLAIN) < 0) {
        Truth.assertThat(
                batch.cursors().stream()
                    .map(MongotCursorBatch::cursor)
                    .filter(Optional::isPresent)
                    .count())
            .isEqualTo(0);
        verify(cursorManager, times(2)).killCursor(anyLong());
      } else {
        Truth.assertThat(
                batch.cursors().stream()
                    .map(MongotCursorBatch::cursor)
                    .filter(Optional::isPresent)
                    .count())
            .isEqualTo(2);
        verify(cursorManager, never()).killCursor(anyLong());
      }
    }
  }

  @Test
  public void testIntermediateExplainQueryPlannerNoCursors() throws Exception {
    SearchExplainInformation explain =
        SearchExplainInformationBuilder.newBuilder()
            .queryExplainInfos(
                List.of(
                    QueryExplainInformationBuilder.builder()
                        .type(LuceneQuerySpecification.Type.TERM_QUERY)
                        .args(TermQueryBuilder.builder().path("a").value("hello").build())
                        .build()))
            .build();

    var cursorManager = getCursorManager(explain);

    SearchCommand command =
        new SearchCommand(
            new SearchCommand.Metrics(mockMetricsFactory()),
            cursorManager,
            new DefaultIndexCatalog(),
            new InitializedIndexCatalog(),
            new IncompatibleIndexes(TestMeterRegistry.get()),
            SearchCommandDefinitionBuilder.builder()
                .db(DATABASE_NAME)
                .collectionName(COLLECTION_NAME)
                .collectionUuid(COLLECTION_UUID)
                .query(VALID_OPERATOR_QUERY)
                .intermediate(1)
                .explain(
                    ExplainDefinitionBuilder.builder()
                        .verbosity(Explain.Verbosity.ALL_PLANS_EXECUTION)
                        .build())
                .build(),
            BOOTSTRAPPER_METADATA,
            CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT,
            () -> null);

    try (var unused =
        FakeExplain.setup(
            Explain.Verbosity.QUERY_PLANNER, NUM_PARTITIONS.getDefaultValue(), explain)) {
      BsonDocument result = command.run();
      MongotIntermediateCursorBatch batch = MongotIntermediateCursorBatch.fromBson(result);
      Truth.assertThat(
              batch.cursors().stream()
                  .map(MongotCursorBatch::cursor)
                  .filter(Optional::isEmpty)
                  .count())
          .isEqualTo(2);
    }
  }

  @Test
  public void testRegistersMetrics() throws Exception {
    AtomicInteger metricUpdateCount = new AtomicInteger(0);
    MongotCursorManager manager =
        getCursorManager((cursorId) -> metricUpdateCount.incrementAndGet());
    SearchCommand command =
        new SearchCommand(
            new SearchCommand.Metrics(mockMetricsFactory()),
            manager,
            new DefaultIndexCatalog(),
            new InitializedIndexCatalog(),
            new IncompatibleIndexes(TestMeterRegistry.get()),
            SearchCommandDefinitionBuilder.builder()
                .db(DATABASE_NAME)
                .collectionName(COLLECTION_NAME)
                .collectionUuid(COLLECTION_UUID)
                .query(VALID_OPERATOR_QUERY)
                .build(),
            BOOTSTRAPPER_METADATA,
            CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT,
            () -> null);

    command.run();
    checkState(
        metricUpdateCount.get() > 0,
        "SearchCommand should have called accept on the metrics updater.");
  }

  @Test
  public void testRegistersMetricsIntermediate() throws Exception {

    AtomicInteger metricUpdateCount = new AtomicInteger(0);
    MongotCursorManager manager =
        getCursorManager((cursorId) -> metricUpdateCount.incrementAndGet());
    SearchCommand command =
        new SearchCommand(
            new SearchCommand.Metrics(mockMetricsFactory()),
            manager,
            new DefaultIndexCatalog(),
            new InitializedIndexCatalog(),
            new IncompatibleIndexes(TestMeterRegistry.get()),
            SearchCommandDefinitionBuilder.builder()
                .db(DATABASE_NAME)
                .collectionName(COLLECTION_NAME)
                .collectionUuid(COLLECTION_UUID)
                .query(VALID_OPERATOR_QUERY)
                .intermediate(1)
                .build(),
            BOOTSTRAPPER_METADATA,
            CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT,
            () -> null);

    command.run();
    checkState(
        metricUpdateCount.get() > 0,
        "SearchCommand should have called accept on the metrics updater.");
  }

  @Test
  public void testInvalidDatabaseIntermediate() throws Exception {
    SearchCommand command =
        new SearchCommand(
            new SearchCommand.Metrics(mockMetricsFactory()),
            getCursorManager(),
            new DefaultIndexCatalog(),
            new InitializedIndexCatalog(),
            new IncompatibleIndexes(TestMeterRegistry.get()),
            SearchCommandDefinitionBuilder.builder()
                .db("invalid")
                .collectionName(COLLECTION_NAME)
                .collectionUuid(COLLECTION_UUID)
                .query(VALID_OPERATOR_QUERY)
                .intermediate(1)
                .build(),
            BOOTSTRAPPER_METADATA,
            CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT,
            () -> null);

    BsonDocument result = command.run();
    Assert.assertEquals(INVALID_INDEX_RESPONSE, result);
  }

  @Test
  public void testInvalidCollectionUuid() throws Exception {
    SearchCommand command =
        new SearchCommand(
            new SearchCommand.Metrics(mockMetricsFactory()),
            getCursorManager(),
            new DefaultIndexCatalog(),
            new InitializedIndexCatalog(),
            new IncompatibleIndexes(TestMeterRegistry.get()),
            SearchCommandDefinitionBuilder.builder()
                .db(DATABASE_NAME)
                .collectionName(COLLECTION_NAME)
                .collectionUuid(UUID.randomUUID())
                .query(VALID_OPERATOR_QUERY)
                .build(),
            BOOTSTRAPPER_METADATA,
            CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT,
            () -> null);

    BsonDocument result = command.run();
    Assert.assertEquals(INVALID_INDEX_RESPONSE, result);
  }

  @Test
  public void testInvalidCollectionUuidIntermediate() throws Exception {
    SearchCommand command =
        new SearchCommand(
            new SearchCommand.Metrics(mockMetricsFactory()),
            getCursorManager(),
            new DefaultIndexCatalog(),
            new InitializedIndexCatalog(),
            new IncompatibleIndexes(TestMeterRegistry.get()),
            SearchCommandDefinitionBuilder.builder()
                .db(DATABASE_NAME)
                .collectionName(COLLECTION_NAME)
                .collectionUuid(UUID.randomUUID())
                .query(VALID_OPERATOR_QUERY)
                .intermediate(1)
                .build(),
            BOOTSTRAPPER_METADATA,
            CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT,
            () -> null);

    BsonDocument result = command.run();
    Assert.assertEquals(INVALID_INDEX_RESPONSE, result);
  }

  @Test
  public void testInvalidIndexName() throws Exception {
    var metricsFactory = mockMetricsFactory();
    SearchCommand command =
        new SearchCommand(
            new SearchCommand.Metrics(metricsFactory),
            getCursorManager(),
            new DefaultIndexCatalog(),
            new InitializedIndexCatalog(),
            new IncompatibleIndexes(TestMeterRegistry.get()),
            SearchCommandDefinitionBuilder.builder()
                .db(DATABASE_NAME)
                .collectionName(COLLECTION_NAME)
                .collectionUuid(COLLECTION_UUID)
                .query(INVALID_INDEX_QUERY)
                .build(),
            BOOTSTRAPPER_METADATA,
            CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT,
            () -> null);

    command.handleSearchEnvoyMetadata(SearchEnvoyMetadata.getDefaultInstance());

    BsonDocument result = command.run();
    Assert.assertEquals(INVALID_INDEX_RESPONSE, result);
    Truth.assertThat(metricsFactory.counter("searchCommandNonexistentIndex").count())
        .isEqualTo(1.0);
  }

  /**
   * A quarantined index is absent from the catalog just like a nonexistent one, but must report why
   * it is unavailable rather than return the empty cursor of the test above.
   */
  @Test
  public void testQuarantinedIndexName() throws Exception {
    var incompatibleIndexes = new IncompatibleIndexes(TestMeterRegistry.get());
    incompatibleIndexes.addLiveIndex(
        quarantined("invalid", "unsupported field 'fieldFromTheFuture'"));

    var metricsFactory = mockMetricsFactory();
    SearchCommand command =
        new SearchCommand(
            new SearchCommand.Metrics(metricsFactory),
            getCursorManager(),
            new DefaultIndexCatalog(),
            new InitializedIndexCatalog(),
            incompatibleIndexes,
            SearchCommandDefinitionBuilder.builder()
                .db(DATABASE_NAME)
                .collectionName(COLLECTION_NAME)
                .collectionUuid(COLLECTION_UUID)
                .query(INVALID_INDEX_QUERY)
                .build(),
            BOOTSTRAPPER_METADATA,
            CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT,
            () -> null);

    BsonDocument result = command.run();

    Assert.assertEquals(
        new BsonDocument()
            .append("ok", new BsonInt32(0))
            .append(
                "errmsg",
                new BsonString(
                    "Index 'invalid' is unavailable: its definition cannot be parsed (unsupported"
                        + " field 'fieldFromTheFuture').")),
        result);
    Truth.assertThat(metricsFactory.counter("searchCommandNonexistentIndex").count())
        .isEqualTo(0.0);
    command.handleSearchEnvoyMetadata(SearchEnvoyMetadata.getDefaultInstance());
    Assert.assertEquals(result, command.run());
  }

  /** A quarantined index of another name leaves the nonexistent-index behaviour untouched. */
  @Test
  public void testQuarantinedIndexOfAnotherName() throws Exception {
    var incompatibleIndexes = new IncompatibleIndexes(TestMeterRegistry.get());
    incompatibleIndexes.addLiveIndex(quarantined("someOtherIndex", "unsupported field"));

    SearchCommand command =
        new SearchCommand(
            new SearchCommand.Metrics(mockMetricsFactory()),
            getCursorManager(),
            new DefaultIndexCatalog(),
            new InitializedIndexCatalog(),
            incompatibleIndexes,
            SearchCommandDefinitionBuilder.builder()
                .db(DATABASE_NAME)
                .collectionName(COLLECTION_NAME)
                .collectionUuid(COLLECTION_UUID)
                .query(INVALID_INDEX_QUERY)
                .build(),
            BOOTSTRAPPER_METADATA,
            CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT,
            () -> null);

    Assert.assertEquals(INVALID_INDEX_RESPONSE, command.run());
  }

  private static QuarantinedIndex quarantined(String indexName, String parseError) {
    return new QuarantinedIndex(
        new JournalEntryIdentity(
            new ObjectId(),
            DATABASE_NAME,
            COLLECTION_UUID,
            indexName,
            new Generation(new UserIndexVersion(1), IndexFormatVersion.CURRENT)),
        IndexDefinitionGeneration.Type.SEARCH,
        parseError,
        new BsonDocument("fieldFromTheFuture", new BsonBoolean(true)));
  }

  @Test
  public void testInvalidIndexNameIntermediate() throws Exception {
    SearchCommand command =
        new SearchCommand(
            new SearchCommand.Metrics(mockMetricsFactory()),
            getCursorManager(),
            new DefaultIndexCatalog(),
            new InitializedIndexCatalog(),
            new IncompatibleIndexes(TestMeterRegistry.get()),
            SearchCommandDefinitionBuilder.builder()
                .db(DATABASE_NAME)
                .collectionName(COLLECTION_NAME)
                .collectionUuid(COLLECTION_UUID)
                .query(INVALID_INDEX_QUERY)
                .intermediate(1)
                .build(),
            BOOTSTRAPPER_METADATA,
            CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT,
            () -> null);

    BsonDocument result = command.run();
    Assert.assertEquals(INVALID_INDEX_RESPONSE, result);
  }

  @Test
  public void testInvalidOperatorQuery() throws Exception {
    SearchCommand command =
        new SearchCommand(
            new SearchCommand.Metrics(mockMetricsFactory()),
            getCursorManager(),
            new DefaultIndexCatalog(),
            new InitializedIndexCatalog(),
            new IncompatibleIndexes(TestMeterRegistry.get()),
            SearchCommandDefinitionBuilder.builder()
                .db(DATABASE_NAME)
                .collectionName(COLLECTION_NAME)
                .collectionUuid(COLLECTION_UUID)
                .query(INVALID_OPERATOR_QUERY)
                .build(),
            BOOTSTRAPPER_METADATA,
            CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT,
            () -> null);

    Assert.assertEquals(0, (int) command.getSearchCommandsTotalCount().count());
    Assert.assertEquals(0, (int) command.getSearchCommandInvalidQueries().count());
    BsonDocument result = command.run();
    Assert.assertEquals(1, (int) command.getSearchCommandsTotalCount().count());
    Assert.assertEquals(1, (int) command.getSearchCommandInvalidQueries().count());
    Assert.assertEquals(0, result.getInt32("ok").getValue());
    Assert.assertTrue(result.getString("errmsg").getValue().endsWith("path\" is required"));
  }

  @Test
  public void testInvalidOperatorQueryIntermediate() throws Exception {
    SearchCommand command =
        new SearchCommand(
            new SearchCommand.Metrics(mockMetricsFactory()),
            getCursorManager(),
            new DefaultIndexCatalog(),
            new InitializedIndexCatalog(),
            new IncompatibleIndexes(TestMeterRegistry.get()),
            SearchCommandDefinitionBuilder.builder()
                .db(DATABASE_NAME)
                .collectionName(COLLECTION_NAME)
                .collectionUuid(COLLECTION_UUID)
                .query(INVALID_OPERATOR_QUERY)
                .intermediate(1)
                .build(),
            BOOTSTRAPPER_METADATA,
            CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT,
            () -> null);

    Assert.assertEquals(0, (int) command.getSearchCommandsTotalCount().count());
    Assert.assertEquals(0, (int) command.getSearchCommandInvalidQueries().count());
    BsonDocument result = command.run();
    Assert.assertEquals(1, (int) command.getSearchCommandsTotalCount().count());
    Assert.assertEquals(1, (int) command.getSearchCommandInvalidQueries().count());
    Assert.assertEquals(0, result.getInt32("ok").getValue());
    Assert.assertTrue(result.getString("errmsg").getValue().endsWith("path\" is required"));
  }

  @Test
  public void testInvalidCollectorQuery() throws Exception {
    SearchCommand command =
        new SearchCommand(
            new SearchCommand.Metrics(mockMetricsFactory()),
            getCursorManager(),
            new DefaultIndexCatalog(),
            new InitializedIndexCatalog(),
            new IncompatibleIndexes(TestMeterRegistry.get()),
            SearchCommandDefinitionBuilder.builder()
                .db(DATABASE_NAME)
                .collectionName(COLLECTION_NAME)
                .collectionUuid(COLLECTION_UUID)
                .query(INVALID_COLLECTOR_QUERY)
                .build(),
            BOOTSTRAPPER_METADATA,
            CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT,
            () -> null);

    BsonDocument result = command.run();
    Assert.assertEquals(0, result.getInt32("ok").getValue());
    Assert.assertTrue(result.getString("errmsg").getValue().endsWith("type\" is required"));
  }

  @Test
  public void testInvalidCollectorQueryIntermediate() throws Exception {
    SearchCommand command =
        new SearchCommand(
            new SearchCommand.Metrics(mockMetricsFactory()),
            getCursorManager(),
            new DefaultIndexCatalog(),
            new InitializedIndexCatalog(),
            new IncompatibleIndexes(TestMeterRegistry.get()),
            SearchCommandDefinitionBuilder.builder()
                .db(DATABASE_NAME)
                .collectionName(COLLECTION_NAME)
                .collectionUuid(COLLECTION_UUID)
                .query(INVALID_COLLECTOR_QUERY)
                .intermediate(1)
                .build(),
            BOOTSTRAPPER_METADATA,
            CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT,
            () -> null);

    BsonDocument result = command.run();
    Assert.assertEquals(0, result.getInt32("ok").getValue());
    Assert.assertTrue(result.getString("errmsg").getValue().endsWith("type\" is required"));
  }

  @Test
  public void testInvalidQuery() throws Exception {
    SearchCommand command =
        new SearchCommand(
            new SearchCommand.Metrics(mockMetricsFactory()),
            getCursorManager(),
            new DefaultIndexCatalog(),
            new InitializedIndexCatalog(),
            new IncompatibleIndexes(TestMeterRegistry.get()),
            SearchCommandDefinitionBuilder.builder()
                .db(DATABASE_NAME)
                .collectionName(COLLECTION_NAME)
                .collectionUuid(COLLECTION_UUID)
                .query(UNKNOWN_QUERY)
                .build(),
            BOOTSTRAPPER_METADATA,
            CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT,
            () -> null);

    BsonDocument result = command.run();
    Assert.assertEquals(0, result.getInt32("ok").getValue());
    Assert.assertEquals(
        "Query should contain either an operator [autocomplete, compound, embeddedDocument,"
            + " equals, exists, geoShape, geoWithin, hasAncestor, hasRoot, in, knnBeta,"
            + " moreLikeThis, near, phrase, queryString, range, regex, search, span, term, text,"
            + " vectorSearch, wildcard] or a collector [facet]",
        result.getString("errmsg").getValue());
  }

  @Test
  public void testInvalidQueryIntermediate() throws Exception {
    SearchCommand command =
        new SearchCommand(
            new SearchCommand.Metrics(mockMetricsFactory()),
            getCursorManager(),
            new DefaultIndexCatalog(),
            new InitializedIndexCatalog(),
            new IncompatibleIndexes(TestMeterRegistry.get()),
            SearchCommandDefinitionBuilder.builder()
                .db(DATABASE_NAME)
                .collectionName(COLLECTION_NAME)
                .collectionUuid(COLLECTION_UUID)
                .query(UNKNOWN_QUERY)
                .intermediate(1)
                .build(),
            BOOTSTRAPPER_METADATA,
            CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT,
            () -> null);

    BsonDocument result = command.run();
    Assert.assertEquals(0, result.getInt32("ok").getValue());
    Assert.assertEquals(
        "Query should contain either an operator [autocomplete, compound, embeddedDocument,"
            + " equals, exists, geoShape, geoWithin, hasAncestor, hasRoot, in, knnBeta,"
            + " moreLikeThis, near, phrase, queryString, range, regex, search, span, term, text,"
            + " vectorSearch, wildcard] or a collector [facet]",
        result.getString("errmsg").getValue());
  }

  @Test
  public void testQueryWithValidCount() throws Exception {
    SearchCommand command =
        new SearchCommand(
            new SearchCommand.Metrics(mockMetricsFactory()),
            getCursorManager(),
            new DefaultIndexCatalog(),
            new InitializedIndexCatalog(),
            new IncompatibleIndexes(TestMeterRegistry.get()),
            SearchCommandDefinitionBuilder.builder()
                .db(DATABASE_NAME)
                .collectionName(COLLECTION_NAME)
                .collectionUuid(COLLECTION_UUID)
                .query(QUERY_WITH_VALID_COUNT)
                .build(),
            BOOTSTRAPPER_METADATA,
            CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT,
            () -> null);

    Assert.assertEquals(0, (int) command.getSearchCommandsTotalCount().count());
    Assert.assertEquals(0, (int) command.getSearchCommandInvalidQueries().count());
    BsonDocument result = command.run();
    Assert.assertEquals(1, (int) command.getSearchCommandsTotalCount().count());
    Assert.assertEquals(0, (int) command.getSearchCommandInvalidQueries().count());
    Assert.assertEquals(1, result.getInt32("ok").getValue());
    Assert.assertFalse(result.containsKey("errmsg"));
  }

  @Test
  public void testQueryWithValidCountIntermediate() throws Exception {
    SearchCommand command =
        new SearchCommand(
            new SearchCommand.Metrics(mockMetricsFactory()),
            getCursorManager(),
            new DefaultIndexCatalog(),
            new InitializedIndexCatalog(),
            new IncompatibleIndexes(TestMeterRegistry.get()),
            SearchCommandDefinitionBuilder.builder()
                .db(DATABASE_NAME)
                .collectionName(COLLECTION_NAME)
                .collectionUuid(COLLECTION_UUID)
                .query(QUERY_WITH_VALID_COUNT)
                .intermediate(1)
                .build(),
            BOOTSTRAPPER_METADATA,
            CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT,
            () -> null);

    Assert.assertEquals(0, (int) command.getSearchCommandsTotalCount().count());
    Assert.assertEquals(0, (int) command.getSearchCommandInvalidQueries().count());
    BsonDocument result = command.run();
    Assert.assertEquals(1, (int) command.getSearchCommandsTotalCount().count());
    Assert.assertEquals(0, (int) command.getSearchCommandInvalidQueries().count());
    Assert.assertEquals(1, result.getInt32("ok").getValue());
    Assert.assertFalse(result.containsKey("errmsg"));
  }

  @Test
  public void testQueryWithInvalidCount() throws Exception {
    SearchCommand command =
        new SearchCommand(
            new SearchCommand.Metrics(mockMetricsFactory()),
            getCursorManager(),
            new DefaultIndexCatalog(),
            new InitializedIndexCatalog(),
            new IncompatibleIndexes(TestMeterRegistry.get()),
            SearchCommandDefinitionBuilder.builder()
                .db(DATABASE_NAME)
                .collectionName(COLLECTION_NAME)
                .collectionUuid(COLLECTION_UUID)
                .query(QUERY_WITH_INVALID_COUNT)
                .build(),
            BOOTSTRAPPER_METADATA,
            CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT,
            () -> null);

    Assert.assertEquals(0, (int) command.getSearchCommandsTotalCount().count());
    Assert.assertEquals(0, (int) command.getSearchCommandInvalidQueries().count());
    BsonDocument result = command.run();
    Assert.assertEquals(1, (int) command.getSearchCommandsTotalCount().count());
    Assert.assertEquals(1, (int) command.getSearchCommandInvalidQueries().count());
    Assert.assertEquals(0, result.getInt32("ok").getValue());
    Assert.assertTrue(
        result.getString("errmsg").getValue().endsWith("threshold\" cannot be negative"));
  }

  @Test
  public void testQueryWithInvalidCountIntermediate() throws Exception {
    SearchCommand command =
        new SearchCommand(
            new SearchCommand.Metrics(mockMetricsFactory()),
            getCursorManager(),
            new DefaultIndexCatalog(),
            new InitializedIndexCatalog(),
            new IncompatibleIndexes(TestMeterRegistry.get()),
            SearchCommandDefinitionBuilder.builder()
                .db(DATABASE_NAME)
                .collectionName(COLLECTION_NAME)
                .collectionUuid(COLLECTION_UUID)
                .query(QUERY_WITH_INVALID_COUNT)
                .intermediate(1)
                .build(),
            BOOTSTRAPPER_METADATA,
            CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT,
            () -> null);

    Assert.assertEquals(0, (int) command.getSearchCommandsTotalCount().count());
    Assert.assertEquals(0, (int) command.getSearchCommandInvalidQueries().count());
    BsonDocument result = command.run();
    Assert.assertEquals(1, (int) command.getSearchCommandsTotalCount().count());
    Assert.assertEquals(1, (int) command.getSearchCommandInvalidQueries().count());
    Assert.assertEquals(0, result.getInt32("ok").getValue());
    Assert.assertTrue(
        result.getString("errmsg").getValue().endsWith("threshold\" cannot be negative"));
  }

  @Test
  public void testValidSearchCommandWithCursorOptions() throws Exception {
    SearchCommand command =
        new SearchCommand(
            new SearchCommand.Metrics(mockMetricsFactory()),
            getCursorManager(),
            new DefaultIndexCatalog(),
            new InitializedIndexCatalog(),
            new IncompatibleIndexes(TestMeterRegistry.get()),
            SearchCommandDefinitionBuilder.builder()
                .db(DATABASE_NAME)
                .collectionName(COLLECTION_NAME)
                .collectionUuid(COLLECTION_UUID)
                .query(VALID_OPERATOR_QUERY)
                .intermediate(1)
                .cursorOptions(CursorOptionsDefinitionBuilder.builder().docsRequested(25).build())
                .build(),
            BOOTSTRAPPER_METADATA,
            CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT,
            () -> null);
    BsonDocument result = command.run();
    Assert.assertEquals(1, result.getInt32("ok").getValue());
    Assert.assertFalse(result.containsKey("errmsg"));
  }

  @Test
  public void testIoExceptionSearchCommand() throws Exception {
    SearchCommand command =
        new SearchCommand(
            new SearchCommand.Metrics(mockMetricsFactory()),
            getThrowableCursorManager(() -> new IOException("IO error")),
            new DefaultIndexCatalog(),
            new InitializedIndexCatalog(),
            new IncompatibleIndexes(TestMeterRegistry.get()),
            SearchCommandDefinitionBuilder.builder()
                .db(DATABASE_NAME)
                .collectionName(COLLECTION_NAME)
                .collectionUuid(COLLECTION_UUID)
                .query(VALID_OPERATOR_QUERY)
                .build(),
            BOOTSTRAPPER_METADATA,
            CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT,
            () -> null);

    BsonDocument result = command.run();
    Assert.assertEquals(IO_ERROR_RESPONSE, result);
  }

  @Test
  public void testIoExceptionSearchCommandIntermediate() throws Exception {
    SearchCommand command =
        new SearchCommand(
            new SearchCommand.Metrics(mockMetricsFactory()),
            getThrowableCursorManager(() -> new IOException("IO error")),
            new DefaultIndexCatalog(),
            new InitializedIndexCatalog(),
            new IncompatibleIndexes(TestMeterRegistry.get()),
            SearchCommandDefinitionBuilder.builder()
                .db(DATABASE_NAME)
                .collectionName(COLLECTION_NAME)
                .collectionUuid(COLLECTION_UUID)
                .query(VALID_OPERATOR_QUERY)
                .intermediate(1)
                .build(),
            BOOTSTRAPPER_METADATA,
            CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT,
            () -> null);

    BsonDocument result = command.run();
    Assert.assertEquals(IO_ERROR_RESPONSE, result);
  }

  // Test to prove that a RunTimeException that is wrapping an InvalidQueryException results in
  // the system treating it as an invalid query (user error) and not an internal failure
  // (system failure).
  @Test
  public void testWrappedInvalidQueryException() throws Exception {
    SearchCommand command =
        new SearchCommand(
            new SearchCommand.Metrics(mockMetricsFactory()),
            // Mock a RuntimeException wrapping an InvalidQueryException.
            getThrowableCursorManager(
                () -> new RuntimeException(new InvalidQueryException("wrapped user error"))),
            new DefaultIndexCatalog(),
            new InitializedIndexCatalog(),
            new IncompatibleIndexes(TestMeterRegistry.get()),
            SearchCommandDefinitionBuilder.builder()
                .db(DATABASE_NAME)
                .collectionName(COLLECTION_NAME)
                .collectionUuid(COLLECTION_UUID)
                .query(VALID_OPERATOR_QUERY)
                .build(),
            BOOTSTRAPPER_METADATA,
            CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT,
            () -> null);

    BsonDocument result = command.run();

    Assert.assertEquals(1, (int) command.getSearchCommandInvalidQueries().count());
    Assert.assertEquals(1, (int) command.getSearchCommandUnwrappedExceptions().count());

    // Verify response format
    Assert.assertEquals(0, result.getInt32("ok").getValue());
    Assert.assertTrue(result.getString("errmsg").getValue().contains("wrapped user error"));
  }

  @Test
  public void run_regexLimit_returnsUserError() throws Exception {
    var metricsFactory = mockMetricsFactory();
    SearchCommand command =
        new SearchCommand(
            new SearchCommand.Metrics(metricsFactory),
            getThrowableCursorManager(RegexLimitExceededException::new),
            new DefaultIndexCatalog(),
            new InitializedIndexCatalog(),
            new IncompatibleIndexes(TestMeterRegistry.get()),
            SearchCommandDefinitionBuilder.builder()
                .db(DATABASE_NAME)
                .collectionName(COLLECTION_NAME)
                .collectionUuid(COLLECTION_UUID)
                .query(VALID_OPERATOR_QUERY)
                .build(),
            BOOTSTRAPPER_METADATA,
            CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT,
            () -> null);

    BsonDocument result = command.run();

    Assert.assertEquals(0, result.getInt32("ok").getValue());
    Assert.assertEquals(
        new RegexLimitExceededException().getMessage(), result.getString("errmsg").getValue());
    Assert.assertEquals(1, (int) command.getSearchCommandInvalidQueries().count());
    Truth.assertThat(
            metricsFactory
                .counter("regexAnalysisLimitExceeded", Tags.of("command", "search"))
                .count())
        .isEqualTo(1.0);
    Truth.assertThat(
            internalFailureCount(
                metricsFactory,
                RegexLimitExceededException.class.getSimpleName(),
                ErrorType.INTERNAL_ERROR))
        .isEqualTo(0.0);
    Truth.assertThat(
            internalFailureCount(
                metricsFactory, RuntimeException.class.getSimpleName(), ErrorType.INTERNAL_ERROR))
        .isEqualTo(0.0);
  }

  @Test
  public void run_regexInterruption_doesNotCountAsLimitFailure() throws Exception {
    var metricsFactory = mockMetricsFactory();
    SearchCommand command =
        new SearchCommand(
            new SearchCommand.Metrics(metricsFactory),
            getThrowableCursorManager(RegexInterruptedException::new),
            new DefaultIndexCatalog(),
            new InitializedIndexCatalog(),
            new IncompatibleIndexes(TestMeterRegistry.get()),
            SearchCommandDefinitionBuilder.builder()
                .db(DATABASE_NAME)
                .collectionName(COLLECTION_NAME)
                .collectionUuid(COLLECTION_UUID)
                .query(VALID_OPERATOR_QUERY)
                .build(),
            BOOTSTRAPPER_METADATA,
            CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT,
            () -> null);

    BsonDocument result = command.run();

    Assert.assertEquals(0, result.getInt32("ok").getValue());
    Truth.assertThat(
            metricsFactory
                .counter("regexAnalysisLimitExceeded", Tags.of("command", "search"))
                .count())
        .isEqualTo(0.0);
  }

  @Test
  public void unreachableErrorThrown_internalMetricNotIncremented() throws Exception {
    var metricsFactory = mockMetricsFactory();
    SearchCommand command =
        new SearchCommand(
            new SearchCommand.Metrics(metricsFactory),
            getThrowableCursorManager(Check::unreachableError),
            new DefaultIndexCatalog(),
            new InitializedIndexCatalog(),
            new IncompatibleIndexes(TestMeterRegistry.get()),
            SearchCommandDefinitionBuilder.builder()
                .db(DATABASE_NAME)
                .collectionName(COLLECTION_NAME)
                .collectionUuid(COLLECTION_UUID)
                .query(VALID_OPERATOR_QUERY)
                .build(),
            BOOTSTRAPPER_METADATA,
            CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT,
            () -> null);

    Assert.assertThrows(AssertionError.class, command::run);
    Truth.assertThat(
            internalFailureCount(
                metricsFactory, AssertionError.class.getSimpleName(), ErrorType.INTERNAL_ERROR))
        .isEqualTo(0.0);
  }

  @Test
  public void intermediateBatch_unreachableErrorThrown_internalMetricNotIncremented()
      throws Exception {
    var metricsFactory = mockMetricsFactory();
    SearchCommand command =
        new SearchCommand(
            new SearchCommand.Metrics(metricsFactory),
            getThrowableCursorManager(Check::unreachableError),
            new DefaultIndexCatalog(),
            new InitializedIndexCatalog(),
            new IncompatibleIndexes(TestMeterRegistry.get()),
            SearchCommandDefinitionBuilder.builder()
                .db(DATABASE_NAME)
                .collectionName(COLLECTION_NAME)
                .collectionUuid(COLLECTION_UUID)
                .query(VALID_OPERATOR_QUERY)
                .intermediate(1)
                .build(),
            BOOTSTRAPPER_METADATA,
            CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT,
            () -> null);

    Assert.assertThrows(AssertionError.class, command::run);
    Truth.assertThat(
            internalFailureCount(
                metricsFactory, AssertionError.class.getSimpleName(), ErrorType.INTERNAL_ERROR))
        .isEqualTo(0.0);
  }

  @Test
  public void internalException_metricIncrementedWithExceptionNameAndErrorType() throws Exception {
    var metricsFactory = mockMetricsFactory();
    SearchCommand command =
        new SearchCommand(
            new SearchCommand.Metrics(metricsFactory),
            getThrowableCursorManager(() -> new IOException("IO error")),
            new DefaultIndexCatalog(),
            new InitializedIndexCatalog(),
            new IncompatibleIndexes(TestMeterRegistry.get()),
            SearchCommandDefinitionBuilder.builder()
                .db(DATABASE_NAME)
                .collectionName(COLLECTION_NAME)
                .collectionUuid(COLLECTION_UUID)
                .query(VALID_OPERATOR_QUERY)
                .build(),
            BOOTSTRAPPER_METADATA,
            CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT,
            () -> null);

    command.run();
    Truth.assertThat(
            internalFailureCount(
                metricsFactory, IOException.class.getSimpleName(), ErrorType.INTERNAL_ERROR))
        .isEqualTo(1.0);
    Truth.assertThat(
            metricsFactory
                .counter("regexAnalysisLimitExceeded", Tags.of("command", "search"))
                .count())
        .isEqualTo(0.0);
  }

  @Theory
  public void searchCommand_executeExplain_passes(MongoDbVersion mdbVersion) throws Exception {
    var explainInformation =
        SearchExplainInformationBuilder.newBuilder()
            .queryExplainInfos(
                List.of(
                    QueryExplainInformationBuilder.builder()
                        .type(LuceneQuerySpecification.Type.TERM_QUERY)
                        .args(TermQueryBuilder.builder().path("a").value("hello").build())
                        .build()))
            .build();

    var cursorManager = getCursorManager(explainInformation);

    SearchCommand command =
        new SearchCommand(
            new SearchCommand.Metrics(mockMetricsFactory()),
            cursorManager,
            new DefaultIndexCatalog(),
            new InitializedIndexCatalog(),
            new IncompatibleIndexes(TestMeterRegistry.get()),
            SearchCommandDefinitionBuilder.builder()
                .db(DATABASE_NAME)
                .collectionName(COLLECTION_NAME)
                .collectionUuid(COLLECTION_UUID)
                .query(VALID_OPERATOR_QUERY)
                .explain(
                    ExplainDefinitionBuilder.builder()
                        .verbosity(Explain.Verbosity.EXECUTION_STATS)
                        .build())
                .build(),
            new SearchCommandsRegister.BootstrapperMetadata(
                "local",
                "localhost",
                () -> new MongoDbServerInfo(Optional.of(mdbVersion), Optional.of(RSID)),
                FeatureFlags.getDefault(),
                DynamicFeatureFlagRegistry.empty()),
            CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT,
            () -> null);

    try (var unused = FakeExplain.setup(Explain.Verbosity.EXECUTION_STATS, 0, explainInformation)) {
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
  public void testExplainQueryPlannerNoCursors() throws Exception {
    var explainInformation =
        SearchExplainInformationBuilder.newBuilder()
            .queryExplainInfos(
                List.of(
                    QueryExplainInformationBuilder.builder()
                        .type(LuceneQuerySpecification.Type.TERM_QUERY)
                        .args(TermQueryBuilder.builder().path("a").value("hello").build())
                        .build()))
            .build();

    var cursorManager = getCursorManager(explainInformation);

    SearchCommand command =
        new SearchCommand(
            new SearchCommand.Metrics(mockMetricsFactory()),
            cursorManager,
            new DefaultIndexCatalog(),
            new InitializedIndexCatalog(),
            new IncompatibleIndexes(TestMeterRegistry.get()),
            SearchCommandDefinitionBuilder.builder()
                .db(DATABASE_NAME)
                .collectionName(COLLECTION_NAME)
                .collectionUuid(COLLECTION_UUID)
                .query(VALID_OPERATOR_QUERY)
                .explain(
                    ExplainDefinitionBuilder.builder()
                        .verbosity(Explain.Verbosity.ALL_PLANS_EXECUTION)
                        .build())
                .build(),
            BOOTSTRAPPER_METADATA,
            CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT,
            () -> null);

    try (var unused = FakeExplain.setup(Explain.Verbosity.QUERY_PLANNER, 0, explainInformation)) {
      BsonDocument result = command.run();
      MongotCursorBatch batch = MongotCursorBatch.fromBson(result);
      Truth.assertThat(batch.cursor()).isEmpty();
    }

    verify(cursorManager, times(1)).killCursor(MOCK_SEARCH_CURSOR_ID);
  }

  @Test
  public void testExplainRecordsMetadata() throws Exception {
    var cursorManager = getCursorManager();

    SearchCommand command =
        new SearchCommand(
            new SearchCommand.Metrics(mockMetricsFactory()),
            cursorManager,
            new DefaultIndexCatalog(),
            new InitializedIndexCatalog(),
            new IncompatibleIndexes(TestMeterRegistry.get()),
            SearchCommandDefinitionBuilder.builder()
                .db(DATABASE_NAME)
                .collectionName(COLLECTION_NAME)
                .collectionUuid(COLLECTION_UUID)
                .query(VALID_OPERATOR_QUERY)
                .explain(
                    ExplainDefinitionBuilder.builder()
                        .verbosity(Explain.Verbosity.ALL_PLANS_EXECUTION)
                        .build())
                .cursorOptions(CursorOptionsDefinitionBuilder.builder().batchSize(5).build())
                .build(),
            BOOTSTRAPPER_METADATA,
            CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT,
            () -> null);

    try (var unused =
        Explain.setup(
            Optional.of(Explain.Verbosity.QUERY_PLANNER),
            Optional.of(NUM_PARTITIONS.getDefaultValue()))) {
      command.run();

      var metadata = Explain.collect().get().metadata().get();
      Truth.assertThat(metadata)
          .isEqualTo(
              new MetadataExplainInformation(
                  Optional.of("testVersion"),
                  Optional.of("localhost"),
                  Optional.of(INDEX_NAME),
                  Optional.of(
                      CursorOptionsDefinitionBuilder.builder().batchSize(5).build().toBson()),
                  Optional.empty(),
                  Optional.empty()));
    }
  }

  @Test
  public void testSearchCommandIndexPartitionsExplain() throws Exception {
    var explainInformation =
        SearchExplainInformationBuilder.newBuilder()
            .addIndexPartitionExplainInformation(
                SearchExplainInformationBuilder.newBuilder()
                    .queryExplainInfos(
                        List.of(
                            QueryExplainInformationBuilder.builder()
                                .type(LuceneQuerySpecification.Type.TERM_QUERY)
                                .args(TermQueryBuilder.builder().path("a").value("hello").build())
                                .build()))
                    .build())
            .addIndexPartitionExplainInformation(
                SearchExplainInformationBuilder.newBuilder()
                    .queryExplainInfos(
                        List.of(
                            QueryExplainInformationBuilder.builder()
                                .type(LuceneQuerySpecification.Type.TERM_QUERY)
                                .args(TermQueryBuilder.builder().path("a").value("hello").build())
                                .build()))
                    .build())
            .build();

    IndexCatalog catalog = mock(IndexCatalog.class);
    InitializedIndexCatalog initializedIndexCatalog = mock(InitializedIndexCatalog.class);
    InitializedSearchIndex initializedIndex = Mockito.mock(InitializedSearchIndex.class);
    SearchIndexDefinition indexDefinition =
        SearchIndexDefinitionBuilder.builder()
            .defaultMetadata()
            .dynamicMapping()
            .numPartitions(2)
            .indexFeatureVersion(SearchIndexCapabilities.CURRENT_FEATURE_VERSION)
            .build();
    var indexGeneration = mockIndexGeneration();

    when(initializedIndex.getDefinition()).thenReturn(indexDefinition);
    when(catalog.getIndex(DATABASE_NAME, COLLECTION_UUID, Optional.empty(), INDEX_NAME))
        .thenReturn(Optional.of(indexGeneration));
    when(initializedIndexCatalog.getIndex(indexGeneration.getGenerationId()))
        .thenReturn(Optional.of(initializedIndex));

    SearchCommand command =
        new SearchCommand(
            new SearchCommand.Metrics(mockMetricsFactory()),
            getCursorManager(explainInformation),
            catalog,
            initializedIndexCatalog,
            new IncompatibleIndexes(TestMeterRegistry.get()),
            SearchCommandDefinitionBuilder.builder()
                .db(DATABASE_NAME)
                .collectionName(COLLECTION_NAME)
                .collectionUuid(COLLECTION_UUID)
                .query(VALID_OPERATOR_QUERY)
                .explain(
                    ExplainDefinitionBuilder.builder()
                        .verbosity(Explain.Verbosity.ALL_PLANS_EXECUTION)
                        .build())
                .build(),
            BOOTSTRAPPER_METADATA,
            CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT,
            () -> null);

    try (var unused = FakeExplain.setup(Explain.Verbosity.EXECUTION_STATS, 2, explainInformation)) {
      BsonDocument result = command.run();
      MongotCursorBatch batch = MongotCursorBatch.fromBson(result);
      Assert.assertTrue(batch.explain().isPresent());
      Assert.assertTrue(batch.cursor().isPresent());
      Assert.assertEquals(batch.explain().get(), explainInformation);
    }
  }

  @Test
  public void testValidFacetSearchCommand() throws Exception {
    SearchCommand command =
        new SearchCommand(
            new SearchCommand.Metrics(mockMetricsFactory()),
            getCursorManager(),
            new DefaultIndexCatalog(),
            new InitializedIndexCatalog(),
            new IncompatibleIndexes(TestMeterRegistry.get()),
            SearchCommandDefinitionBuilder.builder()
                .db(DATABASE_NAME)
                .collectionName(COLLECTION_NAME)
                .collectionUuid(COLLECTION_UUID)
                .query(VALID_COLLECTOR_QUERY)
                .build(),
            BOOTSTRAPPER_METADATA,
            CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT,
            () -> null);

    BsonDocument result = command.run();
    Assert.assertEquals(1, result.getInt32("ok").getValue());
    Assert.assertFalse(result.containsKey("errmsg"));
  }

  @Test
  public void testExplainFacetSearchCommand() throws Exception {
    var explainInformation =
        SearchExplainInformationBuilder.newBuilder()
            .queryExplainInfos(
                List.of(
                    QueryExplainInformationBuilder.builder()
                        .type(LuceneQuerySpecification.Type.TERM_QUERY)
                        .args(TermQueryBuilder.builder().path("a").value("hello").build())
                        .build()))
            .build();

    SearchCommand command =
        new SearchCommand(
            new SearchCommand.Metrics(mockMetricsFactory()),
            getCursorManager(explainInformation),
            new DefaultIndexCatalog(),
            new InitializedIndexCatalog(),
            new IncompatibleIndexes(TestMeterRegistry.get()),
            SearchCommandDefinitionBuilder.builder()
                .db(DATABASE_NAME)
                .collectionName(COLLECTION_NAME)
                .collectionUuid(COLLECTION_UUID)
                .query(VALID_COLLECTOR_QUERY)
                .explain(
                    ExplainDefinitionBuilder.builder()
                        .verbosity(Explain.Verbosity.ALL_PLANS_EXECUTION)
                        .build())
                .build(),
            BOOTSTRAPPER_METADATA,
            CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT,
            () -> null);

    try (var unused =
        FakeExplain.setup(
            Explain.Verbosity.QUERY_PLANNER,
            NUM_PARTITIONS.getDefaultValue(),
            explainInformation)) {
      BsonDocument result = command.run();
      MongotCursorBatch batch = MongotCursorBatch.fromBson(result);
      Assert.assertTrue(batch.explain().isPresent());
      Assert.assertEquals(batch.explain().get(), explainInformation);
    }
  }

  @Test
  public void testValidIntermediateFacetSearchCommand() throws Exception {
    SearchCommand command =
        new SearchCommand(
            new SearchCommand.Metrics(mockMetricsFactory()),
            getCursorManager(),
            new DefaultIndexCatalog(),
            new InitializedIndexCatalog(),
            new IncompatibleIndexes(TestMeterRegistry.get()),
            SearchCommandDefinitionBuilder.builder()
                .db(DATABASE_NAME)
                .collectionName(COLLECTION_NAME)
                .collectionUuid(COLLECTION_UUID)
                .query(VALID_COLLECTOR_QUERY)
                .intermediate(1)
                .build(),
            BOOTSTRAPPER_METADATA,
            CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT,
            () -> null);

    BsonDocument result = command.run();
    Assert.assertEquals(1, result.getInt32("ok").getValue());
    Assert.assertFalse(result.containsKey("errmsg"));
  }

  /** When DFF is off, numBuckets > 1000 is rejected (same as old behavior: user gets error). */
  @Test
  public void testStringFacetNumBucketsRejectedWhenDffOff() throws Exception {
    SearchCommand command =
        new SearchCommand(
            new SearchCommand.Metrics(mockMetricsFactory()),
            getCursorManager(),
            new DefaultIndexCatalog(),
            new InitializedIndexCatalog(),
            new IncompatibleIndexes(TestMeterRegistry.get()),
            SearchCommandDefinitionBuilder.builder()
                .db(DATABASE_NAME)
                .collectionName(COLLECTION_NAME)
                .collectionUuid(COLLECTION_UUID)
                .query(COLLECTOR_QUERY_NUM_BUCKETS_2000)
                .build(),
            BOOTSTRAPPER_METADATA,
            CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT,
            () -> null);

    BsonDocument result = command.run();
    Assert.assertEquals(0, result.getInt32("ok").getValue());
    Assert.assertTrue(result.containsKey("errmsg"));
    // When DFF is off, parser throws BsonParseException with "must be within bounds [1..1000]"
    String errmsg = result.getString("errmsg").getValue();
    Assert.assertTrue(
        "errmsg should be BSON parse error mentioning 1000 limit; got: " + errmsg,
        errmsg.contains("must be within bounds") && errmsg.contains("1000"));
  }

  @Test
  public void run_scopedSortFlagOff_rejectedBeforeCursorCreation() throws Exception {
    AtomicReference<SearchQuery> capturedQuery = new AtomicReference<>();
    SearchCommand command = scopedSortCommand(DynamicFeatureFlagRegistry.empty(), capturedQuery);

    BsonDocument result = command.run();

    Assert.assertEquals(0, result.getInt32("ok").getValue());
    Assert.assertTrue(result.getString("errmsg").getValue().contains("not currently supported"));
    Assert.assertNull(capturedQuery.get());
  }

  @Test
  public void run_scopedSortFlagOn_reachesCursorCreation() throws Exception {
    DynamicFeatureFlagRegistry registry = mock(DynamicFeatureFlagRegistry.class);
    when(registry.evaluateClusterInvariant(DynamicFeatureFlags.ENABLE_EMBEDDED_SCOPED_SORT))
        .thenReturn(true);
    AtomicReference<SearchQuery> capturedQuery = new AtomicReference<>();
    SearchCommand command = scopedSortCommand(registry, capturedQuery);

    BsonDocument result = command.run();

    Assert.assertEquals(1, result.getInt32("ok").getValue());
    Assert.assertNotNull(capturedQuery.get());
    Assert.assertTrue(
        capturedQuery
            .get()
            .toBson()
            .getDocument("sort")
            .getDocument("variants.price")
            .containsKey("embeddedDocument"));
  }

  private SearchCommand scopedSortCommand(
      DynamicFeatureFlagRegistry registry, AtomicReference<SearchQuery> capturedQuery)
      throws Exception {
    BsonDocument query =
        BsonDocument.parse(
            """
            {"exists": {"path": "variants"}, "sort": {"variants.price": {
              "order": 1, "embeddedDocument": {"path": "variants", "operator": {
                "exists": {"path": "variants.price"}
              }}
            }}}
            """);
    var metadata =
        new SearchCommandsRegister.BootstrapperMetadata(
            "testVersion",
            "localhost",
            () -> MongoDbServerInfo.EMPTY,
            FeatureFlags.getDefault(),
            registry);
    return new SearchCommand(
        new SearchCommand.Metrics(mockMetricsFactory()),
        getCursorManagerCapturingQuery(capturedQuery),
        new DefaultIndexCatalog(),
        new InitializedIndexCatalog(),
        new IncompatibleIndexes(TestMeterRegistry.get()),
        SearchCommandDefinitionBuilder.builder()
            .db(DATABASE_NAME)
            .collectionName(COLLECTION_NAME)
            .collectionUuid(COLLECTION_UUID)
            .query(query)
            .build(),
        metadata,
        CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT,
        () -> null);
  }

  @Test
  public void testStringFacetNumBucketsUnchangedWhenDffOn() throws Exception {
    DynamicFeatureFlagRegistry registry = mock(DynamicFeatureFlagRegistry.class);
    when(registry.evaluateClusterInvariant(DynamicFeatureFlags.ENABLE_10K_BUCKET_LIMIT))
        .thenReturn(true);

    AtomicReference<SearchQuery> capturedQuery = new AtomicReference<>();
    MongotCursorManager cursorManager = getCursorManagerCapturingQuery(capturedQuery);

    SearchCommandsRegister.BootstrapperMetadata metadataWithDffOn =
        new SearchCommandsRegister.BootstrapperMetadata(
            "testVersion",
            "localhost",
            () -> MongoDbServerInfo.EMPTY,
            FeatureFlags.getDefault(),
            registry);

    SearchCommand command =
        new SearchCommand(
            new SearchCommand.Metrics(mockMetricsFactory()),
            cursorManager,
            new DefaultIndexCatalog(),
            new InitializedIndexCatalog(),
            new IncompatibleIndexes(TestMeterRegistry.get()),
            SearchCommandDefinitionBuilder.builder()
                .db(DATABASE_NAME)
                .collectionName(COLLECTION_NAME)
                .collectionUuid(COLLECTION_UUID)
                .query(COLLECTOR_QUERY_NUM_BUCKETS_2000)
                .build(),
            metadataWithDffOn,
            CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT,
            () -> null);

    BsonDocument result = command.run();
    Assert.assertEquals(1, result.getInt32("ok").getValue());
    Assert.assertFalse(result.containsKey("errmsg"));

    SearchQuery query = capturedQuery.get();
    Assert.assertNotNull(query);
    Assert.assertTrue(query instanceof CollectorQuery);
    FacetCollector collector = (FacetCollector) ((CollectorQuery) query).collector();
    FacetDefinition.StringFacetDefinition directorFacet =
        (FacetDefinition.StringFacetDefinition) collector.facetDefinitions().get("directorFacet");
    Assert.assertNotNull(directorFacet);
    Assert.assertEquals(2000, directorFacet.numBuckets());
  }

  @Test
  public void testStringFacetNumBucketsUnder1kUnchangedWhenDffOff() throws Exception {
    AtomicReference<SearchQuery> capturedQuery = new AtomicReference<>();
    MongotCursorManager cursorManager = getCursorManagerCapturingQuery(capturedQuery);

    BsonDocument queryWith500Buckets =
        new BsonDocument()
            .append(
                "facet",
                new BsonDocument()
                    .append("operator", VALID_OPERATOR_QUERY)
                    .append(
                        "facets",
                        new BsonDocument()
                            .append(
                                "smallFacet",
                                new BsonDocument()
                                    .append("type", new BsonString("string"))
                                    .append("path", new BsonString("genre"))
                                    .append("numBuckets", new BsonInt32(500)))));

    SearchCommand command =
        new SearchCommand(
            new SearchCommand.Metrics(mockMetricsFactory()),
            cursorManager,
            new DefaultIndexCatalog(),
            new InitializedIndexCatalog(),
            new IncompatibleIndexes(TestMeterRegistry.get()),
            SearchCommandDefinitionBuilder.builder()
                .db(DATABASE_NAME)
                .collectionName(COLLECTION_NAME)
                .collectionUuid(COLLECTION_UUID)
                .query(queryWith500Buckets)
                .build(),
            BOOTSTRAPPER_METADATA,
            CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT,
            () -> null);

    BsonDocument result = command.run();
    Assert.assertEquals(1, result.getInt32("ok").getValue());

    SearchQuery query = capturedQuery.get();
    Assert.assertNotNull(query);
    FacetCollector collector = (FacetCollector) ((CollectorQuery) query).collector();
    FacetDefinition.StringFacetDefinition smallFacet =
        (FacetDefinition.StringFacetDefinition) collector.facetDefinitions().get("smallFacet");
    Assert.assertNotNull(smallFacet);
    Assert.assertEquals(500, smallFacet.numBuckets());
  }

  private static DynamicFeatureFlagConfig indexRetryConfig(DynamicFeatureFlagConfig.Phase phase) {
    return new DynamicFeatureFlagConfig(
        DynamicFeatureFlags.INDEX_UNAVAILABLE_RETRY.getName(),
        phase,
        List.of(),
        List.of(),
        0,
        DynamicFeatureFlagConfig.Scope.MONGOT_CLUSTER);
  }

  @Test
  public void testIndexNotInitializedReturnsErrorResponse() throws Exception {
    var retryRegistry =
        new DynamicFeatureFlagRegistry(
            Optional.empty(), Optional.empty(), Optional.empty(), Optional.of(new ObjectId()));
    var metadata =
        new SearchCommandsRegister.BootstrapperMetadata(
            BOOTSTRAPPER_METADATA.mongotVersion(),
            BOOTSTRAPPER_METADATA.mongotHostName(),
            BOOTSTRAPPER_METADATA.mongoDbServerInfoProvider(),
            BOOTSTRAPPER_METADATA.featureFlags(),
            retryRegistry);
    var metricsFactory = mockMetricsFactory();
    IndexCatalog catalog = mock(IndexCatalog.class);
    InitializedIndexCatalog initializedIndexCatalog = mock(InitializedIndexCatalog.class);

    var searchIndexDefinition = SearchIndex.mockSearchDefinition(new ObjectId());
    var definitionGeneration = mockDefinitionGeneration(searchIndexDefinition);
    com.xgen.mongot.index.SearchIndex mockSearchIndex = SearchIndex.mockIndex(definitionGeneration);
    // Set status to STEADY so throwIfUnavailableForQuerying() won't throw
    mockSearchIndex.setStatus(IndexStatus.steady());

    var indexGeneration = new IndexGeneration(mockSearchIndex, definitionGeneration);

    when(catalog.getIndex(DATABASE_NAME, COLLECTION_UUID, Optional.empty(), INDEX_NAME))
        .thenReturn(Optional.of(indexGeneration));
    when(initializedIndexCatalog.getIndex(indexGeneration.getGenerationId()))
        .thenReturn(Optional.empty());

    SearchCommand command =
        new SearchCommand(
            new SearchCommand.Metrics(metricsFactory),
            getCursorManager(),
            catalog,
            initializedIndexCatalog,
            new IncompatibleIndexes(TestMeterRegistry.get()),
            SearchCommandDefinitionBuilder.builder()
                .db(DATABASE_NAME)
                .collectionName(COLLECTION_NAME)
                .collectionUuid(COLLECTION_UUID)
                .query(VALID_OPERATOR_QUERY)
                .build(),
            metadata,
            CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT,
            () -> null);

    BsonDocument result = command.run();
    Assert.assertEquals(0, result.getInt32("ok").getValue());
    Assert.assertTrue(result.containsKey("errmsg"));
    Assert.assertEquals(
        String.format("Index %s not initialized", INDEX_NAME),
        result.getString("errmsg").getValue());
    // Verify that throwIfUnavailableForQuerying() was called before the initialization check
    verify(mockSearchIndex).throwIfUnavailableForQuerying();
    Truth.assertThat(
            internalFailureCount(
                metricsFactory,
                IndexUnavailableException.class.getSimpleName(),
                ErrorType.USER_FACING_ERROR))
        .isEqualTo(1.0);
    command.handleSearchEnvoyMetadata(SearchEnvoyMetadata.getDefaultInstance());

    Truth.assertThat(command.run()).isEqualTo(result);
    retryRegistry.updateDynamicFeatureFlags(
        List.of(indexRetryConfig(DynamicFeatureFlagConfig.Phase.ENABLED)));

    var failure = Assert.assertThrows(CompletionException.class, command::run);
    Truth.assertThat(failure.getCause()).isInstanceOf(IndexUnavailableException.class);
    retryRegistry.updateDynamicFeatureFlags(
        List.of(indexRetryConfig(DynamicFeatureFlagConfig.Phase.DISABLED)));
    Truth.assertThat(command.run()).isEqualTo(result);
    Truth.assertThat(metricsFactory.counter("searchCommandNonexistentIndex").count())
        .isEqualTo(0.0);
  }

  @Test
  public void testIndexInInitialSyncRecordsIndexUnavailableMetric() throws Exception {
    var retryRegistry =
        new DynamicFeatureFlagRegistry(
            Optional.empty(), Optional.empty(), Optional.empty(), Optional.of(new ObjectId()));
    var metadata =
        new SearchCommandsRegister.BootstrapperMetadata(
            BOOTSTRAPPER_METADATA.mongotVersion(),
            BOOTSTRAPPER_METADATA.mongotHostName(),
            BOOTSTRAPPER_METADATA.mongoDbServerInfoProvider(),
            BOOTSTRAPPER_METADATA.featureFlags(),
            retryRegistry);
    var metricsFactory = mockMetricsFactory();
    IndexCatalog catalog = mock(IndexCatalog.class);
    InitializedIndexCatalog initializedIndexCatalog = mock(InitializedIndexCatalog.class);

    var searchIndexDefinition = SearchIndex.mockSearchDefinition(new ObjectId());
    var definitionGeneration = mockDefinitionGeneration(searchIndexDefinition);
    com.xgen.mongot.index.SearchIndex mockSearchIndex = SearchIndex.mockIndex(definitionGeneration);
    doThrow(
            new InitialSyncIndexUnavailableException(
                String.format(
                    "cannot query search index %s while in state %s",
                    searchIndexDefinition, IndexStatus.StatusCode.INITIAL_SYNC)))
        .when(mockSearchIndex)
        .throwIfUnavailableForQuerying();

    var indexGeneration = new IndexGeneration(mockSearchIndex, definitionGeneration);

    when(catalog.getIndex(DATABASE_NAME, COLLECTION_UUID, Optional.empty(), INDEX_NAME))
        .thenReturn(Optional.of(indexGeneration));

    SearchCommand command =
        new SearchCommand(
            new SearchCommand.Metrics(metricsFactory),
            getCursorManager(),
            catalog,
            initializedIndexCatalog,
            new IncompatibleIndexes(TestMeterRegistry.get()),
            SearchCommandDefinitionBuilder.builder()
                .db(DATABASE_NAME)
                .collectionName(COLLECTION_NAME)
                .collectionUuid(COLLECTION_UUID)
                .query(VALID_OPERATOR_QUERY)
                .build(),
            metadata,
            CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT,
            () -> null);

    BsonDocument result = command.run();
    Assert.assertEquals(0, result.getInt32("ok").getValue());
    Assert.assertTrue(result.getString("errmsg").getValue().contains("INITIAL_SYNC"));
    Truth.assertThat(
            internalFailureCount(
                metricsFactory,
                InitialSyncIndexUnavailableException.class.getSimpleName(),
                ErrorType.USER_FACING_ERROR))
        .isEqualTo(1.0);
    command.handleSearchEnvoyMetadata(SearchEnvoyMetadata.getDefaultInstance());

    Truth.assertThat(command.run()).isEqualTo(result);
    retryRegistry.updateDynamicFeatureFlags(
        List.of(indexRetryConfig(DynamicFeatureFlagConfig.Phase.ENABLED)));

    var failure = Assert.assertThrows(CompletionException.class, command::run);
    Truth.assertThat(failure.getCause()).isInstanceOf(IndexUnavailableException.class);
    retryRegistry.updateDynamicFeatureFlags(
        List.of(indexRetryConfig(DynamicFeatureFlagConfig.Phase.DISABLED)));
    Truth.assertThat(command.run()).isEqualTo(result);
    Truth.assertThat(metricsFactory.counter("searchCommandNonexistentIndex").count())
        .isEqualTo(0.0);
  }

  @Test
  public void searchVectorSearchOmittedNumCandidatesWithoutAutoFlag_returnsUserError()
      throws Exception {
    BsonDocument vectorSearchQuery =
        new BsonDocument()
            .append(
                "vectorSearch",
                new BsonDocument()
                    .append("path", new BsonString("embedding"))
                    .append(
                        "queryVector",
                        new BsonArray(
                            List.of(new BsonDouble(1.0), new BsonDouble(2.0), new BsonDouble(3.0))))
                    .append("limit", new BsonInt32(10)));

    SearchCommand command =
        new SearchCommand(
            new SearchCommand.Metrics(mockMetricsFactory()),
            getCursorManager(),
            new DefaultIndexCatalog(),
            new InitializedIndexCatalog(),
            new IncompatibleIndexes(TestMeterRegistry.get()),
            SearchCommandDefinitionBuilder.builder()
                .db(DATABASE_NAME)
                .collectionName(COLLECTION_NAME)
                .collectionUuid(COLLECTION_UUID)
                .query(vectorSearchQuery)
                .build(),
            new SearchCommandsRegister.BootstrapperMetadata(
                "testVersion",
                "localhost",
                () -> MongoDbServerInfo.EMPTY,
                FeatureFlags.withDefaults().disable(Feature.AUTO_NUM_CANDIDATES).build(),
                DynamicFeatureFlagRegistry.empty()),
            CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT,
            () -> null);

    BsonDocument result = command.run();

    Assert.assertEquals(0, result.getInt32("ok").getValue());
    Assert.assertTrue(
        result
            .getString("errmsg")
            .getValue()
            .contains("numCandidates is required for approximate vector search"));
  }

  @Test
  public void searchVectorSearchOmittedNumCandidatesWithoutAutoFlag_rejectsBeforeCatalogLookup()
      throws Exception {
    // The omitted-numCandidates flag-off validation must run before the catalog lookup and
    // embedding, so a request that would otherwise surface an index-unavailable error still gets
    // the expected "numCandidates is required" user error. Set up an index that is unavailable for
    // querying (INITIAL_SYNC) and assert the numCandidates error wins.
    IndexCatalog catalog = mock(IndexCatalog.class);
    InitializedIndexCatalog initializedIndexCatalog = mock(InitializedIndexCatalog.class);

    var searchIndexDefinition = SearchIndex.mockSearchDefinition(new ObjectId());
    var definitionGeneration = mockDefinitionGeneration(searchIndexDefinition);
    com.xgen.mongot.index.SearchIndex mockSearchIndex = SearchIndex.mockIndex(definitionGeneration);
    doThrow(
            new InitialSyncIndexUnavailableException(
                String.format(
                    "cannot query search index %s while in state %s",
                    searchIndexDefinition, IndexStatus.StatusCode.INITIAL_SYNC)))
        .when(mockSearchIndex)
        .throwIfUnavailableForQuerying();
    var indexGeneration = new IndexGeneration(mockSearchIndex, definitionGeneration);
    when(catalog.getIndex(DATABASE_NAME, COLLECTION_UUID, Optional.empty(), INDEX_NAME))
        .thenReturn(Optional.of(indexGeneration));

    BsonDocument vectorSearchQuery =
        new BsonDocument()
            .append(
                "vectorSearch",
                new BsonDocument()
                    .append("path", new BsonString("embedding"))
                    .append(
                        "queryVector",
                        new BsonArray(
                            List.of(new BsonDouble(1.0), new BsonDouble(2.0), new BsonDouble(3.0))))
                    .append("limit", new BsonInt32(10)));

    SearchCommand command =
        new SearchCommand(
            new SearchCommand.Metrics(mockMetricsFactory()),
            getCursorManager(),
            catalog,
            initializedIndexCatalog,
            new IncompatibleIndexes(TestMeterRegistry.get()),
            SearchCommandDefinitionBuilder.builder()
                .db(DATABASE_NAME)
                .collectionName(COLLECTION_NAME)
                .collectionUuid(COLLECTION_UUID)
                .query(vectorSearchQuery)
                .build(),
            new SearchCommandsRegister.BootstrapperMetadata(
                "testVersion",
                "localhost",
                () -> MongoDbServerInfo.EMPTY,
                FeatureFlags.withDefaults().disable(Feature.AUTO_NUM_CANDIDATES).build(),
                DynamicFeatureFlagRegistry.empty()),
            CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT,
            () -> null);

    BsonDocument result = command.run();

    Assert.assertEquals(0, result.getInt32("ok").getValue());
    String errmsg = result.getString("errmsg").getValue();
    Assert.assertTrue(errmsg.contains("numCandidates is required for approximate vector search"));
    Assert.assertFalse(errmsg.contains("INITIAL_SYNC"));
    verify(mockSearchIndex, never()).throwIfUnavailableForQuerying();
  }

  /**
   * An empty text query on an auto-embedding field must be rejected with the auto-embedding error
   * instead of falling through to the vector path without a queryVector, where it failed with the
   * internal "queryVector must be present" assertion (CLOUDP-454602).
   */
  @Test
  public void maybeEmbedSearchQuery_emptyQueryText_throwsEmptyQueryError() throws Exception {
    assertEmptyQueryTextRejected(new VectorSearchQueryInput.Text(""));
  }

  @Test
  public void maybeEmbedSearchQuery_emptyMultiModalQueryText_throwsEmptyQueryError()
      throws Exception {
    assertEmptyQueryTextRejected(new VectorSearchQueryInput.MultiModal(Optional.of("")));
  }

  @Test
  public void searchVectorSearch_emptyQueryText_returnsEmptyQueryError() throws Exception {
    assertSearchWithEmptyQueryTextReturnsEmptyQueryError(new BsonString(""));
  }

  @Test
  public void searchVectorSearch_emptyQueryDocumentText_returnsEmptyQueryError() throws Exception {
    assertSearchWithEmptyQueryTextReturnsEmptyQueryError(
        new BsonDocument().append("text", new BsonString("")));
  }

  /** An omitted query and queryVector is still reported as the missing-input parse error. */
  @Test
  public void searchVectorSearch_omittedQueryAndQueryVector_returnsMissingInputError()
      throws Exception {
    BsonDocument vectorSearch =
        new BsonDocument()
            .append("path", new BsonString("embedding"))
            .append("numCandidates", new BsonInt32(100))
            .append("limit", new BsonInt32(10));
    SearchCommand command =
        buildSearchCommand(
            new DefaultIndexCatalog(), new BsonDocument().append("vectorSearch", vectorSearch));

    BsonDocument result = command.run();

    Assert.assertEquals(0, result.getInt32("ok").getValue());
    Assert.assertTrue(
        result
            .getString("errmsg")
            .getValue()
            .contains("Exactly one and only one of query and queryVector can be present"));
  }

  private static void assertEmptyQueryTextRejected(VectorSearchQueryInput queryInput)
      throws Exception {
    SearchCommand command = buildSearchCommand(new DefaultIndexCatalog());
    SearchIndexDefinition autoEmbeddingDefinition =
        SearchIndex.mockAutoEmbeddingSearchDefinition(new ObjectId());

    InvalidQueryException exception =
        Assert.assertThrows(
            InvalidQueryException.class,
            () ->
                command.maybeEmbedSearchQuery(
                    vectorSearchQueryDefinition(queryInput), autoEmbeddingDefinition));
    Assert.assertEquals(
        "'query' field cannot be empty when querying Automated Embedding index",
        exception.getMessage());
  }

  private static void assertSearchWithEmptyQueryTextReturnsEmptyQueryError(BsonValue queryInput)
      throws Exception {
    SearchIndexDefinition autoEmbeddingDefinition =
        SearchIndex.mockAutoEmbeddingSearchDefinition(new ObjectId());
    var indexGeneration = mockIndexGeneration(autoEmbeddingDefinition);
    IndexCatalog catalog = mock(IndexCatalog.class);
    InitializedIndexCatalog initializedIndexCatalog = mock(InitializedIndexCatalog.class);
    InitializedIndex initializedIndex = mock(InitializedSearchIndex.class);
    when(initializedIndex.getDefinition()).thenReturn(autoEmbeddingDefinition);
    when(catalog.getIndex(DATABASE_NAME, COLLECTION_UUID, Optional.empty(), INDEX_NAME))
        .thenReturn(Optional.of(indexGeneration));
    when(initializedIndexCatalog.getIndex(indexGeneration.getGenerationId()))
        .thenReturn(Optional.of(initializedIndex));

    SearchCommand command =
        new SearchCommand(
            new SearchCommand.Metrics(mockMetricsFactory()),
            getCursorManager(),
            catalog,
            initializedIndexCatalog,
            new IncompatibleIndexes(TestMeterRegistry.get()),
            SearchCommandDefinitionBuilder.builder()
                .db(DATABASE_NAME)
                .collectionName(COLLECTION_NAME)
                .collectionUuid(COLLECTION_UUID)
                .query(emptyQueryTextVectorSearchQuery(queryInput))
                .build(),
            BOOTSTRAPPER_METADATA,
            CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT,
            () -> null);

    BsonDocument result = command.run();
    Assert.assertEquals(0, result.getInt32("ok").getValue());
    Assert.assertEquals(
        "'query' field cannot be empty when querying Automated Embedding index",
        result.getString("errmsg").getValue());
  }

  private static SearchCommand buildSearchCommand(IndexCatalog catalog) throws Exception {
    return buildSearchCommand(catalog, VALID_OPERATOR_QUERY);
  }

  private static SearchCommand buildSearchCommand(IndexCatalog catalog, BsonDocument query)
      throws Exception {
    return new SearchCommand(
        new SearchCommand.Metrics(mockMetricsFactory()),
        getCursorManager(),
        catalog,
        new InitializedIndexCatalog(),
        new IncompatibleIndexes(TestMeterRegistry.get()),
        SearchCommandDefinitionBuilder.builder()
            .db(DATABASE_NAME)
            .collectionName(COLLECTION_NAME)
            .collectionUuid(COLLECTION_UUID)
            .query(query)
            .build(),
        BOOTSTRAPPER_METADATA,
        CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT,
        () -> null);
  }

  private static OperatorQuery vectorSearchQueryDefinition(VectorSearchQueryInput queryInput) {
    VectorSearchCriteria criteria =
        ApproximateVectorQueryCriteriaBuilder.builder()
            .limit(10)
            .numCandidates(100)
            .query(queryInput)
            .path(FieldPath.parse("embedding"))
            .build();
    return new OperatorQuery(
        new VectorSearchOperator(null, criteria),
        INDEX_NAME,
        Count.DEFAULT,
        Optional.empty(),
        Optional.empty(),
        false,
        false,
        false,
        Optional.empty(),
        Optional.empty(),
        Optional.empty());
  }

  private static BsonDocument emptyQueryTextVectorSearchQuery(BsonValue queryInput) {
    return new BsonDocument()
        .append(
            "vectorSearch",
            new BsonDocument()
                .append("path", new BsonString("embedding"))
                .append("query", queryInput)
                .append("numCandidates", new BsonInt32(100))
                .append("limit", new BsonInt32(10)));
  }

  /**
   * Seeds an aggregation explainer on the FakeExplain QueryInfo so {@code recordCursorId} has
   * something to stamp. Without that, both flag states would no-op and the gate would be invisible.
   */
  private static void runSearchAndAssertCursorId(
      FeatureFlags featureFlags, Optional<Long> expectedCursorId) throws Exception {
    var explainInformation = SearchExplainInformationBuilder.newBuilder().build();
    SearchCommand command =
        new SearchCommand(
            new SearchCommand.Metrics(mockMetricsFactory()),
            getCursorManager(),
            new DefaultIndexCatalog(),
            new InitializedIndexCatalog(),
            new IncompatibleIndexes(TestMeterRegistry.get()),
            SearchCommandDefinitionBuilder.builder()
                .db(DATABASE_NAME)
                .collectionName(COLLECTION_NAME)
                .collectionUuid(COLLECTION_UUID)
                .query(VALID_OPERATOR_QUERY)
                .build(),
            new SearchCommandsRegister.BootstrapperMetadata(
                "testVersion",
                "localhost",
                () -> MongoDbServerInfo.EMPTY,
                featureFlags,
                DynamicFeatureFlagRegistry.empty()),
            CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT,
            () -> null);

    try (var unused = FakeExplain.setup(Explain.Verbosity.EXECUTION_STATS, 0, explainInformation)) {
      AggregationFeatureExplainer.recordNodeStats(new AggregationNodeStats("root", List.of()));
      command.run();

      var builder = new SearchExplainInformationBuilder();
      var explainer =
          Explain.getQueryInfo()
              .orElseThrow()
              .getFeatureExplainer(
                  AggregationFeatureExplainer.class, AggregationFeatureExplainer::new);
      if (expectedCursorId.isEmpty()) {
        // queryStats is present from recordNodeStats, so emit requires a cursor. Throwing is how
        // the flag-off path is visible now that emit no longer fabricates an empty id.
        Assert.assertThrows(
            IllegalStateException.class,
            () -> explainer.emitExplanation(Explain.Verbosity.EXECUTION_STATS, builder));
        return;
      }
      explainer.emitExplanation(Explain.Verbosity.EXECUTION_STATS, builder);
      Truth.assertThat(builder.build().aggregation().orElseThrow().query().orElseThrow().cursorId())
          .isEqualTo(expectedCursorId);
    }
  }

  private static MongotCursorManager getCursorManager() throws Exception {
    return getCursorManager((timer) -> {});
  }

  private static MongotCursorManager getCursorManager(QueryBatchTimerRecorder metricRecorder)
      throws Exception {
    return getCursorManager(metricRecorder, Optional.empty());
  }

  private static MongotCursorManager getCursorManager(SearchExplainInformation explain)
      throws Exception {
    return getCursorManager((timer) -> {}, Optional.of(explain));
  }

  /**
   * Creates a MongotCursorManager that behaves in a way that can elicit the desired behavior from
   * SearchCommand.
   */
  private static MongotCursorManager getCursorManager(
      QueryBatchTimerRecorder metricRecorder, Optional<SearchExplainInformation> explainResult)
      throws Exception {
    MongotCursorManager cursorManager = mock(MongotCursorManager.class);

    MongotIntermediateCursorBatch intermediateCursorBatch = mockIntermediateMongotCursorBatch();

    MongotCursorBatch metaBatch = intermediateCursorBatch.cursors().get(0);
    MongotCursorBatch batch = intermediateCursorBatch.cursors().get(1);

    // Only return a cursor id for valid (database, collection uuid, index name)
    // We don't actually care about the collection named, since it's just used to populate the "ns"
    // key in the cursor batch response.
    // Only return a cursor batch for the correct cursor id.
    when(cursorManager.newCursor(any(), any(), any(), any(), any(Query.class), any(), any(), any()))
        .then(
            invocation -> {
              String databaseName = invocation.getArgument(0);
              UUID collectionUuid = invocation.getArgument(2);
              SearchQuery query = invocation.getArgument(4);
              if (!databaseName.equals(DATABASE_NAME)
                  || !collectionUuid.equals(COLLECTION_UUID)
                  || !query.index().equals(INDEX_NAME)) {
                throw new InvalidQueryException("invalid index");
              }

              return new SearchCursorInfo(
                  MOCK_SEARCH_CURSOR_ID, new MetaResults(CountResult.lowerBoundCount(1000)));
            });

    when(cursorManager.newIntermediateCursors(
            any(), any(), any(), any(), any(), anyInt(), any(), any(), any()))
        .then(
            invocation -> {
              String databaseName = invocation.getArgument(0);
              UUID collectionUuid = invocation.getArgument(2);
              SearchQuery query = invocation.getArgument(4);
              if (!databaseName.equals(DATABASE_NAME)
                  || !collectionUuid.equals(COLLECTION_UUID)
                  || !query.index().equals(INDEX_NAME)) {
                throw new InvalidQueryException("invalid index");
              }

              return new IntermediateSearchCursorInfo(MOCK_SEARCH_CURSOR_ID, MOCK_META_CURSOR_ID);
            });

    when(cursorManager.getNextBatch(anyLong(), any(), any()))
        .then(
            invocation -> {
              long cursorId = invocation.getArgument(0);
              if (cursorId != MOCK_SEARCH_CURSOR_ID && cursorId != MOCK_META_CURSOR_ID) {
                throw new MongotCursorNotFoundException(cursorId);
              }

              return new MongotCursorResultInfo(
                  batch.getCursorExpected().getCursorId() == MongotCursorResult.EXHAUSTED_CURSOR_ID,
                  cursorId == MOCK_SEARCH_CURSOR_ID
                      ? batch.getCursorExpected().getBatch()
                      : metaBatch.getCursorExpected().getBatch(),
                  explainResult,
                  batch.getCursorExpected().getNamespace());
            });

    when(cursorManager.getIndexQueryBatchTimerRecorder(anyLong())).thenReturn(metricRecorder);

    return cursorManager;
  }

  /**
   * Like getCursorManager() but captures the SearchQuery passed to newCursor into {@code queryOut}.
   */
  private static MongotCursorManager getCursorManagerCapturingQuery(
      AtomicReference<SearchQuery> queryOut) throws Exception {
    MongotCursorManager cursorManager = mock(MongotCursorManager.class);
    MongotIntermediateCursorBatch intermediateCursorBatch = mockIntermediateMongotCursorBatch();
    MongotCursorBatch metaBatch = intermediateCursorBatch.cursors().get(0);
    MongotCursorBatch batch = intermediateCursorBatch.cursors().get(1);

    when(cursorManager.newCursor(any(), any(), any(), any(), any(Query.class), any(), any(), any()))
        .then(
            invocation -> {
              SearchQuery query = invocation.getArgument(4);
              queryOut.set(query);
              String databaseName = invocation.getArgument(0);
              UUID collectionUuid = invocation.getArgument(2);
              if (!databaseName.equals(DATABASE_NAME)
                  || !collectionUuid.equals(COLLECTION_UUID)
                  || !query.index().equals(INDEX_NAME)) {
                throw new InvalidQueryException("invalid index");
              }
              return new SearchCursorInfo(
                  MOCK_SEARCH_CURSOR_ID, new MetaResults(CountResult.lowerBoundCount(1000)));
            });

    when(cursorManager.newIntermediateCursors(
            any(), any(), any(), any(), any(), anyInt(), any(), any(), any()))
        .then(
            invocation -> {
              SearchQuery query = invocation.getArgument(4);
              queryOut.set(query);
              String databaseName = invocation.getArgument(0);
              UUID collectionUuid = invocation.getArgument(2);
              if (!databaseName.equals(DATABASE_NAME)
                  || !collectionUuid.equals(COLLECTION_UUID)
                  || !query.index().equals(INDEX_NAME)) {
                throw new InvalidQueryException("invalid index");
              }
              return new IntermediateSearchCursorInfo(MOCK_SEARCH_CURSOR_ID, MOCK_META_CURSOR_ID);
            });

    when(cursorManager.getNextBatch(anyLong(), any(), any()))
        .then(
            invocation -> {
              long cursorId = invocation.getArgument(0);
              if (cursorId != MOCK_SEARCH_CURSOR_ID && cursorId != MOCK_META_CURSOR_ID) {
                throw new MongotCursorNotFoundException(cursorId);
              }
              return new MongotCursorResultInfo(
                  batch.getCursorExpected().getCursorId() == MongotCursorResult.EXHAUSTED_CURSOR_ID,
                  cursorId == MOCK_SEARCH_CURSOR_ID
                      ? batch.getCursorExpected().getBatch()
                      : metaBatch.getCursorExpected().getBatch(),
                  Optional.empty(),
                  batch.getCursorExpected().getNamespace());
            });

    when(cursorManager.getIndexQueryBatchTimerRecorder(anyLong())).thenReturn((timer) -> {});

    return cursorManager;
  }

  /** Creates a MongotCursorManager that throws an IOException when getting the batch. */
  private MongotCursorManager getThrowableCursorManager(
      Supplier<? extends Throwable> throwableSupplier) throws Exception {
    MongotCursorManager cursorManager = mock(MongotCursorManager.class);
    when(cursorManager.newCursor(any(), any(), any(), any(), any(Query.class), any(), any(), any()))
        .thenReturn(
            new SearchCursorInfo(
                MOCK_SEARCH_CURSOR_ID, new MetaResults(CountResult.lowerBoundCount(1000))));

    when(cursorManager.newIntermediateCursors(
            any(), any(), any(), any(), any(), anyInt(), any(), any(), any()))
        .thenReturn(new IntermediateSearchCursorInfo(MOCK_SEARCH_CURSOR_ID, MOCK_META_CURSOR_ID));

    when(cursorManager.getNextBatch(anyLong(), any(), any())).thenThrow(throwableSupplier.get());

    when(cursorManager.getIndexQueryBatchTimerRecorder(anyLong()))
        .thenReturn(
            (timer) -> {
              // no-op QueryBatchTimerRecorder
            });

    return cursorManager;
  }
}
