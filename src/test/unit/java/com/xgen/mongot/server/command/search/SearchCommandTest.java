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
import com.xgen.mongot.featureflag.FeatureFlags;
import com.xgen.mongot.index.CountResult;
import com.xgen.mongot.index.IndexGeneration;
import com.xgen.mongot.index.InitializedSearchIndex;
import com.xgen.mongot.index.MetaResults;
import com.xgen.mongot.index.definition.SearchIndexCapabilities;
import com.xgen.mongot.index.definition.SearchIndexDefinition;
import com.xgen.mongot.index.lucene.explain.information.LuceneQuerySpecification;
import com.xgen.mongot.index.lucene.explain.information.MetadataExplainInformation;
import com.xgen.mongot.index.lucene.explain.information.SearchExplainInformation;
import com.xgen.mongot.index.lucene.explain.information.SearchExplainInformationBuilder;
import com.xgen.mongot.index.lucene.explain.tracing.Explain;
import com.xgen.mongot.index.query.InvalidQueryException;
import com.xgen.mongot.index.query.SearchQuery;
import com.xgen.mongot.index.status.IndexStatus;
import com.xgen.mongot.metrics.MetricsFactory;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.mongodb.MongoDbServerInfo;
import com.xgen.mongot.util.mongodb.MongoDbVersion;
import com.xgen.testing.mongot.index.definition.SearchIndexDefinitionBuilder;
import com.xgen.testing.mongot.index.lucene.explain.information.QueryExplainInformationBuilder;
import com.xgen.testing.mongot.index.lucene.explain.information.TermQueryBuilder;
import com.xgen.testing.mongot.index.lucene.explain.tracing.FakeExplain;
import com.xgen.testing.mongot.mock.index.SearchIndex;
import com.xgen.testing.mongot.server.command.search.definition.request.CursorOptionsDefinitionBuilder;
import com.xgen.testing.mongot.server.command.search.definition.request.ExplainDefinitionBuilder;
import com.xgen.testing.mongot.server.command.search.definition.request.SearchCommandDefinitionBuilder;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
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
          "testVersion", "localhost", () -> MongoDbServerInfo.EMPTY, FeatureFlags.getDefault());
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

  @DataPoints
  public static final List<MongoDbVersion> mdbVersions =
      List.of(new MongoDbVersion(8, 1, 0), new MongoDbVersion(7, 0, 0));

  @Test
  public void testValidIndex() throws Exception {
    SearchCommand command =
        new SearchCommand(
            new SearchCommand.Metrics(mockMetricsFactory()),
            getCursorManager(),
            new DefaultIndexCatalog(),
            new InitializedIndexCatalog(),
            SearchCommandDefinitionBuilder.builder()
                .db(DATABASE_NAME)
                .collectionName(COLLECTION_NAME)
                .collectionUuid(COLLECTION_UUID)
                .query(VALID_OPERATOR_QUERY)
                .build(),
            BOOTSTRAPPER_METADATA,
            CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT);

    BsonDocument result = command.run();
    BsonDocument expected = mockInitialMongotCursorBatch().toBson();
    Assert.assertEquals(expected, result);
    Assert.assertEquals(List.of(MOCK_SEARCH_CURSOR_ID), command.getCreatedCursorIds());
  }

  @Test
  public void testValidIndexIntermediate() throws Exception {
    SearchCommand command =
        new SearchCommand(
            new SearchCommand.Metrics(mockMetricsFactory()),
            getCursorManager(),
            new DefaultIndexCatalog(),
            new InitializedIndexCatalog(),
            SearchCommandDefinitionBuilder.builder()
                .db(DATABASE_NAME)
                .collectionName(COLLECTION_NAME)
                .collectionUuid(COLLECTION_UUID)
                .query(VALID_OPERATOR_QUERY)
                .intermediate(1)
                .build(),
            BOOTSTRAPPER_METADATA,
            CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT);

    BsonDocument result = command.run();
    BsonDocument expected = mockIntermediateMongotCursorBatch().toBson();
    Assert.assertEquals(expected, result);
    Assert.assertEquals(
        Arrays.asList(MOCK_SEARCH_CURSOR_ID, MOCK_META_CURSOR_ID), command.getCreatedCursorIds());
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
                FeatureFlags.getDefault()),
            CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT);

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
            CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT);

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
            SearchCommandDefinitionBuilder.builder()
                .db(DATABASE_NAME)
                .collectionName(COLLECTION_NAME)
                .collectionUuid(COLLECTION_UUID)
                .query(VALID_OPERATOR_QUERY)
                .build(),
            BOOTSTRAPPER_METADATA,
            CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT);

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
            SearchCommandDefinitionBuilder.builder()
                .db(DATABASE_NAME)
                .collectionName(COLLECTION_NAME)
                .collectionUuid(COLLECTION_UUID)
                .query(VALID_OPERATOR_QUERY)
                .intermediate(1)
                .build(),
            BOOTSTRAPPER_METADATA,
            CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT);

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
            SearchCommandDefinitionBuilder.builder()
                .db("invalid")
                .collectionName(COLLECTION_NAME)
                .collectionUuid(COLLECTION_UUID)
                .query(VALID_OPERATOR_QUERY)
                .intermediate(1)
                .build(),
            BOOTSTRAPPER_METADATA,
            CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT);

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
            SearchCommandDefinitionBuilder.builder()
                .db(DATABASE_NAME)
                .collectionName(COLLECTION_NAME)
                .collectionUuid(UUID.randomUUID())
                .query(VALID_OPERATOR_QUERY)
                .build(),
            BOOTSTRAPPER_METADATA,
            CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT);

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
            SearchCommandDefinitionBuilder.builder()
                .db(DATABASE_NAME)
                .collectionName(COLLECTION_NAME)
                .collectionUuid(UUID.randomUUID())
                .query(VALID_OPERATOR_QUERY)
                .intermediate(1)
                .build(),
            BOOTSTRAPPER_METADATA,
            CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT);

    BsonDocument result = command.run();
    Assert.assertEquals(INVALID_INDEX_RESPONSE, result);
  }

  @Test
  public void testInvalidIndexName() throws Exception {
    SearchCommand command =
        new SearchCommand(
            new SearchCommand.Metrics(mockMetricsFactory()),
            getCursorManager(),
            new DefaultIndexCatalog(),
            new InitializedIndexCatalog(),
            SearchCommandDefinitionBuilder.builder()
                .db(DATABASE_NAME)
                .collectionName(COLLECTION_NAME)
                .collectionUuid(COLLECTION_UUID)
                .query(INVALID_INDEX_QUERY)
                .build(),
            BOOTSTRAPPER_METADATA,
            CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT);

    BsonDocument result = command.run();
    Assert.assertEquals(INVALID_INDEX_RESPONSE, result);
  }

  @Test
  public void testInvalidIndexNameIntermediate() throws Exception {
    SearchCommand command =
        new SearchCommand(
            new SearchCommand.Metrics(mockMetricsFactory()),
            getCursorManager(),
            new DefaultIndexCatalog(),
            new InitializedIndexCatalog(),
            SearchCommandDefinitionBuilder.builder()
                .db(DATABASE_NAME)
                .collectionName(COLLECTION_NAME)
                .collectionUuid(COLLECTION_UUID)
                .query(INVALID_INDEX_QUERY)
                .intermediate(1)
                .build(),
            BOOTSTRAPPER_METADATA,
            CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT);

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
            SearchCommandDefinitionBuilder.builder()
                .db(DATABASE_NAME)
                .collectionName(COLLECTION_NAME)
                .collectionUuid(COLLECTION_UUID)
                .query(INVALID_OPERATOR_QUERY)
                .build(),
            BOOTSTRAPPER_METADATA,
            CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT);

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
            SearchCommandDefinitionBuilder.builder()
                .db(DATABASE_NAME)
                .collectionName(COLLECTION_NAME)
                .collectionUuid(COLLECTION_UUID)
                .query(INVALID_OPERATOR_QUERY)
                .intermediate(1)
                .build(),
            BOOTSTRAPPER_METADATA,
            CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT);

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
            SearchCommandDefinitionBuilder.builder()
                .db(DATABASE_NAME)
                .collectionName(COLLECTION_NAME)
                .collectionUuid(COLLECTION_UUID)
                .query(INVALID_COLLECTOR_QUERY)
                .build(),
            BOOTSTRAPPER_METADATA,
            CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT);

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
            SearchCommandDefinitionBuilder.builder()
                .db(DATABASE_NAME)
                .collectionName(COLLECTION_NAME)
                .collectionUuid(COLLECTION_UUID)
                .query(INVALID_COLLECTOR_QUERY)
                .intermediate(1)
                .build(),
            BOOTSTRAPPER_METADATA,
            CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT);

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
            SearchCommandDefinitionBuilder.builder()
                .db(DATABASE_NAME)
                .collectionName(COLLECTION_NAME)
                .collectionUuid(COLLECTION_UUID)
                .query(UNKNOWN_QUERY)
                .build(),
            BOOTSTRAPPER_METADATA,
            CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT);

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
            SearchCommandDefinitionBuilder.builder()
                .db(DATABASE_NAME)
                .collectionName(COLLECTION_NAME)
                .collectionUuid(COLLECTION_UUID)
                .query(UNKNOWN_QUERY)
                .intermediate(1)
                .build(),
            BOOTSTRAPPER_METADATA,
            CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT);

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
            SearchCommandDefinitionBuilder.builder()
                .db(DATABASE_NAME)
                .collectionName(COLLECTION_NAME)
                .collectionUuid(COLLECTION_UUID)
                .query(QUERY_WITH_VALID_COUNT)
                .build(),
            BOOTSTRAPPER_METADATA,
            CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT);

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
            SearchCommandDefinitionBuilder.builder()
                .db(DATABASE_NAME)
                .collectionName(COLLECTION_NAME)
                .collectionUuid(COLLECTION_UUID)
                .query(QUERY_WITH_VALID_COUNT)
                .intermediate(1)
                .build(),
            BOOTSTRAPPER_METADATA,
            CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT);

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
            SearchCommandDefinitionBuilder.builder()
                .db(DATABASE_NAME)
                .collectionName(COLLECTION_NAME)
                .collectionUuid(COLLECTION_UUID)
                .query(QUERY_WITH_INVALID_COUNT)
                .build(),
            BOOTSTRAPPER_METADATA,
            CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT);

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
            SearchCommandDefinitionBuilder.builder()
                .db(DATABASE_NAME)
                .collectionName(COLLECTION_NAME)
                .collectionUuid(COLLECTION_UUID)
                .query(QUERY_WITH_INVALID_COUNT)
                .intermediate(1)
                .build(),
            BOOTSTRAPPER_METADATA,
            CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT);

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
            SearchCommandDefinitionBuilder.builder()
                .db(DATABASE_NAME)
                .collectionName(COLLECTION_NAME)
                .collectionUuid(COLLECTION_UUID)
                .query(VALID_OPERATOR_QUERY)
                .intermediate(1)
                .cursorOptions(CursorOptionsDefinitionBuilder.builder().docsRequested(25).build())
                .build(),
            BOOTSTRAPPER_METADATA,
            CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT);
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
            SearchCommandDefinitionBuilder.builder()
                .db(DATABASE_NAME)
                .collectionName(COLLECTION_NAME)
                .collectionUuid(COLLECTION_UUID)
                .query(VALID_OPERATOR_QUERY)
                .build(),
            BOOTSTRAPPER_METADATA,
            CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT);

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
            SearchCommandDefinitionBuilder.builder()
                .db(DATABASE_NAME)
                .collectionName(COLLECTION_NAME)
                .collectionUuid(COLLECTION_UUID)
                .query(VALID_OPERATOR_QUERY)
                .intermediate(1)
                .build(),
            BOOTSTRAPPER_METADATA,
            CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT);

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
            SearchCommandDefinitionBuilder.builder()
                .db(DATABASE_NAME)
                .collectionName(COLLECTION_NAME)
                .collectionUuid(COLLECTION_UUID)
                .query(VALID_OPERATOR_QUERY)
                .build(),
            BOOTSTRAPPER_METADATA,
            CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT);

    BsonDocument result = command.run();

    Assert.assertEquals(1, (int) command.getSearchCommandInvalidQueries().count());
    Assert.assertEquals(1, (int) command.getSearchCommandUnwrappedExceptions().count());

    // Verify response format
    Assert.assertEquals(0, result.getInt32("ok").getValue());
    Assert.assertTrue(result.getString("errmsg").getValue().contains("wrapped user error"));
  }

  @Test
  public void unreachableErrorThrown_metricIncremented() throws Exception {
    var metricsFactory = mockMetricsFactory();
    SearchCommand command =
        new SearchCommand(
            new SearchCommand.Metrics(metricsFactory),
            getThrowableCursorManager(Check::unreachableError),
            new DefaultIndexCatalog(),
            new InitializedIndexCatalog(),
            SearchCommandDefinitionBuilder.builder()
                .db(DATABASE_NAME)
                .collectionName(COLLECTION_NAME)
                .collectionUuid(COLLECTION_UUID)
                .query(VALID_OPERATOR_QUERY)
                .build(),
            BOOTSTRAPPER_METADATA,
            CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT);

    Assert.assertThrows(AssertionError.class, command::run);
    Truth.assertThat(
            metricsFactory
                .counter(
                    "searchCommandInternalFailures",
                    Tags.of("throwableName", AssertionError.class.getSimpleName()))
                .count())
        .isEqualTo(1.0);
  }

  @Test
  public void intermediateBatch_unreachableErrorThrown_metricIncremented() throws Exception {
    var metricsFactory = mockMetricsFactory();
    SearchCommand command =
        new SearchCommand(
            new SearchCommand.Metrics(metricsFactory),
            getThrowableCursorManager(Check::unreachableError),
            new DefaultIndexCatalog(),
            new InitializedIndexCatalog(),
            SearchCommandDefinitionBuilder.builder()
                .db(DATABASE_NAME)
                .collectionName(COLLECTION_NAME)
                .collectionUuid(COLLECTION_UUID)
                .query(VALID_OPERATOR_QUERY)
                .intermediate(1)
                .build(),
            BOOTSTRAPPER_METADATA,
            CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT);

    Assert.assertThrows(AssertionError.class, command::run);
    Truth.assertThat(
            metricsFactory
                .counter(
                    "searchCommandInternalFailures",
                    Tags.of("throwableName", AssertionError.class.getSimpleName()))
                .count())
        .isEqualTo(1.0);
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
                FeatureFlags.getDefault()),
            CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT);

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
            CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT);

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
            CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT);

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
            CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT);

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
            SearchCommandDefinitionBuilder.builder()
                .db(DATABASE_NAME)
                .collectionName(COLLECTION_NAME)
                .collectionUuid(COLLECTION_UUID)
                .query(VALID_COLLECTOR_QUERY)
                .build(),
            BOOTSTRAPPER_METADATA,
            CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT);

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
            CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT);

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
            SearchCommandDefinitionBuilder.builder()
                .db(DATABASE_NAME)
                .collectionName(COLLECTION_NAME)
                .collectionUuid(COLLECTION_UUID)
                .query(VALID_COLLECTOR_QUERY)
                .intermediate(1)
                .build(),
            BOOTSTRAPPER_METADATA,
            CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT);

    BsonDocument result = command.run();
    Assert.assertEquals(1, result.getInt32("ok").getValue());
    Assert.assertFalse(result.containsKey("errmsg"));
  }

  @Test
  public void testIndexNotInitializedReturnsErrorResponse() throws Exception {
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
            new SearchCommand.Metrics(mockMetricsFactory()),
            getCursorManager(),
            catalog,
            initializedIndexCatalog,
            SearchCommandDefinitionBuilder.builder()
                .db(DATABASE_NAME)
                .collectionName(COLLECTION_NAME)
                .collectionUuid(COLLECTION_UUID)
                .query(VALID_OPERATOR_QUERY)
                .build(),
            BOOTSTRAPPER_METADATA,
            CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT);

    BsonDocument result = command.run();
    Assert.assertEquals(0, result.getInt32("ok").getValue());
    Assert.assertTrue(result.containsKey("errmsg"));
    Assert.assertEquals(
        String.format("Index %s not initialized", INDEX_NAME),
        result.getString("errmsg").getValue());
    // Verify that throwIfUnavailableForQuerying() was called before the initialization check
    verify(mockSearchIndex).throwIfUnavailableForQuerying();
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
   * Creates a MongotCursorManager that behaves in a way that can illicit the desired behavior from
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
    when(cursorManager.newCursor(any(), any(), any(), any(), any(), any(), any(), any()))
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

  /** Creates a MongotCursorManager that throws an IOException when getting the batch. */
  private MongotCursorManager getThrowableCursorManager(
      Supplier<? extends Throwable> throwableSupplier) throws Exception {
    MongotCursorManager cursorManager = mock(MongotCursorManager.class);
    when(cursorManager.newCursor(any(), any(), any(), any(), any(), any(), any(), any()))
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
