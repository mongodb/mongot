package com.xgen.mongot.index;

import static com.xgen.mongot.util.bson.FloatVector.OriginalType.NATIVE;
import static com.xgen.testing.BsonTestUtils.bson;
import static com.xgen.testing.mongot.mock.index.SearchIndex.MOCK_INDEX_NAME;
import static org.junit.Assert.assertEquals;

import com.xgen.mongot.cursor.batch.QueryCursorOptions;
import com.xgen.mongot.index.IndexMetricsUpdater.QueryingMetricsUpdater.QueryFeaturesMetricsUpdater;
import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.lucene.explain.tracing.Explain;
import com.xgen.mongot.index.query.Query;
import com.xgen.mongot.index.query.ReturnScope;
import com.xgen.mongot.index.query.SearchQuery;
import com.xgen.mongot.index.query.Tracking;
import com.xgen.mongot.index.query.VectorSearchQuery;
import com.xgen.mongot.index.query.collectors.Collector;
import com.xgen.mongot.index.query.operators.CompoundOperator;
import com.xgen.mongot.index.query.operators.Operator;
import com.xgen.mongot.index.query.operators.TextOperator;
import com.xgen.mongot.index.query.operators.VectorSearchCriteria;
import com.xgen.mongot.index.query.operators.mql.MqlFilterOperator;
import com.xgen.mongot.index.query.scores.Score;
import com.xgen.mongot.index.query.sort.MongotSortField;
import com.xgen.mongot.index.query.sort.NullEmptySortPosition;
import com.xgen.mongot.index.query.sort.SequenceToken;
import com.xgen.mongot.index.query.sort.SortOrder;
import com.xgen.mongot.index.query.sort.UserFieldSortOptions;
import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.bson.Vector;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.testing.mongot.cursor.batch.QueryCursorOptionsBuilder;
import com.xgen.testing.mongot.index.IndexMetricsUpdaterBuilder;
import com.xgen.testing.mongot.index.IndexMetricsUpdaterBuilder.QueryingMetricsUpdaterBuilder.QueryFeaturesMetricsUpdaterBuilder;
import com.xgen.testing.mongot.index.path.string.UnresolvedStringPathBuilder;
import com.xgen.testing.mongot.index.query.ApproximateVectorQueryCriteriaBuilder;
import com.xgen.testing.mongot.index.query.CollectorQueryBuilder;
import com.xgen.testing.mongot.index.query.ExactVectorCriteriaBuilder;
import com.xgen.testing.mongot.index.query.OperatorQueryBuilder;
import com.xgen.testing.mongot.index.query.VectorQueryBuilder;
import com.xgen.testing.mongot.index.query.collectors.CollectorBuilder;
import com.xgen.testing.mongot.index.query.collectors.FacetDefinitionBuilder;
import com.xgen.testing.mongot.index.query.highlights.UnresolvedHighlightBuilder;
import com.xgen.testing.mongot.index.query.operators.FuzzyOptionBuilder;
import com.xgen.testing.mongot.index.query.operators.OperatorBuilder;
import com.xgen.testing.mongot.index.query.operators.TermOperatorBuilder;
import com.xgen.testing.mongot.index.query.scores.ScoreBuilder;
import com.xgen.testing.mongot.index.query.sort.SortOptionsBuilder;
import com.xgen.testing.mongot.index.query.sort.SortSpecBuilder;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.lucene.search.ScoreDoc;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.RawBsonDocument;
import org.junit.Assert;
import org.junit.Test;

public class TestQueryMetricsRecorder {

  private static final double EPSILON = 0.0;

  @Test
  public void testCollectorQueryMetric() {
    var queryFeaturesStatsUpdater =
        IndexMetricsUpdaterBuilder.QueryingMetricsUpdaterBuilder.QueryFeaturesMetricsUpdaterBuilder
            .empty();
    var queryMetricsRecorder = new QueryMetricsRecorder(queryFeaturesStatsUpdater);

    SearchQuery query =
        CollectorQueryBuilder.builder()
            .collector(
                CollectorBuilder.facet()
                    .facetDefinitions(
                        Map.of(
                            "stringFacet",
                            /**/ FacetDefinitionBuilder.string()
                                .numBuckets(5)
                                .path("path")
                                .build()))
                    .operator(OperatorBuilder.text().path("path").query("empty").build())
                    .build())
            .returnStoredSource(false)
            .build();

    queryMetricsRecorder.record(query, QueryCursorOptions.empty());

    Assert.assertEquals(
        1, queryFeaturesStatsUpdater.getCollectorTypeCounter(Collector.Type.FACET).count(), 0);
  }

  @Test
  public void testScoreTypeQueryMetric() {
    var queryFeaturesStatsUpdater =
        IndexMetricsUpdaterBuilder.QueryingMetricsUpdaterBuilder.QueryFeaturesMetricsUpdaterBuilder
            .empty();
    var queryMetricsRecorder = new QueryMetricsRecorder(queryFeaturesStatsUpdater);

    SearchQuery query =
        OperatorQueryBuilder.builder()
            .operator(
                OperatorBuilder.term()
                    .path("path")
                    .prefix(true)
                    .score(ScoreBuilder.constant().value(16).build())
                    .query("query")
                    .build())
            .returnStoredSource(false)
            .build();

    queryMetricsRecorder.record(query, QueryCursorOptions.empty());

    Assert.assertEquals(
        1, queryFeaturesStatsUpdater.getScoreTypeCounter(Score.Type.CONSTANT).count(), EPSILON);
  }

  @Test
  public void testQueryHighlight() {
    var queryFeaturesStatsUpdater =
        IndexMetricsUpdaterBuilder.QueryingMetricsUpdaterBuilder.QueryFeaturesMetricsUpdaterBuilder
            .empty();
    var queryMetricsRecorder = new QueryMetricsRecorder(queryFeaturesStatsUpdater);

    var query =
        OperatorQueryBuilder.builder()
            .operator(
                OperatorBuilder.exists()
                    .path("foo")
                    .score(ScoreBuilder.constant().value(1).build())
                    .build())
            .index(MOCK_INDEX_NAME)
            .highlight(UnresolvedHighlightBuilder.builder().path("foo").build())
            .returnStoredSource(false)
            .build();

    queryMetricsRecorder.record(query, QueryCursorOptions.empty());

    Assert.assertEquals(1, queryFeaturesStatsUpdater.getHighlightingCounter().count(), 0);
    Assert.assertEquals(0, queryFeaturesStatsUpdater.getWildcardPathsCounter().count(), 0);
  }

  @Test
  public void testQueryHighlightWildcardPaths() {
    var queryFeaturesStatsUpdater =
        IndexMetricsUpdaterBuilder.QueryingMetricsUpdaterBuilder.QueryFeaturesMetricsUpdaterBuilder
            .empty();
    var queryMetricsRecorder = new QueryMetricsRecorder(queryFeaturesStatsUpdater);

    var query =
        OperatorQueryBuilder.builder()
            .operator(
                OperatorBuilder.exists()
                    .path("foo")
                    .score(ScoreBuilder.constant().value(1).build())
                    .build())
            .index(MOCK_INDEX_NAME)
            .highlight(
                UnresolvedHighlightBuilder.builder()
                    .path(UnresolvedStringPathBuilder.wildcardPath("foo*"))
                    .build())
            .returnStoredSource(false)
            .build();

    queryMetricsRecorder.record(query, QueryCursorOptions.empty());

    Assert.assertEquals(1, queryFeaturesStatsUpdater.getHighlightingCounter().count(), 0);
    Assert.assertEquals(1, queryFeaturesStatsUpdater.getWildcardPathsCounter().count(), 0);
  }

  @Test
  public void testQueryHighlightMultipleWildcardPaths() {
    var queryFeaturesStatsUpdater =
        IndexMetricsUpdaterBuilder.QueryingMetricsUpdaterBuilder.QueryFeaturesMetricsUpdaterBuilder
            .empty();
    var queryMetricsRecorder = new QueryMetricsRecorder(queryFeaturesStatsUpdater);

    var query =
        OperatorQueryBuilder.builder()
            .operator(
                OperatorBuilder.exists()
                    .path("foo")
                    .score(ScoreBuilder.constant().value(1).build())
                    .build())
            .index(MOCK_INDEX_NAME)
            .highlight(
                UnresolvedHighlightBuilder.builder()
                    .path(UnresolvedStringPathBuilder.wildcardPath("foo*"))
                    .path(UnresolvedStringPathBuilder.wildcardPath("bar*"))
                    .build())
            .returnStoredSource(false)
            .build();

    queryMetricsRecorder.record(query, QueryCursorOptions.empty());

    Assert.assertEquals(1, queryFeaturesStatsUpdater.getHighlightingCounter().count(), 0);
    Assert.assertEquals(1, queryFeaturesStatsUpdater.getWildcardPathsCounter().count(), 0);
  }

  @Test
  public void testSearchBeforeMetric() {
    var queryFeaturesStatsUpdater =
        IndexMetricsUpdaterBuilder.QueryingMetricsUpdaterBuilder.QueryFeaturesMetricsUpdaterBuilder
            .empty();
    var queryMetricsRecorder = new QueryMetricsRecorder(queryFeaturesStatsUpdater);

    BsonValue id = new BsonString("test");
    var query =
        OperatorQueryBuilder.builder()
            .operator(
                OperatorBuilder.exists()
                    .path("foo")
                    .score(ScoreBuilder.constant().value(1).build())
                    .build())
            .index(MOCK_INDEX_NAME)
            .searchBefore(SequenceToken.of(id, new ScoreDoc(1, 1f)))
            .returnStoredSource(false)
            .build();

    queryMetricsRecorder.record(query, QueryCursorOptions.empty());

    Assert.assertEquals(1, queryFeaturesStatsUpdater.getSequenceTokenCounter().count(), EPSILON);
  }

  @Test
  public void testSearchAfterMetric() {
    var queryFeaturesStatsUpdater =
        IndexMetricsUpdaterBuilder.QueryingMetricsUpdaterBuilder.QueryFeaturesMetricsUpdaterBuilder
            .empty();
    var queryMetricsRecorder = new QueryMetricsRecorder(queryFeaturesStatsUpdater);

    BsonValue id = new BsonString("test");
    var query =
        OperatorQueryBuilder.builder()
            .operator(
                OperatorBuilder.exists()
                    .path("foo")
                    .score(ScoreBuilder.constant().value(1).build())
                    .build())
            .index(MOCK_INDEX_NAME)
            .searchAfter(SequenceToken.of(id, new ScoreDoc(1, 1f)))
            .returnStoredSource(false)
            .build();

    queryMetricsRecorder.record(query, QueryCursorOptions.empty());

    Assert.assertEquals(1, queryFeaturesStatsUpdater.getSequenceTokenCounter().count(), EPSILON);
  }

  @Test
  public void testQuerySortMetric() {
    var queryFeaturesStatsUpdater =
        IndexMetricsUpdaterBuilder.QueryingMetricsUpdaterBuilder.QueryFeaturesMetricsUpdaterBuilder
            .empty();
    var queryMetricsRecorder = new QueryMetricsRecorder(queryFeaturesStatsUpdater);

    var query =
        OperatorQueryBuilder.builder()
            .operator(
                OperatorBuilder.exists()
                    .path("foo")
                    .score(ScoreBuilder.constant().value(1).build())
                    .build())
            .index(MOCK_INDEX_NAME)
            .sort(
                SortSpecBuilder.builder()
                    .sortField(
                        new MongotSortField(
                            FieldPath.fromParts("price"), UserFieldSortOptions.DEFAULT_ASC))
                    .buildSort())
            .returnStoredSource(false)
            .build();

    queryMetricsRecorder.record(query, QueryCursorOptions.empty());

    Assert.assertEquals(1, queryFeaturesStatsUpdater.getSortCounter().count(), EPSILON);
  }

  @Test
  public void testQuerySortNoDataMetric() {
    var queryFeaturesStatsUpdater =
        IndexMetricsUpdaterBuilder.QueryingMetricsUpdaterBuilder.QueryFeaturesMetricsUpdaterBuilder
            .empty();
    var queryMetricsRecorder = new QueryMetricsRecorder(queryFeaturesStatsUpdater);

    var query =
        OperatorQueryBuilder.builder()
            .operator(
                OperatorBuilder.exists()
                    .path("foo")
                    .score(ScoreBuilder.constant().value(1).build())
                    .build())
            .index(MOCK_INDEX_NAME)
            .sort(
                SortSpecBuilder.builder()
                    .sortField(
                        new MongotSortField(
                            FieldPath.fromParts("a"),
                            SortOptionsBuilder.user()
                                .sortOrder(SortOrder.ASC)
                                .nullEmptySortPosition(NullEmptySortPosition.LOWEST)
                                .build()))
                    .sortField(
                        new MongotSortField(
                            FieldPath.fromParts("b"),
                            SortOptionsBuilder.user()
                                .sortOrder(SortOrder.DESC)
                                .nullEmptySortPosition(NullEmptySortPosition.LOWEST)
                                .build()))
                    .sortField(
                        new MongotSortField(
                            FieldPath.fromParts("c"),
                            SortOptionsBuilder.user()
                                .sortOrder(SortOrder.ASC)
                                .nullEmptySortPosition(NullEmptySortPosition.HIGHEST)
                                .build()))
                    .sortField(
                        new MongotSortField(
                            FieldPath.fromParts("d"),
                            SortOptionsBuilder.user()
                                .sortOrder(SortOrder.DESC)
                                .nullEmptySortPosition(NullEmptySortPosition.HIGHEST)
                                .build()))
                    .buildSort())
            .returnStoredSource(false)
            .build();

    queryMetricsRecorder.record(query, QueryCursorOptions.empty());

    // Counter is intended to track only the existence of each noData specification in the query,
    // rather than the total number of times noData was used in the query
    Assert.assertEquals(
        1,
        queryFeaturesStatsUpdater
            .getNoDataSortPositionCounter(NullEmptySortPosition.LOWEST)
            .count(),
        EPSILON);

    Assert.assertEquals(
        1,
        queryFeaturesStatsUpdater
            .getNoDataSortPositionCounter(NullEmptySortPosition.HIGHEST)
            .count(),
        EPSILON);
  }

  @Test
  public void testQueryTrackingMetric() {
    var queryFeaturesStatsUpdater =
        IndexMetricsUpdaterBuilder.QueryingMetricsUpdaterBuilder.QueryFeaturesMetricsUpdaterBuilder
            .empty();
    var queryMetricsRecorder = new QueryMetricsRecorder(queryFeaturesStatsUpdater);

    var query =
        OperatorQueryBuilder.builder()
            .operator(
                OperatorBuilder.exists()
                    .path("foo")
                    .score(ScoreBuilder.constant().value(1).build())
                    .build())
            .index(MOCK_INDEX_NAME)
            .tracking(new Tracking("foo"))
            .returnStoredSource(false)
            .build();

    queryMetricsRecorder.record(query, QueryCursorOptions.empty());

    Assert.assertEquals(1, queryFeaturesStatsUpdater.getTrackingCounter().count(), EPSILON);
  }

  @Test
  public void testSimple() {
    var queryFeaturesStatsUpdater =
        IndexMetricsUpdaterBuilder.QueryingMetricsUpdaterBuilder.QueryFeaturesMetricsUpdaterBuilder
            .empty();
    var queryMetricsRecorder = new QueryMetricsRecorder(queryFeaturesStatsUpdater);

    SearchQuery query =
        OperatorQueryBuilder.builder()
            .operator(OperatorBuilder.text().query("simple").build())
            .returnStoredSource(false)
            .build();

    queryMetricsRecorder.record(query, QueryCursorOptions.empty());

    Assert.assertEquals(
        1, queryFeaturesStatsUpdater.getOperatorTypeCounter(Operator.Type.TEXT).count(), 0);
  }

  @Test
  public void testCompound() {
    var queryFeaturesStatsUpdater =
        IndexMetricsUpdaterBuilder.QueryingMetricsUpdaterBuilder.QueryFeaturesMetricsUpdaterBuilder
            .empty();
    var queryMetricsRecorder = new QueryMetricsRecorder(queryFeaturesStatsUpdater);

    SearchQuery query =
        OperatorQueryBuilder.builder()
            .operator(
                OperatorBuilder.compound()
                    .should(OperatorBuilder.term().path("title").query("godfather").build())
                    .should(OperatorBuilder.term().path("title").query("nemo").build())
                    .minimumShouldMatch(1)
                    .build())
            .returnStoredSource(false)
            .build();

    queryMetricsRecorder.record(query, QueryCursorOptions.empty());

    Assert.assertEquals(
        1, queryFeaturesStatsUpdater.getOperatorTypeCounter(Operator.Type.COMPOUND).count(), 0);
    Assert.assertEquals(
        1, queryFeaturesStatsUpdater.getOperatorTypeCounter(Operator.Type.TERM).count(), 0);
  }

  @Test
  public void testCompoundNested() {
    var queryFeaturesStatsUpdater =
        IndexMetricsUpdaterBuilder.QueryingMetricsUpdaterBuilder.QueryFeaturesMetricsUpdaterBuilder
            .empty();
    var queryMetricsRecorder = new QueryMetricsRecorder(queryFeaturesStatsUpdater);

    SearchQuery query =
        OperatorQueryBuilder.builder()
            .operator(
                OperatorBuilder.compound()
                    .should(
                        OperatorBuilder.compound()
                            .filter(OperatorBuilder.term().path("title").query("godfather").build())
                            .build())
                    .should(OperatorBuilder.term().path("title").query("nemo").build())
                    .minimumShouldMatch(1)
                    .build())
            .returnStoredSource(false)
            .build();

    queryMetricsRecorder.record(query, QueryCursorOptions.empty());

    Assert.assertEquals(
        1, queryFeaturesStatsUpdater.getOperatorTypeCounter(Operator.Type.COMPOUND).count(), 0);
    Assert.assertEquals(
        1, queryFeaturesStatsUpdater.getOperatorTypeCounter(Operator.Type.TERM).count(), 0);
  }

  @Test
  public void testCountFeaturesOnce() {
    var queryFeaturesStatsUpdater =
        IndexMetricsUpdaterBuilder.QueryingMetricsUpdaterBuilder.QueryFeaturesMetricsUpdaterBuilder
            .empty();
    var queryMetricsRecorder = new QueryMetricsRecorder(queryFeaturesStatsUpdater);

    SearchQuery query =
        OperatorQueryBuilder.builder()
            .operator(
                OperatorBuilder.compound()
                    .should(
                        OperatorBuilder.compound()
                            .filter(
                                OperatorBuilder.term()
                                    .path("title")
                                    .fuzzy(TermOperatorBuilder.fuzzyBuilder().build())
                                    .query("godfather")
                                    .build())
                            .build())
                    .should(
                        OperatorBuilder.text()
                            .path(UnresolvedStringPathBuilder.wildcardPath("foo*"))
                            .fuzzy(FuzzyOptionBuilder.builder().build())
                            .query("nemo")
                            .build())
                    .minimumShouldMatch(1)
                    .build())
            .highlight(
                UnresolvedHighlightBuilder.builder()
                    .path(UnresolvedStringPathBuilder.wildcardPath("foo*"))
                    .build())
            .returnStoredSource(false)
            .build();

    queryMetricsRecorder.record(query, QueryCursorOptions.empty());

    Assert.assertEquals(
        1, queryFeaturesStatsUpdater.getOperatorTypeCounter(Operator.Type.COMPOUND).count(), 0);
    Assert.assertEquals(
        1, queryFeaturesStatsUpdater.getOperatorTypeCounter(Operator.Type.TEXT).count(), 0);
    Assert.assertEquals(1, queryFeaturesStatsUpdater.getFuzzyCounter().count(), 0);
    Assert.assertEquals(1, queryFeaturesStatsUpdater.getWildcardPathsCounter().count(), 0);
    Assert.assertEquals(1, queryFeaturesStatsUpdater.getHighlightingCounter().count(), 0);
  }

  @Test
  public void testAutocompleteOperatorMetrics() {
    var queryFeaturesStatsUpdater =
        IndexMetricsUpdaterBuilder.QueryingMetricsUpdaterBuilder.QueryFeaturesMetricsUpdaterBuilder
            .empty();
    var queryMetricsRecorder = new QueryMetricsRecorder(queryFeaturesStatsUpdater);

    SearchQuery query =
        OperatorQueryBuilder.builder()
            .operator(
                OperatorBuilder.autocomplete()
                    .query("phrase")
                    .path("a")
                    .fuzzy(FuzzyOptionBuilder.builder().build())
                    .build())
            .returnStoredSource(false)
            .build();

    queryMetricsRecorder.record(query, QueryCursorOptions.empty());

    Assert.assertEquals(
        1, queryFeaturesStatsUpdater.getOperatorTypeCounter(Operator.Type.AUTOCOMPLETE).count(), 0);
    Assert.assertEquals(1, queryFeaturesStatsUpdater.getFuzzyCounter().count(), 0);
  }

  @Test
  public void testTextSynonymsOperatorMetrics() {
    var queryFeaturesStatsUpdater =
        IndexMetricsUpdaterBuilder.QueryingMetricsUpdaterBuilder.QueryFeaturesMetricsUpdaterBuilder
            .empty();
    var queryMetricsRecorder = new QueryMetricsRecorder(queryFeaturesStatsUpdater);

    SearchQuery query =
        OperatorQueryBuilder.builder()
            .operator(
                OperatorBuilder.text()
                    .synonyms("synonyms")
                    .matchCriteria(TextOperator.MatchCriteria.ALL)
                    .query("text")
                    .build())
            .returnStoredSource(false)
            .build();

    queryMetricsRecorder.record(query, QueryCursorOptions.empty());

    Assert.assertEquals(
        1, queryFeaturesStatsUpdater.getOperatorTypeCounter(Operator.Type.TEXT).count(), 0);
    Assert.assertEquals(1, queryFeaturesStatsUpdater.getTextSynonymsCounter().count(), 0);
    Assert.assertEquals(
        1,
        queryFeaturesStatsUpdater
            .getTextMatchCriteriaCounter(TextOperator.MatchCriteria.ALL)
            .count(),
        0);
    Assert.assertEquals(0, queryFeaturesStatsUpdater.getTextDeprecatedSynonymsCounter().count(), 0);
  }

  @Test
  public void testTextOperatorDeprecatedSynonymsMetrics() {
    var queryFeaturesStatsUpdater =
        IndexMetricsUpdaterBuilder.QueryingMetricsUpdaterBuilder.QueryFeaturesMetricsUpdaterBuilder
            .empty();
    var queryMetricsRecorder = new QueryMetricsRecorder(queryFeaturesStatsUpdater);

    SearchQuery query =
        OperatorQueryBuilder.builder()
            .operator(OperatorBuilder.text().synonyms("synonyms").query("text").build())
            .returnStoredSource(false)
            .build();

    queryMetricsRecorder.record(query, QueryCursorOptions.empty());

    Assert.assertEquals(
        1, queryFeaturesStatsUpdater.getOperatorTypeCounter(Operator.Type.TEXT).count(), 0);
    Assert.assertEquals(1, queryFeaturesStatsUpdater.getTextSynonymsCounter().count(), 0);
    Assert.assertEquals(1, queryFeaturesStatsUpdater.getTextDeprecatedSynonymsCounter().count(), 0);
  }

  @Test
  public void testTermFuzzyOperatorMetrics() {
    var queryFeaturesStatsUpdater =
        IndexMetricsUpdaterBuilder.QueryingMetricsUpdaterBuilder.QueryFeaturesMetricsUpdaterBuilder
            .empty();
    var queryMetricsRecorder = new QueryMetricsRecorder(queryFeaturesStatsUpdater);

    SearchQuery query =
        OperatorQueryBuilder.builder()
            .operator(
                OperatorBuilder.term()
                    .query("term_fuzzy")
                    .fuzzy(TermOperatorBuilder.fuzzyBuilder().build())
                    .build())
            .returnStoredSource(false)
            .build();

    queryMetricsRecorder.record(query, QueryCursorOptions.empty());

    Assert.assertEquals(
        1, queryFeaturesStatsUpdater.getOperatorTypeCounter(Operator.Type.TERM_FUZZY).count(), 0);
    Assert.assertEquals(1, queryFeaturesStatsUpdater.getFuzzyCounter().count(), 0);
  }

  @Test
  public void testRegexOperatorMetrics() {
    var queryFeaturesStatsUpdater =
        IndexMetricsUpdaterBuilder.QueryingMetricsUpdaterBuilder.QueryFeaturesMetricsUpdaterBuilder
            .empty();
    var queryMetricsRecorder = new QueryMetricsRecorder(queryFeaturesStatsUpdater);

    SearchQuery query =
        OperatorQueryBuilder.builder()
            .operator(
                OperatorBuilder.regex()
                    .query("regex")
                    .path(UnresolvedStringPathBuilder.wildcardPath("a.*"))
                    .build())
            .returnStoredSource(false)
            .build();

    queryMetricsRecorder.record(query, QueryCursorOptions.empty());

    Assert.assertEquals(
        1, queryFeaturesStatsUpdater.getOperatorTypeCounter(Operator.Type.REGEX).count(), 0);
    Assert.assertEquals(1, queryFeaturesStatsUpdater.getWildcardPathsCounter().count(), 0);
  }

  @Test
  public void testPhraseOperatorMetrics() {
    var queryFeaturesStatsUpdater =
        IndexMetricsUpdaterBuilder.QueryingMetricsUpdaterBuilder.QueryFeaturesMetricsUpdaterBuilder
            .empty();
    var queryMetricsRecorder = new QueryMetricsRecorder(queryFeaturesStatsUpdater);

    SearchQuery query =
        OperatorQueryBuilder.builder()
            .operator(
                OperatorBuilder.phrase()
                    .query("phrase")
                    .path(UnresolvedStringPathBuilder.wildcardPath("a.*"))
                    .synonyms("synonyms")
                    .build())
            .returnStoredSource(false)
            .build();

    queryMetricsRecorder.record(query, QueryCursorOptions.empty());

    Assert.assertEquals(
        1, queryFeaturesStatsUpdater.getOperatorTypeCounter(Operator.Type.PHRASE).count(), 0);
    Assert.assertEquals(1, queryFeaturesStatsUpdater.getWildcardPathsCounter().count(), 0);
    Assert.assertEquals(1, queryFeaturesStatsUpdater.getPhraseSynonymsCounter().count(), 0);
  }

  @Test
  public void testWildcardOperatorOperatorMetrics() {
    var queryFeaturesStatsUpdater =
        IndexMetricsUpdaterBuilder.QueryingMetricsUpdaterBuilder.QueryFeaturesMetricsUpdaterBuilder
            .empty();
    var queryMetricsRecorder = new QueryMetricsRecorder(queryFeaturesStatsUpdater);

    SearchQuery query =
        OperatorQueryBuilder.builder()
            .operator(
                OperatorBuilder.wildcard()
                    .query("wildcard")
                    .path(UnresolvedStringPathBuilder.wildcardPath("a.*"))
                    .build())
            .returnStoredSource(false)
            .build();

    queryMetricsRecorder.record(query, QueryCursorOptions.empty());

    Assert.assertEquals(
        1, queryFeaturesStatsUpdater.getOperatorTypeCounter(Operator.Type.WILDCARD).count(), 0);
    Assert.assertEquals(1, queryFeaturesStatsUpdater.getWildcardPathsCounter().count(), 0);
  }

  @Test
  public void testInOperatorMetrics() {
    var queryFeaturesStatsUpdater =
        IndexMetricsUpdaterBuilder.QueryingMetricsUpdaterBuilder.QueryFeaturesMetricsUpdaterBuilder
            .empty();
    var queryMetricsRecorder = new QueryMetricsRecorder(queryFeaturesStatsUpdater);

    SearchQuery query =
        OperatorQueryBuilder.builder()
            .operator(OperatorBuilder.in().path("in").strings(List.of("foo")).build())
            .returnStoredSource(false)
            .build();

    queryMetricsRecorder.record(query, QueryCursorOptions.empty());

    Assert.assertEquals(
        1, queryFeaturesStatsUpdater.getOperatorTypeCounter(Operator.Type.IN).count(), EPSILON);
  }

  @Test
  public void testRequireSequenceTokensMetrics() {
    var queryFeaturesStatsUpdater =
        IndexMetricsUpdaterBuilder.QueryingMetricsUpdaterBuilder.QueryFeaturesMetricsUpdaterBuilder
            .empty();
    var queryMetricsRecorder = new QueryMetricsRecorder(queryFeaturesStatsUpdater);

    SearchQuery query =
        OperatorQueryBuilder.builder()
            .operator(OperatorBuilder.text().query("simple").build())
            .returnStoredSource(false)
            .build();

    QueryCursorOptions cursorOptions =
        QueryCursorOptionsBuilder.builder().requireSequenceTokens(true).build();
    queryMetricsRecorder.record(query, cursorOptions);

    Assert.assertEquals(
        1, queryFeaturesStatsUpdater.getRequireSequenceTokensCounter().count(), EPSILON);
  }

  @Test
  public void testReturnStoredSourceQueryMetrics() {
    var queryFeaturesStatsUpdater =
        IndexMetricsUpdaterBuilder.QueryingMetricsUpdaterBuilder.QueryFeaturesMetricsUpdaterBuilder
            .empty();
    var queryMetricsRecorder = new QueryMetricsRecorder(queryFeaturesStatsUpdater);

    SearchQuery query =
        OperatorQueryBuilder.builder()
            .operator(OperatorBuilder.text().query("simple").build())
            .returnStoredSource(true)
            .build();

    queryMetricsRecorder.record(query, QueryCursorOptions.empty());

    Assert.assertEquals(
        1, queryFeaturesStatsUpdater.getReturnStoredSourceCounter().count(), EPSILON);
  }

  @Test
  public void record_queryWithReturnScope_incrementsReturnScopeCounter() {
    var queryFeaturesStatsUpdater =
        IndexMetricsUpdaterBuilder.QueryingMetricsUpdaterBuilder.QueryFeaturesMetricsUpdaterBuilder
            .empty();
    var queryMetricsRecorder = new QueryMetricsRecorder(queryFeaturesStatsUpdater);

    SearchQuery query =
        OperatorQueryBuilder.builder()
            .returnStoredSource(true)
            .returnScope(new ReturnScope(FieldPath.parse("path")))
            .operator(OperatorBuilder.text().query("simple").build())
            .build();

    queryMetricsRecorder.record(query, QueryCursorOptions.empty());

    Assert.assertEquals(1, queryFeaturesStatsUpdater.getReturnScopeCounter().count(), EPSILON);
  }

  @Test
  public void testExactVectorQueryMetrics() {
    var queryFeaturesStatsUpdater =
        IndexMetricsUpdaterBuilder.QueryingMetricsUpdaterBuilder.QueryFeaturesMetricsUpdaterBuilder
            .empty();
    var queryMetricsRecorder = new QueryMetricsRecorder(queryFeaturesStatsUpdater);

    VectorSearchQuery query =
        VectorQueryBuilder.builder()
            .index("testIndex")
            .criteria(
                ExactVectorCriteriaBuilder.builder()
                    .path(FieldPath.newRoot("test"))
                    .queryVector(Vector.fromFloats(new float[] {1.f, 2.f}, NATIVE))
                    .limit(10)
                    .build())
            .build();

    queryMetricsRecorder.record(query);

    Assert.assertEquals(
        1,
        queryFeaturesStatsUpdater
            .getVectorSearchQueryTypeCounter(query.criteria().getVectorSearchType())
            .count(),
        EPSILON);
  }

  @Test
  public void testApproximateVectorQueryMetrics() {
    var queryFeaturesStatsUpdater =
        IndexMetricsUpdaterBuilder.QueryingMetricsUpdaterBuilder.QueryFeaturesMetricsUpdaterBuilder
            .empty();
    var queryMetricsRecorder = new QueryMetricsRecorder(queryFeaturesStatsUpdater);

    VectorSearchQuery query =
        VectorQueryBuilder.builder()
            .index("testIndex")
            .criteria(
                ApproximateVectorQueryCriteriaBuilder.builder()
                    .path(FieldPath.newRoot("test"))
                    .queryVector(Vector.fromFloats(new float[] {1.f, 2.f}, NATIVE))
                    .numCandidates(100)
                    .limit(10)
                    .build())
            .build();

    queryMetricsRecorder.record(query);

    Assert.assertEquals(
        1,
        queryFeaturesStatsUpdater
            .getVectorSearchQueryTypeCounter(query.criteria().getVectorSearchType())
            .count(),
        EPSILON);
  }

  @Test
  public void testGetAllLeafOperators() {
    CompoundOperator compoundOperator =
        OperatorBuilder.compound()
            .should(
                OperatorBuilder.compound()
                    .filter(
                        OperatorBuilder.term()
                            .path("title")
                            .fuzzy(TermOperatorBuilder.fuzzyBuilder().build())
                            .query("godfather")
                            .build())
                    .build())
            .should(
                OperatorBuilder.text()
                    .path(UnresolvedStringPathBuilder.wildcardPath("foo*"))
                    .fuzzy(FuzzyOptionBuilder.builder().build())
                    .query("nemo")
                    .build())
            .must(
                OperatorBuilder.compound()
                    .filter(OperatorBuilder.text().path("text").query("foo").build())
                    .filter(OperatorBuilder.text().path("text").query("bar").build())
                    .build())
            .build();
    var operators = QueryMetricsRecorder.getAllLeafOperators(compoundOperator);
    Assert.assertEquals(4, operators.count());
  }

  @Test
  public void record_unfilteredVectorSearch_incrementsTypeCounter() throws BsonParseException {
    RawBsonDocument rawQuery =
        bson(
            """
        {
          "index": "default",
          "path": "vector",
          "numCandidates": 20,
          "limit": 10,
          "queryVector": [2.0,  2.0]
        }
        """);
    Query query = VectorSearchQuery.fromBson(rawQuery);
    QueryFeaturesMetricsUpdater stats = QueryFeaturesMetricsUpdaterBuilder.empty();
    var recorder = new QueryMetricsRecorder(stats);

    recorder.record(query);

    assertEquals(0, stats.getOperatorTypeCounter(Operator.Type.VECTOR_SEARCH).count(), EPSILON);
    assertEquals(
        1,
        stats.getVectorSearchQueryTypeCounter(VectorSearchCriteria.Type.APPROXIMATE).count(),
        EPSILON);
    assertEquals(0, stats.getFilteredVectorSearchCounter().count(), EPSILON);
  }

  @Test
  public void record_filteredVectorSearch_incrementsFilterCounter() throws BsonParseException {
    RawBsonDocument rawQuery =
        bson(
            """
        {
          "index": "default",
          "path": "vector",
          "numCandidates": 20,
          "limit": 10,
          "queryVector": [2.0,  2.0],
          "filter": {"foo":  5}
        }
        """);
    Query query = VectorSearchQuery.fromBson(rawQuery);
    QueryFeaturesMetricsUpdater stats = QueryFeaturesMetricsUpdaterBuilder.empty();
    var recorder = new QueryMetricsRecorder(stats);

    recorder.record(query);

    assertEquals(0, stats.getOperatorTypeCounter(Operator.Type.VECTOR_SEARCH).count(), EPSILON);
    assertEquals(
        1,
        stats.getVectorSearchQueryTypeCounter(VectorSearchCriteria.Type.APPROXIMATE).count(),
        EPSILON);
    assertEquals(1, stats.getFilteredVectorSearchCounter().count(), EPSILON);
    assertEquals(
        1,
        stats.getVectorSearchFilterOperatorTypeCounterMap(MqlFilterOperator.Category.EQ).count(),
        EPSILON);
  }

  @Test
  public void record_filteredVectorSearch_descendsTree() throws BsonParseException {
    RawBsonDocument rawQuery =
        bson(
            """
        {
          "index": "default",
          "path": "vector",
          "numCandidates": 20,
          "limit": 10,
          "queryVector": [2.0,  2.0],
          "filter": {$and: [{"foo":  5}, {"bar": {"$lt":  10}}]}
        }
        """);
    Query query = VectorSearchQuery.fromBson(rawQuery);
    QueryFeaturesMetricsUpdater stats = QueryFeaturesMetricsUpdaterBuilder.empty();
    var recorder = new QueryMetricsRecorder(stats);

    recorder.record(query);

    assertEquals(0, stats.getOperatorTypeCounter(Operator.Type.VECTOR_SEARCH).count(), EPSILON);
    assertEquals(
        1,
        stats.getVectorSearchQueryTypeCounter(VectorSearchCriteria.Type.APPROXIMATE).count(),
        EPSILON);
    assertEquals(1, stats.getFilteredVectorSearchCounter().count(), EPSILON);
    assertEquals(
        1,
        stats.getVectorSearchFilterOperatorTypeCounterMap(MqlFilterOperator.Category.EQ).count(),
        EPSILON);
    assertEquals(
        1,
        stats
            .getVectorSearchFilterOperatorTypeCounterMap(MqlFilterOperator.Category.UPPER_BOUND)
            .count(),
        EPSILON);
  }

  @Test
  public void record_negatedVectorSearch_expandsNotOperator() throws BsonParseException {
    RawBsonDocument rawQuery =
        bson(
            """
        {
          "index": "default",
          "path": "vector",
          "numCandidates": 20,
          "limit": 10,
          "queryVector": [2.0,  2.0],
          "filter": {$and: [{"foo":  {"$not": {"$eq": 5, "$gt": 10}}}]}
        }
        """);
    Query query = VectorSearchQuery.fromBson(rawQuery);
    QueryFeaturesMetricsUpdater stats = QueryFeaturesMetricsUpdaterBuilder.empty();
    var recorder = new QueryMetricsRecorder(stats);

    recorder.record(query);

    assertEquals(0, stats.getOperatorTypeCounter(Operator.Type.VECTOR_SEARCH).count(), EPSILON);
    assertEquals(
        1,
        stats.getVectorSearchQueryTypeCounter(VectorSearchCriteria.Type.APPROXIMATE).count(),
        EPSILON);
    assertEquals(1, stats.getFilteredVectorSearchCounter().count(), EPSILON);
    assertEquals(
        1,
        stats.getVectorSearchFilterOperatorTypeCounterMap(MqlFilterOperator.Category.EQ).count(),
        EPSILON);
    assertEquals(
        1,
        stats.getVectorSearchFilterOperatorTypeCounterMap(MqlFilterOperator.Category.NOT).count(),
        EPSILON);
    assertEquals(
        1,
        stats
            .getVectorSearchFilterOperatorTypeCounterMap(MqlFilterOperator.Category.LOWER_BOUND)
            .count(),
        EPSILON);
  }

  @Test
  public void record_vectorSearchWithLexicalFilters_incrementsOperatorCounter()
      throws BsonParseException {
    RawBsonDocument rawQuery =
        bson(
            """
      {
        "vectorSearch": {
          "path": "vector",
          "numCandidates": 20,
          "limit": 10,
          "queryVector": [2.0,  2.0],
        },
      }
        """);
    Query query = SearchQuery.fromBson(rawQuery);
    QueryFeaturesMetricsUpdater stats = QueryFeaturesMetricsUpdaterBuilder.empty();
    var recorder = new QueryMetricsRecorder(stats);

    recorder.record(query);

    assertEquals(
        1,
        stats.getVectorSearchQueryTypeCounter(VectorSearchCriteria.Type.APPROXIMATE).count(),
        EPSILON);
    assertEquals(1, stats.getOperatorTypeCounter(Operator.Type.VECTOR_SEARCH).count(), EPSILON);
    assertEquals(0, stats.getFilteredVectorSearchCounter().count(), EPSILON);
  }

  @Test
  public void record_vectorSearchWithLexicalFilters_incrementsSearchVectorSearchOperatorCounts()
      throws BsonParseException {
    RawBsonDocument rawQuery =
        bson(
            """
        {
        "vectorSearch": {
          "path": "vector",
          "numCandidates": 20,
          "limit": 10,
          "queryVector": [2.0,  2.0],
          "filter": {
            "text": {
              "path": "name",
              "query": "pizzuh",
              "fuzzy": {
                "maxEdits": 2
              }
            }
          }
        },
      }
        """);
    Query query = SearchQuery.fromBson(rawQuery);
    QueryFeaturesMetricsUpdater stats = QueryFeaturesMetricsUpdaterBuilder.empty();
    var recorder = new QueryMetricsRecorder(stats);

    recorder.record(query);

    assertEquals(
        1,
        stats.getVectorSearchQueryTypeCounter(VectorSearchCriteria.Type.APPROXIMATE).count(),
        EPSILON);
    assertEquals(1, stats.getOperatorTypeCounter(Operator.Type.VECTOR_SEARCH).count(), EPSILON);
    assertEquals(1, stats.getFilteredVectorSearchCounter().count(), EPSILON);
    assertEquals(
        1,
        stats.getSearchVectorSearchFilterOperatorTypeCounterMap(Operator.Type.TEXT).count(),
        EPSILON);
    assertEquals(0, stats.getOperatorTypeCounter(Operator.Type.TEXT).count(), EPSILON);
    // detailed operator stats are not yet incremented for vectorSearch filters,
    assertEquals(0, stats.getFuzzyCounter().count(), EPSILON);
  }

  @Test
  public void record_withExplainEnabled_incrementsExplainCounter() {
    var queryFeaturesStatsUpdater =
        IndexMetricsUpdaterBuilder.QueryingMetricsUpdaterBuilder.QueryFeaturesMetricsUpdaterBuilder
            .empty();
    var queryMetricsRecorder = new QueryMetricsRecorder(queryFeaturesStatsUpdater);

    SearchQuery query =
        OperatorQueryBuilder.builder()
            .operator(OperatorBuilder.text().query("simple").build())
            .returnStoredSource(false)
            .build();

    // Run with explain enabled
    try (var unused =
        Explain.setup(
            Optional.of(Explain.Verbosity.QUERY_PLANNER),
            Optional.of(IndexDefinition.Fields.NUM_PARTITIONS.getDefaultValue()))) {
      queryMetricsRecorder.record(query, QueryCursorOptions.empty());
    }

    Assert.assertEquals(1, queryFeaturesStatsUpdater.getExplainCounter().count(), EPSILON);
  }

  @Test
  public void record_withoutExplain_doesNotIncrementExplainCounter() {
    var queryFeaturesStatsUpdater =
        IndexMetricsUpdaterBuilder.QueryingMetricsUpdaterBuilder.QueryFeaturesMetricsUpdaterBuilder
            .empty();
    var queryMetricsRecorder = new QueryMetricsRecorder(queryFeaturesStatsUpdater);

    SearchQuery query =
        OperatorQueryBuilder.builder()
            .operator(OperatorBuilder.text().query("simple").build())
            .returnStoredSource(false)
            .build();

    // Run without explain
    queryMetricsRecorder.record(query, QueryCursorOptions.empty());

    Assert.assertEquals(0, queryFeaturesStatsUpdater.getExplainCounter().count(), EPSILON);
  }

  @Test
  public void record_vectorSearchWithExplainEnabled_incrementsExplainCounter() {
    var queryFeaturesStatsUpdater =
        IndexMetricsUpdaterBuilder.QueryingMetricsUpdaterBuilder.QueryFeaturesMetricsUpdaterBuilder
            .empty();
    var queryMetricsRecorder = new QueryMetricsRecorder(queryFeaturesStatsUpdater);

    VectorSearchQuery query =
        VectorQueryBuilder.builder()
            .index("testIndex")
            .criteria(
                ApproximateVectorQueryCriteriaBuilder.builder()
                    .path(FieldPath.newRoot("test"))
                    .queryVector(Vector.fromFloats(new float[] {1.f, 2.f}, NATIVE))
                    .numCandidates(100)
                    .limit(10)
                    .build())
            .build();

    // Run with explain enabled
    try (var unused =
        Explain.setup(
            Optional.of(Explain.Verbosity.QUERY_PLANNER),
            Optional.of(IndexDefinition.Fields.NUM_PARTITIONS.getDefaultValue()))) {
      queryMetricsRecorder.record(query);
    }

    Assert.assertEquals(1, queryFeaturesStatsUpdater.getExplainCounter().count(), EPSILON);
  }

  @Test
  public void record_vectorSearchWithoutExplain_doesNotIncrementExplainCounter() {
    var queryFeaturesStatsUpdater =
        IndexMetricsUpdaterBuilder.QueryingMetricsUpdaterBuilder.QueryFeaturesMetricsUpdaterBuilder
            .empty();
    var queryMetricsRecorder = new QueryMetricsRecorder(queryFeaturesStatsUpdater);

    VectorSearchQuery query =
        VectorQueryBuilder.builder()
            .index("testIndex")
            .criteria(
                ApproximateVectorQueryCriteriaBuilder.builder()
                    .path(FieldPath.newRoot("test"))
                    .queryVector(Vector.fromFloats(new float[] {1.f, 2.f}, NATIVE))
                    .numCandidates(100)
                    .limit(10)
                    .build())
            .build();

    // Run without explain
    queryMetricsRecorder.record(query);

    Assert.assertEquals(0, queryFeaturesStatsUpdater.getExplainCounter().count(), EPSILON);
  }
}
