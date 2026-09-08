package com.xgen.mongot.index;

import static com.xgen.mongot.index.SearchAggregationMetricsReporter.AGGREGATION_GROUP_TAG_PREFIX;
import static com.xgen.mongot.index.SearchAggregationMetricsReporter.AGGREGATION_METRIC_TAG_PREFIX;
import static com.xgen.mongot.index.SearchAggregationMetricsReporter.MAX_PATH_BUCKETS_HISTOGRAM;
import static com.xgen.mongot.index.SearchAggregationMetricsReporter.NESTING_HISTOGRAM;
import static com.xgen.mongot.index.SearchAggregationMetricsReporter.REQUESTED_BUCKETS_HISTOGRAM;
import static com.xgen.mongot.util.bson.FloatVector.OriginalType.BSON;
import static com.xgen.mongot.util.bson.FloatVector.OriginalType.NATIVE;
import static com.xgen.testing.BsonTestUtils.bson;
import static com.xgen.testing.mongot.mock.index.SearchIndex.MOCK_INDEX_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import com.google.common.base.CaseFormat;
import com.xgen.mongot.cursor.batch.QueryCursorOptions;
import com.xgen.mongot.index.IndexMetricsUpdater.QueryingMetricsUpdater.QueryFeaturesMetricsUpdater;
import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.lucene.explain.tracing.Explain;
import com.xgen.mongot.index.query.Query;
import com.xgen.mongot.index.query.ReturnScope;
import com.xgen.mongot.index.query.SearchQuery;
import com.xgen.mongot.index.query.Tracking;
import com.xgen.mongot.index.query.VectorSearchQuery;
import com.xgen.mongot.index.query.collectors.AggregationCollector;
import com.xgen.mongot.index.query.collectors.AggregationFacetDefinition;
import com.xgen.mongot.index.query.collectors.Collector;
import com.xgen.mongot.index.query.collectors.DrillSidewaysInfoBuilder.DrillSidewaysInfo;
import com.xgen.mongot.index.query.collectors.DrillSidewaysInfoBuilder.DrillSidewaysInfo.QueryOptimizationStatus;
import com.xgen.mongot.index.query.collectors.EmbeddedDocumentScope;
import com.xgen.mongot.index.query.collectors.FacetCollector;
import com.xgen.mongot.index.query.collectors.FacetDefinition;
import com.xgen.mongot.index.query.collectors.MetricSpec;
import com.xgen.mongot.index.query.operators.CompoundOperator;
import com.xgen.mongot.index.query.operators.Operator;
import com.xgen.mongot.index.query.operators.TextOperator;
import com.xgen.mongot.index.query.operators.VectorSearchCriteria;
import com.xgen.mongot.index.query.operators.VectorSearchQueryInput;
import com.xgen.mongot.index.query.operators.mql.MqlFilterOperator;
import com.xgen.mongot.index.query.scores.Score;
import com.xgen.mongot.index.query.sort.MongotSortField;
import com.xgen.mongot.index.query.sort.NullEmptySortPosition;
import com.xgen.mongot.index.query.sort.SequenceToken;
import com.xgen.mongot.index.query.sort.SortOrder;
import com.xgen.mongot.index.query.sort.UserFieldSortOptions;
import com.xgen.mongot.metrics.PerIndexMetricsFactory;
import com.xgen.mongot.util.Enums;
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
import com.xgen.testing.mongot.mock.index.SearchIndex;
import io.micrometer.core.instrument.Tags;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.lucene.search.ScoreDoc;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.RawBsonDocument;
import org.junit.Assert;
import org.junit.Test;

public class QueryMetricsRecorderTest {

  private static final double EPSILON = 0.0;

  @Test
  public void testAggregationCollectorQueryMetrics() {
    var queryFeaturesStatsUpdater = QueryFeaturesMetricsUpdaterBuilder.empty();
    var metricsFactory = SearchIndex.mockMetricsFactory();
    var queryMetricsRecorder =
        new QueryMetricsRecorder(
            queryFeaturesStatsUpdater,
            MicrometerSearchAggregationMetricsReporter.create(
                metricsFactory.getProcessMetricsFactory(), true));

    // Two levels of nesting: root groups on "outer", its single nested leaf carries sum + count.
    AggregationFacetDefinition leaf =
        AggregationFacetDefinition.create(
            FacetDefinitionBuilder.string().numBuckets(5).path("inner").build(),
            Map.of(
                "total",
                new MetricSpec(
                    com.xgen.mongot.index.query.aggregation.Operator.SUM,
                    Optional.of(FieldPath.parse("amount"))),
                "count",
                new MetricSpec(
                    com.xgen.mongot.index.query.aggregation.Operator.COUNT, Optional.empty())),
            Map.of());
    AggregationFacetDefinition root =
        AggregationFacetDefinition.create(
            FacetDefinitionBuilder.string().numBuckets(5).path("outer").build(),
            Map.of(),
            Map.of("byInner", leaf));

    SearchQuery query =
        CollectorQueryBuilder.builder()
            .collector(
                new AggregationCollector(
                    OperatorBuilder.text().path("path").query("empty").build(),
                    Map.of("byOuter", root),
                    Optional.empty()))
            .returnStoredSource(false)
            .build();

    queryMetricsRecorder.record(query, QueryCursorOptions.empty());

    assertEquals(
        1,
        queryFeaturesStatsUpdater.getCollectorTypeCounter(Collector.Type.AGGREGATION).count(),
        0);
    assertEquals(
        1, metricCounter(metricsFactory, com.xgen.mongot.index.query.aggregation.Operator.SUM), 0);
    assertEquals(
        1,
        metricCounter(metricsFactory, com.xgen.mongot.index.query.aggregation.Operator.COUNT),
        0);
    assertEquals(
        0, metricCounter(metricsFactory, com.xgen.mongot.index.query.aggregation.Operator.AVG), 0);
    assertEquals(1, summaryCount(metricsFactory, NESTING_HISTOGRAM), 0);
    assertEquals(2, summaryMax(metricsFactory, NESTING_HISTOGRAM), 0);
    assertEquals(1, groupCounter(metricsFactory, FacetDefinition.Type.STRING), 0);
    assertEquals(0, groupCounter(metricsFactory, FacetDefinition.Type.NUMBER), 0);
    // 5 outer buckets x 5 inner buckets: the ask down the single path is the product.
    assertEquals(25, summaryMax(metricsFactory, REQUESTED_BUCKETS_HISTOGRAM), 0);
    assertEquals(25, summaryMax(metricsFactory, MAX_PATH_BUCKETS_HISTOGRAM), 0);
  }

  /**
   * Several independent top-level trees of differing depth must land in exactly one nesting band --
   * the deepest -- so the histogram count cannot exceed the {@code aggregation} collector count.
   */
  @Test
  public void testAggregationNestingSampleUsesDeepestTopLevelTree() {
    var queryFeaturesStatsUpdater = QueryFeaturesMetricsUpdaterBuilder.empty();
    var metricsFactory = SearchIndex.mockMetricsFactory();
    var queryMetricsRecorder =
        new QueryMetricsRecorder(
            queryFeaturesStatsUpdater,
            MicrometerSearchAggregationMetricsReporter.create(
                metricsFactory.getProcessMetricsFactory(), true));

    AggregationFacetDefinition shallowRoot =
        AggregationFacetDefinition.create(
            FacetDefinitionBuilder.string().numBuckets(5).path("shallow").build(),
            Map.of(
                "count",
                new MetricSpec(
                    com.xgen.mongot.index.query.aggregation.Operator.COUNT, Optional.empty())),
            Map.of());
    AggregationFacetDefinition deepLeaf =
        AggregationFacetDefinition.create(
            FacetDefinitionBuilder.string().numBuckets(5).path("inner").build(),
            Map.of(
                "total",
                new MetricSpec(
                    com.xgen.mongot.index.query.aggregation.Operator.SUM,
                    Optional.of(FieldPath.parse("amount")))),
            Map.of());
    AggregationFacetDefinition deepRoot =
        AggregationFacetDefinition.create(
            FacetDefinitionBuilder.string().numBuckets(5).path("outer").build(),
            Map.of(),
            Map.of("byInner", deepLeaf));

    SearchQuery query =
        CollectorQueryBuilder.builder()
            .collector(
                new AggregationCollector(
                    OperatorBuilder.text().path("path").query("empty").build(),
                    Map.of("byShallow", shallowRoot, "byOuter", deepRoot),
                    Optional.empty()))
            .returnStoredSource(false)
            .build();

    queryMetricsRecorder.record(query, QueryCursorOptions.empty());

    // Exactly one sample, at the deepest tree's depth, even though the shallow tree bottoms out at
    // depth 1.
    assertEquals(1, summaryCount(metricsFactory, NESTING_HISTOGRAM), 0);
    assertEquals(2, summaryMax(metricsFactory, NESTING_HISTOGRAM), 0);

    // Metrics are still collected across every tree, shallow one included.
    assertEquals(
        1,
        metricCounter(metricsFactory, com.xgen.mongot.index.query.aggregation.Operator.COUNT),
        0);
    assertEquals(
        1, metricCounter(metricsFactory, com.xgen.mongot.index.query.aggregation.Operator.SUM), 0);
  }

  /**
   * The summed ask and the largest single path are different numbers: several shallow paths can add
   * up to more than one deep path while the deep path is the one at risk of exploding.
   */
  @Test
  public void testAggregationRequestedBucketsSumsPathsAndTracksLargest() {
    var queryFeaturesStatsUpdater = QueryFeaturesMetricsUpdaterBuilder.empty();
    var metricsFactory = SearchIndex.mockMetricsFactory();
    var queryMetricsRecorder =
        new QueryMetricsRecorder(
            queryFeaturesStatsUpdater,
            MicrometerSearchAggregationMetricsReporter.create(
                metricsFactory.getProcessMetricsFactory(), true));

    // Deep tree: 10 x 10 = 100 buckets down one path.
    AggregationFacetDefinition deepLeaf =
        AggregationFacetDefinition.create(
            FacetDefinitionBuilder.string().numBuckets(10).path("inner").build(),
            Map.of(
                "count",
                new MetricSpec(
                    com.xgen.mongot.index.query.aggregation.Operator.COUNT, Optional.empty())),
            Map.of());
    AggregationFacetDefinition deepRoot =
        AggregationFacetDefinition.create(
            FacetDefinitionBuilder.string().numBuckets(10).path("outer").build(),
            Map.of(),
            Map.of("byInner", deepLeaf));
    // Independent boundary tree: 3 boundaries produce 2 buckets.
    AggregationFacetDefinition boundaryRoot =
        AggregationFacetDefinition.create(
            FacetDefinitionBuilder.numeric()
                .boundaries(List.of(new BsonInt32(0), new BsonInt32(10), new BsonInt32(20)))
                .path("price")
                .build(),
            Map.of(
                "count",
                new MetricSpec(
                    com.xgen.mongot.index.query.aggregation.Operator.COUNT, Optional.empty())),
            Map.of());

    SearchQuery query =
        CollectorQueryBuilder.builder()
            .collector(
                new AggregationCollector(
                    OperatorBuilder.text().path("path").query("empty").build(),
                    Map.of("byOuter", deepRoot, "byPrice", boundaryRoot),
                    Optional.empty()))
            .returnStoredSource(false)
            .build();

    queryMetricsRecorder.record(query, QueryCursorOptions.empty());

    assertEquals(102, summaryMax(metricsFactory, REQUESTED_BUCKETS_HISTOGRAM), 0);
    assertEquals(100, summaryMax(metricsFactory, MAX_PATH_BUCKETS_HISTOGRAM), 0);
    assertEquals(1, groupCounter(metricsFactory, FacetDefinition.Type.STRING), 0);
    assertEquals(1, groupCounter(metricsFactory, FacetDefinition.Type.NUMBER), 0);
    assertEquals(0, groupCounter(metricsFactory, FacetDefinition.Type.DATE), 0);
  }

  /** With the feature flag off no aggregation metric is registered, so nothing is emitted. */
  @Test
  public void testAggregationMetricsAreNotRegisteredWhenDisabled() {
    var queryFeaturesStatsUpdater =
        QueryFeaturesMetricsUpdaterBuilder.empty(/* aggregationEnabled= */ false);
    var queryMetricsRecorder = new QueryMetricsRecorder(queryFeaturesStatsUpdater);

    SearchQuery query =
        CollectorQueryBuilder.builder()
            .collector(
                new AggregationCollector(
                    OperatorBuilder.text().path("path").query("empty").build(),
                    Map.of(
                        "byOuter",
                        AggregationFacetDefinition.create(
                            FacetDefinitionBuilder.string().numBuckets(5).path("outer").build(),
                            Map.of(
                                "count",
                                new MetricSpec(
                                    com.xgen.mongot.index.query.aggregation.Operator.COUNT,
                                    Optional.empty())),
                            Map.of())),
                    Optional.empty()))
            .returnStoredSource(false)
            .build();

    queryMetricsRecorder.record(query, QueryCursorOptions.empty());

    assertNull(queryFeaturesStatsUpdater.getCollectorTypeCounter(Collector.Type.AGGREGATION));
  }

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
    // No drill sideways (text-only operator) -> neither drill sideways counter is incremented
    Assert.assertEquals(
        0,
        queryFeaturesStatsUpdater
            .getFacetDrillSidewaysCounter(QueryOptimizationStatus.OPTIMIZABLE)
            .count(),
        EPSILON);
    Assert.assertEquals(
        0,
        queryFeaturesStatsUpdater
            .getFacetDrillSidewaysCounter(QueryOptimizationStatus.GENERIC)
            .count(),
        EPSILON);
  }

  @Test
  public void testUnscopedFacet_doesNotIncrementScopedFacetCounter() {
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
                            FacetDefinitionBuilder.string().numBuckets(5).path("path").build()))
                    .operator(OperatorBuilder.text().path("path").query("empty").build())
                    .build())
            .returnStoredSource(false)
            .build();

    queryMetricsRecorder.record(query, QueryCursorOptions.empty());

    Assert.assertEquals(0, queryFeaturesStatsUpdater.getScopedFacetCounter().count(), EPSILON);
  }

  @Test
  public void testScopedFacet_incrementsScopedFacetCounterOnce() {
    var queryFeaturesStatsUpdater =
        IndexMetricsUpdaterBuilder.QueryingMetricsUpdaterBuilder.QueryFeaturesMetricsUpdaterBuilder
            .empty();
    var queryMetricsRecorder = new QueryMetricsRecorder(queryFeaturesStatsUpdater);

    var childOperator = OperatorBuilder.equals().path("variants.inStock").value(true).build();
    var scopedColorFacet =
        new FacetDefinition.StringFacetDefinition(
            "variants.color",
            10,
            Optional.of(new EmbeddedDocumentScope(FieldPath.parse("variants"), childOperator)));
    var scopedSizeFacet =
        new FacetDefinition.StringFacetDefinition(
            "variants.size",
            10,
            Optional.of(new EmbeddedDocumentScope(FieldPath.parse("variants"), childOperator)));

    SearchQuery query =
        CollectorQueryBuilder.builder()
            .collector(
                CollectorBuilder.facet()
                    // Two scoped facets: the counter records feature usage once per query, not once
                    // per scoped facet.
                    .facetDefinitions(
                        Map.of("colorFacet", scopedColorFacet, "sizeFacet", scopedSizeFacet))
                    .operator(OperatorBuilder.text().path("title").query("jacket").build())
                    .build())
            .returnStoredSource(false)
            .build();

    queryMetricsRecorder.record(query, QueryCursorOptions.empty());

    Assert.assertEquals(1, queryFeaturesStatsUpdater.getScopedFacetCounter().count(), EPSILON);
  }

  @Test
  public void testFacetQueryWithDrillSidewaysOptimizable_incrementsOptimizableCounter() {
    QueryFeaturesMetricsUpdater queryFeaturesStatsUpdater =
        IndexMetricsUpdaterBuilder.QueryingMetricsUpdaterBuilder.QueryFeaturesMetricsUpdaterBuilder
            .empty();
    QueryMetricsRecorder queryMetricsRecorder = new QueryMetricsRecorder(queryFeaturesStatsUpdater);

    Operator operator = OperatorBuilder.text().path("path").query("empty").build();
    Map<String, FacetDefinition> facetDefinitions =
        Map.of("stringFacet", FacetDefinitionBuilder.string().numBuckets(5).path("path").build());
    FacetCollector facetCollector =
        new FacetCollector(
            operator,
            facetDefinitions,
            Optional.of(
                new DrillSidewaysInfo(
                    Map.of(), QueryOptimizationStatus.OPTIMIZABLE, Optional.empty())));

    SearchQuery query =
        CollectorQueryBuilder.builder().collector(facetCollector).returnStoredSource(false).build();

    queryMetricsRecorder.record(query, QueryCursorOptions.empty());

    Assert.assertEquals(
        1, queryFeaturesStatsUpdater.getCollectorTypeCounter(Collector.Type.FACET).count(), 0);
    Assert.assertEquals(
        1,
        queryFeaturesStatsUpdater
            .getFacetDrillSidewaysCounter(QueryOptimizationStatus.OPTIMIZABLE)
            .count(),
        EPSILON);
    Assert.assertEquals(
        0,
        queryFeaturesStatsUpdater
            .getFacetDrillSidewaysCounter(QueryOptimizationStatus.GENERIC)
            .count(),
        EPSILON);
  }

  @Test
  public void testFacetQueryWithDrillSidewaysGeneric_incrementsGenericCounter() {
    QueryFeaturesMetricsUpdater queryFeaturesStatsUpdater =
        IndexMetricsUpdaterBuilder.QueryingMetricsUpdaterBuilder.QueryFeaturesMetricsUpdaterBuilder
            .empty();
    QueryMetricsRecorder queryMetricsRecorder = new QueryMetricsRecorder(queryFeaturesStatsUpdater);

    Operator operator = OperatorBuilder.text().path("path").query("empty").build();
    Map<String, FacetDefinition> facetDefinitions =
        Map.of("stringFacet", FacetDefinitionBuilder.string().numBuckets(5).path("path").build());
    FacetCollector facetCollector =
        new FacetCollector(
            operator,
            facetDefinitions,
            Optional.of(
                new DrillSidewaysInfo(
                    Map.of(), QueryOptimizationStatus.GENERIC, Optional.empty())));

    SearchQuery query =
        CollectorQueryBuilder.builder().collector(facetCollector).returnStoredSource(false).build();

    queryMetricsRecorder.record(query, QueryCursorOptions.empty());

    Assert.assertEquals(
        1, queryFeaturesStatsUpdater.getCollectorTypeCounter(Collector.Type.FACET).count(), 0);
    Assert.assertEquals(
        0,
        queryFeaturesStatsUpdater
            .getFacetDrillSidewaysCounter(QueryOptimizationStatus.OPTIMIZABLE)
            .count(),
        EPSILON);
    Assert.assertEquals(
        1,
        queryFeaturesStatsUpdater
            .getFacetDrillSidewaysCounter(QueryOptimizationStatus.GENERIC)
            .count(),
        EPSILON);
  }

  @Test
  public void testFacetQueryWithoutDrillSideways_doesNotIncrementDrillSidewaysCounters() {
    QueryFeaturesMetricsUpdater queryFeaturesStatsUpdater =
        IndexMetricsUpdaterBuilder.QueryingMetricsUpdaterBuilder.QueryFeaturesMetricsUpdaterBuilder
            .empty();
    QueryMetricsRecorder queryMetricsRecorder = new QueryMetricsRecorder(queryFeaturesStatsUpdater);

    // FacetCollector built without withDrillSideways() has empty drillSidewaysInfo
    SearchQuery query =
        CollectorQueryBuilder.builder()
            .collector(
                CollectorBuilder.facet()
                    .facetDefinitions(
                        Map.of(
                            "stringFacet",
                            FacetDefinitionBuilder.string().numBuckets(5).path("path").build()))
                    .operator(OperatorBuilder.text().path("path").query("empty").build())
                    .build())
            .returnStoredSource(false)
            .build();

    queryMetricsRecorder.record(query, QueryCursorOptions.empty());

    Assert.assertEquals(
        1, queryFeaturesStatsUpdater.getCollectorTypeCounter(Collector.Type.FACET).count(), 0);
    Assert.assertEquals(
        0,
        queryFeaturesStatsUpdater
            .getFacetDrillSidewaysCounter(QueryOptimizationStatus.OPTIMIZABLE)
            .count(),
        EPSILON);
    Assert.assertEquals(
        0,
        queryFeaturesStatsUpdater
            .getFacetDrillSidewaysCounter(QueryOptimizationStatus.GENERIC)
            .count(),
        EPSILON);
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
  public void testHighlightWithAutocompleteMetric() {
    var queryFeaturesStatsUpdater =
        IndexMetricsUpdaterBuilder.QueryingMetricsUpdaterBuilder.QueryFeaturesMetricsUpdaterBuilder
            .empty();
    var queryMetricsRecorder = new QueryMetricsRecorder(queryFeaturesStatsUpdater);

    // Autocomplete nested inside a compound, to confirm the combination counter sees operators the
    // traversal reaches recursively.
    SearchQuery query =
        OperatorQueryBuilder.builder()
            .operator(
                OperatorBuilder.compound()
                    .should(OperatorBuilder.autocomplete().query("god").path("title").build())
                    .should(OperatorBuilder.term().path("title").query("nemo").build())
                    .minimumShouldMatch(1)
                    .build())
            .index(MOCK_INDEX_NAME)
            .highlight(UnresolvedHighlightBuilder.builder().path("title").build())
            .returnStoredSource(false)
            .build();

    queryMetricsRecorder.record(query, QueryCursorOptions.empty());

    Assert.assertEquals(
        1, queryFeaturesStatsUpdater.getHighlightWithAutocompleteCounter().count(), 0);
  }

  @Test
  public void testHighlightWithoutAutocompleteDoesNotIncrementCombinationMetric() {
    var queryFeaturesStatsUpdater =
        IndexMetricsUpdaterBuilder.QueryingMetricsUpdaterBuilder.QueryFeaturesMetricsUpdaterBuilder
            .empty();
    var queryMetricsRecorder = new QueryMetricsRecorder(queryFeaturesStatsUpdater);

    SearchQuery query =
        OperatorQueryBuilder.builder()
            .operator(OperatorBuilder.term().path("title").query("nemo").build())
            .index(MOCK_INDEX_NAME)
            .highlight(UnresolvedHighlightBuilder.builder().path("title").build())
            .returnStoredSource(false)
            .build();

    queryMetricsRecorder.record(query, QueryCursorOptions.empty());

    Assert.assertEquals(1, queryFeaturesStatsUpdater.getHighlightingCounter().count(), 0);
    Assert.assertEquals(
        0, queryFeaturesStatsUpdater.getHighlightWithAutocompleteCounter().count(), 0);
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
  public void record_vectorSearchWithConcurrentAbsent_incrementsConcurrentCounter() {
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

    Assert.assertEquals(1, queryFeaturesStatsUpdater.getConcurrentCounter().count(), EPSILON);
  }

  @Test
  public void record_vectorSearchWithConcurrentFalse_doesNotIncrementConcurrentCounter() {
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
            .concurrent(false)
            .build();

    queryMetricsRecorder.record(query);

    Assert.assertEquals(0, queryFeaturesStatsUpdater.getConcurrentCounter().count(), EPSILON);
  }

  @Test
  public void record_arrayQueryVector_incrementsArrayEncodingCounter() {
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
            .getVectorSearchQueryVectorEncodingCounter(Vector.BsonEncoding.ARRAY)
            .count(),
        EPSILON);
    Assert.assertEquals(
        0,
        queryFeaturesStatsUpdater
            .getVectorSearchQueryVectorEncodingCounter(Vector.BsonEncoding.BIN_DATA_FLOAT32)
            .count(),
        EPSILON);
  }

  @Test
  public void record_binDataQueryVector_incrementsBinDataEncodingCounter() {
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
                    .queryVector(Vector.fromFloats(new float[] {1.f, 2.f}, BSON))
                    .numCandidates(100)
                    .limit(10)
                    .build())
            .build();

    queryMetricsRecorder.record(query);

    Assert.assertEquals(
        1,
        queryFeaturesStatsUpdater
            .getVectorSearchQueryVectorEncodingCounter(Vector.BsonEncoding.BIN_DATA_FLOAT32)
            .count(),
        EPSILON);
    Assert.assertEquals(
        0,
        queryFeaturesStatsUpdater
            .getVectorSearchQueryVectorEncodingCounter(Vector.BsonEncoding.ARRAY)
            .count(),
        EPSILON);
  }

  @Test
  public void record_textQueryInput_doesNotIncrementQueryVectorEncodingCounter() {
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
                    .query(new VectorSearchQueryInput.Text("hello"))
                    .numCandidates(100)
                    .limit(10)
                    .build())
            .build();

    queryMetricsRecorder.record(query);

    assertEncodingCounterIsZero(queryFeaturesStatsUpdater, Vector.BsonEncoding.ARRAY);
    assertEncodingCounterIsZero(queryFeaturesStatsUpdater, Vector.BsonEncoding.BIN_DATA_FLOAT32);
    assertEncodingCounterIsZero(queryFeaturesStatsUpdater, Vector.BsonEncoding.BIN_DATA_FLOAT16);
    assertEncodingCounterIsZero(queryFeaturesStatsUpdater, Vector.BsonEncoding.BIN_DATA_INT8);
    assertEncodingCounterIsZero(queryFeaturesStatsUpdater, Vector.BsonEncoding.BIN_DATA_BIT);
  }

  private static void assertEncodingCounterIsZero(
      QueryFeaturesMetricsUpdater updater, Vector.BsonEncoding encoding) {
    Assert.assertEquals(
        "unexpected count for encoding " + encoding,
        0,
        updater.getVectorSearchQueryVectorEncodingCounter(encoding).count(),
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

  private static double summaryMax(PerIndexMetricsFactory metricsFactory, String name) {
    return metricsFactory.getProcessMetricsFactory().get(name).summary().max();
  }

  private static long summaryCount(PerIndexMetricsFactory metricsFactory, String name) {
    return metricsFactory.getProcessMetricsFactory().get(name).summary().count();
  }

  private static double groupCounter(
      PerIndexMetricsFactory metricsFactory, FacetDefinition.Type type) {
    return featureCounter(
        metricsFactory,
        AGGREGATION_GROUP_TAG_PREFIX + Enums.convertNameTo(CaseFormat.LOWER_CAMEL, type));
  }

  private static double metricCounter(
      PerIndexMetricsFactory metricsFactory,
      com.xgen.mongot.index.query.aggregation.Operator operator) {
    return featureCounter(
        metricsFactory,
        AGGREGATION_METRIC_TAG_PREFIX + Enums.convertNameTo(CaseFormat.LOWER_CAMEL, operator));
  }

  private static double featureCounter(PerIndexMetricsFactory metricsFactory, String nameTag) {
    return metricsFactory
        .getProcessMetricsFactory()
        .get(
            QueryFeaturesMetricsUpdater.METRIC_NAME,
            Tags.of(QueryFeaturesMetricsUpdater.NAME_TAG, nameTag))
        .counter()
        .count();
  }
}
