package com.xgen.mongot.index.query;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import com.xgen.mongot.index.query.aggregation.AggregationRegistry;
import com.xgen.mongot.index.query.aggregation.SearchAggregationBootstrap;
import com.xgen.mongot.index.query.aggregation.TopAggregationSpec;
import com.xgen.mongot.index.query.collectors.AggregationCollector;
import com.xgen.mongot.index.query.collectors.AggregationSpecBuilder;
import com.xgen.mongot.index.query.collectors.AggregationValidator;
import com.xgen.mongot.index.query.collectors.AggregationValidator.AggregationValidationException;
import java.util.Optional;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Plan-time 100k requested-bucket cap for search aggregation. The spec is lowered once (same as
 * Lucene / sharded planning) and {@link AggregationValidator#validateRequestedBuckets} runs on that
 * tree; parse itself does not enforce the cap.
 */
public class SearchAggregationBucketLimitTest {

  @BeforeClass
  public static void installAggregationRegistry() {
    SearchAggregationBootstrap.installRegistry();
  }

  @Test
  public void nestedTermsExactly100k_passesTheCap() throws Exception {
    TopAggregationSpec spec = lower(nestedTermsQuery(1000, 100));

    assertEquals(TopAggregationSpec.MAX_REQUESTED_BUCKETS, spec.requestedBucketCount());
    AggregationValidator.validateRequestedBuckets(spec);
  }

  @Test
  public void nestedTermsOver100k_rejectedOnLoweredSpec() throws Exception {
    TopAggregationSpec spec = lower(nestedTermsQuery(1000, 101));

    AggregationValidationException thrown =
        assertThrows(
            AggregationValidationException.class,
            () -> AggregationValidator.validateRequestedBuckets(spec));

    assertThat(thrown)
        .hasMessageThat()
        .contains(TopAggregationSpec.requestedBucketsLimitExceededMessage(101_000));
  }

  @Test
  public void nestedTermsOver100k_stillParses() throws Exception {
    SearchQuery query = parse(nestedTermsQuery(1000, 101));

    assertThat(query).isInstanceOf(CollectorQuery.class);
    assertThat(((CollectorQuery) query).collector()).isInstanceOf(AggregationCollector.class);
  }

  @Test
  public void boundaryDefault_countsTowardTheTotal() throws Exception {
    BsonDocument inner =
        stringNode("city", 10)
            .append(
                "aggregations",
                new BsonDocument("n", new BsonDocument("type", new BsonString("count"))));
    BsonDocument outer =
        new BsonDocument("type", new BsonString("number"))
            .append("path", new BsonString("price"))
            .append(
                "boundaries",
                new BsonArray(
                    java.util.List.of(new BsonInt32(0), new BsonInt32(10), new BsonInt32(20))))
            .append("default", new BsonString("other"))
            .append("facets", new BsonDocument("byCity", inner));

    TopAggregationSpec spec = lower(facetQuery(new BsonDocument("byPrice", outer)));

    // 2 ranges + default, nested under 10 terms -> 30.
    assertEquals(30, spec.requestedBucketCount());
    AggregationValidator.validateRequestedBuckets(spec);
  }

  private static TopAggregationSpec lower(BsonDocument document) throws Exception {
    SearchQuery query = parse(document);
    AggregationCollector collector = (AggregationCollector) ((CollectorQuery) query).collector();
    return AggregationSpecBuilder.toTopLevelSpec(
        collector, AggregationRegistry.getInstance(), Optional.empty());
  }

  private static SearchQuery parse(BsonDocument document) throws Exception {
    return SearchQuery.fromBson(
        document,
        QueryOptimizationFlags.DEFAULT_OPTIONS,
        new SearchQueryParseOptions(/* allow10k= */ false, /* searchAggregationEnabled= */ true));
  }

  private static BsonDocument nestedTermsQuery(int outerBuckets, int innerBuckets) {
    BsonDocument inner =
        stringNode("color", innerBuckets)
            .append(
                "aggregations",
                new BsonDocument("n", new BsonDocument("type", new BsonString("count"))));
    BsonDocument outer =
        stringNode("city", outerBuckets).append("facets", new BsonDocument("byColor", inner));
    return facetQuery(new BsonDocument("byCity", outer));
  }

  private static BsonDocument stringNode(String path, int numBuckets) {
    return new BsonDocument("type", new BsonString("string"))
        .append("path", new BsonString(path))
        .append("numBuckets", new BsonInt32(numBuckets));
  }

  private static BsonDocument facetQuery(BsonDocument facets) {
    return new BsonDocument("facet", new BsonDocument("facets", facets));
  }
}
