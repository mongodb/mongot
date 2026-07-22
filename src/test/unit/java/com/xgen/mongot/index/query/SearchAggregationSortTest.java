package com.xgen.mongot.index.query;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.xgen.mongot.index.query.collectors.AggregationCollector;
import com.xgen.mongot.index.query.collectors.FacetCollector;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.junit.Test;

/**
 * Covers the global {@code sort} rules for aggregation first/last ordering: on an aggregation
 * request, a {@code sort} is rejected when no facet tree carries a first/last metric (it would be
 * silently ignored) and is otherwise borrowed onto the {@link AggregationCollector}; on a legacy
 * facet request, a {@code sort} stays accepted-and-ignored exactly as today, in both flag states.
 */
public class SearchAggregationSortTest {

  private static final BsonDocument SORT = new BsonDocument("price", new BsonInt32(1));

  private static BsonDocument stringNode(String path) {
    return new BsonDocument("type", new BsonString("string")).append("path", new BsonString(path));
  }

  private static BsonDocument facetWith(BsonDocument nodes) {
    return new BsonDocument("facet", new BsonDocument("facets", nodes));
  }

  private static SearchQuery parse(BsonDocument document, boolean searchAggregationEnabled)
      throws BsonParseException {
    return SearchQuery.fromBson(
        document,
        QueryOptimizationFlags.DEFAULT_OPTIONS,
        /* allow10k= */ false,
        searchAggregationEnabled);
  }

  @Test
  public void aggregationRequestWithSortButNoFirstOrLast_rejected() {
    // The global sort only orders first/last metrics; with none in the tree (here: count only) it
    // would be silently ignored, so it is rejected. Once first/last parse (sort plumbing
    // follow-up), a tree containing one borrows the sort onto the AggregationCollector instead.
    BsonDocument aggNode =
        stringNode("category")
            .append(
                "aggregations",
                new BsonDocument("n", new BsonDocument("type", new BsonString("count"))));
    BsonDocument document = facetWith(new BsonDocument("byCategory", aggNode)).append("sort", SORT);

    BsonParseException ex =
        assertThrows(
            BsonParseException.class, () -> parse(document, /* searchAggregationEnabled= */ true));
    assertThat(ex)
        .hasMessageThat()
        .contains(
            "sort (or sortBetaV1) is only supported when at least one 'first' or 'last' aggregation"
                + " metric");
  }

  @Test
  public void legacySearchMetaWithSort_acceptedAndIgnoredWhenFlagOff() throws Exception {
    assertLegacySortAcceptedAndIgnored(/* searchAggregationEnabled= */ false);
  }

  @Test
  public void legacySearchMetaWithSort_acceptedAndIgnoredWhenFlagOn() throws Exception {
    // The legacy facet path is untouched by the feature flag: a sort on a legacy facet request
    // stays accepted-and-ignored exactly as today, so enabling the flag cannot break existing
    // workloads.
    assertLegacySortAcceptedAndIgnored(/* searchAggregationEnabled= */ true);
  }

  private static void assertLegacySortAcceptedAndIgnored(boolean searchAggregationEnabled)
      throws Exception {
    BsonDocument document =
        facetWith(new BsonDocument("byCategory", stringNode("category"))).append("sort", SORT);

    SearchQuery query = parse(document, searchAggregationEnabled);
    assertThat(query).isInstanceOf(CollectorQuery.class);
    assertThat(((CollectorQuery) query).collector()).isNotInstanceOf(AggregationCollector.class);
  }

  @Test
  public void aggregationCapableRequestWithoutSort_hasEmptySortSpec() throws Exception {
    BsonDocument aggNode =
        stringNode("category")
            .append(
                "aggregations",
                new BsonDocument("n", new BsonDocument("type", new BsonString("count"))));
    BsonDocument document = facetWith(new BsonDocument("byCategory", aggNode));

    SearchQuery query = parse(document, /* searchAggregationEnabled= */ true);

    AggregationCollector collector = (AggregationCollector) ((CollectorQuery) query).collector();
    assertThat(collector.sortSpec()).isEmpty();
  }

  @Test
  public void aggregationCapableRequest_routesToAggregationCollectorWhenFlagOn() throws Exception {
    // An aggregation-capable facet request (carrying an `aggregations` key) is parsed onto the
    // aggregation path only when the flag is on.
    BsonDocument document = facetWith(new BsonDocument("byCategory", aggregationCapableNode()));

    SearchQuery query = parse(document, /* searchAggregationEnabled= */ true);

    assertThat(query).isInstanceOf(CollectorQuery.class);
    assertThat(((CollectorQuery) query).collector()).isInstanceOf(AggregationCollector.class);
  }

  @Test
  public void aggregationCapableRequest_rejectedByLegacyPathWhenFlagOff() {
    // With the flag off the request is parsed on the legacy FacetCollector path, which does not
    // recognize the `aggregations` key and rejects it. Only enabling the feature admits the shape.
    BsonDocument document = facetWith(new BsonDocument("byCategory", aggregationCapableNode()));

    BsonParseException ex =
        assertThrows(
            BsonParseException.class, () -> parse(document, /* searchAggregationEnabled= */ false));
    assertThat(ex).hasMessageThat().contains("unrecognized field \"aggregations\"");
  }

  @Test
  public void legacyFacetRequest_routesToFacetCollectorInBothFlagStates() throws Exception {
    // A purely legacy facet request (no `aggregations`/nested `facets`) always stays on the
    // FacetCollector path regardless of the flag.
    BsonDocument document = facetWith(new BsonDocument("byCategory", stringNode("category")));

    for (boolean flag : new boolean[] {false, true}) {
      SearchQuery query = parse(document, /* searchAggregationEnabled= */ flag);
      assertThat(query).isInstanceOf(CollectorQuery.class);
      assertThat(((CollectorQuery) query).collector()).isInstanceOf(FacetCollector.class);
    }
  }

  private static BsonDocument aggregationCapableNode() {
    return stringNode("category")
        .append(
            "aggregations",
            new BsonDocument("n", new BsonDocument("type", new BsonString("count"))));
  }
}
