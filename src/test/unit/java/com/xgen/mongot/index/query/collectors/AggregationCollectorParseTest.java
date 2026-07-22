package com.xgen.mongot.index.query.collectors;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;
import static org.junit.Assert.assertThrows;

import com.google.common.collect.ImmutableList;
import com.xgen.mongot.index.query.aggregation.Operator;
import com.xgen.mongot.index.query.sort.MongotSortField;
import com.xgen.mongot.index.query.sort.Sort;
import com.xgen.mongot.index.query.sort.UserFieldSortOptions;
import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.bson.parser.BsonDocumentParser;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.junit.Test;

/**
 * Unit coverage for the aggregation-facet parser: {@link AggregationCollector#fromBson}, {@link
 * AggregationFacetDefinition}, {@link MetricSpec}, and the {@link
 * AggregationCollector#isAggregationCapable} discriminator.
 */
public class AggregationCollectorParseTest {

  private static AggregationCollector parse(BsonDocument facet) throws BsonParseException {
    try (DocumentParser parser = BsonDocumentParser.fromRoot(facet).build()) {
      return AggregationCollector.fromBson(parser, /* allow10k= */ false);
    }
  }

  private static BsonDocument metric(String type, String path) {
    BsonDocument doc = new BsonDocument("type", new BsonString(type));
    if (path != null) {
      doc.append("path", new BsonString(path));
    }
    return doc;
  }

  private static BsonDocument stringNode(String path) {
    return new BsonDocument("type", new BsonString("string")).append("path", new BsonString(path));
  }

  private static BsonDocument numberNode(String path) {
    return new BsonDocument("type", new BsonString("number"))
        .append("path", new BsonString(path))
        .append(
            "boundaries",
            new BsonArray(
                java.util.List.of(new BsonInt32(0), new BsonInt32(10), new BsonInt32(20))));
  }

  private static BsonDocument facetWith(BsonDocument facets) {
    return new BsonDocument("facets", facets);
  }

  @Test
  public void canonicalNestedExample_parses() throws Exception {
    // The finalized "Named Recursive Nested Aggregation" example from the syntax doc: a string
    // intermediate node (byGroupFacet) whose child is a number leaf (capacityFacet) carrying
    // average/count metrics.
    BsonDocument capacityFacet =
        new BsonDocument("type", new BsonString("number"))
            .append("path", new BsonString("capacity"))
            .append(
                "boundaries",
                new BsonArray(
                    java.util.List.of(
                        new BsonInt32(0),
                        new BsonInt32(100),
                        new BsonInt32(300),
                        new BsonInt32(500))))
            .append("default", new BsonString("other"))
            .append(
                "aggregations",
                new BsonDocument("avgPrice", metric("avg", "price"))
                    .append("capacityCount", metric("count", null)));
    BsonDocument byGroupFacet =
        stringNode("category")
            .append("numBuckets", new BsonInt32(20))
            .append("facets", new BsonDocument("capacityFacet", capacityFacet));

    AggregationCollector collector =
        parse(facetWith(new BsonDocument("byGroupFacet", byGroupFacet)));

    AggregationFacetDefinition top = collector.facets().get("byGroupFacet");
    assertThat(top.isLeaf()).isFalse();
    assertThat(top.aggregations()).isEmpty(); // aggregation prohibited on intermediate node
    assertThat(((FacetDefinition.StringFacetDefinition) top.group()).numBuckets()).isEqualTo(20);

    AggregationFacetDefinition child = top.facets().get("capacityFacet");
    assertThat(child.isLeaf()).isTrue();
    assertThat(child.aggregations().get("avgPrice").operator()).isEqualTo(Operator.AVG);
    assertThat(child.aggregations().get("avgPrice").path()).hasValue(FieldPath.parse("price"));
    assertThat(child.aggregations().get("capacityCount").operator()).isEqualTo(Operator.COUNT);
    FacetDefinition.NumericFacetDefinition numeric =
        (FacetDefinition.NumericFacetDefinition) child.group();
    assertThat(numeric.defaultBucketName()).hasValue("other");
    assertThat(numeric.boundaries()).hasSize(4);
  }

  @Test
  public void roundTripsThroughBson() throws Exception {
    // Every valid case of the JSON-driven deserialization suite must parse-encode-reparse to an
    // equal model. This includes legacy-shaped leaves, whose materialized implicit count metric is
    // encoded explicitly yet reparses equal.
    BsonDocument suite =
        BsonDocument.parse(
            Files.readString(
                Path.of("src/test/unit/resources/index/query/collectors/aggregation-facet.json")));

    for (BsonValue example : suite.getArray("valid")) {
      BsonDocument exampleDocument = example.asDocument();
      String description = exampleDocument.getString("description").getValue();
      AggregationCollector original = parse(exampleDocument.getDocument("value"));

      BsonDocument encoded = original.collectorToBson().asDocument();
      AggregationCollector reparsed = parse(encoded);

      assertWithMessage("round trip of valid case '%s'", description)
          .that(reparsed)
          .isEqualTo(original);
    }
  }

  @Test
  public void collectorToBson_omitsBorrowedSortSpec() throws Exception {
    // The global sortSpec is a sibling of the collector, serialized by the enclosing query, so it
    // must not leak into the facet-collector document. Encoding a collector carrying a sortSpec
    // therefore yields the same BSON as the same collector without one, and the encoded document
    // reparses to a collector with an empty sortSpec.
    AggregationCollector withoutSort =
        parse(facetWith(new BsonDocument("byCategory", metricLeaf())));
    AggregationCollector withSort =
        withoutSort.withSortSpec(
            Optional.of(
                new Sort(
                    ImmutableList.of(
                        new MongotSortField(
                            FieldPath.newRoot("price"), UserFieldSortOptions.DEFAULT_ASC)))));

    BsonDocument encodedWithoutSort = withoutSort.collectorToBson().asDocument();
    BsonDocument encodedWithSort = withSort.collectorToBson().asDocument();

    assertThat(encodedWithSort).isEqualTo(encodedWithoutSort);
    assertThat(encodedWithSort.toJson()).doesNotContain("sort");
    assertThat(parse(encodedWithSort).sortSpec()).isEmpty();
  }

  private static BsonDocument metricLeaf() {
    return stringNode("category")
        .append("aggregations", new BsonDocument("n", metric("count", null)));
  }

  @Test
  public void stringLeafWithMetrics_parses() throws Exception {
    BsonDocument node =
        stringNode("category")
            .append("numBuckets", new BsonInt32(20))
            .append(
                "aggregations",
                new BsonDocument("total", metric("sum", "price"))
                    .append("n", metric("count", null)));

    AggregationCollector collector = parse(facetWith(new BsonDocument("byCategory", node)));

    assertThat(collector.facets().keySet()).containsExactly("byCategory");
    AggregationFacetDefinition def = collector.facets().get("byCategory");
    assertThat(def.isLeaf()).isTrue();
    assertThat(def.facets()).isEmpty();
    // Declared order is preserved (MapField parses into an insertion-ordered map).
    assertThat(def.aggregations().keySet()).containsExactly("total", "n").inOrder();
    assertThat(def.aggregations().get("total").operator()).isEqualTo(Operator.SUM);
    assertThat(def.aggregations().get("total").path()).hasValue(FieldPath.parse("price"));
    assertThat(def.aggregations().get("n").operator()).isEqualTo(Operator.COUNT);
    assertThat(def.aggregations().get("n").path()).isEmpty();
    assertThat(((FacetDefinition.StringFacetDefinition) def.group()).numBuckets()).isEqualTo(20);
  }

  @Test
  public void nestedFacets_parseAsIntermediateNode() throws Exception {
    BsonDocument inner =
        numberNode("capacity")
            .append("aggregations", new BsonDocument("avgPrice", metric("avg", "price")));
    BsonDocument outer =
        stringNode("category").append("facets", new BsonDocument("byCapacity", inner));

    AggregationCollector collector = parse(facetWith(new BsonDocument("byCategory", outer)));

    AggregationFacetDefinition outerDef = collector.facets().get("byCategory");
    assertThat(outerDef.isLeaf()).isFalse();
    assertThat(outerDef.aggregations()).isEmpty();
    assertThat(outerDef.facets().keySet()).containsExactly("byCapacity");

    AggregationFacetDefinition innerDef = outerDef.facets().get("byCapacity");
    assertThat(innerDef.isLeaf()).isTrue();
    assertThat(innerDef.aggregations().get("avgPrice").operator()).isEqualTo(Operator.AVG);
  }

  @Test
  public void legacyShapedLeaf_materializesImplicitCountMetric() throws Exception {
    AggregationCollector collector =
        parse(facetWith(new BsonDocument("byCategory", stringNode("category"))));

    // A node with neither aggregations nor nested facets is a count leaf per the syntax doc; the
    // implicit metric is materialized at construction time under the legacy bucket field name.
    AggregationFacetDefinition def = collector.facets().get("byCategory");
    assertThat(def.isLeaf()).isTrue();
    assertThat(def.aggregations().keySet()).containsExactly("count");
    MetricSpec implicitCount = def.aggregations().get("count");
    assertThat(implicitCount.operator()).isEqualTo(Operator.COUNT);
    assertThat(implicitCount.path()).isEmpty();
    assertThat(def.facets()).isEmpty();
  }

  @Test
  public void aggregationsAndNestedFacetsOnSameNode_rejected() {
    BsonDocument node =
        stringNode("category")
            .append("aggregations", new BsonDocument("n", metric("count", null)))
            .append("facets", new BsonDocument("byCapacity", numberNode("capacity")));

    BsonParseException ex =
        assertThrows(
            BsonParseException.class, () -> parse(facetWith(new BsonDocument("byCategory", node))));
    assertThat(ex).hasMessageThat().contains("aggregations are leaf-only");
  }

  @Test
  public void multipleNestedFacetBranches_rejected() {
    // Two sibling branches under one intermediate node: deferred until the sharded merge planner
    // supports more than one nested grouping child per level.
    BsonDocument node =
        stringNode("category")
            .append(
                "facets",
                new BsonDocument("byCapacity", numberNode("capacity"))
                    .append("byWeight", numberNode("weight")));

    BsonParseException ex =
        assertThrows(
            BsonParseException.class, () -> parse(facetWith(new BsonDocument("byCategory", node))));
    assertThat(ex)
        .hasMessageThat()
        .contains("multiple branches at an intermediate level are not yet supported");
  }

  @Test
  public void multipleTopLevelFacets_parse() throws Exception {
    // Multiple top-level facets are independent trees, not intermediate branches, so they remain
    // allowed.
    BsonDocument byCategory =
        stringNode("category").append("aggregations", new BsonDocument("n", metric("count", null)));
    BsonDocument byCapacity =
        numberNode("capacity")
            .append("aggregations", new BsonDocument("avgPrice", metric("avg", "price")));

    AggregationCollector collector =
        parse(
            facetWith(new BsonDocument("byCategory", byCategory).append("byCapacity", byCapacity)));

    assertThat(collector.facets().keySet()).containsExactly("byCategory", "byCapacity").inOrder();
  }

  @Test
  public void countWithPath_rejected() {
    BsonDocument node =
        stringNode("category")
            .append("aggregations", new BsonDocument("n", metric("count", "price")));

    BsonParseException ex =
        assertThrows(
            BsonParseException.class, () -> parse(facetWith(new BsonDocument("byCategory", node))));
    assertThat(ex).hasMessageThat().contains("'count' does not take a path");
  }

  @Test
  public void nonCountWithoutPath_rejected() {
    BsonDocument node =
        stringNode("category")
            .append("aggregations", new BsonDocument("total", metric("sum", null)));

    BsonParseException ex =
        assertThrows(
            BsonParseException.class, () -> parse(facetWith(new BsonDocument("byCategory", node))));
    assertThat(ex).hasMessageThat().contains("requires a path");
  }

  // Group options of another facet type (numBuckets on number/date, boundaries/default on string)
  // are rejected by the node parser's close as unrecognized fields, matching legacy facet errors.
  @Test
  public void numBucketsOnNumberNode_rejectedAsUnrecognizedField() {
    BsonDocument node =
        numberNode("capacity")
            .append("numBuckets", new BsonInt32(5))
            .append(
                "aggregations",
                new BsonDocument("n", new BsonDocument("type", new BsonString("count"))));

    BsonParseException ex =
        assertThrows(
            BsonParseException.class, () -> parse(facetWith(new BsonDocument("byCapacity", node))));
    assertThat(ex).hasMessageThat().contains("unrecognized field \"numBuckets\"");
  }

  @Test
  public void boundariesOnStringNode_rejectedAsUnrecognizedField() {
    assertMisplacedStringNodeFieldRejected(
        stringNode("category")
            .append("boundaries", new BsonArray(java.util.List.of(new BsonInt32(0)))),
        "boundaries");
  }

  @Test
  public void defaultOnStringNode_rejectedAsUnrecognizedField() {
    assertMisplacedStringNodeFieldRejected(
        stringNode("category").append("default", new BsonString("other")), "default");
  }

  private static void assertMisplacedStringNodeFieldRejected(BsonDocument node, String fieldName) {
    node.append(
        "aggregations", new BsonDocument("n", new BsonDocument("type", new BsonString("count"))));

    BsonParseException ex =
        assertThrows(
            BsonParseException.class, () -> parse(facetWith(new BsonDocument("byCategory", node))));
    assertThat(ex).hasMessageThat().contains("unrecognized field \"" + fieldName + "\"");
  }

  @Test
  public void nonStringMetricPath_rejectedAtParse() {
    // Metric paths convert to FieldPath at parse time (via FieldPathField), so a non-string path
    // is a parse-time type error rather than a runtime failure at lowering.
    BsonDocument node =
        stringNode("category")
            .append(
                "aggregations",
                new BsonDocument(
                    "total",
                    new BsonDocument("type", new BsonString("sum"))
                        .append("path", new BsonInt32(5))));

    assertThrows(
        BsonParseException.class, () -> parse(facetWith(new BsonDocument("byCategory", node))));
  }

  // first/last are absent from the Operator enum until global-sort plumbing lands
  // (TODO(CLOUDP-425978)), so they are rejected as unknown metric types.
  @Test
  public void firstMetric_rejectedAsUnsupported() {
    assertMetricRejectedAsUnsupported("first");
  }

  @Test
  public void lastMetric_rejectedAsUnsupported() {
    assertMetricRejectedAsUnsupported("last");
  }

  private static void assertMetricRejectedAsUnsupported(String op) {
    BsonDocument node =
        stringNode("category")
            .append("aggregations", new BsonDocument("pick", metric(op, "price")));

    assertThrows(
        BsonParseException.class, () -> parse(facetWith(new BsonDocument("byCategory", node))));
  }

  @Test
  public void isAggregationCapable_detectsAggregationsAndNestedFacets() {
    BsonDocument aggCapable =
        new BsonDocument(
            "facet",
            facetWith(
                new BsonDocument(
                    "byCategory",
                    stringNode("category")
                        .append("aggregations", new BsonDocument("n", metric("count", null))))));
    assertThat(AggregationCollector.isAggregationCapable(aggCapable)).isTrue();

    BsonDocument nested =
        new BsonDocument(
            "facet",
            facetWith(
                new BsonDocument(
                    "byCategory",
                    stringNode("category")
                        .append(
                            "facets", new BsonDocument("byCapacity", numberNode("capacity"))))));
    assertThat(AggregationCollector.isAggregationCapable(nested)).isTrue();
  }

  @Test
  public void isAggregationCapable_falseForLegacyOnlyAndMissingFacet() {
    BsonDocument legacy =
        new BsonDocument(
            "facet", facetWith(new BsonDocument("byCategory", stringNode("category"))));
    assertThat(AggregationCollector.isAggregationCapable(legacy)).isFalse();

    assertThat(AggregationCollector.isAggregationCapable(new BsonDocument())).isFalse();
  }
}
