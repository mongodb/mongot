package com.xgen.mongot.index.query.collectors;

import static com.xgen.testing.BsonDeserializationTestSuite.fromDocument;

import com.xgen.mongot.index.query.aggregation.Operator;
import com.xgen.mongot.index.query.operators.AllDocumentsOperator;
import com.xgen.mongot.util.FieldPath;
import com.xgen.testing.BsonDeserializationTestSuite;
import com.xgen.testing.BsonDeserializationTestSuite.ValidSpec;
import com.xgen.testing.mongot.index.query.operators.OperatorBuilder;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.bson.BsonInt32;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * JSON-driven deserialization suite for {@link AggregationCollector#fromBson}, following the {@link
 * FacetCollectorTest} convention: valid examples pair a resource-file document with the expected
 * parsed object (the suite also calls toBson to check encoding does not throw), and invalid
 * examples assert a {@link com.xgen.mongot.util.bson.parser.BsonParseException} whose message
 * contains the resource-declared fragment.
 *
 * <p>Full parse-encode-reparse round-trip equality is covered by {@code
 * AggregationCollectorParseTest#roundTripsThroughBson}.
 */
@RunWith(Parameterized.class)
public class AggregationCollectorDeserializationTest {

  private static final String SUITE_NAME = "aggregation-facet";
  private static final BsonDeserializationTestSuite<AggregationCollector> TEST_SUITE =
      fromDocument(
          "src/test/unit/resources/index/query/collectors/",
          SUITE_NAME,
          parser -> AggregationCollector.fromBson(parser, /* allow10k= */ false));

  private final BsonDeserializationTestSuite.TestSpecWrapper<AggregationCollector> testSpec;

  public AggregationCollectorDeserializationTest(
      BsonDeserializationTestSuite.TestSpecWrapper<AggregationCollector> testSpec) {
    this.testSpec = testSpec;
  }

  @Parameterized.Parameters(name = "{0}")
  public static Iterable<BsonDeserializationTestSuite.TestSpecWrapper<AggregationCollector>>
      data() {
    return TEST_SUITE.withExamples(
        legacyShapedEntry(),
        stringLeafWithMetrics(),
        nestedFacetsWithMetricsLeaf(),
        mixedLegacyAndAggregationEntries(),
        explicitOperator(),
        threeLevelNestingWithAverageLeaf(),
        multipleTopLevelFacetsWithDateSibling(),
        numberLeafWithMetrics(),
        dateLeafWithMetrics());
  }

  @Test
  public void runTest() throws Exception {
    TEST_SUITE.runTest(this.testSpec);
  }

  private static ValidSpec<AggregationCollector> legacyShapedEntry() {
    return BsonDeserializationTestSuite.TestSpec.valid(
        "legacy shaped entry",
        AggregationCollector.create(
            AllDocumentsOperator.INSTANCE,
            Map.of("byCategory", node(stringGroup("category", 10), Map.of(), Map.of()))));
  }

  private static ValidSpec<AggregationCollector> stringLeafWithMetrics() {
    return BsonDeserializationTestSuite.TestSpec.valid(
        "string leaf with metrics",
        AggregationCollector.create(
            AllDocumentsOperator.INSTANCE,
            Map.of(
                "byCategory",
                node(
                    stringGroup("category", 20),
                    Map.of("total", sum("price"), "n", count()),
                    Map.of()))));
  }

  private static ValidSpec<AggregationCollector> nestedFacetsWithMetricsLeaf() {
    AggregationFacetDefinition capacityFacet =
        node(
            new FacetDefinition.NumericFacetDefinition(
                "capacity",
                Optional.of("other"),
                List.of(
                    new BsonInt32(0), new BsonInt32(100), new BsonInt32(300), new BsonInt32(500))),
            Map.of("avgPrice", average("price"), "capacityCount", count()),
            Map.of());
    return BsonDeserializationTestSuite.TestSpec.valid(
        "nested facets with metrics leaf",
        AggregationCollector.create(
            AllDocumentsOperator.INSTANCE,
            Map.of(
                "byGroupFacet",
                node(
                    stringGroup("category", 20),
                    Map.of(),
                    Map.of("capacityFacet", capacityFacet)))));
  }

  private static ValidSpec<AggregationCollector> mixedLegacyAndAggregationEntries() {
    return BsonDeserializationTestSuite.TestSpec.valid(
        "mixed legacy and aggregation entries",
        AggregationCollector.create(
            AllDocumentsOperator.INSTANCE,
            Map.of(
                "byBrand",
                node(stringGroup("brand", 10), Map.of(), Map.of()),
                "byCategory",
                node(stringGroup("category", 10), Map.of("total", sum("price")), Map.of()))));
  }

  private static ValidSpec<AggregationCollector> explicitOperator() {
    return BsonDeserializationTestSuite.TestSpec.valid(
        "explicit operator",
        AggregationCollector.create(
            OperatorBuilder.text().path("review").query("good").build(),
            Map.of(
                "byCategory", node(stringGroup("category", 10), Map.of("n", count()), Map.of()))));
  }

  private static ValidSpec<AggregationCollector> threeLevelNestingWithAverageLeaf() {
    AggregationFacetDefinition capacityFacet =
        node(
            numberGroup("capacity", Optional.of("other"), 0, 100, 300, 500),
            Map.of("avgCapacity", average("capacity"), "n", count()),
            Map.of());
    AggregationFacetDefinition createdAtFacet =
        node(
            dateGroup(
                "createdAt",
                Optional.of("other"),
                "2023-01-01T00:00:00Z",
                "2024-01-01T00:00:00Z",
                "2025-01-01T00:00:00Z"),
            Map.of(),
            Map.of("capacityFacet", capacityFacet));
    return BsonDeserializationTestSuite.TestSpec.valid(
        "three level nesting with average leaf",
        AggregationCollector.create(
            AllDocumentsOperator.INSTANCE,
            Map.of(
                "byGroupFacet",
                node(
                    stringGroup("category", 20),
                    Map.of(),
                    Map.of("createdAtFacet", createdAtFacet)))));
  }

  private static ValidSpec<AggregationCollector> multipleTopLevelFacetsWithDateSibling() {
    return BsonDeserializationTestSuite.TestSpec.valid(
        "multiple top level facets with date sibling",
        AggregationCollector.create(
            OperatorBuilder.exists().path("category").build(),
            Map.of(
                "categoryFacet",
                node(
                    stringGroup("category", 20),
                    Map.of("avgCapacity", average("capacity")),
                    Map.of()),
                "createdAtFacet",
                node(
                    dateGroup(
                        "createdAt",
                        Optional.of("other"),
                        "2023-01-01T00:00:00Z",
                        "2024-01-01T00:00:00Z"),
                    Map.of(),
                    Map.of()),
                "capacityFacet",
                node(
                    numberGroup("capacity", Optional.of("other"), 0, 100, 300, 500),
                    Map.of(),
                    Map.of()))));
  }

  private static ValidSpec<AggregationCollector> numberLeafWithMetrics() {
    return BsonDeserializationTestSuite.TestSpec.valid(
        "number leaf with metrics",
        AggregationCollector.create(
            AllDocumentsOperator.INSTANCE,
            Map.of(
                "byCapacity",
                node(
                    numberGroup("capacity", Optional.of("other"), 0, 100, 300, 500),
                    Map.of("total", sum("price"), "n", count()),
                    Map.of()))));
  }

  private static ValidSpec<AggregationCollector> dateLeafWithMetrics() {
    return BsonDeserializationTestSuite.TestSpec.valid(
        "date leaf with metrics",
        AggregationCollector.create(
            AllDocumentsOperator.INSTANCE,
            Map.of(
                "byCreatedAt",
                node(
                    dateGroup(
                        "createdAt",
                        Optional.of("other"),
                        "2023-01-01T00:00:00Z",
                        "2024-01-01T00:00:00Z"),
                    Map.of("avgPrice", average("price")),
                    Map.of()))));
  }

  private static FacetDefinition.NumericFacetDefinition numberGroup(
      String path, Optional<String> defaultBucket, int... boundaries) {
    return new FacetDefinition.NumericFacetDefinition(
        path,
        defaultBucket,
        java.util.Arrays.stream(boundaries)
            .mapToObj(BsonInt32::new)
            .collect(java.util.stream.Collectors.toList()));
  }

  private static FacetDefinition.DateFacetDefinition dateGroup(
      String path, Optional<String> defaultBucket, String... instants) {
    return new FacetDefinition.DateFacetDefinition(
        path,
        defaultBucket,
        java.util.Arrays.stream(instants)
            .map(i -> new org.bson.BsonDateTime(java.time.Instant.parse(i).toEpochMilli()))
            .collect(java.util.stream.Collectors.toList()));
  }

  private static FacetDefinition.StringFacetDefinition stringGroup(String path, int numBuckets) {
    return new FacetDefinition.StringFacetDefinition(path, numBuckets);
  }

  private static AggregationFacetDefinition node(
      FacetDefinition group,
      Map<String, MetricSpec> aggregations,
      Map<String, AggregationFacetDefinition> facets) {
    return AggregationFacetDefinition.create(group, aggregations, facets);
  }

  private static MetricSpec count() {
    return new MetricSpec(Operator.COUNT, Optional.empty());
  }

  private static MetricSpec sum(String path) {
    return new MetricSpec(Operator.SUM, Optional.of(FieldPath.parse(path)));
  }

  private static MetricSpec average(String path) {
    return new MetricSpec(Operator.AVG, Optional.of(FieldPath.parse(path)));
  }
}
