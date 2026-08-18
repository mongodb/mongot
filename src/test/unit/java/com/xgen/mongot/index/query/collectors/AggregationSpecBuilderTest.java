package com.xgen.mongot.index.query.collectors;

import static org.junit.Assert.assertEquals;

import com.xgen.mongot.index.query.aggregation.AggregationRegistry;
import com.xgen.mongot.index.query.aggregation.AggregationSpec;
import com.xgen.mongot.index.query.aggregation.GroupAggregationSpec;
import com.xgen.mongot.index.query.aggregation.Operator;
import com.xgen.mongot.index.query.aggregation.OperatorAggregationSpec;
import com.xgen.mongot.index.query.aggregation.TestOperatorSpecs;
import com.xgen.mongot.index.query.aggregation.TopAggregationSpec;
import com.xgen.mongot.index.query.operators.AllDocumentsOperator;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.FieldPath;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.bson.BsonInt32;
import org.junit.BeforeClass;
import org.junit.Test;

/** Lowering tests for {@link AggregationSpecBuilder#toTopLevelSpec}. */
public class AggregationSpecBuilderTest {

  @BeforeClass
  public static void setUpAggregationRegistry() {
    TestOperatorSpecs.installRegistry();
  }

  private static TopAggregationSpec lower(Map<String, AggregationFacetDefinition> facets) {
    return AggregationSpecBuilder.toTopLevelSpec(
        AggregationCollector.create(AllDocumentsOperator.INSTANCE, facets),
        AggregationRegistry.getInstance(),
        Optional.empty());
  }

  @Test
  public void legacyShapedLeaf_lowersToImplicitCountNamedCount() {
    // A legacy-shaped leaf (no aggregations) must expose its implicit count under the stable
    // legacy bucket field name "count", not the facet name, so bucket docs stay shaped like the
    // legacy facet response ({_id, count}).
    AggregationFacetDefinition leaf =
        AggregationFacetDefinition.create(
            new FacetDefinition.StringFacetDefinition("category", 10), Map.of(), Map.of());

    TopAggregationSpec top = lower(Map.of("byCategory", leaf));

    assertEquals(1, top.children().size());
    AggregationSpec group = top.children().get(0);
    assertEquals("byCategory", group.name());
    assertEquals(1, group.children().size());
    OperatorAggregationSpec count =
        Check.instanceOf(group.children().get(0), OperatorAggregationSpec.class);
    assertEquals("count", count.name());
    assertEquals(Operator.COUNT, count.getOperator());
  }

  @Test
  public void explicitMetricLeaf_keepsDeclaredOutputName() {
    AggregationFacetDefinition leaf =
        AggregationFacetDefinition.create(
            new FacetDefinition.StringFacetDefinition("category", 10),
            Map.of("n", new MetricSpec(Operator.COUNT, Optional.empty())),
            Map.of());

    TopAggregationSpec top = lower(Map.of("byCategory", leaf));

    AggregationSpec group = top.children().get(0);
    assertEquals(1, group.children().size());
    OperatorAggregationSpec metric =
        Check.instanceOf(group.children().get(0), OperatorAggregationSpec.class);
    assertEquals("n", metric.name());
    assertEquals(Operator.COUNT, metric.getOperator());
  }

  @Test
  public void nestedFacets_lowerRecursivelyWithImplicitCountLeaf() {
    AggregationFacetDefinition child =
        AggregationFacetDefinition.create(
            new FacetDefinition.StringFacetDefinition("color", 5), Map.of(), Map.of());
    AggregationFacetDefinition parent =
        AggregationFacetDefinition.create(
            new FacetDefinition.StringFacetDefinition("city", 10),
            Map.of(),
            Map.of("byColor", child));

    TopAggregationSpec top = lower(Map.of("byCity", parent));

    GroupAggregationSpec citySpec =
        Check.instanceOf(top.children().get(0), GroupAggregationSpec.class);
    assertEquals("byCity", citySpec.name());
    assertEquals(FieldPath.parse("city"), citySpec.getPath());
    assertEquals(1, citySpec.children().size());

    GroupAggregationSpec colorSpec =
        Check.instanceOf(citySpec.children().get(0), GroupAggregationSpec.class);
    assertEquals("byColor", colorSpec.name());
    assertEquals(FieldPath.parse("color"), colorSpec.getPath());
    assertEquals(1, colorSpec.children().size());
    assertEquals("count", colorSpec.children().get(0).name());
  }

  @Test
  public void numberBoundaryWithoutResolver_lowersThroughRegistry() {
    AggregationFacetDefinition leaf =
        AggregationFacetDefinition.create(
            new FacetDefinition.NumericFacetDefinition(
                "price", Optional.empty(), List.of(new BsonInt32(0), new BsonInt32(100))),
            Map.of(),
            Map.of());

    TopAggregationSpec top = lower(Map.of("byPrice", leaf));

    assertEquals(1, top.children().size());
    GroupAggregationSpec group =
        Check.instanceOf(top.children().get(0), GroupAggregationSpec.class);
    assertEquals("byPrice", group.name());
    assertEquals(FieldPath.parse("price"), group.getPath());
    assertEquals(1, group.children().size());
    assertEquals("count", group.children().get(0).name());
  }
}
