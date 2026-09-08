package com.xgen.mongot.index.query.aggregation;

import static org.junit.Assert.assertEquals;

import com.xgen.mongot.index.lucene.field.FieldName.TypeField;
import com.xgen.mongot.index.query.aggregation.AggregationRegistry.GroupingArgs.BoundaryArgs;
import com.xgen.mongot.index.query.aggregation.AggregationRegistry.GroupingArgs.TermsArgs;
import com.xgen.mongot.util.FieldPath;
import java.util.List;
import java.util.Optional;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.junit.Test;

/** Tests for {@link AggregationSpec#requestedBucketCount()}. */
public class TopAggregationSpecTest {

  private static final FieldPath CITY = FieldPath.parse("city");
  private static final FieldPath COLOR = FieldPath.parse("color");
  private static final FieldPath PRICE = FieldPath.parse("price");

  @Test
  public void requestedBucketCount_singleTermsLeaf_isNumBuckets() {
    TopAggregationSpec top =
        new TopAggregationSpec(List.of(terms("byCity", CITY, 10, countLeaf())));

    assertEquals(10, top.requestedBucketCount());
  }

  @Test
  public void requestedBucketCount_nestedTerms_isProduct() {
    TermsGroupAggregationSpec inner = terms("byColor", COLOR, 10, countLeaf());
    TopAggregationSpec top = new TopAggregationSpec(List.of(terms("byCity", CITY, 10, inner)));

    assertEquals(100, top.requestedBucketCount());
  }

  @Test
  public void requestedBucketCount_independentTopLevelTrees_areSummed() {
    TopAggregationSpec top =
        new TopAggregationSpec(
            List.of(
                terms("byCity", CITY, 10, countLeaf()), terms("byColor", COLOR, 20, countLeaf())));

    assertEquals(30, top.requestedBucketCount());
  }

  @Test
  public void requestedBucketCount_boundaryRanges_excludeUpperEdge() {
    TopAggregationSpec top =
        new TopAggregationSpec(List.of(boundary("byPrice", PRICE, false, countLeaf())));

    // Three boundary values -> two half-open ranges.
    assertEquals(2, top.requestedBucketCount());
  }

  @Test
  public void requestedBucketCount_boundaryDefault_countsAsExtraBucket() {
    TopAggregationSpec top =
        new TopAggregationSpec(List.of(boundary("byPrice", PRICE, true, countLeaf())));

    // Two ranges + default.
    assertEquals(3, top.requestedBucketCount());
  }

  @Test
  public void requestedBucketCount_nestedTermsUnderBoundaryWithDefault_multiplies() {
    TermsGroupAggregationSpec inner = terms("byCity", CITY, 10, countLeaf());
    TopAggregationSpec top =
        new TopAggregationSpec(List.of(boundary("byPrice", PRICE, true, inner)));

    assertEquals(30, top.requestedBucketCount());
  }

  @Test
  public void requestedBucketCount_metricLeaves_doNotMultiply() {
    TermsGroupAggregationSpec twoMetrics =
        terms(
            "byCity",
            CITY,
            10,
            new CountAggregationSpec("n"),
            new SumAggregationSpec("sum_price", PRICE));
    TopAggregationSpec top = new TopAggregationSpec(List.of(twoMetrics));

    assertEquals(10, top.requestedBucketCount());
  }

  @Test
  public void requestedBucketCount_topLevelMetric_contributesZero() {
    TopAggregationSpec top = new TopAggregationSpec(List.of(new CountAggregationSpec("n")));

    assertEquals(0, top.requestedBucketCount());
  }

  @Test
  public void requestedBucketCount_exactlyMax_isAllowedByTheCap() {
    TermsGroupAggregationSpec inner = terms("byColor", COLOR, 100, countLeaf());
    TopAggregationSpec top = new TopAggregationSpec(List.of(terms("byCity", CITY, 1000, inner)));

    assertEquals(TopAggregationSpec.MAX_REQUESTED_BUCKETS, top.requestedBucketCount());
  }

  @Test
  public void requestedBucketCount_oneOverMax_exceedsTheCap() {
    TermsGroupAggregationSpec inner = terms("byColor", COLOR, 101, countLeaf());
    TopAggregationSpec top = new TopAggregationSpec(List.of(terms("byCity", CITY, 1000, inner)));

    assertEquals(101_000, top.requestedBucketCount());
  }

  private static CountAggregationSpec countLeaf() {
    return new CountAggregationSpec("count");
  }

  private static TermsGroupAggregationSpec terms(
      String name, FieldPath path, int numBuckets, AggregationSpec... children) {
    return new TermsGroupAggregationSpec(
        name, path, List.of(children), new TermsArgs(true, TypeField.TOKEN, numBuckets));
  }

  private static BoundaryGroupAggregationSpec boundary(
      String name, FieldPath path, boolean withDefault, AggregationSpec... children) {
    return BoundaryGroupAggregationSpec.createBoundarySpecWithoutResolving(
        name,
        path,
        List.of(children),
        new BoundaryArgs(
            List.of(new BsonInt32(0), new BsonInt32(10), new BsonInt32(20)),
            withDefault ? Optional.of(new BsonString("other")) : Optional.empty(),
            Optional.empty(),
            Optional.empty()));
  }
}
