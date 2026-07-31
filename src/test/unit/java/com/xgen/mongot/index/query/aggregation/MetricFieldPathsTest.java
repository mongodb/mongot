package com.xgen.mongot.index.query.aggregation;

import static org.junit.Assert.assertEquals;

import com.xgen.mongot.index.lucene.field.FieldName.TypeField;
import com.xgen.mongot.index.query.aggregation.AggregationRegistry.GroupingArgs.TermsArgs;
import com.xgen.mongot.util.FieldPath;
import java.util.List;
import java.util.Set;
import org.junit.Test;

/**
 * Path collection feeding {@code LuceneAggregationContextFieldResolver.withResolvedFields}.
 *
 * <p>The resolver asserts each collected path resolves at most once, so the de-duplication here is
 * load-bearing rather than an optimisation: a duplicate trips that check state and fails the query.
 */
public class MetricFieldPathsTest {

  private static final FieldPath PRICE = FieldPath.parse("price");
  private static final FieldPath RATING = FieldPath.parse("rating");
  private static final FieldPath CITY = FieldPath.parse("city");

  @Test
  public void collectNumeric_includesMinAndMaxPaths() {
    Set<FieldPath> paths =
        MetricFieldPaths.collectNumeric(
            new TopAggregationSpec(
                List.of(
                    new MinAggregationSpec("min_price", PRICE),
                    new MaxAggregationSpec("max_rating", RATING))));

    assertEquals(Set.of(PRICE, RATING), paths);
  }

  @Test
  public void collectNumeric_dedupesPathSharedByMinMaxAndSum() {
    // min, max and sum over the same field is the obvious user query, and it is exactly the shape
    // that would trip the resolver's "resolved at most once" state check if paths were not a set.
    Set<FieldPath> paths =
        MetricFieldPaths.collectNumeric(
            new TopAggregationSpec(
                List.of(
                    new MinAggregationSpec("min_price", PRICE),
                    new MaxAggregationSpec("max_price", PRICE),
                    new SumAggregationSpec("sum_price", PRICE),
                    new AvgAggregationSpec("avg_price", PRICE))));

    assertEquals(Set.of(PRICE), paths);
  }

  @Test
  public void collectNumeric_recursesIntoNestedGroups() {
    // Metrics nested under a grouping stage must still be pre-resolved; missing them would surface
    // as an unresolved-path failure only once execution reached the inner bucket.
    AggregationSpec innerTerms =
        new TermsGroupAggregationSpec(
            "by_city",
            CITY,
            List.of(new MinAggregationSpec("min_price", PRICE)),
            new TermsArgs(true, TypeField.TOKEN, 100));

    Set<FieldPath> paths =
        MetricFieldPaths.collectNumeric(new TopAggregationSpec(List.of(innerTerms)));

    assertEquals(Set.of(PRICE), paths);
  }

  @Test
  public void collectNumeric_ignoresGroupingSpecsThemselves() {
    // A grouping stage's own path is a group key, not a metric path: resolving it as a numeric
    // metric would reject any terms grouping over a token field.
    Set<FieldPath> paths =
        MetricFieldPaths.collectNumeric(
            new TopAggregationSpec(
                List.of(
                    new TermsGroupAggregationSpec(
                        "by_city", CITY, List.of(), new TermsArgs(true, TypeField.TOKEN, 100)))));

    assertEquals(Set.of(), paths);
  }

  @Test
  public void collectToken_noLongerSeesMinAndMax() {
    // min/max became numeric metrics; if they were still collected as token metrics the resolver
    // would additionally demand stored-source coverage for the path.
    Set<FieldPath> paths =
        MetricFieldPaths.collectToken(
            new TopAggregationSpec(
                List.of(
                    new MinAggregationSpec("min_price", PRICE),
                    new MaxAggregationSpec("max_price", PRICE))));

    assertEquals(Set.of(), paths);
  }
}
