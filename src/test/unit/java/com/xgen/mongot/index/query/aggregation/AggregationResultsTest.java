package com.xgen.mongot.index.query.aggregation;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import com.xgen.mongot.index.query.aggregation.TestOperatorSpecs.SumResult;
import java.util.ArrayList;
import java.util.List;
import org.junit.Test;

public class AggregationResultsTest {

  @Test
  public void add_matchingChildOrder_mergesByPosition() {
    AggregationResults.Builder builder = new AggregationResults.Builder();
    builder.add(children(new SumResult("a", 1.0), new SumResult("b", 2.0)));
    builder.add(children(new SumResult("a", 10.0), new SumResult("b", 20.0)));

    AggregationResults merged = builder.build();

    assertEquals(List.of("a", "b"), names(merged));
    assertEquals(11.0, ((SumResult) merged.get("a")).value(), 0.0001);
    assertEquals(22.0, ((SumResult) merged.get("b")).value(), 0.0001);
  }

  @Test
  public void add_reorderedChildren_rejectsWithoutMerging() {
    AggregationResults.Builder builder = new AggregationResults.Builder();
    builder.add(children(new SumResult("a", 1.0), new SumResult("b", 2.0)));

    IllegalArgumentException error =
        assertThrows(
            IllegalArgumentException.class,
            () -> builder.add(children(new SumResult("b", 20.0), new SumResult("a", 10.0))));

    assertEquals(
        "Cannot merge aggregation results with different result names: [a, b] and [b, a]",
        error.getMessage());
    AggregationResults merged = builder.build();
    assertEquals(1.0, ((SumResult) merged.get("a")).value(), 0.0001);
    assertEquals(2.0, ((SumResult) merged.get("b")).value(), 0.0001);
  }

  @Test
  public void add_fewerChildren_rejectsWithoutMerging() {
    AggregationResults.Builder builder = new AggregationResults.Builder();
    builder.add(children(new SumResult("a", 1.0), new SumResult("b", 2.0)));

    IllegalArgumentException error =
        assertThrows(
            IllegalArgumentException.class, () -> builder.add(children(new SumResult("a", 10.0))));

    assertEquals(
        "Cannot merge aggregation results with different result names: [a, b] and [a]",
        error.getMessage());
    AggregationResults merged = builder.build();
    assertEquals(1.0, ((SumResult) merged.get("a")).value(), 0.0001);
    assertEquals(2.0, ((SumResult) merged.get("b")).value(), 0.0001);
  }

  @Test
  public void add_emptyThenNonEmpty_adoptsLaterChildren() {
    AggregationResults.Builder builder = new AggregationResults.Builder();
    builder.add(AggregationResults.from(List.of()));
    builder.add(children(new SumResult("a", 4.0)));

    AggregationResults merged = builder.build();

    assertEquals(List.of("a"), names(merged));
    assertEquals(4.0, ((SumResult) merged.get("a")).value(), 0.0001);
  }

  @Test
  public void add_nonEmptyThenEmpty_rejects() {
    AggregationResults.Builder builder = new AggregationResults.Builder();
    builder.add(children(new SumResult("a", 4.0)));

    IllegalArgumentException error =
        assertThrows(
            IllegalArgumentException.class, () -> builder.add(AggregationResults.from(List.of())));

    assertEquals(
        "Cannot merge aggregation results with different result names: [a] and []",
        error.getMessage());
    assertEquals(4.0, ((SumResult) builder.build().get("a")).value(), 0.0001);
  }

  private static AggregationResults children(AggregationResult... results) {
    return AggregationResults.from(List.of(results));
  }

  private static List<String> names(AggregationResults results) {
    List<String> names = new ArrayList<>();
    for (AggregationResult result : results) {
      names.add(result.getName());
    }
    return names;
  }
}
