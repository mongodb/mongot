package com.xgen.mongot.index.query.aggregation;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Container of a set of AggregationResult.
 *
 * <p>This class along with MultiBucketAggregationResult forms a tree structure that represents the
 * aggregation result.
 */
public final class AggregationResults implements Iterable<AggregationResult> {
  // Each result represents a sub-accumulator aggregation result.
  private final List<AggregationResult> results;

  // Map from user set aggregation name to its result.
  private final Map<String, AggregationResult> resultsByName;

  private AggregationResults(List<AggregationResult> results) {
    this.results = results;
    // We don't expect duplicated name for sub-accumulator results.
    this.resultsByName =
        results.stream()
            .collect(
                Collectors.toUnmodifiableMap(
                    AggregationResult::getName,
                    Function.identity(),
                    (a, b) -> {
                      throw new IllegalStateException("Duplicate result name: " + a.getName());
                    }));
  }

  @Override
  public Iterator<AggregationResult> iterator() {
    return this.results.iterator();
  }

  /** Retrieve the result by name. */
  @SuppressWarnings("unchecked")
  public <A extends AggregationResult> A get(String name) {
    Optional<AggregationResult> result = Optional.ofNullable(this.resultsByName.get(name));
    return (A) result.orElseThrow(() -> new IllegalStateException("Result not found: " + name));
  }

  public Map<String, AggregationResult> getResultsByName() {
    return this.resultsByName;
  }

  public static AggregationResults from(List<AggregationResult> results) {
    return new AggregationResults(results);
  }
}
