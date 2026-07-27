package com.xgen.mongot.index.query.aggregation;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Container of a set of {@link AggregationResult}, ordered and indexed by name.
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

  /**
   * Apply the top-N limit to every contained result. See {@link AggregationResult#applyLimit()} for
   * the semantics and the standalone-only calling restriction.
   */
  AggregationResults applyLimit() {
    return from(this.results.stream().map(AggregationResult::applyLimit).toList());
  }

  /**
   * Merge state for an {@link AggregationResults}: one {@link AggregationResultBuilder} per named
   * result, matched by name across partitions (same spec tree on every partition).
   *
   * <p>Starts as the merge identity: the first {@link #add} lays out empty builders (fixing the
   * order to that container's order) and then folds like any other add, so there is no
   * first-element special case. Subsequent adds must carry the same names.
   *
   * <p>Not thread-safe: {@link #add} must be called from a single thread. Behavior is undefined if
   * concurrent threads call {@link #add} on the same builder.
   */
  static final class Builder {
    private final LinkedHashMap<String, AggregationResultBuilder> buildersByName =
        new LinkedHashMap<>();

    void add(AggregationResults other) {
      if (this.buildersByName.isEmpty()) {
        for (AggregationResult result : other) {
          this.buildersByName.put(result.getName(), result.toBuilder());
        }
      }
      Map<String, AggregationResult> otherByName = other.getResultsByName();
      if (!this.buildersByName.keySet().equals(otherByName.keySet())) {
        throw new IllegalArgumentException(
            String.format(
                "Cannot merge aggregation results with different result names: %s and %s",
                this.buildersByName.keySet(), otherByName.keySet()));
      }
      this.buildersByName.forEach((name, builder) -> builder.add(otherByName.get(name)));
    }

    AggregationResults build() {
      return AggregationResults.from(
          this.buildersByName.values().stream()
              .map(AggregationResultBuilder::build)
              .collect(Collectors.toList()));
    }
  }
}
