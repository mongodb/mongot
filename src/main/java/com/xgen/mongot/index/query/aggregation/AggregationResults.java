package com.xgen.mongot.index.query.aggregation;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Container of {@link AggregationResult}s in the producing accumulator's child order.
 *
 * <p>This class along with {@link MultiBucketAggregationResult} forms a tree structure that
 * represents the aggregation result.
 */
public final class AggregationResults implements Iterable<AggregationResult> {
  // Child order of the accumulator that produced these results. Merges match by position (see
  // Builder). Name lookup scans this list; a node has a handful of children, and production reads
  // iterate.
  private final List<AggregationResult> results;

  private AggregationResults(List<AggregationResult> results) {
    checkDistinctNames(results);
    this.results = results;
  }

  // Runs once per bucket. A node has a handful of children, and the common single-child case
  // compares nothing.
  private static void checkDistinctNames(List<AggregationResult> results) {
    for (int i = 1; i < results.size(); i++) {
      String name = results.get(i).getName();
      for (int j = 0; j < i; j++) {
        if (name.equals(results.get(j).getName())) {
          throw new IllegalStateException("Duplicate result name: " + name);
        }
      }
    }
  }

  @Override
  public Iterator<AggregationResult> iterator() {
    return this.results.iterator();
  }

  /**
   * Returns the result with {@code name}.
   *
   * <p>Scans the child list. A node has a handful of children, and production reads iterate.
   */
  @SuppressWarnings("unchecked")
  public <A extends AggregationResult> A get(String name) {
    for (AggregationResult result : this.results) {
      if (result.getName().equals(name)) {
        return (A) result;
      }
    }
    throw new IllegalStateException("Result not found: " + name);
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
   * Merge state for an {@link AggregationResults}: one {@link AggregationResultBuilder} per child,
   * matched by position. Producers emit children in a fixed order; the name check fails the merge
   * if that order is broken.
   *
   * <p>Starts as the merge identity. The first non-empty {@link #add} lays out builders and then
   * folds like any later add, so an empty add leaves the identity in place. Subsequent adds must
   * carry the same names in the same positions.
   *
   * <p>Not thread-safe: {@link #add} must be called from a single thread. Behavior is undefined if
   * concurrent threads call {@link #add} on the same builder.
   */
  static final class Builder {
    private record Child(String name, AggregationResultBuilder resultBuilder) {}

    // Empty until the first non-empty add, which fixes child order. An empty add is a no-op.
    private final List<Child> children = new ArrayList<>();

    void add(AggregationResults other) {
      List<AggregationResult> incoming = other.results;
      if (this.children.isEmpty()) {
        for (AggregationResult result : incoming) {
          this.children.add(new Child(result.getName(), result.toBuilder()));
        }
      } else if (!sameNames(incoming)) {
        throw differentNames(incoming);
      }
      for (int i = 0; i < incoming.size(); i++) {
        this.children.get(i).resultBuilder().add(incoming.get(i));
      }
    }

    private boolean sameNames(List<AggregationResult> incoming) {
      if (incoming.size() != this.children.size()) {
        return false;
      }
      for (int i = 0; i < this.children.size(); i++) {
        if (!this.children.get(i).name().equals(incoming.get(i).getName())) {
          return false;
        }
      }
      return true;
    }

    private IllegalArgumentException differentNames(List<AggregationResult> incoming) {
      return new IllegalArgumentException(
          String.format(
              "Cannot merge aggregation results with different result names: %s and %s",
              this.children.stream().map(Child::name).toList(),
              incoming.stream().map(AggregationResult::getName).toList()));
    }

    AggregationResults build() {
      List<AggregationResult> built = new ArrayList<>(this.children.size());
      for (Child child : this.children) {
        built.add(child.resultBuilder().build());
      }
      return AggregationResults.from(built);
    }
  }
}
