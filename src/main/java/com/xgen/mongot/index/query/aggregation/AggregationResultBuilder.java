package com.xgen.mongot.index.query.aggregation;

/**
 * Mutable accumulation state for merging {@link AggregationResult}s across partitions, segments or
 * search slices, so results themselves stay immutable: obtain an empty builder from any result of
 * the target shape with {@link AggregationResult#toBuilder()}, {@link #add} every result (including
 * that one), then {@link #build} exactly once.
 *
 * <p>Builders keep full bucket cardinality: top-N ({@code numBuckets}) is not associative across
 * merges and is applied exactly once on the built result via {@link AggregationResult#applyLimit}.
 * Instances are single-use and not thread-safe.
 */
public interface AggregationResultBuilder {

  /**
   * Fold a result into this builder. The result must come from the same aggregation spec (same
   * type, name and shape) as the result this builder was obtained from.
   */
  void add(AggregationResult other);

  /**
   * Materialize the merged, immutable result. Call exactly once, after at least one {@link #add}.
   */
  AggregationResult build();
}
