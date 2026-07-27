package com.xgen.mongot.index.query.aggregation;

import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonValue;

/** The interface for the result of any aggregation query. */
public interface AggregationResult {

  /**
   * User specified name for the result.
   *
   * <p>e.g. { group: { "byColor": { _id: "color", $sum: "$qty" } }} the name for the result will be
   * "byColor"
   */
  String getName();

  /**
   * Create an empty builder for merging partition/segment results of this result's shape (type,
   * name, path). This result's data is NOT included — {@link AggregationResultBuilder#add} every
   * result, including this one, then materialize the combined result once with {@link
   * AggregationResultBuilder#build}. Results are immutable; all merge state lives in the builder.
   */
  AggregationResultBuilder toBuilder();

  /**
   * Apply the per-node top-N ({@code numBuckets}) truncation to this result and its descendants,
   * returning the finalized result. Copy-on-write: {@code this} is never mutated; callers must use
   * the returned value. Each truncating node carries its own limit (nested facet levels can
   * differ); containers and leaf metrics simply recurse or return {@code this}.
   *
   * <p>Call exactly once, at the outermost merge point where bucket counts are global: {@link
   * AggregationResultBuilder} keeps full cardinality until then, because top-N-by-count is not
   * associative across merges. Concretely, this must only be called on a fully-merged, non-sharded
   * / non-partitioned (standalone) result -- never in sharded mode on a per-shard or per-partition
   * result.
   */
  AggregationResult applyLimit();

  /** Result for non-intermediate query path. */
  BsonValue toStandaloneBson();

  /**
   * Flat intermediate wire rows for sharded execution.
   *
   * <p>Group and top-level results return one mergeable document per leaf bucket path. Leaf metric
   * results return an empty array and contribute fields via {@link
   * #toIntermediateMetricPartials()}.
   *
   * <p>Top-level aggregation branches stamp {@link IntermediateAggregationMetaField#GROUP_NAME} on
   * each row in {@link TopLevelAggregationResult}.
   */
  BsonArray toIntermediateBson();

  /**
   * Mergeable metric partial fields for a single bucket row.
   *
   * <p>Only leaf metric results populate this; grouping results return an empty document.
   */
  default BsonDocument toIntermediateMetricPartials() {
    return new BsonDocument();
  }
}
