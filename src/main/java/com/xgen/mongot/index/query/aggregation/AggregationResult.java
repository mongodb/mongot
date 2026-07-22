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
   * Merge this result with another partition's result, returning the combined result. This is used
   * for merging results from different partitions/ segments.
   */
  AggregationResult mergePartitionResult(AggregationResult other);

  /**
   * Apply the per-node top-N ({@code numBuckets}) truncation to this result and its descendants,
   * returning the finalized result.
   *
   * <p>Copy-on-write: implementations do not mutate {@code this}; they return a new, finalized
   * result (or {@code this} unchanged when there is nothing to truncate). Callers must use the
   * returned value.
   *
   * <p>The limit is intentionally not a parameter. It is stored only on the node that actually
   * truncates -- {@link TermsAggregationResult} carries its own {@code numBuckets} (nested facet
   * levels can differ) -- while container/pass-through results ({@link AggregationResults},
   * top-level and boundary results) hold no limit and simply recurse. A single caller-supplied
   * limit could not express the differing per-level top-Ns, and threading a per-node map would just
   * duplicate state the truncating node already owns.
   *
   * <p>Precondition: call exactly once, at the outermost merge point where bucket counts are
   * global. {@link #mergePartitionResult} keeps full cardinality until then, because top-N-by-count
   * is not associative across merges.
   *
   * <p>No default is provided on purpose: every implementer must consciously decide its top-N
   * behavior. Bucketed results truncate and/or recurse; leaf/metric results (which have no buckets)
   * return {@code this}.
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
