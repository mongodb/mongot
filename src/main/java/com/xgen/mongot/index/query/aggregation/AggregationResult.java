package com.xgen.mongot.index.query.aggregation;

/**
 * The interface for the result of any aggregation query.
 */
public interface AggregationResult {

  /**
   * User specified name for the result.
   *
   * <p>e.g. { group: { "byColor": { _id: "color", $sum: "$qty" } }} the
   * name for the result will be "byColor"
   */
  String getName();

  /**
   * Merge this result with another partition's result, returning the combined result.
   * This is used for merging results from different partitions/ segments.
   */
  AggregationResult mergePartitionResult(AggregationResult other);
}
