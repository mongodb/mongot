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
