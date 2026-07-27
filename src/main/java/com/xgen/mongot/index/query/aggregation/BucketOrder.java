package com.xgen.mongot.index.query.aggregation;

/**
 * Presentation order for multi-bucket aggregation results, applied once by {@link
 * MultiBucketAggregationResult#getBuckets(BucketOrder)} when buckets are read out.
 *
 * <p>Merging itself is hash-based and order-free; the order is applied only at read time, so the
 * sharded intermediate path and the non-sharded output path can each pick their own order without
 * any sorting during the merge.
 */
public enum BucketOrder {
  /**
   * Ascending key order — matches the SSDV term (ordinal) order produced by a single un-partitioned
   * reader.
   */
  KEY_ASC,

  /**
   * Descending doc count, ascending key as tiebreak — facet-style presentation, suited for applying
   * a top-N bucket limit.
   */
  COUNT_DESC
}
