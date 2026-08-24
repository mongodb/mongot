package com.xgen.mongot.index.query.aggregation;

import com.xgen.mongot.index.lucene.query.pushdown.MqlComparator;
import java.util.Comparator;
import org.bson.BsonValue;

/**
 * How two {@code min} / {@code max} values are ordered when partial results are merged.
 *
 * <p>Ordering must match BSON total order, including null below numbers. The sharded path merges
 * with mongod {@code $min}/{@code $max}, which ignore BSON null, so a real null is sent as MinKey
 * (see {@link MinMaxShardMerge}) and this comparator only ever sees decoded values -- including
 * actual null -- from partition merge. Any divergence here makes a standalone result differ from a
 * sharded one over the same documents.
 *
 * <p>Collection never comes through here: {@link MinMaxAccumulator} compares decoded {@link
 * BsonValue}s with {@link com.xgen.mongot.index.lucene.query.sort.mixed.SortUtil}. This class is
 * used only by the {@link MinAggregationResult} / {@link MaxAggregationResult} builders, which fold
 * already-decoded values across partitions and shards. Those values can be any sortable BSON type,
 * so {@link MqlComparator#LEXICOGRAPHIC_COMPARATOR} is doing the full BSON total ordering --
 * including cross-bracket pairs such as an {@code int64} against a string.
 *
 * <p>{@link com.xgen.mongot.index.lucene.query.sort.mixed.SortUtil} and {@link MqlComparator} agree
 * on everything these columns can produce except large-magnitude {@code int64} vs {@code double}
 * (above 2^53), where {@code SortUtil} promotes the int64 to double. Collection follows the sort
 * path; merge follows MQL. That residual is pinned by test, not closed here.
 *
 * <p>Eligibility is not decided here. Missing values never reach the accumulator ({@code
 * advanceExact} skips them). A bucket that saw no value carries an empty {@code Optional} instead,
 * and omits its intermediate partial so the cross-shard {@code $min} skips it. A bucket whose
 * extremum is BSON null still emits a partial, encoded as MinKey.
 */
final class MinMaxSelection {

  private static final Comparator<BsonValue> COMPARATOR = MqlComparator.LEXICOGRAPHIC_COMPARATOR;

  private MinMaxSelection() {}

  /** Whether {@code candidate} is strictly smaller than {@code current} in BSON order. */
  static boolean isSmaller(BsonValue candidate, BsonValue current) {
    return COMPARATOR.compare(candidate, current) < 0;
  }

  /** Whether {@code candidate} is strictly larger than {@code current} in BSON order. */
  static boolean isLarger(BsonValue candidate, BsonValue current) {
    return COMPARATOR.compare(candidate, current) > 0;
  }
}
