package com.xgen.mongot.index.query.aggregation;

import com.xgen.mongot.index.lucene.query.pushdown.MqlComparator;
import java.util.Comparator;
import org.bson.BsonValue;

/**
 * How two {@code min} / {@code max} values are ordered when partial results are merged.
 *
 * <p>Ordering must match the MQL {@code $min} / {@code $max} accumulators, because the sharded path
 * merges each shard's partial with those very accumulators (see {@code buildShardMergePipeline}).
 * Any divergence here makes a standalone result differ from a sharded one over the same documents.
 *
 * <p>Collection never comes through here: {@link MinMaxAccumulator} compares encoded {@code long}
 * doc values directly. This class is used only by the {@link MinAggregationResult} / {@link
 * MaxAggregationResult} builders, which fold already-decoded values across partitions and shards.
 * Those values are always numeric, so {@link MqlComparator#LEXICOGRAPHIC_COMPARATOR} is doing
 * cross-numeric-type ordering here -- an {@code int64} against a {@code double} -- rather than the
 * BSON canonical-type ordering it also implements.
 *
 * <p>Eligibility is not decided here. Missing values never reach the accumulator ({@code
 * advanceExact} skips them) and doc values cannot hold {@code null} or {@code undefined}, so there
 * is nothing to filter out. A bucket that saw no value carries an empty {@code Optional} instead,
 * and omits its intermediate partial so the cross-shard {@code $min} skips it.
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
