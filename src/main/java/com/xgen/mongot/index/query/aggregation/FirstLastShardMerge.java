package com.xgen.mongot.index.query.aggregation;

import java.util.List;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonNull;
import org.bson.BsonString;
import org.bson.BsonValue;

/**
 * The cross-shard contract for {@code first} and {@code last}: the partial fields a shard writes
 * onto a leaf bucket row, and the merge stages mongod reads them back with.
 *
 * <p>Both halves live here because neither means anything alone -- a renamed field or a flipped
 * direction has to change on both sides at once.
 *
 * <p>A shard emits two partials per metric: the winning value, and the winning document's sort key
 * as positional components ({@code _0}, {@code _1}, ...), the same convention {@code
 * $searchSortValues} uses for the ordinary result merge-sort. Merging then re-runs across shards
 * the comparison each shard's accumulator ran locally -- one shard's bucket winner against
 * another's -- as {@code $top}/{@code $bottom} over that key.
 *
 * <p>The key is always emitted the query's way round, for both operators: {@code last} collects
 * under an inverted sort, but that is a shard-local implementation detail, and inverting the key
 * too would invert it twice. Direction therefore lives in one place only: the accumulator picked
 * here.
 */
final class FirstLastShardMerge {

  private FirstLastShardMerge() {}

  /** The winning value of one bucket, as read from the winning document. */
  static String valueField(Operator operator, String name) {
    return operator.registryKey() + "_value_" + name;
  }

  /**
   * Whether this bucket had a winning document on this shard.
   *
   * <p>Needed because a bucket can legitimately be empty on one shard and not another -- boundary
   * grouping emits every bucket on every shard, so an empty range still reaches the wire with a doc
   * count and no winner. Unlike the additive and selective merges ({@code $sum}, {@code $min},
   * {@code $max}), which simply ignore a row that contributes nothing, {@code $top}/{@code $bottom}
   * rank every row they see: a missing sort key sorts lowest in BSON and would beat the shard that
   * actually holds the winner. Sorting on this flag first keeps winner-less rows last, whichever
   * way the key itself is ordered.
   */
  static String hasValueField(Operator operator, String name) {
    return operator.registryKey() + "_has_" + name;
  }

  /** The winning document's sort key; absent from the row when the query had no sort. */
  static String keyField(Operator operator, String name) {
    return operator.registryKey() + "_key_" + name;
  }

  /**
   * Positional name of one sort-key component, mirroring the {@code $searchSortValues._<i>} keys
   * mongod already merge-sorts ordinary sorted results by.
   */
  static String keyComponent(int index) {
    return "_" + index;
  }

  /**
   * Registers the merge accumulator and projection for one {@code first}/{@code last} metric.
   *
   * <p>{@code operator} chooses the direction -- {@code $top} for {@code first}, {@code $bottom}
   * for {@code last} -- over the query's own sort directions, which the builder carries because
   * they belong to the query rather than to any one aggregation node.
   */
  static void buildShardMergePipeline(
      ShardMergePipelineBuilder builder, Operator operator, String name) {
    String mergedField = operator.registryKey() + "_" + name;
    // When no shard had a winner the value field is absent from every row, so the accumulator
    // would yield a missing field where the standalone answer is null.
    BsonValue winningValue =
        new BsonDocument(
            "$ifNull",
            new BsonArray(
                List.of(new BsonString("$" + valueField(operator, name)), BsonNull.VALUE)));

    builder.addMetricAccumulator(
        mergedField, mergeAccumulator(operator, name, winningValue, builder.sortDirections()));
    builder.addMetricProjection(name, new BsonString("$$b." + mergedField));
  }

  private static BsonDocument mergeAccumulator(
      Operator operator, String name, BsonValue winningValue, List<Integer> sortDirections) {
    boolean isFirst = operator == Operator.FIRST;

    // Rows that hold a winner must sort ahead of the rest for $top and behind them for $bottom, so
    // the flag's direction follows the accumulator rather than the query.
    BsonDocument sortBy =
        new BsonDocument(hasValueField(operator, name), new BsonInt32(isFirst ? -1 : 1));
    for (int i = 0; i < sortDirections.size(); i++) {
      sortBy.append(
          keyField(operator, name) + "." + keyComponent(i), new BsonInt32(sortDirections.get(i)));
    }

    // With no sort the flag is the whole comparison: every document in a bucket is an equally good
    // answer, so the merge only has to pick one of the shards' winners over any winner-less row --
    // the cross-shard counterpart of the encounter order a single shard resolves the bucket by.
    return new BsonDocument(
        isFirst ? "$top" : "$bottom",
        new BsonDocument().append("output", winningValue).append("sortBy", sortBy));
  }
}
