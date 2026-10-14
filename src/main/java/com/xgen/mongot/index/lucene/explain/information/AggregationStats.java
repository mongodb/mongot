package com.xgen.mongot.index.lucene.explain.information;

import com.xgen.mongot.index.lucene.explain.timing.QueryExecutionArea;
import com.xgen.mongot.util.Equality;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import java.util.Objects;
import java.util.Optional;
import org.apache.commons.collections4.Equator;
import org.bson.BsonDocument;

/**
 * Phase timings and the per-node tree for one search aggregation.
 *
 * <p>Either phase may be absent — a single-partition query never merges, and an aggregation that
 * failed before collecting has neither. {@code tree} and {@code query} are present when the query
 * was explained and collection finished far enough to materialize the accumulator tree.
 *
 * @param collect the whole collector fan-out for one partition: matching documents, walking the
 *     aggregation tree once per match to update accumulators, and the per-slice reduce that
 *     materializes each slice's buckets.
 * @param accumulatorCollect time spent inside the accumulators, excluding matching and the slice
 *     reduce. Summed over every matched document across every search slice, so this is thread time,
 *     not wall time: with concurrent slices it can exceed {@code collect}, and the two never sum to
 *     anything meaningful. Compare it against {@code collect} times the slice count, or use it to
 *     rank aggregations against each other rather than as a share of one query's latency. Costs a
 *     clock read per matched document, so it also inflates the explained latency it is reported
 *     alongside.
 * @param partitionMerge folding the per-partition result trees into one, for a partitioned index:
 *     every {@code AggregationResultBuilder.add} plus the terminal {@code build()}. One timing for
 *     the whole {@code merge}, not per partition. Excludes the per-partition collection above and
 *     the cross-shard merge, which happens on mongod after mongot returns.
 * @param tree the accumulator tree, nested as the query nests its facets, with per-node counters
 *     that are deliberately not rolled up into the parent.
 * @param query the flat query-level header alongside the tree: partition count, tree depth, type
 *     histogram, and the observed-bucket and observed-heap totals the process metrics also record.
 */
public record AggregationStats(
    Optional<QueryExecutionArea> collect,
    Optional<QueryExecutionArea> accumulatorCollect,
    Optional<QueryExecutionArea> partitionMerge,
    Optional<AggregationNodeStatsSpec> tree,
    Optional<AggregationQueryStats> query)
    implements DocumentEncodable, EqualsWithTimingEquator<AggregationStats> {

  static class Fields {
    static final Field.Optional<QueryExecutionArea> COLLECT =
        Field.builder("collect")
            .classField(QueryExecutionArea::fromBson)
            .disallowUnknownFields()
            .optional()
            .noDefault();

    static final Field.Optional<QueryExecutionArea> ACCUMULATOR_COLLECT =
        Field.builder("accumulatorCollect")
            .classField(QueryExecutionArea::fromBson)
            .disallowUnknownFields()
            .optional()
            .noDefault();

    static final Field.Optional<QueryExecutionArea> PARTITION_MERGE =
        Field.builder("partitionMerge")
            .classField(QueryExecutionArea::fromBson)
            .disallowUnknownFields()
            .optional()
            .noDefault();

    static final Field.Optional<AggregationNodeStatsSpec> TREE =
        Field.builder("tree")
            .classField(AggregationNodeStatsSpec::fromBson)
            .allowUnknownFields()
            .optional()
            .noDefault();

    static final Field.Optional<AggregationQueryStats> QUERY =
        Field.builder("query")
            .classField(AggregationQueryStats::fromBson)
            .allowUnknownFields()
            .optional()
            .noDefault();
  }

  /** Returns empty when nothing was recorded at all, so no empty section is emitted. */
  public static Optional<AggregationStats> create(
      Optional<QueryExecutionArea> collect,
      Optional<QueryExecutionArea> accumulatorCollect,
      Optional<QueryExecutionArea> partitionMerge,
      Optional<AggregationNodeStatsSpec> tree,
      Optional<AggregationQueryStats> query) {
    if (collect.isEmpty()
        && accumulatorCollect.isEmpty()
        && partitionMerge.isEmpty()
        && tree.isEmpty()
        && query.isEmpty()) {
      return Optional.empty();
    }
    return Optional.of(
        new AggregationStats(collect, accumulatorCollect, partitionMerge, tree, query));
  }

  public static AggregationStats fromBson(DocumentParser parser) throws BsonParseException {
    return new AggregationStats(
        parser.getField(Fields.COLLECT).unwrap(),
        parser.getField(Fields.ACCUMULATOR_COLLECT).unwrap(),
        parser.getField(Fields.PARTITION_MERGE).unwrap(),
        parser.getField(Fields.TREE).unwrap(),
        parser.getField(Fields.QUERY).unwrap());
  }

  /**
   * Timing areas compare under the test equator that ignores durations. {@code tree} and {@code
   * query} compare strictly: both empty, or both present and equal. Fixtures that omit {@code
   * cursorId} still match a runtime value. Fixtures must include {@code observedHeapBytes}; the
   * runtime value must be present and greater than zero. See {@link
   * AggregationQueryStats#equalsIgnoringAbsentCursorId}.
   */
  @Override
  public boolean equals(AggregationStats other, Equator<QueryExecutionArea> timingEquator) {
    return Equality.equals(this.collect, other.collect, timingEquator)
        && Equality.equals(this.accumulatorCollect, other.accumulatorCollect, timingEquator)
        && Equality.equals(this.partitionMerge, other.partitionMerge, timingEquator)
        && Objects.equals(this.tree, other.tree)
        && queryEquals(this.query, other.query);
  }

  private static boolean queryEquals(
      Optional<AggregationQueryStats> expected, Optional<AggregationQueryStats> actual) {
    if (expected.isEmpty() && actual.isEmpty()) {
      return true;
    }
    if (expected.isEmpty() || actual.isEmpty()) {
      return false;
    }
    return expected.get().equalsIgnoringAbsentCursorId(actual.get());
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder()
        .field(Fields.COLLECT, this.collect)
        .field(Fields.ACCUMULATOR_COLLECT, this.accumulatorCollect)
        .field(Fields.PARTITION_MERGE, this.partitionMerge)
        .field(Fields.TREE, this.tree)
        .field(Fields.QUERY, this.query)
        .build();
  }
}
