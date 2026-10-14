package com.xgen.mongot.index.lucene.explain.information;

import com.google.common.base.CaseFormat;
import com.xgen.mongot.index.query.aggregation.AggregationNodeStats;
import com.xgen.mongot.index.query.aggregation.AggregationNodeStats.NodeType;
import com.xgen.mongot.util.Enums;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import com.xgen.mongot.util.bson.parser.Value;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import org.bson.BsonDocument;

/**
 * The flat, query-level half of the aggregation explain section, alongside the per-node tree.
 *
 * <p>These are deliberately <em>not</em> per node. Partition count is a property of the index;
 * {@code cursorId} belongs to the query as a whole; and the observed totals are what the process
 * metrics record, repeated here so a single explain output is self-contained without a dashboard.
 *
 * @param cursorId the id this query's outputs are correlated by -- explain, log lines and kill
 *     errors all carry it, including across getMore batches, which record under the same id. It is
 *     never a metric tag: one series per query id is unbounded cardinality. Absent until the cursor
 *     exists.
 * @param partitionCount index partitions searched; absent when the explain state reports none.
 * @param treeDepth levels in the accumulator tree, counting the synthetic root.
 * @param nodeCountsByType nodes of each declared {@link AggregationNodeStats.NodeType}. BSON map
 *     keys are lower-camel ({@code terms}, {@code boundary}, {@code metric}). The synthetic root is
 *     undeclared and omitted.
 * @param observedBuckets grouping buckets instantiated in this tree, summed over nodes and search
 *     slices -- the same total the process histogram records.
 * @param observedHeapBytes accumulator heap charged during this search, in bytes -- the same total
 *     the process histogram records. Absent until collection finishes far enough to record it.
 *     Integration fixtures include it as a presence marker; fixture comparison requires the runtime
 *     field to be present and greater than zero rather than matching the exact HPPC footprint.
 */
public record AggregationQueryStats(
    Optional<Long> cursorId,
    Optional<Integer> partitionCount,
    int treeDepth,
    Map<NodeType, Long> nodeCountsByType,
    long observedBuckets,
    Optional<Long> observedHeapBytes)
    implements DocumentEncodable {

  static class Fields {
    static final Field.Optional<Long> CURSOR_ID =
        Field.builder("cursorId").longField().optional().noDefault();

    static final Field.Optional<Integer> PARTITION_COUNT =
        Field.builder("partitionCount").intField().optional().noDefault();

    static final Field.Required<Integer> TREE_DEPTH =
        Field.builder("treeDepth").intField().required();

    static final Field.Required<Map<String, Long>> NODE_COUNTS_BY_TYPE =
        Field.builder("nodeCountsByType").mapOf(Value.builder().longValue().required()).required();

    static final Field.Required<Long> OBSERVED_BUCKETS =
        Field.builder("observedBuckets").longField().required();

    static final Field.Optional<Long> OBSERVED_HEAP_BYTES =
        Field.builder("observedHeapBytes").longField().optional().noDefault();
  }

  /** Returns a copy carrying {@code cursorId}, which is only known once the cursor exists. */
  public AggregationQueryStats withCursorId(Optional<Long> cursorId) {
    return new AggregationQueryStats(
        cursorId,
        this.partitionCount,
        this.treeDepth,
        this.nodeCountsByType,
        this.observedBuckets,
        this.observedHeapBytes);
  }

  /**
   * Returns a copy carrying {@code observedHeapBytes}, recorded after collection from the
   * partition's heap accountant.
   */
  public AggregationQueryStats withObservedHeapBytes(Optional<Long> observedHeapBytes) {
    return new AggregationQueryStats(
        this.cursorId,
        this.partitionCount,
        this.treeDepth,
        this.nodeCountsByType,
        this.observedBuckets,
        observedHeapBytes);
  }

  /**
   * Fixture comparison: {@code cursorId} is omitted from expected fixtures because it is
   * non-deterministic, so an expected value that omits it still matches an actual that includes
   * one. {@code observedHeapBytes} is present in fixtures as a presence marker (the exact HPPC
   * footprint is JVM-dependent and is not compared). The actual value must be present and greater
   * than zero. Every other field compares strictly.
   */
  boolean equalsIgnoringAbsentCursorId(AggregationQueryStats other) {
    boolean cursorOk = this.cursorId.isEmpty() || Objects.equals(this.cursorId, other.cursorId);
    boolean heapOk =
        this.observedHeapBytes.isPresent()
            && other.observedHeapBytes.filter(bytes -> bytes > 0).isPresent();
    return cursorOk
        && heapOk
        && Objects.equals(this.partitionCount, other.partitionCount)
        && this.treeDepth == other.treeDepth
        && Objects.equals(this.nodeCountsByType, other.nodeCountsByType)
        && this.observedBuckets == other.observedBuckets;
  }

  public static AggregationQueryStats fromBson(DocumentParser parser) throws BsonParseException {
    return new AggregationQueryStats(
        parser.getField(Fields.CURSOR_ID).unwrap(),
        parser.getField(Fields.PARTITION_COUNT).unwrap(),
        parser.getField(Fields.TREE_DEPTH).unwrap(),
        decodeNodeCounts(parser.getField(Fields.NODE_COUNTS_BY_TYPE).unwrap()),
        parser.getField(Fields.OBSERVED_BUCKETS).unwrap(),
        parser.getField(Fields.OBSERVED_HEAP_BYTES).unwrap());
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder()
        .field(Fields.CURSOR_ID, this.cursorId)
        .field(Fields.PARTITION_COUNT, this.partitionCount)
        .field(Fields.TREE_DEPTH, this.treeDepth)
        .field(Fields.NODE_COUNTS_BY_TYPE, encodeNodeCounts(this.nodeCountsByType))
        .field(Fields.OBSERVED_BUCKETS, this.observedBuckets)
        .field(Fields.OBSERVED_HEAP_BYTES, this.observedHeapBytes)
        .build();
  }

  private static Map<String, Long> encodeNodeCounts(Map<NodeType, Long> counts) {
    Map<String, Long> encoded = new LinkedHashMap<>();
    for (var entry : counts.entrySet()) {
      encoded.put(Enums.convertNameTo(CaseFormat.LOWER_CAMEL, entry.getKey()), entry.getValue());
    }
    return encoded;
  }

  private static Map<NodeType, Long> decodeNodeCounts(Map<String, Long> encoded) {
    Map<NodeType, Long> counts = new EnumMap<>(NodeType.class);
    for (var entry : encoded.entrySet()) {
      counts.put(
          NodeType.valueOf(CaseFormat.LOWER_CAMEL.to(CaseFormat.UPPER_UNDERSCORE, entry.getKey())),
          entry.getValue());
    }
    return counts;
  }
}
