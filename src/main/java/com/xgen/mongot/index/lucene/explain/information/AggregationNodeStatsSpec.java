package com.xgen.mongot.index.lucene.explain.information;

import com.xgen.mongot.index.query.aggregation.AggregationNodeStats;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import java.util.List;
import java.util.Optional;
import org.bson.BsonDocument;

/**
 * One node of the aggregation explain tree, nested exactly as the query nests its facets.
 *
 * <p>The counters are per node and are deliberately <em>not</em> rolled up into the parent: the
 * useful comparison is between siblings and between levels, and a rolled-up parent hides which
 * child dominated. To read it, walk down and find the level where {@code observedBuckets} jumps
 * away from {@code requestedBuckets}.
 *
 * <p>Two reading notes that will otherwise look like bugs:
 *
 * <ul>
 *   <li>{@code collectInvocations} counts leaf-collector calls, not documents: a document fanned
 *       into several buckets by a multi-valued group field is counted once per bucket. Comparing a
 *       parent's count to a child's is what shows where the fan-out happened.
 *   <li>{@code observedBuckets} counts buckets <em>instantiated</em>, summed over search slices, so
 *       on a multi-slice search it exceeds the number of distinct buckets returned.
 * </ul>
 *
 * @param name the user-facing aggregation name; {@code _top} for the synthetic root.
 * @param type {@link AggregationNodeStats.NodeType}; absent on the synthetic root. Encoded as
 *     lower-camel {@code terms}, {@code boundary}, or {@code metric}.
 * @param path the user-requested field path this node grouped or measured; absent when the node has
 *     none (count, or the synthetic root). Sum and avg emit the Lucene field name instead.
 * @param requestedBuckets {@code numBuckets} for terms, or the boundary count; absent on metric
 *     leaves.
 * @param observedBuckets buckets this node instantiated, summed over search slices.
 * @param collectInvocations leaf-collector calls on this node.
 * @param children nested sub-aggregations, omitted when empty.
 */
public record AggregationNodeStatsSpec(
    String name,
    Optional<AggregationNodeStats.NodeType> type,
    Optional<String> path,
    Optional<Integer> requestedBuckets,
    long observedBuckets,
    long collectInvocations,
    Optional<List<AggregationNodeStatsSpec>> children)
    implements DocumentEncodable {

  static class Fields {
    static final Field.Required<String> NAME = Field.builder("name").stringField().required();

    static final Field.Optional<AggregationNodeStats.NodeType> TYPE =
        Field.builder("type")
            .enumField(AggregationNodeStats.NodeType.class)
            .asCamelCase()
            .optional()
            .noDefault();

    static final Field.Optional<String> PATH =
        Field.builder("path").stringField().optional().noDefault();

    static final Field.Optional<Integer> REQUESTED_BUCKETS =
        Field.builder("requestedBuckets").intField().optional().noDefault();

    static final Field.Required<Long> OBSERVED_BUCKETS =
        Field.builder("observedBuckets").longField().required();

    static final Field.Required<Long> COLLECT_INVOCATIONS =
        Field.builder("collectInvocations").longField().required();

    static final Field.Optional<List<AggregationNodeStatsSpec>> CHILDREN =
        Field.builder("children")
            .classField(AggregationNodeStatsSpec::fromBson)
            .allowUnknownFields()
            .asList()
            .mustNotBeEmpty()
            .optional()
            .noDefault();
  }

  public static AggregationNodeStatsSpec fromBson(DocumentParser parser) throws BsonParseException {
    return new AggregationNodeStatsSpec(
        parser.getField(Fields.NAME).unwrap(),
        parser.getField(Fields.TYPE).unwrap(),
        parser.getField(Fields.PATH).unwrap(),
        parser.getField(Fields.REQUESTED_BUCKETS).unwrap(),
        parser.getField(Fields.OBSERVED_BUCKETS).unwrap(),
        parser.getField(Fields.COLLECT_INVOCATIONS).unwrap(),
        parser.getField(Fields.CHILDREN).unwrap());
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder()
        .field(Fields.NAME, this.name)
        .field(Fields.TYPE, this.type)
        .field(Fields.PATH, this.path)
        .field(Fields.REQUESTED_BUCKETS, this.requestedBuckets)
        .field(Fields.OBSERVED_BUCKETS, this.observedBuckets)
        .field(Fields.COLLECT_INVOCATIONS, this.collectInvocations)
        .field(Fields.CHILDREN, this.children)
        .build();
  }
}
