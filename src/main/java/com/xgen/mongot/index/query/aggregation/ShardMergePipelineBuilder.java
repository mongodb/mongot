package com.xgen.mongot.index.query.aggregation;

import com.xgen.mongot.util.Check;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonNull;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.jetbrains.annotations.Nullable;

/**
 * Collects contributions from each AggregationSpec node and assembles the 3-stage merge pipeline
 * ($group, $facet, $replaceWith).
 *
 * <p>Usage:
 *
 * <pre>{@code
 * builder.beginPath("byDirector");
 * directorSpec.buildShardMergePipeline(builder);  // recursive
 * builder.endPath();
 * List<BsonDocument> pipeline = builder.build("lowerBound");
 * }</pre>
 *
 * <p>Stages:
 *
 * <ul>
 *   <li>Stage 1 ({@code $group}) — merge shard rows that share the same routing metadata and
 *       grouping keys; sum metric partials and count docs.
 *   <li>Stage 2 ({@code $facet}) — route grouped rows into one arm per top-level branch (e.g.
 *       {@code by_city}) plus a {@code count} arm.
 *   <li>Stage 3 ({@code $replaceWith}) — project each facet arm into {@code buckets} with grouping
 *       key values and finalized metrics.
 * </ul>
 *
 * <p>Example — nested {@code by_city -> by_color -> sum_price}. Intermediate BSON is already flat
 * (one row per leaf bucket path); this builder merges and projects those rows. Nested output field
 * names like {@code by_color} are materialized later from the {@code AggregationSpec} tree.
 *
 * <pre>{@code
 * // Query tree (conceptual):
 * //   by_city  (path: city)
 * //     └── by_color  (path: color)
 * //           └── sum_price
 *
 * // --- shard intermediate rows (merge-pipeline input) ---
 * { $meta/group_name: "by_city", $meta/keys: {city: "NYC", color: "red"},  sum_sum_price: 30 }
 * { $meta/group_name: "by_city", $meta/keys: {city: "NYC", color: "blue"}, sum_sum_price: 20 }
 * { $meta/group_name: "by_city", $meta/keys: {city: "LA",  color: "red"},  sum_sum_price: 5  }
 *
 * // --- after Stage 1: $group ---
 * // _id reads $meta/group_name and $meta/keys via $getField; metrics are summed per (_id) key.
 * {
 *   _id: {
 *     $meta/group_name: "by_city",
 *     $meta/keys:       {city: "NYC", color: "red"}
 *   },
 *   sum_sum_price: 30,
 *   count_value:   1
 * }
 * // ... one grouped document per distinct (group_name, keys) pair ...
 *
 * // --- after Stage 2: $facet ---
 * // Each top-level branch arm $match-es on _id.$meta/group_name; count arm $match-es null routing.
 * {
 *   by_city: [
 *     {_id: {$meta/group_name: "by_city", $meta/keys: {city: "NYC", color: "red"}},  ...},
 *     {_id: {$meta/group_name: "by_city", $meta/keys: {city: "NYC", color: "blue"}}, ...},
 *     {_id: {$meta/group_name: "by_city", $meta/keys: {city: "LA",  color: "red"}},  ...}
 *   ],
 *   count: [ ... ]
 * }
 *
 * // --- after Stage 3: $replaceWith ---
 * // $map projects _id.$meta/keys and merged metric fields into flat bucket documents.
 * {
 *   count: {lowerBound: 3},
 *   facet: {
 *     by_city: {
 *       buckets: [
 *         {city: "NYC", color: "red",  sum_price: 30},
 *         {city: "NYC", color: "blue", sum_price: 20},
 *         {city: "LA",  color: "red",  sum_price: 5 }
 *       ]
 *     }
 *   }
 * }
 * }</pre>
 */
public final class ShardMergePipelineBuilder {

  // Stage 1: $group — metric field accumulators from leaf specs
  private final BsonDocument metricAccumulators = new BsonDocument();

  // Stage 2: $facet — per-path sub-pipelines from top-level specs
  private final BsonDocument facetPipelines = new BsonDocument();

  // Stage 3: $replaceWith — per-path $map "in" fields from all specs along path
  private final Map<String, BsonDocument> projectionFields = new LinkedHashMap<>();

  @Nullable private String currentGroupName;

  /**
   * Called by the coordinator before traversing each top-level spec. Registers the group name for
   * Stage 2 $facet sub-pipeline ($match on {@link IntermediateAggregationMetaField#GROUP_NAME}).
   */
  public void beginPath(String groupName) {
    this.currentGroupName = groupName;
    this.facetPipelines.append(
        groupName,
        new BsonArray(
            List.of(
                new BsonDocument(
                    "$match",
                    new BsonDocument(
                        "_id",
                        new BsonDocument(
                            IntermediateAggregationMetaField.GROUP_NAME,
                            new BsonString(groupName)))))));
  }

  /** Called by the coordinator after traversing each top-level spec. */
  @SuppressWarnings("NullAway")
  public void endPath() {
    this.currentGroupName = null;
  }

  /** Called by leaf metric specs — adds accumulator to Stage 1 $group. */
  public void addMetricAccumulator(String field, BsonDocument expr) {
    this.metricAccumulators.append(field, expr);
  }

  /**
   * Called by leaf specs that need post-$group computation (e.g. HLL, TDigest). Appends a stage to
   * the current path's $facet sub-pipeline, after $match. Simple metrics (avg, sum, min) never call
   * this — their sub-pipeline stays as just $match.
   */
  public void addFacetStage(BsonDocument stage) {
    Check.isNotNull(this.currentGroupName, "beginPath() must be called before addFacetStage()");
    this.facetPipelines.get(this.currentGroupName).asArray().add(stage);
  }

  /** Called by bucket specs — extracts a key from _id.keys into the final output. */
  public void addKeyProjection(String keyName) {
    this.projectionFields
        .computeIfAbsent(this.currentGroupName, k -> new BsonDocument())
        .append(
            keyName, new BsonString(IntermediateAggregationMetaField.groupedKeyFieldPath(keyName)));
  }

  /** Called by leaf metric specs — adds finalization expression to final output. */
  public void addMetricProjection(String field, BsonValue expr) {
    this.projectionFields
        .computeIfAbsent(this.currentGroupName, k -> new BsonDocument())
        .append(field, expr);
  }

  /**
   * Assemble the 3-stage pipeline ($group, $facet, $replaceWith).
   *
   * @param countType "lowerBound" or "total" — determines the count field name in the output
   */
  public List<BsonDocument> build(String countType) {
    // Stage1: $group
    BsonDocument groupId =
        new BsonDocument()
            .append(
                IntermediateAggregationMetaField.GROUP_NAME,
                IntermediateAggregationMetaField.rootFieldExpression(
                    IntermediateAggregationMetaField.GROUP_NAME))
            .append(
                IntermediateAggregationMetaField.KEYS,
                IntermediateAggregationMetaField.rootFieldExpression(
                    IntermediateAggregationMetaField.KEYS));
    BsonDocument groupBody =
        new BsonDocument()
            .append("_id", groupId)
            .append("count_value", new BsonDocument("$sum", new BsonString("$count")));
    // Append all metric accumulators from leaf specs.
    for (Map.Entry<String, BsonValue> entry : this.metricAccumulators.entrySet()) {
      groupBody.append(entry.getKey(), entry.getValue());
    }
    BsonDocument groupStage = new BsonDocument("$group", groupBody);

    // Stage2: $facet
    // Add "count" sub-pipeline: match count docs (they have null group_name)
    this.facetPipelines.append(
        "count",
        new BsonArray(
            List.of(
                new BsonDocument(
                    "$match",
                    new BsonDocument(
                        "_id",
                        new BsonDocument(
                            IntermediateAggregationMetaField.GROUP_NAME, new BsonNull()))))));
    BsonDocument facetStage = new BsonDocument("$facet", this.facetPipelines);

    // Stage3: $replaceWith
    BsonDocument replaceWithBody = new BsonDocument();
    replaceWithBody.append(
        "count",
        new BsonDocument(
            countType, new BsonDocument("$first", new BsonString("$count.count_value"))));
    BsonDocument facetBody = new BsonDocument();
    for (Map.Entry<String, BsonDocument> entry : this.projectionFields.entrySet()) {
      String groupName = entry.getKey();
      BsonDocument inFields = entry.getValue();

      BsonDocument mapExpr =
          new BsonDocument()
              .append("input", new BsonString("$" + groupName))
              .append("as", new BsonString("b"))
              .append("in", inFields);
      facetBody.append(groupName, new BsonDocument("buckets", new BsonDocument("$map", mapExpr)));
    }
    replaceWithBody.append("facet", facetBody);
    BsonDocument replaceWithStage = new BsonDocument("$replaceWith", replaceWithBody);

    return List.of(groupStage, facetStage, replaceWithStage);
  }
}
