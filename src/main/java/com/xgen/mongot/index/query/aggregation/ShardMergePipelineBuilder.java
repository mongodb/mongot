package com.xgen.mongot.index.query.aggregation;

import com.google.errorprone.annotations.Var;
import com.xgen.mongot.util.Check;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonNull;
import org.bson.BsonString;
import org.bson.BsonValue;

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
 * // Each top-level branch arm $match-es on {@code $_id.$meta/group_name} via {@code $getField};
 * // count arm $match-es null routing the same way.
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

  /**
   * {@code $map} row variable for flat single-level facet projection (Stage 3). Leaf metric specs
   * should reference merged partials as {@code $$b.<field>} in {@link #addMetricProjection}; nested
   * reconstruction rewrites that prefix to inner row variables.
   */
  private static final String FLAT_BUCKET_ROW_VAR = "b";

  private static final String BUCKET_FIELD_PREFIX = "$$" + FLAT_BUCKET_ROW_VAR + ".";

  /** One grouping level in a top-level facet branch's merge/reconstruction chain. */
  record GroupMergeLevel(String keyPath, Optional<String> nestedBranchName) {}

  private static final class PathMergeConfig {
    /**
     * Grouping levels along one top-level facet branch, outermost first.
     *
     * <p>Mirrors the recursive {@code facets} chain in the query syntax for a single named branch
     * (e.g. {@code byGroupFacet -> capacityFacet -> avgPrice}). Each entry records the document
     * field path grouped at that level ({@link GroupMergeLevel#keyPath}) and, when a nested child
     * facet exists, the child branch name in the response tree ({@link
     * GroupMergeLevel#nestedBranchName}). The leaf level has an empty nested branch name.
     */
    final List<GroupMergeLevel> groupLevels = new ArrayList<>();

    final BsonDocument metricProjections = new BsonDocument();

    /**
     * Intermediate partial field names for leaf metrics on this path (e.g. {@code sum_sum_price},
     * {@code avg_sum_avgPrice}, {@code avg_count_avgPrice}).
     *
     * <p>One entry per {@link #addMetricAccumulator} call. A leaf grouping level can define
     * multiple operators in its {@code aggregations} block, and some operators (e.g. average) emit
     * more than one partial. Nested facet-arm {@code $group} stages copy these fields into pushed
     * rows so metric partials survive bucket reconstruction.
     */
    final List<String> accumulatorFields = new ArrayList<>();
  }

  // Stage 1: $group — metric field accumulators from leaf specs
  private final BsonDocument metricAccumulators = new BsonDocument();

  // Stage 2: $facet — per-path sub-pipelines from top-level specs
  private final BsonDocument facetPipelines = new BsonDocument();

  // Per top-level group_name: grouping keys and projection metadata
  private final Map<String, PathMergeConfig> pathConfigs = new LinkedHashMap<>();

  @Var private Optional<String> currentGroupName = Optional.empty();

  private String requireCurrentGroupName() {
    return this.currentGroupName.orElseThrow(
        () -> new IllegalStateException("beginPath() must be called first"));
  }

  /**
   * Called by the coordinator before traversing each top-level spec. Registers the group name for
   * Stage 2 $facet sub-pipeline ($match on {@link IntermediateAggregationMetaField#GROUP_NAME}).
   */
  public void beginPath(String groupName) {
    this.currentGroupName = Optional.of(groupName);
    this.pathConfigs.putIfAbsent(groupName, new PathMergeConfig());
    this.facetPipelines.append(
        groupName,
        new BsonArray(
            List.of(
                IntermediateAggregationMetaField.matchIdFieldStage(
                    IntermediateAggregationMetaField.GROUP_NAME, new BsonString(groupName)))));
  }

  /** Called by the coordinator after traversing each top-level spec. */
  public void endPath() {
    this.currentGroupName = Optional.empty();
  }

  /**
   * Records one grouping level along a nested group chain.
   *
   * @param keyPath document field path for this level (e.g. {@code city})
   * @param nestedBranchName child branch name in the output (e.g. {@code by_color}), or {@link
   *     Optional#empty()} at the leaf grouping level
   */
  public void addGroupLevel(String keyPath, Optional<String> nestedBranchName) {
    this.pathConfigs
        .get(requireCurrentGroupName())
        .groupLevels
        .add(new GroupMergeLevel(keyPath, nestedBranchName));
  }

  /** Called by leaf metric specs — adds accumulator to Stage 1 $group. */
  public void addMetricAccumulator(String field, BsonDocument expr) {
    String groupName = requireCurrentGroupName();
    this.metricAccumulators.append(field, expr);
    this.pathConfigs.get(groupName).accumulatorFields.add(field);
  }

  /** Called by leaf metric specs — adds finalization expression to final output. */
  public void addMetricProjection(String field, BsonValue expr) {
    this.pathConfigs.get(requireCurrentGroupName()).metricProjections.append(field, expr);
  }

  /**
   * Assemble the 3-stage pipeline ($group, $facet, $replaceWith).
   *
   * <p>Known gap: the pipeline emits no bucket ordering or numBuckets top-N. Shards stream full
   * cardinality (top-N-by-count is not associative, so mongot must not pre-truncate), which means
   * the limit and the count-desc presentation order must be applied here, after the {@code $group}
   * merge — like the legacy facet planner's {@code $sort {value: -1, _id: 1}} + {@code $limit
   * numBuckets} (see ShardedSearchPlanner#buildStringFacetBucket). Until those stages are added,
   * sharded aggregation results are unordered and unlimited; nested groupings additionally need
   * per-level top-N inside the reconstructed buckets, which a flat $sort/$limit cannot express.
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

    // Stage2: $facet — append reconstruction stages per path
    for (Map.Entry<String, PathMergeConfig> entry : this.pathConfigs.entrySet()) {
      String groupName = entry.getKey();
      PathMergeConfig config = entry.getValue();
      BsonArray arm = this.facetPipelines.get(groupName).asArray();

      int depth = config.groupLevels.size();
      if (depth >= 2) {
        for (int stage = 0; stage < depth - 1; stage++) {
          arm.add(buildNestedGroupStage(config, stage));
        }
        arm.add(buildNestedProjectStage(config));
      } else if (depth == 1) {
        Check.checkState(
            !config.metricProjections.isEmpty(),
            "single-level facet arm %s requires leaf metric projections",
            groupName);
        arm.add(buildFlatProjectStage(config));
      }
      // depth == 0: operator-only top-level branch; facet arm stays at $match only.
    }

    // Add "count" sub-pipeline: match count docs (they have null group_name)
    this.facetPipelines.append(
        "count",
        new BsonArray(
            List.of(
                IntermediateAggregationMetaField.matchIdFieldStage(
                    IntermediateAggregationMetaField.GROUP_NAME, new BsonNull()))));
    BsonDocument facetStage = new BsonDocument("$facet", this.facetPipelines);

    // Stage3: $replaceWith
    BsonDocument replaceWithBody = new BsonDocument();
    replaceWithBody.append(
        "count",
        new BsonDocument(
            countType, new BsonDocument("$first", new BsonString("$count.count_value"))));

    BsonDocument facetBody = new BsonDocument();
    for (Map.Entry<String, PathMergeConfig> entry : this.pathConfigs.entrySet()) {
      String groupName = entry.getKey();
      PathMergeConfig config = entry.getValue();

      if (usesDirectFacetReference(config)) {
        facetBody.append(groupName, new BsonDocument("buckets", new BsonString("$" + groupName)));
      } else if (!config.metricProjections.isEmpty()) {
        BsonDocument inFields = new BsonDocument();
        if (!config.groupLevels.isEmpty()) {
          inFields.append(
              "_id",
              new BsonString(
                  IntermediateAggregationMetaField.groupedKeyFieldPath(
                      config.groupLevels.get(0).keyPath())));
        }
        inFields.putAll(config.metricProjections);
        BsonDocument mapExpr =
            new BsonDocument()
                .append("input", new BsonString("$" + groupName))
                .append("as", new BsonString(FLAT_BUCKET_ROW_VAR))
                .append("in", inFields);
        facetBody.append(groupName, new BsonDocument("buckets", new BsonDocument("$map", mapExpr)));
      }
    }
    replaceWithBody.append("facet", facetBody);
    BsonDocument replaceWithStage = new BsonDocument("$replaceWith", replaceWithBody);

    return List.of(groupStage, facetStage, replaceWithStage);
  }

  private static boolean usesDirectFacetReference(PathMergeConfig config) {
    return config.groupLevels.size() >= 2
        || (config.groupLevels.size() == 1 && !config.metricProjections.isEmpty());
  }

  /**
   * Strategy B: one {@code $group} stage in the facet arm. Stage {@code stageIndex} is 0 for the
   * innermost grouping (longest key prefix) and increases toward the root.
   */
  private static BsonDocument buildNestedGroupStage(PathMergeConfig config, int stageIndex) {
    int depth = config.groupLevels.size();
    if (stageIndex == 0) {
      // Innermost facet-arm $group: bucket flat Stage-1 rows by all parent grouping keys
      // (indices 0..depth-2). The leaf grouping dimension (depth-1) is stored in each pushed
      // row, not in _id — there are depth-1 $group stages for depth grouping levels.
      BsonValue groupId = flatKeysIdExpr(config, 0, depth - 2);
      BsonDocument pushDoc =
          new BsonDocument()
              .append(config.groupLevels.get(depth - 1).keyPath(), flatKeysLeafExpr(config));
      for (String field : config.accumulatorFields) {
        pushDoc.append(field, new BsonString("$" + field));
      }
      return groupStage(groupId, pushDoc);
    }

    int prefixEnd = depth - 2 - stageIndex;
    BsonValue groupId = priorStageIdExpr(config, prefixEnd);
    String pushedKey = config.groupLevels.get(depth - 1 - stageIndex).keyPath();
    BsonDocument pushDoc =
        new BsonDocument()
            .append(pushedKey, new BsonString("$_id." + pushedKey))
            .append("_leaf_rows", new BsonString("$_rows"));
    return groupStage(groupId, pushDoc);
  }

  /** Strategy B: {@code $project} nested child branch buckets from grouped rows. */
  private static BsonDocument buildNestedProjectStage(PathMergeConfig config) {
    BsonValue bucketsExpr = buildBucketsAtLevel(config, 0, "$_rows", "row");
    return new BsonDocument(
        "$project",
        new BsonDocument()
            .append("_id", new BsonString("$_id"))
            .append(
                config.groupLevels.get(0).nestedBranchName().orElseThrow(),
                new BsonDocument("buckets", bucketsExpr)));
  }

  /**
   * Builds nested {@code $map} expressions from the innermost grouping level outward. {@code
   * levelIndex} 0 is the outermost nested branch.
   */
  private static BsonValue buildBucketsAtLevel(
      PathMergeConfig config, int levelIndex, String input, String rowVar) {
    int depth = config.groupLevels.size();
    if (levelIndex == depth - 2) {
      return mapExpr(input, rowVar, leafRowBody(config, rowVar, depth - 1));
    }

    String childBranch = config.groupLevels.get(levelIndex + 1).nestedBranchName().orElseThrow();
    String rowKey = config.groupLevels.get(levelIndex + 1).keyPath();
    BsonValue innerBuckets =
        buildBucketsAtLevel(config, levelIndex + 1, "$$" + rowVar + "._leaf_rows", "leaf");

    BsonDocument rowBody =
        new BsonDocument()
            .append("_id", new BsonString("$$" + rowVar + "." + rowKey))
            .append(childBranch, new BsonDocument("buckets", innerBuckets));
    return mapExpr(input, rowVar, rowBody);
  }

  /** Single-level branch: finalize metrics in the $facet arm. */
  private static BsonDocument buildFlatProjectStage(PathMergeConfig config) {
    BsonDocument projectBody =
        new BsonDocument()
            .append(
                "_id",
                new BsonString(
                    IntermediateAggregationMetaField.idKeysFieldPath(
                        config.groupLevels.get(0).keyPath())));
    for (Map.Entry<String, BsonValue> metric : config.metricProjections.entrySet()) {
      projectBody.append(metric.getKey(), scopeToRoot(metric.getValue()));
    }
    return new BsonDocument("$project", projectBody);
  }

  private static BsonDocument groupStage(BsonValue groupId, BsonDocument pushDoc) {
    return new BsonDocument(
        "$group",
        new BsonDocument()
            .append("_id", groupId)
            .append("_rows", new BsonDocument("$push", pushDoc)));
  }

  private static BsonDocument mapExpr(String input, String rowVar, BsonDocument rowBody) {
    return new BsonDocument(
        "$map",
        new BsonDocument()
            .append("input", new BsonString(input))
            .append("as", new BsonString(rowVar))
            .append("in", rowBody));
  }

  private static BsonDocument leafRowBody(PathMergeConfig config, String rowVar, int keyIndex) {
    BsonDocument rowBody =
        new BsonDocument()
            .append(
                "_id",
                new BsonString("$$" + rowVar + "." + config.groupLevels.get(keyIndex).keyPath()));
    for (Map.Entry<String, BsonValue> metric : config.metricProjections.entrySet()) {
      rowBody.append(metric.getKey(), scopeToVar(metric.getValue(), rowVar));
    }
    return rowBody;
  }

  /** {@code _id} for the innermost facet-arm {@code $group} over flat Stage-1 rows. */
  private static BsonValue flatKeysIdExpr(PathMergeConfig config, int start, int end) {
    if (start == end) {
      return new BsonString(
          IntermediateAggregationMetaField.idKeysFieldPath(
              config.groupLevels.get(start).keyPath()));
    }
    BsonDocument composite = new BsonDocument();
    for (int i = start; i <= end; i++) {
      String keyPath = config.groupLevels.get(i).keyPath();
      composite.append(
          keyPath, new BsonString(IntermediateAggregationMetaField.idKeysFieldPath(keyPath)));
    }
    return composite;
  }

  private static BsonString flatKeysLeafExpr(PathMergeConfig config) {
    String leafKey = config.groupLevels.get(config.groupLevels.size() - 1).keyPath();
    return new BsonString(IntermediateAggregationMetaField.idKeysFieldPath(leafKey));
  }

  /** {@code _id} for outer facet-arm {@code $group} stages over prior grouped rows. */
  private static BsonValue priorStageIdExpr(PathMergeConfig config, int prefixEnd) {
    if (prefixEnd == 0) {
      return new BsonString("$_id." + config.groupLevels.get(0).keyPath());
    }
    BsonDocument composite = new BsonDocument();
    for (int i = 0; i <= prefixEnd; i++) {
      String keyPath = config.groupLevels.get(i).keyPath();
      composite.append(keyPath, new BsonString("$_id." + keyPath));
    }
    return composite;
  }

  /**
   * Rewrites leaf metric projection expressions from the flat {@code $$b.} bucket prefix to {@code
   * $$<rowVar>.} for nested {@code $map} bodies.
   *
   * <p>Only whole-string field references with the {@link #BUCKET_FIELD_PREFIX} prefix are
   * rewritten. Leaf specs must not embed that substring elsewhere (e.g. inside a literal).
   */
  private static BsonValue scopeToVar(BsonValue expr, String rowVar) {
    if (expr.isString()) {
      String value = expr.asString().getValue();
      if (value.startsWith(BUCKET_FIELD_PREFIX)) {
        return new BsonString("$$" + rowVar + "." + value.substring(BUCKET_FIELD_PREFIX.length()));
      }
      return expr;
    }
    if (expr.isArray()) {
      BsonArray scoped = new BsonArray();
      for (BsonValue value : expr.asArray()) {
        scoped.add(scopeToVar(value, rowVar));
      }
      return scoped;
    }
    if (!expr.isDocument()) {
      return expr;
    }
    BsonDocument scoped = new BsonDocument();
    for (Map.Entry<String, BsonValue> entry : expr.asDocument().entrySet()) {
      scoped.append(entry.getKey(), scopeToVar(entry.getValue(), rowVar));
    }
    return scoped;
  }

  /**
   * Rewrites leaf metric projection expressions from the flat {@code $$b.} bucket prefix to root
   * field paths ({@code $<field>}) for the final facet-arm {@code $project}.
   *
   * <p>Only whole-string field references with the {@link #BUCKET_FIELD_PREFIX} prefix are
   * rewritten. Leaf specs must not embed that substring elsewhere (e.g. inside a literal).
   */
  private static BsonValue scopeToRoot(BsonValue expr) {
    if (expr.isString()) {
      String value = expr.asString().getValue();
      if (value.startsWith(BUCKET_FIELD_PREFIX)) {
        return new BsonString("$" + value.substring(BUCKET_FIELD_PREFIX.length()));
      }
      return expr;
    }
    if (expr.isArray()) {
      BsonArray scoped = new BsonArray();
      for (BsonValue value : expr.asArray()) {
        scoped.add(scopeToRoot(value));
      }
      return scoped;
    }
    if (!expr.isDocument()) {
      return expr;
    }
    BsonDocument scoped = new BsonDocument();
    for (Map.Entry<String, BsonValue> entry : expr.asDocument().entrySet()) {
      scoped.append(entry.getKey(), scopeToRoot(entry.getValue()));
    }
    return scoped;
  }
}
