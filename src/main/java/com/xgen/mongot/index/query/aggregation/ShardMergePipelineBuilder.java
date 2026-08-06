package com.xgen.mongot.index.query.aggregation;

import static com.xgen.mongot.util.Check.checkArg;

import com.google.errorprone.annotations.Var;
import com.xgen.mongot.util.Check;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
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
 * ShardMergePipelineBuilder builder = ShardMergePipelineBuilder.unsorted();
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
 *     group_name: "by_city",
 *     keys:       {city: "NYC", color: "red"}
 *   },
 *   sum_sum_price: 30,
 *   count_value:   1
 * }
 * // ... one grouped document per distinct (group_name, keys) pair ...
 *
 * // --- after Stage 2: $facet ---
 * // Each top-level branch arm $match-es on {@code $_id.group_name} via {@code $getField};
 * // count arm $match-es null routing the same way.
 * {
 *   by_city: [
 *     {_id: {group_name: "by_city", keys: {city: "NYC", color: "red"}},  ...},
 *     {_id: {group_name: "by_city", keys: {city: "NYC", color: "blue"}}, ...},
 *     {_id: {group_name: "by_city", keys: {city: "LA",  color: "red"}},  ...}
 *   ],
 *   count: [ ... ]
 * }
 *
 * // --- after Stage 3: $replaceWith ---
 * // $map projects _id.keys and merged metric fields into flat bucket documents.
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

  /** Temporary merge-pipeline field; stripped before facet output. */
  private static final String BOUNDARY_SORT_RANK_FIELD = "__boundarySortRank";

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

    /**
     * When a grouping level is terms, the {@code numBuckets} top-N to apply after merging shard
     * rows at that level (sort by {@code count_value} desc, bucket key asc, then {@code $limit}).
     */
    final Map<Integer, Integer> termsLevelLimits = new LinkedHashMap<>();

    /**
     * Grouping levels that use boundary (range) bucketing. Boundary buckets have a fixed request
     * order; shard merge must sort by bucket key ascending after the stage-1 {@code $group}.
     */
    final Set<Integer> boundaryLevelIndices = new HashSet<>();

    /**
     * When a boundary level has a default bucket, its label ({@link BsonString}) is stored here so
     * merge sort can place the default after range keys (MongoDB compares String before Date).
     */
    final Map<Integer, BsonValue> boundaryDefaultKeys = new LinkedHashMap<>();
  }

  // Stage 1: $group — metric field accumulators from leaf specs
  private final BsonDocument metricAccumulators = new BsonDocument();

  // Stage 2: $facet — per-path sub-pipelines from top-level specs
  private final BsonDocument facetPipelines = new BsonDocument();

  // Per top-level group_name: grouping keys and projection metadata
  private final Map<String, PathMergeConfig> pathConfigs = new LinkedHashMap<>();

  /**
   * Directions of the query's sort fields, {@code 1} ascending and {@code -1} descending, in query
   * order; empty when the query has no sort.
   *
   * <p>Only {@code first}/{@code last} read this: their merge orders one shard's bucket winner
   * against another's, which is the query's sort re-run on mongod. It lives on the builder rather
   * than on those specs because the sort belongs to the query as a whole, not to any one
   * aggregation node.
   */
  private final List<Integer> sortDirections;

  @Var private Optional<String> currentGroupName = Optional.empty();

  private ShardMergePipelineBuilder(List<Integer> sortDirections) {
    this.sortDirections = List.copyOf(sortDirections);
  }

  /** For a query with a global sort, given as one direction per sort field in query order. */
  public static ShardMergePipelineBuilder forSort(List<Integer> sortDirections) {
    return new ShardMergePipelineBuilder(sortDirections);
  }

  /** For a query with no global sort. */
  public static ShardMergePipelineBuilder unsorted() {
    return new ShardMergePipelineBuilder(List.of());
  }

  List<Integer> sortDirections() {
    return this.sortDirections;
  }

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
    addGroupLevel(keyPath, nestedBranchName, false, Optional.empty());
  }

  /**
   * Records one grouping level along a nested group chain.
   *
   * @param boundaryGrouping when true, bucket order is fixed by the request's boundary list (key
   *     ascending), not by doc count
   */
  public void addGroupLevel(
      String keyPath, Optional<String> nestedBranchName, boolean boundaryGrouping) {
    addGroupLevel(keyPath, nestedBranchName, boundaryGrouping, Optional.empty());
  }

  /**
   * Records one grouping level along a nested group chain.
   *
   * @param boundaryDefaultKey when present on a boundary level, the default bucket's label used to
   *     sort the default bucket after range keys in the merge pipeline
   */
  public void addGroupLevel(
      String keyPath,
      Optional<String> nestedBranchName,
      boolean boundaryGrouping,
      Optional<BsonValue> boundaryDefaultKey) {
    PathMergeConfig config = this.pathConfigs.get(requireCurrentGroupName());
    int levelIndex = config.groupLevels.size();
    config.groupLevels.add(new GroupMergeLevel(keyPath, nestedBranchName));
    if (boundaryGrouping) {
      config.boundaryLevelIndices.add(levelIndex);
      boundaryDefaultKey.ifPresent(key -> config.boundaryDefaultKeys.put(levelIndex, key));
    }
  }

  /** Called by leaf metric specs — adds accumulator to Stage 1 $group. */
  public void addMetricAccumulator(String field, BsonDocument expr) {
    String groupName = requireCurrentGroupName();
    this.metricAccumulators.append(field, expr);
    this.pathConfigs.get(groupName).accumulatorFields.add(field);
  }

  /** Records {@code numBuckets} for a terms grouping at the current level. */
  public void configureTermsGrouping(int bucketLimit) {
    checkArg(bucketLimit > 0, "bucketLimit must be positive, got: %s", bucketLimit);
    PathMergeConfig config = this.pathConfigs.get(requireCurrentGroupName());
    int levelIndex = config.groupLevels.size() - 1;
    config.termsLevelLimits.put(levelIndex, bucketLimit);
  }

  /** Called by leaf metric specs — adds finalization expression to final output. */
  public void addMetricProjection(String field, BsonValue expr) {
    this.pathConfigs.get(requireCurrentGroupName()).metricProjections.append(field, expr);
  }

  /**
   * Assemble the 3-stage pipeline ($group, $facet, $replaceWith).
   *
   * <p>Flat leaf terms branches apply {@code $sort} (count desc, bucket key asc) and {@code $limit}
   * ({@code numBuckets}) in the facet arm after the stage-1 {@code $group} merge. Nested groupings
   * still need per-level top-N inside reconstructed buckets.
   *
   * @param countType "lowerBound" or "total" — determines the count field name in the output
   */
  public List<BsonDocument> build(String countType) {
    // Stage1: $group
    BsonDocument groupId =
        new BsonDocument()
            .append(
                IntermediateAggregationMetaField.ID_GROUP_NAME,
                IntermediateAggregationMetaField.rootFieldExpression(
                    IntermediateAggregationMetaField.GROUP_NAME))
            .append(
                IntermediateAggregationMetaField.ID_KEYS,
                IntermediateAggregationMetaField.rootFieldExpression(
                    IntermediateAggregationMetaField.KEYS));
    BsonDocument groupBody =
        new BsonDocument()
            .append("_id", groupId)
            .append(
                "count_value",
                new BsonDocument(
                    "$sum",
                    new BsonString("$" + IntermediateAggregationMetaField.BUCKET_DOC_COUNT)));
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
        if (isBoundaryLevel(config, 0)) {
          appendGroupedBoundarySortStage(arm, boundaryDefaultKey(config, 0));
        } else {
          appendNestedTermsTopNStages(arm, config, 0);
        }
        arm.add(buildNestedProjectStage(config));
      } else if (depth == 1) {
        Check.checkState(
            !config.metricProjections.isEmpty(),
            "single-level facet arm %s requires leaf metric projections",
            groupName);
        if (isBoundaryLevel(config, 0)) {
          appendFlatBoundarySortStage(
              arm, config.groupLevels.get(0).keyPath(), boundaryDefaultKey(config, 0));
        } else {
          appendFlatTermsTopNStages(arm, config);
        }
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
      if (config.termsLevelLimits.containsKey(0)) {
        return innermostGroupStage(groupId, pushDoc);
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
    BsonValue bucketsExpr = buildBucketsAtLevel(config, 0, new BsonString("$_rows"), "row");
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
      PathMergeConfig config, int levelIndex, BsonValue input, String rowVar) {
    int depth = config.groupLevels.size();
    if (levelIndex == depth - 2) {
      int leafKeyIndex = depth - 1;
      @Var BsonValue mapInput = input;
      if (isBoundaryLevel(config, leafKeyIndex)) {
        mapInput =
            boundarySortArrayExpr(
                mapInput,
                config.groupLevels.get(leafKeyIndex).keyPath(),
                boundaryDefaultKey(config, leafKeyIndex));
      }
      return mapExpr(mapInput, rowVar, leafRowBody(config, rowVar, leafKeyIndex));
    }

    String childBranch = config.groupLevels.get(levelIndex + 1).nestedBranchName().orElseThrow();
    String rowKey = config.groupLevels.get(levelIndex + 1).keyPath();
    @Var BsonValue innerInput = new BsonString("$$" + rowVar + "._leaf_rows");
    if (isBoundaryLevel(config, levelIndex + 1)) {
      innerInput =
          boundarySortArrayExpr(
              innerInput,
              config.groupLevels.get(levelIndex + 1).keyPath(),
              boundaryDefaultKey(config, levelIndex + 1));
    }
    BsonValue innerBuckets = buildBucketsAtLevel(config, levelIndex + 1, innerInput, "leaf");

    BsonDocument rowBody =
        new BsonDocument()
            .append("_id", new BsonString("$$" + rowVar + "." + rowKey))
            .append(childBranch, new BsonDocument("buckets", innerBuckets));
    return mapExpr(input, rowVar, rowBody);
  }

  /** Adds {@code $sort} and {@code $limit} for flat leaf terms before {@code $project}. */
  private static boolean isBoundaryLevel(PathMergeConfig config, int levelIndex) {
    return config.boundaryLevelIndices.contains(levelIndex);
  }

  /** Sort flat leaf boundary buckets by request order before {@code $project}. */
  private static void appendFlatBoundarySortStage(
      BsonArray arm, String keyPath, Optional<BsonValue> defaultKey) {
    if (defaultKey.isEmpty()) {
      arm.add(buildFlatBoundarySortStage(keyPath));
      return;
    }
    String keyField = "_id." + IntermediateAggregationMetaField.ID_KEYS + "." + keyPath;
    arm.add(
        new BsonDocument(
            "$addFields",
            new BsonDocument(
                BOUNDARY_SORT_RANK_FIELD,
                boundaryDefaultRankExpr(new BsonString("$" + keyField), defaultKey.get()))));
    arm.add(
        new BsonDocument(
            "$sort",
            new BsonDocument()
                .append(BOUNDARY_SORT_RANK_FIELD, new BsonInt32(1))
                .append(keyField, new BsonInt32(1))));
    arm.add(new BsonDocument("$unset", new BsonString(BOUNDARY_SORT_RANK_FIELD)));
  }

  /**
   * Sort nested outer boundary buckets by request order before {@code $project}. After the
   * innermost facet-arm {@code $group}, {@code _id} is the outer boundary key.
   */
  private static void appendGroupedBoundarySortStage(
      BsonArray arm, Optional<BsonValue> defaultKey) {
    if (defaultKey.isEmpty()) {
      arm.add(new BsonDocument("$sort", new BsonDocument("_id", new BsonInt32(1))));
      return;
    }
    arm.add(
        new BsonDocument(
            "$addFields",
            new BsonDocument(
                BOUNDARY_SORT_RANK_FIELD,
                boundaryDefaultRankExpr(new BsonString("$_id"), defaultKey.get()))));
    arm.add(
        new BsonDocument(
            "$sort",
            new BsonDocument()
                .append(BOUNDARY_SORT_RANK_FIELD, new BsonInt32(1))
                .append("_id", new BsonInt32(1))));
    arm.add(new BsonDocument("$unset", new BsonString(BOUNDARY_SORT_RANK_FIELD)));
  }

  private static void appendFlatTermsTopNStages(BsonArray arm, PathMergeConfig config) {
    appendTermsTopNStages(arm, config, 0, true);
  }

  private static void appendNestedTermsTopNStages(
      BsonArray arm, PathMergeConfig config, int levelIndex) {
    appendTermsTopNStages(arm, config, levelIndex, false);
  }

  private static void appendTermsTopNStages(
      BsonArray arm, PathMergeConfig config, int levelIndex, boolean flatKeys) {
    Integer bucketLimit = config.termsLevelLimits.get(levelIndex);
    if (bucketLimit == null) {
      return;
    }
    String keyPath = config.groupLevels.get(levelIndex).keyPath();
    arm.add(buildTermsSortStage(keyPath, flatKeys));
    arm.add(new BsonDocument("$limit", new BsonInt32(bucketLimit)));
  }

  private static BsonDocument buildTermsSortStage(String keyPath, boolean flatKeys) {
    BsonDocument sortKeys = new BsonDocument().append("count_value", new BsonInt32(-1));
    if (flatKeys) {
      sortKeys.append(
          "_id." + IntermediateAggregationMetaField.ID_KEYS + "." + keyPath, new BsonInt32(1));
    } else {
      sortKeys.append("_id", new BsonInt32(1));
    }
    return new BsonDocument("$sort", sortKeys);
  }

  private static BsonDocument buildFlatBoundarySortStage(String keyPath) {
    return new BsonDocument(
        "$sort",
        new BsonDocument(
            "_id." + IntermediateAggregationMetaField.ID_KEYS + "." + keyPath, new BsonInt32(1)));
  }

  /** Single-level branch: finalize metrics in the $facet arm. */
  private static BsonDocument buildFlatProjectStage(PathMergeConfig config) {
    BsonDocument projectBody =
        new BsonDocument()
            .append(
                "_id",
                IntermediateAggregationMetaField.idKeyExpression(
                    config.groupLevels.get(0).keyPath(), "$_id"));
    for (Map.Entry<String, BsonValue> metric : config.metricProjections.entrySet()) {
      projectBody.append(metric.getKey(), scopeToRoot(metric.getValue()));
    }
    return new BsonDocument("$project", projectBody);
  }

  private static BsonDocument innermostGroupStage(BsonValue groupId, BsonDocument pushDoc) {
    return new BsonDocument(
        "$group",
        new BsonDocument()
            .append("_id", groupId)
            .append("count_value", new BsonDocument("$sum", new BsonString("$count_value")))
            .append("_rows", new BsonDocument("$push", pushDoc)));
  }

  private static BsonDocument groupStage(BsonValue groupId, BsonDocument pushDoc) {
    return new BsonDocument(
        "$group",
        new BsonDocument()
            .append("_id", groupId)
            .append("_rows", new BsonDocument("$push", pushDoc)));
  }

  private static BsonDocument mapExpr(BsonValue input, String rowVar, BsonDocument rowBody) {
    return new BsonDocument(
        "$map",
        new BsonDocument()
            .append("input", input)
            .append("as", new BsonString(rowVar))
            .append("in", rowBody));
  }

  private static BsonValue boundarySortArrayExpr(
      BsonValue input, String keyPath, Optional<BsonValue> defaultKey) {
    if (defaultKey.isEmpty()) {
      return new BsonDocument(
          "$sortArray",
          new BsonDocument()
              .append("input", input)
              .append("sortBy", new BsonDocument(keyPath, new BsonInt32(1))));
    }
    BsonValue rankedInput =
        new BsonDocument(
            "$map",
            new BsonDocument()
                .append("input", input)
                .append("as", new BsonString("row"))
                .append(
                    "in",
                    new BsonDocument(
                        "$mergeObjects",
                        new BsonArray(
                            List.of(
                                new BsonString("$$row"),
                                new BsonDocument(
                                    BOUNDARY_SORT_RANK_FIELD,
                                    boundaryDefaultRankExpr(
                                        new BsonString("$$row." + keyPath), defaultKey.get())))))));
    BsonValue sorted =
        new BsonDocument(
            "$sortArray",
            new BsonDocument()
                .append("input", rankedInput)
                .append(
                    "sortBy",
                    new BsonDocument()
                        .append(BOUNDARY_SORT_RANK_FIELD, new BsonInt32(1))
                        .append(keyPath, new BsonInt32(1))));
    return new BsonDocument(
        "$map",
        new BsonDocument()
            .append("input", sorted)
            .append("as", new BsonString("row"))
            .append("in", stripBoundarySortRankField(new BsonString("$$row"))));
  }

  private static Optional<BsonValue> boundaryDefaultKey(PathMergeConfig config, int levelIndex) {
    return Optional.ofNullable(config.boundaryDefaultKeys.get(levelIndex));
  }

  private static BsonValue boundaryDefaultRankExpr(BsonValue keyExpr, BsonValue defaultKey) {
    return new BsonDocument(
        "$cond",
        new BsonArray(
            List.of(
                new BsonDocument("$eq", new BsonArray(List.of(keyExpr, defaultKey))),
                new BsonInt32(1),
                new BsonInt32(0))));
  }

  private static BsonValue stripBoundarySortRankField(BsonValue rowExpr) {
    return new BsonDocument(
        "$arrayToObject",
        new BsonDocument(
            "$filter",
            new BsonDocument()
                .append("input", new BsonDocument("$objectToArray", rowExpr))
                .append("as", new BsonString("entry"))
                .append(
                    "cond",
                    new BsonDocument(
                        "$ne",
                        new BsonArray(
                            List.of(
                                new BsonString("$$entry.k"),
                                new BsonString(BOUNDARY_SORT_RANK_FIELD)))))));
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
      return IntermediateAggregationMetaField.idKeyExpression(
          config.groupLevels.get(start).keyPath(), "$_id");
    }
    BsonDocument composite = new BsonDocument();
    for (int i = start; i <= end; i++) {
      String keyPath = config.groupLevels.get(i).keyPath();
      composite.append(keyPath, IntermediateAggregationMetaField.idKeyExpression(keyPath, "$_id"));
    }
    return composite;
  }

  private static BsonValue flatKeysLeafExpr(PathMergeConfig config) {
    String leafKey = config.groupLevels.get(config.groupLevels.size() - 1).keyPath();
    return IntermediateAggregationMetaField.idKeyExpression(leafKey, "$_id");
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
