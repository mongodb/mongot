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
 * { $meta/group_name: "by_city", $meta/keys: {k0: "NYC", k1: "red"},  sum_sum_price: 30 }
 * { $meta/group_name: "by_city", $meta/keys: {k0: "NYC", k1: "blue"}, sum_sum_price: 20 }
 * { $meta/group_name: "by_city", $meta/keys: {k0: "LA",  k1: "red"},  sum_sum_price: 5  }
 *
 * // --- after Stage 1: $group ---
 * // _id reads $meta/group_name and $meta/keys via $getField; metrics are summed per (_id) key.
 * {
 *   _id: {
 *     group_name: "by_city",
 *     keys:       {k0: "NYC", k1: "red"}
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
 *     {_id: {group_name: "by_city", keys: {k0: "NYC", k1: "red"}},  ...},
 *     {_id: {group_name: "by_city", keys: {k0: "NYC", k1: "blue"}}, ...},
 *     {_id: {group_name: "by_city", keys: {k0: "LA",  k1: "red"}},  ...}
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

  /**
   * Per-row bucket doc count pushed through nested {@code $group} reconstruction so inner terms
   * levels can {@code $sortArray} by count desc. Same name as the {@code $group} accumulator.
   * Dropped by {@code $map} output bodies.
   */
  private static final String COUNT_VALUE_FIELD = "count_value";

  /** One grouping level in a top-level facet branch's merge/reconstruction chain. */
  record GroupMergeLevel(String keyPath, Optional<String> nestedBranchName) {}

  private static final class PathMergeConfig {
    /** User-facing top-level facet branch name in the response. */
    final String userFacetName;

    PathMergeConfig(String userFacetName) {
      this.userFacetName = userFacetName;
    }

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
     * rows at that level (count desc, bucket key asc). Flat and nested-outer levels use document
     * {@code $sort}/{@code $limit}; nested inner levels use {@code $sortArray}/{@code $slice}.
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

  // Per facet arm alias: grouping keys and projection metadata
  private final Map<String, PathMergeConfig> pathConfigs = new LinkedHashMap<>();

  private final ShardMergeAliasPlan aliasPlan;

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

  @Var private Optional<String> currentFacetAlias = Optional.empty();

  private ShardMergePipelineBuilder(ShardMergeAliasPlan aliasPlan, List<Integer> sortDirections) {
    this.aliasPlan = aliasPlan;
    this.sortDirections = List.copyOf(sortDirections);
  }

  /** For a query with a global sort, given as one direction per sort field in query order. */
  public static ShardMergePipelineBuilder forSort(
      ShardMergeAliasPlan aliasPlan, List<Integer> sortDirections) {
    return new ShardMergePipelineBuilder(aliasPlan, sortDirections);
  }

  /** For a query with a global sort, given as one direction per sort field in query order. */
  public static ShardMergePipelineBuilder forSort(List<Integer> sortDirections) {
    throw new UnsupportedOperationException(
        "Use forSort(ShardMergeAliasPlan, List<Integer>) with a plan built from the spec tree");
  }

  /** For a query with no global sort. */
  public static ShardMergePipelineBuilder create(ShardMergeAliasPlan aliasPlan) {
    return new ShardMergePipelineBuilder(aliasPlan, List.of());
  }

  /** For a query with no global sort. */
  public static ShardMergePipelineBuilder unsorted() {
    throw new UnsupportedOperationException(
        "Use create(ShardMergeAliasPlan) with a plan built from the spec tree");
  }

  List<Integer> sortDirections() {
    return this.sortDirections;
  }

  private String requireCurrentFacetAlias() {
    return this.currentFacetAlias.orElseThrow(
        () -> new IllegalStateException("beginPath() must be called first"));
  }

  private String wirePartialAlias(String wirePartial) {
    return this.aliasPlan.wirePartialAlias(wirePartial);
  }

  /**
   * Called by the coordinator before traversing each top-level spec. Registers the facet arm alias
   * for Stage 2 $facet sub-pipeline ($match on {@link
   * IntermediateAggregationMetaField#GROUP_NAME}).
   */
  public void beginPath(String userFacetName) {
    String facetAlias = this.aliasPlan.facetAlias(userFacetName);
    this.currentFacetAlias = Optional.of(facetAlias);
    this.pathConfigs.putIfAbsent(facetAlias, new PathMergeConfig(userFacetName));
    this.facetPipelines.append(
        facetAlias,
        new BsonArray(
            List.of(
                IntermediateAggregationMetaField.matchIdFieldStage(
                    IntermediateAggregationMetaField.GROUP_NAME, new BsonString(facetAlias)))));
  }

  /** Called by the coordinator after traversing each top-level spec. */
  public void endPath() {
    this.currentFacetAlias = Optional.empty();
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
    nestedBranchName.ifPresent(
        name -> {
          if (IntermediateAggregationMetaField.RESERVED_BUCKET_FIELDS.contains(name)) {
            throw new IllegalArgumentException(
                "Nested group name '" + name + "' is reserved for internal use");
          }
        });
    PathMergeConfig config = this.pathConfigs.get(requireCurrentFacetAlias());
    int levelIndex = config.groupLevels.size();
    config.groupLevels.add(new GroupMergeLevel(keyPath, nestedBranchName));
    if (boundaryGrouping) {
      config.boundaryLevelIndices.add(levelIndex);
      boundaryDefaultKey.ifPresent(key -> config.boundaryDefaultKeys.put(levelIndex, key));
    }
  }

  /** Called by leaf metric specs — adds accumulator to Stage 1 $group. */
  public void addMetricAccumulator(String wireField, BsonDocument expr) {
    String facetAlias = requireCurrentFacetAlias();
    String alias = wirePartialAlias(wireField);
    this.metricAccumulators.append(
        alias,
        ShardMergeAliasExpressions.rewriteWireRefs(expr, this.aliasPlan.wirePartialToAliasMap())
            .asDocument());
    this.pathConfigs.get(facetAlias).accumulatorFields.add(alias);
  }

  /** Records {@code numBuckets} for a terms grouping at the current level. */
  public void configureTermsGrouping(int bucketLimit) {
    checkArg(bucketLimit > 0, "bucketLimit must be positive, got: %s", bucketLimit);
    PathMergeConfig config = this.pathConfigs.get(requireCurrentFacetAlias());
    int levelIndex = config.groupLevels.size() - 1;
    config.termsLevelLimits.put(levelIndex, bucketLimit);
  }

  /** Called by leaf metric specs — adds finalization expression to final output. */
  public void addMetricProjection(String userOutputField, BsonValue expr) {
    this.pathConfigs
        .get(requireCurrentFacetAlias())
        .metricProjections
        .append(
            userOutputField,
            ShardMergeAliasExpressions.rewriteWireRefs(
                expr, this.aliasPlan.wirePartialToAliasMap()));
  }

  /**
   * Assemble the 3-stage pipeline ($group, $facet, $replaceWith).
   *
   * <p>Flat leaf terms branches apply {@code $sort} (count desc, bucket key asc) and {@code $limit}
   * ({@code numBuckets}) in the facet arm after the stage-1 {@code $group} merge. Nested outer
   * terms use the same document {@code $sort}/{@code $limit}; inner nested terms apply per-parent
   * top-N via {@code $sortArray}+{@code $slice} while reconstructing buckets.
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
      String facetAlias = entry.getKey();
      PathMergeConfig config = entry.getValue();
      BsonArray arm = this.facetPipelines.get(facetAlias).asArray();

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
            config.userFacetName);
        if (isBoundaryLevel(config, 0)) {
          appendFlatBoundarySortStage(arm, 0, boundaryDefaultKey(config, 0));
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
      String facetAlias = entry.getKey();
      PathMergeConfig config = entry.getValue();
      String userFacetName = config.userFacetName;

      if (usesDirectFacetReference(config)) {
        facetBody.append(
            userFacetName, new BsonDocument("buckets", new BsonString("$" + facetAlias)));
      } else if (!config.metricProjections.isEmpty()) {
        BsonDocument inFields = new BsonDocument();
        if (!config.groupLevels.isEmpty()) {
          inFields.append(
              "_id",
              new BsonString(
                  IntermediateAggregationMetaField.groupedKeyFieldPath(
                      GroupingKeyAliases.alias(0))));
        }
        inFields.putAll(config.metricProjections);
        BsonDocument mapExpr =
            new BsonDocument()
                .append("input", new BsonString("$" + facetAlias))
                .append("as", new BsonString(FLAT_BUCKET_ROW_VAR))
                .append("in", inFields);
        facetBody.append(
            userFacetName, new BsonDocument("buckets", new BsonDocument("$map", mapExpr)));
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
      BsonValue groupId = flatKeysIdExpr(0, depth - 2);
      BsonDocument pushDoc =
          new BsonDocument()
              .append(GroupingKeyAliases.alias(depth - 1), flatKeysLeafExpr(config))
              .append(COUNT_VALUE_FIELD, new BsonString("$" + COUNT_VALUE_FIELD));
      for (String field : config.accumulatorFields) {
        pushDoc.append(field, new BsonString("$" + field));
      }
      return groupStage(groupId, pushDoc);
    }

    int prefixEnd = depth - 2 - stageIndex;
    BsonValue groupId = priorStageIdExpr(prefixEnd);
    String pushedAlias = GroupingKeyAliases.alias(depth - 1 - stageIndex);
    BsonDocument pushDoc =
        new BsonDocument()
            .append(pushedAlias, new BsonString("$_id." + pushedAlias))
            .append(COUNT_VALUE_FIELD, new BsonString("$" + COUNT_VALUE_FIELD))
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
                IntermediateAggregationMetaField.BUCKET_DOC_COUNT,
                new BsonDocument(
                    IntermediateAggregationMetaField.BUCKET_DOC_COUNT_TYPE,
                    new BsonString("$" + COUNT_VALUE_FIELD)))
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
      return mapExpr(
          orderLevelBuckets(config, leafKeyIndex, input),
          rowVar,
          leafRowBody(config, rowVar, leafKeyIndex));
    }

    String childBranch = config.groupLevels.get(levelIndex + 1).nestedBranchName().orElseThrow();
    String rowAlias = GroupingKeyAliases.alias(levelIndex + 1);
    // Sort/trim this level's child rows; pass unwrapped _leaf_rows so the recursive call
    // applies the next level's own order (parent keys are not present on grandchild rows).
    BsonValue mapInput = orderLevelBuckets(config, levelIndex + 1, input);
    BsonValue innerInput = new BsonString("$$" + rowVar + "._leaf_rows");
    BsonValue innerBuckets = buildBucketsAtLevel(config, levelIndex + 1, innerInput, "leaf");

    BsonDocument rowBody =
        new BsonDocument()
            .append("_id", new BsonString("$$" + rowVar + "." + rowAlias))
            .append(
                IntermediateAggregationMetaField.BUCKET_DOC_COUNT,
                new BsonDocument(
                    IntermediateAggregationMetaField.BUCKET_DOC_COUNT_TYPE,
                    new BsonString("$$" + rowVar + "." + COUNT_VALUE_FIELD)))
            .append(childBranch, new BsonDocument("buckets", innerBuckets));
    return mapExpr(mapInput, rowVar, rowBody);
  }

  private static boolean isBoundaryLevel(PathMergeConfig config, int levelIndex) {
    return config.boundaryLevelIndices.contains(levelIndex);
  }

  /** Sort flat leaf boundary buckets by request order before {@code $project}. */
  private static void appendFlatBoundarySortStage(
      BsonArray arm, int levelIndex, Optional<BsonValue> defaultKey) {
    String keyAlias = GroupingKeyAliases.alias(levelIndex);
    if (defaultKey.isEmpty()) {
      arm.add(buildFlatBoundarySortStage(keyAlias));
      return;
    }
    String keyField = "_id." + IntermediateAggregationMetaField.ID_KEYS + "." + keyAlias;
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
    arm.add(buildTermsSortStage(levelIndex, flatKeys));
    arm.add(new BsonDocument("$limit", new BsonInt32(bucketLimit)));
  }

  private static BsonDocument buildTermsSortStage(int levelIndex, boolean flatKeys) {
    String keyAlias = GroupingKeyAliases.alias(levelIndex);
    BsonDocument sortKeys = new BsonDocument().append(COUNT_VALUE_FIELD, new BsonInt32(-1));
    if (flatKeys) {
      sortKeys.append(
          "_id." + IntermediateAggregationMetaField.ID_KEYS + "." + keyAlias, new BsonInt32(1));
    } else {
      sortKeys.append("_id", new BsonInt32(1));
    }
    return new BsonDocument("$sort", sortKeys);
  }

  private static BsonDocument buildFlatBoundarySortStage(String keyAlias) {
    return new BsonDocument(
        "$sort",
        new BsonDocument(
            "_id." + IntermediateAggregationMetaField.ID_KEYS + "." + keyAlias, new BsonInt32(1)));
  }

  /** Single-level branch: finalize metrics in the $facet arm. */
  private static BsonDocument buildFlatProjectStage(PathMergeConfig config) {
    BsonDocument projectBody =
        new BsonDocument()
            .append(
                "_id",
                IntermediateAggregationMetaField.idKeyExpression(
                    GroupingKeyAliases.alias(0), "$_id"));
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
            .append(
                COUNT_VALUE_FIELD,
                new BsonDocument("$sum", new BsonString("$" + COUNT_VALUE_FIELD)))
            .append("_rows", new BsonDocument("$push", pushDoc)));
  }

  /**
   * Apply this grouping level's output order to {@code input} (the child-row array being {@code
   * $map}ped). Terms levels take count-desc / key-asc top-N; boundary levels take request key
   * order. Mutually exclusive per level.
   */
  private static BsonValue orderLevelBuckets(
      PathMergeConfig config, int keyIndex, BsonValue input) {
    if (isBoundaryLevel(config, keyIndex)) {
      return boundarySortArrayExpr(
          input, GroupingKeyAliases.alias(keyIndex), boundaryDefaultKey(config, keyIndex));
    }
    return maybeTermsTopN(config, keyIndex, input);
  }

  /**
   * Sort+trim a reconstructed child-bucket array by doc count desc, key asc, keeping {@code n}
   * buckets. No-op when this grouping level has no terms {@code numBuckets}.
   */
  private static BsonValue maybeTermsTopN(PathMergeConfig config, int keyIndex, BsonValue input) {
    Integer bucketLimit = config.termsLevelLimits.get(keyIndex);
    if (bucketLimit == null) {
      return input;
    }
    return termsTopNArrayExpr(input, GroupingKeyAliases.alias(keyIndex), bucketLimit);
  }

  private static BsonValue termsTopNArrayExpr(BsonValue input, String keyAlias, int n) {
    BsonDocument sortBy =
        new BsonDocument()
            .append(COUNT_VALUE_FIELD, new BsonInt32(-1))
            .append(keyAlias, new BsonInt32(1));
    BsonDocument sorted =
        new BsonDocument(
            "$sortArray", new BsonDocument().append("input", input).append("sortBy", sortBy));
    return new BsonDocument("$slice", new BsonArray(List.of(sorted, new BsonInt32(n))));
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
      BsonValue input, String keyAlias, Optional<BsonValue> defaultKey) {
    if (defaultKey.isEmpty()) {
      return new BsonDocument(
          "$sortArray",
          new BsonDocument()
              .append("input", input)
              .append("sortBy", new BsonDocument(keyAlias, new BsonInt32(1))));
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
                                        new BsonString("$$row." + keyAlias),
                                        defaultKey.get())))))));
    BsonValue sorted =
        new BsonDocument(
            "$sortArray",
            new BsonDocument()
                .append("input", rankedInput)
                .append(
                    "sortBy",
                    new BsonDocument()
                        .append(BOUNDARY_SORT_RANK_FIELD, new BsonInt32(1))
                        .append(keyAlias, new BsonInt32(1))));
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
                "_id", new BsonString("$$" + rowVar + "." + GroupingKeyAliases.alias(keyIndex)));
    for (Map.Entry<String, BsonValue> metric : config.metricProjections.entrySet()) {
      rowBody.append(metric.getKey(), scopeToVar(metric.getValue(), rowVar));
    }
    return rowBody;
  }

  /** {@code _id} for the innermost facet-arm {@code $group} over flat Stage-1 rows. */
  private static BsonValue flatKeysIdExpr(int start, int end) {
    if (start == end) {
      return IntermediateAggregationMetaField.idKeyExpression(
          GroupingKeyAliases.alias(start), "$_id");
    }
    BsonDocument composite = new BsonDocument();
    for (int i = start; i <= end; i++) {
      composite.append(
          GroupingKeyAliases.alias(i),
          IntermediateAggregationMetaField.idKeyExpression(GroupingKeyAliases.alias(i), "$_id"));
    }
    return composite;
  }

  private static BsonValue flatKeysLeafExpr(PathMergeConfig config) {
    int leafIndex = config.groupLevels.size() - 1;
    return IntermediateAggregationMetaField.idKeyExpression(
        GroupingKeyAliases.alias(leafIndex), "$_id");
  }

  /** {@code _id} for outer facet-arm {@code $group} stages over prior grouped rows. */
  private static BsonValue priorStageIdExpr(int prefixEnd) {
    if (prefixEnd == 0) {
      return new BsonString("$_id." + GroupingKeyAliases.alias(0));
    }
    BsonDocument composite = new BsonDocument();
    for (int i = 0; i <= prefixEnd; i++) {
      String keyAlias = GroupingKeyAliases.alias(i);
      composite.append(keyAlias, new BsonString("$_id." + keyAlias));
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
