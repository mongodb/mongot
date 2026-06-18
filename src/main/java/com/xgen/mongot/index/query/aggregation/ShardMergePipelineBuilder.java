package com.xgen.mongot.index.query.aggregation;

import com.xgen.mongot.util.Check;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.bson.BsonArray;
import org.bson.BsonDocument;
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
 * builder.beginTag("byDirector");
 * directorSpec.buildShardMergePipeline(builder);  // recursive
 * builder.endTag();
 * List<BsonDocument> pipeline = builder.build("lowerBound");
 * }</pre>
 *
 * <p>Stage 1: $group metric field accumulators. _id is always { tag: "$tag", keys: "$keys" } —
 * hardcoded in build(). Stage 2: $facet per-path computation. Stage 3: $replaceWith projections.
 * [
 *    $group: {
 *      _id: { tag: "$tag", keys: "$keys" },
 *      count_value: { $sum: "$count"},
 *      avg_sum_color: { $sum: "$avg_sum_color"}
 *      ...
 *    },
 *    $facet: {
 *      "byDirector": [{ $match: { "_id.tag": "byDirector" } }],
 *      ....,
 *      "count": [{ $match: { "_id.tag": null } }]
 *    },
 *    $replaceWith: {
 *      "count": {
 *        "lowerBound": { $first: "$count.count_value" }
 *      },
 *      "facet": {
 *        "byDirector": {
 *          "buckets": {
 *            "$map": {
 *              "input": "$byDirector",
 *              "as": "b",
 *              "in": {
 *                "director": "$$b._id.keys.director",
 *                "avg_price": {"$divide": ["$$b.avg_sum_avg_price", "$$b.avg_count_avg_price"]}
 *              }
 *            }
 *          }
 *        }
 *      }
 *    }
 * ]
 */
public final class ShardMergePipelineBuilder {

  // Stage 1: $group — metric field accumulators from leaf specs
  private final BsonDocument metricAccumulators = new BsonDocument();

  // Stage 2: $facet — per-path sub-pipelines from top-level specs
  private final BsonDocument facetPipelines = new BsonDocument();

  // Stage 3: $replaceWith — per-path $map "in" fields from all specs along path
  private final Map<String, BsonDocument> projectionFields = new LinkedHashMap<>();

  @Nullable private String currentTag;

  /**
   * Called by the coordinator before traversing each top-level spec. Registers the tag for Stage 2
   * $facet sub-pipeline ($match on tag).
   */
  public void beginTag(String tag) {
    this.currentTag = tag;
    this.facetPipelines.append(
        tag,
        new BsonArray(
            List.of(new BsonDocument("$match", new BsonDocument("_id.tag", new BsonString(tag))))));
  }

  /** Called by the coordinator after traversing each top-level spec. */
  @SuppressWarnings("NullAway")
  public void endTag() {
    this.currentTag = null;
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
    Check.isNotNull(this.currentTag, "beginTag() must be called before addFacetStage()");
    this.facetPipelines.get(this.currentTag).asArray().add(stage);
  }

  /** Called by bucket specs — extracts a key from _id.keys into the final output. */
  public void addKeyProjection(String keyName) {
    this.projectionFields
        .computeIfAbsent(this.currentTag, k -> new BsonDocument())
        .append(keyName, new BsonString("$$b._id.keys." + keyName));
  }

  /** Called by leaf metric specs — adds finalization expression to final output. */
  public void addMetricProjection(String field, BsonValue expr) {
    this.projectionFields
        .computeIfAbsent(this.currentTag, k -> new BsonDocument())
        .append(field, expr);
  }

  /**
   * Assemble the 3-stage pipeline ($group, $facet, $replaceWith).
   *
   * @param countType "lowerBound" or "total" — determines the count field name in the output
   */
  public List<BsonDocument> build(String countType) {
    // Stage1: $group
    // _id is always { tag: "$tag", keys: "$keys"}
    // Also sum the "count" field from count docs (0 for aggregation docs that lack it)
    BsonDocument groupId =
        new BsonDocument()
            .append("tag", new BsonString("$tag"))
            .append("keys", new BsonString("$keys"));
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
    // Add "count" sub-pipeline: match count docs (they have null tag since count doc has no tag)
    this.facetPipelines.append(
        "count",
        new BsonArray(
            List.of(
                new BsonDocument("$match", new BsonDocument("_id.tag", new org.bson.BsonNull())))));
    BsonDocument facetStage = new BsonDocument("$facet", this.facetPipelines);

    // Stage3: $replaceWith
    // Add count field, then facet results
    BsonDocument replaceWithBody = new BsonDocument();
    // Count: extract from the "count" sub-pipeline
    replaceWithBody.append(
        "count",
        new BsonDocument(
            countType, new BsonDocument("$first", new BsonString("$count.count_value"))));
    // Facet results: wrap in "facet" object
    BsonDocument facetBody = new BsonDocument();
    for (Map.Entry<String, BsonDocument> entry : this.projectionFields.entrySet()) {
      String tag = entry.getKey();
      BsonDocument inFields = entry.getValue();

      BsonDocument mapExpr =
          new BsonDocument()
              .append("input", new BsonString("$" + tag))
              .append("as", new BsonString("b"))
              .append("in", inFields);
      facetBody.append(tag, new BsonDocument("buckets", new BsonDocument("$map", mapExpr)));
    }
    replaceWithBody.append("facet", facetBody);
    BsonDocument replaceWithStage = new BsonDocument("$replaceWith", replaceWithBody);

    return List.of(groupStage, facetStage, replaceWithStage);
  }
}
