package com.xgen.mongot.index.query.aggregation;

import com.google.common.annotations.VisibleForTesting;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.FieldPath;
import java.util.ArrayList;
import java.util.List;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonValue;

/**
 * The intermediate node for the result tree.
 *
 * <p>Each instance contains a set of buckets, representing the result given a certain grouping key.
 * e.g. group by term "city", it will have buckets: "New York", "San Francisco", or group by range
 * "price", it will have buckets: "[0, 100)", "[100, 200)", "[200, 300)"
 */
public abstract class MultiBucketAggregationResult implements AggregationResult {

  // The index field path for the group by key.
  protected final FieldPath path;

  public MultiBucketAggregationResult(FieldPath path) {
    this.path = path;
  }

  public final FieldPath getPath() {
    return this.path;
  }

  /**
   * Bucket result for a particular grouping key.
   *
   * <p>Bucket is a recursive data structure through AggregationResults to represent the multi-field
   * grouping.
   */
  public interface Bucket {
    /** The key associated with this bucket. */
    BsonValue getKey();

    /** Return immediate child results for this bucket. */
    AggregationResults getChildResults();

    /** Document count for this bucket on the emitting shard or partition. */
    long getDocCount();
  }

  // Non-sharded standalone output order: count desc, key-asc tiebreak (CLOUDP-419089), matching
  // the legacy string-facet top-N; see the tiebreak note in TermsAggregationResult#comparator.
  // Flat leaf terms on the sharded merge path apply the same order via $sort in
  // ShardMergePipelineBuilder; nested groupings still need per-level top-N in facet arms.
  private static final BucketOrder STANDALONE_BUCKET_ORDER = BucketOrder.COUNT_DESC;
  // Sharded intermediate wire order: only needs to be deterministic (mongod reconstructs buckets
  // with the shard merge pipeline).
  private static final BucketOrder INTERMEDIATE_BUCKET_ORDER = BucketOrder.KEY_ASC;

  /**
   * Return this result's buckets in the requested order, applied here at read time rather than
   * during merges: {@link AggregationResultBuilder} carries hash-based state across the whole fold,
   * so no intermediate results are materialized and nothing is sorted until buckets are read.
   * Public for tests only; production reads go through {@link #toStandaloneBson()} and {@link
   * #toIntermediateBson()}.
   */
  @VisibleForTesting
  public abstract List<? extends Bucket> getBuckets(BucketOrder order);

  @Override
  public BsonValue toStandaloneBson() {
    // Build bson document with the `buckets` array field.
    BsonArray bucketsBson = new BsonArray();
    for (Bucket bucket : this.getBuckets(STANDALONE_BUCKET_ORDER)) {
      BsonDocument bucketDoc = new BsonDocument();
      bucketDoc.append("_id", bucket.getKey());

      // DFS into next levels
      AggregationResults childResults = bucket.getChildResults();
      for (AggregationResult child : childResults) {
        bucketDoc.append(child.getName(), child.toStandaloneBson());
      }
      bucketsBson.add(bucketDoc);
    }
    return new BsonDocument("buckets", bucketsBson);
  }

  /**
   * Flattens nested buckets into one intermediate document per leaf bucket path. Each document
   * contains {@link IntermediateAggregationMetaField#KEYS} and mergeable metric partials; the
   * top-level routing field {@link IntermediateAggregationMetaField#GROUP_NAME} is stamped by
   * {@link TopLevelAggregationResult}.
   *
   * <p>Example — nested {@code by_city -> by_color -> avg_price} over two leaf buckets:
   *
   * <pre>{@code
   * // Nested child results (conceptual):
   * by_city.buckets[0] = {_id: "NYC", by_color: {buckets: [{_id: "red", avg_price: ...}]}}
   * by_city.buckets[1] = {_id: "LA",  by_color: {buckets: [{_id: "red", avg_price: ...}]}}
   *
   * // Flat intermediate rows emitted here (group_name added by TopLevelAggregationResult):
   * [
   *   {$meta/keys: {city: "NYC", color: "red"}, avg_sum_avg_price: 30, avg_count_avg_price: 2},
   *   {$meta/keys: {city: "LA",  color: "red"}, avg_sum_avg_price: 5,  avg_count_avg_price: 1}
   * ]
   * }</pre>
   */
  @Override
  public final BsonArray toIntermediateBson() {
    BsonArray docs = new BsonArray();
    for (Bucket bucket : this.getBuckets(INTERMEDIATE_BUCKET_ORDER)) {
      BsonDocument keys = new BsonDocument().append(this.path.toString(), bucket.getKey());
      collectBucketDocs(keys, bucket, docs);
    }
    return docs;
  }

  private static void collectBucketDocs(BsonDocument keys, Bucket bucket, BsonArray docs) {
    BsonDocument metricFields = new BsonDocument();
    List<BsonArray> subBucketDocs = new ArrayList<>();

    for (AggregationResult child : bucket.getChildResults()) {
      BsonArray childRows = child.toIntermediateBson();
      if (!childRows.isEmpty()) {
        subBucketDocs.add(childRows);
      }
      BsonDocument metricPartials = child.toIntermediateMetricPartials();
      if (!metricPartials.isEmpty()) {
        IntermediateAggregationMetaField.assertNoReservedFields(metricPartials);
        metricFields.putAll(metricPartials);
      }
    }

    // Leaf grouping level: every child is a metric result (empty toIntermediateBson(), partials
    // only). Nested group children would have contributed non-empty subBucketDocs wire rows.
    // When a nested group bucket has no leaf wire rows and no metric partials (e.g. an empty
    // boundary range with an empty inner terms child), omit it from the wire format entirely.
    // Leaf grouping buckets with metric children still emit even when all partials are absent.
    if (subBucketDocs.isEmpty()) {
      if (metricFields.isEmpty() && hasGroupChild(bucket)) {
        return;
      }
      docs.add(buildLeafIntermediateDoc(keys, metricFields, bucket.getDocCount()));
      return;
    }

    Check.checkState(
        metricFields.isEmpty(), "Metric operators must be defined at the leaf grouping level only");
    for (BsonArray subDocs : subBucketDocs) {
      mergeIntoSubDocs(keys, subDocs, docs);
    }
  }

  private static boolean hasGroupChild(Bucket bucket) {
    for (AggregationResult child : bucket.getChildResults()) {
      if (child instanceof MultiBucketAggregationResult) {
        return true;
      }
    }
    return false;
  }

  private static BsonDocument buildLeafIntermediateDoc(
      BsonDocument keys, BsonDocument metricFields, long docCount) {
    BsonDocument doc = new BsonDocument();
    doc.append(IntermediateAggregationMetaField.KEYS, keys);
    doc.append(IntermediateAggregationMetaField.BUCKET_DOC_COUNT, new org.bson.BsonInt64(docCount));
    doc.putAll(metricFields);
    return doc;
  }

  private static void mergeIntoSubDocs(BsonDocument parentKeys, BsonArray subDocs, BsonArray out) {
    for (BsonValue value : subDocs) {
      BsonDocument subDoc = value.asDocument();
      if (subDoc.containsKey(IntermediateAggregationMetaField.GROUP_NAME)) {
        throw new IllegalStateException(
            "Child intermediate document must not set routing field "
                + IntermediateAggregationMetaField.GROUP_NAME);
      }
      if (!subDoc.containsKey(IntermediateAggregationMetaField.KEYS)) {
        throw new IllegalStateException(
            "Child intermediate document must set grouping-keys field "
                + IntermediateAggregationMetaField.KEYS);
      }
      BsonDocument childKeys = subDoc.getDocument(IntermediateAggregationMetaField.KEYS);
      BsonDocument mergedKeys =
          IntermediateAggregationMetaField.mergeGroupingKeys(parentKeys, childKeys);
      subDoc.put(IntermediateAggregationMetaField.KEYS, mergedKeys);
      out.add(subDoc);
    }
  }
}
