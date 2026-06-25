package com.xgen.mongot.index.query.aggregation;

import com.xgen.mongot.util.FieldPath;
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
  }

  public abstract List<? extends Bucket> getBuckets();

  @Override
  public BsonValue toStandaloneBson() {
    // Build bson document with the `buckets` array field.
    BsonArray bucketsBson = new BsonArray();
    for (Bucket bucket : this.getBuckets()) {
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
}
