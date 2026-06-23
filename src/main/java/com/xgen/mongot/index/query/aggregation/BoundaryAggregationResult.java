package com.xgen.mongot.index.query.aggregation;

import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.FieldPath;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.bson.BsonValue;

/**
 * Result node for numeric / date boundary bucketing. One bucket per range, plus optional default.
 *
 * <p>Buckets are positionally aligned across partitions (identical boundaries =&gt; identical
 * order), so the merge is a straight element-wise sum + recursive child merge -- no key union and
 * no truncation.
 */
public final class BoundaryAggregationResult extends MultiBucketAggregationResult {

  private final String name;
  private final List<BoundaryBucket> buckets;

  public static final class BoundaryBucket implements Bucket {
    // Lower-bound value of the range (BsonNumber / BsonDateTime), or the default bucket's label.
    private final BsonValue key;
    private final long docCount;
    private final AggregationResults childResults;

    BoundaryBucket(BsonValue key, long docCount, AggregationResults childResults) {
      this.key = key;
      this.docCount = docCount;
      this.childResults = childResults;
    }

    // Covariant override of Bucket.getKey(): Object.
    @Override
    public BsonValue getKey() {
      return this.key;
    }

    public long getDocCount() {
      return this.docCount;
    }

    @Override
    public AggregationResults getChildResults() {
      return this.childResults;
    }
  }

  public BoundaryAggregationResult(String name, FieldPath path, List<BoundaryBucket> buckets) {
    super(path);
    this.name = name;
    this.buckets = buckets;
  }

  @Override
  public String getName() {
    return this.name;
  }

  @Override
  public List<? extends Bucket> getBuckets() {
    return this.buckets;
  }

  @Override
  public AggregationResult mergePartitionResult(AggregationResult other) {
    BoundaryAggregationResult that = Check.instanceOf(other, BoundaryAggregationResult.class);
    if (!this.name.equals(that.name)) {
      throw new IllegalArgumentException(
          String.format(
              "Cannot merge boundary results with different names: %s and %s",
              this.name, that.name));
    }
    if (!this.path.equals(that.path)) {
      throw new IllegalArgumentException(
          String.format(
              "Cannot merge boundary results with different paths: %s and %s",
              this.path, that.path));
    }
    if (this.buckets.size() != that.buckets.size()) {
      throw new IllegalArgumentException(
          String.format(
              "Cannot merge boundary results with different bucket counts: %s and %s",
              this.buckets.size(), that.buckets.size()));
    }

    List<BoundaryBucket> merged = new ArrayList<>(this.buckets.size());
    for (int i = 0; i < this.buckets.size(); i++) {
      BoundaryBucket left = this.buckets.get(i);
      BoundaryBucket right = that.buckets.get(i);
      if (!Objects.equals(left.getKey(), right.getKey())) {
        throw new IllegalArgumentException(
            String.format(
                "Cannot merge boundary results with diverging bucket keys at %s: %s and %s",
                i, left.getKey(), right.getKey()));
      }
      merged.add(
          new BoundaryBucket(
              left.getKey(),
              left.getDocCount() + right.getDocCount(),
              mergeChildren(left.getChildResults(), right.getChildResults())));
    }
    return new BoundaryAggregationResult(this.name, this.path, merged);
  }

  // Element-wise merge of child results, matched by name (same spec tree on every partition).
  private AggregationResults mergeChildren(AggregationResults a, AggregationResults b) {
    Map<String, AggregationResult> leftByName = a.getResultsByName();
    Map<String, AggregationResult> rightByName = b.getResultsByName();
    if (!leftByName.keySet().equals(rightByName.keySet())) {
      throw new IllegalArgumentException(
          String.format(
              "Cannot merge child results with different names: %s and %s",
              leftByName.keySet(), rightByName.keySet()));
    }
    List<AggregationResult> result = new ArrayList<>();
    for (String childName : leftByName.keySet()) {
      result.add(leftByName.get(childName).mergePartitionResult(rightByName.get(childName)));
    }
    return AggregationResults.from(result);
  }
}
