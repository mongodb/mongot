package com.xgen.mongot.index.query.aggregation;

import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.FieldPath;
import java.util.List;
import java.util.Objects;
import java.util.stream.IntStream;
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
  public List<? extends Bucket> getBuckets(BucketOrder order) {
    // Boundary buckets have a fixed positional order (one per configured range, plus optional
    // default); the requested order does not apply.
    return this.buckets;
  }

  @Override
  public AggregationResultBuilder toBuilder() {
    return new Builder(this);
  }

  @Override
  public AggregationResult applyLimit() {
    // Boundary buckets are fixed by the request's ranges -- no top-N truncation. Recurse so any
    // nested terms facets get their limit applied.
    return new BoundaryAggregationResult(
        this.name,
        this.path,
        this.buckets.stream()
            .map(
                b ->
                    new BoundaryBucket(
                        b.getKey(), b.getDocCount(), b.getChildResults().applyLimit()))
            .toList());
  }

  /**
   * Merge state for boundary results: buckets are positionally aligned, so each position sums doc
   * counts and recursively merges children via its own child builders.
   */
  private static final class Builder implements AggregationResultBuilder {
    private final String name;
    private final FieldPath path;
    private final List<BucketBuilder> bucketBuilders;

    // Identity element of the merge for `shape`'s (name, path, boundary keys): all bucket
    // builders start at zero until add() folds results in, so the fold has no first-element
    // special case. Boundary keys are fixed by the request, so the skeleton comes from the shape.
    Builder(BoundaryAggregationResult shape) {
      this.name = shape.name;
      this.path = shape.path;
      this.bucketBuilders =
          shape.buckets.stream().map(bucket -> new BucketBuilder(bucket.getKey())).toList();
    }

    @Override
    public void add(AggregationResult other) {
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
      if (this.bucketBuilders.size() != that.buckets.size()) {
        throw new IllegalArgumentException(
            String.format(
                "Cannot merge boundary results with different bucket counts: %s and %s",
                this.bucketBuilders.size(), that.buckets.size()));
      }
      IntStream.range(0, this.bucketBuilders.size())
          .forEach(i -> this.bucketBuilders.get(i).add(i, that.buckets.get(i)));
    }

    @Override
    public BoundaryAggregationResult build() {
      return new BoundaryAggregationResult(
          this.name, this.path, this.bucketBuilders.stream().map(BucketBuilder::build).toList());
    }
  }

  // Per-bucket merge state: summed doc count plus one child builder per named child result.
  private static final class BucketBuilder {
    private final BsonValue key;
    private long docCount;
    private final AggregationResults.Builder childBuilders = new AggregationResults.Builder();

    BucketBuilder(BsonValue key) {
      this.key = key;
    }

    void add(int position, BoundaryBucket bucket) {
      if (!Objects.equals(this.key, bucket.getKey())) {
        throw new IllegalArgumentException(
            String.format(
                "Cannot merge boundary results with diverging bucket keys at %s: %s and %s",
                position, this.key, bucket.getKey()));
      }
      this.docCount += bucket.getDocCount();
      this.childBuilders.add(bucket.getChildResults());
    }

    BoundaryBucket build() {
      return new BoundaryBucket(this.key, this.docCount, this.childBuilders.build());
    }
  }
}
