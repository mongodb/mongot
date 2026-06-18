package com.xgen.mongot.index.query.aggregation;

import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.FieldPath;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.lucene.util.BytesRef;

/**
 * Aggregation result for token field.
 *
 * <p>It is an intermediate aggregation result node with multiple buckets. Each bucket has a
 * unique string value</p>
 */
public final class TermsAggregationResult extends MultiBucketAggregationResult {

  private final String name;
  // Max number of buckets this result may hold. Carried through merges but NOT applied here yet;
  // the top-N truncation is handled during mongot integration once the merge topology is known.
  private final int limit;
  private final List<TermsBucket> buckets;

  public static final class TermsBucket implements Bucket {
    private final BytesRef key;
    private final long docCount;
    private final AggregationResults childResults;

    TermsBucket(BytesRef key, long docCount, AggregationResults childResults) {
      this.key = key;
      this.docCount = docCount;
      this.childResults = childResults;
    }

    @Override
    public BytesRef getKey() {
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

  public TermsAggregationResult(
      String name, FieldPath path, int limit, List<TermsBucket> buckets) {
    super(path);
    this.name = name;
    this.limit = limit;
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
    TermsAggregationResult that = Check.instanceOf(other, TermsAggregationResult.class);
    if (!this.name.equals(that.name)) {
      throw new IllegalArgumentException(
          String.format(
              "Cannot merge aggregation results with different group name: %s and %s",
              this.name, that.name));
    }

    if (!this.path.equals(that.path)) {
      throw new IllegalArgumentException(
          String.format(
              "Cannot merge aggregation results with different paths: %s and %s",
              this.path, that.path));
    }

    if (this.limit != that.limit) {
      throw new IllegalArgumentException(
          String.format(
              "Cannot merge aggregation results with different limits: %s and %s",
              this.limit, that.limit));
    }

    // Group buckets across both sides by the key, summing doc counts and recursively merging
    // children. Full cardinality is retained (no top-N truncation) so that chained segment /
    // partition / shard merges stay exact; the `limit` is applied later during mongot integration.
    // LinkedHashMap keeps the iteration order deterministic.
    LinkedHashMap<BytesRef, TermsBucket> mergedBuckets = new LinkedHashMap<>();
    for (TermsBucket b : this.buckets) {
      mergedBuckets.put(b.getKey(), b);
    }
    for (TermsBucket b : that.buckets) {
      mergedBuckets.merge(b.getKey(), b, (b1, b2) -> {
        AggregationResults mergedChildren =
            mergeChildren(b1.getChildResults(), b2.getChildResults());
        return new TermsBucket(b1.getKey(), b1.getDocCount() + b2.getDocCount(), mergedChildren);
      });
    }

    return new TermsAggregationResult(
        this.name, this.path, this.limit, List.copyOf(mergedBuckets.values()));
  }

  // Element-wise merge of two child AggregationResults. Each side must contain the same set of
  // child names (same spec tree on every partition), matched by name and merged via each child's
  // own AggregationResult.mergePartitionResult().
  private AggregationResults mergeChildren(AggregationResults a, AggregationResults b) {
    Map<String, AggregationResult> leftByName = a.getResultsByName();
    Map<String, AggregationResult> rightByName = b.getResultsByName();
    if (!leftByName.keySet().equals(rightByName.keySet())) {
      throw new IllegalArgumentException(
          String.format(
              "Cannot merge aggregation results with different child names: %s and %s",
              leftByName.keySet(), rightByName.keySet()));
    }
    List<AggregationResult> merged = new ArrayList<>();
    for (String name : leftByName.keySet()) {
      merged.add(leftByName.get(name).mergePartitionResult(rightByName.get(name)));
    }
    return AggregationResults.from(merged);
  }
}
