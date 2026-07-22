package com.xgen.mongot.index.query.aggregation;

import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.FieldPath;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import org.apache.lucene.util.BytesRef;
import org.bson.BsonString;

/**
 * Aggregation result for token field.
 *
 * <p>It is an intermediate aggregation result node with multiple buckets. Each bucket has a unique
 * string value
 */
public final class TermsAggregationResult extends MultiBucketAggregationResult {

  private final String name;
  // Max number of buckets this result may hold. Carried through merges (kept at full cardinality)
  // and applied exactly once by applyLimit() at the outermost merge point.
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
    public BsonString getKey() {
      return new BsonString(this.key.utf8ToString());
    }

    public BytesRef getBytesRefKey() {
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

  public TermsAggregationResult(String name, FieldPath path, int limit, List<TermsBucket> buckets) {
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
    // partition / shard merges stay exact; the `limit` is applied exactly once by applyLimit()
    // at the outermost merge point. LinkedHashMap keeps the iteration order deterministic.
    LinkedHashMap<BytesRef, TermsBucket> mergedBuckets = new LinkedHashMap<>();
    for (TermsBucket b : this.buckets) {
      mergedBuckets.put(b.getBytesRefKey(), b);
    }
    for (TermsBucket b : that.buckets) {
      mergedBuckets.merge(
          b.getBytesRefKey(),
          b,
          (b1, b2) -> {
            AggregationResults mergedChildren =
                mergeChildren(b1.getChildResults(), b2.getChildResults());
            return new TermsBucket(
                b1.getBytesRefKey(), b1.getDocCount() + b2.getDocCount(), mergedChildren);
          });
    }

    return new TermsAggregationResult(
        this.name, this.path, this.limit, List.copyOf(mergedBuckets.values()));
  }

  // Order buckets by descending doc count, ties broken by ascending key. BytesRef.compareTo is
  // unsigned lexicographic, which equals term (and global-ordinal) ascending -- matching the
  // existing Lucene string-facet top-N ordering (TopOrdAndIntQueue: largest value, then smallest
  // ordinal).
  private static final Comparator<TermsBucket> TOP_K_ORDER =
      Comparator.comparingLong(TermsBucket::getDocCount)
          .reversed()
          .thenComparing(TermsBucket::getBytesRefKey);

  @Override
  public AggregationResult applyLimit() {
    // Select the top `keep` buckets by (count desc, key asc) with a bounded min-heap, matching the
    // legacy string-facet top-N (TopOrdAndIntQueue). The heap holds at most `keep` buckets with the
    // worst-kept at its head, so each of the n buckets costs O(log keep) -- O(n log keep) overall,
    // beating a full O(n log n) sort when numBuckets is well below the bucket cardinality. This is
    // the single, outermost application of the deferred limit; counts here are already global (see
    // the contract on AggregationResult#applyLimit).
    int keep = Math.min(this.limit, this.buckets.size());
    // Head = worst-kept bucket (reversed order), so it can be evicted in O(log keep).
    PriorityQueue<TermsBucket> heap = new PriorityQueue<>(TOP_K_ORDER.reversed());
    for (TermsBucket b : this.buckets) {
      if (heap.size() < keep) {
        heap.add(b);
      } else if (keep > 0 && TOP_K_ORDER.compare(b, heap.peek()) < 0) {
        // b ranks better than the current worst-kept (TOP_K_ORDER treats "better" as smaller).
        heap.poll();
        heap.add(b);
      }
    }
    // The min-heap drains worst-first, so fill back-to-front to get the required best-first output
    // order (count desc, key asc). Recurse into survivors so nested facets get their own limit.
    TermsBucket[] ordered = new TermsBucket[heap.size()];
    for (int i = ordered.length - 1; i >= 0; i--) {
      TermsBucket b = heap.poll();
      ordered[i] =
          new TermsBucket(b.getBytesRefKey(), b.getDocCount(), b.getChildResults().applyLimit());
    }
    return new TermsAggregationResult(this.name, this.path, this.limit, List.of(ordered));
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
