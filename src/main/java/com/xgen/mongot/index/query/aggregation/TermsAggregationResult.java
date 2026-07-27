package com.xgen.mongot.index.query.aggregation;

import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.FieldPath;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.PriorityQueue;
import java.util.function.Function;
import java.util.stream.Collectors;
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
  // Max number of buckets. Carried through merges at full cardinality; applied exactly once by
  // applyLimit() at the outermost merge point.
  private final int limit;
  // Buckets keyed by term for the O(1) lookup merges need; ordering is applied at read time in
  // getBuckets(BucketOrder). Same shape in every scenario: a single (non-partitioned) reader's
  // result holds the buckets that reader counted; a Builder-built result holds the term-union
  // with summed counts.
  private final HashMap<BytesRef, TermsBucket> bucketsByKey;

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

    // Private on purpose: BytesRef is mutable and this returns the map-key instance, so exposing
    // it would let callers corrupt bucketsByKey. External readers use getKey().
    private BytesRef getBytesRefKey() {
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
    this.bucketsByKey =
        buckets.stream()
            .collect(
                Collectors.toMap(
                    TermsBucket::getBytesRefKey,
                    Function.identity(),
                    (a, b) -> {
                      throw new IllegalArgumentException("Duplicate bucket key: " + a.getKey());
                    },
                    () -> new HashMap<>(buckets.size())));
  }

  @Override
  public String getName() {
    return this.name;
  }

  @Override
  public List<? extends Bucket> getBuckets(BucketOrder order) {
    return this.bucketsByKey.values().stream().sorted(comparator(order)).toList();
  }

  private static Comparator<TermsBucket> comparator(BucketOrder order) {
    return switch (order) {
      // Ascending term order matches a single un-partitioned reader (Lucene SSDV ordinal order).
      case KEY_ASC -> Comparator.comparing(TermsBucket::getBytesRefKey);
      // Count ties break by BytesRef.compareTo (unsigned UTF-8), matching Lucene ordinal order --
      // intentionally NOT the legacy StringFacetCountComparator's UTF-16 String.compareTo, which
      // disagrees for supplementary-plane code points.
      case COUNT_DESC ->
          Comparator.comparingLong(TermsBucket::getDocCount)
              .reversed()
              .thenComparing(TermsBucket::getBytesRefKey);
    };
  }

  @Override
  public AggregationResultBuilder toBuilder() {
    return new Builder(this);
  }

  @Override
  public AggregationResult applyLimit() {
    // numBuckets means "keep the N most frequent terms", so selection is by COUNT_DESC regardless
    // of how survivors are later ordered by getBuckets(BucketOrder). Bounded min-heap: O(n log
    // keep) instead of a full sort, matching the legacy string-facet top-N (TopOrdAndIntQueue).
    Comparator<TermsBucket> topKOrder = comparator(BucketOrder.COUNT_DESC);
    int keep = Math.min(this.limit, this.bucketsByKey.size());
    // Head = worst-kept bucket (reversed order), so it can be evicted in O(log keep).
    PriorityQueue<TermsBucket> heap = new PriorityQueue<>(topKOrder.reversed());
    for (TermsBucket b : this.bucketsByKey.values()) {
      if (heap.size() < keep) {
        heap.add(b);
      } else if (keep > 0 && topKOrder.compare(b, heap.peek()) < 0) {
        // b ranks better than the current worst-kept (topKOrder treats "better" as smaller).
        heap.poll();
        heap.add(b);
      }
    }
    // Drain in any order (presentation order is applied at read time); recurse into survivors so
    // nested facets get their own limit.
    List<TermsBucket> kept = new ArrayList<>(heap.size());
    while (!heap.isEmpty()) {
      TermsBucket b = heap.poll();
      kept.add(
          new TermsBucket(b.getBytesRefKey(), b.getDocCount(), b.getChildResults().applyLimit()));
    }
    return new TermsAggregationResult(this.name, this.path, this.limit, kept);
  }

  /**
   * Unions buckets by term into one hash map carried across the whole fold, summing doc counts and
   * recursively merging children. Keeps full cardinality and never sorts; see {@link
   * AggregationResultBuilder}.
   */
  private static final class Builder implements AggregationResultBuilder {
    private final String name;
    private final FieldPath path;
    private final int limit;
    private final HashMap<BytesRef, BucketBuilder> bucketsByKey = new HashMap<>();

    // Identity element of the merge for `shape`'s (name, path, limit): holds no buckets until
    // add() folds results in, so the fold has no first-element special case.
    Builder(TermsAggregationResult shape) {
      this.name = shape.name;
      this.path = shape.path;
      this.limit = shape.limit;
    }

    @Override
    public void add(AggregationResult other) {
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
      fold(that);
    }

    private void fold(TermsAggregationResult result) {
      result
          .bucketsByKey
          .values()
          .forEach(
              bucket ->
                  this.bucketsByKey
                      .computeIfAbsent(bucket.getBytesRefKey(), BucketBuilder::new)
                      .add(bucket));
    }

    @Override
    public TermsAggregationResult build() {
      return new TermsAggregationResult(
          this.name,
          this.path,
          this.limit,
          this.bucketsByKey.values().stream().map(BucketBuilder::build).toList());
    }
  }

  // Per-bucket merge state: summed doc count plus one child builder per named child result.
  private static final class BucketBuilder {
    private final BytesRef key;
    private long docCount;
    private final AggregationResults.Builder childBuilders = new AggregationResults.Builder();

    BucketBuilder(BytesRef key) {
      this.key = key;
    }

    void add(TermsBucket bucket) {
      this.docCount += bucket.getDocCount();
      this.childBuilders.add(bucket.getChildResults());
    }

    TermsBucket build() {
      return new TermsBucket(this.key, this.docCount, this.childBuilders.build());
    }
  }
}
