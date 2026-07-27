package com.xgen.mongot.index.query.aggregation;

import static org.junit.Assert.assertEquals;

import com.xgen.mongot.index.query.aggregation.BoundaryAggregationResult.BoundaryBucket;
import com.xgen.mongot.index.query.aggregation.TermsAggregationResult.TermsBucket;
import com.xgen.mongot.util.FieldPath;
import java.util.List;
import org.apache.lucene.util.BytesRef;
import org.bson.BsonInt32;
import org.junit.Test;

/**
 * Unit tests for {@link AggregationResult#applyLimit()} -- the deferred numBuckets top-N
 * truncation. Ordering matches the Lucene string facet: doc count descending, ties by term
 * ascending.
 */
public class ApplyLimitTest extends AggregationTestSupport {

  private static final FieldPath CITY = FieldPath.parse("city");
  private static final FieldPath SIZE = FieldPath.parse("size");
  private static final FieldPath PRICE = FieldPath.parse("price");

  // Every assertion in this class checks the top-N presentation order (count desc, key asc),
  // which is applied at read time via getBuckets(BucketOrder.COUNT_DESC) rather than stored by
  // applyLimit().
  private static List<String> topBucketKeys(TermsAggregationResult result) {
    return bucketKeysInOrder(result, BucketOrder.COUNT_DESC);
  }

  private static TermsBucket termsBucket(String key, long docCount, AggregationResult... children) {
    return new TermsBucket(new BytesRef(key), docCount, AggregationResults.from(List.of(children)));
  }

  private static TermsAggregationResult terms(int limit, TermsBucket... buckets) {
    return new TermsAggregationResult("by_city", CITY, limit, List.of(buckets));
  }

  // ===== single-level truncation =====

  @Test
  public void truncatesToTopLimitByDocCountDescending() {
    TermsAggregationResult result =
        terms(
            2, termsBucket("a", 5), termsBucket("b", 10), termsBucket("c", 1), termsBucket("d", 8));
    assertEquals(List.of("b", "d"), topBucketKeys((TermsAggregationResult) result.applyLimit()));
  }

  @Test
  public void tiesBrokenByKeyAscending() {
    TermsAggregationResult result =
        terms(2, termsBucket("banana", 5), termsBucket("apple", 5), termsBucket("cherry", 5));
    // All tied on count 5 -> ascending key: apple, banana.
    assertEquals(
        List.of("apple", "banana"), topBucketKeys((TermsAggregationResult) result.applyLimit()));
  }

  @Test
  public void tieBreakEvictsAlreadyKeptBucket() {
    // banana(5) and cherry(5) fill the size-2 heap first; apple(5) then ties on count but has a
    // smaller key than the worst-kept (cherry), so it must EVICT cherry rather than be rejected.
    // Exercises the heap's tie-break eviction branch. Expected top-2 (count desc, key asc): apple,
    // banana.
    TermsAggregationResult result =
        terms(2, termsBucket("banana", 5), termsBucket("cherry", 5), termsBucket("apple", 5));
    assertEquals(
        List.of("apple", "banana"), topBucketKeys((TermsAggregationResult) result.applyLimit()));
  }

  @Test
  public void topKIndependentOfInputOrder() {
    // Same buckets in a shuffled order must yield the same top-2 and ordering: the heap selection
    // and drain order do not depend on insertion order.
    TermsBucket a = termsBucket("apple", 5);
    TermsBucket b = termsBucket("banana", 5);
    TermsBucket c = termsBucket("cherry", 3);
    TermsBucket d = termsBucket("date", 9);
    List<String> expected = List.of("date", "apple"); // date(9), then apple(5) before banana(5).
    assertEquals(
        expected, topBucketKeys((TermsAggregationResult) terms(2, a, b, c, d).applyLimit()));
    assertEquals(
        expected, topBucketKeys((TermsAggregationResult) terms(2, d, c, b, a).applyLimit()));
    assertEquals(
        expected, topBucketKeys((TermsAggregationResult) terms(2, b, d, a, c).applyLimit()));
  }

  @Test
  public void keepsAllButReordersWhenLimitExceedsBucketCount() {
    TermsAggregationResult result =
        terms(100, termsBucket("LA", 1), termsBucket("NYC", 2), termsBucket("SF", 2));
    // No truncation, but still ordered count desc / key asc: NYC(2), SF(2), LA(1).
    assertEquals(
        List.of("NYC", "SF", "LA"), topBucketKeys((TermsAggregationResult) result.applyLimit()));
  }

  @Test
  public void limitOfOneKeepsSingleHighestBucket() {
    TermsAggregationResult result =
        terms(1, termsBucket("NYC", 2), termsBucket("SF", 2), termsBucket("LA", 1));
    // NYC and SF tie on 2; key asc -> NYC wins.
    assertEquals(List.of("NYC"), topBucketKeys((TermsAggregationResult) result.applyLimit()));
  }

  @Test
  public void emptyBucketsStaysEmpty() {
    assertEquals(List.of(), topBucketKeys((TermsAggregationResult) terms(5).applyLimit()));
  }

  // ===== nested terms: limit applied per parent bucket =====

  @Test
  public void nestedTermsLimitAppliedIndependentlyPerParent() {
    TermsAggregationResult redSizes =
        new TermsAggregationResult(
            "by_size", SIZE, 1, List.of(bareBucket("S", 3), bareBucket("M", 1)));
    TermsAggregationResult blueSizes =
        new TermsAggregationResult(
            "by_size", SIZE, 1, List.of(bareBucket("L", 2), bareBucket("XL", 5)));

    TermsAggregationResult outer =
        terms(100, termsBucket("red", 4, redSizes), termsBucket("blue", 7, blueSizes));
    TermsAggregationResult limited = (TermsAggregationResult) outer.applyLimit();

    // Outer ordered by count desc: blue(7), red(4). Each parent keeps its OWN top-1 inner size.
    assertEquals(List.of("blue", "red"), topBucketKeys(limited));
    assertEquals(List.of("S"), topBucketKeys(childTerms(bucket(limited, "red"), "by_size")));
    assertEquals(List.of("XL"), topBucketKeys(childTerms(bucket(limited, "blue"), "by_size")));
  }

  // ===== metric children are untouched =====

  @Test
  public void metricChildResultsPreserved() {
    TermsAggregationResult result =
        terms(
            1,
            termsBucket("NYC", 2, new TestOperatorSpecs.SumResult("total", 42.0)),
            termsBucket("LA", 1, new TestOperatorSpecs.SumResult("total", 9.0)));
    TermsAggregationResult limited = (TermsAggregationResult) result.applyLimit();

    assertEquals(List.of("NYC"), topBucketKeys(limited));
    assertEquals(42.0, sumOf(bucket(limited, "NYC"), "total"), 1e-9);
  }

  // ===== container recursion =====

  @Test
  public void topLevelRecursesIntoChildren() {
    TopLevelAggregationResult top =
        new TopLevelAggregationResult(
            List.of(terms(1, termsBucket("NYC", 2), termsBucket("LA", 1))));
    TopLevelAggregationResult limited = (TopLevelAggregationResult) top.applyLimit();
    assertEquals(
        List.of("NYC"), topBucketKeys((TermsAggregationResult) limited.childResults().get(0)));
  }

  @Test
  public void boundaryDoesNotTruncateButRecursesIntoTermsChildren() {
    BoundaryAggregationResult boundary =
        new BoundaryAggregationResult(
            "by_price",
            PRICE,
            List.of(
                new BoundaryBucket(
                    new BsonInt32(0),
                    4,
                    AggregationResults.from(
                        List.of(terms(1, termsBucket("NYC", 3), termsBucket("LA", 1))))),
                new BoundaryBucket(
                    new BsonInt32(100),
                    3,
                    AggregationResults.from(
                        List.of(terms(1, termsBucket("SF", 2), termsBucket("SD", 1)))))));

    BoundaryAggregationResult limited = (BoundaryAggregationResult) boundary.applyLimit();

    // Boundary keeps all buckets (no top-N); nested terms under each are still limited.
    assertEquals(2, limited.getBuckets(BucketOrder.KEY_ASC).size());
    assertEquals(
        List.of("NYC"),
        topBucketKeys(
            (TermsAggregationResult)
                limited.getBuckets(BucketOrder.KEY_ASC).get(0).getChildResults().get("by_city")));
    assertEquals(
        List.of("SF"),
        topBucketKeys(
            (TermsAggregationResult)
                limited.getBuckets(BucketOrder.KEY_ASC).get(1).getChildResults().get("by_city")));
  }

  // ===== merge stays full-cardinality; truncation only happens in applyLimit =====

  @Test
  public void mergeKeepsFullCardinalityThenApplyLimitTruncates() {
    TermsAggregationResult left = terms(1, termsBucket("NYC", 2), termsBucket("LA", 3));
    TermsAggregationResult right = terms(1, termsBucket("NYC", 5), termsBucket("SF", 1));

    TermsAggregationResult merged = (TermsAggregationResult) merge(left, right);
    // Merge does NOT truncate to limit=1: all three keys survive with summed counts.
    assertEquals(3, merged.getBuckets(BucketOrder.KEY_ASC).size());
    assertEquals(7, bucket(merged, "NYC").getDocCount()); // 2 + 5

    // applyLimit then picks the single highest: NYC(7).
    assertEquals(List.of("NYC"), topBucketKeys((TermsAggregationResult) merged.applyLimit()));
  }

  private static TermsBucket bareBucket(String key, long docCount) {
    return new TermsBucket(new BytesRef(key), docCount, AggregationResults.from(List.of()));
  }
}
