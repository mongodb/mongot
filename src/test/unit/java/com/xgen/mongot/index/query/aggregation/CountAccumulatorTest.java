package com.xgen.mongot.index.query.aggregation;

import static org.junit.Assert.assertEquals;

import com.xgen.mongot.index.lucene.field.FieldName.TypeField;
import com.xgen.mongot.index.query.aggregation.AggregationRegistry.GroupingArgs.TermsArgs;
import com.xgen.mongot.index.query.aggregation.MultiBucketAggregationResult.Bucket;
import com.xgen.mongot.util.FieldPath;
import java.util.List;
import java.util.Set;
import org.bson.BsonInt64;
import org.junit.Test;

public class CountAccumulatorTest extends AggregationTestSupport {

  private static final String COUNT_NAME = "cnt";

  private static TopAggregationSpec cityTermsWithCount(AggregationSpec... extraMetrics) {
    List<AggregationSpec> metrics =
        java.util.stream.Stream.concat(
                java.util.stream.Stream.of(new CountAggregationSpec(COUNT_NAME)),
                java.util.Arrays.stream(extraMetrics))
            .toList();
    AggregationSpec byCity =
        new TermsGroupAggregationSpec(
            "by_city",
            FieldPath.parse(CITY_FIELD),
            metrics,
            new TermsArgs(true, TypeField.TOKEN, Integer.MAX_VALUE));
    return new TopAggregationSpec(List.of(byCity));
  }

  private static TermsAggregationResult firstTerms(Accumulator top) throws java.io.IOException {
    TopLevelAggregationResult result = (TopLevelAggregationResult) top.buildTopLevelResult();
    return (TermsAggregationResult) result.childResults().get(0);
  }

  private static long countIn(TermsAggregationResult byCity, String city) {
    CountAggregationResult result =
        (CountAggregationResult) bucket(byCity, city).getChildResults().get(COUNT_NAME);
    return result.getValue();
  }

  @Test
  public void countsDocumentsPerBucket() throws Exception {
    buildIndex(
        List.of(
            new TestDoc("NYC", "x", 10),
            new TestDoc("NYC", "x", 20),
            new TestDoc("NYC", "x", 30),
            new TestDoc("LA", "x", 5),
            new TestDoc("LA", "x", 15)),
        Integer.MAX_VALUE);

    TermsAggregationResult byCity =
        firstTerms(collectAll(cityTermsWithCount(), newContextWithFieldResolver()));

    assertEquals(3L, countIn(byCity, "NYC"));
    assertEquals(2L, countIn(byCity, "LA"));
  }

  /** A count metric must agree with the bucket doc count the grouping level already reports. */
  @Test
  public void matchesBucketDocCount() throws Exception {
    buildIndex(
        List.of(
            new TestDoc("NYC", "x", 10), new TestDoc("NYC", "x", 20), new TestDoc("LA", "x", 5)),
        Integer.MAX_VALUE);

    TermsAggregationResult byCity =
        firstTerms(collectAll(cityTermsWithCount(), newContextWithFieldResolver()));

    for (Bucket bucket : byCity.getBuckets(BucketOrder.KEY_ASC)) {
      CountAggregationResult count =
          (CountAggregationResult) bucket.getChildResults().get(COUNT_NAME);
      assertEquals(bucket.getDocCount(), count.getValue());
    }
  }

  /**
   * The semantic that separates count from the other additive metrics: one document with two values
   * for the group field lands in two buckets and counts once in each, where {@code sum} drops it to
   * avoid double-counting a single measurement. Pinned together in one test so the contrast cannot
   * drift.
   */
  @Test
  public void multiValuedGroupKey_countsDocInEveryBucketWhereSumSkipsIt() throws Exception {
    buildIndexWithMultiValueCityDoc(100);

    TermsAggregationResult byCity =
        firstTerms(
            collectAll(
                cityTermsWithCount(
                    new SumAggregationSpec("sum_price", FieldPath.parse(PRICE_FIELD))),
                newContextWithFieldResolver()));

    assertEquals(Set.of("LA", "NYC"), bucketKeys(byCity));
    for (String city : Set.of("LA", "NYC")) {
      assertEquals("count includes the fanned-out doc in " + city, 1L, countIn(byCity, city));
      SumAggregationResult sum =
          (SumAggregationResult) bucket(byCity, city).getChildResults().get("sum_price");
      assertEquals("sum skips the fanned-out doc in " + city, 0.0, sum.getValue(), 1e-9);
      assertEquals(1L, bucket(byCity, city).getDocCount());
    }
  }

  @Test
  public void mergeAddsCountsAcrossPartitions() {
    AggregationResult merged =
        merge(
            new CountAggregationResult(COUNT_NAME, 4L), new CountAggregationResult(COUNT_NAME, 7L));
    assertEquals(11L, ((CountAggregationResult) merged).getValue());
  }

  @Test
  public void standaloneBsonIsInt64() {
    assertEquals(
        new BsonInt64(42L), new CountAggregationResult(COUNT_NAME, 42L).toStandaloneBson());
  }

  /** The partial must stay prefixed so it never collides with the row's own bucket doc count. */
  @Test
  public void intermediatePartialIsPrefixed() {
    assertEquals(
        new BsonInt64(9L),
        new CountAggregationResult(COUNT_NAME, 9L).toIntermediateMetricPartials().get("count_cnt"));
    assertEquals(
        "count metric partial must not be the unprefixed bucket doc-count field",
        false,
        new CountAggregationResult(COUNT_NAME, 9L)
            .toIntermediateMetricPartials()
            .containsKey(IntermediateAggregationMetaField.BUCKET_DOC_COUNT));
  }
}
