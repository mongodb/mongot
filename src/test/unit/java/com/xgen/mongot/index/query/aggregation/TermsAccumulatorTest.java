package com.xgen.mongot.index.query.aggregation;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.xgen.mongot.index.lucene.field.FieldName.TypeField;
import com.xgen.mongot.index.lucene.util.TopLevelDocValues;
import com.xgen.mongot.index.query.aggregation.AggregationRegistry.GroupingArgs.TermsArgs;
import com.xgen.mongot.util.FieldPath;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.util.BytesRef;
import org.junit.Test;

/**
 * Drives the real {@link DenseTermsAccumulator} / {@link RemapTermsAccumulator} against an
 * in-memory token-field index, plus the {@link TermsAggregationResult} partition merge.
 */
public class TermsAccumulatorTest extends AggregationTestSupport {

  private static final List<TestDoc> DATASET =
      List.of(
          new TestDoc("NYC", "red", 10),
          new TestDoc("NYC", "blue", 20),
          new TestDoc("NYC", "red", 30),
          new TestDoc("LA", "blue", 5),
          new TestDoc("LA", "red", 15),
          new TestDoc("SF", "green", 7));

  private static TopAggregationSpec cityTermsWithSum(boolean isRoot, int limit) {
    AggregationSpec sum = new TestOperatorSpecs.SumSpec("sum_price", PRICE_LUCENE_FIELD);
    AggregationSpec byCity =
        new TermsGroupAggregationSpec(
            "by_city",
            FieldPath.parse(CITY_FIELD),
            List.of(sum),
            new TermsArgs(isRoot, TypeField.TOKEN, limit));
    return new TopAggregationSpec(List.of(byCity));
  }

  private static TermsAggregationResult firstTerms(Accumulator top) throws IOException {
    TopLevelAggregationResult result = (TopLevelAggregationResult) top.buildTopLevelResult();
    return (TermsAggregationResult) result.childResults().get(0);
  }

  @Test
  public void denseTerms_returnsAllBucketsWithCountsAndChildSums() throws Exception {
    buildIndex(DATASET, DATASET.size()); // single segment
    Accumulator top = collectAll(cityTermsWithSum(true, 100), newContext());

    assertTrue(top.children().get(0) instanceof DenseTermsAccumulator);

    AggregationResult cityResult =
        ((TopLevelAggregationResult) top.buildTopLevelResult()).childResults().get(0);
    assertTrue(cityResult instanceof TermsAggregationResult);
    TermsAggregationResult byCity = (TermsAggregationResult) cityResult;

    assertEquals(Set.of("NYC", "LA", "SF"), bucketKeys(byCity));
    // Dense enumerates buckets in global-ordinal order, which is term-ascending.
    assertEquals(List.of("LA", "NYC", "SF"), bucketKeysInOrder(byCity));

    assertEquals(3L, bucket(byCity, "NYC").getDocCount());
    assertEquals(2L, bucket(byCity, "LA").getDocCount());
    assertEquals(1L, bucket(byCity, "SF").getDocCount());

    assertEquals(60.0, sumOf(bucket(byCity, "NYC"), "sum_price"), 1e-9);
    assertEquals(20.0, sumOf(bucket(byCity, "LA"), "sum_price"), 1e-9);
    assertEquals(7.0, sumOf(bucket(byCity, "SF"), "sum_price"), 1e-9);
  }

  @Test
  public void remapTerms_returnsAllBucketsWithCountsAndChildSums() throws Exception {
    buildIndex(DATASET, DATASET.size()); // single segment
    Accumulator top = collectAll(cityTermsWithSum(false, 100), newContext());

    assertTrue(top.children().get(0) instanceof RemapTermsAccumulator);

    AggregationResult cityResult =
        ((TopLevelAggregationResult) top.buildTopLevelResult()).childResults().get(0);
    assertTrue(cityResult instanceof TermsAggregationResult);
    TermsAggregationResult byCity = (TermsAggregationResult) cityResult;

    // Remap enumerates hash entries -- no order guarantee, assert as a set.
    assertEquals(Set.of("NYC", "LA", "SF"), bucketKeys(byCity));

    assertEquals(3L, bucket(byCity, "NYC").getDocCount());
    assertEquals(2L, bucket(byCity, "LA").getDocCount());
    assertEquals(1L, bucket(byCity, "SF").getDocCount());

    assertEquals(60.0, sumOf(bucket(byCity, "NYC"), "sum_price"), 1e-9);
    assertEquals(20.0, sumOf(bucket(byCity, "LA"), "sum_price"), 1e-9);
    assertEquals(7.0, sumOf(bucket(byCity, "SF"), "sum_price"), 1e-9);
  }

  @Test
  public void termsAccumulator_usesCachedTopLevelValuesFromContext() throws Exception {
    buildIndex(DATASET, 2); // 3 segments -> the cached view carries a real OrdinalMap
    List<String> requestedFields = new ArrayList<>();
    AggregationContext.CachedGlobalValueLookup lookup =
        luceneField -> {
          requestedFields.add(luceneField);
          return Optional.of(
              TopLevelDocValues.getDocValues(this.reader, luceneField, Optional.empty()));
        };

    Accumulator top = collectAll(cityTermsWithSum(true, 100), new TestContext(this.reader, lookup));

    assertEquals(List.of(CITY_LUCENE_FIELD), requestedFields);
    TermsAggregationResult byCity = firstTerms(top);
    assertEquals(Set.of("NYC", "LA", "SF"), bucketKeys(byCity));
    assertEquals(3L, bucket(byCity, "NYC").getDocCount());
    assertEquals(2L, bucket(byCity, "LA").getDocCount());
    assertEquals(1L, bucket(byCity, "SF").getDocCount());
    assertEquals(60.0, sumOf(bucket(byCity, "NYC"), "sum_price"), 1e-9);
    assertEquals(20.0, sumOf(bucket(byCity, "LA"), "sum_price"), 1e-9);
    assertEquals(7.0, sumOf(bucket(byCity, "SF"), "sum_price"), 1e-9);
  }

  @Test
  public void denseTerms_multiSegmentUsesGlobalOrdinals() throws Exception {
    buildIndex(DATASET, 2); // 3 segments -> exercises the cross-segment OrdinalMap
    Accumulator top = collectAll(cityTermsWithSum(true, 100), newContext());

    assertTrue(top.children().get(0) instanceof DenseTermsAccumulator);

    TermsAggregationResult byCity = firstTerms(top);
    assertEquals(Set.of("NYC", "LA", "SF"), bucketKeys(byCity));
    assertEquals(3L, bucket(byCity, "NYC").getDocCount());
    assertEquals(2L, bucket(byCity, "LA").getDocCount());
    assertEquals(1L, bucket(byCity, "SF").getDocCount());
    assertEquals(60.0, sumOf(bucket(byCity, "NYC"), "sum_price"), 1e-9);
    assertEquals(20.0, sumOf(bucket(byCity, "LA"), "sum_price"), 1e-9);
    assertEquals(7.0, sumOf(bucket(byCity, "SF"), "sum_price"), 1e-9);
  }

  @Test
  public void remapTerms_multiSegmentUsesGlobalOrdinals() throws Exception {
    buildIndex(DATASET, 2); // 3 segments
    Accumulator top = collectAll(cityTermsWithSum(false, 100), newContext());

    assertTrue(top.children().get(0) instanceof RemapTermsAccumulator);

    TermsAggregationResult byCity = firstTerms(top);
    assertEquals(Set.of("NYC", "LA", "SF"), bucketKeys(byCity));
    assertEquals(3L, bucket(byCity, "NYC").getDocCount());
    assertEquals(2L, bucket(byCity, "LA").getDocCount());
    assertEquals(1L, bucket(byCity, "SF").getDocCount());
    assertEquals(60.0, sumOf(bucket(byCity, "NYC"), "sum_price"), 1e-9);
    assertEquals(20.0, sumOf(bucket(byCity, "LA"), "sum_price"), 1e-9);
    assertEquals(7.0, sumOf(bucket(byCity, "SF"), "sum_price"), 1e-9);
  }

  @Test
  public void twoFieldTerms_denseOuterRemapInner() throws Exception {
    buildIndex(DATASET, DATASET.size());

    AggregationSpec sum = new TestOperatorSpecs.SumSpec("sum_price", PRICE_LUCENE_FIELD);
    AggregationSpec byColor =
        new TermsGroupAggregationSpec(
            "by_color",
            FieldPath.parse(COLOR_FIELD),
            List.of(sum),
            new TermsArgs(false, TypeField.TOKEN, 100));
    AggregationSpec byCity =
        new TermsGroupAggregationSpec(
            "by_city",
            FieldPath.parse(CITY_FIELD),
            List.of(byColor),
            new TermsArgs(true, TypeField.TOKEN, 100));
    Accumulator top = collectAll(new TopAggregationSpec(List.of(byCity)), newContext());

    Accumulator outer = top.children().get(0);
    assertTrue(outer instanceof DenseTermsAccumulator);
    assertTrue(outer.children().get(0) instanceof RemapTermsAccumulator);

    TermsAggregationResult byCityResult = firstTerms(top);
    assertEquals(Set.of("NYC", "LA", "SF"), bucketKeys(byCityResult));

    TermsAggregationResult nyc = childTerms(bucket(byCityResult, "NYC"), "by_color");
    assertEquals(Set.of("red", "blue"), bucketKeys(nyc));
    assertEquals(2L, bucket(nyc, "red").getDocCount());
    assertEquals(40.0, sumOf(bucket(nyc, "red"), "sum_price"), 1e-9); // 10 + 30
    assertEquals(1L, bucket(nyc, "blue").getDocCount());
    assertEquals(20.0, sumOf(bucket(nyc, "blue"), "sum_price"), 1e-9);

    TermsAggregationResult la = childTerms(bucket(byCityResult, "LA"), "by_color");
    assertEquals(Set.of("blue", "red"), bucketKeys(la));
    assertEquals(5.0, sumOf(bucket(la, "blue"), "sum_price"), 1e-9);
    assertEquals(15.0, sumOf(bucket(la, "red"), "sum_price"), 1e-9);

    TermsAggregationResult sf = childTerms(bucket(byCityResult, "SF"), "by_color");
    assertEquals(Set.of("green"), bucketKeys(sf));
    assertEquals(7.0, sumOf(bucket(sf, "green"), "sum_price"), 1e-9);
  }

  @Test
  public void denseTerms_emptyWhenNoMatches() throws Exception {
    buildIndex(DATASET, DATASET.size());
    Accumulator top = collect(cityTermsWithSum(true, 100), newContext(), new MatchNoDocsQuery());
    assertTrue(firstTerms(top).getBuckets().isEmpty());
  }

  @Test
  public void remapTerms_emptyWhenNoMatches() throws Exception {
    buildIndex(DATASET, DATASET.size());
    Accumulator top = collect(cityTermsWithSum(false, 100), newContext(), new MatchNoDocsQuery());
    assertTrue(firstTerms(top).getBuckets().isEmpty());
  }

  @Test
  public void merge_unionsTwoLevelTermsSummingCountsAndChildren() {
    // Each partition is a two-level result: group by_city, then nested group by_color, with a
    // $sum leaf at the bottom. Merging must union at BOTH levels.
    TermsAggregationResult left =
        byCity(
            cityBucket("NYC", 3, colorBucket("red", 2, 40.0), colorBucket("blue", 1, 20.0)),
            cityBucket("LA", 2, colorBucket("blue", 1, 5.0), colorBucket("red", 1, 15.0)));
    TermsAggregationResult right =
        byCity(
            cityBucket("NYC", 2, colorBucket("red", 1, 5.0), colorBucket("green", 1, 8.0)),
            cityBucket("SEA", 1, colorBucket("blue", 1, 9.0)));

    TermsAggregationResult merged = (TermsAggregationResult) left.mergePartitionResult(right);

    // Top level: full city union, LinkedHashMap order = left buckets then new right keys.
    assertEquals(List.of("NYC", "LA", "SEA"), bucketKeysInOrder(merged));
    assertEquals(5L, bucket(merged, "NYC").getDocCount()); // 3 + 2
    assertEquals(2L, bucket(merged, "LA").getDocCount());
    assertEquals(1L, bucket(merged, "SEA").getDocCount());

    // NYC's nested by_color merged: red present on both sides (counts + sums summed),
    // blue only on left, green only on right.
    TermsAggregationResult nycColors = childTerms(bucket(merged, "NYC"), "by_color");
    assertEquals(Set.of("red", "blue", "green"), bucketKeys(nycColors));
    assertEquals(3L, bucket(nycColors, "red").getDocCount()); // 2 + 1
    assertEquals(45.0, sumOf(bucket(nycColors, "red"), "sum_price"), 1e-9); // 40 + 5
    assertEquals(1L, bucket(nycColors, "blue").getDocCount());
    assertEquals(20.0, sumOf(bucket(nycColors, "blue"), "sum_price"), 1e-9);
    assertEquals(1L, bucket(nycColors, "green").getDocCount());
    assertEquals(8.0, sumOf(bucket(nycColors, "green"), "sum_price"), 1e-9);

    // LA only in left -> nested colors carried through unchanged.
    TermsAggregationResult laColors = childTerms(bucket(merged, "LA"), "by_color");
    assertEquals(Set.of("blue", "red"), bucketKeys(laColors));
    assertEquals(5.0, sumOf(bucket(laColors, "blue"), "sum_price"), 1e-9);
    assertEquals(15.0, sumOf(bucket(laColors, "red"), "sum_price"), 1e-9);

    // SEA only in right.
    TermsAggregationResult seaColors = childTerms(bucket(merged, "SEA"), "by_color");
    assertEquals(Set.of("blue"), bucketKeys(seaColors));
    assertEquals(9.0, sumOf(bucket(seaColors, "blue"), "sum_price"), 1e-9);
  }

  private static TermsAggregationResult.TermsBucket colorBucket(
      String color, long docCount, double sum) {
    AggregationResults children =
        AggregationResults.from(List.of(new TestOperatorSpecs.SumResult("sum_price", sum)));
    return new TermsAggregationResult.TermsBucket(new BytesRef(color), docCount, children);
  }

  private static TermsAggregationResult.TermsBucket cityBucket(
      String city, long docCount, TermsAggregationResult.TermsBucket... colorBuckets) {
    TermsAggregationResult byColor =
        new TermsAggregationResult(
            "by_color", FieldPath.parse(COLOR_FIELD), 100, List.of(colorBuckets));
    AggregationResults children = AggregationResults.from(List.of(byColor));
    return new TermsAggregationResult.TermsBucket(new BytesRef(city), docCount, children);
  }

  private static TermsAggregationResult byCity(TermsAggregationResult.TermsBucket... cityBuckets) {
    return new TermsAggregationResult(
        "by_city", FieldPath.parse(CITY_FIELD), 100, List.of(cityBuckets));
  }
}
