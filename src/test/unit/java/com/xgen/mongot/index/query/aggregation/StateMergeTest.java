package com.xgen.mongot.index.query.aggregation;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.xgen.mongot.index.lucene.field.FieldName.TypeField;
import com.xgen.mongot.index.query.aggregation.AggregationRegistry.GroupingArgs.TermsArgs;
import com.xgen.mongot.util.FieldPath;
import java.io.IOException;
import java.util.List;
import java.util.Set;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.bson.BsonInt64;
import org.junit.Test;

/**
 * Two trees built from the same spec against the same reader, collected over disjoint docs, must
 * match whether their state is merged by ordinal or their results are folded by term.
 */
public class StateMergeTest extends AggregationTestSupport {

  private static final List<TestDoc> DATASET =
      List.of(
          new TestDoc("NYC", "red", 10),
          new TestDoc("NYC", "blue", 20),
          new TestDoc("NYC", "red", 30),
          new TestDoc("LA", "blue", 5),
          new TestDoc("LA", "red", 15),
          new TestDoc("SF", "green", 7));

  // LOW matches prices 10, 5, 7. HIGH matches 20, 30, 15.
  private static final Query LOW_PRICES =
      SortedNumericDocValuesField.newSlowRangeQuery(PRICE_LUCENE_FIELD, Long.MIN_VALUE, 10);
  private static final Query HIGH_PRICES =
      SortedNumericDocValuesField.newSlowRangeQuery(PRICE_LUCENE_FIELD, 11, Long.MAX_VALUE);

  private static TopAggregationSpec cityTermsWith(AggregationSpec... metrics) {
    return new TopAggregationSpec(
        List.of(
            new TermsGroupAggregationSpec(
                "by_city",
                FieldPath.parse(CITY_FIELD),
                List.of(metrics),
                new TermsArgs(/* isRoot= */ true, TypeField.TOKEN, 100))));
  }

  private static TermsAggregationResult terms(AggregationResult top) {
    return (TermsAggregationResult) ((TopLevelAggregationResult) top).childResults().get(0);
  }

  private record MergedPair(AggregationResult byState, AggregationResult byFold) {}

  private MergedPair mergeBothWays(TopAggregationSpec spec, AggregationContext context)
      throws IOException {
    Accumulator left = collect(spec, context, LOW_PRICES);
    Accumulator right = collect(spec, context, HIGH_PRICES);

    // Fold first. mergeStateFrom mutates left.
    AggregationResult byFold =
        mergePartitionResults(left.buildTopLevelResult(), right.buildTopLevelResult());

    assertTrue(left.supportsStateMerge());
    left.mergeStateFrom(right);
    return new MergedPair(left.buildTopLevelResult(), byFold);
  }

  @Test
  public void mergeStateFrom_avgUnderDenseTerms_matchesResultFold() throws Exception {
    buildIndex(DATASET, 2);
    MergedPair merged =
        mergeBothWays(
            cityTermsWith(new AvgAggregationSpec("avg_price", FieldPath.parse(PRICE_FIELD))),
            newContextWithFieldResolver());

    TermsAggregationResult byCity = terms(merged.byState());
    assertEquals(Set.of("NYC", "LA", "SF"), bucketKeys(byCity));
    assertEquals(3L, bucket(byCity, "NYC").getDocCount());
    assertEquals(2L, bucket(byCity, "LA").getDocCount());
    assertEquals(1L, bucket(byCity, "SF").getDocCount());
    assertEquals(
        bucket(terms(merged.byFold()), "NYC").getChildResults().get("avg_price").toStandaloneBson(),
        bucket(byCity, "NYC").getChildResults().get("avg_price").toStandaloneBson());
    assertEquals(
        bucket(terms(merged.byFold()), "LA").getChildResults().get("avg_price").toStandaloneBson(),
        bucket(byCity, "LA").getChildResults().get("avg_price").toStandaloneBson());
  }

  @Test
  public void mergeStateFrom_allMergeableMetricsUnderDenseTerms_matchesResultFold()
      throws Exception {
    buildIndex(DATASET, 2);
    MergedPair merged =
        mergeBothWays(
            cityTermsWith(
                new CountAggregationSpec("count"),
                new SumAggregationSpec("sum_price", FieldPath.parse(PRICE_FIELD)),
                new AvgAggregationSpec("avg_price", FieldPath.parse(PRICE_FIELD)),
                new MinAggregationSpec("min_price", FieldPath.parse(PRICE_FIELD)),
                new MaxAggregationSpec("max_price", FieldPath.parse(PRICE_FIELD))),
            newContextWithFieldResolver());

    TermsAggregationResult byState = terms(merged.byState());
    TermsAggregationResult byFold = terms(merged.byFold());
    assertEquals(bucketKeys(byFold), bucketKeys(byState));
    for (String city : bucketKeys(byFold)) {
      assertEquals(bucket(byFold, city).getDocCount(), bucket(byState, city).getDocCount());
      for (String metric : List.of("count", "sum_price", "avg_price", "min_price", "max_price")) {
        assertEquals(
            bucket(byFold, city).getChildResults().get(metric).toStandaloneBson(),
            bucket(byState, city).getChildResults().get(metric).toStandaloneBson());
      }
    }
  }

  @Test
  public void mergeStateFrom_minSeenOnOneSideOnly_keepsThatExtremum() throws Exception {
    buildIndex(DATASET, 2);
    AggregationContext context = newContextWithFieldResolver();
    TopAggregationSpec spec =
        cityTermsWith(new MinAggregationSpec("min_price", FieldPath.parse(PRICE_FIELD)));
    // SF (price 7) matches only LOW. The other tree never saw that bucket.
    Accumulator left = collect(spec, context, LOW_PRICES);
    Accumulator right = collect(spec, context, HIGH_PRICES);

    left.mergeStateFrom(right);

    TermsAggregationResult byCity = terms(left.buildTopLevelResult());
    assertEquals(
        new BsonInt64(7),
        bucket(byCity, "SF").getChildResults().get("min_price").toStandaloneBson());
  }

  @Test
  public void mergeStateFrom_emptyOtherTree_keepsThisTreesResult() throws Exception {
    buildIndex(DATASET, 2);
    AggregationContext context = newContextWithFieldResolver();
    TopAggregationSpec spec =
        cityTermsWith(new AvgAggregationSpec("avg_price", FieldPath.parse(PRICE_FIELD)));
    Accumulator collected = collect(spec, context, LOW_PRICES);
    Accumulator empty = collect(spec, context, new MatchNoDocsQuery());

    collected.mergeStateFrom(empty);

    TermsAggregationResult byCity = terms(collected.buildTopLevelResult());
    assertEquals(Set.of("NYC", "LA", "SF"), bucketKeys(byCity));
    assertEquals(1L, bucket(byCity, "NYC").getDocCount());
  }

  @Test
  public void supportsStateMerge_nestedRemapTerms_isFalse() throws Exception {
    buildIndex(DATASET, DATASET.size());
    TopAggregationSpec spec =
        new TopAggregationSpec(
            List.of(
                new TermsGroupAggregationSpec(
                    "by_city",
                    FieldPath.parse(CITY_FIELD),
                    List.of(
                        new TermsGroupAggregationSpec(
                            "by_color",
                            FieldPath.parse(COLOR_FIELD),
                            List.of(),
                            new TermsArgs(/* isRoot= */ false, TypeField.TOKEN, 100))),
                    new TermsArgs(/* isRoot= */ true, TypeField.TOKEN, 100))));

    Accumulator top = collectAll(spec, newContext());

    // Remap allocates ordinals in collection order, so two slices key the same term differently.
    assertFalse(top.supportsStateMerge());
  }

  @Test
  public void supportsStateMerge_unoptedInLeaf_isFalse() throws Exception {
    buildIndex(DATASET, DATASET.size());
    TopAggregationSpec spec =
        cityTermsWith(new TestOperatorSpecs.SumSpec("sum_price", PRICE_LUCENE_FIELD));

    Accumulator top = collectAll(spec, newContext());

    assertFalse(top.supportsStateMerge());
  }
}
