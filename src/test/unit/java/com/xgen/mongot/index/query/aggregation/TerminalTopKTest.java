package com.xgen.mongot.index.query.aggregation;

import static org.junit.Assert.assertEquals;

import com.xgen.mongot.index.lucene.field.FieldName.TypeField;
import com.xgen.mongot.index.query.aggregation.AggregationRegistry.GroupingArgs.TermsArgs;
import com.xgen.mongot.util.FieldPath;
import java.util.List;
import org.junit.Test;

/**
 * Pruning to top-N while building results must select the same buckets as building full cardinality
 * and then {@link AggregationResult#applyLimit()}.
 */
public class TerminalTopKTest extends AggregationTestSupport {

  private static final List<TestDoc> DATASET =
      List.of(
          new TestDoc("NYC", "red", 10),
          new TestDoc("NYC", "blue", 20),
          new TestDoc("NYC", "red", 30),
          new TestDoc("LA", "blue", 5),
          new TestDoc("LA", "red", 15),
          new TestDoc("SF", "green", 7));

  private static TopAggregationSpec cityTermsWithAvg(int limit) {
    return new TopAggregationSpec(
        List.of(
            new TermsGroupAggregationSpec(
                "by_city",
                FieldPath.parse(CITY_FIELD),
                List.of(new AvgAggregationSpec("avg_price", FieldPath.parse(PRICE_FIELD))),
                new TermsArgs(/* isRoot= */ true, TypeField.TOKEN, limit))));
  }

  private static TermsAggregationResult terms(AggregationResult top) {
    return (TermsAggregationResult) ((TopLevelAggregationResult) top).childResults().get(0);
  }

  @Test
  public void enableTerminalTopK_limitBelowCardinality_matchesApplyLimit() throws Exception {
    buildIndex(DATASET, 2);
    Accumulator top = collectAll(cityTermsWithAvg(2), newContextWithFieldResolver());

    TermsAggregationResult gold = terms(top.buildTopLevelResult().applyLimit());

    top.enableTerminalTopK();
    TermsAggregationResult pruned = terms(top.buildTopLevelResult());

    assertEquals(List.of("NYC", "LA"), bucketKeysInOrder(gold, BucketOrder.COUNT_DESC));
    assertEquals(
        bucketKeysInOrder(gold, BucketOrder.COUNT_DESC),
        bucketKeysInOrder(pruned, BucketOrder.COUNT_DESC));
    assertEquals(bucket(gold, "NYC").getDocCount(), bucket(pruned, "NYC").getDocCount());
    assertEquals(bucket(gold, "LA").getDocCount(), bucket(pruned, "LA").getDocCount());
    assertEquals(
        bucket(gold, "NYC").getChildResults().get("avg_price").toStandaloneBson(),
        bucket(pruned, "NYC").getChildResults().get("avg_price").toStandaloneBson());
    assertEquals(
        bucket(gold, "LA").getChildResults().get("avg_price").toStandaloneBson(),
        bucket(pruned, "LA").getChildResults().get("avg_price").toStandaloneBson());
  }

  @Test
  public void enableTerminalTopK_prunedResult_applyLimitIsNoOp() throws Exception {
    buildIndex(DATASET, 2);
    Accumulator top = collectAll(cityTermsWithAvg(2), newContextWithFieldResolver());
    top.enableTerminalTopK();

    TermsAggregationResult pruned = terms(top.buildTopLevelResult());
    TermsAggregationResult reLimited = terms(top.buildTopLevelResult().applyLimit());

    assertEquals(
        bucketKeysInOrder(pruned, BucketOrder.COUNT_DESC),
        bucketKeysInOrder(reLimited, BucketOrder.COUNT_DESC));
  }

  @Test
  public void enableTerminalTopK_countTies_breakAscendingByTerm() throws Exception {
    buildIndex(
        List.of(
            new TestDoc("SF", "red", 1), new TestDoc("NYC", "red", 2), new TestDoc("LA", "red", 3)),
        3);
    Accumulator top = collectAll(cityTermsWithAvg(2), newContextWithFieldResolver());

    TermsAggregationResult gold = terms(top.buildTopLevelResult().applyLimit());
    top.enableTerminalTopK();
    TermsAggregationResult pruned = terms(top.buildTopLevelResult());

    assertEquals(List.of("LA", "NYC"), bucketKeysInOrder(pruned, BucketOrder.COUNT_DESC));
    assertEquals(
        bucketKeysInOrder(gold, BucketOrder.COUNT_DESC),
        bucketKeysInOrder(pruned, BucketOrder.COUNT_DESC));
  }

  @Test
  public void enableTerminalTopK_limitAboveCardinality_returnsAllBuckets() throws Exception {
    buildIndex(DATASET, 2);
    Accumulator top = collectAll(cityTermsWithAvg(100), newContextWithFieldResolver());
    top.enableTerminalTopK();

    TermsAggregationResult pruned = terms(top.buildTopLevelResult());

    assertEquals(List.of("LA", "NYC", "SF"), bucketKeysInOrder(pruned));
    assertEquals(3L, bucket(pruned, "NYC").getDocCount());
  }
}
