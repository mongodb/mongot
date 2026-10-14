package com.xgen.mongot.index.query.aggregation;

import static com.google.common.truth.Truth.assertThat;

import com.xgen.mongot.index.definition.NumericFieldOptions;
import com.xgen.mongot.index.lucene.field.FieldName.TypeField;
import com.xgen.mongot.index.query.aggregation.AggregationRegistry.GroupingArgs.TermsArgs;
import com.xgen.mongot.util.FieldPath;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.TermQuery;
import org.junit.Test;

/**
 * Accounting has to reach a real accumulator tree: the context carries the accountant down to every
 * array a query allocates, and terms keys are charged when buckets materialise.
 */
public class AggregationHeapAccountingTest extends AggregationTestSupport {

  private static TopAggregationSpec termsByCityWithCount() {
    return new TopAggregationSpec(
        List.of(
            new TermsGroupAggregationSpec(
                "by_city",
                FieldPath.parse(CITY_FIELD),
                List.of(new CountAggregationSpec("cnt")),
                new TermsArgs(true, TypeField.TOKEN, Integer.MAX_VALUE))));
  }

  private static List<TestDoc> docsAcross(int cities) {
    List<TestDoc> docs = new ArrayList<>();
    for (int i = 0; i < cities; i++) {
      docs.add(new TestDoc("city-" + i, "x", i));
    }
    return docs;
  }

  @Test
  public void collect_accountsHeapForTheWholeAccumulatorTree() throws Exception {
    buildIndex(docsAcross(50), Integer.MAX_VALUE);
    var accountant = new AggregationHeapAccountant();

    collectAll(termsByCityWithCount(), newContextWithHeapAccountant(accountant));

    assertThat(accountant.currentBytes()).isGreaterThan(0L);
  }

  @Test
  public void collect_higherCardinality_accountsMoreHeap() throws Exception {
    buildIndex(docsAcross(10), Integer.MAX_VALUE);
    var lowCardinality = new AggregationHeapAccountant();
    collectAll(termsByCityWithCount(), newContextWithHeapAccountant(lowCardinality));
    tearDownIndex();

    buildIndex(docsAcross(5_000), Integer.MAX_VALUE);
    var highCardinality = new AggregationHeapAccountant();
    collectAll(termsByCityWithCount(), newContextWithHeapAccountant(highCardinality));

    assertThat(highCardinality.currentBytes()).isGreaterThan(lowCardinality.currentBytes());
  }

  @Test
  public void collect_matchingFewerDocuments_accountsLessThanMatchingAll() throws Exception {
    buildIndex(docsAcross(2_000), Integer.MAX_VALUE);

    var matchAll = new AggregationHeapAccountant();
    collectAll(termsByCityWithCount(), newContextWithHeapAccountant(matchAll));

    var singleCity = new AggregationHeapAccountant();
    collect(
        termsByCityWithCount(),
        newContextWithHeapAccountant(singleCity),
        new TermQuery(new Term(CITY_LUCENE_FIELD, "city-7")));

    assertThat(singleCity.currentBytes()).isLessThan(matchAll.currentBytes());
  }

  @Test
  public void collect_withoutAnAccountant_isInert() throws Exception {
    buildIndex(docsAcross(50), Integer.MAX_VALUE);

    collectAll(termsByCityWithCount(), newContext());

    assertThat(AggregationHeapAccountant.disabled().currentBytes()).isEqualTo(0L);
  }

  @Test
  public void collect_minMax_accountsObjectArrayHeap() throws Exception {
    buildIndex(docsAcross(50), Integer.MAX_VALUE);
    var accountant = new AggregationHeapAccountant();

    collectAll(
        new TopAggregationSpec(
            List.of(
                new TermsGroupAggregationSpec(
                    "by_city",
                    FieldPath.parse(CITY_FIELD),
                    List.of(new MinAggregationSpec("min_price", FieldPath.parse(PRICE_FIELD))),
                    new TermsArgs(true, TypeField.TOKEN, Integer.MAX_VALUE)))),
        int64ContextWith(accountant));

    assertThat(accountant.currentBytes()).isGreaterThan(0L);
  }

  private AggregationContext int64ContextWith(AggregationHeapAccountant accountant) {
    return AggregationContext.create(
        this.reader,
        Optional.of(
            new AggregationFieldResolver() {
              @Override
              public ResolvedNumericMetricField resolveNumeric(FieldPath path) {
                if (path.equals(FieldPath.parse(PRICE_FIELD))) {
                  return new ResolvedNumericMetricField(
                      PRICE_LUCENE_FIELD, NumericFieldOptions.Representation.INT64);
                }
                throw new IllegalStateException("unexpected path: " + path);
              }

              @Override
              public ResolvedTokenField resolveToken(FieldPath path) {
                throw new IllegalStateException("unexpected path: " + path);
              }
            }),
        luceneField -> Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        0,
        false,
        accountant);
  }
}
