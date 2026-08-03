package com.xgen.mongot.index.query.optimizer.rules;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlagRegistry;
import com.xgen.mongot.index.IndexMetricsUpdater;
import com.xgen.mongot.index.lucene.query.context.QueryFactoryContext;
import com.xgen.mongot.index.query.operators.bound.DateRangeBound;
import com.xgen.mongot.index.query.operators.bound.NumericRangeBound;
import com.xgen.mongot.index.query.operators.bound.RangeBound;
import com.xgen.mongot.index.query.operators.mql.AndClause;
import com.xgen.mongot.index.query.operators.mql.Clause;
import com.xgen.mongot.index.query.operators.mql.ClosedRangeBoundComparisonOperator;
import com.xgen.mongot.index.query.operators.mql.SimpleClause;
import com.xgen.mongot.index.query.operators.type.ValueType;
import com.xgen.mongot.index.query.optimizer.RewriteAudit;
import com.xgen.mongot.index.query.optimizer.RewriteContext;
import com.xgen.mongot.index.query.points.DatePoint;
import com.xgen.testing.mongot.index.query.operators.mql.ClauseBuilder;
import com.xgen.testing.mongot.index.query.operators.mql.MqlFilterOperatorBuilder;
import com.xgen.testing.mongot.index.query.operators.mql.ValueBuilder;
import com.xgen.testing.mongot.mock.index.SearchIndex;
import java.util.Date;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Per-rule tests for {@link RangeBoundsMergeMqlSimpleClauseRule}. Eligible shapes (NUMBER and DATE
 * pairs in each operand order, with each combination of inclusive/exclusive endpoints) collapse to
 * a single {@link ClosedRangeBoundComparisonOperator}; ineligible shapes (single operator, two
 * lower bounds, mismatched value types, mixed-with-eq, non-NUMBER/DATE) leave the clause untouched.
 *
 * <p>The factory's index-state precondition is intentionally NOT exercised here — the rule emits
 * the closed operator optimistically and the factory falls back to AND-of-two-opens when the index
 * is multi-valued. End-to-end behaviour for both branches lives in {@code
 * RangeBoundsMergeVectorQueryTest}.
 */
public class RangeBoundsMergeMqlSimpleClauseRuleTest {

  private final RangeBoundsMergeMqlSimpleClauseRule rule =
      new RangeBoundsMergeMqlSimpleClauseRule();

  @Test
  public void gtAndLt_numeric_collapsesToClosedRange() throws Exception {
    SimpleClause input =
        ClauseBuilder.simpleClause()
            .path("price")
            .addOperator(MqlFilterOperatorBuilder.gt().value(ValueBuilder.longNumber(1L)))
            .addOperator(MqlFilterOperatorBuilder.lt().value(ValueBuilder.longNumber(10L)))
            .build();

    Clause result = applyRule(input);

    assertClosedRange(result, "price", ValueType.NUMBER, false, false);
  }

  @Test
  public void gteAndLte_numeric_collapsesToClosedRange() throws Exception {
    SimpleClause input =
        ClauseBuilder.simpleClause()
            .path("price")
            .addOperator(MqlFilterOperatorBuilder.gte().value(ValueBuilder.doubleNumber(2.5)))
            .addOperator(MqlFilterOperatorBuilder.lte().value(ValueBuilder.doubleNumber(7.5)))
            .build();

    Clause result = applyRule(input);

    assertClosedRange(result, "price", ValueType.NUMBER, true, true);
  }

  @Test
  public void gtAndLte_numeric_collapsesToClosedRange() throws Exception {
    SimpleClause input =
        ClauseBuilder.simpleClause()
            .path("price")
            .addOperator(MqlFilterOperatorBuilder.gt().value(ValueBuilder.longNumber(0L)))
            .addOperator(MqlFilterOperatorBuilder.lte().value(ValueBuilder.longNumber(100L)))
            .build();

    assertClosedRange(applyRule(input), "price", ValueType.NUMBER, false, true);
  }

  @Test
  public void gteAndLt_numeric_collapsesToClosedRange() throws Exception {
    SimpleClause input =
        ClauseBuilder.simpleClause()
            .path("price")
            .addOperator(MqlFilterOperatorBuilder.gte().value(ValueBuilder.longNumber(0L)))
            .addOperator(MqlFilterOperatorBuilder.lt().value(ValueBuilder.longNumber(100L)))
            .build();

    assertClosedRange(applyRule(input), "price", ValueType.NUMBER, true, false);
  }

  @Test
  public void upperBeforeLower_collapsesToClosedRangeAndPreservesInclusivity() throws Exception {
    SimpleClause input =
        ClauseBuilder.simpleClause()
            .path("price")
            .addOperator(MqlFilterOperatorBuilder.lte().value(ValueBuilder.longNumber(100L)))
            .addOperator(MqlFilterOperatorBuilder.gt().value(ValueBuilder.longNumber(0L)))
            .build();

    assertClosedRange(applyRule(input), "price", ValueType.NUMBER, false, true);
  }

  @Test
  public void gtAndLt_date_collapsesToClosedRange() throws Exception {
    SimpleClause input =
        ClauseBuilder.simpleClause()
            .path("createdAt")
            .addOperator(
                MqlFilterOperatorBuilder.gt().value(ValueBuilder.date(new DatePoint(new Date(0)))))
            .addOperator(
                MqlFilterOperatorBuilder.lt()
                    .value(ValueBuilder.date(new DatePoint(new Date(1_000_000)))))
            .build();

    Clause result = applyRule(input);

    assertClosedRange(result, "createdAt", ValueType.DATE, false, false);
    RangeBound<?> bounds =
        ((ClosedRangeBoundComparisonOperator) ((SimpleClause) result).mqlFilterOperators().get(0))
            .bounds();
    assertTrue(bounds instanceof DateRangeBound);
  }

  @Test
  public void singleOperator_doesNotApply() throws Exception {
    SimpleClause input =
        ClauseBuilder.simpleClause()
            .path("price")
            .addOperator(MqlFilterOperatorBuilder.gt().value(ValueBuilder.longNumber(0L)))
            .build();

    assertNotApplied(input);
  }

  @Test
  public void twoLowerBounds_doesNotApply() throws Exception {
    SimpleClause input =
        ClauseBuilder.simpleClause()
            .path("price")
            .addOperator(MqlFilterOperatorBuilder.gt().value(ValueBuilder.longNumber(0L)))
            .addOperator(MqlFilterOperatorBuilder.gte().value(ValueBuilder.longNumber(5L)))
            .build();

    assertNotApplied(input);
  }

  @Test
  public void contradictoryGtAboveLt_doesNotApply() throws Exception {
    // gt 10 + lt 5 is complementary by shape, but the merged bound would violate the closed-range
    // invariant (lower above upper), so the clause is left unchanged.
    SimpleClause input =
        ClauseBuilder.simpleClause()
            .path("price")
            .addOperator(MqlFilterOperatorBuilder.gt().value(ValueBuilder.longNumber(10L)))
            .addOperator(MqlFilterOperatorBuilder.lt().value(ValueBuilder.longNumber(5L)))
            .build();

    assertNotApplied(input);
  }

  @Test
  public void equalValuesWithExclusiveEndpoint_doesNotApply() throws Exception {
    // gte 5 + lt 5: equal endpoints form a valid closed range only when both sides are inclusive.
    SimpleClause input =
        ClauseBuilder.simpleClause()
            .path("price")
            .addOperator(MqlFilterOperatorBuilder.gte().value(ValueBuilder.longNumber(5L)))
            .addOperator(MqlFilterOperatorBuilder.lt().value(ValueBuilder.longNumber(5L)))
            .build();

    assertNotApplied(input);
  }

  @Test
  public void gteAndLteEqualValues_collapsesToClosedRange() throws Exception {
    SimpleClause input =
        ClauseBuilder.simpleClause()
            .path("price")
            .addOperator(MqlFilterOperatorBuilder.gte().value(ValueBuilder.longNumber(5L)))
            .addOperator(MqlFilterOperatorBuilder.lte().value(ValueBuilder.longNumber(5L)))
            .build();

    assertClosedRange(applyRule(input), "price", ValueType.NUMBER, true, true);
  }

  @Test
  public void twoUpperBounds_doesNotApply() throws Exception {
    SimpleClause input =
        ClauseBuilder.simpleClause()
            .path("price")
            .addOperator(MqlFilterOperatorBuilder.lt().value(ValueBuilder.longNumber(10L)))
            .addOperator(MqlFilterOperatorBuilder.lte().value(ValueBuilder.longNumber(20L)))
            .build();

    assertNotApplied(input);
  }

  @Test
  public void mixedEqAndRange_doesNotApply() throws Exception {
    SimpleClause input =
        ClauseBuilder.simpleClause()
            .path("price")
            .addOperator(MqlFilterOperatorBuilder.gt().value(ValueBuilder.longNumber(0L)))
            .addOperator(MqlFilterOperatorBuilder.eq().value(ValueBuilder.longNumber(5L)))
            .build();

    assertNotApplied(input);
  }

  @Test
  public void mismatchedValueTypes_doesNotApply() throws Exception {
    SimpleClause input =
        ClauseBuilder.simpleClause()
            .path("ambiguous")
            .addOperator(MqlFilterOperatorBuilder.gt().value(ValueBuilder.longNumber(0L)))
            .addOperator(
                MqlFilterOperatorBuilder.lt().value(ValueBuilder.date(new DatePoint(new Date(0)))))
            .build();

    assertNotApplied(input);
  }

  @Test
  public void stringRange_doesNotApply() throws Exception {
    SimpleClause input =
        ClauseBuilder.simpleClause()
            .path("name")
            .addOperator(MqlFilterOperatorBuilder.gt().value(ValueBuilder.string("a")))
            .addOperator(MqlFilterOperatorBuilder.lt().value(ValueBuilder.string("z")))
            .build();

    assertNotApplied(input);
  }

  @Test
  public void compoundClause_doesNotApply() throws Exception {
    AndClause input =
        ClauseBuilder.andClause()
            .addClause(
                ClauseBuilder.simpleClause()
                    .path("price")
                    .addOperator(MqlFilterOperatorBuilder.gt().value(ValueBuilder.longNumber(0L)))
                    .addOperator(MqlFilterOperatorBuilder.lt().value(ValueBuilder.longNumber(10L)))
                    .build())
            .explicitAnd(true)
            .build();

    Clause result = this.rule.apply(input, newContext());
    assertSame("rule should not match a compound clause", result, input);
  }

  private Clause applyRule(SimpleClause input) {
    Clause rewritten = this.rule.apply(input, newContext());
    assertNotSame("rule expected to fire", rewritten, input);
    return rewritten;
  }

  private void assertNotApplied(SimpleClause input) {
    Clause rewritten = this.rule.apply(input, newContext());
    assertSame("rule should leave clause unchanged", rewritten, input);
  }

  private static void assertClosedRange(
      Clause clause,
      String path,
      ValueType valueType,
      boolean expectLowerInclusive,
      boolean expectUpperInclusive) {
    assertTrue(clause instanceof SimpleClause);
    SimpleClause sc = (SimpleClause) clause;
    assertEquals(path, sc.path().toString());
    assertEquals(1, sc.mqlFilterOperators().size());
    assertTrue(sc.mqlFilterOperators().get(0) instanceof ClosedRangeBoundComparisonOperator);
    ClosedRangeBoundComparisonOperator closed =
        (ClosedRangeBoundComparisonOperator) sc.mqlFilterOperators().get(0);
    assertEquals(valueType, closed.getValueType());
    RangeBound<?> bounds = closed.bounds();
    assertTrue("merged bounds should retain the lower endpoint", bounds.getLower().isPresent());
    assertTrue("merged bounds should retain the upper endpoint", bounds.getUpper().isPresent());
    assertEquals(expectLowerInclusive, bounds.lowerInclusive());
    assertEquals(expectUpperInclusive, bounds.upperInclusive());
    if (valueType == ValueType.NUMBER) {
      assertTrue(bounds instanceof NumericRangeBound);
    } else if (valueType == ValueType.DATE) {
      assertTrue(bounds instanceof DateRangeBound);
    }
  }

  private static RewriteContext newContext() {
    QueryFactoryContext factoryContext = Mockito.mock(QueryFactoryContext.class);
    IndexMetricsUpdater.QueryingMetricsUpdater metrics =
        new IndexMetricsUpdater.QueryingMetricsUpdater(SearchIndex.mockMetricsFactory());
    return new RewriteContext(
        null, factoryContext, DynamicFeatureFlagRegistry.empty(), metrics, new RewriteAudit());
  }

  /**
   * Idempotence guard: applying the rule a second time on its own output must not fire again,
   * because the closed-range form has only one operator and the rule requires two.
   */
  @Test
  public void idempotent_doesNotReapply() throws Exception {
    SimpleClause input =
        ClauseBuilder.simpleClause()
            .path("price")
            .addOperator(MqlFilterOperatorBuilder.gt().value(ValueBuilder.longNumber(0L)))
            .addOperator(MqlFilterOperatorBuilder.lt().value(ValueBuilder.longNumber(10L)))
            .build();

    Clause once = applyRule(input);
    Clause twice = this.rule.apply(once, newContext());
    assertSame("second application should produce no further rewrite", twice, once);
  }
}
