package com.xgen.mongot.index.query.optimizer.rules;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlagRegistry;
import com.xgen.mongot.index.IndexMetricsUpdater;
import com.xgen.mongot.index.lucene.query.context.QueryFactoryContext;
import com.xgen.mongot.index.query.operators.mql.AndClause;
import com.xgen.mongot.index.query.operators.mql.Clause;
import com.xgen.mongot.index.query.operators.mql.MqlFilterOperator;
import com.xgen.mongot.index.query.operators.mql.OpenRangeBoundComparisonOperator;
import com.xgen.mongot.index.query.operators.mql.SimpleClause;
import com.xgen.mongot.index.query.optimizer.RewriteAudit;
import com.xgen.mongot.index.query.optimizer.RewriteContext;
import com.xgen.mongot.index.query.points.DatePoint;
import com.xgen.testing.mongot.index.query.operators.mql.ClauseBuilder;
import com.xgen.testing.mongot.index.query.operators.mql.MqlFilterOperatorBuilder;
import com.xgen.testing.mongot.index.query.operators.mql.ValueBuilder;
import com.xgen.testing.mongot.mock.index.SearchIndex;
import java.util.Date;
import java.util.List;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Per-rule tests for {@link RangeBoundsMergeMqlAndClauseRule}. Pairs of single-operator {@link
 * SimpleClause}s on the same path with complementary NUMBER or DATE half-open operators fuse into
 * one two-operator {@link SimpleClause} inside a rewritten {@link AndClause}; ineligible shapes
 * leave the clause untouched.
 */
public class RangeBoundsMergeMqlAndClauseRuleTest {

  private final RangeBoundsMergeMqlAndClauseRule rule = new RangeBoundsMergeMqlAndClauseRule();

  @Test
  public void gtAndLt_numeric_implicit_fuses() throws Exception {
    AndClause input =
        ClauseBuilder.andClause()
            .addClause(
                singleOpClause(
                    "x", MqlFilterOperatorBuilder.gt().value(ValueBuilder.longNumber(1L))))
            .addClause(
                singleOpClause(
                    "x", MqlFilterOperatorBuilder.lt().value(ValueBuilder.longNumber(10L))))
            .explicitAnd(false)
            .build();

    AndClause result = applyRule(input);

    assertEquals(1, result.getClauses().size());
    assertFusedSimpleClause(result.getClauses().get(0), "x");
    assertFalse(result.isExplicitAnd());
  }

  @Test
  public void gtAndLt_numeric_explicit_fuses_and_preservesExplicitAnd() throws Exception {
    AndClause input =
        ClauseBuilder.andClause()
            .addClause(
                singleOpClause(
                    "x", MqlFilterOperatorBuilder.gt().value(ValueBuilder.longNumber(1L))))
            .addClause(
                singleOpClause(
                    "x", MqlFilterOperatorBuilder.lt().value(ValueBuilder.longNumber(10L))))
            .explicitAnd(true)
            .build();

    AndClause result = applyRule(input);

    assertEquals(1, result.getClauses().size());
    assertFusedSimpleClause(result.getClauses().get(0), "x");
    assertTrue(result.isExplicitAnd());
  }

  @Test
  public void upperBeforeLower_fuses() throws Exception {
    // lt comes before gt — rule should still pair them
    AndClause input =
        ClauseBuilder.andClause()
            .addClause(
                singleOpClause(
                    "price", MqlFilterOperatorBuilder.lt().value(ValueBuilder.longNumber(100L))))
            .addClause(
                singleOpClause(
                    "price", MqlFilterOperatorBuilder.gt().value(ValueBuilder.longNumber(0L))))
            .explicitAnd(false)
            .build();

    AndClause result = applyRule(input);

    assertEquals(1, result.getClauses().size());
    assertFusedSimpleClause(result.getClauses().get(0), "price");
    // lower should be first operator in the fused clause
    SimpleClause sc = (SimpleClause) result.getClauses().get(0);
    assertEquals(
        MqlFilterOperator.Category.LOWER_BOUND,
        ((OpenRangeBoundComparisonOperator) sc.mqlFilterOperators().get(0)).getCategory());
  }

  @Test
  public void gteAndLte_numeric_fuses() throws Exception {
    AndClause input =
        ClauseBuilder.andClause()
            .addClause(
                singleOpClause(
                    "score", MqlFilterOperatorBuilder.gte().value(ValueBuilder.doubleNumber(2.5))))
            .addClause(
                singleOpClause(
                    "score", MqlFilterOperatorBuilder.lte().value(ValueBuilder.doubleNumber(7.5))))
            .explicitAnd(false)
            .build();

    AndClause result = applyRule(input);

    assertEquals(1, result.getClauses().size());
    assertFusedSimpleClause(result.getClauses().get(0), "score");
  }

  @Test
  public void gtAndLt_date_fuses() throws Exception {
    AndClause input =
        ClauseBuilder.andClause()
            .addClause(
                singleOpClause(
                    "ts",
                    MqlFilterOperatorBuilder.gt()
                        .value(ValueBuilder.date(new DatePoint(new Date(0))))))
            .addClause(
                singleOpClause(
                    "ts",
                    MqlFilterOperatorBuilder.lt()
                        .value(ValueBuilder.date(new DatePoint(new Date(1_000_000))))))
            .explicitAnd(false)
            .build();

    AndClause result = applyRule(input);

    assertEquals(1, result.getClauses().size());
    assertFusedSimpleClause(result.getClauses().get(0), "ts");
  }

  @Test
  public void threeChildren_eligiblePairPlusThird_fusesOnlyPair() throws Exception {
    // {$and: [{x: {$gt: 1}}, {y: {$eq: 5}}, {x: {$lt: 10}}]}
    // Only the x pair is eligible; y clause is left untouched
    Clause ypathClause =
        ClauseBuilder.simpleClause()
            .path("y")
            .addOperator(MqlFilterOperatorBuilder.eq().value(ValueBuilder.longNumber(5L)))
            .build();

    AndClause input =
        ClauseBuilder.andClause()
            .addClause(
                singleOpClause(
                    "x", MqlFilterOperatorBuilder.gt().value(ValueBuilder.longNumber(1L))))
            .addClause(ypathClause)
            .addClause(
                singleOpClause(
                    "x", MqlFilterOperatorBuilder.lt().value(ValueBuilder.longNumber(10L))))
            .explicitAnd(false)
            .build();

    AndClause result = applyRule(input);

    assertEquals(2, result.getClauses().size());
    // fused x clause is placed at index 0 (the lo of i=0, j=2)
    List<Clause> children = result.getClauses();
    assertFusedSimpleClause(children.get(0), "x");
    // y clause remains
    assertTrue(children.contains(ypathClause));
  }

  @Test
  public void twoPairsDifferentPaths_fusesBothInOneApplication() throws Exception {
    AndClause input =
        ClauseBuilder.andClause()
            .addClause(
                singleOpClause(
                    "x", MqlFilterOperatorBuilder.gt().value(ValueBuilder.longNumber(1L))))
            .addClause(
                singleOpClause(
                    "x", MqlFilterOperatorBuilder.lt().value(ValueBuilder.longNumber(10L))))
            .addClause(
                singleOpClause(
                    "y", MqlFilterOperatorBuilder.gte().value(ValueBuilder.longNumber(2L))))
            .addClause(
                singleOpClause(
                    "y", MqlFilterOperatorBuilder.lte().value(ValueBuilder.longNumber(8L))))
            .explicitAnd(false)
            .build();

    AndClause result = applyRule(input);

    assertEquals(2, result.getClauses().size());
    assertFusedSimpleClause(result.getClauses().get(0), "x");
    assertFusedSimpleClause(result.getClauses().get(1), "y");
  }

  @Test
  public void twoPairsSamePath_fusesBothInOneApplication() throws Exception {
    // Two lowers and two uppers on one path form two complementary pairs. Either pairing yields
    // the same conjunction, so the test only requires that both pairs fuse.
    AndClause input =
        ClauseBuilder.andClause()
            .addClause(
                singleOpClause(
                    "x", MqlFilterOperatorBuilder.gt().value(ValueBuilder.longNumber(1L))))
            .addClause(
                singleOpClause(
                    "x", MqlFilterOperatorBuilder.gt().value(ValueBuilder.longNumber(2L))))
            .addClause(
                singleOpClause(
                    "x", MqlFilterOperatorBuilder.lt().value(ValueBuilder.longNumber(10L))))
            .addClause(
                singleOpClause(
                    "x", MqlFilterOperatorBuilder.lt().value(ValueBuilder.longNumber(8L))))
            .explicitAnd(false)
            .build();

    AndClause result = applyRule(input);

    assertEquals(2, result.getClauses().size());
    assertFusedSimpleClause(result.getClauses().get(0), "x");
    assertFusedSimpleClause(result.getClauses().get(1), "x");
  }

  @Test
  public void differentPaths_doesNotApply() throws Exception {
    AndClause input =
        ClauseBuilder.andClause()
            .addClause(
                singleOpClause(
                    "a", MqlFilterOperatorBuilder.gt().value(ValueBuilder.longNumber(0L))))
            .addClause(
                singleOpClause(
                    "b", MqlFilterOperatorBuilder.lt().value(ValueBuilder.longNumber(10L))))
            .explicitAnd(false)
            .build();

    assertNotApplied(input);
  }

  @Test
  public void twoLowerBounds_doesNotApply() throws Exception {
    AndClause input =
        ClauseBuilder.andClause()
            .addClause(
                singleOpClause(
                    "x", MqlFilterOperatorBuilder.gt().value(ValueBuilder.longNumber(0L))))
            .addClause(
                singleOpClause(
                    "x", MqlFilterOperatorBuilder.gte().value(ValueBuilder.longNumber(1L))))
            .explicitAnd(false)
            .build();

    assertNotApplied(input);
  }

  @Test
  public void twoUpperBounds_doesNotApply() throws Exception {
    AndClause input =
        ClauseBuilder.andClause()
            .addClause(
                singleOpClause(
                    "x", MqlFilterOperatorBuilder.lt().value(ValueBuilder.longNumber(10L))))
            .addClause(
                singleOpClause(
                    "x", MqlFilterOperatorBuilder.lte().value(ValueBuilder.longNumber(20L))))
            .explicitAnd(false)
            .build();

    assertNotApplied(input);
  }

  @Test
  public void stringRange_doesNotApply() throws Exception {
    AndClause input =
        ClauseBuilder.andClause()
            .addClause(
                singleOpClause(
                    "name", MqlFilterOperatorBuilder.gt().value(ValueBuilder.string("a"))))
            .addClause(
                singleOpClause(
                    "name", MqlFilterOperatorBuilder.lt().value(ValueBuilder.string("z"))))
            .explicitAnd(false)
            .build();

    assertNotApplied(input);
  }

  @Test
  public void mismatchedValueTypes_doesNotApply() throws Exception {
    AndClause input =
        ClauseBuilder.andClause()
            .addClause(
                singleOpClause(
                    "f", MqlFilterOperatorBuilder.gt().value(ValueBuilder.longNumber(0L))))
            .addClause(
                singleOpClause(
                    "f",
                    MqlFilterOperatorBuilder.lt()
                        .value(ValueBuilder.date(new DatePoint(new Date(0))))))
            .explicitAnd(false)
            .build();

    assertNotApplied(input);
  }

  @Test
  public void multiOperatorChild_doesNotApply() throws Exception {
    // The child that has two operators is not a "single open range" clause — rule should skip it.
    Clause twoOpClause =
        ClauseBuilder.simpleClause()
            .path("x")
            .addOperator(MqlFilterOperatorBuilder.gt().value(ValueBuilder.longNumber(0L)))
            .addOperator(MqlFilterOperatorBuilder.lt().value(ValueBuilder.longNumber(100L)))
            .build();

    AndClause input =
        ClauseBuilder.andClause()
            .addClause(twoOpClause)
            .addClause(
                singleOpClause(
                    "x", MqlFilterOperatorBuilder.lt().value(ValueBuilder.longNumber(50L))))
            .explicitAnd(false)
            .build();

    assertNotApplied(input);
  }

  @Test
  public void simpleClause_doesNotApply() throws Exception {
    // Rule only matches AndClause at the top level
    SimpleClause input =
        ClauseBuilder.simpleClause()
            .path("x")
            .addOperator(MqlFilterOperatorBuilder.gt().value(ValueBuilder.longNumber(0L)))
            .build();

    Clause result = this.rule.apply(input, newContext());
    assertSame("rule should not match a SimpleClause", input, result);
  }

  @Test
  public void idempotent_fusedOutputDoesNotReapply() throws Exception {
    // After fusion the AndClause has one SimpleClause child with two operators.
    // That child is not a single-operator SimpleClause, so the next iteration must not fuse again.
    AndClause input =
        ClauseBuilder.andClause()
            .addClause(
                singleOpClause(
                    "x", MqlFilterOperatorBuilder.gt().value(ValueBuilder.longNumber(1L))))
            .addClause(
                singleOpClause(
                    "x", MqlFilterOperatorBuilder.lt().value(ValueBuilder.longNumber(10L))))
            .explicitAnd(false)
            .build();

    AndClause once = applyRule(input);
    Clause twice = this.rule.apply(once, newContext());
    assertSame(
        "second application should not fire (fused child has 2 operators, not 1)", twice, once);
  }

  // ---- helpers ----

  private AndClause applyRule(AndClause input) {
    Clause result = this.rule.apply(input, newContext());
    assertNotSame("rule expected to fire", result, input);
    assertTrue(result instanceof AndClause);
    return (AndClause) result;
  }

  private void assertNotApplied(AndClause input) {
    Clause result = this.rule.apply(input, newContext());
    assertSame("rule should not have fired", result, input);
  }

  private static void assertFusedSimpleClause(Clause clause, String expectedPath) {
    assertTrue("fused clause should be a SimpleClause", clause instanceof SimpleClause);
    SimpleClause sc = (SimpleClause) clause;
    assertEquals(expectedPath, sc.path().toString());
    assertEquals("fused clause must have exactly two operators", 2, sc.mqlFilterOperators().size());
    assertTrue(
        "first operator must be a lower bound",
        sc.mqlFilterOperators().get(0) instanceof OpenRangeBoundComparisonOperator open1
            && open1.getCategory() == MqlFilterOperator.Category.LOWER_BOUND);
    assertTrue(
        "second operator must be an upper bound",
        sc.mqlFilterOperators().get(1) instanceof OpenRangeBoundComparisonOperator open2
            && open2.getCategory() == MqlFilterOperator.Category.UPPER_BOUND);
  }

  private static SimpleClause singleOpClause(
      String path, MqlFilterOperatorBuilder<?> operatorBuilder) throws Exception {
    return ClauseBuilder.simpleClause().path(path).addOperator(operatorBuilder.build()).build();
  }

  private static RewriteContext newContext() {
    QueryFactoryContext factoryContext = Mockito.mock(QueryFactoryContext.class);
    IndexMetricsUpdater.QueryingMetricsUpdater metrics =
        new IndexMetricsUpdater.QueryingMetricsUpdater(SearchIndex.mockMetricsFactory());
    return new RewriteContext(
        null, factoryContext, DynamicFeatureFlagRegistry.empty(), metrics, new RewriteAudit());
  }
}
