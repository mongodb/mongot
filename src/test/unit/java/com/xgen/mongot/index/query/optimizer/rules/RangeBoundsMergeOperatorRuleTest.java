package com.xgen.mongot.index.query.optimizer.rules;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlagRegistry;
import com.xgen.mongot.index.IndexMetricsUpdater;
import com.xgen.mongot.index.lucene.query.context.QueryFactoryContext;
import com.xgen.mongot.index.query.operators.CompoundClause;
import com.xgen.mongot.index.query.operators.CompoundOperator;
import com.xgen.mongot.index.query.operators.Operator;
import com.xgen.mongot.index.query.operators.RangeOperator;
import com.xgen.mongot.index.query.operators.bound.DateRangeBound;
import com.xgen.mongot.index.query.operators.bound.NumericRangeBound;
import com.xgen.mongot.index.query.operators.bound.StringRangeBound;
import com.xgen.mongot.index.query.optimizer.RewriteAudit;
import com.xgen.mongot.index.query.optimizer.RewriteContext;
import com.xgen.mongot.index.query.points.DatePoint;
import com.xgen.mongot.index.query.points.DoublePoint;
import com.xgen.mongot.index.query.points.LongPoint;
import com.xgen.mongot.index.query.points.StringPoint;
import com.xgen.mongot.index.query.scores.Score;
import com.xgen.mongot.util.FieldPath;
import com.xgen.testing.mongot.mock.index.SearchIndex;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Per-rule tests for {@link RangeBoundsMergeOperatorRule}. Complementary half-open {@link
 * RangeOperator} pairs on the same path fuse into a single fully-bounded operator marked {@code
 * fusedByOptimizer}; ineligible shapes (wrong slot, mismatched or multiple paths,
 * non-complementary, differing {@code doesNotAffect}) are left untouched.
 *
 * <p>The rule never consults the index: the array-data fallback for fused operators lives in the
 * translation step, covered in {@code RangeBoundsMergeSearchQueryTest}.
 */
public class RangeBoundsMergeOperatorRuleTest {

  private final RangeBoundsMergeOperatorRule rule = new RangeBoundsMergeOperatorRule();

  // ---- filter slot ----

  @Test
  public void filterSlot_complementaryNumericPair_fuses() {
    RangeOperator lower = numericLower("price", 1L, false);
    RangeOperator upper = numericUpper("price", 10L, false);
    CompoundOperator input = compoundWithFilter(List.of(lower, upper));

    CompoundOperator result = applyRule(input);

    assertEquals(1, result.filter().operators().size());
    RangeOperator fused = (RangeOperator) result.filter().operators().get(0);
    assertTrue(fused.bounds() instanceof NumericRangeBound);
    NumericRangeBound bounds = (NumericRangeBound) fused.bounds();
    assertTrue(bounds.getLower().isPresent());
    assertTrue(bounds.getUpper().isPresent());
    assertFalse(bounds.lowerInclusive());
    assertFalse(bounds.upperInclusive());
    assertEquals(lower.score(), fused.score());
    assertTrue(
        "the fused operator must carry the marker that keys the translation fallback",
        fused.fusedByOptimizer());
  }

  @Test
  public void filterSlot_inclusiveBounds_fusePreservesInclusivity() {
    RangeOperator lower = numericLower("x", 0L, true);
    RangeOperator upper = numericUpper("x", 100L, true);
    CompoundOperator input = compoundWithFilter(List.of(lower, upper));

    CompoundOperator result = applyRule(input);

    RangeOperator fused = (RangeOperator) result.filter().operators().get(0);
    assertTrue(fused.bounds().lowerInclusive());
    assertTrue(fused.bounds().upperInclusive());
  }

  @Test
  public void filterSlot_upperBeforeLower_fuses() {
    // Order in the list should not matter — rule identifies by presence of lower/upper
    RangeOperator upper = numericUpper("price", 10L, false);
    RangeOperator lower = numericLower("price", 1L, false);
    CompoundOperator input = compoundWithFilter(List.of(upper, lower));

    CompoundOperator result = applyRule(input);

    assertEquals(1, result.filter().operators().size());
    RangeOperator fused = (RangeOperator) result.filter().operators().get(0);
    assertTrue(fused.bounds().getLower().isPresent());
    assertTrue(fused.bounds().getUpper().isPresent());
  }

  @Test
  public void filterSlot_dateType_fuses() {
    RangeOperator lower = dateLower("ts", new Date(0), false);
    RangeOperator upper = dateUpper("ts", new Date(1_000_000), false);
    CompoundOperator input = compoundWithFilter(List.of(lower, upper));

    CompoundOperator result = applyRule(input);

    RangeOperator fused = (RangeOperator) result.filter().operators().get(0);
    assertTrue(fused.bounds() instanceof DateRangeBound);
  }

  @Test
  public void filterSlot_mixedNumericPointTypes_fuses() {
    RangeOperator lower = numericLower("price", 1L, false);
    RangeOperator upper = numericUpperDouble("price", 10.5);
    CompoundOperator input = compoundWithFilter(List.of(lower, upper));

    CompoundOperator result = applyRule(input);

    assertEquals(1, result.filter().operators().size());
    RangeOperator fused = (RangeOperator) result.filter().operators().get(0);
    // The fused bound keeps each endpoint's original point type — no cross-type coercion.
    NumericRangeBound bounds = (NumericRangeBound) fused.bounds();
    assertEquals(Optional.of(new LongPoint(1L)), bounds.getLower());
    assertEquals(Optional.of(new DoublePoint(10.5)), bounds.getUpper());
  }

  @Test
  public void filterSlot_threeOperatorsTwoPlusOne_fusesOnlyPair() {
    RangeOperator lower = numericLower("x", 0L, false);
    RangeOperator upper = numericUpper("x", 10L, false);
    RangeOperator unrelated = numericLower("y", 5L, true);
    CompoundOperator input = compoundWithFilter(List.of(lower, upper, unrelated));

    CompoundOperator result = applyRule(input);

    assertEquals(2, result.filter().operators().size());
    assertTrue(result.filter().operators().stream().anyMatch(op -> op == unrelated));
  }

  @Test
  public void filterSlot_twoPairsDifferentPaths_fusesBothInOneApplication() {
    RangeOperator lowerX = numericLower("x", 1L, false);
    RangeOperator upperX = numericUpper("x", 10L, false);
    RangeOperator lowerY = numericLower("y", 2L, true);
    RangeOperator upperY = numericUpper("y", 8L, true);
    CompoundOperator input = compoundWithFilter(List.of(lowerX, upperX, lowerY, upperY));

    CompoundOperator result = applyRule(input);

    assertEquals(2, result.filter().operators().size());
    RangeOperator fusedX = (RangeOperator) result.filter().operators().get(0);
    assertEquals(List.of(FieldPath.newRoot("x")), fusedX.paths());
    assertTrue(fusedX.bounds().getLower().isPresent());
    assertTrue(fusedX.bounds().getUpper().isPresent());
    RangeOperator fusedY = (RangeOperator) result.filter().operators().get(1);
    assertEquals(List.of(FieldPath.newRoot("y")), fusedY.paths());
    assertTrue(fusedY.bounds().getLower().isPresent());
    assertTrue(fusedY.bounds().getUpper().isPresent());
  }

  @Test
  public void filterSlot_twoPairsSamePath_fusesBothInOneApplication() {
    // Two lowers and two uppers on one path form two complementary pairs. Either pairing yields
    // the same conjunction, so the test only requires that both pairs fuse.
    RangeOperator lower1 = numericLower("x", 1L, false);
    RangeOperator lower2 = numericLower("x", 2L, false);
    RangeOperator upper1 = numericUpper("x", 10L, false);
    RangeOperator upper2 = numericUpper("x", 8L, false);
    CompoundOperator input = compoundWithFilter(List.of(lower1, lower2, upper1, upper2));

    CompoundOperator result = applyRule(input);

    assertEquals(2, result.filter().operators().size());
    for (Operator op : result.filter().operators()) {
      RangeOperator fused = (RangeOperator) op;
      assertTrue(fused.bounds().getLower().isPresent());
      assertTrue(fused.bounds().getUpper().isPresent());
    }
  }

  // ---- must slot ----

  @Test
  public void mustSlot_complementaryNumericPair_doesNotApply() {
    RangeOperator lower = numericLower("score", 50L, true);
    RangeOperator upper = numericUpper("score", 100L, false);
    CompoundOperator input = compoundWithMust(List.of(lower, upper));

    Operator result = this.rule.apply(input, newContext());
    assertSame("rule must not fire in the must slot", result, input);
  }

  // ---- doesNotAffect semantics ----

  @Test
  public void doesNotAffect_equalLists_fuseCarriesList() {
    RangeOperator lower = numericLowerDna("x", 1L, List.of("a", "b"));
    RangeOperator upper = numericUpperDna("x", 10L, List.of("a", "b"));
    CompoundOperator input = compoundWithFilter(List.of(lower, upper));

    CompoundOperator result = applyRule(input);

    RangeOperator fused = (RangeOperator) result.filter().operators().get(0);
    assertEquals(Optional.of(List.of("a", "b")), fused.doesNotAffect());
  }

  @Test
  public void doesNotAffect_equalSetsDifferentOrder_fuses() {
    RangeOperator lower = numericLowerDna("x", 1L, List.of("a", "b"));
    RangeOperator upper = numericUpperDna("x", 10L, List.of("b", "a"));
    CompoundOperator input = compoundWithFilter(List.of(lower, upper));

    CompoundOperator result = applyRule(input);

    assertEquals(1, result.filter().operators().size());
    // The fused operator keeps the lower-bound operator's value.
    RangeOperator fused = (RangeOperator) result.filter().operators().get(0);
    assertEquals(Optional.of(List.of("a", "b")), fused.doesNotAffect());
  }

  @Test
  public void doesNotAffect_absentAndExplicitlyEmpty_fuses() {
    // An absent doesNotAffect and an explicitly empty one both mean "affects every facet".
    RangeOperator lower = numericLower("x", 1L, false);
    RangeOperator upper = numericUpperDna("x", 10L, List.of());
    CompoundOperator input = compoundWithFilter(List.of(lower, upper));

    CompoundOperator result = applyRule(input);

    assertEquals(1, result.filter().operators().size());
  }

  @Test
  public void doesNotAffect_differingLists_doesNotApply() {
    // doesNotAffect excludes its operator from counting the named facets; a fused operator
    // cannot express excluding only one half of the pair, so differing lists must block fusion.
    RangeOperator lower = numericLowerDna("x", 1L, List.of("a", "b"));
    RangeOperator upper = numericUpperDna("x", 10L, List.of("b", "c"));
    CompoundOperator input = compoundWithFilter(List.of(lower, upper));

    Operator result = this.rule.apply(input, newContext());
    assertSame("rule must not fire when doesNotAffect lists differ", result, input);
  }

  @Test
  public void doesNotAffect_onlyOneOperandDeclares_doesNotApply() {
    RangeOperator lower = numericLowerDna("x", 1L, List.of("a"));
    RangeOperator upper = numericUpper("x", 10L, false); // no doesNotAffect
    CompoundOperator input = compoundWithFilter(List.of(lower, upper));

    Operator result = this.rule.apply(input, newContext());
    assertSame("rule must not fire when only one operator declares doesNotAffect", result, input);
  }

  // ---- guard conditions that prevent firing ----

  @Test
  public void embeddedFieldsIndex_fuses() {
    // Fusion no longer needs Lucene field names at rule time, so an index with embedded fields
    // does not block it; the per-scope array-data decision happens at translation.
    RangeOperator lower = numericLower("x", 1L, false);
    RangeOperator upper = numericUpper("x", 10L, false);
    CompoundOperator input = compoundWithFilter(List.of(lower, upper));

    QueryFactoryContext ctx = Mockito.mock(QueryFactoryContext.class);
    Mockito.when(ctx.isIndexWithEmbeddedFields()).thenReturn(true);
    CompoundOperator result = (CompoundOperator) this.rule.apply(input, newContext(ctx));

    assertNotSame("rule expected to fire on an index with embedded fields", result, input);
    assertEquals(1, result.filter().operators().size());
  }

  @Test
  public void differentPaths_doesNotApply() {
    RangeOperator lower = numericLower("a", 1L, false);
    RangeOperator upper = numericUpper("b", 10L, false);
    CompoundOperator input = compoundWithFilter(List.of(lower, upper));

    Operator result = this.rule.apply(input, newContext());
    assertSame("rule must not fire when paths differ", result, input);
  }

  @Test
  public void multiPathOperators_samePathLists_doesNotApply() {
    // A multi-path range matches when any one path is in range: {x: 20, y: 0} satisfies
    // (x > 1 or y > 1) and (x < 10 or y < 10), but no single path satisfies the fused range.
    List<FieldPath> paths = List.of(FieldPath.newRoot("x"), FieldPath.newRoot("y"));
    RangeOperator lower =
        new RangeOperator(
            Score.defaultScore(),
            paths,
            new NumericRangeBound(Optional.of(new LongPoint(1L)), Optional.empty(), false, false),
            Optional.empty());
    RangeOperator upper =
        new RangeOperator(
            Score.defaultScore(),
            paths,
            new NumericRangeBound(Optional.empty(), Optional.of(new LongPoint(10L)), false, false),
            Optional.empty());
    CompoundOperator input = compoundWithFilter(List.of(lower, upper));

    Operator result = this.rule.apply(input, newContext());
    assertSame("rule must not fire for multi-path operators", result, input);
  }

  @Test
  public void twoLowerBounds_doesNotApply() {
    RangeOperator lower1 = numericLower("x", 1L, false);
    RangeOperator lower2 = numericLower("x", 5L, true);
    CompoundOperator input = compoundWithFilter(List.of(lower1, lower2));

    Operator result = this.rule.apply(input, newContext());
    assertSame("rule must not fire when both operators are lower-bound", result, input);
  }

  @Test
  public void contradictoryPair_lowerAboveUpper_doesNotApply() {
    // gt 10 + lt 5 is complementary by shape, but the fused bound would violate the closed-range
    // invariant (lower above upper), so the pair is left unchanged.
    RangeOperator lower = numericLower("x", 10L, false);
    RangeOperator upper = numericUpper("x", 5L, false);
    CompoundOperator input = compoundWithFilter(List.of(lower, upper));

    Operator result = this.rule.apply(input, newContext());
    assertSame("rule must not fire when the lower bound exceeds the upper bound", result, input);
  }

  @Test
  public void equalBoundsWithExclusiveEndpoint_doesNotApply() {
    // gte 5 + lt 5: equal endpoints form a valid closed range only when both sides are inclusive.
    RangeOperator lower = numericLower("x", 5L, true);
    RangeOperator upper = numericUpper("x", 5L, false);
    CompoundOperator input = compoundWithFilter(List.of(lower, upper));

    Operator result = this.rule.apply(input, newContext());
    assertSame("rule must not fire when equal bounds are not both inclusive", result, input);
  }

  @Test
  public void equalBoundsBothInclusive_fuses() {
    RangeOperator lower = numericLower("x", 5L, true);
    RangeOperator upper = numericUpper("x", 5L, true);
    CompoundOperator input = compoundWithFilter(List.of(lower, upper));

    CompoundOperator result = applyRule(input);

    assertEquals(1, result.filter().operators().size());
  }

  @Test
  public void mismatchedBoundTypes_doesNotApply() {
    RangeOperator numericLower = numericLower("x", 1L, false);
    RangeOperator dateLower = dateLower("x", new Date(0), false);
    // Both are lower-bound; also mismatched type — neither condition is met
    CompoundOperator input = compoundWithFilter(List.of(numericLower, dateLower));

    Operator result = this.rule.apply(input, newContext());
    assertSame("rule must not fire on mismatched bound types", result, input);
  }

  @Test
  public void stringBoundType_doesNotApply() {
    // StringRangeBound is not supported by the rule
    RangeOperator strLower = stringLower("name", "a");
    RangeOperator strUpper = stringUpper("name", "z");
    CompoundOperator input = compoundWithFilter(List.of(strLower, strUpper));

    Operator result = this.rule.apply(input, newContext());
    assertSame("rule must not fire for non-numeric/date bound types", result, input);
  }

  @Test
  public void nonRangeOperatorInCompound_doesNotApply() {
    // A compound with no RangeOperators at all should not fire
    CompoundOperator nestedCompound =
        new CompoundOperator(
            Score.defaultScore(),
            Optional.of(new CompoundClause(List.of(numericLower("x", 1L, false)))),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            0,
            Optional.empty());
    CompoundOperator input = compoundWithFilter(List.of(nestedCompound));

    Operator result = this.rule.apply(input, newContext());
    assertSame("rule must not fire with no RangeOperator pair", result, input);
  }

  @Test
  public void nonCompoundOperator_doesNotApply() {
    RangeOperator lower = numericLower("x", 1L, false);

    Operator result = this.rule.apply(lower, newContext());
    assertSame("rule must not match a non-CompoundOperator", result, lower);
  }

  @Test
  public void idempotent_fusedOutputDoesNotReapply() {
    RangeOperator lower = numericLower("price", 1L, false);
    RangeOperator upper = numericUpper("price", 10L, false);
    CompoundOperator input = compoundWithFilter(List.of(lower, upper));

    CompoundOperator once = applyRule(input);
    Operator twice = this.rule.apply(once, newContext());
    assertSame(
        "second application must not fire (fused operator is fully-bounded, not half-open)",
        twice,
        once);
  }

  // ---- construction helpers ----

  private static RangeOperator numericLower(String path, long value, boolean inclusive) {
    NumericRangeBound bounds =
        new NumericRangeBound(
            Optional.of(new LongPoint(value)), Optional.empty(), inclusive, false);
    return new RangeOperator(
        Score.defaultScore(), List.of(FieldPath.newRoot(path)), bounds, Optional.empty());
  }

  private static RangeOperator numericUpper(String path, long value, boolean inclusive) {
    NumericRangeBound bounds =
        new NumericRangeBound(
            Optional.empty(), Optional.of(new LongPoint(value)), false, inclusive);
    return new RangeOperator(
        Score.defaultScore(), List.of(FieldPath.newRoot(path)), bounds, Optional.empty());
  }

  private static RangeOperator numericUpperDouble(String path, double value) {
    NumericRangeBound bounds =
        new NumericRangeBound(Optional.empty(), Optional.of(new DoublePoint(value)), false, false);
    return new RangeOperator(
        Score.defaultScore(), List.of(FieldPath.newRoot(path)), bounds, Optional.empty());
  }

  private static RangeOperator numericLowerDna(String path, long value, List<String> dna) {
    NumericRangeBound bounds =
        new NumericRangeBound(Optional.of(new LongPoint(value)), Optional.empty(), false, false);
    return new RangeOperator(
        Score.defaultScore(), List.of(FieldPath.newRoot(path)), bounds, Optional.of(dna));
  }

  private static RangeOperator numericUpperDna(String path, long value, List<String> dna) {
    NumericRangeBound bounds =
        new NumericRangeBound(Optional.empty(), Optional.of(new LongPoint(value)), false, false);
    return new RangeOperator(
        Score.defaultScore(), List.of(FieldPath.newRoot(path)), bounds, Optional.of(dna));
  }

  private static RangeOperator dateLower(String path, Date value, boolean inclusive) {
    DateRangeBound bounds =
        new DateRangeBound(Optional.of(new DatePoint(value)), Optional.empty(), inclusive, false);
    return new RangeOperator(
        Score.defaultScore(), List.of(FieldPath.newRoot(path)), bounds, Optional.empty());
  }

  private static RangeOperator dateUpper(String path, Date value, boolean inclusive) {
    DateRangeBound bounds =
        new DateRangeBound(Optional.empty(), Optional.of(new DatePoint(value)), false, inclusive);
    return new RangeOperator(
        Score.defaultScore(), List.of(FieldPath.newRoot(path)), bounds, Optional.empty());
  }

  private static RangeOperator stringLower(String path, String value) {
    StringRangeBound bounds =
        new StringRangeBound(Optional.of(new StringPoint(value)), Optional.empty(), false, false);
    return new RangeOperator(
        Score.defaultScore(), List.of(FieldPath.newRoot(path)), bounds, Optional.empty());
  }

  private static RangeOperator stringUpper(String path, String value) {
    StringRangeBound bounds =
        new StringRangeBound(Optional.empty(), Optional.of(new StringPoint(value)), false, false);
    return new RangeOperator(
        Score.defaultScore(), List.of(FieldPath.newRoot(path)), bounds, Optional.empty());
  }

  private static CompoundOperator compoundWithFilter(List<? extends Operator> operators) {
    return new CompoundOperator(
        Score.defaultScore(),
        Optional.of(new CompoundClause(operators)),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        0,
        Optional.empty());
  }

  private static CompoundOperator compoundWithMust(List<? extends Operator> operators) {
    return new CompoundOperator(
        Score.defaultScore(),
        Optional.empty(),
        Optional.of(new CompoundClause(operators)),
        Optional.empty(),
        Optional.empty(),
        0,
        Optional.empty());
  }

  private CompoundOperator applyRule(CompoundOperator input) {
    Operator result = this.rule.apply(input, newContext());
    assertNotSame("rule expected to fire", result, input);
    assertTrue(result instanceof CompoundOperator);
    return (CompoundOperator) result;
  }

  private static RewriteContext newContext() {
    QueryFactoryContext factoryContext = Mockito.mock(QueryFactoryContext.class);
    Mockito.when(factoryContext.isIndexWithEmbeddedFields()).thenReturn(false);
    return newContext(factoryContext);
  }

  private static RewriteContext newContext(QueryFactoryContext factoryContext) {
    IndexMetricsUpdater.QueryingMetricsUpdater metrics =
        new IndexMetricsUpdater.QueryingMetricsUpdater(SearchIndex.mockMetricsFactory());
    return new RewriteContext(
        factoryContext, DynamicFeatureFlagRegistry.empty(), metrics, new RewriteAudit());
  }
}
