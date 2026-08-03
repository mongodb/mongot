package com.xgen.mongot.index.query.optimizer.rules;

import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlags;
import com.xgen.mongot.index.query.operators.bound.DateRangeBound;
import com.xgen.mongot.index.query.operators.bound.NumericRangeBound;
import com.xgen.mongot.index.query.operators.bound.RangeBound;
import com.xgen.mongot.index.query.operators.mql.Clause;
import com.xgen.mongot.index.query.operators.mql.ClosedRangeBoundComparisonOperator;
import com.xgen.mongot.index.query.operators.mql.MqlFilterOperator;
import com.xgen.mongot.index.query.operators.mql.OpenRangeBoundComparisonOperator;
import com.xgen.mongot.index.query.operators.mql.SimpleClause;
import com.xgen.mongot.index.query.operators.type.ValueType;
import com.xgen.mongot.index.query.optimizer.ClauseRewriteRule;
import com.xgen.mongot.index.query.optimizer.RewriteContext;
import com.xgen.mongot.util.bson.parser.BsonParseContext;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import java.util.List;
import java.util.Optional;

/**
 * Collapses two complementary half-open {@link OpenRangeBoundComparisonOperator}s on the same
 * {@link SimpleClause} into a single {@link ClosedRangeBoundComparisonOperator}, which the factory
 * translates as one Lucene closed-range query when the field is single-valued (with a safe
 * AND-of-two-opens fallback when not).
 *
 * <p>Eligibility:
 *
 * <ul>
 *   <li>Exactly two operators on the clause.
 *   <li>Both are {@link OpenRangeBoundComparisonOperator}s — one {@link
 *       MqlFilterOperator.Category#LOWER_BOUND} (i.e., {@code $gt} or {@code $gte}) and one {@link
 *       MqlFilterOperator.Category#UPPER_BOUND} ({@code $lt} or {@code $lte}); operand order does
 *       not matter.
 *   <li>Both bounds carry the same {@link ValueType}, restricted to {@code NUMBER} or {@code DATE}
 *       (the BKD-scan-dominated translations).
 *   <li>The fused bounds satisfy the closed-range invariant enforced at parse time: the lower
 *       endpoint must not exceed the upper, and equal endpoints require both sides inclusive.
 *       Contradictory pairs are left unchanged.
 * </ul>
 *
 * <p>The rule emits the closed operator without consulting the index — the multi-valued fallback
 * lives in the factory translation step (see {@code VectorSearchFilterQueryFactory}). This
 * "optimistic emit, factory falls back" split keeps the rule's pre-condition trivially observable
 * and concentrates the index-state check at one site.
 */
public final class RangeBoundsMergeMqlSimpleClauseRule implements ClauseRewriteRule {

  public static final String NAME = "RangeBoundsMergeMqlSimpleClauseRule";

  @Override
  public String name() {
    return NAME;
  }

  @Override
  public DynamicFeatureFlags flag() {
    return DynamicFeatureFlags.RANGE_BOUNDS_MERGE_OPTIMIZATION;
  }

  @Override
  public Clause apply(Clause clause, RewriteContext context) {
    if (!(clause instanceof SimpleClause simpleClause)) {
      return clause;
    }
    List<MqlFilterOperator> operators = simpleClause.mqlFilterOperators();
    if (operators.size() != 2) {
      return clause;
    }
    if (!(operators.get(0) instanceof OpenRangeBoundComparisonOperator first)
        || !(operators.get(1) instanceof OpenRangeBoundComparisonOperator second)) {
      return clause;
    }
    if (first.getValueType() != second.getValueType()) {
      return clause;
    }
    ValueType valueType = first.getValueType();
    if (valueType != ValueType.NUMBER && valueType != ValueType.DATE) {
      return clause;
    }

    OpenRangeBoundComparisonOperator lower;
    OpenRangeBoundComparisonOperator upper;
    if (first.getCategory() == MqlFilterOperator.Category.LOWER_BOUND
        && second.getCategory() == MqlFilterOperator.Category.UPPER_BOUND) {
      lower = first;
      upper = second;
    } else if (first.getCategory() == MqlFilterOperator.Category.UPPER_BOUND
        && second.getCategory() == MqlFilterOperator.Category.LOWER_BOUND) {
      lower = second;
      upper = first;
    } else {
      return clause;
    }

    Optional<RangeBound<? extends Comparable<?>>> mergedBounds =
        mergeBounds(lower.getBounds(), upper.getBounds());
    if (mergedBounds.isEmpty()) {
      return clause;
    }
    return new SimpleClause(
        simpleClause.path(), List.of(new ClosedRangeBoundComparisonOperator(mergedBounds.get())));
  }

  private static Optional<RangeBound<? extends Comparable<?>>> mergeBounds(
      RangeBound<? extends Comparable<?>> lowerBounds,
      RangeBound<? extends Comparable<?>> upperBounds) {
    try {
      RangeBound<? extends Comparable<?>> merged =
          switch (lowerBounds) {
            case NumericRangeBound numericLower ->
                NumericRangeBound.create(
                    BsonParseContext.root(),
                    numericLower.getLower(),
                    ((NumericRangeBound) upperBounds).getUpper(),
                    numericLower.lowerInclusive(),
                    upperBounds.upperInclusive());
            case DateRangeBound dateLower ->
                DateRangeBound.create(
                    BsonParseContext.root(),
                    dateLower.getLower(),
                    ((DateRangeBound) upperBounds).getUpper(),
                    dateLower.lowerInclusive(),
                    upperBounds.upperInclusive());
            default ->
                throw new IllegalStateException(
                    "RangeBoundsMergeMqlSimpleClauseRule reached unsupported bound type: "
                        + lowerBounds.getClass().getSimpleName());
          };
      return Optional.of(merged);
    } catch (BsonParseException e) {
      // The validated factory refused the fused form: the pair is contradictory (lower above
      // upper, or equal endpoints with an exclusive side), so leave the operators unfused.
      return Optional.empty();
    }
  }
}
