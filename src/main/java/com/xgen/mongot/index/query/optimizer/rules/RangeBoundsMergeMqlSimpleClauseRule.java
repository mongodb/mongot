package com.xgen.mongot.index.query.optimizer.rules;

import com.google.errorprone.annotations.Var;
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
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Collapses complementary half-open {@link OpenRangeBoundComparisonOperator} pairs on a {@link
 * SimpleClause} into single {@link ClosedRangeBoundComparisonOperator}s, which the factory
 * translates as one Lucene closed-range query when the field is single-valued (with a safe
 * AND-of-two-opens fallback when not).
 *
 * <p>The clause may carry any number of operators. Each {@link
 * MqlFilterOperator.Category#LOWER_BOUND} operator ({@code $gt} or {@code $gte}) pairs with the
 * earliest unpaired {@link MqlFilterOperator.Category#UPPER_BOUND} operator ({@code $lt} or {@code
 * $lte}) carrying the same {@link ValueType}, and vice versa; operand order does not matter, and
 * the fused operator replaces the pair's earlier member in place. Operators outside a fused pair —
 * non-range operators, unpaired bounds, and bounds of ineligible types — stay where they are:
 * operators on one {@code SimpleClause} are independent conjuncts, so fusing two of them cannot
 * affect the rest.
 *
 * <p>Pairing eligibility:
 *
 * <ul>
 *   <li>Both bounds carry the same {@link ValueType}, restricted to {@code NUMBER} or {@code DATE}
 *       (the BKD-scan-dominated translations).
 *   <li>The fused bounds satisfy the closed-range invariant enforced at parse time: the lower
 *       endpoint must not exceed the upper, and equal endpoints require both sides inclusive. A
 *       contradictory pair is left unfused and neither member is offered another partner — pairing
 *       is greedy in operator order, without backtracking.
 * </ul>
 *
 * <p>The rule emits closed operators without consulting the index — the multi-valued fallback lives
 * in the factory translation step (see {@code VectorSearchFilterQueryFactory}). This "optimistic
 * emit, factory falls back" split keeps the rule's pre-condition trivially observable and
 * concentrates the index-state check at one site.
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
    if (operators.size() < 2) {
      return clause;
    }

    List<MqlFilterOperator> result = new ArrayList<>(operators.size());
    Map<ValueType, Deque<Integer>> pendingLowers = new EnumMap<>(ValueType.class);
    Map<ValueType, Deque<Integer>> pendingUppers = new EnumMap<>(ValueType.class);
    @Var boolean fusedAny = false;

    for (MqlFilterOperator operator : operators) {
      if (!(operator instanceof OpenRangeBoundComparisonOperator open)
          || (open.getValueType() != ValueType.NUMBER && open.getValueType() != ValueType.DATE)) {
        result.add(operator);
        continue;
      }
      ValueType valueType = open.getValueType();
      boolean isLower = open.getCategory() == MqlFilterOperator.Category.LOWER_BOUND;

      Deque<Integer> partners = (isLower ? pendingUppers : pendingLowers).get(valueType);
      if (partners == null || partners.isEmpty()) {
        (isLower ? pendingLowers : pendingUppers)
            .computeIfAbsent(valueType, unused -> new ArrayDeque<>())
            .addLast(result.size());
        result.add(operator);
        continue;
      }

      int partnerIndex = partners.removeFirst();
      OpenRangeBoundComparisonOperator partner =
          (OpenRangeBoundComparisonOperator) result.get(partnerIndex);
      OpenRangeBoundComparisonOperator lower = isLower ? open : partner;
      OpenRangeBoundComparisonOperator upper = isLower ? partner : open;
      Optional<RangeBound<? extends Comparable<?>>> mergedBounds =
          mergeBounds(lower.getBounds(), upper.getBounds());
      if (mergedBounds.isEmpty()) {
        // Contradictory pair: keep both operators where they are, and offer neither to a later
        // complement — greedy pairing without backtracking.
        result.add(operator);
        continue;
      }
      // Fuse in place of the pair's earlier member.
      result.set(partnerIndex, new ClosedRangeBoundComparisonOperator(mergedBounds.get()));
      fusedAny = true;
    }

    if (!fusedAny) {
      return clause;
    }
    return new SimpleClause(simpleClause.path(), result);
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
