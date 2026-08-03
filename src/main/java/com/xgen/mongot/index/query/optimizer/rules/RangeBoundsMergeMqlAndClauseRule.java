package com.xgen.mongot.index.query.optimizer.rules;

import com.google.errorprone.annotations.Var;
import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlags;
import com.xgen.mongot.index.query.operators.mql.AndClause;
import com.xgen.mongot.index.query.operators.mql.Clause;
import com.xgen.mongot.index.query.operators.mql.MqlFilterOperator;
import com.xgen.mongot.index.query.operators.mql.OpenRangeBoundComparisonOperator;
import com.xgen.mongot.index.query.operators.mql.SimpleClause;
import com.xgen.mongot.index.query.operators.type.ValueType;
import com.xgen.mongot.index.query.optimizer.ClauseRewriteRule;
import com.xgen.mongot.index.query.optimizer.RewriteContext;
import com.xgen.mongot.util.FieldPath;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Fuses pairs of single-operator {@link SimpleClause}s on the same path — each clause carrying a
 * complementary half-open {@link OpenRangeBoundComparisonOperator} — into one {@link SimpleClause}
 * per pair, containing both operators in {@code [lower, upper]} order.
 *
 * <p>The output {@link SimpleClause} with two open-range operators is exactly the shape that {@link
 * RangeBoundsMergeMqlSimpleClauseRule} matches, so the two rules compose within one fixed-point
 * iteration cycle: this rule normalizes the cross-clause form ({@code {$and: [{x: {$gt: 1}}, {x:
 * {$lt: 10}}]}}) into the same-clause form ({@code {x: {$gt: 1, $lt: 10}}}), which {@link
 * RangeBoundsMergeMqlSimpleClauseRule} then collapses into a single closed-range operator.
 *
 * <p>Eligible bound types: {@link ValueType#NUMBER} and {@link ValueType#DATE} only. Pairs match in
 * child order, and all eligible pairs fuse in one rule application, so a many-pair {@code $and} is
 * resolved all at once.
 */
public final class RangeBoundsMergeMqlAndClauseRule implements ClauseRewriteRule {

  public static final String NAME = "RangeBoundsMergeMqlAndClauseRule";

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
    if (!(clause instanceof AndClause andClause)) {
      return clause;
    }

    List<Clause> children = andClause.getClauses();
    List<Clause> result = new ArrayList<>(children.size());
    Map<BoundKey, Deque<Integer>> pendingLowers = new HashMap<>();
    Map<BoundKey, Deque<Integer>> pendingUppers = new HashMap<>();
    @Var boolean fusedAny = false;

    for (Clause child : children) {
      Optional<OpenRangeBoundComparisonOperator> maybeOp = extractSingleOpenRange(child);
      if (maybeOp.isEmpty()
          || (maybeOp.get().getValueType() != ValueType.NUMBER
              && maybeOp.get().getValueType() != ValueType.DATE)) {
        result.add(child);
        continue;
      }
      OpenRangeBoundComparisonOperator op = maybeOp.get();
      SimpleClause simpleClause = (SimpleClause) child;
      BoundKey key = new BoundKey(simpleClause.path(), op.getValueType());
      boolean isLower = op.getCategory() == MqlFilterOperator.Category.LOWER_BOUND;

      Deque<Integer> partners = (isLower ? pendingUppers : pendingLowers).get(key);
      if (partners == null || partners.isEmpty()) {
        (isLower ? pendingLowers : pendingUppers)
            .computeIfAbsent(key, unused -> new ArrayDeque<>())
            .addLast(result.size());
        result.add(child);
        continue;
      }

      // Fuse in place of the earlier clause, with the lower bound first.
      int partnerIndex = partners.removeFirst();
      SimpleClause partner = (SimpleClause) result.get(partnerIndex);
      OpenRangeBoundComparisonOperator partnerOp =
          (OpenRangeBoundComparisonOperator) partner.mqlFilterOperators().get(0);
      OpenRangeBoundComparisonOperator lower = isLower ? op : partnerOp;
      OpenRangeBoundComparisonOperator upper = isLower ? partnerOp : op;
      result.set(partnerIndex, new SimpleClause(partner.path(), List.of(lower, upper)));
      fusedAny = true;
    }

    if (!fusedAny) {
      return clause;
    }
    return new AndClause(result, andClause.isExplicitAnd());
  }

  private record BoundKey(FieldPath path, ValueType valueType) {}

  private static Optional<OpenRangeBoundComparisonOperator> extractSingleOpenRange(Clause clause) {
    if (!(clause instanceof SimpleClause sc)) {
      return Optional.empty();
    }
    if (sc.mqlFilterOperators().size() != 1) {
      return Optional.empty();
    }
    MqlFilterOperator op = sc.mqlFilterOperators().get(0);
    return op instanceof OpenRangeBoundComparisonOperator open
        ? Optional.of(open)
        : Optional.empty();
  }
}
