package com.xgen.mongot.index.query.optimizer.rules;

import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlags;
import com.xgen.mongot.index.query.operators.CompoundClause;
import com.xgen.mongot.index.query.operators.CompoundOperator;
import com.xgen.mongot.index.query.operators.Operator;
import com.xgen.mongot.index.query.operators.RangeOperator;
import com.xgen.mongot.index.query.operators.bound.DateRangeBound;
import com.xgen.mongot.index.query.operators.bound.NumericRangeBound;
import com.xgen.mongot.index.query.operators.bound.RangeBound;
import com.xgen.mongot.index.query.optimizer.OperatorRewriteRule;
import com.xgen.mongot.index.query.optimizer.RewriteContext;
import com.xgen.mongot.util.bson.parser.BsonParseContext;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * Fuses complementary half-open {@link RangeOperator} pairs on the same path within the same {@link
 * CompoundOperator} {@code filter} slot into single fully-bounded {@link RangeOperator}s. All
 * eligible pairs in the slot fuse in one rule application, so a many-pair slot does not spend one
 * engine fixed-point iteration per pair.
 *
 * <p>Like {@link RangeBoundsMergeMqlSimpleClauseRule}, the rule fuses without consulting the index
 * — the multi-valued fallback lives in the factory translation step (see {@code
 * RangeQueryFactory}). The fused operator is marked {@link RangeOperator#fusedByOptimizer()} so
 * translation can distinguish it from a user-written closed range, whose per-element array
 * semantics are documented behavior and must not be decomposed.
 *
 * <p>Eligible bound types: {@link NumericRangeBound} and {@link DateRangeBound} only (the
 * BKD-scan-dominated translations). Both operators in the pair must carry the same bound type and
 * the same single path: a multi-path range matches when any one of its paths is in range, so
 * different paths may satisfy the two halves of the pair and fusing them would drop matches.
 * Overlapping pairs (e.g., two lower bounds) are left unchanged, as are contradictory pairs whose
 * fused form would violate the closed-range invariant enforced at parse time (a lower endpoint
 * above the upper, or equal endpoints with an exclusive side).
 *
 * <p>Score is taken from the lower-bound operator. {@code doesNotAffect} exclusions apply per
 * operator during facet counting, and a single fused operator cannot express excluding only one
 * half of the pair, so a pair fuses only when both operators carry the same {@code doesNotAffect}
 * fields (order-insensitive, absent equivalent to empty); the fused operator keeps the lower-bound
 * operator's value.
 */
public final class RangeBoundsMergeOperatorRule implements OperatorRewriteRule {

  public static final String NAME = "RangeBoundsMergeOperatorRule";

  @Override
  public String name() {
    return NAME;
  }

  @Override
  public DynamicFeatureFlags flag() {
    return DynamicFeatureFlags.RANGE_BOUNDS_MERGE_OPTIMIZATION;
  }

  @Override
  public Operator apply(Operator operator, RewriteContext context) {
    if (!(operator instanceof CompoundOperator compound)) {
      return operator;
    }

    // Only the filter slot is fused: must is not supported because each must clause contributes
    // its own score, so fusing a must pair would change user-visible scoring.
    Optional<List<Operator>> rewrittenFilter = fuseEligiblePairs(compound.filter().operators());
    if (rewrittenFilter.isPresent()) {
      return new CompoundOperator(
          compound.score(),
          new CompoundClause(rewrittenFilter.get()),
          compound.must(),
          compound.mustNot(),
          compound.should(),
          compound.minimumShouldMatch(),
          compound.doesNotAffect());
    }

    return operator;
  }

  private static Optional<List<Operator>> fuseEligiblePairs(List<? extends Operator> operators) {
    List<Operator> result = new ArrayList<>(operators);
    for (int i = 0; i < result.size(); i++) {
      if (!(result.get(i) instanceof RangeOperator op1) || !isEligibleHalfOpen(op1)) {
        continue;
      }
      for (int j = i + 1; j < result.size(); j++) {
        if (!(result.get(j) instanceof RangeOperator op2) || !isEligibleHalfOpen(op2)) {
          continue;
        }
        // A multi-path range is a disjunction across its paths, so different paths may satisfy
        // each half of the pair; only single-path pairs fuse soundly.
        if (op1.paths().size() != 1 || !op1.paths().equals(op2.paths())) {
          continue;
        }
        if (!isSameSupportedBoundType(op1.bounds(), op2.bounds())) {
          continue;
        }
        if (!areComplementary(op1, op2)) {
          continue;
        }
        if (!sameDoesNotAffect(op1.doesNotAffect(), op2.doesNotAffect())) {
          continue;
        }

        RangeOperator lower = isLowerOnly(op1) ? op1 : op2;
        RangeOperator upper = isLowerOnly(op1) ? op2 : op1;

        Optional<RangeBound<?>> fusedBounds = fuseBounds(lower.bounds(), upper.bounds());
        if (fusedBounds.isEmpty()) {
          continue;
        }

        RangeOperator fused =
            new RangeOperator(
                lower.score(), lower.paths(), fusedBounds.get(), lower.doesNotAffect(), true);

        result.set(i, fused);
        result.remove(j);
        // The fused operator is fully bounded and no longer eligible; move to the next element.
        break;
      }
    }
    return result.size() == operators.size() ? Optional.empty() : Optional.of(result);
  }

  private static boolean isEligibleHalfOpen(RangeOperator op) {
    boolean lowerOnly = op.bounds().getLower().isPresent() && op.bounds().getUpper().isEmpty();
    boolean upperOnly = op.bounds().getLower().isEmpty() && op.bounds().getUpper().isPresent();
    return lowerOnly || upperOnly;
  }

  private static boolean isSameSupportedBoundType(RangeBound<?> a, RangeBound<?> b) {
    return (a instanceof NumericRangeBound && b instanceof NumericRangeBound)
        || (a instanceof DateRangeBound && b instanceof DateRangeBound);
  }

  private static boolean areComplementary(RangeOperator op1, RangeOperator op2) {
    return isLowerOnly(op1) != isLowerOnly(op2);
  }

  private static boolean isLowerOnly(RangeOperator op) {
    return op.bounds().getLower().isPresent() && op.bounds().getUpper().isEmpty();
  }

  private static Optional<RangeBound<?>> fuseBounds(RangeBound<?> lower, RangeBound<?> upper) {
    try {
      RangeBound<?> fused =
          switch (lower) {
            case NumericRangeBound numLower ->
                NumericRangeBound.create(
                    BsonParseContext.root(),
                    numLower.getLower(),
                    ((NumericRangeBound) upper).getUpper(),
                    lower.lowerInclusive(),
                    upper.upperInclusive());
            case DateRangeBound dateLower ->
                DateRangeBound.create(
                    BsonParseContext.root(),
                    dateLower.getLower(),
                    ((DateRangeBound) upper).getUpper(),
                    lower.lowerInclusive(),
                    upper.upperInclusive());
            default ->
                throw new IllegalStateException(
                    "RangeBoundsMergeOperatorRule reached unsupported bound type: "
                        + lower.getClass().getSimpleName());
          };
      return Optional.of(fused);
    } catch (BsonParseException e) {
      // The validated factory refused the fused form: the pair is contradictory (lower above
      // upper, or equal endpoints with an exclusive side), so leave the operators unfused.
      return Optional.empty();
    }
  }

  /**
   * {@code doesNotAffect} excludes its operator from counting the named facets; a fused operator
   * cannot express excluding only one half of the pair, so the operators' fields must match.
   */
  private static boolean sameDoesNotAffect(Optional<List<String>> a, Optional<List<String>> b) {
    return Set.copyOf(a.orElse(List.of())).equals(Set.copyOf(b.orElse(List.of())));
  }
}
