package com.xgen.mongot.index.query.optimizer;

import com.google.common.collect.ImmutableList;
import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlagRegistry;
import java.util.List;

/**
 * Ordered registry of rewrite rules; produces per-query subsets via {@link #forContext} with each
 * rule's feature flag evaluated once per query, not per node.
 */
public final class RuleRegistry {

  private final ImmutableList<ClauseRewriteRule> clauseRules;
  private final ImmutableList<OperatorRewriteRule> operatorRules;

  private RuleRegistry(
      ImmutableList<ClauseRewriteRule> clauseRules,
      ImmutableList<OperatorRewriteRule> operatorRules) {
    this.clauseRules = clauseRules;
    this.operatorRules = operatorRules;
  }

  /** Test-only: registry with no rules registered. */
  public static RuleRegistry empty() {
    return new RuleRegistry(ImmutableList.of(), ImmutableList.of());
  }

  /** Test-only: build a custom registry. */
  public static Builder builder() {
    return new Builder();
  }

  /** Per-query view excluding rules whose feature flag evaluates false. */
  public Filtered forContext(DynamicFeatureFlagRegistry dynamicFeatureFlagRegistry) {
    ImmutableList.Builder<ClauseRewriteRule> activeClauseRules = ImmutableList.builder();
    for (ClauseRewriteRule rule : this.clauseRules) {
      if (dynamicFeatureFlagRegistry.evaluateClusterInvariant(rule.flag())) {
        activeClauseRules.add(rule);
      }
    }
    ImmutableList.Builder<OperatorRewriteRule> activeOperatorRules = ImmutableList.builder();
    for (OperatorRewriteRule rule : this.operatorRules) {
      if (dynamicFeatureFlagRegistry.evaluateClusterInvariant(rule.flag())) {
        activeOperatorRules.add(rule);
      }
    }
    return new Filtered(activeClauseRules.build(), activeOperatorRules.build());
  }

  /** Per-query, pre-filtered view of the registry. */
  public record Filtered(
      List<ClauseRewriteRule> clauseRules, List<OperatorRewriteRule> operatorRules) {

    public boolean isEmpty() {
      return this.clauseRules.isEmpty() && this.operatorRules.isEmpty();
    }
  }

  /** Builder for {@link RuleRegistry}; rules are added in the order they should be tried. */
  public static final class Builder {
    private final ImmutableList.Builder<ClauseRewriteRule> clauseRules = ImmutableList.builder();
    private final ImmutableList.Builder<OperatorRewriteRule> operatorRules =
        ImmutableList.builder();

    private Builder() {}

    public Builder addClauseRule(ClauseRewriteRule rule) {
      this.clauseRules.add(rule);
      return this;
    }

    public Builder addOperatorRule(OperatorRewriteRule rule) {
      this.operatorRules.add(rule);
      return this;
    }

    public RuleRegistry build() {
      return new RuleRegistry(this.clauseRules.build(), this.operatorRules.build());
    }
  }
}
