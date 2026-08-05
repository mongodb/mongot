package com.xgen.mongot.index.query.optimizer.rules;

import com.xgen.mongot.index.query.optimizer.RuleRegistry;

/** The list of optimization rules enabled for the {@link QueryOptimizer}. */
public final class DefaultRuleRegistry {

  private static final RuleRegistry INSTANCE =
      RuleRegistry.builder()
          .addClauseRule(new RangeBoundsMergeMqlAndClauseRule())
          .addClauseRule(new RangeBoundsMergeMqlSimpleClauseRule())
          .addOperatorRule(new RangeBoundsMergeOperatorRule())
          .build();

  private DefaultRuleRegistry() {}

  /** The process-wide registry of optimizer rules. */
  public static RuleRegistry get() {
    return INSTANCE;
  }
}
