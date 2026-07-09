package com.xgen.mongot.index.query.optimizer;

import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlags;

/** Common parent for clause and operator rewrite rules; sealed to force dispatch on extension. */
public sealed interface RewriteRule permits ClauseRewriteRule, OperatorRewriteRule {

  /** Stable identifier used in metrics, explain output, and the audit. */
  String name();

  /** Per-rule feature flag; rules whose flag evaluates false are skipped. */
  DynamicFeatureFlags flag();
}
