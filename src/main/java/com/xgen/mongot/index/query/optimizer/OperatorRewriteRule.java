package com.xgen.mongot.index.query.optimizer;

import com.xgen.mongot.index.query.operators.Operator;

/**
 * Rewrites an {@link Operator} into a semantically-equivalent shape. Called by {@link
 * OperatorRewriteEngine} post-order; return the input instance itself (not an equal copy) to signal
 * no rewrite.
 */
public non-sealed interface OperatorRewriteRule extends RewriteRule {

  /** Deterministic rewrite; result must match the same documents as the input. */
  Operator apply(Operator operator, RewriteContext context);
}
