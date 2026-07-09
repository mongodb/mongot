package com.xgen.mongot.index.query.optimizer;

import com.xgen.mongot.index.query.operators.mql.Clause;

/**
 * Rewrites a {@link Clause} into a semantically-equivalent shape. Called by {@link
 * ClauseRewriteEngine} post-order; return the input instance itself (not an equal copy) to signal
 * no rewrite.
 */
public non-sealed interface ClauseRewriteRule extends RewriteRule {

  /** Deterministic rewrite; result must match the same documents as the input. */
  Clause apply(Clause clause, RewriteContext context);
}
