package com.xgen.mongot.index.query.optimizer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Mutable audit of rule firings and iteration count for one optimize call. Returned via {@link
 * QueryOptimizer.Result}; not thread-safe.
 */
public final class RewriteAudit {
  private final List<String> firedRules = new ArrayList<>();
  private int iterations;

  public void recordFiring(String ruleName) {
    this.firedRules.add(ruleName);
  }

  public void recordIterations(int iterations) {
    this.iterations = iterations;
  }

  public List<String> firedRules() {
    return Collections.unmodifiableList(this.firedRules);
  }

  public int iterations() {
    return this.iterations;
  }

  public boolean isEmpty() {
    return this.firedRules.isEmpty();
  }
}
