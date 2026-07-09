package com.xgen.mongot.index.query.optimizer;

import com.google.common.flogger.FluentLogger;
import com.google.errorprone.annotations.Var;
import com.xgen.mongot.index.query.operators.mql.AndClause;
import com.xgen.mongot.index.query.operators.mql.Clause;
import com.xgen.mongot.index.query.operators.mql.CompoundClause;
import com.xgen.mongot.index.query.operators.mql.NorClause;
import com.xgen.mongot.index.query.operators.mql.OrClause;
import com.xgen.mongot.index.query.operators.mql.SimpleClause;
import java.util.ArrayList;
import java.util.List;

/**
 * Bottom-up rewrite engine for the {@link Clause} hierarchy. Rewrites children first, then applies
 * rules at the parent. Iterates to a fixed point capped at {@link #MAX_ITERATIONS}.
 */
public final class ClauseRewriteEngine {

  static final int MAX_ITERATIONS = 8;
  private static final FluentLogger LOGGER = FluentLogger.forEnclosingClass();

  private final List<ClauseRewriteRule> rules;

  ClauseRewriteEngine(List<ClauseRewriteRule> rules) {
    this.rules = rules;
  }

  public Clause rewrite(Clause root, RewriteContext context) {
    @Var Clause current = root;
    @Var int iterations = 0;
    while (true) {
      iterations++;
      Clause next = rewriteOnce(current, context);
      if (next == current) {
        break;
      }
      current = next;
      if (iterations >= MAX_ITERATIONS) {
        LOGGER.atWarning().log(
            "QueryOptimizer ClauseRewriteEngine exceeded %d fixed-point iterations; "
                + "emitting current tree to avoid an infinite loop. fired=%s",
            MAX_ITERATIONS, context.audit().firedRules());
        break;
      }
    }
    context.audit().recordIterations(iterations);
    return current;
  }

  private Clause rewriteOnce(Clause clause, RewriteContext context) {
    Clause withRewrittenChildren = rewriteChildren(clause, context);
    return applyRules(withRewrittenChildren, context);
  }

  private Clause rewriteChildren(Clause clause, RewriteContext context) {
    return switch (clause) {
      case AndClause andClause -> rewriteCompoundChildren(andClause, context);
      case OrClause orClause -> rewriteCompoundChildren(orClause, context);
      case NorClause norClause -> rewriteCompoundChildren(norClause, context);
      case SimpleClause simpleClause -> simpleClause;
    };
  }

  private CompoundClause rewriteCompoundChildren(CompoundClause compound, RewriteContext context) {
    List<Clause> originalChildren = compound.getClauses();
    List<Clause> rewrittenChildren = new ArrayList<>(originalChildren.size());
    @Var boolean anyChanged = false;
    for (Clause child : originalChildren) {
      Clause rewritten = rewriteOnce(child, context);
      if (rewritten != child) {
        anyChanged = true;
      }
      rewrittenChildren.add(rewritten);
    }
    if (!anyChanged) {
      return compound;
    }
    return rebuildCompound(compound, rewrittenChildren);
  }

  private static CompoundClause rebuildCompound(CompoundClause compound, List<Clause> children) {
    return switch (compound) {
      case AndClause andClause -> new AndClause(children, andClause.isExplicitAnd());
      case OrClause orClause -> new OrClause(children);
      case NorClause norClause -> new NorClause(children);
    };
  }

  private Clause applyRules(Clause clause, RewriteContext context) {
    for (ClauseRewriteRule rule : this.rules) {
      Clause rewritten = rule.apply(clause, context);
      if (rewritten != clause) {
        context.audit().recordFiring(rule.name());
        return rewritten;
      }
    }
    return clause;
  }
}
