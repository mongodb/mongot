package com.xgen.mongot.index.query.optimizer;

import com.google.common.flogger.FluentLogger;
import com.google.errorprone.annotations.Var;
import com.xgen.mongot.index.query.operators.AllDocumentsOperator;
import com.xgen.mongot.index.query.operators.AutocompleteOperator;
import com.xgen.mongot.index.query.operators.CompoundClause;
import com.xgen.mongot.index.query.operators.CompoundOperator;
import com.xgen.mongot.index.query.operators.EmbeddedDocumentOperator;
import com.xgen.mongot.index.query.operators.EqualsOperator;
import com.xgen.mongot.index.query.operators.ExistsOperator;
import com.xgen.mongot.index.query.operators.GeoShapeOperator;
import com.xgen.mongot.index.query.operators.GeoWithinOperator;
import com.xgen.mongot.index.query.operators.HasAncestorOperator;
import com.xgen.mongot.index.query.operators.HasRootOperator;
import com.xgen.mongot.index.query.operators.InOperator;
import com.xgen.mongot.index.query.operators.KnnBetaOperator;
import com.xgen.mongot.index.query.operators.MoreLikeThisOperator;
import com.xgen.mongot.index.query.operators.NearOperator;
import com.xgen.mongot.index.query.operators.Operator;
import com.xgen.mongot.index.query.operators.PhraseOperator;
import com.xgen.mongot.index.query.operators.QueryStringOperator;
import com.xgen.mongot.index.query.operators.RangeOperator;
import com.xgen.mongot.index.query.operators.SearchOperator;
import com.xgen.mongot.index.query.operators.SpanOperator;
import com.xgen.mongot.index.query.operators.TermLevelOperator;
import com.xgen.mongot.index.query.operators.TermOperator;
import com.xgen.mongot.index.query.operators.TextOperator;
import com.xgen.mongot.index.query.operators.VectorSearchOperator;
import java.util.ArrayList;
import java.util.List;

/**
 * Bottom-up rewrite engine for the {@link Operator} hierarchy. Rewrites children first, then
 * applies rules at the parent. Iterates to a fixed point capped at {@link #MAX_ITERATIONS}.
 */
public final class OperatorRewriteEngine {

  static final int MAX_ITERATIONS = 8;
  private static final FluentLogger LOGGER = FluentLogger.forEnclosingClass();

  private final List<OperatorRewriteRule> rules;

  OperatorRewriteEngine(List<OperatorRewriteRule> rules) {
    this.rules = rules;
  }

  public Operator rewrite(Operator root, RewriteContext context) {
    @Var Operator current = root;
    @Var int iterations = 0;
    while (true) {
      iterations++;
      Operator next = rewriteOnce(current, context);
      if (next == current) {
        break;
      }
      current = next;
      if (iterations >= MAX_ITERATIONS) {
        LOGGER.atWarning().log(
            "QueryOptimizer OperatorRewriteEngine exceeded %d fixed-point iterations; "
                + "emitting current tree to avoid an infinite loop. fired=%s",
            MAX_ITERATIONS, context.audit().firedRules());
        break;
      }
    }
    context.audit().recordIterations(iterations);
    return current;
  }

  private Operator rewriteOnce(Operator operator, RewriteContext context) {
    Operator withRewrittenChildren = rewriteChildren(operator, context);
    return applyRules(withRewrittenChildren, context);
  }

  // Mirrors DrillSidewaysInfoBuilder.traverseOperator: explicit case for every permitted subtype
  // of Operator. Composite operators (CompoundOperator, EmbeddedDocumentOperator) recurse;
  // everything else is a leaf for the optimizer. Adding a new sealed permits entry forces a
  // compile error here, which is the correct failure mode.
  private Operator rewriteChildren(Operator operator, RewriteContext context) {
    return switch (operator) {
      case CompoundOperator compoundOperator -> rewriteCompoundChildren(compoundOperator, context);
      case EmbeddedDocumentOperator embeddedDocumentOperator ->
          rewriteEmbeddedDocumentChild(embeddedDocumentOperator, context);
      case AllDocumentsOperator ignored -> operator;
      case AutocompleteOperator ignored -> operator;
      case EqualsOperator ignored -> operator;
      case ExistsOperator ignored -> operator;
      case GeoShapeOperator ignored -> operator;
      case GeoWithinOperator ignored -> operator;
      case HasAncestorOperator ignored -> operator;
      case HasRootOperator ignored -> operator;
      case InOperator ignored -> operator;
      case KnnBetaOperator ignored -> operator;
      case MoreLikeThisOperator ignored -> operator;
      case NearOperator ignored -> operator;
      case PhraseOperator ignored -> operator;
      case QueryStringOperator ignored -> operator;
      case RangeOperator ignored -> operator;
      case SearchOperator ignored -> operator;
      case SpanOperator ignored -> operator;
      case TermLevelOperator ignored -> operator;
      case TermOperator ignored -> operator;
      case TextOperator ignored -> operator;
      case VectorSearchOperator ignored -> operator;
    };
  }

  private CompoundOperator rewriteCompoundChildren(
      CompoundOperator compoundOperator, RewriteContext context) {
    CompoundClause filter = rewriteCompoundClause(compoundOperator.filter(), context);
    CompoundClause must = rewriteCompoundClause(compoundOperator.must(), context);
    CompoundClause mustNot = rewriteCompoundClause(compoundOperator.mustNot(), context);
    CompoundClause should = rewriteCompoundClause(compoundOperator.should(), context);
    if (filter == compoundOperator.filter()
        && must == compoundOperator.must()
        && mustNot == compoundOperator.mustNot()
        && should == compoundOperator.should()) {
      return compoundOperator;
    }
    return new CompoundOperator(
        compoundOperator.score(),
        filter,
        must,
        mustNot,
        should,
        compoundOperator.minimumShouldMatch(),
        compoundOperator.doesNotAffect());
  }

  private CompoundClause rewriteCompoundClause(CompoundClause clause, RewriteContext context) {
    List<? extends Operator> originals = clause.operators();
    List<Operator> rewritten = new ArrayList<>(originals.size());
    @Var boolean anyChanged = false;
    for (Operator child : originals) {
      Operator next = rewriteOnce(child, context);
      if (next != child) {
        anyChanged = true;
      }
      rewritten.add(next);
    }
    return anyChanged ? new CompoundClause(rewritten) : clause;
  }

  private Operator rewriteEmbeddedDocumentChild(
      EmbeddedDocumentOperator embedded, RewriteContext context) {
    Operator inner = rewriteOnce(embedded.operator(), context);
    if (inner == embedded.operator()) {
      return embedded;
    }
    return new EmbeddedDocumentOperator(embedded.score(), embedded.path(), inner);
  }

  private Operator applyRules(Operator operator, RewriteContext context) {
    for (OperatorRewriteRule rule : this.rules) {
      Operator rewritten = rule.apply(operator, context);
      if (rewritten != operator) {
        context.audit().recordFiring(rule.name());
        var counter = context.metrics().getOptimizerRuleFiredCounter(rule.name());
        if (counter != null) {
          counter.increment();
        }
        return rewritten;
      }
    }
    return operator;
  }
}
