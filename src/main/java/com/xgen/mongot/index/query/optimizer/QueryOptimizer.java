package com.xgen.mongot.index.query.optimizer;

import com.google.common.flogger.FluentLogger;
import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlagRegistry;
import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlags;
import com.xgen.mongot.index.lucene.explain.explainers.OptimizerFeatureExplainer;
import com.xgen.mongot.index.lucene.explain.tracing.Explain;
import com.xgen.mongot.index.lucene.query.context.QueryFactoryContext;
import com.xgen.mongot.index.query.operators.Operator;
import com.xgen.mongot.index.query.operators.VectorSearchFilter;
import com.xgen.mongot.index.query.operators.mql.Clause;
import java.util.concurrent.TimeUnit;
import org.apache.lucene.index.IndexReader;

/**
 * Entry point for the rule-based query optimizer. One instance per {@link QueryFactoryContext};
 * checks the {@code QUERY_OPTIMIZER} kill switch once per optimize call.
 *
 * <p>Optimization is best-effort and must never fail a query: if any rule or engine throws, the
 * failure is counted and logged and the original tree is returned unchanged.
 *
 * <p>A call with no active rules for its tree type increments {@code optimizerInvocations} but
 * skips traversal and the iteration/latency samples, so entry points that can never rewrite (e.g.,
 * an Operator call while only Clause rules are registered) do not dilute those distributions.
 */
public final class QueryOptimizer {

  private static final FluentLogger LOGGER = FluentLogger.forEnclosingClass();

  private final RuleRegistry registry;
  private final QueryFactoryContext factoryContext;
  private final DynamicFeatureFlagRegistry dynamicFeatureFlagRegistry;

  public QueryOptimizer(
      RuleRegistry registry,
      QueryFactoryContext factoryContext,
      DynamicFeatureFlagRegistry dynamicFeatureFlagRegistry) {
    this.registry = registry;
    this.factoryContext = factoryContext;
    this.dynamicFeatureFlagRegistry = dynamicFeatureFlagRegistry;
  }

  /**
   * Dispatches to the engine matching the filter subtype. The kill switch is evaluated once, by the
   * delegated overload.
   */
  public Result<VectorSearchFilter> optimize(VectorSearchFilter filter, IndexReader indexReader) {
    return switch (filter) {
      case VectorSearchFilter.ClauseFilter clauseFilter -> {
        Result<Clause> result = optimize(clauseFilter.clause(), indexReader);
        yield new Result<>(
            result.tree() == clauseFilter.clause()
                ? clauseFilter
                : new VectorSearchFilter.ClauseFilter(result.tree()),
            result.audit());
      }
      case VectorSearchFilter.OperatorFilter operatorFilter -> {
        Result<Operator> result = optimize(operatorFilter.operator(), indexReader);
        yield new Result<>(
            result.tree() == operatorFilter.operator()
                ? operatorFilter
                : new VectorSearchFilter.OperatorFilter(result.tree()),
            result.audit());
      }
    };
  }

  /** Optimize a {@link Clause} tree directly. */
  public Result<Clause> optimize(Clause root, IndexReader indexReader) {
    if (!isEnabled()) {
      return Result.unchanged(root);
    }
    long startNanos = System.nanoTime();
    try {
      this.factoryContext.getMetrics().getOptimizerInvocations().increment();
      RuleRegistry.Filtered filtered = this.registry.forContext(this.dynamicFeatureFlagRegistry);
      if (filtered.clauseRules().isEmpty()) {
        return Result.unchanged(root);
      }
      RewriteContext context = newContext(indexReader);
      Clause rewritten = new ClauseRewriteEngine(filtered.clauseRules()).rewrite(root, context);
      return finish(root, rewritten, context.audit(), startNanos);
    } catch (RuntimeException e) {
      return failOpen(root, e);
    }
  }

  /** Optimize an {@link Operator} tree directly. */
  public Result<Operator> optimize(Operator root, IndexReader indexReader) {
    if (!isEnabled()) {
      return Result.unchanged(root);
    }
    long startNanos = System.nanoTime();
    try {
      this.factoryContext.getMetrics().getOptimizerInvocations().increment();
      RuleRegistry.Filtered filtered = this.registry.forContext(this.dynamicFeatureFlagRegistry);
      if (filtered.operatorRules().isEmpty()) {
        return Result.unchanged(root);
      }
      RewriteContext context = newContext(indexReader);
      Operator rewritten =
          new OperatorRewriteEngine(filtered.operatorRules()).rewrite(root, context);
      return finish(root, rewritten, context.audit(), startNanos);
    } catch (RuntimeException e) {
      return failOpen(root, e);
    }
  }

  /** Fail open: count and log the failure, then hand back the original tree. */
  private <T> Result<T> failOpen(T root, RuntimeException e) {
    LOGGER.atWarning().atMostEvery(1, TimeUnit.MINUTES).withCause(e).log(
        "QueryOptimizer failed; returning the query tree unoptimized");
    try {
      this.factoryContext.getMetrics().getOptimizerFailures().increment();
    } catch (RuntimeException metricsFailure) {
      // Counting the failure must not itself fail the query.
    }
    return Result.unchanged(root);
  }

  private RewriteContext newContext(IndexReader indexReader) {
    return new RewriteContext(
        indexReader,
        this.factoryContext,
        this.dynamicFeatureFlagRegistry,
        this.factoryContext.getMetrics(),
        new RewriteAudit());
  }

  private <T> Result<T> finish(T before, T after, RewriteAudit audit, long startNanos) {
    var metrics = this.factoryContext.getMetrics();
    metrics.getOptimizerIterations().record(audit.iterations());
    metrics
        .getOptimizerLatencySeconds()
        .record((System.nanoTime() - startNanos) / (double) TimeUnit.SECONDS.toNanos(1));
    appendToExplainIfEnabled(audit);
    logExit(before, after, audit);
    return new Result<>(after, audit);
  }

  /** Debug-level exit log: iterations, fired-rule names, value-based before/after hashes. */
  private static <T> void logExit(T before, T after, RewriteAudit audit) {
    if (!LOGGER.atFine().isEnabled()) {
      return;
    }
    LOGGER.atFine().log(
        "QueryOptimizer exit: iterations=%d firedRules=%s beforeHash=%x afterHash=%x",
        audit.iterations(), audit.firedRules(), before.hashCode(), after.hashCode());
  }

  /** Surface firings as {@code optimizerRulesFired} on explain; no-op if not explaining. */
  private static void appendToExplainIfEnabled(RewriteAudit audit) {
    if (audit.isEmpty() || !Explain.isEnabled()) {
      return;
    }
    Explain.getQueryInfo()
        .map(
            queryInfo ->
                queryInfo.getFeatureExplainer(
                    OptimizerFeatureExplainer.class, OptimizerFeatureExplainer::new))
        .ifPresent(explainer -> audit.firedRules().forEach(explainer::addRuleFiring));
  }

  private boolean isEnabled() {
    return this.dynamicFeatureFlagRegistry.evaluateClusterInvariant(
        DynamicFeatureFlags.QUERY_OPTIMIZER);
  }

  /** Outcome of an optimize call: rewritten tree and audit of firings. */
  public record Result<T>(T tree, RewriteAudit audit) {

    static <T> Result<T> unchanged(T tree) {
      RewriteAudit emptyAudit = new RewriteAudit();
      emptyAudit.recordIterations(0);
      return new Result<>(tree, emptyAudit);
    }
  }
}
