package com.xgen.mongot.index.query.optimizer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlagConfig;
import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlagConfig.Phase;
import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlagConfig.Scope;
import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlagRegistry;
import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlags;
import com.xgen.mongot.index.IndexMetricsUpdater;
import com.xgen.mongot.index.lucene.explain.tracing.Explain;
import com.xgen.mongot.index.lucene.query.context.QueryFactoryContext;
import com.xgen.mongot.index.query.operators.EqualsOperator;
import com.xgen.mongot.index.query.operators.Operator;
import com.xgen.mongot.index.query.operators.VectorSearchFilter;
import com.xgen.mongot.index.query.operators.mql.Clause;
import com.xgen.mongot.index.query.operators.mql.SimpleClause;
import com.xgen.testing.mongot.index.query.operators.EqualsOperatorBuilder;
import com.xgen.testing.mongot.index.query.operators.mql.ClauseBuilder;
import com.xgen.testing.mongot.index.query.operators.mql.MqlFilterOperatorBuilder;
import com.xgen.testing.mongot.index.query.operators.mql.ValueBuilder;
import com.xgen.testing.mongot.mock.index.SearchIndex;
import java.util.List;
import java.util.Optional;
import org.bson.types.ObjectId;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.LoggerFactory;

/** Entry-point tests for {@link QueryOptimizer}. */
public class QueryOptimizerTest {

  @Test
  public void flagOff_returnsSameReference_andDoesNotIncrementInvocations() throws Exception {
    SimpleClause clause = simpleEq("title", "fiction");
    Fixture fixture = Fixture.create(/* flagOn= */ false, RuleRegistry.empty());

    double before = fixture.metrics.getOptimizerInvocations().count();
    QueryOptimizer.Result<Clause> result = fixture.optimizer.optimize(clause);

    assertSame(clause, result.tree());
    assertTrue(result.audit().firedRules().isEmpty());
    assertEquals(before, fixture.metrics.getOptimizerInvocations().count(), 0.0);
  }

  @Test
  public void flagOff_vectorSearchFilter_returnsSameReference() throws Exception {
    var filter = new VectorSearchFilter.ClauseFilter(simpleEq("title", "fiction"));
    Fixture fixture = Fixture.create(/* flagOn= */ false, RuleRegistry.empty());

    QueryOptimizer.Result<VectorSearchFilter> result = fixture.optimizer.optimize(filter);

    assertSame(filter, result.tree());
  }

  /**
   * With no active rules for the tree type the optimizer short-circuits: it counts the invocation
   * but skips traversal, so the iteration/latency histograms record no sample and the tree comes
   * back by reference.
   */
  @Test
  public void flagOn_emptyRegistry_shortCircuitsAndCountsOnlyInvocations() throws Exception {
    SimpleClause clause = simpleEq("title", "fiction");
    Fixture fixture = Fixture.create(/* flagOn= */ true, RuleRegistry.empty());

    double invocationsBefore = fixture.metrics.getOptimizerInvocations().count();
    long iterationSamplesBefore = fixture.metrics.getOptimizerIterations().count();
    long latencySamplesBefore = fixture.metrics.getOptimizerLatencySeconds().count();
    QueryOptimizer.Result<Clause> result = fixture.optimizer.optimize(clause);

    assertSame("empty rule list must not rebuild the tree", clause, result.tree());
    assertTrue(result.audit().firedRules().isEmpty());
    assertEquals(0, result.audit().iterations());
    assertEquals(invocationsBefore + 1.0, fixture.metrics.getOptimizerInvocations().count(), 0.0);
    assertEquals(
        "short-circuited calls must not record iteration samples",
        iterationSamplesBefore,
        fixture.metrics.getOptimizerIterations().count());
    assertEquals(
        "short-circuited calls must not record latency samples",
        latencySamplesBefore,
        fixture.metrics.getOptimizerLatencySeconds().count());
  }

  @Test
  public void flagOn_operatorEntryPoint_alsoIncrementsInvocations() {
    EqualsOperator op = new EqualsOperatorBuilder().path("a").value("1").build();
    Fixture fixture = Fixture.create(/* flagOn= */ true, RuleRegistry.empty());

    double before = fixture.metrics.getOptimizerInvocations().count();
    QueryOptimizer.Result<Operator> result = fixture.optimizer.optimize(op);

    assertSame(op, result.tree());
    assertEquals(before + 1.0, fixture.metrics.getOptimizerInvocations().count(), 0.0);
  }

  /**
   * The short-circuit is per tree type, not per registry: with only clause rules registered, an
   * Operator optimize call must skip traversal even though the registry is non-empty. This is the
   * production state while every registered rule is clause-side — the Operator entry point serves
   * all search-operator queries as guaranteed no-ops.
   */
  @Test
  public void operatorEntryPoint_onlyClauseRulesRegistered_shortCircuits() {
    var registry =
        RuleRegistry.builder().addClauseRule(new OneShotCloneRule("ClauseOnlyRule")).build();
    Fixture fixture = Fixture.create(/* flagOn= */ true, registry);
    EqualsOperator op = new EqualsOperatorBuilder().path("a").value("1").build();

    double invocationsBefore = fixture.metrics.getOptimizerInvocations().count();
    long iterationSamplesBefore = fixture.metrics.getOptimizerIterations().count();
    QueryOptimizer.Result<Operator> result = fixture.optimizer.optimize(op);

    assertSame(op, result.tree());
    assertTrue(result.audit().firedRules().isEmpty());
    assertEquals(invocationsBefore + 1.0, fixture.metrics.getOptimizerInvocations().count(), 0.0);
    assertEquals(
        "operator calls with no operator rules must not record iteration samples",
        iterationSamplesBefore,
        fixture.metrics.getOptimizerIterations().count());
  }

  @Test
  public void maxIterationCap_emitsCurrentTreeWithoutThrowing() throws Exception {
    SimpleClause original = simpleEq("title", "fiction");

    // Test-only rule: every visit returns a fresh equivalent SimpleClause, forcing the engine
    // to keep iterating until it hits the cap.
    var registry =
        RuleRegistry.builder()
            .addClauseRule(
                new ClauseRewriteRule() {
                  @Override
                  public String name() {
                    return "TestThrashRule";
                  }

                  @Override
                  public DynamicFeatureFlags flag() {
                    return DynamicFeatureFlags.QUERY_OPTIMIZER;
                  }

                  @Override
                  public Clause apply(Clause clause, RewriteContext context) {
                    if (clause instanceof SimpleClause sc) {
                      return new SimpleClause(sc.path(), sc.mqlFilterOperators());
                    }
                    return clause;
                  }
                })
            .build();

    Fixture fixture = Fixture.create(/* flagOn= */ true, registry);

    var engineLogger =
        (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(ClauseRewriteEngine.class);
    Level previousLevel = engineLogger.getLevel();
    ListAppender<ILoggingEvent> appender = new ListAppender<>();
    appender.start();
    engineLogger.addAppender(appender);
    engineLogger.setLevel(Level.WARN);
    try {
      QueryOptimizer.Result<Clause> result = fixture.optimizer.optimize(original);

      // Engine emits whatever it has after MAX_ITERATIONS; we don't make any guarantee about
      // identity here (the rule produced a different reference) but we DO require:
      //   - no exception
      //   - iteration count exactly the cap
      //   - the audit shows the rule fired one or more times
      //   - a WARN-level log was emitted naming the cap, so operators can detect thrashing rules
      assertEquals(ClauseRewriteEngine.MAX_ITERATIONS, result.audit().iterations());
      assertEquals(ClauseRewriteEngine.MAX_ITERATIONS, result.audit().firedRules().size());
      assertNotSame(original, result.tree());

      assertTrue(
          "expected a WARN log mentioning the iteration cap; got: " + appender.list,
          appender.list.stream()
              .anyMatch(
                  event ->
                      event.getLevel() == Level.WARN
                          && event.getFormattedMessage().contains("exceeded")
                          && event
                              .getFormattedMessage()
                              .contains(String.valueOf(ClauseRewriteEngine.MAX_ITERATIONS))));
    } finally {
      engineLogger.detachAppender(appender);
      engineLogger.setLevel(previousLevel);
    }
  }

  /**
   * End-to-end check that rule firings recorded by the engine surface as {@code
   * optimizerRulesFired} on {@code SearchExplainInformation}, via {@link
   * com.xgen.mongot.index.lucene.explain.explainers.OptimizerFeatureExplainer}.
   */
  @Test
  public void firedRules_surfaceInExplainInformation() throws Exception {
    var registry =
        RuleRegistry.builder()
            .addClauseRule(new OneShotCloneRule("FakeRuleForExplainTest"))
            .build();
    Fixture fixture = Fixture.create(/* flagOn= */ true, registry);
    SimpleClause clause = simpleEq("title", "fiction");

    try (var unused =
        Explain.setup(Optional.of(Explain.Verbosity.QUERY_PLANNER), Optional.empty())) {
      QueryOptimizer.Result<Clause> result = fixture.optimizer.optimize(clause);
      assertEquals(List.of("FakeRuleForExplainTest"), result.audit().firedRules());

      var explain = Explain.collect().orElseThrow();
      assertEquals(Optional.of(List.of("FakeRuleForExplainTest")), explain.optimizerRulesFired());
    }
  }

  /**
   * Sibling check: when no rules fire (empty registry / no-op pass) the explain field stays absent
   * rather than appearing as an empty list. Mirrors how {@code dynamicFeatureFlags} behaves when
   * nothing was evaluated.
   */
  @Test
  public void noFiredRules_doesNotPopulateExplainField() throws Exception {
    Fixture fixture = Fixture.create(/* flagOn= */ true, RuleRegistry.empty());
    SimpleClause clause = simpleEq("title", "fiction");

    try (var unused =
        Explain.setup(Optional.of(Explain.Verbosity.QUERY_PLANNER), Optional.empty())) {
      fixture.optimizer.optimize(clause);
      var explain = Explain.collect().orElseThrow();
      assertTrue(explain.optimizerRulesFired().isEmpty());
    }
  }

  /**
   * The optimizer is best-effort: a rule that throws must not fail the query. The original tree
   * comes back unchanged, the failure is counted in {@code optimizerFailures}, and the invocation
   * still counts toward {@code optimizerInvocations}.
   */
  @Test
  public void clauseRuleThrows_failsOpen_returnsOriginalTreeAndCountsFailure() throws Exception {
    var registry =
        RuleRegistry.builder()
            .addClauseRule(
                new ClauseRewriteRule() {
                  @Override
                  public String name() {
                    return "TestThrowingClauseRule";
                  }

                  @Override
                  public DynamicFeatureFlags flag() {
                    return DynamicFeatureFlags.QUERY_OPTIMIZER;
                  }

                  @Override
                  public Clause apply(Clause clause, RewriteContext context) {
                    throw new IllegalStateException("rule failure for test");
                  }
                })
            .build();
    Fixture fixture = Fixture.create(/* flagOn= */ true, registry);
    SimpleClause clause = simpleEq("title", "fiction");

    double invocationsBefore = fixture.metrics.getOptimizerInvocations().count();
    double failuresBefore = fixture.metrics.getOptimizerFailures().count();
    QueryOptimizer.Result<Clause> result = fixture.optimizer.optimize(clause);

    assertSame("a throwing rule must not alter the query tree", clause, result.tree());
    assertTrue(result.audit().firedRules().isEmpty());
    assertEquals(invocationsBefore + 1.0, fixture.metrics.getOptimizerInvocations().count(), 0.0);
    assertEquals(failuresBefore + 1.0, fixture.metrics.getOptimizerFailures().count(), 0.0);
  }

  /** Operator-entry sibling of the fail-open test above. */
  @Test
  public void operatorRuleThrows_failsOpen_returnsOriginalTreeAndCountsFailure() {
    var registry =
        RuleRegistry.builder()
            .addOperatorRule(
                new OperatorRewriteRule() {
                  @Override
                  public String name() {
                    return "TestThrowingOperatorRule";
                  }

                  @Override
                  public DynamicFeatureFlags flag() {
                    return DynamicFeatureFlags.QUERY_OPTIMIZER;
                  }

                  @Override
                  public Operator apply(Operator operator, RewriteContext context) {
                    throw new IllegalStateException("rule failure for test");
                  }
                })
            .build();
    Fixture fixture = Fixture.create(/* flagOn= */ true, registry);
    EqualsOperator op = new EqualsOperatorBuilder().path("a").value("1").build();

    double failuresBefore = fixture.metrics.getOptimizerFailures().count();
    QueryOptimizer.Result<Operator> result = fixture.optimizer.optimize(op);

    assertSame("a throwing rule must not alter the query tree", op, result.tree());
    assertEquals(failuresBefore + 1.0, fixture.metrics.getOptimizerFailures().count(), 0.0);
  }

  /** Test-only rule that fires once, returning a structurally-identical fresh SimpleClause. */
  private static final class OneShotCloneRule implements ClauseRewriteRule {
    private final String name;

    private boolean fired = false;

    OneShotCloneRule(String name) {
      this.name = name;
    }

    @Override
    public String name() {
      return this.name;
    }

    @Override
    public DynamicFeatureFlags flag() {
      return DynamicFeatureFlags.QUERY_OPTIMIZER;
    }

    @Override
    public Clause apply(Clause clause, RewriteContext context) {
      if (this.fired || !(clause instanceof SimpleClause sc)) {
        return clause;
      }
      this.fired = true;
      return new SimpleClause(sc.path(), sc.mqlFilterOperators());
    }
  }

  private static SimpleClause simpleEq(String path, String value) throws Exception {
    return ClauseBuilder.simpleClause()
        .path(path)
        .addOperator(MqlFilterOperatorBuilder.eq().value(ValueBuilder.string(value)))
        .build();
  }

  /** Bundles a configured QueryOptimizer with the metrics it writes to, for assertions. */
  private static final class Fixture {
    final QueryOptimizer optimizer;
    final IndexMetricsUpdater.QueryingMetricsUpdater metrics;

    Fixture(QueryOptimizer optimizer, IndexMetricsUpdater.QueryingMetricsUpdater metrics) {
      this.optimizer = optimizer;
      this.metrics = metrics;
    }

    static Fixture create(boolean flagOn, RuleRegistry registry) {
      var metrics =
          new IndexMetricsUpdater.QueryingMetricsUpdater(SearchIndex.mockMetricsFactory());
      QueryFactoryContext factoryContext = Mockito.mock(QueryFactoryContext.class);
      Mockito.when(factoryContext.getMetrics()).thenReturn(metrics);
      DynamicFeatureFlagRegistry dffRegistry = newRegistry(flagOn);
      return new Fixture(new QueryOptimizer(registry, factoryContext, dffRegistry), metrics);
    }
  }

  /**
   * Build a {@link DynamicFeatureFlagRegistry} where {@code QUERY_OPTIMIZER} resolves to the
   * requested value via the cached evaluation path (Phase.ENABLED/DISABLED on a GROUP-scoped flag
   * with a configured groupId).
   *
   * <p>{@code QUERY_OPTIMIZER}'s flag fallback is {@code false}, so {@code flagOn=false} with the
   * empty registry would also work; we go through the cached path here so test coverage exercises
   * the same evaluation logic that production hits.
   */
  private static DynamicFeatureFlagRegistry newRegistry(boolean enabled) {
    DynamicFeatureFlagConfig config =
        new DynamicFeatureFlagConfig(
            DynamicFeatureFlags.QUERY_OPTIMIZER.getName(),
            enabled ? Phase.ENABLED : Phase.DISABLED,
            List.of(),
            List.of(),
            0,
            Scope.GROUP);
    return new DynamicFeatureFlagRegistry(
        Optional.of(List.of(config)),
        Optional.empty(),
        Optional.of(new ObjectId()),
        Optional.empty());
  }
}
