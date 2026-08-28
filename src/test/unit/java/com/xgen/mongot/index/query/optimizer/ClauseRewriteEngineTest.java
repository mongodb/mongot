package com.xgen.mongot.index.query.optimizer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlagRegistry;
import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlags;
import com.xgen.mongot.index.IndexMetricsUpdater;
import com.xgen.mongot.index.lucene.query.context.QueryFactoryContext;
import com.xgen.mongot.index.query.operators.mql.AndClause;
import com.xgen.mongot.index.query.operators.mql.Clause;
import com.xgen.mongot.index.query.operators.mql.NorClause;
import com.xgen.mongot.index.query.operators.mql.OrClause;
import com.xgen.mongot.index.query.operators.mql.SimpleClause;
import com.xgen.testing.mongot.index.query.operators.mql.ClauseBuilder;
import com.xgen.testing.mongot.index.query.operators.mql.MqlFilterOperatorBuilder;
import com.xgen.testing.mongot.index.query.operators.mql.ValueBuilder;
import com.xgen.testing.mongot.mock.index.SearchIndex;
import java.util.List;
import java.util.function.UnaryOperator;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Engine tests. Empty-rule-list tests verify every {@link Clause} subtype passes through unchanged
 * by reference, in a single iteration, with an empty audit. Stub-rule tests verify rule firing and
 * audit recording, parent rebuild on child change, fixed-point chaining, first-matching-rule-wins
 * ordering, and the {@link ClauseRewriteEngine#MAX_ITERATIONS} cap.
 */
public class ClauseRewriteEngineTest {

  private final ClauseRewriteEngine engine = new ClauseRewriteEngine(List.of());

  @Test
  public void simpleClause_passesThroughUnchanged() throws Exception {
    SimpleClause clause = simpleEq("title", "fiction");

    RewriteContext context = newContext();
    Clause result = this.engine.rewrite(clause, context);

    assertSame(clause, result);
    assertTrue(context.audit().firedRules().isEmpty());
    assertEquals(1, context.audit().iterations());
  }

  @Test
  public void andClause_passesThroughUnchanged() throws Exception {
    AndClause clause =
        ClauseBuilder.andClause()
            .addClause(simpleEq("title", "fiction"))
            .addClause(simpleEq("author", "tolkien"))
            .explicitAnd(true)
            .build();

    RewriteContext context = newContext();
    Clause result = this.engine.rewrite(clause, context);

    assertSame(clause, result);
    assertTrue(context.audit().firedRules().isEmpty());
    assertEquals(1, context.audit().iterations());
  }

  @Test
  public void orClause_passesThroughUnchanged() throws Exception {
    OrClause clause =
        new OrClause(List.of(simpleEq("title", "fiction"), simpleEq("title", "history")));

    RewriteContext context = newContext();
    Clause result = this.engine.rewrite(clause, context);

    assertSame(clause, result);
    assertTrue(context.audit().firedRules().isEmpty());
    assertEquals(1, context.audit().iterations());
  }

  @Test
  public void norClause_passesThroughUnchanged() throws Exception {
    NorClause clause = new NorClause(List.of(simpleEq("title", "fiction")));

    RewriteContext context = newContext();
    Clause result = this.engine.rewrite(clause, context);

    assertSame(clause, result);
    assertTrue(context.audit().firedRules().isEmpty());
    assertEquals(1, context.audit().iterations());
  }

  @Test
  public void nestedCompound_passesThroughUnchangedAtEveryLevel() throws Exception {
    Clause inner =
        ClauseBuilder.andClause()
            .addClause(simpleEq("a", "1"))
            .addClause(simpleEq("b", "2"))
            .explicitAnd(true)
            .build();
    Clause outer = new OrClause(List.of(inner, simpleEq("c", "3")));

    RewriteContext context = newContext();
    Clause result = this.engine.rewrite(outer, context);

    assertSame(outer, result);
    assertTrue(context.audit().firedRules().isEmpty());
    assertEquals(1, context.audit().iterations());
  }

  @Test
  public void rewritingRule_replacesLeafAndRecordsFiring() throws Exception {
    SimpleClause original = simpleEq("field", "a");
    SimpleClause replacement = simpleEq("field", "b");
    ClauseRewriteEngine engine =
        new ClauseRewriteEngine(
            List.of(stubRule("swap", clause -> clause == original ? replacement : clause)));

    RewriteContext context = newContext();
    Clause result = engine.rewrite(original, context);

    assertSame(replacement, result);
    assertEquals(List.of("swap"), context.audit().firedRules());
    assertEquals(2, context.audit().iterations());
  }

  @Test
  public void rewritingRule_rebuildsParentAndPreservesSiblings() throws Exception {
    SimpleClause original = simpleEq("field", "a");
    SimpleClause replacement = simpleEq("field", "b");
    SimpleClause sibling = simpleEq("other", "keep");
    AndClause parent =
        ClauseBuilder.andClause().addClause(original).addClause(sibling).explicitAnd(true).build();
    ClauseRewriteEngine engine =
        new ClauseRewriteEngine(
            List.of(stubRule("swap", clause -> clause == original ? replacement : clause)));

    RewriteContext context = newContext();
    Clause result = engine.rewrite(parent, context);

    assertNotSame(parent, result);
    AndClause rebuilt = (AndClause) result;
    assertSame(replacement, rebuilt.getClauses().get(0));
    assertSame(sibling, rebuilt.getClauses().get(1));
    assertTrue(rebuilt.isExplicitAnd());
    assertEquals(List.of("swap"), context.audit().firedRules());
    assertEquals(2, context.audit().iterations());
  }

  @Test
  public void chainedRules_convergeAcrossIterations() throws Exception {
    SimpleClause first = simpleEq("field", "a");
    SimpleClause second = simpleEq("field", "b");
    SimpleClause third = simpleEq("field", "c");
    ClauseRewriteEngine engine =
        new ClauseRewriteEngine(
            List.of(
                stubRule("a-to-b", clause -> clause == first ? second : clause),
                stubRule("b-to-c", clause -> clause == second ? third : clause)));

    RewriteContext context = newContext();
    Clause result = engine.rewrite(first, context);

    assertSame(third, result);
    assertEquals(List.of("a-to-b", "b-to-c"), context.audit().firedRules());
    assertEquals(3, context.audit().iterations());
  }

  @Test
  public void multipleMatchingRules_firstRegisteredWins() throws Exception {
    SimpleClause original = simpleEq("field", "a");
    SimpleClause firstOutput = simpleEq("field", "b");
    SimpleClause secondOutput = simpleEq("field", "c");
    ClauseRewriteEngine engine =
        new ClauseRewriteEngine(
            List.of(
                stubRule("first", clause -> clause == original ? firstOutput : clause),
                stubRule("second", clause -> clause == original ? secondOutput : clause)));

    RewriteContext context = newContext();
    Clause result = engine.rewrite(original, context);

    assertSame(firstOutput, result);
    assertEquals(List.of("first"), context.audit().firedRules());
    assertEquals(2, context.audit().iterations());
  }

  @Test
  public void nonConvergingRule_stopsAtMaxIterations() throws Exception {
    SimpleClause ping = simpleEq("field", "ping");
    SimpleClause pong = simpleEq("field", "pong");
    ClauseRewriteEngine engine =
        new ClauseRewriteEngine(List.of(stubRule("churn", clause -> clause == ping ? pong : ping)));

    RewriteContext context = newContext();
    engine.rewrite(ping, context);

    assertEquals(ClauseRewriteEngine.MAX_ITERATIONS, context.audit().iterations());
    assertEquals(ClauseRewriteEngine.MAX_ITERATIONS, context.audit().firedRules().size());
  }

  @Test
  public void identityReturningRule_treatedAsNoOp() throws Exception {
    SimpleClause clause = simpleEq("field", "a");
    ClauseRewriteEngine engine = new ClauseRewriteEngine(List.of(stubRule("noop", c -> c)));

    RewriteContext context = newContext();
    Clause result = engine.rewrite(clause, context);

    assertSame(clause, result);
    assertTrue(context.audit().firedRules().isEmpty());
    assertEquals(1, context.audit().iterations());
  }

  private static SimpleClause simpleEq(String path, String value) throws Exception {
    return ClauseBuilder.simpleClause()
        .path(path)
        .addOperator(MqlFilterOperatorBuilder.eq().value(ValueBuilder.string(value)))
        .build();
  }

  private static RewriteContext newContext() {
    QueryFactoryContext factoryContext = Mockito.mock(QueryFactoryContext.class);
    IndexMetricsUpdater.QueryingMetricsUpdater metrics =
        new IndexMetricsUpdater.QueryingMetricsUpdater(SearchIndex.mockMetricsFactory());
    return new RewriteContext(
        factoryContext, DynamicFeatureFlagRegistry.empty(), metrics, new RewriteAudit());
  }

  /** Rule stub driven by the given function; the flag is arbitrary — the engine never reads it. */
  private static ClauseRewriteRule stubRule(String name, UnaryOperator<Clause> rewrite) {
    return new ClauseRewriteRule() {
      @Override
      public Clause apply(Clause clause, RewriteContext context) {
        return rewrite.apply(clause);
      }

      @Override
      public String name() {
        return name;
      }

      @Override
      public DynamicFeatureFlags flag() {
        return DynamicFeatureFlags.QUERY_OPTIMIZER;
      }
    };
  }
}
