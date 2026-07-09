package com.xgen.mongot.index.query.optimizer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlagRegistry;
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
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Engine traversal tests with an empty rule list. Verifies that every {@link Clause} subtype passes
 * through unchanged, the engine terminates in a single iteration, and the audit records no rule
 * firings.
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
        null, factoryContext, DynamicFeatureFlagRegistry.empty(), metrics, new RewriteAudit());
  }
}
