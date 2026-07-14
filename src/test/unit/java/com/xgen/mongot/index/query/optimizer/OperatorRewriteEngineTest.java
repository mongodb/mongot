package com.xgen.mongot.index.query.optimizer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import com.mongodb.client.model.geojson.Position;
import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlagRegistry;
import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlags;
import com.xgen.mongot.index.IndexMetricsUpdater;
import com.xgen.mongot.index.lucene.query.context.QueryFactoryContext;
import com.xgen.mongot.index.query.operators.AllDocumentsOperator;
import com.xgen.mongot.index.query.operators.CompoundOperator;
import com.xgen.mongot.index.query.operators.EmbeddedDocumentOperator;
import com.xgen.mongot.index.query.operators.EqualsOperator;
import com.xgen.mongot.index.query.operators.ExactVectorSearchCriteria;
import com.xgen.mongot.index.query.operators.ExistsOperator;
import com.xgen.mongot.index.query.operators.GeoShapeOperator;
import com.xgen.mongot.index.query.operators.GeoWithinOperator;
import com.xgen.mongot.index.query.operators.InOperator;
import com.xgen.mongot.index.query.operators.KnnBetaOperator;
import com.xgen.mongot.index.query.operators.MoreLikeThisOperator;
import com.xgen.mongot.index.query.operators.NearOperator;
import com.xgen.mongot.index.query.operators.Operator;
import com.xgen.mongot.index.query.operators.QueryStringOperator;
import com.xgen.mongot.index.query.operators.RangeOperator;
import com.xgen.mongot.index.query.operators.SearchDisjunctionOperator;
import com.xgen.mongot.index.query.operators.SpanTermOperator;
import com.xgen.mongot.index.query.operators.TermBaseOperator;
import com.xgen.mongot.index.query.operators.VectorSearchOperator;
import com.xgen.mongot.index.query.operators.WildcardOperator;
import com.xgen.mongot.index.query.operators.bound.NumericRangeBound;
import com.xgen.mongot.index.query.operators.value.StringValue;
import com.xgen.mongot.index.query.points.LongPoint;
import com.xgen.mongot.index.query.scores.Score;
import com.xgen.mongot.index.query.shapes.Circle;
import com.xgen.mongot.index.query.shapes.GeometryShape;
import com.xgen.mongot.util.FieldPath;
import com.xgen.testing.mongot.index.path.string.StringPathBuilder;
import com.xgen.testing.mongot.index.path.string.UnresolvedStringPathBuilder;
import com.xgen.testing.mongot.index.query.operators.AutocompleteOperatorBuilder;
import com.xgen.testing.mongot.index.query.operators.CompoundOperatorBuilder;
import com.xgen.testing.mongot.index.query.operators.EqualsOperatorBuilder;
import com.xgen.testing.mongot.index.query.operators.HasAncestorOperatorBuilder;
import com.xgen.testing.mongot.index.query.operators.HasRootOperatorBuilder;
import com.xgen.testing.mongot.index.query.operators.PhraseOperatorBuilder;
import com.xgen.testing.mongot.index.query.operators.TextOperatorBuilder;
import com.xgen.testing.mongot.mock.index.SearchIndex;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import org.bson.BsonDocument;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Engine tests. Empty-rule-list tests verify leaf and composite {@link Operator} subtypes pass
 * through unchanged by reference, in a single iteration, with an empty audit. Stub-rule tests
 * verify rule firing and audit recording, parent rebuild on child change, fixed-point chaining,
 * first-matching-rule-wins ordering, and the {@link OperatorRewriteEngine#MAX_ITERATIONS} cap.
 */
public class OperatorRewriteEngineTest {

  private final OperatorRewriteEngine engine = new OperatorRewriteEngine(List.of());

  @Test
  public void leafOperator_passesThroughUnchanged() {
    Operator op = equalsOp("title", "fiction");

    RewriteContext context = newContext();
    Operator result = this.engine.rewrite(op, context);

    assertSame(op, result);
    assertTrue(context.audit().firedRules().isEmpty());
    assertEquals(1, context.audit().iterations());
  }

  @Test
  public void compoundOperator_passesThroughUnchangedAtEveryClause() {
    CompoundOperator op =
        new CompoundOperatorBuilder()
            .filter(equalsOp("title", "fiction"))
            .must(equalsOp("author", "tolkien"))
            .mustNot(equalsOp("draft", "true"))
            .should(equalsOp("genre", "fantasy"))
            .build();

    RewriteContext context = newContext();
    Operator result = this.engine.rewrite(op, context);

    assertSame(op, result);
    assertTrue(context.audit().firedRules().isEmpty());
    assertEquals(1, context.audit().iterations());
  }

  @Test
  public void embeddedDocumentOperator_passesThroughUnchanged() {
    Operator inner = equalsOp("nested.title", "fiction");
    EmbeddedDocumentOperator op =
        new EmbeddedDocumentOperator(Score.defaultScore(), FieldPath.parse("nested"), inner);

    RewriteContext context = newContext();
    Operator result = this.engine.rewrite(op, context);

    assertSame(op, result);
    assertTrue(context.audit().firedRules().isEmpty());
    assertEquals(1, context.audit().iterations());
  }

  @Test
  public void nestedCompound_passesThroughUnchangedAtEveryLevel() {
    CompoundOperator inner =
        new CompoundOperatorBuilder().must(equalsOp("a", "1")).must(equalsOp("b", "2")).build();
    CompoundOperator outer =
        new CompoundOperatorBuilder().filter(inner).should(equalsOp("c", "3")).build();

    RewriteContext context = newContext();
    Operator result = this.engine.rewrite(outer, context);

    assertSame(outer, result);
    assertTrue(context.audit().firedRules().isEmpty());
    assertEquals(1, context.audit().iterations());
  }

  /**
   * Traversal coverage for every direct permitted subtype of {@link Operator} not exercised by the
   * dedicated tests above. Each instance is fed to the engine with an empty rule list and must
   * round-trip by reference. Sealed-permits sub-interfaces (Search/Span/Term/TermLevel) are
   * represented by one concrete leaf each — the engine dispatches on the parent type, so any one
   * concrete subtype exercises the same branch.
   *
   * <p>The bottom of the test cross-checks {@code Operator.class.getPermittedSubclasses()} against
   * the fixture set so adding a new {@code Operator} subtype without a fixture fails loudly.
   */
  @Test
  public void everyDirectPermitsSubtype_passesThroughUnchanged() throws Exception {
    Map<Class<?>, Operator> fixtures = directPermitsFixtures();

    for (Map.Entry<Class<?>, Operator> entry : fixtures.entrySet()) {
      Operator op = entry.getValue();
      RewriteContext context = newContext();
      Operator result = this.engine.rewrite(op, context);

      String name = entry.getKey().getSimpleName();
      assertSame("pass-through failed for " + name, op, result);
      assertTrue("audit should be empty for " + name, context.audit().firedRules().isEmpty());
      assertEquals("iterations should be 1 for " + name, 1, context.audit().iterations());
    }

    Set<Class<?>> covered = fixtures.keySet();
    Set<Class<?>> permitted =
        java.util.Arrays.stream(Operator.class.getPermittedSubclasses())
            .collect(Collectors.toSet());
    Set<Class<?>> missing = new java.util.LinkedHashSet<>(permitted);
    missing.removeAll(covered);
    assertTrue(
        "every direct permits subtype of Operator must have a fixture; missing: " + missing,
        missing.isEmpty());
  }

  @Test
  public void rewritingRule_replacesLeafAndRecordsFiring() {
    EqualsOperator original = equalsOp("field", "a");
    EqualsOperator replacement = equalsOp("field", "b");
    OperatorRewriteEngine engine =
        new OperatorRewriteEngine(
            List.of(stubRule("swap", op -> op == original ? replacement : op)));

    RewriteContext context = newContext();
    Operator result = engine.rewrite(original, context);

    assertSame(replacement, result);
    assertEquals(List.of("swap"), context.audit().firedRules());
    assertEquals(2, context.audit().iterations());
  }

  @Test
  public void rewritingRule_rebuildsParentAndPreservesSiblings() {
    EqualsOperator original = equalsOp("field", "a");
    EqualsOperator replacement = equalsOp("field", "b");
    EqualsOperator sibling = equalsOp("other", "keep");
    CompoundOperator parent =
        new CompoundOperatorBuilder()
            .must(original)
            .must(sibling)
            .filter(equalsOp("f", "1"))
            .build();
    OperatorRewriteEngine engine =
        new OperatorRewriteEngine(
            List.of(stubRule("swap", op -> op == original ? replacement : op)));

    RewriteContext context = newContext();
    Operator result = engine.rewrite(parent, context);

    assertNotSame(parent, result);
    CompoundOperator rebuilt = (CompoundOperator) result;
    assertSame(replacement, rebuilt.must().operators().get(0));
    assertSame(sibling, rebuilt.must().operators().get(1));
    assertSame(parent.filter(), rebuilt.filter());
    assertEquals(List.of("swap"), context.audit().firedRules());
    assertEquals(2, context.audit().iterations());
  }

  @Test
  public void chainedRules_convergeAcrossIterations() {
    EqualsOperator first = equalsOp("field", "a");
    EqualsOperator second = equalsOp("field", "b");
    EqualsOperator third = equalsOp("field", "c");
    OperatorRewriteEngine engine =
        new OperatorRewriteEngine(
            List.of(
                stubRule("a-to-b", op -> op == first ? second : op),
                stubRule("b-to-c", op -> op == second ? third : op)));

    RewriteContext context = newContext();
    Operator result = engine.rewrite(first, context);

    assertSame(third, result);
    assertEquals(List.of("a-to-b", "b-to-c"), context.audit().firedRules());
    assertEquals(3, context.audit().iterations());
  }

  @Test
  public void multipleMatchingRules_firstRegisteredWins() {
    EqualsOperator original = equalsOp("field", "a");
    EqualsOperator firstOutput = equalsOp("field", "b");
    EqualsOperator secondOutput = equalsOp("field", "c");
    OperatorRewriteEngine engine =
        new OperatorRewriteEngine(
            List.of(
                stubRule("first", op -> op == original ? firstOutput : op),
                stubRule("second", op -> op == original ? secondOutput : op)));

    RewriteContext context = newContext();
    Operator result = engine.rewrite(original, context);

    assertSame(firstOutput, result);
    assertEquals(List.of("first"), context.audit().firedRules());
    assertEquals(2, context.audit().iterations());
  }

  @Test
  public void nonConvergingRule_stopsAtMaxIterations() {
    EqualsOperator ping = equalsOp("field", "ping");
    EqualsOperator pong = equalsOp("field", "pong");
    OperatorRewriteEngine engine =
        new OperatorRewriteEngine(List.of(stubRule("churn", op -> op == ping ? pong : ping)));

    RewriteContext context = newContext();
    engine.rewrite(ping, context);

    assertEquals(OperatorRewriteEngine.MAX_ITERATIONS, context.audit().iterations());
    assertEquals(OperatorRewriteEngine.MAX_ITERATIONS, context.audit().firedRules().size());
  }

  @Test
  public void identityReturningRule_treatedAsNoOp() {
    EqualsOperator op = equalsOp("field", "a");
    OperatorRewriteEngine engine =
        new OperatorRewriteEngine(List.of(stubRule("noop", operator -> operator)));

    RewriteContext context = newContext();
    Operator result = engine.rewrite(op, context);

    assertSame(op, result);
    assertTrue(context.audit().firedRules().isEmpty());
    assertEquals(1, context.audit().iterations());
  }

  /**
   * One fixture per direct permit of {@link Operator}. Order is insertion order so test failures
   * are reproducible. Field values are the minimum needed to satisfy the records' constructors —
   * semantic validity (e.g., a real geometry, valid query vector) is irrelevant to the engine's
   * traversal, which only dispatches on type.
   */
  private static Map<Class<?>, Operator> directPermitsFixtures() throws Exception {
    Score score = Score.defaultScore();
    FieldPath fp = FieldPath.parse("title");
    EqualsOperator dummyChild = equalsOp("a", "1");

    Map<Class<?>, Operator> m = new LinkedHashMap<>();
    m.put(AllDocumentsOperator.class, AllDocumentsOperator.INSTANCE);
    m.put(
        com.xgen.mongot.index.query.operators.AutocompleteOperator.class,
        new AutocompleteOperatorBuilder().path("title").query("q").build());
    m.put(CompoundOperator.class, new CompoundOperatorBuilder().filter(dummyChild).build());
    m.put(
        EmbeddedDocumentOperator.class,
        new EmbeddedDocumentOperator(score, FieldPath.parse("nested"), dummyChild));
    m.put(EqualsOperator.class, dummyChild);
    m.put(ExistsOperator.class, new ExistsOperator(score, "title"));
    m.put(
        GeoShapeOperator.class,
        new GeoShapeOperator(
            score,
            List.of(fp),
            new GeometryShape(new com.mongodb.client.model.geojson.Point(new Position(0, 0))),
            GeoShapeOperator.Relation.INTERSECTS));
    m.put(
        GeoWithinOperator.class, new GeoWithinOperator(score, List.of(fp), new Circle(0, 0, 1.0)));
    m.put(
        com.xgen.mongot.index.query.operators.HasAncestorOperator.class,
        new HasAncestorOperatorBuilder().ancestorPath("anc").operator(dummyChild).build());
    m.put(
        com.xgen.mongot.index.query.operators.HasRootOperator.class,
        new HasRootOperatorBuilder().operator(dummyChild).build());
    m.put(
        InOperator.class,
        new InOperator(score, List.of(fp), List.of(new StringValue("v")), Optional.empty()));
    m.put(
        KnnBetaOperator.class,
        new KnnBetaOperator(score, List.of(fp), List.of(), Optional.empty(), 1));
    m.put(MoreLikeThisOperator.class, new MoreLikeThisOperator(List.of(new BsonDocument()), score));
    m.put(NearOperator.class, new NearOperator(score, List.of(fp), new LongPoint(0L), 1.0));
    m.put(
        com.xgen.mongot.index.query.operators.PhraseOperator.class,
        new PhraseOperatorBuilder().path("title").query("q").build());
    m.put(QueryStringOperator.class, new QueryStringOperator(score, "q", "title"));
    m.put(
        RangeOperator.class,
        new RangeOperator(
            score,
            List.of(fp),
            new NumericRangeBound(Optional.of(new LongPoint(0L)), Optional.empty(), true, false),
            Optional.empty()));
    // Sealed sub-interfaces: one concrete representative each.
    m.put(
        com.xgen.mongot.index.query.operators.SearchOperator.class,
        new SearchDisjunctionOperator(
            score, List.of(StringPathBuilder.fieldPath("title")), List.of("q")));
    m.put(
        com.xgen.mongot.index.query.operators.SpanOperator.class,
        new SpanTermOperator(
            score,
            new TermBaseOperator(
                score, List.of(StringPathBuilder.fieldPath("title")), List.of("q"))));
    m.put(
        com.xgen.mongot.index.query.operators.TermLevelOperator.class,
        new WildcardOperator(
            score, List.of(UnresolvedStringPathBuilder.fieldPath("title")), List.of("q"), false));
    m.put(
        com.xgen.mongot.index.query.operators.TermOperator.class,
        new TermBaseOperator(score, List.of(StringPathBuilder.fieldPath("title")), List.of("q")));
    m.put(
        com.xgen.mongot.index.query.operators.TextOperator.class,
        new TextOperatorBuilder().path("title").query("q").build());
    m.put(
        VectorSearchOperator.class,
        new VectorSearchOperator(
            score,
            new ExactVectorSearchCriteria(
                fp,
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                1,
                false,
                Optional.empty())));
    return m;
  }

  private static EqualsOperator equalsOp(String path, String value) {
    return new EqualsOperatorBuilder().path(path).value(value).build();
  }

  private static RewriteContext newContext() {
    QueryFactoryContext factoryContext = Mockito.mock(QueryFactoryContext.class);
    IndexMetricsUpdater.QueryingMetricsUpdater metrics =
        new IndexMetricsUpdater.QueryingMetricsUpdater(SearchIndex.mockMetricsFactory());
    return new RewriteContext(
        null, factoryContext, DynamicFeatureFlagRegistry.empty(), metrics, new RewriteAudit());
  }

  /** Rule stub driven by the given function; the flag is arbitrary — the engine never reads it. */
  private static OperatorRewriteRule stubRule(String name, UnaryOperator<Operator> rewrite) {
    return new OperatorRewriteRule() {
      @Override
      public Operator apply(Operator operator, RewriteContext context) {
        return rewrite.apply(operator);
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
