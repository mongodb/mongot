package com.xgen.mongot.index.query.optimizer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import com.xgen.mongot.featureflag.FeatureFlags;
import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlagConfig;
import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlagConfig.Phase;
import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlagConfig.Scope;
import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlagRegistry;
import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlags;
import com.xgen.mongot.index.IndexMetricsUpdater;
import com.xgen.mongot.index.definition.VectorFieldDefinitionResolver;
import com.xgen.mongot.index.definition.VectorIndexCapabilities;
import com.xgen.mongot.index.definition.VectorIndexDefinition;
import com.xgen.mongot.index.lucene.field.FieldName;
import com.xgen.mongot.index.lucene.query.VectorSearchFilterQueryFactory;
import com.xgen.mongot.index.lucene.query.context.VectorQueryFactoryContext;
import com.xgen.mongot.index.query.operators.VectorSearchFilter;
import com.xgen.mongot.index.query.operators.mql.AndClause;
import com.xgen.mongot.index.query.operators.mql.Clause;
import com.xgen.mongot.index.query.operators.mql.SimpleClause;
import com.xgen.mongot.index.query.optimizer.DifferentialTestHarness.DocField;
import com.xgen.mongot.index.query.optimizer.rules.RangeBoundsMergeMqlAndClauseRule;
import com.xgen.mongot.index.query.optimizer.rules.RangeBoundsMergeMqlSimpleClauseRule;
import com.xgen.mongot.index.version.IndexFormatVersion;
import com.xgen.mongot.util.FieldPath;
import com.xgen.testing.mongot.index.definition.VectorIndexDefinitionBuilder;
import com.xgen.testing.mongot.index.query.operators.mql.ClauseBuilder;
import com.xgen.testing.mongot.index.query.operators.mql.MqlFilterOperatorBuilder;
import com.xgen.testing.mongot.index.query.operators.mql.ValueBuilder;
import com.xgen.testing.mongot.mock.index.SearchIndex;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.apache.lucene.search.Query;
import org.bson.types.ObjectId;
import org.junit.Test;

/**
 * Differential equivalence tests for the range-bounds-merge MQL clause rules, run through the real
 * pipeline: the same {@link VectorSearchFilter} is translated twice by {@link
 * VectorSearchFilterQueryFactory} — once with the optimizer flags off (the AND-of-two-opens shape)
 * and once with them on (the fused closed-range shape) — and both Lucene queries execute against a
 * real in-memory index. The matched document sets must be identical.
 *
 * <p>Documents are indexed with the Lucene field names and encodings that vector-index ingestion
 * writes for int64 filter fields ({@link FieldName.TypeField#NUMBER_INT64_MULTIPLE} points), so the
 * factory-produced queries match them exactly as they would in production. The multi-valued cases
 * exercise the translation-time fallback: a fused operator on a field with array data must
 * decompose back into the two half-open scans, because an array can satisfy {@code $gte lo} and
 * {@code $lte hi} with two different elements while no single element lies in {@code [lo, hi]}.
 *
 * <p>Each firing test also asserts the expected rule actually fired (via the per-rule counter), so
 * a silently-disabled rule cannot produce a vacuous pass. The operator-side rule is differentially
 * tested through the search-side translation stack in {@link
 * RangeBoundsMergeSearchDifferentialTest}.
 */
public class RangeBoundsMergeDifferentialTest {

  @Test
  public void simpleClause_gteLtePair_singleValued_matchesSameDocs() throws Exception {
    try (var harness =
        DifferentialTestHarness.builder()
            .addDocument("below", int64("price", 5))
            .addDocument("inside", int64("price", 50))
            .addDocument("above", int64("price", 500))
            .build()) {

      // {price: {$gte: 10, $lte: 100}} — one SimpleClause carrying both operators.
      SimpleClause pair =
          ClauseBuilder.simpleClause()
              .path("price")
              .addOperator(MqlFilterOperatorBuilder.gte().value(ValueBuilder.longNumber(10L)))
              .addOperator(MqlFilterOperatorBuilder.lte().value(ValueBuilder.longNumber(100L)))
              .build();

      assertFusedAndEquivalent(
          harness,
          new VectorSearchFilter.ClauseFilter(pair),
          RangeBoundsMergeMqlSimpleClauseRule.NAME,
          Set.of("inside"),
          "price");
    }
  }

  @Test
  public void andOfTwoSimpleClauses_gteLtePair_matchesSameDocs() throws Exception {
    try (var harness =
        DifferentialTestHarness.builder()
            .addDocument("below", int64("price", 5))
            .addDocument("inside", int64("price", 50))
            .addDocument("above", int64("price", 500))
            .build()) {

      // {$and: [{price: {$gte: 10}}, {price: {$lte: 100}}]} — the pair split across two sibling
      // SimpleClauses, which is the AndClause rule's input shape.
      Clause filter = new AndClause(List.of(gte("price", 10L), lte("price", 100L)), false);

      assertFusedAndEquivalent(
          harness,
          new VectorSearchFilter.ClauseFilter(filter),
          RangeBoundsMergeMqlAndClauseRule.NAME,
          Set.of("inside"),
          "price");
    }
  }

  @Test
  public void simpleClause_gteLtePair_multiValuedField_fallbackMatchesSameDocs() throws Exception {
    try (var harness =
        DifferentialTestHarness.builder()
            // {price: [5, 500]}: 500 satisfies $gte 10 and 5 satisfies $lte 100, but no single
            // element lies in [10, 100] — a single closed scan would drop this document, so the
            // translation fallback must keep the two half-open scans.
            .addDocument("straddles", int64("price", 5), int64("price", 500))
            .addDocument("inside", int64("price", 50))
            .addDocument("above", int64("price", 500))
            .build()) {

      SimpleClause pair =
          ClauseBuilder.simpleClause()
              .path("price")
              .addOperator(MqlFilterOperatorBuilder.gte().value(ValueBuilder.longNumber(10L)))
              .addOperator(MqlFilterOperatorBuilder.lte().value(ValueBuilder.longNumber(100L)))
              .build();
      VectorSearchFilter filter = new VectorSearchFilter.ClauseFilter(pair);

      Pipeline original = pipeline(DynamicFeatureFlagRegistry.empty(), "price");
      Pipeline optimized = pipeline(rangeMergeFlags(), "price");

      Query originalQuery = original.factory().createLuceneFilter(filter, harness.reader());
      Query optimizedQuery = optimized.factory().createLuceneFilter(filter, harness.reader());

      // The rule fuses without consulting the index; only translation sees the array data. The
      // fallback decomposes the fused operator back into two half-open scans, so no structural
      // difference from the original is asserted here — only set equivalence.
      assertRuleFired(optimized, RangeBoundsMergeMqlSimpleClauseRule.NAME);

      var result = harness.compare(originalQuery, optimizedQuery);
      assertTrue(result.describe(), result.equivalent());
      assertEquals(Set.of("straddles", "inside"), result.matchedByOriginal());
    }
  }

  // ----- pipeline plumbing -----

  private record Pipeline(
      VectorSearchFilterQueryFactory factory, IndexMetricsUpdater.QueryingMetricsUpdater metrics) {}

  /**
   * Translate {@code filter} through the factory twice — optimizer off (original) and on
   * (optimized) — execute both against the harness index, and assert: the expected rule fired, the
   * optimized query is structurally different, the matched doc sets are identical, and they equal
   * {@code expectedKeys}.
   */
  private static void assertFusedAndEquivalent(
      DifferentialTestHarness harness,
      VectorSearchFilter filter,
      String expectedRule,
      Set<String> expectedKeys,
      String... filterPaths)
      throws Exception {
    Pipeline original = pipeline(DynamicFeatureFlagRegistry.empty(), filterPaths);
    Pipeline optimized = pipeline(rangeMergeFlags(), filterPaths);

    Query originalQuery = original.factory().createLuceneFilter(filter, harness.reader());
    Query optimizedQuery = optimized.factory().createLuceneFilter(filter, harness.reader());

    assertRuleFired(optimized, expectedRule);
    assertNotEquals(
        "the optimizer should reshape the translated query", originalQuery, optimizedQuery);

    var result = harness.compare(originalQuery, optimizedQuery);
    assertTrue(result.describe(), result.equivalent());
    assertEquals(expectedKeys, result.matchedByOriginal());
  }

  private static void assertRuleFired(Pipeline pipeline, String ruleName) {
    assertTrue(
        ruleName + " should have fired",
        pipeline.metrics().getOptimizerRuleFiredCounter(ruleName).count() >= 1.0);
  }

  private static Pipeline pipeline(DynamicFeatureFlagRegistry flags, String... filterPaths) {
    IndexMetricsUpdater.QueryingMetricsUpdater metrics =
        new IndexMetricsUpdater.QueryingMetricsUpdater(SearchIndex.mockMetricsFactory());
    VectorIndexDefinitionBuilder builder =
        VectorIndexDefinitionBuilder.builder()
            .withEuclideanVectorField("embedding", 3)
            .indexFeatureVersion(VectorIndexCapabilities.CURRENT_FEATURE_VERSION);
    for (String path : filterPaths) {
      builder.withFilterPath(path);
    }
    VectorIndexDefinition definition = builder.build();
    VectorQueryFactoryContext context =
        new VectorQueryFactoryContext(
            new VectorFieldDefinitionResolver(definition, IndexFormatVersion.CURRENT),
            FeatureFlags.getDefault(),
            metrics);
    return new Pipeline(VectorSearchFilterQueryFactory.create(context, flags), metrics);
  }

  // ----- flag registries -----

  private static DynamicFeatureFlagRegistry rangeMergeFlags() {
    return registryWith(
        flagConfig(DynamicFeatureFlags.QUERY_OPTIMIZER),
        flagConfig(DynamicFeatureFlags.RANGE_BOUNDS_MERGE_OPTIMIZATION));
  }

  private static DynamicFeatureFlagConfig flagConfig(DynamicFeatureFlags flag) {
    return new DynamicFeatureFlagConfig(
        flag.getName(), Phase.ENABLED, List.of(), List.of(), 0, Scope.GROUP);
  }

  private static DynamicFeatureFlagRegistry registryWith(DynamicFeatureFlagConfig... configs) {
    return new DynamicFeatureFlagRegistry(
        Optional.of(List.of(configs)),
        Optional.empty(),
        Optional.of(new ObjectId()),
        Optional.empty());
  }

  // ----- document and clause helpers -----

  /** Index an integral value the way vector-index ingestion writes int64 filter fields. */
  private static DocField int64(String path, long value) {
    return DocField.longValue(
        FieldName.TypeField.NUMBER_INT64_MULTIPLE.getLuceneFieldName(
            FieldPath.parse(path), Optional.empty()),
        value);
  }

  private static SimpleClause gte(String path, long value) throws Exception {
    return ClauseBuilder.simpleClause()
        .path(path)
        .addOperator(MqlFilterOperatorBuilder.gte().value(ValueBuilder.longNumber(value)))
        .build();
  }

  private static SimpleClause lte(String path, long value) throws Exception {
    return ClauseBuilder.simpleClause()
        .path(path)
        .addOperator(MqlFilterOperatorBuilder.lte().value(ValueBuilder.longNumber(value)))
        .build();
  }
}
