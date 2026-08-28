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
import com.xgen.mongot.index.analyzer.AnalyzerRegistry;
import com.xgen.mongot.index.definition.SearchIndexDefinition;
import com.xgen.mongot.index.lucene.field.FieldName;
import com.xgen.mongot.index.lucene.query.LuceneSearchQueryFactoryDistributor;
import com.xgen.mongot.index.lucene.synonym.LuceneSynonymRegistry;
import com.xgen.mongot.index.query.QueryOptimizationFlags;
import com.xgen.mongot.index.query.operators.Operator;
import com.xgen.mongot.index.query.operators.RangeOperator;
import com.xgen.mongot.index.query.optimizer.DifferentialTestHarness.DocField;
import com.xgen.mongot.index.query.optimizer.rules.RangeBoundsMergeOperatorRule;
import com.xgen.mongot.index.query.points.LongPoint;
import com.xgen.mongot.index.version.IndexFormatVersion;
import com.xgen.mongot.util.FieldPath;
import com.xgen.testing.mongot.index.analyzer.AnalyzerRegistryBuilder;
import com.xgen.testing.mongot.index.definition.DocumentFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.SearchIndexDefinitionBuilder;
import com.xgen.testing.mongot.index.query.operators.OperatorBuilder;
import com.xgen.testing.mongot.mock.index.SearchIndex;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.lucene.search.Query;
import org.bson.types.ObjectId;
import org.junit.Test;

/**
 * Differential equivalence tests for {@link RangeBoundsMergeOperatorRule}, run through the real
 * {@code $search} pipeline: the same compound operator is translated twice by {@link
 * LuceneSearchQueryFactoryDistributor#createQuery} — once with the optimizer flags off (two
 * half-open scans) and once with them on (one fused closed scan) — and both Lucene queries execute
 * against a real in-memory index. The matched document sets must be identical.
 *
 * <p>The multi-valued case exercises the translation-time fallback keyed off {@link
 * RangeOperator#fusedByOptimizer()}: with array data on the field, an array can satisfy {@code gt
 * lo} and {@code lt hi} with two different elements while no single element lies in the
 * intersection, so the fused operator must decompose back into the two half-open scans.
 */
public class RangeBoundsMergeSearchDifferentialTest {

  private static final String PATH = "price";

  /** Dynamic mappings so the range path resolves without an explicit field definition. */
  private static final SearchIndexDefinition INDEX_DEFINITION =
      SearchIndexDefinitionBuilder.builder()
          .defaultMetadata()
          .mappings(DocumentFieldDefinitionBuilder.builder().dynamic(true).build())
          .build();

  @Test
  public void compoundFilter_gtAndLt_singleValued_fuses_andMatchesSameDocs() throws Exception {
    try (var harness =
        DifferentialTestHarness.builder()
            .addDocument("below", int64(PATH, 0))
            .addDocument("inside", int64(PATH, 5))
            .addDocument("above", int64(PATH, 20))
            .build()) {

      Operator compound =
          OperatorBuilder.compound()
              .filter(lowerHalfOpen(PATH))
              .filter(upperHalfOpen(PATH))
              .build();

      Translations translations = translate(harness, compound);

      assertTrue(
          RangeBoundsMergeOperatorRule.NAME + " should have fired",
          translations.ruleFiredCount() >= 1.0);
      assertNotEquals(
          "the optimizer should reshape the translated query",
          translations.original(),
          translations.optimized());

      assertMatchedKeys(harness, translations, Set.of("inside"));
    }
  }

  @Test
  public void compoundFilter_gtAndLt_multiValuedField_fallbackMatchesSameDocs() throws Exception {
    // {price: [0, 20]}: 20 satisfies gt 1 and 0 satisfies lt 10, but no single element lies in
    // (1, 10) — a single closed scan would drop the document, so the translation fallback must
    // keep the two half-open scans.
    try (var harness =
        DifferentialTestHarness.builder()
            .addDocument("straddles", int64(PATH, 0), int64(PATH, 20))
            .addDocument("inside", int64(PATH, 5))
            .addDocument("outside", int64(PATH, 12), int64(PATH, 15))
            .build()) {

      Operator compound =
          OperatorBuilder.compound()
              .filter(lowerHalfOpen(PATH))
              .filter(upperHalfOpen(PATH))
              .build();

      Translations translations = translate(harness, compound);

      // The rule fuses without consulting the index; only translation sees the array data and
      // decomposes the fused operator back into two half-open scans, so no structural difference
      // is asserted here — only set equivalence.
      assertTrue(
          RangeBoundsMergeOperatorRule.NAME + " should have fired",
          translations.ruleFiredCount() >= 1.0);

      assertMatchedKeys(harness, translations, Set.of("straddles", "inside"));
    }
  }

  @Test
  public void compoundMust_gtAndLt_declines_andMatchesSameDocs() throws Exception {
    // The rule never fuses must clauses (each contributes its own score); the translation must be
    // byte-identical with the optimizer on.
    try (var harness =
        DifferentialTestHarness.builder()
            .addDocument("inside", int64(PATH, 5))
            .addDocument("above", int64(PATH, 20))
            .build()) {

      Operator compound =
          OperatorBuilder.compound().must(lowerHalfOpen(PATH)).must(upperHalfOpen(PATH)).build();

      Translations translations = translate(harness, compound);

      assertEquals(
          RangeBoundsMergeOperatorRule.NAME + " should not have fired",
          0.0,
          translations.ruleFiredCount(),
          0.0);
      assertEquals(
          "a declined rewrite must leave the translation unchanged",
          translations.original(),
          translations.optimized());

      assertMatchedKeys(harness, translations, Set.of("inside"));
    }
  }

  // ----- pipeline plumbing -----

  private record Pipeline(
      LuceneSearchQueryFactoryDistributor factory,
      IndexMetricsUpdater.QueryingMetricsUpdater metrics) {}

  private record Translations(Query original, Query optimized, double ruleFiredCount) {}

  private static Translations translate(DifferentialTestHarness harness, Operator operator)
      throws Exception {
    Pipeline original = pipeline(DynamicFeatureFlagRegistry.empty());
    Pipeline optimized = pipeline(rangeMergeFlags());

    Query originalQuery = createQuery(original, harness, operator);
    Query optimizedQuery = createQuery(optimized, harness, operator);
    double fired =
        optimized.metrics().getOptimizerRuleFiredCounter(RangeBoundsMergeOperatorRule.NAME).count();
    return new Translations(originalQuery, optimizedQuery, fired);
  }

  private static Query createQuery(
      Pipeline pipeline, DifferentialTestHarness harness, Operator operator) throws Exception {
    return pipeline
        .factory()
        .createQuery(
            operator, harness.reader(), Optional.empty(), QueryOptimizationFlags.DEFAULT_OPTIONS);
  }

  private static void assertMatchedKeys(
      DifferentialTestHarness harness, Translations translations, Set<String> expectedKeys)
      throws Exception {
    var result = harness.compare(translations.original(), translations.optimized());
    assertTrue(result.describe(), result.equivalent());
    assertEquals(expectedKeys, result.matchedByOriginal());
  }

  private static Pipeline pipeline(DynamicFeatureFlagRegistry flags) {
    IndexMetricsUpdater.QueryingMetricsUpdater metrics =
        new IndexMetricsUpdater.QueryingMetricsUpdater(SearchIndex.mockMetricsFactory());
    AnalyzerRegistry analyzerRegistry = AnalyzerRegistryBuilder.empty();
    LuceneSearchQueryFactoryDistributor factory =
        LuceneSearchQueryFactoryDistributor.create(
            INDEX_DEFINITION,
            IndexFormatVersion.CURRENT,
            analyzerRegistry,
            LuceneSynonymRegistry.create(analyzerRegistry, Map.of(), Optional.empty()),
            metrics,
            false,
            FeatureFlags.getDefault(),
            flags);
    return new Pipeline(factory, metrics);
  }

  // ----- flag registries -----

  private static DynamicFeatureFlagRegistry rangeMergeFlags() {
    DynamicFeatureFlagConfig[] configs =
        new DynamicFeatureFlagConfig[] {
          flagConfig(DynamicFeatureFlags.QUERY_OPTIMIZER),
          flagConfig(DynamicFeatureFlags.RANGE_BOUNDS_MERGE_OPTIMIZATION)
        };
    return new DynamicFeatureFlagRegistry(
        Optional.of(List.of(configs)),
        Optional.empty(),
        Optional.of(new ObjectId()),
        Optional.empty());
  }

  private static DynamicFeatureFlagConfig flagConfig(DynamicFeatureFlags flag) {
    return new DynamicFeatureFlagConfig(
        flag.getName(), Phase.ENABLED, List.of(), List.of(), 0, Scope.GROUP);
  }

  // ----- document and operator helpers -----

  /** Index an integral value under the int64 multi-value point field the range query scans. */
  private static DocField int64(String path, long value) {
    return DocField.longValue(
        FieldName.TypeField.NUMBER_INT64_MULTIPLE.getLuceneFieldName(
            FieldPath.parse(path), Optional.empty()),
        value);
  }

  private static RangeOperator lowerHalfOpen(String path) {
    return OperatorBuilder.range()
        .path(path)
        .numericBounds(Optional.of(new LongPoint(1L)), Optional.empty(), false, false)
        .build();
  }

  private static RangeOperator upperHalfOpen(String path) {
    return OperatorBuilder.range()
        .path(path)
        .numericBounds(Optional.empty(), Optional.of(new LongPoint(10L)), false, false)
        .build();
  }
}
