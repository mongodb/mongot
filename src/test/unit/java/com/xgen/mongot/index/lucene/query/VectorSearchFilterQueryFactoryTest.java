package com.xgen.mongot.index.lucene.query;

import static com.xgen.mongot.util.bson.FloatVector.OriginalType.NATIVE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.xgen.mongot.featureflag.FeatureFlags;
import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlagConfig;
import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlagConfig.Phase;
import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlagConfig.Scope;
import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlagRegistry;
import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlags;
import com.xgen.mongot.index.IndexMetricsUpdater;
import com.xgen.mongot.index.definition.VectorAutoEmbedFieldDefinition;
import com.xgen.mongot.index.definition.VectorFieldDefinitionResolver;
import com.xgen.mongot.index.definition.VectorIndexCapabilities;
import com.xgen.mongot.index.definition.VectorIndexDefinition;
import com.xgen.mongot.index.definition.VectorIndexFilterFieldDefinition;
import com.xgen.mongot.index.definition.VectorSimilarity;
import com.xgen.mongot.index.definition.VectorTextFieldDefinition;
import com.xgen.mongot.index.definition.quantization.VectorQuantization;
import com.xgen.mongot.index.lucene.query.context.VectorQueryFactoryContext;
import com.xgen.mongot.index.query.InvalidQueryException;
import com.xgen.mongot.index.query.operators.VectorSearchFilter;
import com.xgen.mongot.index.query.operators.VectorSearchFilter.ClauseFilter;
import com.xgen.mongot.index.version.IndexFormatVersion;
import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.bson.Vector;
import com.xgen.testing.mongot.index.definition.VectorDataFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.VectorIndexDefinitionBuilder;
import com.xgen.testing.mongot.index.query.ApproximateVectorQueryCriteriaBuilder;
import com.xgen.testing.mongot.index.query.VectorQueryBuilder;
import com.xgen.testing.mongot.index.query.operators.mql.ClauseBuilder;
import com.xgen.testing.mongot.index.query.operators.mql.MqlFilterOperatorBuilder;
import com.xgen.testing.mongot.index.query.operators.mql.ValueBuilder;
import com.xgen.testing.mongot.mock.index.SearchIndex;
import java.util.List;
import java.util.Optional;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.bson.types.ObjectId;
import org.junit.Assert;
import org.junit.Test;

public class VectorSearchFilterQueryFactoryTest {

  private static final IndexMetricsUpdater.QueryingMetricsUpdater metrics =
      new IndexMetricsUpdater.QueryingMetricsUpdater(SearchIndex.mockMetricsFactory());

  @Test
  public void isAutoEmbedField_validationSucceeds() {
    FieldPath textPath = FieldPath.parse("content");
    FieldPath autoEmbedPath = FieldPath.parse("description");
    FieldPath filterPath = FieldPath.parse("category");

    VectorIndexDefinition indexDefinition =
        VectorIndexDefinitionBuilder.builder()
            .setFields(
                List.of(
                    new VectorTextFieldDefinition("voyage-3-large", textPath),
                    new VectorAutoEmbedFieldDefinition("voyage-3-large", autoEmbedPath),
                    VectorIndexFilterFieldDefinition.create(filterPath)))
            .build();

    VectorQueryFactoryContext context =
        new VectorQueryFactoryContext(
            new VectorFieldDefinitionResolver(indexDefinition, IndexFormatVersion.CURRENT),
            FeatureFlags.getDefault(),
            metrics);

    assertTrue(context.isAutoEmbedField(textPath));
    assertTrue(context.isAutoEmbedField(autoEmbedPath));

    assertFalse(context.isAutoEmbedField(filterPath));
  }

  @Test
  public void filterOnNonAutoEmbedField_success() throws Exception {
    // Index with vector data field and separate filter field
    FieldPath vectorPath = FieldPath.parse("embedding");
    FieldPath filterPath = FieldPath.parse("category");

    var query =
        VectorQueryBuilder.builder()
            .index("test")
            .criteria(
                ApproximateVectorQueryCriteriaBuilder.builder()
                    .path(vectorPath)
                    .numCandidates(20)
                    .limit(10)
                    .queryVector(Vector.fromFloats(new float[] {1f, 2f, 3f}, NATIVE))
                    .filter(
                        new ClauseFilter(
                            ClauseBuilder.simpleClause()
                                .path(filterPath) // Filter on NON-auto-embed field
                                .addOperator(
                                    MqlFilterOperatorBuilder.eq()
                                        .value(ValueBuilder.string("electronics"))
                                        .build())
                                .build()))
                    .build())
            .build();

    // filtering on regular field works fine
    new LuceneVectorTranslation(
            List.of(
                VectorDataFieldDefinitionBuilder.builder()
                    .path(vectorPath)
                    .numDimensions(3)
                    .similarity(VectorSimilarity.EUCLIDEAN)
                    .quantization(VectorQuantization.NONE)
                    .build(),
                VectorIndexFilterFieldDefinition.create(filterPath)))
        .translate(query);
  }

  @Test
  public void filterOnAutoEmbedField_throwsException() throws Exception {
    // Index with TEXT auto-embedding field and separate vector data field
    FieldPath textPath = FieldPath.parse("description");
    FieldPath vectorPath = FieldPath.parse("embedding");

    var query =
        VectorQueryBuilder.builder()
            .index("test")
            .criteria(
                ApproximateVectorQueryCriteriaBuilder.builder()
                    .path(vectorPath)
                    .numCandidates(20)
                    .limit(10)
                    .queryVector(Vector.fromFloats(new float[] {1f, 2f, 3f}, NATIVE))
                    .filter(
                        new ClauseFilter(
                            ClauseBuilder.simpleClause()
                                .path(textPath) // Filter on auto-embed field
                                .addOperator(
                                    MqlFilterOperatorBuilder.eq()
                                        .value(ValueBuilder.string("laptop"))
                                        .build())
                                .build()))
                    .build())
            .build();

    // Should throw InvalidQueryException
    var exception =
        assertThrows(
            InvalidQueryException.class,
            () ->
                new LuceneVectorTranslation(
                        List.of(
                            new VectorTextFieldDefinition("voyage-3-large", textPath),
                            VectorDataFieldDefinitionBuilder.builder()
                                .path(vectorPath)
                                .numDimensions(3)
                                .similarity(VectorSimilarity.EUCLIDEAN)
                                .quantization(VectorQuantization.NONE)
                                .build()))
                    .translate(query));

    assertTrue(
        "Expected error about auto-embedding field, got: " + exception.getMessage(),
        exception.getMessage().contains("is an auto-embedding field"));
    assertTrue(
        "Expected field name in error message, got: " + exception.getMessage(),
        exception.getMessage().contains("description"));
  }

  @Test
  public void existsOnIndexedFilterField_success() throws Exception {
    FieldPath vectorPath = FieldPath.parse("embedding");
    FieldPath filterPath = FieldPath.parse("category");

    var query =
        VectorQueryBuilder.builder()
            .index("test")
            .criteria(
                ApproximateVectorQueryCriteriaBuilder.builder()
                    .path(vectorPath)
                    .numCandidates(20)
                    .limit(10)
                    .queryVector(Vector.fromFloats(new float[] {1f, 2f, 3f}, NATIVE))
                    .filter(
                        new ClauseFilter(
                            ClauseBuilder.simpleClause()
                                .path(filterPath)
                                .addOperator(MqlFilterOperatorBuilder.exists().value(true).build())
                                .build()))
                    .build())
            .build();

    VectorIndexDefinition definition =
        VectorIndexDefinitionBuilder.builder()
            .withEuclideanVectorField("embedding", 3)
            .withFilterPath("category")
            .indexFeatureVersion(VectorIndexCapabilities.CURRENT_FEATURE_VERSION)
            .build();

    new LuceneVectorTranslation(definition).translate(query);
  }

  @Test
  public void existsOnNonIndexedField_throwsException() throws Exception {
    FieldPath vectorPath = FieldPath.parse("embedding");
    FieldPath unindexedPath = FieldPath.parse("not_a_filter");

    var query =
        VectorQueryBuilder.builder()
            .index("test")
            .criteria(
                ApproximateVectorQueryCriteriaBuilder.builder()
                    .path(vectorPath)
                    .numCandidates(20)
                    .limit(10)
                    .queryVector(Vector.fromFloats(new float[] {1f, 2f, 3f}, NATIVE))
                    .filter(
                        new ClauseFilter(
                            ClauseBuilder.simpleClause()
                                .path(unindexedPath)
                                .addOperator(MqlFilterOperatorBuilder.exists().value(true).build())
                                .build()))
                    .build())
            .build();

    VectorIndexDefinition definition =
        VectorIndexDefinitionBuilder.builder()
            .withEuclideanVectorField("embedding", 3)
            .withFilterPath("category")
            .indexFeatureVersion(VectorIndexCapabilities.CURRENT_FEATURE_VERSION)
            .build();

    var exception =
        assertThrows(
            InvalidQueryException.class,
            () -> new LuceneVectorTranslation(definition).translate(query));

    Assert.assertEquals(
        "Path 'not_a_filter' needs to be indexed as filter", exception.getMessage());
  }

  /**
   * Pins the optimizer wiring contract: every entry into filter translation must run through {@link
   * com.xgen.mongot.index.query.optimizer.QueryOptimizer#optimize}. Production callers (e.g.,
   * {@code VectorSearchQueryFactory.fromQuery}) reach the package-private {@code
   * createLuceneFilter(filter, SingleQueryContext)} overload, so this test exercises that exact
   * entry point and asserts {@code optimizerInvocations} increments. If the optimizer is ever
   * disconnected from this overload again, this test fails.
   */
  @Test
  public void singleQueryContextEntryPoint_invokesOptimizer() throws Exception {
    FieldPath vectorPath = FieldPath.parse("embedding");
    FieldPath filterPath = FieldPath.parse("category");

    IndexMetricsUpdater.QueryingMetricsUpdater testMetrics =
        new IndexMetricsUpdater.QueryingMetricsUpdater(SearchIndex.mockMetricsFactory());
    VectorIndexDefinition indexDefinition =
        VectorIndexDefinitionBuilder.builder()
            .setFields(
                List.of(
                    VectorDataFieldDefinitionBuilder.builder()
                        .path(vectorPath)
                        .numDimensions(3)
                        .similarity(VectorSimilarity.EUCLIDEAN)
                        .quantization(VectorQuantization.NONE)
                        .build(),
                    VectorIndexFilterFieldDefinition.create(filterPath)))
            .build();
    VectorQueryFactoryContext context =
        new VectorQueryFactoryContext(
            new VectorFieldDefinitionResolver(indexDefinition, IndexFormatVersion.CURRENT),
            FeatureFlags.getDefault(),
            testMetrics);
    DynamicFeatureFlagRegistry dffRegistry = registryWithQueryOptimizerEnabled();
    VectorSearchFilterQueryFactory factory =
        VectorSearchFilterQueryFactory.create(context, dffRegistry);

    VectorSearchFilter filter =
        new ClauseFilter(
            ClauseBuilder.simpleClause()
                .path(filterPath)
                .addOperator(
                    MqlFilterOperatorBuilder.eq().value(ValueBuilder.string("electronics")).build())
                .build());

    double before = testMetrics.getOptimizerInvocations().count();

    try (var directory = new ByteBuffersDirectory()) {
      try (IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig())) {
        writer.commit();
      }
      try (DirectoryReader reader = DirectoryReader.open(directory)) {
        // The package-private overload is the production entry point — see VectorSearchQueryFactory
        // and LuceneSearchQueryFactoryDistributor (KnnBetaOperator filter translation).
        factory.createLuceneFilter(filter, SingleQueryContext.createQueryRoot(reader));
      }
    }

    assertEquals(
        "QueryOptimizer.optimize must be invoked from the SingleQueryContext entry point",
        before + 1.0,
        testMetrics.getOptimizerInvocations().count(),
        0.0);
  }

  private static DynamicFeatureFlagRegistry registryWithQueryOptimizerEnabled() {
    DynamicFeatureFlagConfig config =
        new DynamicFeatureFlagConfig(
            DynamicFeatureFlags.QUERY_OPTIMIZER.getName(),
            Phase.ENABLED,
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

  /**
   * Pins the flag-off filter-translation tree structurally. With QUERY_OPTIMIZER off the optimizer
   * must not run, and the produced Lucene query must match a hand-constructed expected tree built
   * from the same primitives the factory uses internally (see {@code
   * ExistsQueryFactory.existsQuery} and {@code
   * VectorSearchFilterQueryFactory.createLuceneFilterForSimpleClause}). Asserts both directions:
   * {@code optimizerInvocations} does not move, and the {@link Query} {@code .equals} the expected
   * tree. {@link BooleanQuery#equals} is structural, so any silent reshape on the no-op path makes
   * this fail.
   */
  @Test
  public void flagOffTranslation_simpleExists_isStructurallyEqualToHandConstructedBaseline()
      throws Exception {
    FieldPath vectorPath = FieldPath.parse("embedding");
    FieldPath filterPath = FieldPath.parse("category");

    IndexMetricsUpdater.QueryingMetricsUpdater testMetrics =
        new IndexMetricsUpdater.QueryingMetricsUpdater(SearchIndex.mockMetricsFactory());
    VectorIndexDefinition indexDefinition =
        VectorIndexDefinitionBuilder.builder()
            .withEuclideanVectorField(vectorPath.toString(), 3)
            .withFilterPath(filterPath.toString())
            .indexFeatureVersion(VectorIndexCapabilities.CURRENT_FEATURE_VERSION)
            .build();
    VectorQueryFactoryContext context =
        new VectorQueryFactoryContext(
            new VectorFieldDefinitionResolver(indexDefinition, IndexFormatVersion.CURRENT),
            FeatureFlags.getDefault(),
            testMetrics);

    // Flag OFF: an empty DFF registry resolves QUERY_OPTIMIZER to its fallback (false), so the
    // master kill switch is off and QueryOptimizer.optimize short-circuits before any traversal.
    VectorSearchFilterQueryFactory factory =
        VectorSearchFilterQueryFactory.create(context, DynamicFeatureFlagRegistry.empty());

    VectorSearchFilter filter =
        new ClauseFilter(
            ClauseBuilder.simpleClause()
                .path(filterPath)
                .addOperator(MqlFilterOperatorBuilder.exists().value(true).build())
                .build());

    double before = testMetrics.getOptimizerInvocations().count();
    Query actual;
    try (ByteBuffersDirectory directory = new ByteBuffersDirectory()) {
      try (IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig())) {
        writer.commit();
      }
      try (DirectoryReader reader = DirectoryReader.open(directory)) {
        actual = factory.createLuceneFilter(filter, SingleQueryContext.createQueryRoot(reader));
      }
    }

    assertEquals(
        "optimizer must not run when QUERY_OPTIMIZER is off",
        before,
        testMetrics.getOptimizerInvocations().count(),
        0.0);

    // ExistsQueryFactory.existsQuery for exists=true on a non-embedded index returns
    // ConstantScoreQuery(TermQuery($meta/fieldNames, <path>)); createLuceneFilterForSimpleClause
    // then wraps each operator in a single-clause BooleanQuery with Occur.MUST.
    Query expectedExistsLeaf =
        new ConstantScoreQuery(new TermQuery(new Term("$meta/fieldNames", "category")));
    Query expected =
        new BooleanQuery.Builder().add(expectedExistsLeaf, BooleanClause.Occur.MUST).build();

    assertEquals(expected, actual);
  }

  /**
   * Companion to the simple-clause test above, covering the compound path. With QUERY_OPTIMIZER
   * off, an {@code $and} of two exists clauses must translate to the same nested {@link
   * BooleanQuery} structure the factory has always produced: an outer {@code MUST/MUST} pair (from
   * {@code createLuceneFilterForCompoundClause}), each wrapping a single-MUST {@link BooleanQuery}
   * (from {@code createLuceneFilterForSimpleClause}) around the leaf {@code
   * ConstantScoreQuery(TermQuery($meta/fieldNames, <path>))}. Compound translation has its own
   * factory branch and its own optimizer-traversal hook, so a wiring regression there would surface
   * differently than a simple-clause regression — the {@code SimpleClause} test alone wouldn't
   * catch it.
   */
  @Test
  public void flagOffTranslation_compoundAnd_isStructurallyEqualToHandConstructedBaseline()
      throws Exception {
    FieldPath vectorPath = FieldPath.parse("embedding");
    FieldPath categoryPath = FieldPath.parse("category");
    FieldPath tagPath = FieldPath.parse("tag");

    IndexMetricsUpdater.QueryingMetricsUpdater testMetrics =
        new IndexMetricsUpdater.QueryingMetricsUpdater(SearchIndex.mockMetricsFactory());
    VectorIndexDefinition indexDefinition =
        VectorIndexDefinitionBuilder.builder()
            .withEuclideanVectorField(vectorPath.toString(), 3)
            .withFilterPath(categoryPath.toString())
            .withFilterPath(tagPath.toString())
            .indexFeatureVersion(VectorIndexCapabilities.CURRENT_FEATURE_VERSION)
            .build();
    VectorQueryFactoryContext context =
        new VectorQueryFactoryContext(
            new VectorFieldDefinitionResolver(indexDefinition, IndexFormatVersion.CURRENT),
            FeatureFlags.getDefault(),
            testMetrics);

    VectorSearchFilterQueryFactory factory =
        VectorSearchFilterQueryFactory.create(context, DynamicFeatureFlagRegistry.empty());

    VectorSearchFilter filter =
        new ClauseFilter(
            ClauseBuilder.andClause()
                .addClause(
                    ClauseBuilder.simpleClause()
                        .path(categoryPath)
                        .addOperator(MqlFilterOperatorBuilder.exists().value(true).build())
                        .build())
                .addClause(
                    ClauseBuilder.simpleClause()
                        .path(tagPath)
                        .addOperator(MqlFilterOperatorBuilder.exists().value(true).build())
                        .build())
                .explicitAnd(true)
                .build());

    double before = testMetrics.getOptimizerInvocations().count();
    Query actual;
    try (ByteBuffersDirectory directory = new ByteBuffersDirectory()) {
      try (IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig())) {
        writer.commit();
      }
      try (DirectoryReader reader = DirectoryReader.open(directory)) {
        actual = factory.createLuceneFilter(filter, SingleQueryContext.createQueryRoot(reader));
      }
    }

    assertEquals(
        "optimizer must not run when QUERY_OPTIMIZER is off",
        before,
        testMetrics.getOptimizerInvocations().count(),
        0.0);

    Query categoryChild =
        new BooleanQuery.Builder()
            .add(
                new ConstantScoreQuery(new TermQuery(new Term("$meta/fieldNames", "category"))),
                BooleanClause.Occur.MUST)
            .build();
    Query tagChild =
        new BooleanQuery.Builder()
            .add(
                new ConstantScoreQuery(new TermQuery(new Term("$meta/fieldNames", "tag"))),
                BooleanClause.Occur.MUST)
            .build();
    Query expected =
        new BooleanQuery.Builder()
            .add(categoryChild, BooleanClause.Occur.MUST)
            .add(tagChild, BooleanClause.Occur.MUST)
            .build();

    assertEquals(expected, actual);
  }
}
