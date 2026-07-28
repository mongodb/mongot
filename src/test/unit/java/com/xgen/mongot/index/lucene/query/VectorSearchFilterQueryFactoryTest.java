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
import com.xgen.mongot.index.lucene.field.FieldName;
import com.xgen.mongot.index.lucene.query.context.VectorQueryFactoryContext;
import com.xgen.mongot.index.lucene.util.LuceneDoubleConversionUtils;
import com.xgen.mongot.index.query.InvalidQueryException;
import com.xgen.mongot.index.query.operators.VectorSearchFilter;
import com.xgen.mongot.index.query.operators.VectorSearchFilter.ClauseFilter;
import com.xgen.mongot.index.query.operators.bound.DateRangeBound;
import com.xgen.mongot.index.query.operators.bound.NumericRangeBound;
import com.xgen.mongot.index.query.operators.mql.ClosedRangeBoundComparisonOperator;
import com.xgen.mongot.index.query.operators.mql.SimpleClause;
import com.xgen.mongot.index.query.points.DatePoint;
import com.xgen.mongot.index.query.points.DoublePoint;
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
import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FilterDirectoryReader;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.IndexSearcher;
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

  /**
   * Vector-index ingestion routes each numeric value to exactly one point-field variant by BSON
   * type: integral values to the int64 multi-value field, floating-point values to the double
   * multi-value field. A document like {@code {a: [1, 2.5]}} therefore contributes one point to
   * each variant, and both variants look single-valued when inspected one field at a time — but the
   * document is still multi-valued.
   *
   * <p>Closed-range translation must preserve the semantics of the original pair of open bounds,
   * under which different array elements may satisfy each bound: {@code {a: {$gt: 2.0, $lt: 2.4}}}
   * matches {@code {a: [1, 2.5]}} because 2.5 satisfies the lower bound and 1 satisfies the upper
   * bound. A single fused closed-range query over the point fields cannot match this document, so
   * translation may only fuse when the document's values all live in one field.
   */
  @Test
  public void closedRangeOnMixedIntDoubleArrayDocument_matchesLikeTwoOpenBounds() throws Exception {
    FieldPath filterPath = FieldPath.parse("a");
    VectorQueryFactoryContext context = contextWithFilterPath(filterPath);
    VectorSearchFilterQueryFactory factory =
        VectorSearchFilterQueryFactory.create(context, DynamicFeatureFlagRegistry.empty());

    VectorSearchFilter filter =
        new ClauseFilter(
            new SimpleClause(
                filterPath,
                List.of(
                    new ClosedRangeBoundComparisonOperator(
                        new NumericRangeBound(
                            Optional.of(new DoublePoint(2.0)),
                            Optional.of(new DoublePoint(2.4)),
                            false,
                            false)))));

    String int64Field =
        FieldName.TypeField.NUMBER_INT64_MULTIPLE.getLuceneFieldName(filterPath, Optional.empty());
    String doubleField =
        FieldName.TypeField.NUMBER_DOUBLE_MULTIPLE.getLuceneFieldName(filterPath, Optional.empty());

    try (var directory = new ByteBuffersDirectory()) {
      try (IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig())) {
        // {a: [1, 2.5]} — the same fields and encodings LuceneVectorIndexFieldValueHandler
        // writes: the int element as a raw long point, the double element as a sortable long.
        Document mixedArrayDoc = new Document();
        mixedArrayDoc.add(new LongPoint(int64Field, 1L));
        mixedArrayDoc.add(new LongPoint(doubleField, LuceneDoubleConversionUtils.toLong(2.5)));
        writer.addDocument(mixedArrayDoc);

        // {a: 2.2} — control document inside the range, matched by fused and unfused forms
        // alike; proves the translated query is not trivially empty.
        Document inRangeDoc = new Document();
        inRangeDoc.add(new LongPoint(doubleField, LuceneDoubleConversionUtils.toLong(2.2)));
        writer.addDocument(inRangeDoc);

        writer.commit();
      }
      try (DirectoryReader reader = DirectoryReader.open(directory)) {
        Query query =
            factory.createLuceneFilter(filter, SingleQueryContext.createQueryRoot(reader));
        assertEquals(
            "both documents must match: {a: [1, 2.5]} satisfies $gt via 2.5 and $lt via 1, "
                + "and {a: 2.2} lies inside the range",
            2,
            new IndexSearcher(reader).count(query));
      }
    }
  }

  /**
   * When every value of the filter field lives in a single point-field variant and no document
   * carries more than one, closed-range translation must fuse: the produced query is exactly the
   * single-range translation of the same bounds (wrapped in the single-MUST {@link BooleanQuery}
   * that simple-clause translation always adds), not the AND-of-two-opens fallback — and it still
   * matches only the documents inside the range.
   */
  @Test
  public void closedRangeOnSingleValuedDoubleData_fusesToSingleRangeQuery() throws Exception {
    FieldPath filterPath = FieldPath.parse("a");
    VectorQueryFactoryContext context = contextWithFilterPath(filterPath);
    VectorSearchFilterQueryFactory factory =
        VectorSearchFilterQueryFactory.create(context, DynamicFeatureFlagRegistry.empty());
    NumericRangeBound bounds =
        new NumericRangeBound(
            Optional.of(new DoublePoint(2.0)), Optional.of(new DoublePoint(2.4)), false, false);
    VectorSearchFilter filter =
        new ClauseFilter(
            new SimpleClause(filterPath, List.of(new ClosedRangeBoundComparisonOperator(bounds))));
    String doubleField =
        FieldName.TypeField.NUMBER_DOUBLE_MULTIPLE.getLuceneFieldName(filterPath, Optional.empty());

    try (var directory = new ByteBuffersDirectory()) {
      try (IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig())) {
        // {a: 2.2} inside the range, {a: 3.0} outside it — one double value per document, so the
        // point-field group is provably single-valued and fusion is sound.
        Document inRangeDoc = new Document();
        inRangeDoc.add(new LongPoint(doubleField, LuceneDoubleConversionUtils.toLong(2.2)));
        writer.addDocument(inRangeDoc);
        Document outOfRangeDoc = new Document();
        outOfRangeDoc.add(new LongPoint(doubleField, LuceneDoubleConversionUtils.toLong(3.0)));
        writer.addDocument(outOfRangeDoc);
        writer.commit();
      }
      try (DirectoryReader reader = DirectoryReader.open(directory)) {
        SingleQueryContext queryContext = SingleQueryContext.createQueryRoot(reader);

        Query actual = factory.createLuceneFilter(filter, queryContext);

        Query fusedRange =
            new RangeQueryFactory(context, new EqualsQueryFactory(context))
                .fromBounds(bounds, List.of(filterPath), queryContext);
        Query expected =
            new BooleanQuery.Builder().add(fusedRange, BooleanClause.Occur.MUST).build();
        assertEquals("fusion must emit the single-range translation", expected, actual);
        assertEquals(
            "only {a: 2.2} lies inside (2.0, 2.4)", 1, new IndexSearcher(reader).count(actual));
      }
    }
  }

  /**
   * Closed-range fusion applies to dates exactly as to numerics: when every date value lives in the
   * single date point field and no document carries more than one, translation fuses to the
   * single-range form.
   */
  @Test
  public void closedRangeOnSingleValuedDateData_fusesToSingleRangeQuery() throws Exception {
    FieldPath filterPath = FieldPath.parse("a");
    VectorQueryFactoryContext context = contextWithFilterPath(filterPath);
    VectorSearchFilterQueryFactory factory =
        VectorSearchFilterQueryFactory.create(context, DynamicFeatureFlagRegistry.empty());
    DateRangeBound bounds =
        new DateRangeBound(
            Optional.of(new DatePoint(new Date(1_000L))),
            Optional.of(new DatePoint(new Date(2_000L))),
            false,
            false);
    VectorSearchFilter filter =
        new ClauseFilter(
            new SimpleClause(filterPath, List.of(new ClosedRangeBoundComparisonOperator(bounds))));
    String dateField =
        FieldName.TypeField.DATE_MULTIPLE.getLuceneFieldName(filterPath, Optional.empty());

    try (var directory = new ByteBuffersDirectory()) {
      try (IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig())) {
        // {a: Date(1500)} inside the range, {a: Date(3000)} outside it — one date value per
        // document, written as ingestion writes it: an epoch-millis long point.
        Document inRangeDoc = new Document();
        inRangeDoc.add(new LongPoint(dateField, 1_500L));
        writer.addDocument(inRangeDoc);
        Document outOfRangeDoc = new Document();
        outOfRangeDoc.add(new LongPoint(dateField, 3_000L));
        writer.addDocument(outOfRangeDoc);
        writer.commit();
      }
      try (DirectoryReader reader = DirectoryReader.open(directory)) {
        SingleQueryContext queryContext = SingleQueryContext.createQueryRoot(reader);

        Query actual = factory.createLuceneFilter(filter, queryContext);

        Query fusedRange =
            new RangeQueryFactory(context, new EqualsQueryFactory(context))
                .fromBounds(bounds, List.of(filterPath), queryContext);
        Query expected =
            new BooleanQuery.Builder().add(fusedRange, BooleanClause.Occur.MUST).build();
        assertEquals("fusion must emit the single-range translation", expected, actual);
        assertEquals(
            "only {a: Date(1500)} lies inside (Date(1000), Date(2000))",
            1,
            new IndexSearcher(reader).count(actual));
      }
    }
  }

  /**
   * Two single-valued documents whose numeric values live in different point-field variants — one
   * integral, one floating-point. Summary statistics alone cannot admit this shape, but the exact
   * per-segment docID-disjointness check proves no document spans both variants, so translation
   * must still fuse.
   */
  @Test
  public void closedRangeOnMixedIntDoubleScalarDocuments_fusesToSingleRangeQuery()
      throws Exception {
    FieldPath filterPath = FieldPath.parse("a");
    VectorQueryFactoryContext context = contextWithFilterPath(filterPath);
    VectorSearchFilterQueryFactory factory =
        VectorSearchFilterQueryFactory.create(context, DynamicFeatureFlagRegistry.empty());
    NumericRangeBound bounds =
        new NumericRangeBound(
            Optional.of(new DoublePoint(2.0)), Optional.of(new DoublePoint(2.4)), false, false);
    VectorSearchFilter filter =
        new ClauseFilter(
            new SimpleClause(filterPath, List.of(new ClosedRangeBoundComparisonOperator(bounds))));
    String int64Field =
        FieldName.TypeField.NUMBER_INT64_MULTIPLE.getLuceneFieldName(filterPath, Optional.empty());
    String doubleField =
        FieldName.TypeField.NUMBER_DOUBLE_MULTIPLE.getLuceneFieldName(filterPath, Optional.empty());

    try (var directory = new ByteBuffersDirectory()) {
      try (IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig())) {
        // {a: 1} and {a: 2.2} — scalar values routed to different variants by BSON type.
        Document intDoc = new Document();
        intDoc.add(new LongPoint(int64Field, 1L));
        writer.addDocument(intDoc);
        Document doubleDoc = new Document();
        doubleDoc.add(new LongPoint(doubleField, LuceneDoubleConversionUtils.toLong(2.2)));
        writer.addDocument(doubleDoc);
        writer.commit();
      }
      try (DirectoryReader reader = DirectoryReader.open(directory)) {
        SingleQueryContext queryContext = SingleQueryContext.createQueryRoot(reader);

        Query actual = factory.createLuceneFilter(filter, queryContext);

        Query fusedRange =
            new RangeQueryFactory(context, new EqualsQueryFactory(context))
                .fromBounds(bounds, List.of(filterPath), queryContext);
        Query expected =
            new BooleanQuery.Builder().add(fusedRange, BooleanClause.Occur.MUST).build();
        assertEquals("fusion must emit the single-range translation", expected, actual);
        assertEquals(
            "only {a: 2.2} lies inside (2.0, 2.4)", 1, new IndexSearcher(reader).count(actual));
      }
    }
  }

  /**
   * A date-array document may satisfy the two original open bounds with different elements. The
   * date point field is then multi-valued, so the fusion gate refuses and translation falls back to
   * the AND-of-two-opens shape, which must still match such documents.
   */
  @Test
  public void closedRangeOnDateArrayDocument_matchesLikeTwoOpenBounds() throws Exception {
    FieldPath filterPath = FieldPath.parse("a");
    VectorQueryFactoryContext context = contextWithFilterPath(filterPath);
    VectorSearchFilterQueryFactory factory =
        VectorSearchFilterQueryFactory.create(context, DynamicFeatureFlagRegistry.empty());
    VectorSearchFilter filter =
        new ClauseFilter(
            new SimpleClause(
                filterPath,
                List.of(
                    new ClosedRangeBoundComparisonOperator(
                        new DateRangeBound(
                            Optional.of(new DatePoint(new Date(1_000L))),
                            Optional.of(new DatePoint(new Date(2_000L))),
                            false,
                            false)))));
    String dateField =
        FieldName.TypeField.DATE_MULTIPLE.getLuceneFieldName(filterPath, Optional.empty());

    try (var directory = new ByteBuffersDirectory()) {
      try (IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig())) {
        // {a: [Date(500), Date(2500)]} — Date(2500) satisfies the lower bound and Date(500)
        // satisfies the upper bound; no single element satisfies both.
        Document dateArrayDoc = new Document();
        dateArrayDoc.add(new LongPoint(dateField, 500L));
        dateArrayDoc.add(new LongPoint(dateField, 2_500L));
        writer.addDocument(dateArrayDoc);

        // {a: Date(1500)} — control document inside the range, matched by fused and unfused
        // forms alike; proves the translated query is not trivially empty.
        Document inRangeDoc = new Document();
        inRangeDoc.add(new LongPoint(dateField, 1_500L));
        writer.addDocument(inRangeDoc);

        writer.commit();
      }
      try (DirectoryReader reader = DirectoryReader.open(directory)) {
        Query query =
            factory.createLuceneFilter(filter, SingleQueryContext.createQueryRoot(reader));
        assertEquals(
            "both documents must match: {a: [Date(500), Date(2500)]} satisfies $gt via Date(2500) "
                + "and $lt via Date(500), and {a: Date(1500)} lies inside the range",
            2,
            new IndexSearcher(reader).count(query));
      }
    }
  }

  /**
   * An I/O failure while reading the point statistics that gate fusion must not fail the query:
   * {@code canEmitFusedClosedRange} answers false and translation falls back to the
   * AND-of-two-opens shape. Translation runs against a reader whose {@code getPointValues} always
   * throws; the produced query is then executed against the healthy reader to prove it still
   * matches correctly.
   */
  @Test
  public void closedRangeWhenPointValuesReadFails_fallsBackToAndOfTwoOpens() throws Exception {
    FieldPath filterPath = FieldPath.parse("a");
    VectorQueryFactoryContext context = contextWithFilterPath(filterPath);
    VectorSearchFilterQueryFactory factory =
        VectorSearchFilterQueryFactory.create(context, DynamicFeatureFlagRegistry.empty());
    VectorSearchFilter filter =
        new ClauseFilter(
            new SimpleClause(
                filterPath,
                List.of(
                    new ClosedRangeBoundComparisonOperator(
                        new NumericRangeBound(
                            Optional.of(new DoublePoint(2.0)),
                            Optional.of(new DoublePoint(2.4)),
                            false,
                            false)))));
    String doubleField =
        FieldName.TypeField.NUMBER_DOUBLE_MULTIPLE.getLuceneFieldName(filterPath, Optional.empty());

    try (var directory = new ByteBuffersDirectory()) {
      try (IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig())) {
        Document inRangeDoc = new Document();
        inRangeDoc.add(new LongPoint(doubleField, LuceneDoubleConversionUtils.toLong(2.2)));
        writer.addDocument(inRangeDoc);
        Document outOfRangeDoc = new Document();
        outOfRangeDoc.add(new LongPoint(doubleField, LuceneDoubleConversionUtils.toLong(3.0)));
        writer.addDocument(outOfRangeDoc);
        writer.commit();
      }
      try (DirectoryReader reader = DirectoryReader.open(directory)) {
        // Not closed separately: closing the delegate via try-with-resources releases it.
        DirectoryReader throwingReader = pointValuesThrowingReader(reader);

        Query actual =
            factory.createLuceneFilter(filter, SingleQueryContext.createQueryRoot(throwingReader));

        SingleQueryContext healthyContext = SingleQueryContext.createQueryRoot(reader);
        RangeQueryFactory rangeQueryFactory =
            new RangeQueryFactory(context, new EqualsQueryFactory(context));
        Query lowerQuery =
            rangeQueryFactory.fromBounds(
                new NumericRangeBound(
                    Optional.of(new DoublePoint(2.0)), Optional.empty(), false, false),
                List.of(filterPath),
                healthyContext);
        Query upperQuery =
            rangeQueryFactory.fromBounds(
                new NumericRangeBound(
                    Optional.empty(), Optional.of(new DoublePoint(2.4)), false, false),
                List.of(filterPath),
                healthyContext);
        Query expected =
            new BooleanQuery.Builder()
                .add(
                    new BooleanQuery.Builder()
                        .add(lowerQuery, BooleanClause.Occur.MUST)
                        .add(upperQuery, BooleanClause.Occur.MUST)
                        .build(),
                    BooleanClause.Occur.MUST)
                .build();
        assertEquals("I/O failure must produce the AND-of-two-opens fallback", expected, actual);
        assertEquals(
            "only {a: 2.2} lies inside (2.0, 2.4)", 1, new IndexSearcher(reader).count(actual));
      }
    }
  }

  private static VectorQueryFactoryContext contextWithFilterPath(FieldPath filterPath) {
    VectorIndexDefinition indexDefinition =
        VectorIndexDefinitionBuilder.builder()
            .withEuclideanVectorField("embedding", 3)
            .withFilterPath(filterPath.toString())
            .indexFeatureVersion(VectorIndexCapabilities.CURRENT_FEATURE_VERSION)
            .build();
    return new VectorQueryFactoryContext(
        new VectorFieldDefinitionResolver(indexDefinition, IndexFormatVersion.CURRENT),
        FeatureFlags.getDefault(),
        metrics);
  }

  /**
   * Wraps {@code reader} so every leaf's {@code getPointValues} throws {@link IOException},
   * simulating an I/O failure while the fusion precondition reads segment point statistics.
   */
  private static DirectoryReader pointValuesThrowingReader(DirectoryReader reader)
      throws IOException {
    return new FilterDirectoryReader(
        reader,
        new FilterDirectoryReader.SubReaderWrapper() {
          @Override
          public LeafReader wrap(LeafReader leaf) {
            return new FilterLeafReader(leaf) {
              @Override
              public PointValues getPointValues(String field) throws IOException {
                throw new IOException("simulated PointValues read failure");
              }

              @Override
              public CacheHelper getCoreCacheHelper() {
                return null;
              }

              @Override
              public CacheHelper getReaderCacheHelper() {
                return null;
              }
            };
          }
        }) {
      @Override
      protected DirectoryReader doWrapDirectoryReader(DirectoryReader in) throws IOException {
        return pointValuesThrowingReader(in);
      }

      @Override
      public CacheHelper getReaderCacheHelper() {
        return null;
      }
    };
  }
}
