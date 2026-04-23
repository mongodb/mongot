package com.xgen.mongot.index.lucene.query;

import static com.xgen.mongot.util.bson.FloatVector.OriginalType.NATIVE;

import com.xgen.mongot.featureflag.FeatureFlags;
import com.xgen.mongot.index.IndexMetricsUpdater;
import com.xgen.mongot.index.definition.VectorIndexDefinition;
import com.xgen.mongot.index.lucene.query.context.VectorQueryFactoryContext;
import com.xgen.mongot.index.lucene.query.custom.MongotKnnFloatQuery;
import com.xgen.mongot.index.lucene.query.util.WrappedToParentBlockJoinQuery;
import com.xgen.mongot.index.query.InvalidQueryException;
import com.xgen.mongot.index.query.MaterializedVectorSearchQuery;
import com.xgen.mongot.index.query.VectorSearchQuery;
import com.xgen.mongot.index.query.operators.VectorEmbeddedOptions;
import com.xgen.mongot.index.query.operators.VectorSearchFilter;
import com.xgen.mongot.index.version.IndexFormatVersion;
import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.bson.Vector;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.testing.TestUtils;
import com.xgen.testing.mongot.index.definition.VectorIndexDefinitionBuilder;
import com.xgen.testing.mongot.index.query.ApproximateVectorQueryCriteriaBuilder;
import com.xgen.testing.mongot.index.query.VectorQueryBuilder;
import com.xgen.testing.mongot.index.query.operators.mql.ClauseBuilder;
import com.xgen.testing.mongot.index.query.operators.mql.MqlFilterOperatorBuilder;
import com.xgen.testing.mongot.index.query.operators.mql.ValueBuilder;
import com.xgen.testing.mongot.mock.index.SearchIndex;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.IndexOrDocValuesQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.join.QueryBitSetProducer;
import org.apache.lucene.search.join.ScoreMode;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.util.BytesRef;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Tests for nested vector search query construction with filter and parentFilter. Verifies that
 * {@link VectorSearchQueryFactory} correctly produces Lucene queries for vector indexes with {@code
 * nestedRoot}, including proper application of child-level filters and parent-level filters.
 */
public class VectorSearchNestedFilterTest {

  private static final IndexMetricsUpdater.QueryingMetricsUpdater METRICS =
      new IndexMetricsUpdater.QueryingMetricsUpdater(SearchIndex.mockMetricsFactory());

  private static final float[] QUERY_VECTOR = new float[] {1f, 2f, 3f};
  private static final int NUM_CANDIDATES = 100;
  private static final int LIMIT = 2;
  private static final int NUM_DIMENSIONS = 3;
  private static final FeatureFlags FLAGS =
      FeatureFlags.withDefaults()
          .enable(com.xgen.mongot.featureflag.Feature.NESTED_VECTOR)
          .build();

  private static final String EMBEDDED_VECTOR_FIELD =
      "$embedded:8/sections/$type:knnVector/sections.section_vector";
  private static final String EMBEDDED_TOKEN_FIELD =
      "$embedded:8/sections/$type:token/sections.section_name";
  private static final String ROOT_TOKEN_FIELD = "$type:token/name";
  private static final String DOC_NAME_STORED_FIELD = "doc_name";

  private Directory directory;
  private IndexWriter writer;

  @Before
  public void setUp() throws IOException {
    TemporaryFolder temporaryFolder = TestUtils.getTempFolder();
    this.directory = new MMapDirectory(temporaryFolder.getRoot().toPath());
    // A default IndexWriterConfig (without LuceneCodec) is intentional here.
    // These tests verify query-construction and filter behavior
    // (child-level vs parent-level filters under nestedRoot), which depend on
    // StringField/SortedSetDocValues/KnnFloatVectorField semantics — none of
    // which are affected by the custom LuceneCodec. The codec only controls
    // HNSW graph parameters and vector quantization format, which are
    // orthogonal to the filter logic under test.
    this.writer = new IndexWriter(this.directory, new IndexWriterConfig());
    this.writer.commit();
  }

  @After
  public void tearDown() throws IOException {
    this.writer.close();
    this.directory.close();
  }

  @Test
  public void testNestedVectorQueryWithParentFilter() throws Exception {
    VectorIndexDefinition definition = createNestedDefinition();

    VectorSearchQuery query =
        VectorQueryBuilder.builder()
            .index("test")
            .criteria(
                ApproximateVectorQueryCriteriaBuilder.builder()
                    .path(FieldPath.parse("sections.section_vector"))
                    .numCandidates(NUM_CANDIDATES)
                    .limit(LIMIT)
                    .queryVector(Vector.fromFloats(QUERY_VECTOR, NATIVE))
                    .parentFilter(eqClauseFilter("name", "value1"))
                    .embeddedOptions(new VectorEmbeddedOptions(VectorEmbeddedOptions.ScoreMode.MAX))
                    .build())
            .build();

    Query result = translate(definition, query);

    MongotKnnFloatQuery expectedKnn =
        new MongotKnnFloatQuery(METRICS, EMBEDDED_VECTOR_FIELD, QUERY_VECTOR, NUM_CANDIDATES);

    Query expectedBlockJoin =
        new WrappedToParentBlockJoinQuery(
            expectedKnn,
            new QueryBitSetProducer(EmbeddedDocumentQueryFactory.ROOT_DOCUMENTS_QUERY),
            ScoreMode.Max);

    Query expected =
        new BooleanQuery.Builder()
            .add(expectedBlockJoin, BooleanClause.Occur.MUST)
            .add(
                scopedParentFilter(eqQuery(ROOT_TOKEN_FIELD, "value1")), BooleanClause.Occur.FILTER)
            .build();

    Assert.assertEquals("Nested vector query with parentFilter:", expected, result);
  }

  @Test
  public void testNestedVectorQueryWithFilterAndParentFilter() throws Exception {
    VectorIndexDefinition definition = createNestedDefinition();

    VectorSearchQuery query =
        VectorQueryBuilder.builder()
            .index("test")
            .criteria(
                ApproximateVectorQueryCriteriaBuilder.builder()
                    .path(FieldPath.parse("sections.section_vector"))
                    .numCandidates(NUM_CANDIDATES)
                    .limit(LIMIT)
                    .queryVector(Vector.fromFloats(QUERY_VECTOR, NATIVE))
                    .filter(eqClauseFilter("sections.section_name", "value3"))
                    .parentFilter(eqClauseFilter("name", "value1"))
                    .embeddedOptions(new VectorEmbeddedOptions(VectorEmbeddedOptions.ScoreMode.MAX))
                    .build())
            .build();

    Query result = translate(definition, query);

    Query childFilter = wrapInMustClause(eqQuery(EMBEDDED_TOKEN_FIELD, "value3"));

    MongotKnnFloatQuery expectedKnn =
        new MongotKnnFloatQuery(
            METRICS, FLAGS, EMBEDDED_VECTOR_FIELD, QUERY_VECTOR, NUM_CANDIDATES, childFilter);

    Query expectedBlockJoin =
        new WrappedToParentBlockJoinQuery(
            expectedKnn,
            new QueryBitSetProducer(EmbeddedDocumentQueryFactory.ROOT_DOCUMENTS_QUERY),
            ScoreMode.Max);

    Query expected =
        new BooleanQuery.Builder()
            .add(expectedBlockJoin, BooleanClause.Occur.MUST)
            .add(
                scopedParentFilter(eqQuery(ROOT_TOKEN_FIELD, "value1")), BooleanClause.Occur.FILTER)
            .build();

    Assert.assertEquals("Nested vector query with filter and parentFilter:", expected, result);
  }

  @Test
  public void testNestedVectorQueryWithoutParentFilter() throws Exception {
    VectorIndexDefinition definition = createNestedDefinition();

    VectorSearchQuery query =
        VectorQueryBuilder.builder()
            .index("test")
            .criteria(
                ApproximateVectorQueryCriteriaBuilder.builder()
                    .path(FieldPath.parse("sections.section_vector"))
                    .numCandidates(NUM_CANDIDATES)
                    .limit(LIMIT)
                    .queryVector(Vector.fromFloats(QUERY_VECTOR, NATIVE))
                    .embeddedOptions(new VectorEmbeddedOptions(VectorEmbeddedOptions.ScoreMode.MAX))
                    .build())
            .build();

    Query result = translate(definition, query);

    MongotKnnFloatQuery expectedKnn =
        new MongotKnnFloatQuery(METRICS, EMBEDDED_VECTOR_FIELD, QUERY_VECTOR, NUM_CANDIDATES);

    Query expected =
        new WrappedToParentBlockJoinQuery(
            expectedKnn,
            new QueryBitSetProducer(EmbeddedDocumentQueryFactory.ROOT_DOCUMENTS_QUERY),
            ScoreMode.Max);

    Assert.assertEquals(
        "Without parentFilter, query should be a bare blockJoin:", expected, result);
  }

  @Test
  public void testNestedVectorQueryWithFilterOnly() throws Exception {
    VectorIndexDefinition definition = createNestedDefinition();

    VectorSearchQuery query =
        VectorQueryBuilder.builder()
            .index("test")
            .criteria(
                ApproximateVectorQueryCriteriaBuilder.builder()
                    .path(FieldPath.parse("sections.section_vector"))
                    .numCandidates(NUM_CANDIDATES)
                    .limit(LIMIT)
                    .queryVector(Vector.fromFloats(QUERY_VECTOR, NATIVE))
                    .filter(eqClauseFilter("sections.section_name", "value3"))
                    .embeddedOptions(new VectorEmbeddedOptions(VectorEmbeddedOptions.ScoreMode.MAX))
                    .build())
            .build();

    Query result = translate(definition, query);

    Query childFilter = wrapInMustClause(eqQuery(EMBEDDED_TOKEN_FIELD, "value3"));

    MongotKnnFloatQuery expectedKnn =
        new MongotKnnFloatQuery(
            METRICS, FLAGS, EMBEDDED_VECTOR_FIELD, QUERY_VECTOR, NUM_CANDIDATES, childFilter);

    Query expected =
        new WrappedToParentBlockJoinQuery(
            expectedKnn,
            new QueryBitSetProducer(EmbeddedDocumentQueryFactory.ROOT_DOCUMENTS_QUERY),
            ScoreMode.Max);

    Assert.assertEquals(
        "With filter only, query should be blockJoin (no outer FILTER):", expected, result);
  }

  @Test
  public void testNestedVectorQueryWithoutNestedOptionsDefaultsToMax() throws Exception {
    VectorIndexDefinition definition = createNestedDefinition();

    // Query without embeddedOptions (nestedOptions) — should default to ScoreMode.Max
    VectorSearchQuery query =
        VectorQueryBuilder.builder()
            .index("test")
            .criteria(
                ApproximateVectorQueryCriteriaBuilder.builder()
                    .path(FieldPath.parse("sections.section_vector"))
                    .numCandidates(NUM_CANDIDATES)
                    .limit(LIMIT)
                    .queryVector(Vector.fromFloats(QUERY_VECTOR, NATIVE))
                    .build())
            .build();

    Query result = translate(definition, query);

    MongotKnnFloatQuery expectedKnn =
        new MongotKnnFloatQuery(METRICS, EMBEDDED_VECTOR_FIELD, QUERY_VECTOR, NUM_CANDIDATES);

    Query expected =
        new WrappedToParentBlockJoinQuery(
            expectedKnn,
            new QueryBitSetProducer(EmbeddedDocumentQueryFactory.ROOT_DOCUMENTS_QUERY),
            ScoreMode.Max);

    Assert.assertEquals(
        "Without nestedOptions, query should default to ScoreMode.Max:", expected, result);
  }

  @Test
  public void testNestedVectorQueryWithoutNestedOptionsAndWithParentFilter() throws Exception {
    VectorIndexDefinition definition = createNestedDefinition();

    // Query without embeddedOptions but with parentFilter
    VectorSearchQuery query =
        VectorQueryBuilder.builder()
            .index("test")
            .criteria(
                ApproximateVectorQueryCriteriaBuilder.builder()
                    .path(FieldPath.parse("sections.section_vector"))
                    .numCandidates(NUM_CANDIDATES)
                    .limit(LIMIT)
                    .queryVector(Vector.fromFloats(QUERY_VECTOR, NATIVE))
                    .parentFilter(eqClauseFilter("name", "value1"))
                    .build())
            .build();

    Query result = translate(definition, query);

    MongotKnnFloatQuery expectedKnn =
        new MongotKnnFloatQuery(METRICS, EMBEDDED_VECTOR_FIELD, QUERY_VECTOR, NUM_CANDIDATES);

    Query expectedBlockJoin =
        new WrappedToParentBlockJoinQuery(
            expectedKnn,
            new QueryBitSetProducer(EmbeddedDocumentQueryFactory.ROOT_DOCUMENTS_QUERY),
            ScoreMode.Max);

    Query expected =
        new BooleanQuery.Builder()
            .add(expectedBlockJoin, BooleanClause.Occur.MUST)
            .add(
                scopedParentFilter(eqQuery(ROOT_TOKEN_FIELD, "value1")), BooleanClause.Occur.FILTER)
            .build();

    Assert.assertEquals(
        "Without nestedOptions + parentFilter, should default to ScoreMode.Max:", expected, result);
  }

  @Test
  public void testNestedVectorQueryWithoutNestedOptionsAndWithFilter() throws Exception {
    VectorIndexDefinition definition = createNestedDefinition();

    // Query without embeddedOptions but with child filter
    VectorSearchQuery query =
        VectorQueryBuilder.builder()
            .index("test")
            .criteria(
                ApproximateVectorQueryCriteriaBuilder.builder()
                    .path(FieldPath.parse("sections.section_vector"))
                    .numCandidates(NUM_CANDIDATES)
                    .limit(LIMIT)
                    .queryVector(Vector.fromFloats(QUERY_VECTOR, NATIVE))
                    .filter(eqClauseFilter("sections.section_name", "value3"))
                    .build())
            .build();

    Query result = translate(definition, query);

    Query childFilter = wrapInMustClause(eqQuery(EMBEDDED_TOKEN_FIELD, "value3"));

    MongotKnnFloatQuery expectedKnn =
        new MongotKnnFloatQuery(
            METRICS, FLAGS, EMBEDDED_VECTOR_FIELD, QUERY_VECTOR, NUM_CANDIDATES, childFilter);

    Query expected =
        new WrappedToParentBlockJoinQuery(
            expectedKnn,
            new QueryBitSetProducer(EmbeddedDocumentQueryFactory.ROOT_DOCUMENTS_QUERY),
            ScoreMode.Max);

    Assert.assertEquals(
        "Without nestedOptions + filter, should default to ScoreMode.Max:", expected, result);
  }

  @Test
  public void testNestedVectorQueryWithoutNestedOptionsAndWithFilterAndParentFilter()
      throws Exception {
    VectorIndexDefinition definition = createNestedDefinition();

    // Query without embeddedOptions but with both filter and parentFilter
    VectorSearchQuery query =
        VectorQueryBuilder.builder()
            .index("test")
            .criteria(
                ApproximateVectorQueryCriteriaBuilder.builder()
                    .path(FieldPath.parse("sections.section_vector"))
                    .numCandidates(NUM_CANDIDATES)
                    .limit(LIMIT)
                    .queryVector(Vector.fromFloats(QUERY_VECTOR, NATIVE))
                    .filter(eqClauseFilter("sections.section_name", "value3"))
                    .parentFilter(eqClauseFilter("name", "value1"))
                    .build())
            .build();

    Query result = translate(definition, query);

    Query childFilter = wrapInMustClause(eqQuery(EMBEDDED_TOKEN_FIELD, "value3"));

    MongotKnnFloatQuery expectedKnn =
        new MongotKnnFloatQuery(
            METRICS, FLAGS, EMBEDDED_VECTOR_FIELD, QUERY_VECTOR, NUM_CANDIDATES, childFilter);

    Query expectedBlockJoin =
        new WrappedToParentBlockJoinQuery(
            expectedKnn,
            new QueryBitSetProducer(EmbeddedDocumentQueryFactory.ROOT_DOCUMENTS_QUERY),
            ScoreMode.Max);

    Query expected =
        new BooleanQuery.Builder()
            .add(expectedBlockJoin, BooleanClause.Occur.MUST)
            .add(
                scopedParentFilter(eqQuery(ROOT_TOKEN_FIELD, "value1")), BooleanClause.Occur.FILTER)
            .build();

    Assert.assertEquals(
        "Without nestedOptions + filter + parentFilter, should default to ScoreMode.Max:",
        expected,
        result);
  }

  @Test
  public void testIsIndexWithEmbeddedFieldsForNestedVectorIndex() {
    VectorIndexDefinition definition = createNestedDefinition();
    var context =
        new VectorQueryFactoryContext(
            definition, IndexFormatVersion.CURRENT, FLAGS, METRICS);

    Assert.assertTrue(
        "isIndexWithEmbeddedFields() should return true for vector indexes with nestedRoot",
        context.isIndexWithEmbeddedFields());
  }

  @Test
  public void testIsIndexWithEmbeddedFieldsForNonNestedVectorIndex() {
    VectorIndexDefinition definition =
        VectorIndexDefinitionBuilder.builder()
            .withEuclideanVectorField("foo.bar", NUM_DIMENSIONS)
            .withFilterPath("foo.filter")
            .build();
    var context =
        new VectorQueryFactoryContext(
            definition, IndexFormatVersion.CURRENT, FLAGS, METRICS);

    Assert.assertFalse(
        "isIndexWithEmbeddedFields() should return false for vector indexes without nestedRoot",
        context.isIndexWithEmbeddedFields());
  }

  @Test
  public void testNestedOptionsOnNonEmbeddedFieldThrowsDescriptiveError() throws Exception {
    VectorIndexDefinition definition =
        VectorIndexDefinitionBuilder.builder()
            .withEuclideanVectorField("chapters.vec", NUM_DIMENSIONS)
            .build();

    VectorSearchQuery query =
        VectorQueryBuilder.builder()
            .index("test")
            .criteria(
                ApproximateVectorQueryCriteriaBuilder.builder()
                    .path(FieldPath.parse("chapters.vec"))
                    .numCandidates(NUM_CANDIDATES)
                    .limit(LIMIT)
                    .queryVector(Vector.fromFloats(QUERY_VECTOR, NATIVE))
                    .embeddedOptions(new VectorEmbeddedOptions(VectorEmbeddedOptions.ScoreMode.MAX))
                    .build())
            .build();

    InvalidQueryException e =
        Assert.assertThrows(InvalidQueryException.class, () -> translate(definition, query));
    Assert.assertEquals(
        "\"nestedOptions\" requires a vector path within the index's nested root, but"
            + " 'chapters.vec' is outside it. Specify a path under the index's \"nestedRoot\""
            + " field, or remove \"nestedOptions\" to query 'chapters.vec' as a standard vector"
            + " field.",
        e.getMessage());
  }

  @Test
  public void testParentFilterUsesRootFieldNaming() throws Exception {
    VectorIndexDefinition definition = createNestedDefinition();

    VectorSearchQuery query =
        VectorQueryBuilder.builder()
            .index("test")
            .criteria(
                ApproximateVectorQueryCriteriaBuilder.builder()
                    .path(FieldPath.parse("sections.section_vector"))
                    .numCandidates(NUM_CANDIDATES)
                    .limit(LIMIT)
                    .queryVector(Vector.fromFloats(QUERY_VECTOR, NATIVE))
                    .parentFilter(eqClauseFilter("name", "value1"))
                    .embeddedOptions(new VectorEmbeddedOptions(VectorEmbeddedOptions.ScoreMode.MAX))
                    .build())
            .build();

    Query result = translate(definition, query);

    Assert.assertTrue("Result should be BooleanQuery", result instanceof BooleanQuery);
    BooleanQuery boolQuery = (BooleanQuery) result;

    BooleanClause filterClause =
        boolQuery.clauses().stream()
            .filter(c -> c.occur() == BooleanClause.Occur.FILTER)
            .findFirst()
            .orElseThrow(() -> new AssertionError("No FILTER clause"));

    // Structurally inspect the query tree for TermQuery field names instead of relying on
    // Query#toString(), which can change across Lucene versions.
    Set<String> termFields = collectTermFields(filterClause.query());

    Assert.assertTrue(
        "parentFilter should reference the root field ($type:token/name), found fields: "
            + termFields,
        termFields.contains(ROOT_TOKEN_FIELD));
    Assert.assertTrue(
        "parentFilter must NOT reference any embedded field ($embedded: prefix), found fields: "
            + termFields,
        termFields.stream().noneMatch(f -> f.startsWith("$embedded:")));
  }

  /**
   * Regression test for CLOUDP-398738. Verifies that a vector query with {@code nestedOptions} on
   * an auto-embedding + nestedRoot index does not throw "nestedOptions can only be specified for
   * embedded vector fields". Before the fix, the derived definition's {@code nestedRoot} was the
   * user-facing path ({@code sections}) while the materialized query path was the MV-namespaced
   * path ({@code _autoEmbed.sections.section_vector}), so embedded-vector detection failed. The
   * fix remaps {@code nestedRoot} into the MV namespace as well.
   */
  @Test
  public void testAutoEmbedNestedRootRemappedAcceptsNestedOptions() throws Exception {
    // Mirrors the shape of the derived definition produced by
    // AutoEmbeddingIndexDefinitionUtils.getDerivedVectorIndexDefinition after the fix: both the
    // vector field path and the nestedRoot are in the _autoEmbed.* namespace.
    VectorIndexDefinition derivedDefinition =
        VectorIndexDefinitionBuilder.builder()
            .nestedRoot("_autoEmbed.sections")
            .withEuclideanVectorField("_autoEmbed.sections.section_vector", NUM_DIMENSIONS)
            .withFilterPath("sections.section_name")
            .build();

    // The user-provided query uses the user-facing path; MaterializedVectorSearchQuery rewrites
    // it to the MV path via the autoEmbedding field mapping before invoking the query factory.
    VectorSearchQuery userQuery =
        VectorQueryBuilder.builder()
            .index("test")
            .criteria(
                ApproximateVectorQueryCriteriaBuilder.builder()
                    .path(FieldPath.parse("sections.section_vector"))
                    .numCandidates(NUM_CANDIDATES)
                    .limit(LIMIT)
                    .queryVector(Vector.fromFloats(QUERY_VECTOR, NATIVE))
                    .embeddedOptions(new VectorEmbeddedOptions(VectorEmbeddedOptions.ScoreMode.MAX))
                    .build())
            .build();

    MaterializedVectorSearchQuery materialized =
        new MaterializedVectorSearchQuery(
            userQuery,
            userQuery.criteria().queryVector().get(),
            java.util.Map.of(
                FieldPath.parse("sections.section_vector"),
                FieldPath.parse("_autoEmbed.sections.section_vector")));

    var context =
        new VectorQueryFactoryContext(
            derivedDefinition, IndexFormatVersion.CURRENT, FLAGS, METRICS);
    var factory = LuceneVectorQueryFactoryDistributor.create(context);

    try (var reader = DirectoryReader.open(this.directory)) {
      // Before the fix, this call threw InvalidQueryException: "nestedOptions can only be
      // specified for embedded vector fields, but '_autoEmbed.sections.section_vector' is not
      // embedded".
      Query result = factory.createQuery(materialized, reader);

      Assert.assertTrue(
          "Auto-embed + nestedRoot query with nestedOptions should produce an embedded "
              + "block-join query, got: "
              + result.getClass().getName(),
          result instanceof WrappedToParentBlockJoinQuery);
    }
  }

  /**
   * Companion test to {@link #testAutoEmbedNestedRootRemappedAcceptsNestedOptions} covering the
   * no-{@code nestedOptions} path. Before the fix, such queries were silently classified as
   * non-embedded and skipped the block-join wrap, returning bogus rankings against an actually
   * embedded index. After the fix, the remapped {@code nestedRoot} lets embedded-vector detection
   * succeed regardless of whether the user supplied {@code nestedOptions}, so the query is still
   * wrapped in a block-join. See CLOUDP-398738.
   */
  @Test
  public void testAutoEmbedNestedRootRemappedProducesEmbeddedQueryWithoutNestedOptions()
      throws Exception {
    VectorIndexDefinition derivedDefinition =
        VectorIndexDefinitionBuilder.builder()
            .nestedRoot("_autoEmbed.sections")
            .withEuclideanVectorField("_autoEmbed.sections.section_vector", NUM_DIMENSIONS)
            .withFilterPath("sections.section_name")
            .build();

    VectorSearchQuery userQuery =
        VectorQueryBuilder.builder()
            .index("test")
            .criteria(
                ApproximateVectorQueryCriteriaBuilder.builder()
                    .path(FieldPath.parse("sections.section_vector"))
                    .numCandidates(NUM_CANDIDATES)
                    .limit(LIMIT)
                    .queryVector(Vector.fromFloats(QUERY_VECTOR, NATIVE))
                    .build())
            .build();

    MaterializedVectorSearchQuery materialized =
        new MaterializedVectorSearchQuery(
            userQuery,
            userQuery.criteria().queryVector().get(),
            java.util.Map.of(
                FieldPath.parse("sections.section_vector"),
                FieldPath.parse("_autoEmbed.sections.section_vector")));

    var context =
        new VectorQueryFactoryContext(
            derivedDefinition, IndexFormatVersion.CURRENT, FLAGS, METRICS);
    var factory = LuceneVectorQueryFactoryDistributor.create(context);

    try (var reader = DirectoryReader.open(this.directory)) {
      Query result = factory.createQuery(materialized, reader);

      Assert.assertTrue(
          "Auto-embed + nestedRoot query without nestedOptions should still produce an embedded "
              + "block-join query, got: "
              + result.getClass().getName(),
          result instanceof WrappedToParentBlockJoinQuery);
    }
  }

  // ---- integration tests: execute queries against an indexed Lucene directory ----

  @Test
  public void testRuntimeFilterAndParentFilterBothApplied() throws Exception {
    indexTestBlocks();

    VectorSearchQuery query =
        buildNestedQuery(
            eqClauseFilter("sections.section_name", "value3"), eqClauseFilter("name", "value1"));

    VectorIndexDefinition definition = createNestedDefinition();
    var context =
        new VectorQueryFactoryContext(
            definition, IndexFormatVersion.CURRENT, FLAGS, METRICS);
    var factory = LuceneVectorQueryFactoryDistributor.create(context);

    try (var reader = DirectoryReader.open(this.directory)) {
      Query luceneQuery =
          factory.createQuery(
              new MaterializedVectorSearchQuery(query, query.criteria().queryVector().get()),
              reader);
      IndexSearcher searcher = new IndexSearcher(reader);
      TopDocs results = searcher.search(luceneQuery, 10);

      Assert.assertEquals(
          "With filter(section_name=value3) AND parentFilter(name=value1): "
              + "only block 2 matches both",
          1,
          results.scoreDocs.length);
      Set<String> names = getStoredDocNames(reader, results);
      Assert.assertTrue("Returned doc should have name='value1'", names.contains("value1"));
      Assert.assertFalse("Should NOT return doc with name='value7'", names.contains("value7"));
    }
  }

  @Test
  public void testRuntimeParentFilterOnly() throws Exception {
    indexTestBlocks();

    VectorSearchQuery query = buildNestedQuery(null, eqClauseFilter("name", "value1"));

    VectorIndexDefinition definition = createNestedDefinition();
    var context =
        new VectorQueryFactoryContext(
            definition, IndexFormatVersion.CURRENT, FLAGS, METRICS);
    var factory = LuceneVectorQueryFactoryDistributor.create(context);

    try (var reader = DirectoryReader.open(this.directory)) {
      Query luceneQuery =
          factory.createQuery(
              new MaterializedVectorSearchQuery(query, query.criteria().queryVector().get()),
              reader);
      IndexSearcher searcher = new IndexSearcher(reader);
      TopDocs results = searcher.search(luceneQuery, 10);

      Assert.assertEquals(
          "With parentFilter(name=value1) only: blocks 2 and 3 both match",
          2,
          results.scoreDocs.length);
      Set<String> names = getStoredDocNames(reader, results);
      Assert.assertEquals("All returned docs should have name='value1'", Set.of("value1"), names);
    }
  }

  @Test
  public void testRuntimeChildFilterOnly() throws Exception {
    indexTestBlocks();

    VectorSearchQuery query =
        buildNestedQuery(eqClauseFilter("sections.section_name", "value3"), null);

    VectorIndexDefinition definition = createNestedDefinition();
    var context =
        new VectorQueryFactoryContext(
            definition, IndexFormatVersion.CURRENT, FLAGS, METRICS);
    var factory = LuceneVectorQueryFactoryDistributor.create(context);

    try (var reader = DirectoryReader.open(this.directory)) {
      Query luceneQuery =
          factory.createQuery(
              new MaterializedVectorSearchQuery(query, query.criteria().queryVector().get()),
              reader);
      IndexSearcher searcher = new IndexSearcher(reader);
      TopDocs results = searcher.search(luceneQuery, 10);

      Assert.assertEquals(
          "With filter(section_name=value3) only: blocks 1 and 2 match",
          2,
          results.scoreDocs.length);
      Set<String> names = getStoredDocNames(reader, results);
      Assert.assertTrue("Should include doc with name='value7'", names.contains("value7"));
      Assert.assertTrue("Should include doc with name='value1'", names.contains("value1"));
    }
  }

  @Test
  public void testRuntimeNegationFilterUnderNestedRoot() throws Exception {
    indexTestBlocks();

    // Use $ne on the child filter: section_name != "value3" excludes children with "value3".
    // Combined with parentFilter name != "value7", only block 3 should match:
    //   Block 1: parent name="value7" → excluded by parentFilter(ne "value7")
    //   Block 2: parent name="value1" → passes parentFilter, but both children
    //            section_name={"value3","value6"}: child "value3" excluded, child "value6" passes
    //   Block 3: parent name="value1" → passes parentFilter, children
    //            section_name={"value9","value8"}: both pass the ne filter
    // So blocks 2 and 3 should match (both have parent name="value1" which != "value7",
    // and both have at least one child with section_name != "value3").
    VectorSearchQuery query =
        buildNestedQuery(
            neClauseFilter("sections.section_name", "value3"), neClauseFilter("name", "value7"));

    VectorIndexDefinition definition = createNestedDefinition();
    var context =
        new VectorQueryFactoryContext(
            definition, IndexFormatVersion.CURRENT, FLAGS, METRICS);
    var factory = LuceneVectorQueryFactoryDistributor.create(context);

    try (var reader = DirectoryReader.open(this.directory)) {
      Query luceneQuery =
          factory.createQuery(
              new MaterializedVectorSearchQuery(query, query.criteria().queryVector().get()),
              reader);
      IndexSearcher searcher = new IndexSearcher(reader);
      TopDocs results = searcher.search(luceneQuery, 10);

      Assert.assertEquals(
          "With ne filter(section_name!='value3') AND ne parentFilter(name!='value7'): "
              + "blocks 2 and 3 match",
          2,
          results.scoreDocs.length);
      Set<String> names = getStoredDocNames(reader, results);
      Assert.assertEquals("All returned docs should have name='value1'", Set.of("value1"), names);
      Assert.assertFalse("Should NOT return doc with name='value7'", names.contains("value7"));
    }
  }

  @Test
  public void testNestedVectorQueryRejectedWhenFeatureFlagDisabled() throws Exception {
    VectorIndexDefinition definition = createNestedDefinition();

    VectorSearchQuery query =
        VectorQueryBuilder.builder()
            .index("test")
            .criteria(
                ApproximateVectorQueryCriteriaBuilder.builder()
                    .path(FieldPath.parse("sections.section_vector"))
                    .numCandidates(NUM_CANDIDATES)
                    .limit(LIMIT)
                    .queryVector(Vector.fromFloats(QUERY_VECTOR, NATIVE))
                    .embeddedOptions(new VectorEmbeddedOptions(VectorEmbeddedOptions.ScoreMode.MAX))
                    .build())
            .build();

    FeatureFlags flagsWithNestedDisabled =
        FeatureFlags.withDefaults()
            .disable(com.xgen.mongot.featureflag.Feature.NESTED_VECTOR)
            .build();
    var context =
        new VectorQueryFactoryContext(
            definition, IndexFormatVersion.CURRENT, flagsWithNestedDisabled, METRICS);
    var factory = LuceneVectorQueryFactoryDistributor.create(context);

    try (var reader = DirectoryReader.open(this.directory)) {
      Assert.assertThrows(
          "Nested vector query should be rejected when NESTED_VECTOR flag is disabled",
          InvalidQueryException.class,
          () ->
              factory.createQuery(
                  new MaterializedVectorSearchQuery(query, query.criteria().queryVector().get()),
                  reader));
    }
  }

  @Test
  public void testNestedVectorQueryWithoutNestedOptionsRejectedWhenFeatureFlagDisabled()
      throws Exception {
    VectorIndexDefinition definition = createNestedDefinition();

    VectorSearchQuery query =
        VectorQueryBuilder.builder()
            .index("test")
            .criteria(
                ApproximateVectorQueryCriteriaBuilder.builder()
                    .path(FieldPath.parse("sections.section_vector"))
                    .numCandidates(NUM_CANDIDATES)
                    .limit(LIMIT)
                    .queryVector(Vector.fromFloats(QUERY_VECTOR, NATIVE))
                    .build())
            .build();

    FeatureFlags flagsWithNestedDisabled =
        FeatureFlags.withDefaults()
            .disable(com.xgen.mongot.featureflag.Feature.NESTED_VECTOR)
            .build();
    var context =
        new VectorQueryFactoryContext(
            definition, IndexFormatVersion.CURRENT, flagsWithNestedDisabled, METRICS);
    var factory = LuceneVectorQueryFactoryDistributor.create(context);

    try (var reader = DirectoryReader.open(this.directory)) {
      Assert.assertThrows(
          "Nested vector query should be rejected even without nestedOptions when flag is disabled",
          InvalidQueryException.class,
          () ->
              factory.createQuery(
                  new MaterializedVectorSearchQuery(query, query.criteria().queryVector().get()),
                  reader));
    }
  }

  // ---- helpers ----

  private static VectorIndexDefinition createNestedDefinition() {
    return VectorIndexDefinitionBuilder.builder()
        .nestedRoot("sections")
        .withEuclideanVectorField("sections.section_vector", NUM_DIMENSIONS)
        .withFilterPath("sections.section_name")
        .withFilterPath("name")
        .build();
  }

  private Query translate(VectorIndexDefinition definition, VectorSearchQuery query)
      throws IOException, InvalidQueryException {
    var context =
        new VectorQueryFactoryContext(
            definition, IndexFormatVersion.CURRENT, FLAGS, METRICS);
    var factory = LuceneVectorQueryFactoryDistributor.create(context);
    try (var reader = DirectoryReader.open(this.directory)) {
      return factory.createQuery(
          new MaterializedVectorSearchQuery(query, query.criteria().queryVector().get()), reader);
    }
  }

  private static VectorSearchFilter.ClauseFilter eqClauseFilter(String path, String value)
      throws BsonParseException {
    return new VectorSearchFilter.ClauseFilter(
        ClauseBuilder.simpleClause()
            .addOperator(MqlFilterOperatorBuilder.eq().value(ValueBuilder.string(value)))
            .path(FieldPath.parse(path))
            .build());
  }

  private static VectorSearchFilter.ClauseFilter neClauseFilter(String path, String value)
      throws BsonParseException {
    return new VectorSearchFilter.ClauseFilter(
        ClauseBuilder.simpleClause()
            .addOperator(MqlFilterOperatorBuilder.ne().value(ValueBuilder.string(value)))
            .path(FieldPath.parse(path))
            .build());
  }

  private static Query eqQuery(String luceneField, String value) {
    return new IndexOrDocValuesQuery(
        new ConstantScoreQuery(new TermQuery(new Term(luceneField, new BytesRef(value)))),
        SortedSetDocValuesField.newSlowExactQuery(luceneField, new BytesRef(value)));
  }

  /**
   * Recursively collects all field names referenced by {@link TermQuery} instances within a query
   * tree. This provides a Lucene-version-stable way to inspect which fields a query references,
   * avoiding brittle {@link Query#toString()} assertions.
   */
  private static Set<String> collectTermFields(Query query) {
    Set<String> fields = new HashSet<>();
    collectTermFieldsRecursive(query, fields);
    return fields;
  }

  private static void collectTermFieldsRecursive(Query query, Set<String> fields) {
    if (query instanceof TermQuery termQuery) {
      fields.add(termQuery.getTerm().field());
    } else if (query instanceof BooleanQuery boolQuery) {
      for (BooleanClause clause : boolQuery.clauses()) {
        collectTermFieldsRecursive(clause.query(), fields);
      }
    } else if (query instanceof ConstantScoreQuery csQuery) {
      collectTermFieldsRecursive(csQuery.getQuery(), fields);
    } else if (query instanceof IndexOrDocValuesQuery iodvQuery) {
      // Only inspect the index query side; the doc-values side mirrors the same field.
      collectTermFieldsRecursive(iodvQuery.getIndexQuery(), fields);
    }
  }

  /** Wraps a query in a BooleanQuery with a single MUST clause (matches SimpleClause structure). */
  private static Query wrapInMustClause(Query query) {
    return new BooleanQuery.Builder().add(query, BooleanClause.Occur.MUST).build();
  }

  /**
   * Builds the scoped parent filter: ROOT_DOCUMENTS_QUERY AND parentFilterQuery. The
   * parentFilterQuery is wrapped in a SimpleClause-style BooleanQuery (single MUST clause).
   */
  private static Query scopedParentFilter(Query parentFilterEqQuery) {
    Query simpleClauseWrapper = wrapInMustClause(parentFilterEqQuery);
    return new BooleanQuery.Builder()
        .add(EmbeddedDocumentQueryFactory.ROOT_DOCUMENTS_QUERY, BooleanClause.Occur.FILTER)
        .add(simpleClauseWrapper, BooleanClause.Occur.FILTER)
        .build();
  }

  // ---- helpers for integration tests ----

  /**
   * Indexes three document blocks that mirror the user's reported scenario:
   *
   * <ul>
   *   <li>Block 1: parent name="value7", children section_name={"value3","value5"}
   *   <li>Block 2: parent name="value1", children section_name={"value3","value6"}
   *   <li>Block 3: parent name="value1", children section_name={"value9","value8"}
   * </ul>
   *
   * <p>With filter=section_name:value3 AND parentFilter=name:value1, only block 2 should match.
   */
  private void indexTestBlocks() throws IOException {
    this.writer.addDocuments(
        createDocumentBlock(
            "value7",
            createChildDocument("value3", new float[] {1f, 2f, 3f}),
            createChildDocument("value5", new float[] {0f, 0f, 1f})));

    this.writer.addDocuments(
        createDocumentBlock(
            "value1",
            createChildDocument("value3", new float[] {1f, 2f, 2.9f}),
            createChildDocument("value6", new float[] {0f, 1f, 0f})));

    this.writer.addDocuments(
        createDocumentBlock(
            "value1",
            createChildDocument("value9", new float[] {0.9f, 1.9f, 2.9f}),
            createChildDocument("value8", new float[] {0f, 0f, 0.5f})));

    this.writer.commit();
  }

  private static List<Document> createDocumentBlock(String parentName, Document... children) {
    List<Document> block = new ArrayList<>();
    for (Document child : children) {
      block.add(child);
    }
    Document root = new Document();
    root.add(new StringField("$meta/embeddedRoot", "T", Field.Store.NO));
    root.add(new StringField(ROOT_TOKEN_FIELD, parentName, Field.Store.NO));
    root.add(new SortedSetDocValuesField(ROOT_TOKEN_FIELD, new BytesRef(parentName)));
    root.add(new StoredField(DOC_NAME_STORED_FIELD, parentName));
    block.add(root);
    return block;
  }

  private static Document createChildDocument(String sectionName, float[] vector) {
    Document doc = new Document();
    doc.add(new StringField("$meta/embeddedPath", "sections", Field.Store.NO));
    doc.add(
        new KnnFloatVectorField(EMBEDDED_VECTOR_FIELD, vector, VectorSimilarityFunction.EUCLIDEAN));
    doc.add(new StringField(EMBEDDED_TOKEN_FIELD, sectionName, Field.Store.NO));
    doc.add(new SortedSetDocValuesField(EMBEDDED_TOKEN_FIELD, new BytesRef(sectionName)));
    return doc;
  }

  private VectorSearchQuery buildNestedQuery(
      VectorSearchFilter.ClauseFilter childFilter, VectorSearchFilter.ClauseFilter parentFilter)
      throws BsonParseException {
    var criteriaBuilder =
        ApproximateVectorQueryCriteriaBuilder.builder()
            .path(FieldPath.parse("sections.section_vector"))
            .numCandidates(NUM_CANDIDATES)
            .limit(10)
            .queryVector(Vector.fromFloats(QUERY_VECTOR, NATIVE))
            .embeddedOptions(new VectorEmbeddedOptions(VectorEmbeddedOptions.ScoreMode.MAX));
    if (childFilter != null) {
      criteriaBuilder.filter(childFilter);
    }
    if (parentFilter != null) {
      criteriaBuilder.parentFilter(parentFilter);
    }
    return VectorQueryBuilder.builder().index("test").criteria(criteriaBuilder.build()).build();
  }

  private static Set<String> getStoredDocNames(DirectoryReader reader, TopDocs results)
      throws IOException {
    Set<String> names = new HashSet<>();
    var storedFields = reader.storedFields();
    for (ScoreDoc scoreDoc : results.scoreDocs) {
      Document doc = storedFields.document(scoreDoc.doc);
      String name = doc.get(DOC_NAME_STORED_FIELD);
      if (name != null) {
        names.add(name);
      }
    }
    return names;
  }
}
