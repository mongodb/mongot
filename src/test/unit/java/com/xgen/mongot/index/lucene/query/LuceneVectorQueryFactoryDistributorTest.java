package com.xgen.mongot.index.lucene.query;

import static com.xgen.mongot.index.lucene.query.util.BooleanComposer.shouldClause;
import static com.xgen.mongot.util.bson.FloatVector.OriginalType.NATIVE;

import com.xgen.mongot.featureflag.FeatureFlags;
import com.xgen.mongot.index.IndexMetricsUpdater;
import com.xgen.mongot.index.definition.VectorAutoEmbedFieldDefinition;
import com.xgen.mongot.index.definition.VectorIndexFilterFieldDefinition;
import com.xgen.mongot.index.definition.VectorSimilarity;
import com.xgen.mongot.index.definition.quantization.VectorAutoEmbedQuantization;
import com.xgen.mongot.index.definition.quantization.VectorQuantization;
import com.xgen.mongot.index.lucene.query.context.VectorQueryFactoryContext;
import com.xgen.mongot.index.lucene.query.custom.ExactVectorSearchQuery;
import com.xgen.mongot.index.lucene.query.custom.MongotKnnFloatQuery;
import com.xgen.mongot.index.lucene.query.custom.WrappedKnnQuery;
import com.xgen.mongot.index.query.InvalidQueryException;
import com.xgen.mongot.index.query.MaterializedVectorSearchQuery;
import com.xgen.mongot.index.query.operators.CompoundOperator;
import com.xgen.mongot.index.query.operators.VectorSearchFilter;
import com.xgen.mongot.index.version.IndexFormatVersion;
import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.bson.Vector;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.testing.TestUtils;
import com.xgen.testing.mongot.index.definition.VectorDataFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.VectorIndexDefinitionBuilder;
import com.xgen.testing.mongot.index.query.ApproximateVectorQueryCriteriaBuilder;
import com.xgen.testing.mongot.index.query.ExactVectorCriteriaBuilder;
import com.xgen.testing.mongot.index.query.VectorQueryBuilder;
import com.xgen.testing.mongot.index.query.operators.OperatorBuilder;
import com.xgen.testing.mongot.index.query.operators.mql.ClauseBuilder;
import com.xgen.testing.mongot.index.query.operators.mql.MqlFilterOperatorBuilder;
import com.xgen.testing.mongot.index.query.operators.mql.ValueBuilder;
import com.xgen.testing.mongot.mock.index.SearchIndex;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.FieldExistsQuery;
import org.apache.lucene.search.IndexOrDocValuesQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.util.BytesRef;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class LuceneVectorQueryFactoryDistributorTest {

  private static final IndexMetricsUpdater.QueryingMetricsUpdater metrics =
      new IndexMetricsUpdater.QueryingMetricsUpdater(SearchIndex.mockMetricsFactory());

  private static Directory directory;
  private static IndexWriter writer;

  @Before
  public void setUp() throws IOException {
    TemporaryFolder temporaryFolder = TestUtils.getTempFolder();
    directory = new MMapDirectory(temporaryFolder.getRoot().toPath());
    writer = new IndexWriter(directory, new IndexWriterConfig());
    writer.commit();
  }

  @After
  public void tearDown() throws IOException {
    writer.close();
    directory.close();
  }

  @Test
  public void testCompoundDefinition() throws Exception {
    CompoundOperator definition =
        OperatorBuilder.compound()
            .should(OperatorBuilder.term().path("title").query("godfather").build())
            .build();

    BooleanQuery.Builder builder = new BooleanQuery.Builder();
    builder.add(shouldClause(new TermQuery(new Term("$type:string/title", "godfather"))));
    Query expected = builder.build();
    LuceneSearchTranslation.get().assertTranslatedTo(definition, expected);
  }

  @Test
  public void testApproximateVectorQuery() throws IOException, InvalidQueryException {
    var path = FieldPath.parse("foo.bar");
    var mongotQuery =
        VectorQueryBuilder.builder()
            .index("test")
            .criteria(
                ApproximateVectorQueryCriteriaBuilder.builder()
                    .path(path)
                    .numCandidates(20)
                    .limit(10)
                    .queryVector(Vector.fromFloats(new float[] {1f, 2f, 3f}, NATIVE))
                    .build())
            .build();

    MongotKnnFloatQuery luceneQuery =
        new MongotKnnFloatQuery(metrics, "$type:knnVector/foo.bar", new float[] {1, 2, 3}, 20);

    new LuceneVectorTranslation(
            List.of(
                VectorDataFieldDefinitionBuilder.builder()
                    .path(path)
                    .numDimensions(3)
                    .similarity(VectorSimilarity.EUCLIDEAN)
                    .quantization(VectorQuantization.NONE)
                    .build()))
        .assertTranslatedTo(mongotQuery, luceneQuery);
  }

  @Test
  public void testCosineQuery_featureVersionFive_normalizesTargetWithoutMutatingQueryVector()
      throws IOException, InvalidQueryException {
    var path = FieldPath.parse("foo.bar");
    var queryVector = Vector.fromFloats(new float[] {3f, 4f}, NATIVE);

    var luceneQuery = translateCosineApproximateQuery(path, queryVector, 5);

    Assert.assertEquals(
        new MongotKnnFloatQuery(metrics, "$type:knnVector/foo.bar", new float[] {0.6f, 0.8f}, 20),
        luceneQuery);
    // The query vector is shared by every partition's query, so it must not be modified.
    Assert.assertArrayEquals(new float[] {3f, 4f}, queryVector.asFloatVector().getFloatVector(), 0);
  }

  @Test
  public void testCosineQuery_featureVersionFive_sharedQueryVectorGivesEveryPartitionUnitTarget()
      throws IOException, InvalidQueryException {
    var path = FieldPath.parse("foo.bar");
    var queryVector = Vector.fromFloats(new float[] {3f, 4f}, NATIVE);
    var expected =
        new MongotKnnFloatQuery(metrics, "$type:knnVector/foo.bar", new float[] {0.6f, 0.8f}, 20);

    // Each partition builds its own Lucene query from the same shared query vector.
    var firstPartition = translateCosineApproximateQuery(path, queryVector, 5);
    var secondPartition = translateCosineApproximateQuery(path, queryVector, 5);
    var thirdPartition = translateCosineApproximateQuery(path, queryVector, 5);

    Assert.assertEquals(expected, firstPartition);
    Assert.assertEquals(expected, secondPartition);
    Assert.assertEquals(expected, thirdPartition);
  }

  @Test
  public void testCosineQuery_featureVersionFour_doesNotNormalize()
      throws IOException, InvalidQueryException {
    var path = FieldPath.parse("foo.bar");
    var queryVector = Vector.fromFloats(new float[] {3f, 4f}, NATIVE);

    var luceneQuery = translateCosineApproximateQuery(path, queryVector, 4);

    Assert.assertEquals(
        new MongotKnnFloatQuery(metrics, "$type:knnVector/foo.bar", new float[] {3f, 4f}, 20),
        luceneQuery);
    Assert.assertArrayEquals(new float[] {3f, 4f}, queryVector.asFloatVector().getFloatVector(), 0);
  }

  private static Query translateCosineApproximateQuery(
      FieldPath path, Vector queryVector, int indexFeatureVersion)
      throws IOException, InvalidQueryException {
    var mongotQuery =
        VectorQueryBuilder.builder()
            .index("test")
            .criteria(
                ApproximateVectorQueryCriteriaBuilder.builder()
                    .path(path)
                    .numCandidates(20)
                    .limit(10)
                    .queryVector(queryVector)
                    .build())
            .build();
    var indexDefinition =
        VectorIndexDefinitionBuilder.builder()
            .setFields(
                List.of(
                    VectorDataFieldDefinitionBuilder.builder()
                        .path(path)
                        .numDimensions(2)
                        .similarity(VectorSimilarity.COSINE)
                        .quantization(VectorQuantization.NONE)
                        .build()))
            .indexFeatureVersion(indexFeatureVersion)
            .build();
    return new LuceneVectorTranslation(indexDefinition).translate(mongotQuery);
  }

  @Test
  public void testAutoNumCandidates_binaryQuantizedField_usesBinaryRatioForK()
      throws IOException, InvalidQueryException {
    var path = FieldPath.parse("foo.bar");
    var mongotQuery =
        VectorQueryBuilder.builder()
            .index("test")
            .criteria(
                ApproximateVectorQueryCriteriaBuilder.builder()
                    .path(path)
                    .limit(100)
                    .queryVector(Vector.fromFloats(new float[8], NATIVE))
                    .build())
            .build();

    // Binary-quantized field in auto mode: k = clamp(16 * 100, 100, 10000) = 1600.
    MongotKnnFloatQuery expected =
        new MongotKnnFloatQuery(metrics, "$type:knnF32Q1/foo.bar", new float[8], 1600);

    new LuceneVectorTranslation(
            List.of(
                VectorDataFieldDefinitionBuilder.builder()
                    .path(path)
                    .numDimensions(8)
                    .similarity(VectorSimilarity.EUCLIDEAN)
                    .quantization(VectorQuantization.BINARY)
                    .build()))
        .assertTranslatedTo(mongotQuery, expected);
  }

  @Test
  public void testAutoNumCandidates_scalarQuantizedField_usesDefaultRatioForK()
      throws IOException, InvalidQueryException {
    var path = FieldPath.parse("foo.bar");
    var mongotQuery =
        VectorQueryBuilder.builder()
            .index("test")
            .criteria(
                ApproximateVectorQueryCriteriaBuilder.builder()
                    .path(path)
                    .limit(100)
                    .queryVector(Vector.fromFloats(new float[] {1f, 2f, 3f}, NATIVE))
                    .build())
            .build();

    // Non-binary field in auto mode: k = clamp(8 * 100, 100, 10000) = 800.
    MongotKnnFloatQuery expected =
        new MongotKnnFloatQuery(metrics, "$type:knnF32Q7/foo.bar", new float[] {1, 2, 3}, 800);

    new LuceneVectorTranslation(
            List.of(
                VectorDataFieldDefinitionBuilder.builder()
                    .path(path)
                    .numDimensions(3)
                    .similarity(VectorSimilarity.EUCLIDEAN)
                    .quantization(VectorQuantization.SCALAR)
                    .build()))
        .assertTranslatedTo(mongotQuery, expected);
  }

  @Test
  public void testExplicitNumCandidates_binaryQuantizedField_usesUserValueForK()
      throws IOException, InvalidQueryException {
    var path = FieldPath.parse("foo.bar");
    var mongotQuery =
        VectorQueryBuilder.builder()
            .index("test")
            .criteria(
                ApproximateVectorQueryCriteriaBuilder.builder()
                    .path(path)
                    .limit(10)
                    .numCandidates(500)
                    .queryVector(Vector.fromFloats(new float[8], NATIVE))
                    .build())
            .build();

    // Explicit numCandidates is never changed, even on a binary-quantized field.
    MongotKnnFloatQuery expected =
        new MongotKnnFloatQuery(metrics, "$type:knnF32Q1/foo.bar", new float[8], 500);

    new LuceneVectorTranslation(
            List.of(
                VectorDataFieldDefinitionBuilder.builder()
                    .path(path)
                    .numDimensions(8)
                    .similarity(VectorSimilarity.EUCLIDEAN)
                    .quantization(VectorQuantization.BINARY)
                    .build()))
        .assertTranslatedTo(mongotQuery, expected);
  }

  @Test
  public void testApproximateVectorQueryWithFilter()
      throws IOException, InvalidQueryException, BsonParseException {

    var vectorPath = FieldPath.parse("foo.vector");
    var filterPath = FieldPath.parse("foo.filter");
    var mongotQuery =
        VectorQueryBuilder.builder()
            .criteria(
                ApproximateVectorQueryCriteriaBuilder.builder()
                    .limit(10)
                    .numCandidates(20)
                    .queryVector(Vector.fromFloats(new float[] {1f, 2f, 3f}, NATIVE))
                    .path(vectorPath)
                    .filter(
                        new VectorSearchFilter.ClauseFilter(
                            ClauseBuilder.simpleClause()
                                .addOperator(
                                    MqlFilterOperatorBuilder.eq().value(ValueBuilder.string("bar")))
                                .path(filterPath)
                                .build()))
                    .build())
            .index("test")
            .build();

    MongotKnnFloatQuery luceneQuery =
        new MongotKnnFloatQuery(
            metrics,
            FeatureFlags.getDefault(),
            "$type:knnVector/foo.vector",
            new float[] {1, 2, 3},
            20,
            new BooleanQuery.Builder()
                .add(
                    new IndexOrDocValuesQuery(
                        new ConstantScoreQuery(
                            new TermQuery(new Term("$type:token/foo.filter", new BytesRef("bar")))),
                        SortedSetDocValuesField.newSlowExactQuery(
                            "$type:token/foo.filter", new BytesRef("bar"))),
                    BooleanClause.Occur.MUST)
                .build());

    new LuceneVectorTranslation(
            List.of(
                VectorDataFieldDefinitionBuilder.builder()
                    .path(vectorPath)
                    .numDimensions(3)
                    .similarity(VectorSimilarity.EUCLIDEAN)
                    .quantization(VectorQuantization.NONE)
                    .build(),
                VectorIndexFilterFieldDefinition.create(filterPath)))
        .assertTranslatedTo(mongotQuery, luceneQuery);
  }

  @Test
  public void testApproximateVectorExplainQuery() throws IOException, InvalidQueryException {

    var path = FieldPath.parse("foo.bar");

    var definition =
        LuceneVectorTranslation.getIndexDefinition(
            List.of(
                VectorDataFieldDefinitionBuilder.builder()
                    .path(path)
                    .numDimensions(3)
                    .similarity(VectorSimilarity.EUCLIDEAN)
                    .quantization(VectorQuantization.NONE)
                    .build()));

    var context =
        new VectorQueryFactoryContext(
            definition, IndexFormatVersion.CURRENT, FeatureFlags.getDefault(), metrics);
    var factory = LuceneVectorQueryFactoryDistributor.create(context);

    var mongotQuery =
        VectorQueryBuilder.builder()
            .index("test")
            .criteria(
                ApproximateVectorQueryCriteriaBuilder.builder()
                    .path(path)
                    .numCandidates(20)
                    .limit(10)
                    .queryVector(Vector.fromFloats(new float[] {1f, 2f, 3f}, NATIVE))
                    .build())
            .build();

    var expected =
        new WrappedKnnQuery(
            new MongotKnnFloatQuery(metrics, "$type:knnVector/foo.bar", new float[] {1, 2, 3}, 20));

    try (var reader = DirectoryReader.open(directory)) {
      Query result =
          factory.createExplainQuery(
              new MaterializedVectorSearchQuery(
                  mongotQuery, mongotQuery.criteria().queryVector().get()),
              reader);
      Assert.assertEquals("VectorQuery should be wrapped", expected, result);
      Assert.assertEquals("Queries should be equal", expected, result);
    }
  }

  @Test
  public void testExactVectorQuery() throws IOException, InvalidQueryException {
    var path = FieldPath.parse("foo.bar");
    var mongotQuery =
        VectorQueryBuilder.builder()
            .index("test")
            .criteria(
                ExactVectorCriteriaBuilder.builder()
                    .path(path)
                    .limit(10)
                    .queryVector(Vector.fromFloats(new float[] {1f, 2f, 3f}, NATIVE))
                    .build())
            .build();

    String field = "$type:knnVector/foo.bar";
    ExactVectorSearchQuery luceneQuery =
        new ExactVectorSearchQuery(
            field,
            Vector.fromFloats(new float[] {1, 2, 3}, NATIVE),
            VectorSimilarityFunction.EUCLIDEAN,
            new FieldExistsQuery(field));

    new LuceneVectorTranslation(
            List.of(
                VectorDataFieldDefinitionBuilder.builder()
                    .path(path)
                    .numDimensions(3)
                    .similarity(VectorSimilarity.EUCLIDEAN)
                    .quantization(VectorQuantization.NONE)
                    .build()))
        .assertTranslatedTo(mongotQuery, luceneQuery);
  }

  @Test
  public void testExactVectorQueryWithFilter()
      throws IOException, InvalidQueryException, BsonParseException {
    var vectorPath = FieldPath.parse("foo.vector");
    var filterPath = FieldPath.parse("foo.filter");
    var mongotQuery =
        VectorQueryBuilder.builder()
            .criteria(
                ExactVectorCriteriaBuilder.builder()
                    .path(vectorPath)
                    .limit(10)
                    .queryVector(Vector.fromFloats(new float[] {1f, 2f, 3f}, NATIVE))
                    .filter(
                        new VectorSearchFilter.ClauseFilter(
                            ClauseBuilder.simpleClause()
                                .addOperator(
                                    MqlFilterOperatorBuilder.eq().value(ValueBuilder.string("bar")))
                                .path(filterPath)
                                .build()))
                    .build())
            .index("test")
            .build();

    var luceneVectorPath = "$type:knnVector/foo.vector";
    ExactVectorSearchQuery luceneQuery =
        new ExactVectorSearchQuery(
            luceneVectorPath,
            Vector.fromFloats(new float[] {1, 2, 3}, NATIVE),
            VectorSimilarityFunction.EUCLIDEAN,
            new BooleanQuery.Builder()
                .add(new FieldExistsQuery(luceneVectorPath), BooleanClause.Occur.FILTER)
                .add(
                    new BooleanQuery.Builder()
                        .add(
                            new IndexOrDocValuesQuery(
                                new ConstantScoreQuery(
                                    new TermQuery(
                                        new Term("$type:token/foo.filter", new BytesRef("bar")))),
                                SortedSetDocValuesField.newSlowExactQuery(
                                    "$type:token/foo.filter", new BytesRef("bar"))),
                            BooleanClause.Occur.MUST)
                        .build(),
                    BooleanClause.Occur.FILTER)
                .build());

    new LuceneVectorTranslation(
            List.of(
                VectorDataFieldDefinitionBuilder.builder()
                    .path(vectorPath)
                    .numDimensions(3)
                    .similarity(VectorSimilarity.EUCLIDEAN)
                    .quantization(VectorQuantization.NONE)
                    .build(),
                VectorIndexFilterFieldDefinition.create(filterPath)))
        .assertTranslatedTo(mongotQuery, luceneQuery);
  }

  @Test
  public void testExactVectorExplainQuery() throws IOException, InvalidQueryException {

    var path = FieldPath.parse("foo.bar");

    var definition =
        LuceneVectorTranslation.getIndexDefinition(
            List.of(
                VectorDataFieldDefinitionBuilder.builder()
                    .path(path)
                    .numDimensions(3)
                    .similarity(VectorSimilarity.EUCLIDEAN)
                    .quantization(VectorQuantization.NONE)
                    .build()));

    var context =
        new VectorQueryFactoryContext(
            definition, IndexFormatVersion.CURRENT, FeatureFlags.getDefault(), metrics);
    var factory = LuceneVectorQueryFactoryDistributor.create(context);

    var mongotQuery =
        VectorQueryBuilder.builder()
            .index("test")
            .criteria(
                ExactVectorCriteriaBuilder.builder()
                    .path(path)
                    .limit(10)
                    .queryVector(Vector.fromFloats(new float[] {1f, 2f, 3f}, NATIVE))
                    .build())
            .build();

    var luceneVectorPath = "$type:knnVector/foo.bar";
    var expected =
        new WrappedKnnQuery(
            new ExactVectorSearchQuery(
                luceneVectorPath,
                Vector.fromFloats(new float[] {1, 2, 3}, NATIVE),
                VectorSimilarityFunction.EUCLIDEAN,
                new FieldExistsQuery(luceneVectorPath)));

    try (var reader = DirectoryReader.open(directory)) {
      Query result =
          factory.createExplainQuery(
              new MaterializedVectorSearchQuery(
                  mongotQuery, mongotQuery.criteria().queryVector().get()),
              reader);
      Assert.assertEquals("VectorQuery should be wrapped", expected, result);
      Assert.assertEquals("Queries should be equal", expected, result);
    }
  }

  @Test
  public void testApproximateVectorQueryWithAutoEmbeddingFieldsMapping()
      throws IOException, InvalidQueryException {
    // This test verifies that when autoEmbeddingFieldsMapping is provided,
    // the Lucene query uses the internal mapped path instead of the user-provided path.
    //
    // Scenario:
    // - User query has vectorPath = "title"
    // - autoEmbeddingFieldsMapping = {"title" → "_autoEmbed.title"}
    // - Expected Lucene query should use "$type:knnVector/_autoEmbed.title"

    FieldPath userPath = FieldPath.parse("title");
    FieldPath internalPath = FieldPath.parse("_autoEmbed.title");

    // User's original query (before materialization)
    var userQuery =
        VectorQueryBuilder.builder()
            .index("test")
            .criteria(
                ApproximateVectorQueryCriteriaBuilder.builder()
                    .path(userPath) // User provides "title"
                    .numCandidates(20)
                    .limit(10)
                    .queryVector(Vector.fromFloats(new float[1024], NATIVE))
                    .build())
            .build();

    // Expected Lucene query should use the internal path
    MongotKnnFloatQuery expected =
        new MongotKnnFloatQuery(
            metrics,
            "$type:knnVector/_autoEmbed.title", // Internal path, not "title"
            new float[1024],
            20);

    // Materialized view index definition (what's actually indexed)
    new LuceneVectorTranslation(
            List.of(
                new VectorAutoEmbedFieldDefinition(
                    "voyage-3-large",
                    "text",
                    internalPath, // Internal path where vectors are stored
                    1024,
                    VectorSimilarity.EUCLIDEAN,
                    VectorAutoEmbedQuantization.FLOAT)),
            FeatureFlags.getDefault(),
            Map.of(userPath, internalPath))
        .assertTranslatedTo(
            userQuery, // Mapping: "title" → "_autoEmbed.title"
            expected);
  }

  @Test
  public void testApproximateVectorQueryWithAutoEmbeddingFieldsMappingAndFilter()
      throws IOException, InvalidQueryException, BsonParseException {
    // This test verifies that autoEmbeddingFieldsMapping correctly maps the vector path
    // while leaving the filter path unchanged.
    //
    // Scenario:
    // - User query: vectorPath = "title", filter on "title"
    // - autoEmbeddingFieldsMapping = {"title" → "_autoEmbed.title"}
    // - Expected: vector query uses "_autoEmbed.title", filter uses "title"

    FieldPath userPath = FieldPath.parse("title");
    FieldPath internalVectorPath = FieldPath.parse("_autoEmbed.title");

    // User's original query
    var userQuery =
        VectorQueryBuilder.builder()
            .index("test")
            .criteria(
                ApproximateVectorQueryCriteriaBuilder.builder()
                    .path(userPath) // User provides "title"
                    .numCandidates(20)
                    .limit(10)
                    .queryVector(Vector.fromFloats(new float[1024], NATIVE))
                    .filter(
                        new VectorSearchFilter.ClauseFilter(
                            ClauseBuilder.simpleClause()
                                .path(userPath) // Filter also on "title"
                                .addOperator(
                                    MqlFilterOperatorBuilder.eq()
                                        .value(ValueBuilder.string("laptop")))
                                .build()))
                    .build())
            .build();

    // Expected Lucene query
    MongotKnnFloatQuery expected =
        new MongotKnnFloatQuery(
            metrics,
            FeatureFlags.getDefault(),
            "$type:knnVector/_autoEmbed.title", // Vector uses internal path
            new float[1024],
            20,
            new BooleanQuery.Builder()
                .add(
                    new IndexOrDocValuesQuery(
                        new ConstantScoreQuery(
                            new TermQuery(
                                new Term(
                                    "$type:token/title", // Filter uses user path
                                    new BytesRef("laptop")))),
                        SortedSetDocValuesField.newSlowExactQuery(
                            "$type:token/title", new BytesRef("laptop"))),
                    BooleanClause.Occur.MUST)
                .build());

    // Materialized view index definition
    new LuceneVectorTranslation(
            List.of(
                new VectorAutoEmbedFieldDefinition(
                    "voyage-3-large",
                    "text",
                    internalVectorPath,
                    1024, // Internal vector path
                    VectorSimilarity.EUCLIDEAN,
                    VectorAutoEmbedQuantization.FLOAT),
                VectorIndexFilterFieldDefinition.create(userPath)),
            FeatureFlags.getDefault(),
            Map.of(userPath, internalVectorPath)) // Filter field at user path
        .assertTranslatedTo(
            userQuery, // Mapping: "title" → "_autoEmbed.title"
            expected);
  }
}
