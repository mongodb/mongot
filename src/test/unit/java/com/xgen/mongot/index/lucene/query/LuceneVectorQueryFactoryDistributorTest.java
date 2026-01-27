package com.xgen.mongot.index.lucene.query;

import static com.xgen.mongot.index.lucene.query.util.BooleanComposer.shouldClause;
import static com.xgen.mongot.util.bson.FloatVector.OriginalType.NATIVE;

import com.xgen.mongot.featureflag.FeatureFlags;
import com.xgen.mongot.index.IndexMetricsUpdater;
import com.xgen.mongot.index.definition.VectorIndexFilterFieldDefinition;
import com.xgen.mongot.index.definition.VectorQuantization;
import com.xgen.mongot.index.definition.VectorSimilarity;
import com.xgen.mongot.index.lucene.query.context.VectorQueryFactoryContext;
import com.xgen.mongot.index.lucene.query.custom.ExactVectorSearchQuery;
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
import org.apache.lucene.search.KnnFloatVectorQuery;
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

    KnnFloatVectorQuery luceneQuery =
        new KnnFloatVectorQuery("$type:knnVector/foo.bar", new float[] {1, 2, 3}, 20);

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

    KnnFloatVectorQuery luceneQuery =
        new KnnFloatVectorQuery(
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
            new KnnFloatVectorQuery("$type:knnVector/foo.bar", new float[] {1, 2, 3}, 20));

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
}
