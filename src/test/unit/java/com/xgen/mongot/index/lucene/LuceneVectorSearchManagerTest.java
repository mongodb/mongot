package com.xgen.mongot.index.lucene;

import static com.google.common.truth.Truth.assertThat;
import static com.xgen.mongot.util.bson.FloatVector.OriginalType.NATIVE;
import static org.apache.lucene.index.VectorSimilarityFunction.EUCLIDEAN;

import com.xgen.mongot.featureflag.FeatureFlags;
import com.xgen.mongot.index.IndexMetricsUpdater;
import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.lucene.codec.LuceneCodec;
import com.xgen.mongot.index.lucene.extension.KnnByteVectorField;
import com.xgen.mongot.index.lucene.extension.KnnFloatVectorField;
import com.xgen.mongot.index.lucene.field.FieldName;
import com.xgen.mongot.index.lucene.query.custom.MongotKnnByteQuery;
import com.xgen.mongot.index.lucene.query.custom.MongotKnnFloatQuery;
import com.xgen.mongot.index.lucene.query.custom.WrappedKnnQuery;
import com.xgen.mongot.index.lucene.query.util.WrappedToParentBlockJoinQuery;
import com.xgen.mongot.index.lucene.searcher.LuceneSearcherFactory;
import com.xgen.mongot.index.lucene.searcher.LuceneSearcherManager;
import com.xgen.mongot.index.lucene.searcher.QueryCacheProvider;
import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.bson.FloatVector;
import com.xgen.mongot.util.bson.Vector;
import com.xgen.testing.mongot.index.query.ApproximateVectorQueryCriteriaBuilder;
import com.xgen.testing.mongot.index.query.VectorQueryBuilder;
import com.xgen.testing.mongot.mock.index.SearchIndex;
import com.xgen.testing.mongot.mock.index.VectorIndex;
import java.io.IOException;
import java.util.Arrays;
import java.util.Optional;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.KnnByteVectorQuery;
import org.apache.lucene.search.KnnFloatVectorQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.join.QueryBitSetProducer;
import org.apache.lucene.search.join.ScoreMode;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.junit.Test;

public class LuceneVectorSearchManagerTest {

  private static final IndexMetricsUpdater.QueryingMetricsUpdater metrics =
      new IndexMetricsUpdater.QueryingMetricsUpdater(SearchIndex.mockMetricsFactory());

  private final IndexWriter indexWriter;

  public LuceneVectorSearchManagerTest() throws IOException {
    this.indexWriter =
        new IndexWriter(
            new ByteBuffersDirectory(),
            new org.apache.lucene.index.IndexWriterConfig().setCodec(new LuceneCodec()));
  }

  @Test
  public void shouldCapResultsWithLimitWhenNumCandidatesIsHigherThanLimit_float()
      throws IOException {

    int numCandidates = 3;
    FloatVector queryVector = Vector.fromFloats(new float[] {4f, 4f}, NATIVE);
    var query =
        VectorQueryBuilder.builder()
            .index("myVectorIndex")
            .criteria(
                ApproximateVectorQueryCriteriaBuilder.builder()
                    .queryVector(queryVector)
                    .path(FieldPath.newRoot("root"))
                    .numCandidates(numCandidates)
                    .limit(2)
                    .build())
            .build();
    var luceneFieldName =
        FieldName.TypeField.KNN_VECTOR.getLuceneFieldName(
            FieldPath.newRoot("field"), Optional.empty());

    var manager =
        new LuceneVectorSearchManager(
            new MongotKnnFloatQuery(
                metrics, luceneFieldName, queryVector.getFloatVector(), numCandidates),
            query.criteria(),
            Optional.empty(),
            Optional.empty());

    // insert documents into Lucene
    Document doc1 = new Document();
    doc1.add(new KnnFloatVectorField(luceneFieldName, new float[] {1, 1}, EUCLIDEAN));
    this.indexWriter.addDocument(doc1);

    Document doc2 = new Document();
    doc2.add(new KnnFloatVectorField(luceneFieldName, new float[] {2, 2}, EUCLIDEAN));
    this.indexWriter.addDocument(doc2);

    Document doc3 = new Document();
    doc3.add(new KnnFloatVectorField(luceneFieldName, new float[] {3, 3}, EUCLIDEAN));
    this.indexWriter.addDocument(doc3);

    Document doc4 = new Document();
    doc4.add(new KnnFloatVectorField(luceneFieldName, new float[] {4, 4}, EUCLIDEAN));
    this.indexWriter.addDocument(doc4);

    this.indexWriter.commit();

    LuceneSearcherManager searcherManager =
        LuceneSearcherManager.create(
            this.indexWriter,
            new LuceneSearcherFactory(
                VectorIndex.MOCK_VECTOR_DEFINITION,
                false,
                new QueryCacheProvider.DefaultQueryCacheProvider(),
                Optional.empty(),
                VectorIndex.mockQueryMetricsUpdater()),
            VectorIndex.mockMetricsFactory(),
            () -> false);

    var searcherReference =
        LuceneIndexSearcherReference.create(
            searcherManager,
            SearchIndex.mockQueryMetricsUpdater(IndexDefinition.Type.VECTOR_SEARCH),
            FeatureFlags.getDefault());

    var initialScoreDocs = manager.initialSearch(searcherReference, 2).topDocs.scoreDocs;
    assertThat(Arrays.stream(initialScoreDocs).map(s -> s.doc)).containsExactly(3, 2).inOrder();
  }

  @Test
  public void shouldCapResultsWithLimitWhenNumCandidatesIsHigherThanLimit_byte()
      throws IOException {

    int numCandidates = 3;
    Vector queryVector = Vector.fromBytes(new byte[] {4, 4});
    var query =
        VectorQueryBuilder.builder()
            .index("myVectorIndex")
            .criteria(
                ApproximateVectorQueryCriteriaBuilder.builder()
                    .queryVector(queryVector)
                    .path(FieldPath.newRoot("root"))
                    .numCandidates(numCandidates)
                    .limit(2)
                    .build())
            .build();
    var luceneFieldName =
        FieldName.TypeField.KNN_BYTE.getLuceneFieldName(
            FieldPath.newRoot("field"), Optional.empty());

    var manager =
        new LuceneVectorSearchManager(
            new MongotKnnByteQuery(metrics, luceneFieldName, queryVector.getBytes(), numCandidates),
            query.criteria(),
            Optional.empty(),
            Optional.empty());

    // insert documents into Lucene
    Document doc1 = new Document();
    doc1.add(new KnnByteVectorField(luceneFieldName, new byte[] {1, 1}, EUCLIDEAN));
    this.indexWriter.addDocument(doc1);

    Document doc2 = new Document();
    doc2.add(new KnnByteVectorField(luceneFieldName, new byte[] {2, 2}, EUCLIDEAN));
    this.indexWriter.addDocument(doc2);

    Document doc3 = new Document();
    doc3.add(new KnnByteVectorField(luceneFieldName, new byte[] {3, 3}, EUCLIDEAN));
    this.indexWriter.addDocument(doc3);

    Document doc4 = new Document();
    doc4.add(new KnnByteVectorField(luceneFieldName, new byte[] {4, 4}, EUCLIDEAN));
    this.indexWriter.addDocument(doc4);

    this.indexWriter.commit();

    LuceneSearcherManager searcherManager =
        LuceneSearcherManager.create(
            this.indexWriter,
            new LuceneSearcherFactory(
                VectorIndex.MOCK_VECTOR_DEFINITION,
                false,
                new QueryCacheProvider.DefaultQueryCacheProvider(),
                Optional.empty(),
                VectorIndex.mockQueryMetricsUpdater()),
            VectorIndex.mockMetricsFactory(),
            () -> false);

    var searcherReference =
        LuceneIndexSearcherReference.create(
            searcherManager,
            SearchIndex.mockQueryMetricsUpdater(IndexDefinition.Type.VECTOR_SEARCH),
            FeatureFlags.getDefault());

    var initialScoreDocs = manager.initialSearch(searcherReference, 2).topDocs.scoreDocs;
    assertThat(Arrays.stream(initialScoreDocs).map(s -> s.doc)).containsExactly(3, 2).inOrder();
  }

  // Tests for extractKnnQuery covering all four flat query shapes it must handle.

  @Test
  public void extractKnnQuery_directKnnFloat_returnsIt() {
    KnnFloatVectorQuery knn = new KnnFloatVectorQuery("v", new float[] {1f}, 10);

    assertThat(LuceneVectorSearchManager.extractKnnQuery(knn)).isSameInstanceAs(knn);
  }

  @Test
  public void extractKnnQuery_wrappedKnnAroundKnnFloat_returnsKnnFloat() {
    KnnFloatVectorQuery knn = new KnnFloatVectorQuery("v", new float[] {1f}, 10);
    WrappedKnnQuery wrappedKnn = new WrappedKnnQuery(knn);

    assertThat(LuceneVectorSearchManager.extractKnnQuery(wrappedKnn)).isSameInstanceAs(knn);
  }

  @Test
  public void extractKnnQuery_booleanQueryMustKnnFloat_returnsKnnFloat() {
    KnnFloatVectorQuery knn = new KnnFloatVectorQuery("v", new float[] {1f}, 10);
    TermQuery filter = new TermQuery(new Term("category", "A"));
    BooleanQuery boolQuery =
        new BooleanQuery.Builder()
            .add(knn, BooleanClause.Occur.MUST)
            .add(filter, BooleanClause.Occur.FILTER)
            .build();

    assertThat(LuceneVectorSearchManager.extractKnnQuery(boolQuery)).isSameInstanceAs(knn);
  }

  @Test
  public void extractKnnQuery_wrappedKnnAroundBooleanQueryMustKnnFloat_returnsKnnFloat() {
    KnnFloatVectorQuery knn = new KnnFloatVectorQuery("v", new float[] {1f}, 10);
    TermQuery filter = new TermQuery(new Term("category", "A"));
    BooleanQuery boolQuery =
        new BooleanQuery.Builder()
            .add(knn, BooleanClause.Occur.MUST)
            .add(filter, BooleanClause.Occur.FILTER)
            .build();
    WrappedKnnQuery wrappedKnn = new WrappedKnnQuery(boolQuery);

    assertThat(LuceneVectorSearchManager.extractKnnQuery(wrappedKnn)).isSameInstanceAs(knn);
  }

  // Byte variants of extractKnnQuery — the function was generalized in this PR to also
  // recognize KnnByteVectorQuery via isKnnVectorQuery.

  @Test
  public void extractKnnQuery_directKnnByte_returnsIt() {
    KnnByteVectorQuery knn = new KnnByteVectorQuery("v", new byte[] {1}, 10);

    assertThat(LuceneVectorSearchManager.extractKnnQuery(knn)).isSameInstanceAs(knn);
  }

  @Test
  public void extractKnnQuery_wrappedKnnAroundKnnByte_returnsKnnByte() {
    KnnByteVectorQuery knn = new KnnByteVectorQuery("v", new byte[] {1}, 10);
    WrappedKnnQuery wrappedKnn = new WrappedKnnQuery(knn);

    assertThat(LuceneVectorSearchManager.extractKnnQuery(wrappedKnn)).isSameInstanceAs(knn);
  }

  @Test
  public void extractKnnQuery_booleanQueryMustKnnByte_returnsKnnByte() {
    KnnByteVectorQuery knn = new KnnByteVectorQuery("v", new byte[] {1}, 10);
    TermQuery filter = new TermQuery(new Term("category", "A"));
    BooleanQuery boolQuery =
        new BooleanQuery.Builder()
            .add(knn, BooleanClause.Occur.MUST)
            .add(filter, BooleanClause.Occur.FILTER)
            .build();

    assertThat(LuceneVectorSearchManager.extractKnnQuery(boolQuery)).isSameInstanceAs(knn);
  }

  @Test
  public void extractKnnQuery_wrappedKnnAroundBooleanQueryMustKnnByte_returnsKnnByte() {
    KnnByteVectorQuery knn = new KnnByteVectorQuery("v", new byte[] {1}, 10);
    TermQuery filter = new TermQuery(new Term("category", "A"));
    BooleanQuery boolQuery =
        new BooleanQuery.Builder()
            .add(knn, BooleanClause.Occur.MUST)
            .add(filter, BooleanClause.Occur.FILTER)
            .build();
    WrappedKnnQuery wrappedKnn = new WrappedKnnQuery(boolQuery);

    assertThat(LuceneVectorSearchManager.extractKnnQuery(wrappedKnn)).isSameInstanceAs(knn);
  }

  // Tests for extractBlockJoinQuery covering all four query shapes it must handle.

  @Test
  public void extractBlockJoinQuery_directBlockJoin_returnsIt() {
    KnnFloatVectorQuery knn = new KnnFloatVectorQuery("v", new float[] {1f}, 10);
    QueryBitSetProducer parentFilter =
        new QueryBitSetProducer(new TermQuery(new Term("parent", "T")));
    WrappedToParentBlockJoinQuery blockJoin =
        new WrappedToParentBlockJoinQuery(knn, parentFilter, ScoreMode.Max);

    Optional<WrappedToParentBlockJoinQuery> result =
        LuceneVectorSearchManager.extractBlockJoinQuery(blockJoin);

    assertThat(result).isPresent();
    assertThat(result.get()).isSameInstanceAs(blockJoin);
  }

  @Test
  public void extractBlockJoinQuery_booleanQueryContainingBlockJoin_returnsIt() {
    KnnFloatVectorQuery knn = new KnnFloatVectorQuery("v", new float[] {1f}, 10);
    QueryBitSetProducer parentFilter =
        new QueryBitSetProducer(new TermQuery(new Term("parent", "T")));
    WrappedToParentBlockJoinQuery blockJoin =
        new WrappedToParentBlockJoinQuery(knn, parentFilter, ScoreMode.Max);
    TermQuery filter = new TermQuery(new Term("category", "A"));
    BooleanQuery boolQuery =
        new BooleanQuery.Builder()
            .add(blockJoin, BooleanClause.Occur.MUST)
            .add(filter, BooleanClause.Occur.FILTER)
            .build();

    Optional<WrappedToParentBlockJoinQuery> result =
        LuceneVectorSearchManager.extractBlockJoinQuery(boolQuery);

    assertThat(result).isPresent();
    assertThat(result.get()).isSameInstanceAs(blockJoin);
  }

  @Test
  public void extractBlockJoinQuery_wrappedKnnAroundBlockJoin_returnsBlockJoin() {
    KnnFloatVectorQuery knn = new KnnFloatVectorQuery("v", new float[] {1f}, 10);
    QueryBitSetProducer parentFilter =
        new QueryBitSetProducer(new TermQuery(new Term("parent", "T")));
    WrappedToParentBlockJoinQuery blockJoin =
        new WrappedToParentBlockJoinQuery(knn, parentFilter, ScoreMode.Max);
    WrappedKnnQuery wrappedKnn = new WrappedKnnQuery(blockJoin);

    Optional<WrappedToParentBlockJoinQuery> result =
        LuceneVectorSearchManager.extractBlockJoinQuery(wrappedKnn);

    assertThat(result).isPresent();
    assertThat(result.get()).isSameInstanceAs(blockJoin);
  }

  @Test
  public void extractBlockJoinQuery_wrappedKnnAroundBooleanQuery_returnsBlockJoin() {
    KnnFloatVectorQuery knn = new KnnFloatVectorQuery("v", new float[] {1f}, 10);
    QueryBitSetProducer parentFilter =
        new QueryBitSetProducer(new TermQuery(new Term("parent", "T")));
    WrappedToParentBlockJoinQuery blockJoin =
        new WrappedToParentBlockJoinQuery(knn, parentFilter, ScoreMode.Max);
    TermQuery filter = new TermQuery(new Term("category", "A"));
    BooleanQuery boolQuery =
        new BooleanQuery.Builder()
            .add(blockJoin, BooleanClause.Occur.MUST)
            .add(filter, BooleanClause.Occur.FILTER)
            .build();
    WrappedKnnQuery wrappedKnn = new WrappedKnnQuery(boolQuery);

    Optional<WrappedToParentBlockJoinQuery> result =
        LuceneVectorSearchManager.extractBlockJoinQuery(wrappedKnn);

    assertThat(result).isPresent();
    assertThat(result.get()).isSameInstanceAs(blockJoin);
  }

  @Test
  public void extractBlockJoinQuery_flatKnnQuery_returnsEmpty() {
    KnnFloatVectorQuery knn = new KnnFloatVectorQuery("v", new float[] {1f}, 10);

    Optional<WrappedToParentBlockJoinQuery> result =
        LuceneVectorSearchManager.extractBlockJoinQuery(knn);

    assertThat(result).isEmpty();
  }

  @Test
  public void extractBlockJoinQuery_wrappedKnnAroundFlatKnnQuery_returnsEmpty() {
    KnnFloatVectorQuery knn = new KnnFloatVectorQuery("v", new float[] {1f}, 10);
    WrappedKnnQuery wrappedKnn = new WrappedKnnQuery(knn);

    Optional<WrappedToParentBlockJoinQuery> result =
        LuceneVectorSearchManager.extractBlockJoinQuery(wrappedKnn);

    assertThat(result).isEmpty();
  }
}
