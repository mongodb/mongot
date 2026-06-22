package com.xgen.mongot.index.lucene.query;

import static com.google.common.truth.Truth.assertThat;

import com.xgen.mongot.index.IndexMetricsUpdater;
import com.xgen.mongot.index.lucene.query.custom.MongotKnnFloatQuery;
import com.xgen.mongot.index.lucene.query.util.WrappedToParentBlockJoinQuery;
import com.xgen.testing.TestUtils;
import com.xgen.testing.mongot.mock.index.SearchIndex;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.search.join.QueryBitSetProducer;
import org.apache.lucene.search.join.ScoreMode;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class NestedAvgVectorRescorerTest {

  private static final IndexMetricsUpdater.QueryingMetricsUpdater METRICS =
      new IndexMetricsUpdater.QueryingMetricsUpdater(SearchIndex.mockMetricsFactory());

  private static final String VECTOR_FIELD = "child_vector";
  private static final String PARENT_MARKER_FIELD = "$meta/embeddedRoot";

  private Directory directory;
  private IndexWriter writer;

  @Before
  public void setUp() throws IOException {
    TemporaryFolder temporaryFolder = TestUtils.getTempFolder();
    this.directory = new MMapDirectory(temporaryFolder.getRoot().toPath());
    this.writer = new IndexWriter(this.directory, new IndexWriterConfig());
    this.writer.commit();
  }

  @After
  public void tearDown() throws IOException {
    this.writer.close();
    this.directory.close();
  }

  @Test
  public void testRescoreComputesCorrectAvgOverAllChildren() throws IOException {
    // Parent A: 3 children with DOT_PRODUCT similarities to query [1,0,0]:
    //   child1=[1,0,0] → similarity=1.0, child2=[0.5,0,0] → sim=0.5, child3=[0.2,0,0] → sim=0.2
    //   True avg = (1.0 + 0.5 + 0.2) / 3 = 0.5667
    this.writer.addDocuments(
        createBlock(
            new float[] {1f, 0f, 0f}, new float[] {0.5f, 0f, 0f}, new float[] {0.2f, 0f, 0f}));
    this.writer.commit();

    float[] queryVector = new float[] {1f, 0f, 0f};

    try (DirectoryReader reader = DirectoryReader.open(this.directory)) {
      IndexSearcher searcher = new IndexSearcher(reader);

      WrappedToParentBlockJoinQuery blockJoinQuery = buildBlockJoinQuery(queryVector, 3);
      TopDocs initial = searcher.search(blockJoinQuery, 10);

      assertThat(initial.scoreDocs).hasLength(1);

      NestedAvgVectorRescorer rescorer = new NestedAvgVectorRescorer();
      TopDocs rescored = rescorer.rescore(searcher, initial, blockJoinQuery, 10);

      assertThat(rescored.scoreDocs).hasLength(1);
      float expectedAvg = dotProductAvg(queryVector, 1f, 0.5f, 0.2f);
      assertThat(rescored.scoreDocs[0].score).isWithin(1e-4f).of(expectedAvg);
    }
  }

  @Test
  public void testRescoreReordersParentsByTrueAverage() throws IOException {
    // Parent A (first block): 3 children
    //   child1=[1,0,0] → 1.0, child2=[0.1,0,0] → 0.1, child3=[0.1,0,0] → 0.1
    //   True avg = 0.4
    // Parent B (second block): 2 children
    //   child1=[0.6,0,0] → 0.6, child2=[0.5,0,0] → 0.5
    //   True avg = 0.55
    //
    // ANN with numCandidates=2 might only find child1 of A (score 1.0)
    // and child1 of B (score 0.6), giving A a biased avg > B's biased avg.
    // After rescore, B (true avg 0.55) should outrank A (true avg 0.4).
    this.writer.addDocuments(
        createBlock(
            new float[] {1f, 0f, 0f}, new float[] {0.1f, 0f, 0f}, new float[] {0.1f, 0f, 0f}));
    this.writer.addDocuments(createBlock(new float[] {0.6f, 0f, 0f}, new float[] {0.5f, 0f, 0f}));
    this.writer.commit();

    float[] queryVector = new float[] {1f, 0f, 0f};

    try (DirectoryReader reader = DirectoryReader.open(this.directory)) {
      IndexSearcher searcher = new IndexSearcher(reader);

      // Use a larger numCandidates to get both parents as candidates
      WrappedToParentBlockJoinQuery blockJoinQuery = buildBlockJoinQuery(queryVector, 10);
      TopDocs initial = searcher.search(blockJoinQuery, 10);
      assertThat(initial.scoreDocs).hasLength(2);

      NestedAvgVectorRescorer rescorer = new NestedAvgVectorRescorer();
      TopDocs rescored = rescorer.rescore(searcher, initial, blockJoinQuery, 10);

      assertThat(rescored.scoreDocs).hasLength(2);

      // Parent B (doc 5 = 2 children + parent A + 2 children + parent B)
      // should now rank first with true avg > parent A
      float parentAAvg = dotProductAvg(queryVector, 1f, 0.1f, 0.1f);
      float parentBAvg = dotProductAvg(queryVector, 0.6f, 0.5f);
      assertThat(parentBAvg).isGreaterThan(parentAAvg);

      assertThat(rescored.scoreDocs[0].score).isWithin(1e-4f).of(parentBAvg);
      assertThat(rescored.scoreDocs[1].score).isWithin(1e-4f).of(parentAAvg);
    }
  }

  @Test
  public void testRescoreWithLimitTruncatesResults() throws IOException {
    this.writer.addDocuments(createBlock(new float[] {0.9f, 0f, 0f}));
    this.writer.addDocuments(createBlock(new float[] {0.5f, 0f, 0f}));
    this.writer.addDocuments(createBlock(new float[] {0.3f, 0f, 0f}));
    this.writer.commit();

    float[] queryVector = new float[] {1f, 0f, 0f};

    try (DirectoryReader reader = DirectoryReader.open(this.directory)) {
      IndexSearcher searcher = new IndexSearcher(reader);

      WrappedToParentBlockJoinQuery blockJoinQuery = buildBlockJoinQuery(queryVector, 10);
      TopDocs initial = searcher.search(blockJoinQuery, 10);
      assertThat(initial.scoreDocs).hasLength(3);

      NestedAvgVectorRescorer rescorer = new NestedAvgVectorRescorer();
      TopDocs rescored = rescorer.rescore(searcher, initial, blockJoinQuery, 1);

      assertThat(rescored.scoreDocs).hasLength(1);
    }
  }

  @Test
  public void testRescoreWithEmptyTopDocsReturnsEmpty() throws IOException {
    this.writer.addDocuments(createBlock(new float[] {1f, 0f, 0f}));
    this.writer.commit();

    float[] queryVector = new float[] {1f, 0f, 0f};

    try (DirectoryReader reader = DirectoryReader.open(this.directory)) {
      IndexSearcher searcher = new IndexSearcher(reader);

      WrappedToParentBlockJoinQuery blockJoinQuery = buildBlockJoinQuery(queryVector, 10);
      TopDocs emptyDocs =
          new TopDocs(new TotalHits(0, TotalHits.Relation.EQUAL_TO), new ScoreDoc[0]);

      NestedAvgVectorRescorer rescorer = new NestedAvgVectorRescorer();
      TopDocs result = rescorer.rescore(searcher, emptyDocs, blockJoinQuery, 10);

      assertThat(result.scoreDocs).isEmpty();
    }
  }

  @Test
  public void testExtractBlockJoinQueryFromDirectQuery() {
    WrappedToParentBlockJoinQuery blockJoinQuery =
        new WrappedToParentBlockJoinQuery(
            new MatchAllDocsQuery(),
            new QueryBitSetProducer(EmbeddedDocumentQueryFactory.parentFilter(Optional.empty())),
            ScoreMode.Avg);

    Optional<WrappedToParentBlockJoinQuery> extracted =
        NestedAvgVectorRescorer.extractBlockJoinQuery(blockJoinQuery);

    assertThat(extracted.isPresent()).isTrue();
    assertThat(extracted.get()).isSameInstanceAs(blockJoinQuery);
  }

  @Test
  public void testExtractBlockJoinQueryFromBooleanQuery() {
    WrappedToParentBlockJoinQuery blockJoinQuery =
        new WrappedToParentBlockJoinQuery(
            new MatchAllDocsQuery(),
            new QueryBitSetProducer(EmbeddedDocumentQueryFactory.parentFilter(Optional.empty())),
            ScoreMode.Avg);

    BooleanQuery wrapped =
        new BooleanQuery.Builder()
            .add(blockJoinQuery, BooleanClause.Occur.MUST)
            .add(new MatchAllDocsQuery(), BooleanClause.Occur.FILTER)
            .build();

    Optional<WrappedToParentBlockJoinQuery> extracted =
        NestedAvgVectorRescorer.extractBlockJoinQuery(wrapped);

    assertThat(extracted.isPresent()).isTrue();
    assertThat(extracted.get()).isSameInstanceAs(blockJoinQuery);
  }

  @Test
  public void testExtractBlockJoinQueryFromNonBlockJoinReturnsEmpty() {
    Optional<WrappedToParentBlockJoinQuery> extracted =
        NestedAvgVectorRescorer.extractBlockJoinQuery(new MatchAllDocsQuery());

    assertThat(extracted.isPresent()).isFalse();
  }

  @Test
  public void testRescoreWithBlockJoinWrappedInBooleanQuery() throws IOException {
    this.writer.addDocuments(createBlock(new float[] {1f, 0f, 0f}, new float[] {0.2f, 0f, 0f}));
    this.writer.commit();

    float[] queryVector = new float[] {1f, 0f, 0f};

    try (DirectoryReader reader = DirectoryReader.open(this.directory)) {
      IndexSearcher searcher = new IndexSearcher(reader);

      MongotKnnFloatQuery knnQuery =
          new MongotKnnFloatQuery(METRICS, VECTOR_FIELD, queryVector, 10);
      WrappedToParentBlockJoinQuery blockJoinQuery =
          new WrappedToParentBlockJoinQuery(
              knnQuery,
              new QueryBitSetProducer(EmbeddedDocumentQueryFactory.parentFilter(Optional.empty())),
              ScoreMode.Avg);

      // Wrap in BooleanQuery (simulates parentFilter present)
      BooleanQuery wrappedQuery =
          new BooleanQuery.Builder()
              .add(blockJoinQuery, BooleanClause.Occur.MUST)
              .add(EmbeddedDocumentQueryFactory.ROOT_DOCUMENTS_QUERY, BooleanClause.Occur.FILTER)
              .build();

      TopDocs initial = searcher.search(wrappedQuery, 10);
      assertThat(initial.scoreDocs).hasLength(1);

      NestedAvgVectorRescorer rescorer = new NestedAvgVectorRescorer();
      TopDocs rescored = rescorer.rescore(searcher, initial, wrappedQuery, 10);

      assertThat(rescored.scoreDocs).hasLength(1);
      float expectedAvg = dotProductAvg(queryVector, 1f, 0.2f);
      assertThat(rescored.scoreDocs[0].score).isWithin(1e-4f).of(expectedAvg);
    }
  }

  // ---- helpers ----

  private static WrappedToParentBlockJoinQuery buildBlockJoinQuery(
      float[] queryVector, int numCandidates) {
    MongotKnnFloatQuery knnQuery =
        new MongotKnnFloatQuery(METRICS, VECTOR_FIELD, queryVector, numCandidates);

    return new WrappedToParentBlockJoinQuery(
        knnQuery,
        new QueryBitSetProducer(EmbeddedDocumentQueryFactory.parentFilter(Optional.empty())),
        ScoreMode.Avg);
  }

  private static List<Document> createBlock(float[]... childVectors) {
    List<Document> block = new ArrayList<>();
    for (float[] vector : childVectors) {
      Document child = new Document();
      child.add(new StringField("$meta/embeddedPath", "sections", Field.Store.NO));
      child.add(
          new KnnFloatVectorField(VECTOR_FIELD, vector, VectorSimilarityFunction.DOT_PRODUCT));
      block.add(child);
    }
    Document root = new Document();
    root.add(new StringField(PARENT_MARKER_FIELD, "T", Field.Store.NO));
    block.add(root);
    return block;
  }

  /**
   * Computes the expected DOT_PRODUCT avg score as Lucene would report it. Lucene's DOT_PRODUCT
   * similarity normalizes the raw dot product: score = (1 + dotProduct) / 2.
   */
  private static float dotProductAvg(float[] queryVector, float... childFirstComponents) {
    @com.google.errorprone.annotations.Var float sum = 0;
    for (float component : childFirstComponents) {
      float dot = component * queryVector[0];
      sum += (1 + dot) / 2;
    }
    return sum / childFirstComponents.length;
  }
}
