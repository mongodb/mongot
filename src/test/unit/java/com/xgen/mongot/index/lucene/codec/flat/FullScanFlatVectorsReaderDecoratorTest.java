package com.xgen.mongot.index.lucene.codec.flat;

import static com.google.common.truth.Truth.assertThat;

import com.google.errorprone.annotations.Var;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.perfield.PerFieldKnnVectorsFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.KnnByteVectorField;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.AcceptDocs;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.KnnByteVectorQuery;
import org.apache.lucene.search.KnnFloatVectorQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopKnnCollector;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * End-to-end tests for {@link FullScanFlatVectorsReaderDecorator} over a real in-memory Lucene
 * index.
 *
 * <p>The index is intentionally sparse: only every third document carries vectors, so the scan must
 * map vector ordinals back to the correct doc ids. Documents are also tagged with a {@code group}
 * term so the same index can be queried with and without a pre-filter.
 */
public class FullScanFlatVectorsReaderDecoratorTest {

  private static final String FLOAT_FIELD = "float_vector";
  private static final String BYTE_FIELD = "byte_vector";
  private static final String GROUP_FIELD = "group";
  private static final String GROUP_A = "a";
  private static final String GROUP_B = "b";

  /** Number of docs in the index; large enough to span several bulk scoring batches. */
  private static final int NUM_DOCS = 500;

  /** Only every {@code VECTOR_DOC_INTERVAL}-th document has vectors. */
  private static final int VECTOR_DOC_INTERVAL = 3;

  /** Maps doc id to the vector's distinguishing coordinate, for docs that have vectors. */
  private final Map<Integer, Integer> vectorByDocId = new HashMap<>();

  private Directory directory;
  private DirectoryReader reader;
  private IndexSearcher searcher;

  @Before
  public void setUp() throws IOException {
    this.directory = new ByteBuffersDirectory();

    IndexWriterConfig config = new IndexWriterConfig().setCodec(flatVectorsCodec());
    try (IndexWriter writer = new IndexWriter(this.directory, config)) {
      for (int i = 0; i < NUM_DOCS; i++) {
        Document document = new Document();
        document.add(new StringField(GROUP_FIELD, i % 2 == 0 ? GROUP_A : GROUP_B, Field.Store.NO));
        if (i % VECTOR_DOC_INTERVAL == 0) {
          document.add(new KnnFloatVectorField(FLOAT_FIELD, new float[] {i, 0.0f}));
          document.add(new KnnByteVectorField(BYTE_FIELD, new byte[] {(byte) (i % 128), 0}));
        }
        writer.addDocument(document);
      }
      writer.forceMerge(1);
    }

    this.reader = DirectoryReader.open(this.directory);
    this.searcher = new IndexSearcher(this.reader);

    // The single force-merged segment preserves insertion order, so doc id == insertion index.
    for (int i = 0; i < NUM_DOCS; i += VECTOR_DOC_INTERVAL) {
      this.vectorByDocId.put(i, i);
    }
  }

  @After
  public void tearDown() throws IOException {
    this.reader.close();
    this.directory.close();
  }

  /** A codec that routes every vector field through the full-scan flat format under test. */
  private static Codec flatVectorsCodec() {
    Codec delegate = Codec.getDefault();
    KnnVectorsFormat format = new Float32AndByteFlatVectorsFormat();
    return new FilterCodec(delegate.getName(), delegate) {
      @Override
      public KnnVectorsFormat knnVectorsFormat() {
        return new PerFieldKnnVectorsFormat() {
          @Override
          public KnnVectorsFormat getKnnVectorsFormatForField(String field) {
            return format;
          }
        };
      }
    };
  }

  @Test
  public void testFloatVectorSearchWithoutFilterReturnsEveryVectorDocWithExactScores()
      throws IOException {
    int k = this.vectorByDocId.size();
    TopDocs topDocs =
        this.searcher.search(new KnnFloatVectorQuery(FLOAT_FIELD, new float[] {0.0f, 0.0f}, k), k);

    assertThat(topDocs.scoreDocs).hasLength(k);
    assertScoreDocsValid(topDocs, /* expectedGroup= */ null);
  }

  @Test
  public void testFloatVectorSearchWithFilterOnlyReturnsMatchingDocs() throws IOException {
    Query filter = new TermQuery(new Term(GROUP_FIELD, GROUP_A));
    int k = 25;
    TopDocs topDocs =
        this.searcher.search(
            new KnnFloatVectorQuery(FLOAT_FIELD, new float[] {0.0f, 0.0f}, k, filter), k);

    assertThat(topDocs.scoreDocs).hasLength(k);
    assertScoreDocsValid(topDocs, GROUP_A);
  }

  /**
   * A candidate limit far smaller than one bulk scoring batch must still produce hits; a scan that
   * terminates on a batch boundary before collecting would return nothing here.
   */
  @Test
  public void testFloatVectorSearchWithSmallKReturnsNearestDocs() throws IOException {
    int k = 3;
    TopDocs topDocs =
        this.searcher.search(new KnnFloatVectorQuery(FLOAT_FIELD, new float[] {0.0f, 0.0f}, k), k);

    assertThat(topDocs.scoreDocs).hasLength(k);
    assertScoreDocsValid(topDocs, /* expectedGroup= */ null);
    // Nearest to the origin are the vectors with the smallest coordinate.
    assertThat(topDocs.scoreDocs[0].doc).isEqualTo(0);
    assertThat(topDocs.scoreDocs[1].doc).isEqualTo(VECTOR_DOC_INTERVAL);
    assertThat(topDocs.scoreDocs[2].doc).isEqualTo(2 * VECTOR_DOC_INTERVAL);
  }

  @Test
  public void testFloatVectorSearchWithFilterAndSmallKReturnsNearestMatchingDocs()
      throws IOException {
    Query filter = new TermQuery(new Term(GROUP_FIELD, GROUP_B));
    int k = 2;
    TopDocs topDocs =
        this.searcher.search(
            new KnnFloatVectorQuery(FLOAT_FIELD, new float[] {0.0f, 0.0f}, k, filter), k);

    assertThat(topDocs.scoreDocs).hasLength(k);
    assertScoreDocsValid(topDocs, GROUP_B);
    // Odd-numbered docs that are also multiples of the vector interval: 3, 9, 15, ...
    assertThat(topDocs.scoreDocs[0].doc).isEqualTo(3);
    assertThat(topDocs.scoreDocs[1].doc).isEqualTo(9);
  }

  @Test
  public void testByteVectorSearchWithAndWithoutFilterReturnsMatchingDocs() throws IOException {
    int k = 10;
    byte[] target = {0, 0};

    TopDocs unfiltered = this.searcher.search(new KnnByteVectorQuery(BYTE_FIELD, target, k), k);
    assertThat(unfiltered.scoreDocs).hasLength(k);
    for (ScoreDoc scoreDoc : unfiltered.scoreDocs) {
      assertThat(this.vectorByDocId).containsKey(scoreDoc.doc);
      assertThat(scoreDoc.score).isGreaterThan(0.0f);
    }

    Query filter = new TermQuery(new Term(GROUP_FIELD, GROUP_A));
    TopDocs filtered =
        this.searcher.search(new KnnByteVectorQuery(BYTE_FIELD, target, k, filter), k);
    assertThat(filtered.scoreDocs).hasLength(k);
    for (ScoreDoc scoreDoc : filtered.scoreDocs) {
      assertThat(this.vectorByDocId).containsKey(scoreDoc.doc);
      assertThat(scoreDoc.doc % 2).isEqualTo(0);
      assertThat(scoreDoc.score).isGreaterThan(0.0f);
    }
  }

  /**
   * Drives the reader directly with a collector whose visit limit is smaller than a bulk scoring
   * batch. A scan that records visits before collecting would terminate on the first batch boundary
   * and return nothing.
   */
  @Test
  public void testSearchWithVisitLimitBelowBulkBatchSizeStillReturnsHits() throws IOException {
    CodecReader leafReader = (CodecReader) this.reader.leaves().getFirst().reader();
    int k = 3;
    TopKnnCollector collector = new TopKnnCollector(k, /* visitLimit= */ 5);

    leafReader
        .getVectorReader()
        .search(
            FLOAT_FIELD,
            new float[] {0.0f, 0.0f},
            collector,
            AcceptDocs.fromLiveDocs(leafReader.getLiveDocs(), leafReader.maxDoc()));

    TopDocs topDocs = collector.topDocs();
    assertThat(topDocs.scoreDocs).hasLength(k);
    assertScoreDocsValid(topDocs, /* expectedGroup= */ null);
  }

  @Test
  public void testSearchOnFieldWithNoVectorsReturnsNoHits() throws IOException {
    TopDocs topDocs =
        this.searcher.search(new KnnFloatVectorQuery("missing", new float[] {0.0f, 0.0f}, 10), 10);

    assertThat(topDocs.scoreDocs).isEmpty();
  }

  /**
   * Asserts every hit is a document that actually has a vector, optionally that it belongs to the
   * expected filter group, that its score is exactly the euclidean similarity of the indexed
   * vector, and that hits are ordered by descending score.
   */
  private void assertScoreDocsValid(TopDocs topDocs, String expectedGroup) {
    @Var float previousScore = Float.MAX_VALUE;
    for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
      assertThat(this.vectorByDocId).containsKey(scoreDoc.doc);
      if (expectedGroup != null) {
        int expectedParity = GROUP_A.equals(expectedGroup) ? 0 : 1;
        assertThat(scoreDoc.doc % 2).isEqualTo(expectedParity);
      }
      // Euclidean similarity against the origin: 1 / (1 + squaredDistance).
      float coordinate = this.vectorByDocId.get(scoreDoc.doc);
      float expectedScore = 1.0f / (1.0f + coordinate * coordinate);
      assertThat(scoreDoc.score).isWithin(1e-6f).of(expectedScore);
      assertThat(scoreDoc.score).isAtMost(previousScore);
      previousScore = scoreDoc.score;
    }
  }
}
