package com.xgen.mongot.index.lucene.quantization;

import static com.google.common.truth.Truth.assertThat;

import com.xgen.mongot.index.lucene.extension.KnnByteVectorField;
import com.xgen.mongot.index.lucene.field.FieldName;
import com.xgen.mongot.index.lucene.vector.Similarity;
import com.xgen.mongot.util.FieldPath;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.KnnByteVectorQuery;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.search.VectorScorer;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.junit.Assume;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Covers the native SIMD path of {@link ByteBinaryQuantizedRescoreQuery#fullFidelityScorer}, which
 * is reached through {@link Mongot01042BinaryQuantizedFlatVectorsFormat}'s {@code flatVectorScorer}
 * wrapping {@link com.xgen.mongot.index.lucene.vector.Lucene99NativeMemorySegmentVectorScorer} as
 * its non-quantized delegate.
 *
 * <p>{@link BinaryQuantizedVectorRescorerTest} indexes into a {@link ByteBuffersDirectory}, which
 * is not memory-segment backed, so every one of its cases exercises the Java fallback. These tests
 * use {@link MMapDirectory} so the native branch is actually reachable, and assert it engaged
 * rather than only that scores match — dot-product/L2/max-inner-product are bit-identical between
 * the native and Java int8 paths, so a scores-only assertion would pass regardless of which one
 * ran.
 */
public class NativeByteBinaryQuantizedRescoreTest {

  private static final float DELTA = 1e-5f;

  private static final String FIELD =
      FieldName.TypeField.KNN_BYTE_Q1.getLuceneFieldName(
          FieldPath.newRoot("field"), Optional.empty());

  @Rule public TemporaryFolder tempFolder = new TemporaryFolder();

  /**
   * Includes the int8 extremes so the native kernels are exercised at the edges of the range. No
   * zero-magnitude vector: it is degenerate under COSINE, where Lucene's Java comparison divides by
   * zero and yields {@code NaN} while the native kernel deliberately floors to the minimum
   * similarity (see {@code sim_cos_combine} in {@code mongot_vecsim.c}), so the two are not
   * comparable there. This is pre-existing behavior of the already-shipped native byte path, not
   * something introduced by wiring it into rescoring.
   */
  private static List<byte[]> testVectors() {
    return List.of(
        new byte[] {-128, 100, 0, 7, -1, 42, 127, -60},
        new byte[] {80, -128, 33, -7, 12, -42, 5, 61},
        new byte[] {20, 30, -100, 0, 64, 1, -13, 9},
        new byte[] {127, 127, 127, -128, -128, -128, 0, 0});
  }

  private void indexVectors(
      Directory directory, List<byte[]> vectors, VectorSimilarityFunction similarity)
      throws IOException {
    try (IndexWriter writer =
        new IndexWriter(
            directory,
            new IndexWriterConfig()
                .setMergePolicy(NoMergePolicy.INSTANCE)
                .setCodec(new Mongot01042BinaryQuantizedTestCodec()))) {
      for (byte[] vector : vectors) {
        Document document = new Document();
        document.add(new KnnByteVectorField(FIELD, vector, similarity));
        writer.addDocument(document);
      }
      writer.commit();
    }
  }

  private static VectorScorer fullFidelityScorer(LeafReader leafReader, byte[] target)
      throws IOException {
    ByteBinaryQuantizedRescoreQuery rescoreQuery =
        new ByteBinaryQuantizedRescoreQuery(
            new KnnByteVectorQuery(FIELD, target, Integer.MAX_VALUE));
    return rescoreQuery.fullFidelityScorer(leafReader).orElseThrow();
  }

  /**
   * The scorer returned by {@code ByteVectorValues.scorer(byte[])} is Lucene's own anonymous {@code
   * VectorScorer} (defined inside {@code OffHeapByteVectorValues}), which closes over a {@code
   * RandomVectorScorer} field. When native scoring engages, that field is a {@code
   * NativeByteQueryScorer}; when it falls back, it is one of Lucene's own scorer types. There is no
   * public API to ask "did this engage natively," so this reflects into the closure's captured
   * fields — brittle against a Lucene upgrade changing that inner class's shape, but isolated here
   * so an upgrade produces one clear failure rather than silently losing this assertion.
   */
  private static boolean capturesNativeByteQueryScorer(VectorScorer scorer) {
    for (Field field : scorer.getClass().getDeclaredFields()) {
      field.setAccessible(true);
      Object value;
      try {
        value = field.get(scorer);
      } catch (IllegalAccessException e) {
        throw new AssertionError(e);
      }
      if (value != null && value.getClass().getName().contains("NativeByteQueryScorer")) {
        return true;
      }
    }
    return false;
  }

  /**
   * Walks every doc in the segment through the scorer the same way {@code
   * BinaryQuantizedVectorRescorer.scoreSegment} does — forward-only {@code advance}, then {@code
   * score} at the current position — and asserts each score matches Lucene's Java comparison of the
   * same raw vectors.
   */
  private static void assertScoresMatchJava(
      LeafReader leafReader,
      VectorScorer scorer,
      byte[] target,
      List<byte[]> vectors,
      VectorSimilarityFunction similarity)
      throws IOException {
    ByteVectorValues rawValues = leafReader.getByteVectorValues(FIELD);
    ByteVectorValues.DocIndexIterator rawIterator = rawValues.iterator();

    for (int doc = 0; doc < vectors.size(); doc++) {
      assertThat(scorer.iterator().advance(doc)).isEqualTo(doc);
      assertThat(rawIterator.advance(doc)).isEqualTo(doc);

      byte[] stored = rawValues.vectorValue(rawIterator.index());
      float expected = similarity.compare(target, stored);

      assertThat(scorer.score()).isWithin(DELTA).of(expected);
    }
  }

  @Test
  public void fullFidelityScorer_mmapBacked_engagesNativeScorer() throws IOException {
    Assume.assumeTrue(
        "native int8 acceleration unavailable on this host", Similarity.I8_ACCELERATION);

    List<byte[]> vectors = testVectors();
    try (Directory directory = new MMapDirectory(this.tempFolder.newFolder("native").toPath())) {
      indexVectors(directory, vectors, VectorSimilarityFunction.EUCLIDEAN);

      try (DirectoryReader reader = DirectoryReader.open(directory)) {
        LeafReader leafReader = reader.leaves().get(0).reader();
        VectorScorer scorer = fullFidelityScorer(leafReader, vectors.get(0));

        assertThat(capturesNativeByteQueryScorer(scorer)).isTrue();
      }
    }
  }

  @Test
  public void fullFidelityScorer_nativeScores_matchJavaScores() throws IOException {
    Assume.assumeTrue(
        "native int8 acceleration unavailable on this host", Similarity.I8_ACCELERATION);

    List<byte[]> vectors = testVectors();
    for (VectorSimilarityFunction similarity : VectorSimilarityFunction.values()) {
      Path dir = this.tempFolder.newFolder("parity_" + similarity).toPath();
      try (Directory directory = new MMapDirectory(dir)) {
        indexVectors(directory, vectors, similarity);

        try (DirectoryReader reader = DirectoryReader.open(directory)) {
          LeafReader leafReader = reader.leaves().get(0).reader();
          for (byte[] target : vectors) {
            VectorScorer scorer = fullFidelityScorer(leafReader, target);
            assertThat(capturesNativeByteQueryScorer(scorer)).isTrue();
            assertScoresMatchJava(leafReader, scorer, target, vectors, similarity);
          }
        }
      }
    }
  }

  /**
   * {@code BinaryQuantizedVectorRescorer.scoreNestedSegment} calls {@code scorer.iterator()}
   * repeatedly, branches on {@code docID()} instead of always advancing, and may score the same
   * position more than once. That requires {@code iterator()} to hand back the same live iterator
   * every time and {@code score()} to stay keyed to its current position. This exercises that
   * contract directly against the scorer Lucene hands back, rather than assuming it holds.
   */
  @Test
  public void fullFidelityScorer_nativeScorer_supportsNestedAccessPattern() throws IOException {
    Assume.assumeTrue(
        "native int8 acceleration unavailable on this host", Similarity.I8_ACCELERATION);

    List<byte[]> vectors = testVectors();
    try (Directory directory = new MMapDirectory(this.tempFolder.newFolder("nested").toPath())) {
      indexVectors(directory, vectors, VectorSimilarityFunction.EUCLIDEAN);

      try (DirectoryReader reader = DirectoryReader.open(directory)) {
        LeafReader leafReader = reader.leaves().get(0).reader();
        byte[] target = vectors.get(0);
        VectorScorer scorer = fullFidelityScorer(leafReader, target);
        assertThat(capturesNativeByteQueryScorer(scorer)).isTrue();

        assertThat(scorer.iterator()).isSameInstanceAs(scorer.iterator());

        assertThat(scorer.iterator().advance(1)).isEqualTo(1);
        assertThat(scorer.iterator().docID()).isEqualTo(1);

        // Re-scoring the current position without advancing must be stable and must match Java.
        float first = scorer.score();
        float second = scorer.score();
        assertThat(second).isEqualTo(first);

        ByteVectorValues rawValues = leafReader.getByteVectorValues(FIELD);
        ByteVectorValues.DocIndexIterator rawIterator = rawValues.iterator();
        assertThat(rawIterator.advance(1)).isEqualTo(1);
        assertThat(first)
            .isWithin(DELTA)
            .of(
                VectorSimilarityFunction.EUCLIDEAN.compare(
                    target, rawValues.vectorValue(rawIterator.index())));
      }
    }
  }

  /**
   * End-to-end through the real rescorer entry point, not just the scorer contract in isolation:
   * builds a candidate list covering every doc and drives it through {@code
   * BinaryQuantizedVectorRescorer.FullFidelityRescorer#rescore}, asserting the returned ordering
   * matches ranking by Lucene's own Java similarity comparison.
   */
  @Test
  public void fullFidelityRescorer_mmapBacked_returnsResultsInSimilarityOrder() throws IOException {
    Assume.assumeTrue(
        "native int8 acceleration unavailable on this host", Similarity.I8_ACCELERATION);

    List<byte[]> vectors = testVectors();
    byte[] query = new byte[] {9, 9, 9, 9, 9, 9, 9, 9};
    VectorSimilarityFunction similarity = VectorSimilarityFunction.EUCLIDEAN;

    try (Directory directory = new MMapDirectory(this.tempFolder.newFolder("e2e").toPath())) {
      indexVectors(directory, vectors, similarity);

      try (DirectoryReader reader = DirectoryReader.open(directory)) {
        ScoreDoc[] candidates = new ScoreDoc[vectors.size()];
        for (int doc = 0; doc < vectors.size(); doc++) {
          candidates[doc] = new ScoreDoc(doc, 0f);
        }
        TopDocs approximate =
            new TopDocs(new TotalHits(candidates.length, TotalHits.Relation.EQUAL_TO), candidates);

        BinaryQuantizedVectorRescorer.FullFidelityRescorer rescorer =
            new BinaryQuantizedVectorRescorer.FullFidelityRescorer(
                new ByteBinaryQuantizedRescoreQuery(
                    new KnnByteVectorQuery(FIELD, query, Integer.MAX_VALUE)),
                Optional.empty());

        TopDocs rescored = rescorer.rescore(new IndexSearcher(reader), approximate, vectors.size());

        List<Integer> expectedOrder =
            java.util.stream.IntStream.range(0, vectors.size())
                .boxed()
                .sorted(
                    Comparator.<Integer>comparingDouble(
                            doc -> similarity.compare(query, vectors.get(doc)))
                        .reversed())
                .collect(Collectors.toList());

        List<Integer> actualOrder = new ArrayList<>();
        for (ScoreDoc scoreDoc : rescored.scoreDocs) {
          actualOrder.add(scoreDoc.doc);
        }

        assertThat(actualOrder).containsExactlyElementsIn(expectedOrder).inOrder();
      }
    }
  }

  /** A non-memory-segment-backed directory must fall back to the Java scorer and still work. */
  @Test
  public void fullFidelityScorer_nonMmapBacked_fallsBackToJavaScorer() throws IOException {
    List<byte[]> vectors = testVectors();
    try (Directory directory = new ByteBuffersDirectory()) {
      indexVectors(directory, vectors, VectorSimilarityFunction.EUCLIDEAN);

      try (DirectoryReader reader = DirectoryReader.open(directory)) {
        LeafReader leafReader = reader.leaves().get(0).reader();
        byte[] target = vectors.get(0);
        VectorScorer scorer = fullFidelityScorer(leafReader, target);

        assertThat(capturesNativeByteQueryScorer(scorer)).isFalse();
        assertScoresMatchJava(
            leafReader, scorer, target, vectors, VectorSimilarityFunction.EUCLIDEAN);
      }
    }
  }
}
