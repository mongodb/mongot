package com.xgen.mongot.index.lucene.vector;

import com.google.errorprone.annotations.Var;
import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlagConfig;
import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlagRegistry;
import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlags;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import org.apache.lucene.codecs.hnsw.DefaultFlatVectorScorer;
import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsReader;
import org.apache.lucene.codecs.lucene99.Lucene99ScalarQuantizedVectorScorer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.util.hnsw.RandomVectorScorer;
import org.apache.lucene.util.hnsw.RandomVectorScorerSupplier;
import org.apache.lucene.util.hnsw.UpdateableRandomVectorScorer;
import org.apache.lucene.util.quantization.QuantizedByteVectorValues;
import org.bson.types.ObjectId;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Covers the 4-wide bulk path of the Q7 native scorer.
 *
 * <p>The core guarantee is that {@code bulkScore} agrees with per-node {@code score}
 * <b>exactly</b>, not approximately: both funnel the same raw native integer through {@code
 * applyCorrection}, and the bulk fallback calls the same single-vector kernel. Any drift means the
 * two paths have diverged.
 *
 * <p>Lucene's {@code HnswGraphBuilder} caps a bulk chunk at 8 nodes and commonly passes 1-3, so the
 * {@code numNodes} sweep below deliberately covers the short batches that route through the
 * per-node remainder path instead of a full bulk4 call.
 */
public class NativeScalarQuantizedBulkScoringTest {
  private static final long SEED = 0xb01dfaceL;
  private static final int NUM_VECTORS = 40;
  private static final String FIELD = "vec";

  /**
   * 1030 is a non-register-multiple size above stock Lucene's 1024 cap; exercises the SIMD tail.
   */
  private static final int[] DIMS = {4, 16, 128, 17, 257, 1030};

  /**
   * Max chunk Lucene's HnswGraphBuilder will request; batches of 1-3 exercise the remainder path.
   */
  private static final int MAX_BULK_NODES = 8;

  private static final float SENTINEL = -12345f;

  @Rule public TemporaryFolder tempFolder = new TemporaryFolder();

  private int folderCounter;

  @BeforeClass
  public static void enableNativeScalarScoring() {
    DynamicFeatureFlagRegistry.resetForTest();
    DynamicFeatureFlagRegistry registry =
        new DynamicFeatureFlagRegistry(
            Optional.of(
                List.of(
                    new DynamicFeatureFlagConfig(
                        DynamicFeatureFlags.NATIVE_SCALAR_VECTOR_SCORING.getName(),
                        DynamicFeatureFlagConfig.Phase.ENABLED,
                        List.of(),
                        List.of(),
                        100,
                        DynamicFeatureFlagConfig.Scope.MONGOT_CLUSTER))),
            Optional.empty(),
            Optional.empty(),
            Optional.of(new ObjectId()));
    DynamicFeatureFlagRegistry.initializeProcessInstance(registry);
  }

  @AfterClass
  public static void resetRegistry() {
    DynamicFeatureFlagRegistry.resetForTest();
  }

  @FunctionalInterface
  private interface ValuesBody {
    void accept(QuantizedByteVectorValues qv) throws IOException;
  }

  private static void assumeNativeBulkAvailable() {
    // Both gates are required. On a scalar-only host the bulk symbols still export, but the
    // native scorer falls back to Lucene's Java scorers, so without the acceleration gate every
    // test would fail its native-scorer class assertions -- the scalar bulk fallback runs
    // inside the C export, below this test's reach.
    Assume.assumeTrue(
        "accelerated native mongot_vecsim library is required for this test",
        Similarity.I8_ACCELERATION);
    Assume.assumeTrue(
        "native library must export the bulk int8 entry points", Similarity.I8_BULK_AVAILABLE);
  }

  private static NativeScalarQuantizedVectorsScorer nativeScorer() {
    return new NativeScalarQuantizedVectorsScorer(
        new Lucene99ScalarQuantizedVectorScorer(DefaultFlatVectorScorer.INSTANCE));
  }

  private static float[] randomUnitVector(Random rng, int dim) {
    float[] v = new float[dim];
    double[] sumSq = {0.0};
    for (int i = 0; i < dim; i++) {
      v[i] = rng.nextFloat() * 2f - 1f;
      sumSq[0] += (double) v[i] * v[i];
    }
    float inv = (float) (1.0 / Math.sqrt(Math.max(sumSq[0], 1e-12)));
    for (int i = 0; i < dim; i++) {
      v[i] *= inv;
    }
    return v;
  }

  /** Spreads node ordinals so a batch does not read one contiguous run. */
  private static int[] nodesOfLength(int numNodes) {
    int[] nodes = new int[numNodes];
    for (int i = 0; i < numNodes; i++) {
      nodes[i] = (i * 7 + 3) % NUM_VECTORS;
    }
    return nodes;
  }

  /**
   * Builds a single-segment MMap-backed Q7 index and hands its on-disk quantized values to {@code
   * body}. Read back via {@code DirectoryReader.open(w)} with {@link
   * NativeScalarQuantizedTestCodec} so the native format resolves directly rather than through SPI
   * name lookup.
   */
  private void withQuantizedValues(VectorSimilarityFunction fn, int dim, ValuesBody body)
      throws IOException {
    withQuantizedValues(fn, dim, MMapDirectory.DEFAULT_MAX_CHUNK_SIZE, body);
  }

  /**
   * Same as {@link #withQuantizedValues(VectorSimilarityFunction, int, ValuesBody)}, with an
   * explicit mmap chunk size so a test can force the quantized vector data across more than one
   * mapped segment.
   */
  private void withQuantizedValues(
      VectorSimilarityFunction fn, int dim, long maxChunkSize, ValuesBody body) throws IOException {
    Path dirPath = this.tempFolder.newFolder("idx_" + this.folderCounter++).toPath();
    Random rng = new Random(SEED);
    try (MMapDirectory dir = new MMapDirectory(dirPath, maxChunkSize);
        IndexWriter w =
            new IndexWriter(
                dir, new IndexWriterConfig().setCodec(new NativeScalarQuantizedTestCodec()))) {
      for (int i = 0; i < NUM_VECTORS; i++) {
        Document doc = new Document();
        doc.add(new KnnFloatVectorField(FIELD, randomUnitVector(rng, dim), fn));
        w.addDocument(doc);
      }
      w.commit();
      try (DirectoryReader reader = DirectoryReader.open(w)) {
        CodecReader codecReader = (CodecReader) reader.leaves().get(0).reader();
        Lucene99HnswVectorsReader hnswReader =
            (Lucene99HnswVectorsReader) codecReader.getVectorReader();
        body.accept(hnswReader.getQuantizedVectorValues(FIELD));
      }
    }
  }

  /** Returns the supplier's scorer, asserting the native path was actually selected. */
  private static UpdateableRandomVectorScorer nativeSupplierScorer(
      VectorSimilarityFunction fn, QuantizedByteVectorValues qv) throws IOException {
    RandomVectorScorerSupplier supplier = nativeScorer().getRandomVectorScorerSupplier(fn, qv);
    Assert.assertEquals(
        "expected native supplier, got " + supplier.getClass().getSimpleName(),
        "NativeScalarScorerSupplier",
        supplier.getClass().getSimpleName());
    return supplier.scorer();
  }

  /** Returns a native query scorer, asserting the native path was actually selected. */
  private static RandomVectorScorer nativeQueryScorer(
      VectorSimilarityFunction fn, QuantizedByteVectorValues qv, float[] query) throws IOException {
    RandomVectorScorer scorer = nativeScorer().getRandomVectorScorer(fn, qv, query);
    Assert.assertEquals(
        "expected native query scorer, got " + scorer.getClass().getSimpleName(),
        "NativeScalarQueryScorer",
        scorer.getClass().getSimpleName());
    return scorer;
  }

  /**
   * Asserts bulkScore agrees with per-node score exactly, and that the returned maximum matches,
   * across every batch length Lucene can request.
   */
  private static void assertBulkAgreesWithScore(RandomVectorScorer scorer, String label)
      throws IOException {
    for (int numNodes = 1; numNodes <= MAX_BULK_NODES; numNodes++) {
      int[] nodes = nodesOfLength(numNodes);
      float[] bulkScores = new float[numNodes];
      float bulkMax = scorer.bulkScore(nodes, bulkScores, numNodes);

      @Var float expectedMax = Float.NEGATIVE_INFINITY;
      for (int i = 0; i < numNodes; i++) {
        float expected = scorer.score(nodes[i]);
        Assert.assertEquals(
            label + " numNodes=" + numNodes + " lane=" + i, expected, bulkScores[i], 0f);
        expectedMax = Math.max(expectedMax, expected);
      }
      Assert.assertEquals(label + " max numNodes=" + numNodes, expectedMax, bulkMax, 0f);
    }
  }

  @Test
  public void supplierScorer_bulkScoreMatchesScore() throws IOException {
    assumeNativeBulkAvailable();
    for (VectorSimilarityFunction fn : VectorSimilarityFunction.values()) {
      for (int dim : DIMS) {
        withQuantizedValues(
            fn,
            dim,
            qv -> {
              UpdateableRandomVectorScorer scorer = nativeSupplierScorer(fn, qv);
              scorer.setScoringOrdinal(1);
              assertBulkAgreesWithScore(scorer, "supplier fn=" + fn + " dim=" + dim);
            });
      }
    }
  }

  @Test
  public void queryScorer_bulkScoreMatchesScore() throws IOException {
    assumeNativeBulkAvailable();
    for (VectorSimilarityFunction fn : VectorSimilarityFunction.values()) {
      for (int dim : DIMS) {
        withQuantizedValues(
            fn,
            dim,
            qv -> {
              float[] query = randomUnitVector(new Random(SEED * 3), dim);
              RandomVectorScorer scorer = nativeQueryScorer(fn, qv, query);
              assertBulkAgreesWithScore(scorer, "query fn=" + fn + " dim=" + dim);
            });
      }
    }
  }

  @Test
  public void bulkScore_zeroNodes_returnsNegativeInfinityAndWritesNothing() throws IOException {
    assumeNativeBulkAvailable();
    withQuantizedValues(
        VectorSimilarityFunction.COSINE,
        64,
        qv -> {
          UpdateableRandomVectorScorer scorer =
              nativeSupplierScorer(VectorSimilarityFunction.COSINE, qv);
          scorer.setScoringOrdinal(0);
          float[] scores = new float[4];
          Arrays.fill(scores, SENTINEL);
          float max = scorer.bulkScore(new int[4], scores, 0);
          Assert.assertEquals(Float.NEGATIVE_INFINITY, max, 0f);
          for (float score : scores) {
            Assert.assertEquals("no score should be written when numNodes=0", SENTINEL, score, 0f);
          }
        });
  }

  /** Callers reuse oversized scratch arrays, so anything past numNodes must be left alone. */
  @Test
  public void bulkScore_oversizedArrays_onlyWritesRequestedRange() throws IOException {
    assumeNativeBulkAvailable();
    withQuantizedValues(
        VectorSimilarityFunction.DOT_PRODUCT,
        64,
        qv -> {
          UpdateableRandomVectorScorer scorer =
              nativeSupplierScorer(VectorSimilarityFunction.DOT_PRODUCT, qv);
          scorer.setScoringOrdinal(0);
          int numNodes = 3;
          int[] nodes = nodesOfLength(MAX_BULK_NODES);
          float[] scores = new float[MAX_BULK_NODES];
          Arrays.fill(scores, SENTINEL);

          scorer.bulkScore(nodes, scores, numNodes);

          for (int i = 0; i < numNodes; i++) {
            Assert.assertNotEquals(
                "lane " + i + " should have been scored", SENTINEL, scores[i], 0f);
          }
          for (int i = numNodes; i < MAX_BULK_NODES; i++) {
            Assert.assertEquals("lane " + i + " is past numNodes", SENTINEL, scores[i], 0f);
          }
        });
  }

  @Test
  public void bulkScore_outOfRangeOrdinal_throws() throws IOException {
    assumeNativeBulkAvailable();
    withQuantizedValues(
        VectorSimilarityFunction.EUCLIDEAN,
        64,
        qv -> {
          UpdateableRandomVectorScorer scorer =
              nativeSupplierScorer(VectorSimilarityFunction.EUCLIDEAN, qv);
          scorer.setScoringOrdinal(0);
          Assert.assertThrows(
              IllegalArgumentException.class,
              () -> scorer.bulkScore(new int[] {NUM_VECTORS}, new float[1], 1));
          Assert.assertThrows(
              IllegalArgumentException.class,
              () -> scorer.bulkScore(new int[] {-1}, new float[1], 1));
          // Out-of-range ordinal in a short batch (routed through the per-node remainder path,
          // not a full bulk4 call) must be caught too.
          Assert.assertThrows(
              IllegalArgumentException.class,
              () -> scorer.bulkScore(new int[] {0, NUM_VECTORS}, new float[2], 2));
        });
  }

  @Test
  public void supplierScorer_bulkScoreWithoutScoringOrdinal_throws() throws IOException {
    assumeNativeBulkAvailable();
    withQuantizedValues(
        VectorSimilarityFunction.COSINE,
        64,
        qv -> {
          UpdateableRandomVectorScorer scorer =
              nativeSupplierScorer(VectorSimilarityFunction.COSINE, qv);
          Assert.assertThrows(
              IllegalStateException.class, () -> scorer.bulkScore(new int[] {0}, new float[1], 1));
        });
  }

  /**
   * With the flag on and an mmap-backed directory, {@code nativeInput()} succeeds (unlike {@code
   * NativeScalarQuantizedScoringParityTest#nonMemoryMappedInput_fallsBackToJava}, which forces that
   * check to fail). The vector data is spread across more than one mmap chunk, so {@code
   * wholeFileSegmentOrNull()} returns null and both scorers fall back to {@code perNodeBulkScore}
   * -- a different null check than the native-scorer-never-constructed case.
   */
  @Test
  public void bulkScore_multiChunkMmapInput_fallsBackToPerNodeScoring() throws IOException {
    assumeNativeBulkAvailable();
    long tinyChunkSize = 1024;
    int dim = 128;
    withQuantizedValues(
        VectorSimilarityFunction.DOT_PRODUCT,
        dim,
        tinyChunkSize,
        qv -> {
          UpdateableRandomVectorScorer supplierScorer =
              nativeSupplierScorer(VectorSimilarityFunction.DOT_PRODUCT, qv);
          supplierScorer.setScoringOrdinal(1);
          assertBulkAgreesWithScore(supplierScorer, "supplier multi-chunk dim=" + dim);

          float[] query = randomUnitVector(new Random(SEED * 5), dim);
          RandomVectorScorer queryScorer =
              nativeQueryScorer(VectorSimilarityFunction.DOT_PRODUCT, qv, query);
          assertBulkAgreesWithScore(queryScorer, "query multi-chunk dim=" + dim);
        });
  }
}
