package com.xgen.mongot.index.lucene.vector;

import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlagConfig;
import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlagRegistry;
import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlags;
import com.xgen.mongot.util.GlobalMetricFactory;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import org.apache.lucene.codecs.hnsw.DefaultFlatVectorScorer;
import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsReader;
import org.apache.lucene.codecs.lucene99.Lucene99ScalarQuantizedVectorScorer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.KnnFloatVectorQuery;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.util.hnsw.RandomVectorScorer;
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
 * Verifies that native scalar-quantized (Q7) scoring produces results matching Lucene's Java scalar
 * scorer. The same MMap-backed HNSW index is queried with the {@code NATIVE_SCALAR_VECTOR_SCORING}
 * flag ON (native path) and OFF (delegates to Lucene's scalar scorer); per-document scores must
 * match within tolerance.
 *
 * <p>The flag is read from the process-global {@link DynamicFeatureFlagRegistry}, installed once
 * and toggled per query via {@link DynamicFeatureFlagRegistry#updateDynamicFeatureFlags}.
 */
public class NativeScalarQuantizedScoringParityTest {
  private static final long SEED = 0xfeedfaceL;
  private static final int NUM_VECTORS = 50;
  private static final int NUM_QUERIES = 8;
  private static final float DELTA = 1e-4f;
  private static final String FIELD = "vec";
  private static final String ID = "id";

  private static DynamicFeatureFlagRegistry registry;

  @Rule public TemporaryFolder tempFolder = new TemporaryFolder();

  @BeforeClass
  public static void initRegistry() {
    // Reset first so this is robust if another test class already installed the process-global
    // instance in a shared JVM (initializeProcessInstance throws if one is already present).
    DynamicFeatureFlagRegistry.resetForTest();
    registry =
        new DynamicFeatureFlagRegistry(
            Optional.empty(), Optional.empty(), Optional.empty(), Optional.of(new ObjectId()));
    DynamicFeatureFlagRegistry.initializeProcessInstance(registry);
  }

  @AfterClass
  public static void resetRegistry() {
    DynamicFeatureFlagRegistry.resetForTest();
  }

  /** Toggles the native-scalar-scoring flag on the process-global registry the scorer reads. */
  private static void setNativeScoring(boolean enabled) {
    if (enabled) {
      registry.updateDynamicFeatureFlags(
          List.of(
              new DynamicFeatureFlagConfig(
                  DynamicFeatureFlags.NATIVE_SCALAR_VECTOR_SCORING.getName(),
                  DynamicFeatureFlagConfig.Phase.ENABLED,
                  List.of(),
                  List.of(),
                  100,
                  DynamicFeatureFlagConfig.Scope.MONGOT_CLUSTER)));
    } else {
      registry.updateDynamicFeatureFlags(List.of());
    }
  }

  private static float[] randomUnitVector(Random rng, int dim) {
    float[] v = new float[dim];
    // Accumulate via a single-element array to avoid Error Prone's @Var requirement on a reassigned
    // scalar.
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

  /** Maps stored id -> score for all hits of a KnnFloatVectorQuery over the whole index. */
  private static Map<Integer, Float> scoresById(IndexSearcher searcher, float[] query)
      throws IOException {
    TopDocs topDocs =
        searcher.search(new KnnFloatVectorQuery(FIELD, query, NUM_VECTORS), NUM_VECTORS);
    StoredFields storedFields = searcher.getIndexReader().storedFields();
    Map<Integer, Float> byId = new HashMap<>();
    for (ScoreDoc sd : topDocs.scoreDocs) {
      int id = Integer.parseInt(storedFields.document(sd.doc).get(ID));
      byId.put(id, sd.score);
    }
    return byId;
  }

  /** Returns the on-disk {@link QuantizedByteVectorValues} for {@code field}'s single segment. */
  private static QuantizedByteVectorValues quantizedVectorValues(
      DirectoryReader reader, String field) throws IOException {
    CodecReader codecReader = (CodecReader) reader.leaves().get(0).reader();
    Lucene99HnswVectorsReader hnswReader =
        (Lucene99HnswVectorsReader) codecReader.getVectorReader();
    return hnswReader.getQuantizedVectorValues(field);
  }

  /**
   * Constructs a scorer directly from the on-disk quantized values and asserts it is {@code
   * NativeScalarQueryScorer} -- i.e. that the flag-on run actually took the native path, not just
   * that its scores happen to match the Java path (which would also hold if native silently never
   * engaged).
   */
  private static void assertNativeScorerUsed(
      VectorSimilarityFunction fn, QuantizedByteVectorValues qv, float[] query) throws IOException {
    RandomVectorScorer scorer =
        new NativeScalarQuantizedVectorsScorer(
                new Lucene99ScalarQuantizedVectorScorer(DefaultFlatVectorScorer.INSTANCE))
            .getRandomVectorScorer(fn, qv, query);
    Assert.assertEquals(
        "expected native scorer, got " + scorer.getClass().getSimpleName(),
        "NativeScalarQueryScorer",
        scorer.getClass().getSimpleName());
  }

  /**
   * Constructs a scorer directly from the on-disk quantized values with the flag off and asserts it
   * is NOT {@code NativeScalarQueryScorer} -- i.e. that the flag-off run actually fell through to
   * Lucene's Java scorer, not just that its scores happen to match the native path. The delegate's
   * concrete return type varies by {@code fn} (e.g. {@code Euclidean} vs a dot-product variant), so
   * this checks the one invariant that matters rather than a fixed class name.
   */
  private static void assertDefaultScorerUsed(
      VectorSimilarityFunction fn, QuantizedByteVectorValues qv, float[] query) throws IOException {
    RandomVectorScorer scorer =
        new NativeScalarQuantizedVectorsScorer(
                new Lucene99ScalarQuantizedVectorScorer(DefaultFlatVectorScorer.INSTANCE))
            .getRandomVectorScorer(fn, qv, query);
    Assert.assertNotEquals(
        "expected delegate scorer, got native scorer",
        "NativeScalarQueryScorer",
        scorer.getClass().getSimpleName());
  }

  @Test
  public void nativeScoring_matchesLuceneScalarScorer() throws IOException {
    Assume.assumeTrue(
        "native mongot_vecsim library is required for this parity test",
        Similarity.I8_ACCELERATION);

    Random rng = new Random(SEED);
    for (VectorSimilarityFunction fn : VectorSimilarityFunction.values()) {
      // 1030 is above stock Lucene's 1024 cap (Q7 raises getMaxDimensions to 8192) and is a
      // non-register-multiple tail size, so the raised cap and the SIMD tail are both exercised.
      for (int dim : new int[] {4, 16, 128, 17, 257, 1030}) {
        Path dirPath = this.tempFolder.newFolder("idx_" + fn + "_" + dim).toPath();
        // Read back via DirectoryReader.open(w) with NativeScalarQuantizedTestCodec so the query
        // path resolves to the native format directly. A PerField codec read via open(dir) would
        // resolve the shadowed name through KnnVectorsFormat.forName, which returns Lucene's stock
        // scalar format in the unit-test classpath -- the native scorer would never run and the
        // parity check would trivially compare the stock scorer against itself.
        try (MMapDirectory dir = new MMapDirectory(dirPath)) {
          setNativeScoring(true);
          try (IndexWriter w =
              new IndexWriter(
                  dir, new IndexWriterConfig().setCodec(new NativeScalarQuantizedTestCodec()))) {
            for (int i = 0; i < NUM_VECTORS; i++) {
              Document doc = new Document();
              doc.add(new KnnFloatVectorField(FIELD, randomUnitVector(rng, dim), fn));
              doc.add(new StoredField(ID, Integer.toString(i)));
              w.addDocument(doc);
            }
            w.commit();

            try (DirectoryReader reader = DirectoryReader.open(w)) {
              IndexSearcher searcher = new IndexSearcher(reader);
              QuantizedByteVectorValues qv = quantizedVectorValues(reader, FIELD);
              for (int q = 0; q < NUM_QUERIES; q++) {
                float[] query = randomUnitVector(rng, dim);

                setNativeScoring(true);
                assertNativeScorerUsed(fn, qv, query);
                Map<Integer, Float> nativeScores = scoresById(searcher, query);

                setNativeScoring(false);
                assertDefaultScorerUsed(fn, qv, query);
                Map<Integer, Float> luceneScores = scoresById(searcher, query);

                Assert.assertEquals(
                    "hit set differs fn=" + fn + " dim=" + dim,
                    luceneScores.keySet(),
                    nativeScores.keySet());
                for (Map.Entry<Integer, Float> e : luceneScores.entrySet()) {
                  Assert.assertEquals(
                      "score differs fn=" + fn + " dim=" + dim + " id=" + e.getKey(),
                      e.getValue(),
                      nativeScores.get(e.getKey()),
                      DELTA);
                }
              }
            }
          }
        }
      }
    }
  }

  /**
   * With the flag ON but a non-memory-mapped directory, the quantized slice cannot unwrap to a
   * {@link org.apache.lucene.store.MemorySegmentAccessInput}, so {@code nativeInput()} returns null
   * and the native scorer must delegate to Lucene's Java scalar scorer for both graph build and
   * search. The flag-on scores must therefore equal the flag-off (Java) scores. The native format
   * is still exercised on read (via {@link NativeScalarQuantizedTestCodec} + {@code open(w)}); it
   * is the {@code nativeInput()} precondition, not codec resolution, that forces the fallback here.
   */
  @Test
  public void nonMemoryMappedInput_fallsBackToJava() throws IOException {
    Assume.assumeTrue(
        "native mongot_vecsim library is required for this test", Similarity.I8_ACCELERATION);

    VectorSimilarityFunction fn = VectorSimilarityFunction.COSINE;
    int dim = 64;
    Random rng = new Random(SEED);
    try (ByteBuffersDirectory dir = new ByteBuffersDirectory()) {
      setNativeScoring(true);
      try (IndexWriter w =
          new IndexWriter(
              dir, new IndexWriterConfig().setCodec(new NativeScalarQuantizedTestCodec()))) {
        for (int i = 0; i < NUM_VECTORS; i++) {
          Document doc = new Document();
          doc.add(new KnnFloatVectorField(FIELD, randomUnitVector(rng, dim), fn));
          doc.add(new StoredField(ID, Integer.toString(i)));
          w.addDocument(doc);
        }
        w.commit();

        try (DirectoryReader reader = DirectoryReader.open(w)) {
          IndexSearcher searcher = new IndexSearcher(reader);
          float[] query = randomUnitVector(rng, dim);

          setNativeScoring(true);
          Map<Integer, Float> flagOnScores = scoresById(searcher, query);

          setNativeScoring(false);
          Map<Integer, Float> javaScores = scoresById(searcher, query);

          Assert.assertEquals(
              "hit set differs on non-mmap input", javaScores.keySet(), flagOnScores.keySet());
          for (Map.Entry<Integer, Float> e : javaScores.entrySet()) {
            Assert.assertEquals(
                "score differs on non-mmap input id=" + e.getKey(),
                e.getValue(),
                flagOnScores.get(e.getKey()),
                DELTA);
          }
        }
      }
    }
  }

  /**
   * Verifies {@link GlobalMetricFactory#incrementNativeScalarVectorScoringEngaged} actually fires
   * when the native path is taken, and stops firing once the flag is disabled -- this is the
   * counter's whole purpose: distinguishing "native scoring is genuinely engaged" from "the flag is
   * on but resolution silently fell back to Java", without needing a debugger to tell the two
   * apart.
   */
  @Test
  public void nativeScoring_incrementsEngagedMetric() throws IOException {
    Assume.assumeTrue(
        "native mongot_vecsim library is required for this test", Similarity.I8_ACCELERATION);

    // Metric names must match GlobalMetricFactory's package-private constants, not visible from
    // this package.
    String metricName = "native_scalar_vector_scoring_engaged";
    String queryActiveGaugeName = "native_scalar_vector_scoring_query_active";
    SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
    GlobalMetricFactory.resetForTest();
    GlobalMetricFactory.initialize(meterRegistry);
    try {
      VectorSimilarityFunction fn = VectorSimilarityFunction.COSINE;
      int dim = 32;
      Random rng = new Random(SEED);
      Path dirPath = this.tempFolder.newFolder("metric_test").toPath();
      try (MMapDirectory dir = new MMapDirectory(dirPath)) {
        setNativeScoring(true);
        try (IndexWriter w =
            new IndexWriter(
                dir, new IndexWriterConfig().setCodec(new NativeScalarQuantizedTestCodec()))) {
          for (int i = 0; i < NUM_VECTORS; i++) {
            Document doc = new Document();
            doc.add(new KnnFloatVectorField(FIELD, randomUnitVector(rng, dim), fn));
            doc.add(new StoredField(ID, Integer.toString(i)));
            w.addDocument(doc);
          }
          w.commit();

          // Graph build/merge (getRandomVectorScorerSupplier) engages native scoring too, but must
          // never touch the query-only gauge -- no $vectorSearch has run yet.
          Assert.assertEquals(
              "expected the query-active gauge to stay 0 after commit/merge, before any query",
              0.0,
              meterRegistry.get(queryActiveGaugeName).gauge().value(),
              0.0);

          // Merge already engaged native scoring above, so the counter is already >0 here.
          // Comparing the post-query value against this baseline (rather than against 0) is what
          // isolates the query call site's own increment.
          double engagedAfterMerge = meterRegistry.get(metricName).counter().count();

          try (DirectoryReader reader = DirectoryReader.open(w)) {
            IndexSearcher searcher = new IndexSearcher(reader);
            float[] query = randomUnitVector(rng, dim);

            setNativeScoring(true);
            scoresById(searcher, query);
            double engagedWhileNative = meterRegistry.get(metricName).counter().count();
            Assert.assertTrue(
                "expected the query to increment the metric beyond the merge-time count, got "
                    + engagedAfterMerge
                    + " -> "
                    + engagedWhileNative,
                engagedWhileNative > engagedAfterMerge);
            Assert.assertEquals(
                "expected the query-active gauge to flip to 1 after a native query",
                1.0,
                meterRegistry.get(queryActiveGaugeName).gauge().value(),
                0.0);

            setNativeScoring(false);
            scoresById(searcher, query);
            double engagedWhileDelegate = meterRegistry.get(metricName).counter().count();
            Assert.assertEquals(
                "expected the metric to stay flat once native scoring is disabled",
                engagedWhileNative,
                engagedWhileDelegate,
                0.0);
            Assert.assertEquals(
                "expected the query-active gauge to stay at 1 after falling back to Java",
                1.0,
                meterRegistry.get(queryActiveGaugeName).gauge().value(),
                0.0);
          }
        }
      }
    } finally {
      GlobalMetricFactory.resetForTest();
    }
  }
}
