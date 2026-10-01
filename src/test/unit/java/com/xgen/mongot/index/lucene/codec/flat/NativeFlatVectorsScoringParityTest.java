package com.xgen.mongot.index.lucene.codec.flat;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;
import static com.xgen.mongot.index.lucene.vector.Similarity.I8_ACCELERATION;

import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlagConfig;
import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlagRegistry;
import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlags;
import com.xgen.mongot.index.definition.VectorFieldSpecification;
import com.xgen.mongot.index.lucene.vector.Lucene99NativeMemorySegmentVectorScorer;
import com.xgen.mongot.index.lucene.vector.NativeScalarQuantizedVectorsScorer;
import com.xgen.mongot.util.GlobalMetricFactory;
import com.xgen.testing.util.VectorTestUtils;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import org.apache.lucene.backward_codecs.lucene99.Lucene99Codec;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.KnnVectorsWriter;
import org.apache.lucene.codecs.hnsw.FlatVectorScorerUtil;
import org.apache.lucene.codecs.hnsw.FlatVectorsFormat;
import org.apache.lucene.codecs.lucene99.Lucene99FlatVectorsFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.KnnByteVectorField;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.KnnByteVectorQuery;
import org.apache.lucene.search.KnnFloatVectorQuery;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.util.hnsw.RandomVectorScorer;
import org.bson.types.ObjectId;
import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Verifies that native (int8 / Q7) scoring on the flat brute-force path produces results matching
 * Lucene's Java scorers.
 *
 * <p>Byte flat: an index written with {@link Float32AndByteFlatVectorsFormat} (native scorer wired
 * in place) is queried and its per-doc scores are compared against an identical index written with
 * a pure Java-scorer flat format; native engagement is asserted by constructing the scorer directly
 * over the on-disk values. Unflagged -- it engages whenever the platform is accelerated, like the
 * HNSW byte path.
 *
 * <p>Q7 flat: the same MMap-backed index is queried with {@code NATIVE_SCALAR_VECTOR_SCORING} ON
 * (native {@link NativeScalarQuantizedVectorsScorer}) and OFF (Lucene's Java scalar scorer);
 * per-document scores must match within tolerance, and native engagement per fn x dim is proven by
 * the engaged-metric counter (a flag-ON query must increment it, a flag-OFF query must not).
 *
 * <p>Readers are opened via {@link IndexWriter} with a test {@link Codec} that returns the flat
 * format directly from {@code knnVectorsFormat()}, so the query path resolves through the codec
 * rather than {@code KnnVectorsFormat.forName}.
 */
public class NativeFlatVectorsScoringParityTest {
  private static final long SEED = 0xdeadbeefL;
  private static final int NUM_VECTORS = 50;
  private static final int NUM_QUERIES = 8;
  private static final float DELTA = 1e-4f;
  private static final String FIELD = "vec";
  private static final String ID = "id";

  private static final int[] DIMS = {4, 16, 64, 128, 257, 1030};

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

  /** A test codec that returns {@code format} for every KNN field on both write and read. */
  private static final class FlatParityCodec extends FilterCodec {
    private final KnnVectorsFormat format;

    FlatParityCodec(String name, KnnVectorsFormat format) {
      super(name, new Lucene99Codec());
      this.format = format;
    }

    @Override
    public KnnVectorsFormat knnVectorsFormat() {
      return this.format;
    }
  }

  /**
   * Random bytes, forcing the last component non-zero so COSINE never scores a zero-norm vector.
   */
  private static byte[] randomByteVector(Random rng, int dim) {
    byte[] v = VectorTestUtils.createByteVector(rng, dim);
    v[dim - 1] = 1;
    return v;
  }

  private static void addByteVectorDocs(
      IndexWriter w, Random rng, int dim, VectorSimilarityFunction fn) throws IOException {
    for (int i = 0; i < NUM_VECTORS; i++) {
      Document doc = new Document();
      doc.add(new KnnByteVectorField(FIELD, randomByteVector(rng, dim), fn));
      doc.add(new StoredField(ID, Integer.toString(i)));
      w.addDocument(doc);
    }
  }

  private static void addFloatVectorDocs(
      IndexWriter w, Random rng, int dim, VectorSimilarityFunction fn) throws IOException {
    for (int i = 0; i < NUM_VECTORS; i++) {
      Document doc = new Document();
      doc.add(new KnnFloatVectorField(FIELD, VectorTestUtils.createUnitVector(rng, dim), fn));
      doc.add(new StoredField(ID, Integer.toString(i)));
      w.addDocument(doc);
    }
  }

  /** Maps stored id -> score for all hits of a {@link KnnByteVectorQuery}. */
  private static Map<Integer, Float> byteScoresById(IndexSearcher searcher, byte[] query)
      throws IOException {
    TopDocs topDocs =
        searcher.search(new KnnByteVectorQuery(FIELD, query, NUM_VECTORS), NUM_VECTORS);
    return scoresById(searcher, topDocs);
  }

  /** Maps stored id -> score for all hits of a {@link KnnFloatVectorQuery}. */
  private static Map<Integer, Float> floatScoresById(IndexSearcher searcher, float[] query)
      throws IOException {
    TopDocs topDocs =
        searcher.search(new KnnFloatVectorQuery(FIELD, query, NUM_VECTORS), NUM_VECTORS);
    return scoresById(searcher, topDocs);
  }

  private static Map<Integer, Float> scoresById(IndexSearcher searcher, TopDocs topDocs)
      throws IOException {
    StoredFields storedFields = searcher.getIndexReader().storedFields();
    Map<Integer, Float> byId = new HashMap<>();
    for (ScoreDoc sd : topDocs.scoreDocs) {
      int id = Integer.parseInt(storedFields.document(sd.doc).get(ID));
      byId.put(id, sd.score);
    }
    return byId;
  }

  /** Returns the on-disk {@link ByteVectorValues} for the single segment of {@code reader}. */
  private static ByteVectorValues byteVectorValues(DirectoryReader reader, String field)
      throws IOException {
    CodecReader codecReader = (CodecReader) reader.leaves().get(0).reader();
    ByteVectorValues values = codecReader.getVectorReader().getByteVectorValues(field);
    if (values == null) {
      throw new AssertionError("expected byte vector values for field " + field);
    }
    return values;
  }

  /**
   * A flat format using Lucene's stock Java scorer, used as the pure-Java reference in the byte
   * parity comparison ({@link Float32AndByteFlatVectorsFormat} now accelerates byte vectors
   * natively in place, so it can no longer serve as the Java reference itself).
   */
  private static KnnVectorsFormat javaByteReferenceFormat() {
    FlatVectorsFormat javaFormat =
        new Lucene99FlatVectorsFormat(FlatVectorScorerUtil.getLucene99FlatVectorsScorer());
    return new KnnVectorsFormat("javaByteReference") {
      @Override
      public KnnVectorsWriter fieldsWriter(SegmentWriteState state) throws IOException {
        return new FlatVectorsWriterDecorator(javaFormat.fieldsWriter(state));
      }

      @Override
      public KnnVectorsReader fieldsReader(SegmentReadState state) throws IOException {
        return new FullScanFlatVectorsReaderDecorator(javaFormat.fieldsReader(state));
      }

      @Override
      public int getMaxDimensions(String fieldName) {
        return VectorFieldSpecification.MAX_DIMENSIONS;
      }
    };
  }

  /**
   * Constructs the byte scorer directly from the on-disk values and asserts it is {@code
   * NativeByteQueryScorer} -- i.e. that the flat byte path really took the native scorer, not just
   * that its scores happen to match the Java path.
   */
  private static void assertNativeByteScorerUsed(
      VectorSimilarityFunction fn, ByteVectorValues values, byte[] query) throws IOException {
    RandomVectorScorer scorer =
        new Lucene99NativeMemorySegmentVectorScorer(
                FlatVectorScorerUtil.getLucene99FlatVectorsScorer())
            .getRandomVectorScorer(fn, values, query);
    assertWithMessage(
            "expected native scorer for fn=%s, got %s", fn, scorer.getClass().getSimpleName())
        .that(scorer.getClass().getSimpleName())
        .isEqualTo("NativeByteQueryScorer");
  }

  @Test
  public void flatByteScoring_matchesLuceneByteScorer() throws IOException {
    Assume.assumeTrue(
        "native mongot_vecsim library is required for this parity test", I8_ACCELERATION);

    for (VectorSimilarityFunction fn : VectorSimilarityFunction.values()) {
      // 1030 is above stock Lucene's 1024 cap and is a non-register-multiple tail size, so the SIMD
      // tail is exercised.
      for (int dim : DIMS) {
        Path dirPath = this.tempFolder.newFolder("byte_idx_" + fn + "_" + dim).toPath();
        // Identically seeded data so the native and Java indexes store byte-identical vectors.
        Random nativeRng = new Random(SEED);
        Random javaRng = new Random(SEED);
        Random queryRng = new Random(SEED + 1);
        List<Map<Integer, Float>> nativeScoresByQuery = new ArrayList<>(NUM_QUERIES);
        List<Map<Integer, Float>> javaScoresByQuery = new ArrayList<>(NUM_QUERIES);
        for (int q = 0; q < NUM_QUERIES; q++) {
          nativeScoresByQuery.add(null);
          javaScoresByQuery.add(null);
        }
        byte[][] queries = new byte[NUM_QUERIES][dim];
        for (int q = 0; q < NUM_QUERIES; q++) {
          queries[q] = randomByteVector(queryRng, dim);
        }

        try (MMapDirectory nativeDir = new MMapDirectory(dirPath.resolve("native"))) {
          try (IndexWriter w =
              new IndexWriter(
                  nativeDir,
                  new IndexWriterConfig()
                      .setCodec(
                          new FlatParityCodec(
                              "flatNative", new Float32AndByteFlatVectorsFormat())))) {
            addByteVectorDocs(w, nativeRng, dim, fn);
            w.commit();
            try (DirectoryReader reader = DirectoryReader.open(w)) {
              IndexSearcher searcher = new IndexSearcher(reader);
              ByteVectorValues values = byteVectorValues(reader, FIELD);
              for (int q = 0; q < NUM_QUERIES; q++) {
                assertNativeByteScorerUsed(fn, values, queries[q]);
                nativeScoresByQuery.set(q, byteScoresById(searcher, queries[q]));
              }
            }
          }
        }

        try (MMapDirectory javaDir = new MMapDirectory(dirPath.resolve("java"))) {
          try (IndexWriter w =
              new IndexWriter(
                  javaDir,
                  new IndexWriterConfig()
                      .setCodec(new FlatParityCodec("flatJava", javaByteReferenceFormat())))) {
            addByteVectorDocs(w, javaRng, dim, fn);
            w.commit();
            try (DirectoryReader reader = DirectoryReader.open(w)) {
              IndexSearcher searcher = new IndexSearcher(reader);
              for (int q = 0; q < NUM_QUERIES; q++) {
                javaScoresByQuery.set(q, byteScoresById(searcher, queries[q]));
              }
            }
          }
        }

        for (int q = 0; q < NUM_QUERIES; q++) {
          Map<Integer, Float> javaScores = javaScoresByQuery.get(q);
          Map<Integer, Float> nativeScores = nativeScoresByQuery.get(q);
          assertWithMessage("hit set differs fn=%s dim=%s query=%s", fn, dim, q)
              .that(nativeScores.keySet())
              .isEqualTo(javaScores.keySet());
          for (Map.Entry<Integer, Float> e : javaScores.entrySet()) {
            assertWithMessage("score differs fn=%s dim=%s id=%s", fn, dim, e.getKey())
                .that(nativeScores.get(e.getKey()))
                .isWithin(DELTA)
                .of(e.getValue());
          }
        }
      }
    }
  }

  @Test
  public void flatQ7Scoring_matchesLuceneScalarScorer() throws IOException {
    Assume.assumeTrue(
        "native mongot_vecsim library is required for this parity test", I8_ACCELERATION);

    // The flat Q7 reader does not expose its inner QuantizedByteVectorValues (surfacing it on the
    // reader decorator would be a product-surface change), so native engagement cannot be asserted
    // by scorer class in this loop. Prove it per fn x dim with the same engaged counter the
    // dedicated metric test uses: a flag-ON query must increment it, the following flag-OFF query
    // must not.
    String metricName = "native_scalar_vector_scoring_engaged";
    SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
    GlobalMetricFactory.resetForTest();
    GlobalMetricFactory.initialize(meterRegistry);
    try {
      Random rng = new Random(SEED);
      for (VectorSimilarityFunction fn : VectorSimilarityFunction.values()) {
        for (int dim : DIMS) {
          Path dirPath = this.tempFolder.newFolder("q7_idx_" + fn + "_" + dim).toPath();
          try (MMapDirectory dir = new MMapDirectory(dirPath)) {
            setNativeScoring(true);
            try (IndexWriter w =
                new IndexWriter(
                    dir,
                    new IndexWriterConfig()
                        .setCodec(
                            new FlatParityCodec(
                                "flatQ7", new ScalarQuantizedFlatVectorsFormat())))) {
              addFloatVectorDocs(w, rng, dim, fn);
              w.commit();

              try (DirectoryReader reader = DirectoryReader.open(w)) {
                IndexSearcher searcher = new IndexSearcher(reader);
                for (int q = 0; q < NUM_QUERIES; q++) {
                  float[] query = VectorTestUtils.createUnitVector(rng, dim);

                  double engagedBefore = meterRegistry.get(metricName).counter().count();
                  setNativeScoring(true);
                  Map<Integer, Float> nativeScores = floatScoresById(searcher, query);
                  double engagedAfterNative = meterRegistry.get(metricName).counter().count();
                  assertWithMessage("native scoring did not engage fn=%s dim=%s", fn, dim)
                      .that(engagedAfterNative)
                      .isGreaterThan(engagedBefore);

                  setNativeScoring(false);
                  Map<Integer, Float> luceneScores = floatScoresById(searcher, query);
                  assertWithMessage("flag-off query must not engage native fn=%s dim=%s", fn, dim)
                      .that(meterRegistry.get(metricName).counter().count())
                      .isEqualTo(engagedAfterNative);

                  assertWithMessage("hit set differs fn=%s dim=%s", fn, dim)
                      .that(nativeScores.keySet())
                      .isEqualTo(luceneScores.keySet());
                  for (Map.Entry<Integer, Float> e : luceneScores.entrySet()) {
                    assertWithMessage("score differs fn=%s dim=%s id=%s", fn, dim, e.getKey())
                        .that(nativeScores.get(e.getKey()))
                        .isWithin(DELTA)
                        .of(e.getValue());
                  }
                }
              }
            }
          }
        }
      }
    } finally {
      GlobalMetricFactory.resetForTest();
    }
  }

  /**
   * With the flag ON but a non-memory-mapped directory, the quantized slice cannot unwrap to a
   * {@link org.apache.lucene.store.MemorySegmentAccessInput}, so {@code nativeInput()} returns null
   * and the native scorer must delegate to Lucene's Java scalar scorer. The flag-on scores must
   * therefore equal the flag-off (Java) scores.
   */
  @Test
  public void flatQ7NonMemoryMappedInput_fallsBackToJava() throws IOException {
    Assume.assumeTrue("native mongot_vecsim library is required for this test", I8_ACCELERATION);

    VectorSimilarityFunction fn = VectorSimilarityFunction.COSINE;
    int dim = 64;
    Random rng = new Random(SEED);
    try (ByteBuffersDirectory dir = new ByteBuffersDirectory()) {
      setNativeScoring(true);
      try (IndexWriter w =
          new IndexWriter(
              dir,
              new IndexWriterConfig()
                  .setCodec(
                      new FlatParityCodec(
                          "flatQ7Fallback", new ScalarQuantizedFlatVectorsFormat())))) {
        addFloatVectorDocs(w, rng, dim, fn);
        w.commit();

        try (DirectoryReader reader = DirectoryReader.open(w)) {
          IndexSearcher searcher = new IndexSearcher(reader);
          float[] query = VectorTestUtils.createUnitVector(rng, dim);

          setNativeScoring(true);
          Map<Integer, Float> flagOnScores = floatScoresById(searcher, query);

          setNativeScoring(false);
          Map<Integer, Float> javaScores = floatScoresById(searcher, query);

          assertThat(flagOnScores.keySet()).isEqualTo(javaScores.keySet());
          for (Map.Entry<Integer, Float> e : javaScores.entrySet()) {
            assertWithMessage("score differs on non-mmap input id=%s", e.getKey())
                .that(flagOnScores.get(e.getKey()))
                .isWithin(DELTA)
                .of(e.getValue());
          }
        }
      }
    }
  }

  /**
   * Verifies {@link GlobalMetricFactory#incrementNativeScalarVectorScoringEngaged} actually fires
   * when the flat Q7 native path is taken, and stops firing once the flag is disabled -- this is
   * the counter's purpose: distinguishing "native scoring is genuinely engaged" from "the flag is
   * on but resolution silently fell back to Java".
   */
  @Test
  public void flatQ7Scoring_incrementsEngagedMetric() throws IOException {
    Assume.assumeTrue("native mongot_vecsim library is required for this test", I8_ACCELERATION);

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
                dir,
                new IndexWriterConfig()
                    .setCodec(
                        new FlatParityCodec(
                            "flatQ7Metric", new ScalarQuantizedFlatVectorsFormat())))) {
          addFloatVectorDocs(w, rng, dim, fn);
          w.commit();

          // Writing a flat index does not build an HNSW graph, so no scoring happens during
          // commit/merge and the engaged counter must still be 0 before the first query.
          assertWithMessage("committing a flat index must not score natively")
              .that(meterRegistry.get(metricName).counter().count())
              .isEqualTo(0.0);
          assertThat(meterRegistry.get(queryActiveGaugeName).gauge().value()).isEqualTo(0.0);

          try (DirectoryReader reader = DirectoryReader.open(w)) {
            IndexSearcher searcher = new IndexSearcher(reader);
            float[] query = VectorTestUtils.createUnitVector(rng, dim);

            setNativeScoring(true);
            floatScoresById(searcher, query);
            double engagedWhileNative = meterRegistry.get(metricName).counter().count();
            assertWithMessage("expected the query to increment the engaged metric")
                .that(engagedWhileNative)
                .isGreaterThan(0.0);
            assertThat(meterRegistry.get(queryActiveGaugeName).gauge().value()).isEqualTo(1.0);

            setNativeScoring(false);
            floatScoresById(searcher, query);
            double engagedWhileDelegate = meterRegistry.get(metricName).counter().count();
            assertThat(engagedWhileDelegate).isEqualTo(engagedWhileNative);
          }
        }
      }
    } finally {
      GlobalMetricFactory.resetForTest();
    }
  }
}
