package com.xgen.mongot.index.lucene.vector;

import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlagConfig;
import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlagRegistry;
import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlags;
import com.xgen.mongot.util.GlobalMetricFactory;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import org.apache.lucene.codecs.hnsw.DefaultFlatVectorScorer;
import org.apache.lucene.codecs.hnsw.FlatVectorsScorer;
import org.apache.lucene.codecs.lucene95.HasIndexSlice;
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.store.MemorySegmentAccessInput;
import org.apache.lucene.util.hnsw.RandomVectorScorer;
import org.apache.lucene.util.hnsw.RandomVectorScorerSupplier;
import org.apache.lucene.util.hnsw.UpdateableRandomVectorScorer;
import org.apache.lucene.util.quantization.QuantizedByteVectorValues;
import org.bson.types.ObjectId;
import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class Lucene99NativeMemorySegmentVectorScorerTest {
  private static final long SEED = 0xdeadbeefL;
  private static final int NUM_VECTORS = 8;
  private static final float DELTA = 1e-5f;

  @Rule public TemporaryFolder tempFolder = new TemporaryFolder();

  @After
  public void resetRegistry() {
    DynamicFeatureFlagRegistry.resetForTest();
    GlobalMetricFactory.resetForTest();
  }

  /** Installs a process-global registry with the native byte scoring flag in {@code phase}. */
  private static void installNativeByteScoringFlag(DynamicFeatureFlagConfig.Phase phase) {
    installRegistry(
        List.of(
            new DynamicFeatureFlagConfig(
                DynamicFeatureFlags.NATIVE_BYTE_VECTOR_SCORING.getName(),
                phase,
                List.of(),
                List.of(),
                100,
                DynamicFeatureFlagConfig.Scope.MONGOT_CLUSTER)));
  }

  private static void installRegistry(List<DynamicFeatureFlagConfig> configs) {
    DynamicFeatureFlagRegistry.resetForTest();
    DynamicFeatureFlagRegistry.initializeProcessInstance(
        new DynamicFeatureFlagRegistry(
            Optional.of(configs), Optional.empty(), Optional.empty(), Optional.of(new ObjectId())));
  }

  private static byte[] randomBytes(Random rng, int dim) {
    byte[] v = new byte[dim];
    rng.nextBytes(v);
    return v;
  }

  private static List<byte[]> randomVectors(Random rng, int count, int dim) {
    List<byte[]> vectors = new ArrayList<>(count);
    for (int i = 0; i < count; i++) {
      vectors.add(randomBytes(rng, dim));
    }
    return vectors;
  }

  private static List<float[]> randomFloatVectors(Random rng, int count, int dim) {
    List<float[]> vectors = new ArrayList<>(count);
    for (int i = 0; i < count; i++) {
      float[] v = new float[dim];
      for (int d = 0; d < dim; d++) {
        v[d] = rng.nextFloat();
      }
      vectors.add(v);
    }
    return vectors;
  }

  /**
   * Writes vectors packed end-to-end into an MMap-backed file and returns the open IndexInput. The
   * returned input implements {@link MemorySegmentAccessInput}. The MMap arena is tied to the
   * input, not the directory, so the directory may be closed immediately after opening.
   */
  private IndexInput writeVectors(List<byte[]> vectors, String filename) throws IOException {
    Path dirPath = this.tempFolder.newFolder(filename + "_dir").toPath();
    try (MMapDirectory dir = new MMapDirectory(dirPath);
        IndexOutput out = dir.createOutput(filename, IOContext.DEFAULT)) {
      for (byte[] v : vectors) {
        out.writeBytes(v, v.length);
      }
    }
    try (MMapDirectory dir = new MMapDirectory(dirPath)) {
      IndexInput input = dir.openInput(filename, IOContext.DEFAULT);
      Assert.assertTrue(
          "MMapDirectory must produce a MemorySegmentAccessInput",
          input instanceof MemorySegmentAccessInput);
      return input;
    }
  }

  /**
   * {@link ByteVectorValues} that also implements {@link HasIndexSlice}, pointing at a flat packed
   * {@link IndexInput} as the slice so the native scorer can obtain MemorySegments.
   */
  private static final class HasSliceByteVectorValues extends ByteVectorValues
      implements HasIndexSlice {
    private final List<byte[]> vectors;
    private final int dim;
    private final IndexInput slice;

    HasSliceByteVectorValues(List<byte[]> vectors, int dim, IndexInput slice) {
      this.vectors = vectors;
      this.dim = dim;
      this.slice = slice;
    }

    @Override
    public int size() {
      return this.vectors.size();
    }

    @Override
    public int dimension() {
      return this.dim;
    }

    @Override
    public byte[] vectorValue(int ord) {
      return this.vectors.get(ord);
    }

    @Override
    public ByteVectorValues copy() {
      return this;
    }

    @Override
    public KnnVectorValues.DocIndexIterator iterator() {
      return createDenseIterator();
    }

    @Override
    public IndexInput getSlice() {
      return this.slice;
    }
  }

  private static ByteVectorValues asHasIndexSlice(List<byte[]> vectors, int dim, IndexInput slice) {
    return new HasSliceByteVectorValues(vectors, dim, slice);
  }

  /**
   * Minimal {@link QuantizedByteVectorValues} (Q7) stand-in. {@code getSlice()} returns a real
   * MMap-backed {@link MemorySegmentAccessInput} so the only thing stopping the delegation test
   * below from taking the native path is the {@code instanceof QuantizedByteVectorValues} exclusion
   * -- not an incidentally-null slice, which the base class's default {@code getSlice()} would
   * otherwise supply. {@code getScalarQuantizer()} is unused: the exclusion check short-circuits
   * before it would be called.
   */
  private static final class FakeQuantizedByteVectorValues extends QuantizedByteVectorValues {
    private final List<byte[]> vectors;
    private final int dim;
    private final IndexInput slice;

    FakeQuantizedByteVectorValues(List<byte[]> vectors, int dim, IndexInput slice) {
      this.vectors = vectors;
      this.dim = dim;
      this.slice = slice;
    }

    @Override
    public int size() {
      return this.vectors.size();
    }

    @Override
    public int dimension() {
      return this.dim;
    }

    @Override
    public byte[] vectorValue(int ord) {
      return this.vectors.get(ord);
    }

    @Override
    public float getScoreCorrectionConstant(int ord) {
      return 0f;
    }

    @Override
    public KnnVectorValues.DocIndexIterator iterator() {
      return createDenseIterator();
    }

    @Override
    public IndexInput getSlice() {
      return this.slice;
    }
  }

  /** {@link FlatVectorsScorer} that records whether it was actually invoked. */
  private static final class SpyFlatVectorsScorer implements FlatVectorsScorer {
    boolean called;

    @Override
    public RandomVectorScorerSupplier getRandomVectorScorerSupplier(
        VectorSimilarityFunction fn, KnnVectorValues values) throws IOException {
      this.called = true;
      return DefaultFlatVectorScorer.INSTANCE.getRandomVectorScorerSupplier(fn, values);
    }

    @Override
    public RandomVectorScorer getRandomVectorScorer(
        VectorSimilarityFunction fn, KnnVectorValues values, float[] target) throws IOException {
      this.called = true;
      return DefaultFlatVectorScorer.INSTANCE.getRandomVectorScorer(fn, values, target);
    }

    @Override
    public RandomVectorScorer getRandomVectorScorer(
        VectorSimilarityFunction fn, KnnVectorValues values, byte[] target) throws IOException {
      this.called = true;
      return DefaultFlatVectorScorer.INSTANCE.getRandomVectorScorer(fn, values, target);
    }
  }

  @Test
  public void testScorerSupplierNativePath() throws IOException {
    for (VectorSimilarityFunction fn : VectorSimilarityFunction.values()) {
      Random rng = new Random(SEED);
      Lucene99NativeMemorySegmentVectorScorer scorer =
          new Lucene99NativeMemorySegmentVectorScorer(DefaultFlatVectorScorer.INSTANCE);

      for (int dim : new int[] {4, 16, 64, 128, 256}) {
        List<byte[]> vectors = randomVectors(rng, NUM_VECTORS, dim);
        try (IndexInput input = writeVectors(vectors, "sup_" + fn + "_" + dim)) {
          ByteVectorValues values = asHasIndexSlice(vectors, dim, input);
          RandomVectorScorerSupplier supplier = scorer.getRandomVectorScorerSupplier(fn, values);

          UpdateableRandomVectorScorer s = supplier.scorer();
          for (int ord = 0; ord < NUM_VECTORS; ord++) {
            s.setScoringOrdinal(ord);
            for (int node = 0; node < NUM_VECTORS; node++) {
              Assert.assertEquals(
                  "fn=" + fn + " dim=" + dim + " ord=" + ord + " node=" + node,
                  fn.compare(vectors.get(ord), vectors.get(node)),
                  s.score(node),
                  DELTA);
            }
          }
        }
      }
    }
  }

  @Test
  public void testScorerSupplierCopy() throws IOException {
    for (VectorSimilarityFunction fn : VectorSimilarityFunction.values()) {
      Random rng = new Random(SEED);
      Lucene99NativeMemorySegmentVectorScorer scorer =
          new Lucene99NativeMemorySegmentVectorScorer(DefaultFlatVectorScorer.INSTANCE);

      int dim = 64;
      List<byte[]> vectors = randomVectors(rng, NUM_VECTORS, dim);
      try (IndexInput input = writeVectors(vectors, "copy_" + fn)) {
        ByteVectorValues values = asHasIndexSlice(vectors, dim, input);
        RandomVectorScorerSupplier copy = scorer.getRandomVectorScorerSupplier(fn, values).copy();

        UpdateableRandomVectorScorer s = copy.scorer();
        for (int ord = 0; ord < NUM_VECTORS; ord++) {
          s.setScoringOrdinal(ord);
          for (int node = 0; node < NUM_VECTORS; node++) {
            Assert.assertEquals(
                "fn=" + fn + " ord=" + ord + " node=" + node,
                fn.compare(vectors.get(ord), vectors.get(node)),
                s.score(node),
                DELTA);
          }
        }
      }
    }
  }

  @Test
  public void testQueryScorerNativePath() throws IOException {
    for (VectorSimilarityFunction fn : VectorSimilarityFunction.values()) {
      Random rng = new Random(SEED);
      Lucene99NativeMemorySegmentVectorScorer scorer =
          new Lucene99NativeMemorySegmentVectorScorer(DefaultFlatVectorScorer.INSTANCE);

      for (int dim : new int[] {4, 16, 64, 128, 256}) {
        List<byte[]> vectors = randomVectors(rng, NUM_VECTORS, dim);
        try (IndexInput input = writeVectors(vectors, "qry_" + fn + "_" + dim)) {
          ByteVectorValues values = asHasIndexSlice(vectors, dim, input);

          for (int q = 0; q < NUM_VECTORS; q++) {
            byte[] query = vectors.get(q);
            RandomVectorScorer s = scorer.getRandomVectorScorer(fn, values, query);
            for (int node = 0; node < NUM_VECTORS; node++) {
              Assert.assertEquals(
                  "fn=" + fn + " dim=" + dim + " q=" + q + " node=" + node,
                  fn.compare(query, vectors.get(node)),
                  s.score(node),
                  DELTA);
            }
          }
        }
      }
    }
  }

  /**
   * In-memory byte values (for example HNSW graph build's {@code fromBytes}) do not implement
   * {@code HasIndexSlice}, but they must engage the native scorer via the copied-segment path
   * rather than falling back to the (slow) delegate, and scores must match Lucene's reference.
   */
  @Test
  public void scorerSupplier_fromBytes_usesNativeInMemoryPath() throws IOException {
    Assume.assumeTrue(
        "native in-memory path only engages when int8 SIMD acceleration is available",
        Similarity.I8_ACCELERATION);
    for (VectorSimilarityFunction fn : VectorSimilarityFunction.values()) {
      // Cover register-aligned (32, 1024) and tail (17) dimensions through copyInto + nativeScore.
      for (int dim : new int[] {32, 17, 1024}) {
        Random rng = new Random(SEED);
        SpyFlatVectorsScorer spyDelegate = new SpyFlatVectorsScorer();
        Lucene99NativeMemorySegmentVectorScorer scorer =
            new Lucene99NativeMemorySegmentVectorScorer(spyDelegate);

        List<byte[]> vectors = randomVectors(rng, NUM_VECTORS, dim);
        // fromBytes() does not implement HasIndexSlice — the native in-memory path must
        // still engage
        ByteVectorValues values = ByteVectorValues.fromBytes(vectors, dim);
        RandomVectorScorerSupplier supplier = scorer.getRandomVectorScorerSupplier(fn, values);

        Assert.assertFalse(
            "in-memory byte values must not fall through to the delegate", spyDelegate.called);

        UpdateableRandomVectorScorer s = supplier.scorer();
        for (int ord = 0; ord < NUM_VECTORS; ord++) {
          s.setScoringOrdinal(ord);
          for (int node = 0; node < NUM_VECTORS; node++) {
            Assert.assertEquals(
                "fn=" + fn + " dim=" + dim + " ord=" + ord + " node=" + node,
                fn.compare(vectors.get(ord), vectors.get(node)),
                s.score(node),
                DELTA);
          }
        }
      }
    }
  }

  /** An index slice that is not memory-segment-backed must still fall through to the delegate. */
  @Test
  public void scorerSupplier_hasIndexSlice_nonMemorySegmentInput_delegates() throws IOException {
    for (VectorSimilarityFunction fn : VectorSimilarityFunction.values()) {
      Random rng = new Random(SEED);
      SpyFlatVectorsScorer spyDelegate = new SpyFlatVectorsScorer();
      Lucene99NativeMemorySegmentVectorScorer scorer =
          new Lucene99NativeMemorySegmentVectorScorer(spyDelegate);

      int dim = 32;
      List<byte[]> vectors = randomVectors(rng, NUM_VECTORS, dim);
      try (ByteBuffersDirectory dir = new ByteBuffersDirectory()) {
        try (IndexOutput out = dir.createOutput("vecs", IOContext.DEFAULT)) {
          for (byte[] v : vectors) {
            out.writeBytes(v, v.length);
          }
        }
        try (IndexInput input = dir.openInput("vecs", IOContext.DEFAULT)) {
          Assert.assertFalse(
              "ByteBuffersDirectory input must not be a MemorySegmentAccessInput",
              input instanceof MemorySegmentAccessInput);
          ByteVectorValues values = asHasIndexSlice(vectors, dim, input);
          RandomVectorScorerSupplier supplier = scorer.getRandomVectorScorerSupplier(fn, values);

          Assert.assertTrue("a non-memory-segment-backed slice must delegate", spyDelegate.called);

          UpdateableRandomVectorScorer s = supplier.scorer();
          for (int ord = 0; ord < NUM_VECTORS; ord++) {
            s.setScoringOrdinal(ord);
            for (int node = 0; node < NUM_VECTORS; node++) {
              Assert.assertEquals(
                  "fn=" + fn + " ord=" + ord + " node=" + node,
                  fn.compare(vectors.get(ord), vectors.get(node)),
                  s.score(node),
                  DELTA);
            }
          }
        }
      }
    }
  }

  /** {@code supplier.copy()} must yield independent scorers that still match Lucene's reference. */
  @Test
  public void scorerSupplier_fromBytesCopy_remainsIndependent() throws IOException {
    Assume.assumeTrue(
        "native in-memory path only engages when int8 SIMD acceleration is available",
        Similarity.I8_ACCELERATION);
    Random rng = new Random(SEED);
    int dim = 32;
    List<byte[]> vectors = randomVectors(rng, NUM_VECTORS, dim);
    Lucene99NativeMemorySegmentVectorScorer scorer =
        new Lucene99NativeMemorySegmentVectorScorer(DefaultFlatVectorScorer.INSTANCE);
    ByteVectorValues values = ByteVectorValues.fromBytes(vectors, dim);
    RandomVectorScorerSupplier supplier =
        scorer.getRandomVectorScorerSupplier(VectorSimilarityFunction.DOT_PRODUCT, values);
    RandomVectorScorerSupplier copy = supplier.copy();

    UpdateableRandomVectorScorer original = supplier.scorer();
    UpdateableRandomVectorScorer copied = copy.scorer();
    for (int ord = 0; ord < NUM_VECTORS; ord++) {
      original.setScoringOrdinal(ord);
      copied.setScoringOrdinal(ord);
      for (int node = 0; node < NUM_VECTORS; node++) {
        Assert.assertEquals(
            "ord=" + ord + " node=" + node,
            VectorSimilarityFunction.DOT_PRODUCT.compare(vectors.get(ord), vectors.get(node)),
            copied.score(node),
            DELTA);
        Assert.assertEquals(
            "original and copied scorers must agree at ord=" + ord + " node=" + node,
            original.score(node),
            copied.score(node),
            DELTA);
      }
    }
  }

  /** Non-byte values must still fall through to the delegate scorer. */
  @Test
  public void scorerSupplier_floatValues_delegates() throws IOException {
    for (VectorSimilarityFunction fn : VectorSimilarityFunction.values()) {
      Random rng = new Random(SEED);
      SpyFlatVectorsScorer spyDelegate = new SpyFlatVectorsScorer();
      Lucene99NativeMemorySegmentVectorScorer scorer =
          new Lucene99NativeMemorySegmentVectorScorer(spyDelegate);

      int dim = 32;
      List<float[]> vectors = randomFloatVectors(rng, NUM_VECTORS, dim);
      FloatVectorValues values = FloatVectorValues.fromFloats(vectors, dim);
      RandomVectorScorerSupplier supplier = scorer.getRandomVectorScorerSupplier(fn, values);

      Assert.assertTrue("float values must delegate", spyDelegate.called);

      UpdateableRandomVectorScorer s = supplier.scorer();
      for (int ord = 0; ord < NUM_VECTORS; ord++) {
        s.setScoringOrdinal(ord);
        for (int node = 0; node < NUM_VECTORS; node++) {
          Assert.assertEquals(
              "fn=" + fn + " ord=" + ord + " node=" + node,
              fn.compare(vectors.get(ord), vectors.get(node)),
              s.score(node),
              DELTA);
        }
      }
    }
  }

  /**
   * The {@code instanceof QuantizedByteVectorValues} check is the only thing stopping Q7 from
   * taking this scorer's native path, which uses the wrong stride and formula for it. A regression
   * here wouldn't crash; scores would just be subtly wrong.
   */
  @Test
  public void testQuantizedByteVectorValuesDelegates() throws IOException {
    int dim = 32;
    Random rng = new Random(SEED);
    List<byte[]> vectors = randomVectors(rng, NUM_VECTORS, dim);
    SpyFlatVectorsScorer spyDelegate = new SpyFlatVectorsScorer();
    Lucene99NativeMemorySegmentVectorScorer scorer =
        new Lucene99NativeMemorySegmentVectorScorer(spyDelegate);
    try (IndexInput slice = writeVectors(vectors, "q7_delegate")) {
      QuantizedByteVectorValues qv = new FakeQuantizedByteVectorValues(vectors, dim, slice);

      scorer.getRandomVectorScorerSupplier(VectorSimilarityFunction.EUCLIDEAN, qv);
      Assert.assertTrue(
          "Q7 (QuantizedByteVectorValues) must delegate, never be natively scored",
          spyDelegate.called);

      spyDelegate.called = false;
      scorer.getRandomVectorScorer(VectorSimilarityFunction.EUCLIDEAN, qv, vectors.get(0));
      Assert.assertTrue(
          "Q7 (QuantizedByteVectorValues) must delegate, never be natively scored",
          spyDelegate.called);
    }
  }

  /**
   * Disabling the flag routes mmap, in-memory, and query scoring to the delegate, metrics flat at
   * 0.
   */
  @Test
  public void flagDisabled_allByteScoringDelegates() throws IOException {
    installNativeByteScoringFlag(DynamicFeatureFlagConfig.Phase.DISABLED);
    SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
    GlobalMetricFactory.resetForTest();
    GlobalMetricFactory.initialize(meterRegistry);
    int dim = 32;
    Random rng = new Random(SEED);
    List<byte[]> vectors = randomVectors(rng, NUM_VECTORS, dim);
    VectorSimilarityFunction fn = VectorSimilarityFunction.DOT_PRODUCT;
    SpyFlatVectorsScorer spyDelegate = new SpyFlatVectorsScorer();
    Lucene99NativeMemorySegmentVectorScorer scorer =
        new Lucene99NativeMemorySegmentVectorScorer(spyDelegate);

    try (IndexInput input = writeVectors(vectors, "flag_disabled")) {
      ByteVectorValues mmapValues = asHasIndexSlice(vectors, dim, input);
      scorer.getRandomVectorScorerSupplier(fn, mmapValues);
      Assert.assertTrue("mmap supplier must delegate when disabled", spyDelegate.called);

      spyDelegate.called = false;
      RandomVectorScorer queryScorer = scorer.getRandomVectorScorer(fn, mmapValues, vectors.get(0));
      Assert.assertTrue("query scorer must delegate when disabled", spyDelegate.called);
      Assert.assertEquals(fn.compare(vectors.get(0), vectors.get(1)), queryScorer.score(1), DELTA);
    }

    spyDelegate.called = false;
    scorer.getRandomVectorScorerSupplier(fn, ByteVectorValues.fromBytes(vectors, dim));
    Assert.assertTrue("in-memory supplier must delegate when disabled", spyDelegate.called);

    Assert.assertEquals(
        "engaged counter must stay at 0 when the flag is disabled",
        0.0,
        meterRegistry.find("native_byte_vector_scoring_engaged").counter().count(),
        0.0);
    Assert.assertEquals(
        "query-active gauge must stay at 0 when the flag is disabled",
        0.0,
        meterRegistry.find("native_byte_vector_scoring_query_active").gauge().value(),
        0.0);
  }

  /**
   * Enabled, or absent from a populated registry (fallback), keeps the native path and moves the
   * metrics.
   */
  @Test
  public void flagEnabledOrUnregistered_usesNativePath() throws IOException {
    Assume.assumeTrue(
        "native path only engages when int8 SIMD acceleration is available",
        Similarity.I8_ACCELERATION);
    SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
    GlobalMetricFactory.resetForTest();
    GlobalMetricFactory.initialize(meterRegistry);
    int dim = 32;
    Random rng = new Random(SEED);
    List<byte[]> vectors = randomVectors(rng, NUM_VECTORS, dim);
    VectorSimilarityFunction fn = VectorSimilarityFunction.DOT_PRODUCT;

    for (Runnable install :
        List.<Runnable>of(
            () -> installNativeByteScoringFlag(DynamicFeatureFlagConfig.Phase.ENABLED),
            () -> installRegistry(List.of()))) {
      install.run();
      SpyFlatVectorsScorer spyDelegate = new SpyFlatVectorsScorer();
      Lucene99NativeMemorySegmentVectorScorer scorer =
          new Lucene99NativeMemorySegmentVectorScorer(spyDelegate);
      try (IndexInput input = writeVectors(vectors, "flag_on_" + System.nanoTime())) {
        ByteVectorValues mmapValues = asHasIndexSlice(vectors, dim, input);
        scorer.getRandomVectorScorerSupplier(fn, mmapValues);
        scorer.getRandomVectorScorer(fn, mmapValues, vectors.get(0));
      }
      scorer.getRandomVectorScorerSupplier(fn, ByteVectorValues.fromBytes(vectors, dim));
      Assert.assertFalse("native path must not delegate", spyDelegate.called);
    }

    Counter engaged = meterRegistry.find("native_byte_vector_scoring_engaged").counter();
    Assert.assertNotNull("engaged counter must be registered", engaged);
    Assert.assertTrue("engaged counter must have moved on the native path", engaged.count() > 0.0);
    Assert.assertEquals(
        "query scorer construction must flip the query-active gauge",
        1.0,
        meterRegistry.find("native_byte_vector_scoring_query_active").gauge().value(),
        0.0);
  }
}
