package com.xgen.mongot.index.lucene.vector;

import com.google.errorprone.annotations.Var;
import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlagRegistry;
import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlags;
import com.xgen.mongot.util.GlobalMetricFactory;
import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import org.apache.lucene.codecs.hnsw.FlatVectorsScorer;
import org.apache.lucene.codecs.hnsw.ScalarQuantizedVectorScorer;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.FilterIndexInput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.MemorySegmentAccessInput;
import org.apache.lucene.util.VectorUtil;
import org.apache.lucene.util.hnsw.RandomVectorScorer;
import org.apache.lucene.util.hnsw.RandomVectorScorerSupplier;
import org.apache.lucene.util.hnsw.UpdateableRandomVectorScorer;
import org.apache.lucene.util.hnsw.UpdateableRandomVectorScorer.AbstractUpdateableRandomVectorScorer;
import org.apache.lucene.util.quantization.QuantizedByteVectorValues;
import org.apache.lucene.util.quantization.ScalarQuantizer;
import org.jetbrains.annotations.Nullable;

/**
 * A {@link FlatVectorsScorer} that accelerates 7-bit scalar-quantized byte vector comparisons using
 * native SIMD routines via {@link Similarity}, applying Lucene's scalar-quantized score correction.
 * It operates directly on {@link MemorySegment} slices obtained from the underlying {@link
 * MemorySegmentAccessInput}, avoiding any copying on the hot path when the vector fits within a
 * single mapped segment.
 *
 * <p>The native path is taken only when the {@code NATIVE_SCALAR_VECTOR_SCORING} dynamic feature
 * flag (read from the process-global {@link DynamicFeatureFlagRegistry#getStatic()}) and {@link
 * Similarity#I8_ACCELERATION} are both true, the values are a 7-bit {@link
 * QuantizedByteVectorValues}, and its slice unwraps to a {@link MemorySegmentAccessInput}.
 * Otherwise it falls back to the delegate (Lucene's Java scalar scorer).
 *
 * <p>Q7 quantized values are unsigned in {@code [0, 127]}, so the existing signed {@code int8}
 * primitives ({@link Similarity#dotI8}, {@link Similarity#l2I8}) produce the same integer result
 * and are reused without new native kernels.
 */
public class NativeScalarQuantizedVectorsScorer implements FlatVectorsScorer {

  private static final byte SUPPORTED_BITS = 7;

  private final FlatVectorsScorer delegate;

  public NativeScalarQuantizedVectorsScorer(FlatVectorsScorer delegate) {
    this.delegate = delegate;
  }

  private static boolean nativeEnabled() {
    if (!Similarity.I8_ACCELERATION) {
      return false;
    }
    try {
      return DynamicFeatureFlagRegistry.getStatic()
          .evaluateClusterInvariant(DynamicFeatureFlags.NATIVE_SCALAR_VECTOR_SCORING);
    } catch (IllegalStateException e) {
      // The process-global registry is installed by the bootstrapper before any query runs, so this
      // is unreachable in a normal mongot process. Guard for non-bootstrap contexts that drive the
      // codec directly (e.g. standalone force-merge tooling): default to off and use the Java path.
      return false;
    }
  }

  @Override
  public RandomVectorScorerSupplier getRandomVectorScorerSupplier(
      VectorSimilarityFunction similarityFunction, KnnVectorValues vectorValues)
      throws IOException {
    if (nativeEnabled() && vectorValues instanceof QuantizedByteVectorValues qv) {
      MemorySegmentAccessInput input = nativeInput(qv);
      if (input != null) {
        checkInvariants(qv.size(), qv.getVectorByteLength(), input);
        GlobalMetricFactory.incrementNativeScalarVectorScoringEngaged();
        return new NativeScalarScorerSupplier(similarityFunction, input, qv);
      }
    }
    return this.delegate.getRandomVectorScorerSupplier(similarityFunction, vectorValues);
  }

  @Override
  public RandomVectorScorer getRandomVectorScorer(
      VectorSimilarityFunction similarityFunction, KnnVectorValues vectorValues, float[] target)
      throws IOException {
    if (target.length != vectorValues.dimension()) {
      throw new IllegalArgumentException(
          "vector query dimension: "
              + target.length
              + " differs from field dimension: "
              + vectorValues.dimension());
    }
    if (nativeEnabled() && vectorValues instanceof QuantizedByteVectorValues qv) {
      MemorySegmentAccessInput input = nativeInput(qv);
      if (input != null) {
        checkInvariants(qv.size(), qv.getVectorByteLength(), input);
        ScalarQuantizer scalarQuantizer = qv.getScalarQuantizer();
        byte[] targetBytes = new byte[target.length];
        // quantizeQuery handles the COSINE l2-normalization and returns the query-side correction.
        float queryCorrection =
            ScalarQuantizedVectorScorer.quantizeQuery(
                target, targetBytes, similarityFunction, scalarQuantizer);
        MemorySegment query = Similarity.prepareVector(Arena.ofAuto(), targetBytes);
        GlobalMetricFactory.incrementNativeScalarVectorScoringEngaged();
        GlobalMetricFactory.markNativeScalarVectorScoringQueryActive();
        return new NativeScalarQueryScorer(similarityFunction, input, qv, query, queryCorrection);
      }
    }
    return this.delegate.getRandomVectorScorer(similarityFunction, vectorValues, target);
  }

  @Override
  public RandomVectorScorer getRandomVectorScorer(
      VectorSimilarityFunction similarityFunction, KnnVectorValues vectorValues, byte[] target)
      throws IOException {
    return this.delegate.getRandomVectorScorer(similarityFunction, vectorValues, target);
  }

  /**
   * Returns the {@link MemorySegmentAccessInput} backing {@code qv} when it is a 7-bit quantized,
   * memory-segment-backed values instance; otherwise {@code null} (caller should delegate).
   */
  @Nullable
  private static MemorySegmentAccessInput nativeInput(QuantizedByteVectorValues qv) {
    ScalarQuantizer scalarQuantizer = qv.getScalarQuantizer();
    if (scalarQuantizer == null || scalarQuantizer.getBits() != SUPPORTED_BITS) {
      return null;
    }
    IndexInput slice = qv.getSlice();
    if (slice == null) {
      return null;
    }
    IndexInput unwrapped = FilterIndexInput.unwrapOnlyTest(slice);
    return unwrapped instanceof MemorySegmentAccessInput msInput ? msInput : null;
  }

  /**
   * Verifies {@code input} holds at least {@code maxOrd} records of stride {@code vectorByteLength
   * + Float.BYTES} (the extra float is the per-vector score correction). Can't reuse {@link
   * Lucene99NativeMemorySegmentVectorScorer#checkInvariants}, which bounds against {@code
   * vectorByteLength} alone and would under-count for this format.
   */
  static void checkInvariants(int maxOrd, int vectorByteLength, MemorySegmentAccessInput input) {
    long stride = (long) vectorByteLength + Float.BYTES;
    if (input.length() < stride * maxOrd) {
      throw new IllegalArgumentException("input length is less than expected vector data");
    }
  }

  /**
   * Returns a native {@link MemorySegment} view of the quantized bytes for the vector at {@code
   * ord}. Vectors are stored with stride {@code vectorByteLength + Float.BYTES} (the trailing float
   * is the per-vector score-correction constant), so the segment offset uses {@code stride} while
   * only {@code vectorByteLength} quantized bytes are sliced. Falls back to a copy when the input
   * spans multiple mapped segments.
   */
  static MemorySegment getSegment(
      MemorySegmentAccessInput input, int ord, int stride, int vectorByteLength)
      throws IOException {
    long offset = (long) ord * stride;
    @Var MemorySegment seg = input.segmentSliceOrNull(offset, vectorByteLength);
    if (seg != null) {
      return seg;
    }
    byte[] scratch = new byte[vectorByteLength];
    input.readBytes(offset, scratch, 0, vectorByteLength);
    Arena arena = Arena.ofAuto();
    seg = arena.allocate(vectorByteLength);
    MemorySegment.copy(scratch, 0, seg, ValueLayout.JAVA_BYTE, 0, vectorByteLength);
    return seg;
  }

  /**
   * Applies Lucene's scalar-quantized score formula to the native integer dot product / squared
   * distance. {@code constMultiplier} is the quantizer's alpha^2; {@code queryCorrection} and
   * {@code nodeCorrection} are the per-vector score-correction constants of the query and node
   * vectors.
   */
  static float nativeScore(
      VectorSimilarityFunction similarityFunction,
      MemorySegment query,
      MemorySegment node,
      float constMultiplier,
      float queryCorrection,
      float nodeCorrection) {
    return switch (similarityFunction) {
      case EUCLIDEAN -> 1f / (1f + Similarity.l2I8(query, node) * constMultiplier);
      case DOT_PRODUCT, COSINE -> {
        float adjusted =
            Similarity.dotI8(query, node) * constMultiplier + queryCorrection + nodeCorrection;
        yield Math.max((1f + adjusted) / 2f, 0f);
      }
      case MAXIMUM_INNER_PRODUCT -> {
        float adjusted =
            Similarity.dotI8(query, node) * constMultiplier + queryCorrection + nodeCorrection;
        yield VectorUtil.scaleMaxInnerProductScore(adjusted);
      }
    };
  }

  /** Scores stored vectors against another stored (ordinal) vector; used during graph build. */
  private static final class NativeScalarScorerSupplier implements RandomVectorScorerSupplier {
    private final VectorSimilarityFunction similarityFunction;
    private final MemorySegmentAccessInput input;
    private final QuantizedByteVectorValues values;
    private final int vectorByteLength;
    private final int stride;
    private final float constMultiplier;

    NativeScalarScorerSupplier(
        VectorSimilarityFunction similarityFunction,
        MemorySegmentAccessInput input,
        QuantizedByteVectorValues values) {
      this.similarityFunction = similarityFunction;
      this.input = input;
      this.values = values;
      this.vectorByteLength = values.getVectorByteLength();
      this.stride = this.vectorByteLength + Float.BYTES;
      this.constMultiplier = values.getScalarQuantizer().getConstantMultiplier();
    }

    @Override
    public UpdateableRandomVectorScorer scorer() throws IOException {
      return new AbstractUpdateableRandomVectorScorer(this.values) {
        @Nullable private MemorySegment ordSeg;
        private float ordCorrection;

        @Override
        public void setScoringOrdinal(int ord) throws IOException {
          this.ordSeg =
              getSegment(
                  NativeScalarScorerSupplier.this.input,
                  ord,
                  NativeScalarScorerSupplier.this.stride,
                  NativeScalarScorerSupplier.this.vectorByteLength);
          this.ordCorrection =
              NativeScalarScorerSupplier.this.values.getScoreCorrectionConstant(ord);
        }

        @SuppressWarnings("NullAway")
        @Override
        public float score(int node) throws IOException {
          MemorySegment nodeSeg =
              getSegment(
                  NativeScalarScorerSupplier.this.input,
                  node,
                  NativeScalarScorerSupplier.this.stride,
                  NativeScalarScorerSupplier.this.vectorByteLength);
          float nodeCorrection =
              NativeScalarScorerSupplier.this.values.getScoreCorrectionConstant(node);
          return nativeScore(
              NativeScalarScorerSupplier.this.similarityFunction,
              this.ordSeg,
              nodeSeg,
              NativeScalarScorerSupplier.this.constMultiplier,
              this.ordCorrection,
              nodeCorrection);
        }
      };
    }

    @Override
    public RandomVectorScorerSupplier copy() throws IOException {
      return new NativeScalarScorerSupplier(
          this.similarityFunction, this.input.clone(), this.values.copy());
    }

    @Override
    public String toString() {
      return "NativeScalarScorerSupplier(similarityFunction=" + this.similarityFunction + ")";
    }
  }

  /** Scores stored vectors against a float query vector quantized at query time. */
  private static final class NativeScalarQueryScorer
      extends RandomVectorScorer.AbstractRandomVectorScorer {
    private final VectorSimilarityFunction similarityFunction;
    private final MemorySegmentAccessInput input;
    private final QuantizedByteVectorValues values;
    private final int vectorByteLength;
    private final int stride;
    private final float constMultiplier;
    private final MemorySegment query;
    private final float queryCorrection;

    NativeScalarQueryScorer(
        VectorSimilarityFunction similarityFunction,
        MemorySegmentAccessInput input,
        QuantizedByteVectorValues values,
        MemorySegment query,
        float queryCorrection) {
      super(values);
      this.similarityFunction = similarityFunction;
      this.input = input;
      this.values = values;
      this.vectorByteLength = values.getVectorByteLength();
      this.stride = this.vectorByteLength + Float.BYTES;
      this.constMultiplier = values.getScalarQuantizer().getConstantMultiplier();
      this.query = query;
      this.queryCorrection = queryCorrection;
    }

    @Override
    public float score(int node) throws IOException {
      MemorySegment nodeSeg = getSegment(this.input, node, this.stride, this.vectorByteLength);
      float nodeCorrection = this.values.getScoreCorrectionConstant(node);
      return nativeScore(
          this.similarityFunction,
          this.query,
          nodeSeg,
          this.constMultiplier,
          this.queryCorrection,
          nodeCorrection);
    }
  }
}
