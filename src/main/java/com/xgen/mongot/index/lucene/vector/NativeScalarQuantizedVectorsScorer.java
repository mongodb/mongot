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
 *
 * <p>Both scorers also implement Lucene's {@code bulkScore}, which scores a batch of nodes in one
 * downcall via {@link Similarity#dotI8Bulk4} / {@link Similarity#l2I8Bulk4} instead of one downcall
 * and one {@link MemorySegment} slice per node. That path needs the whole file in a single mapping
 * and a native library exporting the bulk entry points; when either is missing it degrades to
 * per-node scoring, which is always available.
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
      // is unreachable in a normal mongot process. Non-bootstrap contexts that drive the codec
      // directly (e.g. standalone force-merge tooling) keep the flag's default-off behavior.
      return DynamicFeatureFlags.NATIVE_SCALAR_VECTOR_SCORING.getFallback();
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
   * Returns the whole-file {@link MemorySegment} backing {@code input}, or {@code null} if the file
   * spans more than one mapped segment. Bulk scoring addresses vectors as byte offsets into a
   * single base segment, so it is only available when the whole file is one mapping; {@link
   * #getSegment} handles the multi-segment case per vector and remains the fallback.
   */
  @Nullable
  private static MemorySegment wholeFileSegmentOrNull(MemorySegmentAccessInput input)
      throws IOException {
    return input.segmentSliceOrNull(0L, input.length());
  }

  /**
   * Rejects ordinals that would address outside the vector data.
   *
   * <p>Required because the bulk path passes raw byte offsets into the base segment. {@link
   * #getSegment} is bounds-checked for free by {@code segmentSliceOrNull}; raw offsets are not, so
   * an out-of-range ordinal would read out of bounds inside native code instead of throwing.
   */
  private static void checkOrdinals(int maxOrd, int n0, int n1, int n2, int n3) {
    if ((n0 | n1 | n2 | n3) < 0 || n0 >= maxOrd || n1 >= maxOrd || n2 >= maxOrd || n3 >= maxOrd) {
      throw new IllegalArgumentException(
          "illegal ordinal in bulk batch: ["
              + n0
              + ", "
              + n1
              + ", "
              + n2
              + ", "
              + n3
              + "] maxOrd="
              + maxOrd);
    }
  }

  /** Single-ordinal counterpart of {@link #checkOrdinals}, for the per-node remainder path. */
  private static void checkOrdinal(int maxOrd, int n) {
    if (n < 0 || n >= maxOrd) {
      throw new IllegalArgumentException("illegal ordinal: " + n + " maxOrd=" + maxOrd);
    }
  }

  /** True when {@code similarityFunction} scores from a squared L2 distance rather than a dot. */
  private static boolean usesL2(VectorSimilarityFunction similarityFunction) {
    return similarityFunction == VectorSimilarityFunction.EUCLIDEAN;
  }

  /**
   * Applies Lucene's scalar-quantized score formula to a raw native integer dot product / squared
   * distance. {@code constMultiplier} is the quantizer's alpha^2; {@code queryCorrection} and
   * {@code nodeCorrection} are the per-vector score-correction constants of the query and node
   * vectors, and are unused for {@code EUCLIDEAN}.
   *
   * <p>Both the per-vector and the bulk path funnel through here, which is what makes their scores
   * bit-identical rather than merely close.
   */
  static float applyCorrection(
      VectorSimilarityFunction similarityFunction,
      int raw,
      float constMultiplier,
      float queryCorrection,
      float nodeCorrection) {
    return switch (similarityFunction) {
      case EUCLIDEAN -> 1f / (1f + raw * constMultiplier);
      case DOT_PRODUCT, COSINE -> {
        float adjusted = raw * constMultiplier + queryCorrection + nodeCorrection;
        yield Math.max((1f + adjusted) / 2f, 0f);
      }
      case MAXIMUM_INNER_PRODUCT -> {
        float adjusted = raw * constMultiplier + queryCorrection + nodeCorrection;
        yield VectorUtil.scaleMaxInnerProductScore(adjusted);
      }
    };
  }

  /** Scores a single node against {@code query} with one native call. */
  static float nativeScore(
      VectorSimilarityFunction similarityFunction,
      MemorySegment query,
      MemorySegment node,
      float constMultiplier,
      float queryCorrection,
      float nodeCorrection) {
    int raw =
        usesL2(similarityFunction) ? Similarity.l2I8(query, node) : Similarity.dotI8(query, node);
    return applyCorrection(
        similarityFunction, raw, constMultiplier, queryCorrection, nodeCorrection);
  }

  /**
   * Lucene's default {@code bulkScore} body, used when the bulk fast path is unavailable (the file
   * spans multiple mappings, or the native library predates the bulk entry points).
   */
  private static float perNodeBulkScore(
      RandomVectorScorer scorer, int[] nodes, float[] scores, int numNodes) throws IOException {
    @Var float max = Float.NEGATIVE_INFINITY;
    for (int i = 0; i < numNodes; i++) {
      scores[i] = scorer.score(nodes[i]);
      max = Math.max(max, scores[i]);
    }
    return max;
  }

  /**
   * Bulk scoring shared by both Q7 scorers. One instance per scorer, because it owns a reusable
   * off-heap output buffer.
   *
   * <p>Worth the bookkeeping because Lucene calls {@code bulkScore} from {@code HnswGraphBuilder}
   * during merge and from the graph searchers at query time.
   */
  private static final class BulkScorer {
    private final VectorSimilarityFunction similarityFunction;
    private final MemorySegment base;
    private final MemorySegment scratch;
    private final int vectorByteLength;
    private final int stride;
    private final int maxOrd;
    private final float constMultiplier;

    BulkScorer(
        VectorSimilarityFunction similarityFunction,
        MemorySegment base,
        QuantizedByteVectorValues values,
        int vectorByteLength,
        int stride,
        float constMultiplier) {
      this.similarityFunction = similarityFunction;
      this.base = base;
      this.vectorByteLength = vectorByteLength;
      this.stride = stride;
      this.maxOrd = values.size();
      this.constMultiplier = constMultiplier;
      // Java 21 cannot pass a heap int[] across the FFM boundary, so the output buffer must be
      // off-heap. One per scorer, never per call; bulkScore is single-threaded per scorer.
      this.scratch =
          Arena.ofAuto().allocate((long) Similarity.BULK_FANOUT * Integer.BYTES, Integer.BYTES);
    }

    /**
     * The per-vector score-correction constant of {@code node}, read as the trailing 4 bytes of its
     * record in the whole-file {@link #base} segment. Byte-identical to {@code
     * values.getScoreCorrectionConstant(node)} -- that method seeks to this record's trailing float
     * and copies it out of the mapping into a heap {@code float[1]} scratch -- but reads the same
     * bytes in place, avoiding a per-vector segment-to-heap copy. Unaligned because the record's
     * trailing float inherits the data region's file offset, which is not 4-byte aligned.
     */
    private float correctionAt(int node) {
      return this.base.get(
          ValueLayout.JAVA_FLOAT_UNALIGNED, (long) node * this.stride + this.vectorByteLength);
    }

    /**
     * Bulk scores against another stored vector, addressed as a byte offset into the same base
     * segment as the data. Used during graph build.
     */
    float bulkScoreAgainstStored(
        long queryByteOffset, float queryCorrection, int[] nodes, float[] scores, int numNodes)
        throws IOException {
      return bulkScore(this.base, queryByteOffset, queryCorrection, nodes, scores, numNodes);
    }

    /**
     * Bulk scores against a query vector held in its own off-heap segment, quantized at query time
     * rather than read from the data file.
     */
    float bulkScoreAgainstQuery(
        MemorySegment query, float queryCorrection, int[] nodes, float[] scores, int numNodes)
        throws IOException {
      return bulkScore(query, 0L, queryCorrection, nodes, scores, numNodes);
    }

    /**
     * Scores {@code numNodes} nodes in full groups of {@link Similarity#BULK_FANOUT}, returning the
     * maximum score, or {@code Float.NEGATIVE_INFINITY} if {@code numNodes} is zero. A short final
     * group (1-3 nodes) goes through {@link #perNodeRemainder} rather than a padded bulk4 call --
     * see that method for why.
     */
    private float bulkScore(
        MemorySegment queryBase,
        long queryByteOffset,
        float queryCorrection,
        int[] nodes,
        float[] scores,
        int numNodes)
        throws IOException {
      int fanout = Similarity.BULK_FANOUT;
      int limit = numNodes - (numNodes % fanout);
      @Var float max = Float.NEGATIVE_INFINITY;
      @Var int i = 0;
      for (; i < limit; i += fanout) {
        max = Math.max(max, batch(queryBase, queryByteOffset, queryCorrection, nodes, scores, i));
      }
      int remaining = numNodes - i;
      if (remaining > 0) {
        max =
            Math.max(
                max,
                perNodeRemainder(
                    queryBase, queryByteOffset, queryCorrection, nodes, scores, i, remaining));
      }
      return max;
    }

    /**
     * Scores a short final group (1-3 nodes) via the single-vector native path instead of padding
     * to a full bulk4 call. {@code HnswGraphBuilder} caps a chunk at 8 nodes and commonly passes
     * 1-3, so this path is hot -- padding to 4 lanes would cost up to 4x the native work for what
     * {@code lanes} single-vector calls already do exactly.
     *
     * <p>Slices {@link #base} directly rather than going through {@code getSegment}: {@code
     * BulkScorer} only exists in the whole-file-mmap case, so the bytes read are identical either
     * way, and this avoids the multi-chunk copy fallback that {@code getSegment} carries for
     * callers without a whole-file segment.
     */
    private float perNodeRemainder(
        MemorySegment queryBase,
        long queryByteOffset,
        float queryCorrection,
        int[] nodes,
        float[] scores,
        int start,
        int lanes)
        throws IOException {
      MemorySegment query = queryBase.asSlice(queryByteOffset, this.vectorByteLength);
      @Var float max = Float.NEGATIVE_INFINITY;
      for (int k = 0; k < lanes; k++) {
        int node = nodes[start + k];
        checkOrdinal(this.maxOrd, node);
        MemorySegment nodeSeg = this.base.asSlice((long) node * this.stride, this.vectorByteLength);
        // EUCLIDEAN ignores both corrections, so skip the per-node read entirely there.
        float nodeCorrection = usesL2(this.similarityFunction) ? 0f : this.correctionAt(node);
        float score =
            nativeScore(
                this.similarityFunction,
                query,
                nodeSeg,
                this.constMultiplier,
                queryCorrection,
                nodeCorrection);
        scores[start + k] = score;
        max = Math.max(max, score);
      }
      return max;
    }

    /** Scores one full group of {@link Similarity#BULK_FANOUT} nodes via one native bulk4 call. */
    private float batch(
        MemorySegment queryBase,
        long queryByteOffset,
        float queryCorrection,
        int[] nodes,
        float[] scores,
        int start)
        throws IOException {
      int n0 = nodes[start];
      int n1 = nodes[start + 1];
      int n2 = nodes[start + 2];
      int n3 = nodes[start + 3];
      checkOrdinals(this.maxOrd, n0, n1, n2, n3);
      long d0 = (long) n0 * this.stride;
      long d1 = (long) n1 * this.stride;
      long d2 = (long) n2 * this.stride;
      long d3 = (long) n3 * this.stride;
      if (usesL2(this.similarityFunction)) {
        Similarity.l2I8Bulk4(
            queryBase,
            queryByteOffset,
            this.base,
            this.scratch,
            d0,
            d1,
            d2,
            d3,
            this.vectorByteLength);
      } else {
        Similarity.dotI8Bulk4(
            queryBase,
            queryByteOffset,
            this.base,
            this.scratch,
            d0,
            d1,
            d2,
            d3,
            this.vectorByteLength);
      }
      @Var float max = Float.NEGATIVE_INFINITY;
      for (int k = 0; k < Similarity.BULK_FANOUT; k++) {
        // EUCLIDEAN ignores both corrections, so skip the per-node read entirely there.
        float nodeCorrection =
            usesL2(this.similarityFunction) ? 0f : this.correctionAt(nodes[start + k]);
        float score =
            applyCorrection(
                this.similarityFunction,
                this.scratch.getAtIndex(ValueLayout.JAVA_INT, k),
                this.constMultiplier,
                queryCorrection,
                nodeCorrection);
        scores[start + k] = score;
        max = Math.max(max, score);
      }
      return max;
    }
  }

  /** Scores stored vectors against another stored (ordinal) vector; used during graph build. */
  private static final class NativeScalarScorerSupplier implements RandomVectorScorerSupplier {
    private final VectorSimilarityFunction similarityFunction;
    private final MemorySegmentAccessInput input;
    private final QuantizedByteVectorValues values;
    private final int vectorByteLength;
    private final int stride;
    private final float constMultiplier;

    /** Whole-file segment for the bulk path, or {@code null} when bulk is unavailable. */
    @Nullable private final MemorySegment base;

    NativeScalarScorerSupplier(
        VectorSimilarityFunction similarityFunction,
        MemorySegmentAccessInput input,
        QuantizedByteVectorValues values)
        throws IOException {
      this.similarityFunction = similarityFunction;
      this.input = input;
      this.values = values;
      this.vectorByteLength = values.getVectorByteLength();
      this.stride = this.vectorByteLength + Float.BYTES;
      this.constMultiplier = values.getScalarQuantizer().getConstantMultiplier();
      this.base = Similarity.I8_BULK_AVAILABLE ? wholeFileSegmentOrNull(input) : null;
    }

    @Override
    public UpdateableRandomVectorScorer scorer() throws IOException {
      return new AbstractUpdateableRandomVectorScorer(this.values) {
        @Nullable
        private final BulkScorer bulk =
            NativeScalarScorerSupplier.this.base == null
                ? null
                : new BulkScorer(
                    NativeScalarScorerSupplier.this.similarityFunction,
                    NativeScalarScorerSupplier.this.base,
                    NativeScalarScorerSupplier.this.values,
                    NativeScalarScorerSupplier.this.vectorByteLength,
                    NativeScalarScorerSupplier.this.stride,
                    NativeScalarScorerSupplier.this.constMultiplier);

        @Nullable private MemorySegment ordSeg;
        private long ordByteOffset;
        private float ordCorrection;
        private int scoringOrd = -1;

        @Override
        public void setScoringOrdinal(int ord) throws IOException {
          this.ordSeg =
              getSegment(
                  NativeScalarScorerSupplier.this.input,
                  ord,
                  NativeScalarScorerSupplier.this.stride,
                  NativeScalarScorerSupplier.this.vectorByteLength);
          this.ordByteOffset = (long) ord * NativeScalarScorerSupplier.this.stride;
          this.ordCorrection =
              NativeScalarScorerSupplier.this.values.getScoreCorrectionConstant(ord);
          this.scoringOrd = ord;
        }

        /**
         * Turns a missing {@link #setScoringOrdinal} into a clear failure rather than an NPE from
         * inside the FFM call (or, on the bulk path, a read from a null base).
         */
        private void requireScoringOrdinal() {
          if (this.scoringOrd < 0) {
            throw new IllegalStateException("setScoringOrdinal must be called before scoring");
          }
        }

        @SuppressWarnings("NullAway")
        @Override
        public float score(int node) throws IOException {
          requireScoringOrdinal();
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

        @Override
        public float bulkScore(int[] nodes, float[] scores, int numNodes) throws IOException {
          requireScoringOrdinal();
          if (this.bulk == null) {
            return perNodeBulkScore(this, nodes, scores, numNodes);
          }
          return this.bulk.bulkScoreAgainstStored(
              this.ordByteOffset, this.ordCorrection, nodes, scores, numNodes);
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

    /** Bulk fast path, or {@code null} when unavailable; see {@link #wholeFileSegmentOrNull}. */
    @Nullable private final BulkScorer bulk;

    NativeScalarQueryScorer(
        VectorSimilarityFunction similarityFunction,
        MemorySegmentAccessInput input,
        QuantizedByteVectorValues values,
        MemorySegment query,
        float queryCorrection)
        throws IOException {
      super(values);
      this.similarityFunction = similarityFunction;
      this.input = input;
      this.values = values;
      this.vectorByteLength = values.getVectorByteLength();
      this.stride = this.vectorByteLength + Float.BYTES;
      this.constMultiplier = values.getScalarQuantizer().getConstantMultiplier();
      this.query = query;
      this.queryCorrection = queryCorrection;
      MemorySegment base = Similarity.I8_BULK_AVAILABLE ? wholeFileSegmentOrNull(input) : null;
      this.bulk =
          base == null
              ? null
              : new BulkScorer(
                  similarityFunction,
                  base,
                  values,
                  this.vectorByteLength,
                  this.stride,
                  this.constMultiplier);
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

    @Override
    public float bulkScore(int[] nodes, float[] scores, int numNodes) throws IOException {
      if (this.bulk == null) {
        return perNodeBulkScore(this, nodes, scores, numNodes);
      }
      return this.bulk.bulkScoreAgainstQuery(
          this.query, this.queryCorrection, nodes, scores, numNodes);
    }
  }
}
