package com.xgen.mongot.index.lucene.vector;

import com.google.errorprone.annotations.Var;
import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import org.apache.lucene.codecs.hnsw.FlatVectorsScorer;
import org.apache.lucene.codecs.lucene95.HasIndexSlice;
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.FilterIndexInput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.MemorySegmentAccessInput;
import org.apache.lucene.util.hnsw.RandomVectorScorer;
import org.apache.lucene.util.hnsw.RandomVectorScorerSupplier;
import org.apache.lucene.util.hnsw.UpdateableRandomVectorScorer;
import org.apache.lucene.util.hnsw.UpdateableRandomVectorScorer.AbstractUpdateableRandomVectorScorer;
import org.apache.lucene.util.quantization.QuantizedByteVectorValues;
import org.jetbrains.annotations.Nullable;

/**
 * A {@link FlatVectorsScorer} that accelerates byte vector comparisons using native SIMD routines
 * via the {@link Similarity} class. It operates directly on {@link MemorySegment} slices obtained
 * from the underlying {@link MemorySegmentAccessInput}, avoiding any copying on the hot path when
 * the vector fits within a single mapped segment. Falls back to the delegate scorer for float
 * vectors or when the {@link IndexInput} is not memory-segment backed.
 */
public class Lucene99NativeMemorySegmentVectorScorer implements FlatVectorsScorer {
  private final FlatVectorsScorer delegate;

  public Lucene99NativeMemorySegmentVectorScorer(FlatVectorsScorer delegate) {
    this.delegate = delegate;
  }

  @Override
  public RandomVectorScorerSupplier getRandomVectorScorerSupplier(
      VectorSimilarityFunction similarityFunction, KnnVectorValues vectorValues)
      throws IOException {
    if (vectorValues instanceof ByteVectorValues byteValues
        // Q7 values subtype ByteVectorValues but use a different stride/formula; they are
        // handled by NativeScalarQuantizedVectorsScorer, never here.
        && !(byteValues instanceof QuantizedByteVectorValues)
        && Similarity.I8_ACCELERATION) {
      if (byteValues instanceof HasIndexSlice hasSlice) {
        IndexInput slice = hasSlice.getSlice();
        if (slice != null) {
          IndexInput unwrapped = FilterIndexInput.unwrapOnlyTest(slice);
          if (unwrapped instanceof MemorySegmentAccessInput msInput) {
            checkInvariants(byteValues.size(), byteValues.getVectorByteLength(), unwrapped);
            return new NativeByteVectorScorerSupplier(similarityFunction, msInput, byteValues);
          }
          // The values expose an index slice but it is not memory-segment-backed, so we cannot
          // score them zero-copy. Keep Lucene's default scorer rather than broadening native
          // scoring to every non-mmap byte value.
          return this.delegate.getRandomVectorScorerSupplier(similarityFunction, vectorValues);
        }
      }
      // ByteVectorValues with no usable slice (for example HNSW graph build's
      // ByteVectorValues.fromBytes(...)) carry no mmap fast path. Score them natively too, copying
      // each vector into reused native segments, so graph build does not fall back to Lucene's
      // int8 -> int widening PanamaVectorUtilSupport.dotProductBody (a dominant CPU cost).
      return new InMemoryByteVectorScorerSupplier(similarityFunction, byteValues);
    }
    return this.delegate.getRandomVectorScorerSupplier(similarityFunction, vectorValues);
  }

  @Override
  public RandomVectorScorer getRandomVectorScorer(
      VectorSimilarityFunction similarityFunction, KnnVectorValues vectorValues, float[] target)
      throws IOException {
    return this.delegate.getRandomVectorScorer(similarityFunction, vectorValues, target);
  }

  @Override
  public RandomVectorScorer getRandomVectorScorer(
      VectorSimilarityFunction similarityFunction, KnnVectorValues vectorValues, byte[] target)
      throws IOException {
    if (target.length != vectorValues.dimension()) {
      throw new IllegalArgumentException(
          "vector query dimension: "
              + target.length
              + " differs from field dimension: "
              + vectorValues.dimension());
    }
    if (vectorValues instanceof ByteVectorValues byteValues
        && byteValues instanceof HasIndexSlice hasSlice
        // Q7 values subtype ByteVectorValues but use a different stride/formula; they are
        // handled by NativeScalarQuantizedVectorsScorer, never here.
        && !(byteValues instanceof QuantizedByteVectorValues)
        && Similarity.I8_ACCELERATION) {
      IndexInput slice = hasSlice.getSlice();
      if (slice != null) {
        IndexInput unwrapped = FilterIndexInput.unwrapOnlyTest(slice);
        if (unwrapped instanceof MemorySegmentAccessInput msInput) {
          checkInvariants(byteValues.size(), byteValues.getVectorByteLength(), unwrapped);
          return new NativeByteQueryScorer(similarityFunction, msInput, byteValues, target);
        }
      }
    }
    return this.delegate.getRandomVectorScorer(similarityFunction, vectorValues, target);
  }

  @Override
  public String toString() {
    return "Lucene99NativeMemorySegmentVectorScorer(delegate=" + this.delegate + ")";
  }

  static void checkInvariants(int maxOrd, int vectorByteLength, IndexInput input) {
    if (input.length() < (long) vectorByteLength * maxOrd) {
      throw new IllegalArgumentException("input length is less than expected vector data");
    }
  }

  /**
   * Returns a native {@link MemorySegment} view of vector at {@code ord} from {@code input}. If the
   * input can provide a zero-copy slice, that is returned directly. Otherwise the bytes are read
   * into {@code readBuffer} and copied into the pre-allocated native {@code scratch}.
   */
  static MemorySegment getSegment(MemorySegmentAccessInput input, int ord, int vectorByteLength)
      throws IOException {
    long offset = (long) ord * vectorByteLength;
    @Var MemorySegment seg = input.segmentSliceOrNull(offset, vectorByteLength);
    if (seg != null) {
      return seg;
    }
    // This only happens if the underlying input is mapped into multiple segments. This is unusual
    // on 64-bit systems as the input has to be larger than 16GB. The data _must_ be copied into
    // a native segment in Java 21 as Java arrays may not be passed across the FFM boundary (they
    // have an address of 0). This can be avoided in Java 25.
    byte[] scratch = new byte[vectorByteLength];
    input.readBytes(offset, scratch, 0, vectorByteLength);
    Arena arena = Arena.ofAuto();
    seg = arena.allocate(vectorByteLength);
    MemorySegment.copy(scratch, 0, seg, ValueLayout.JAVA_BYTE, 0, vectorByteLength);
    return seg;
  }

  private static final class NativeByteVectorScorerSupplier implements RandomVectorScorerSupplier {
    private final VectorSimilarityFunction similarityFunction;
    private final MemorySegmentAccessInput input;
    private final ByteVectorValues values;
    private final int vectorByteLength;

    NativeByteVectorScorerSupplier(
        VectorSimilarityFunction similarityFunction,
        MemorySegmentAccessInput input,
        ByteVectorValues values) {
      this.similarityFunction = similarityFunction;
      this.input = input;
      this.values = values;
      this.vectorByteLength = values.getVectorByteLength();
    }

    @Override
    public UpdateableRandomVectorScorer scorer() throws IOException {
      return new AbstractUpdateableRandomVectorScorer(this.values) {
        @Nullable private MemorySegment ordSeg;

        @Override
        public void setScoringOrdinal(int node) throws IOException {
          this.ordSeg =
              getSegment(
                  NativeByteVectorScorerSupplier.this.input,
                  node,
                  NativeByteVectorScorerSupplier.this.vectorByteLength);
        }

        @SuppressWarnings("NullAway")
        @Override
        public float score(int node) throws IOException {
          MemorySegment nodeSeg =
              getSegment(
                  NativeByteVectorScorerSupplier.this.input,
                  node,
                  NativeByteVectorScorerSupplier.this.vectorByteLength);
          return nativeScore(
              NativeByteVectorScorerSupplier.this.similarityFunction,
              this.ordSeg,
              nodeSeg,
              NativeByteVectorScorerSupplier.this.vectorByteLength);
        }
      };
    }

    @Override
    public RandomVectorScorerSupplier copy() throws IOException {
      return new NativeByteVectorScorerSupplier(
          this.similarityFunction, this.input.clone(), this.values.copy());
    }

    @Override
    public String toString() {
      return "NativeByteVectorScorerSupplier(similarityFunction=" + this.similarityFunction + ")";
    }
  }

  /**
   * A {@link RandomVectorScorerSupplier} for in-memory {@link ByteVectorValues} (no {@link
   * HasIndexSlice}), such as the list-backed values used while building an HNSW graph. Each vector
   * is copied into a reused native {@link MemorySegment} (Java byte[] cannot cross the FFM
   * boundary) and scored via {@link #nativeScore}, which dispatches to the accelerated {@link
   * Similarity} kernels -- avoiding Lucene's int8 -> int widening dot product on the graph build
   * hot path. The scratch segments are confined to a single scorer, so construction is not
   * thread-safe but each scorer is single-threaded.
   */
  private static final class InMemoryByteVectorScorerSupplier
      implements RandomVectorScorerSupplier {
    private final VectorSimilarityFunction similarityFunction;
    private final ByteVectorValues values;
    private final int vectorByteLength;

    InMemoryByteVectorScorerSupplier(
        VectorSimilarityFunction similarityFunction, ByteVectorValues values) {
      this.similarityFunction = similarityFunction;
      this.values = values;
      this.vectorByteLength = values.getVectorByteLength();
    }

    @Override
    public UpdateableRandomVectorScorer scorer() throws IOException {
      return new AbstractUpdateableRandomVectorScorer(this.values) {
        private final Arena arena = Arena.ofAuto();
        private final MemorySegment ordSeg =
            this.arena.allocate(InMemoryByteVectorScorerSupplier.this.vectorByteLength);
        private final MemorySegment nodeSeg =
            this.arena.allocate(InMemoryByteVectorScorerSupplier.this.vectorByteLength);

        @Override
        public void setScoringOrdinal(int node) throws IOException {
          copyInto(this.ordSeg, InMemoryByteVectorScorerSupplier.this.values.vectorValue(node));
        }

        @SuppressWarnings("NullAway")
        @Override
        public float score(int node) throws IOException {
          copyInto(this.nodeSeg, InMemoryByteVectorScorerSupplier.this.values.vectorValue(node));
          return nativeScore(
              InMemoryByteVectorScorerSupplier.this.similarityFunction,
              this.ordSeg,
              this.nodeSeg,
              InMemoryByteVectorScorerSupplier.this.vectorByteLength);
        }
      };
    }

    @Override
    public RandomVectorScorerSupplier copy() throws IOException {
      return new InMemoryByteVectorScorerSupplier(this.similarityFunction, this.values.copy());
    }

    @Override
    public String toString() {
      return "InMemoryByteVectorScorerSupplier(similarityFunction=" + this.similarityFunction + ")";
    }
  }

  /**
   * Copies {@code src} into the pre-allocated native segment {@code dst}, which is sized to {@code
   * vectorByteLength}. A shorter {@code src} is a silent contract violation: the trailing bytes of
   * the reused segment would retain stale values and be scored.
   */
  private static void copyInto(MemorySegment dst, byte[] src) {
    if (src.length != dst.byteSize()) {
      throw new IllegalArgumentException(
          "vector byte length "
              + src.length
              + " does not match native segment size "
              + dst.byteSize());
    }
    MemorySegment.copy(src, 0, dst, ValueLayout.JAVA_BYTE, 0, src.length);
  }

  private static final class NativeByteQueryScorer
      extends RandomVectorScorer.AbstractRandomVectorScorer {
    private final VectorSimilarityFunction similarityFunction;
    private final MemorySegmentAccessInput input;
    private final int vectorByteLength;
    private final MemorySegment query;

    NativeByteQueryScorer(
        VectorSimilarityFunction similarityFunction,
        MemorySegmentAccessInput input,
        ByteVectorValues values,
        byte[] target) {
      super(values);
      this.similarityFunction = similarityFunction;
      this.input = input;
      this.vectorByteLength = values.getVectorByteLength();
      this.query = Similarity.prepareVector(Arena.ofAuto(), target);
    }

    @Override
    public float score(int node) throws IOException {
      MemorySegment nodeSeg = getSegment(this.input, node, this.vectorByteLength);
      return nativeScore(this.similarityFunction, this.query, nodeSeg, this.vectorByteLength);
    }
  }

  static float nativeScore(
      VectorSimilarityFunction similarityFunction, MemorySegment a, MemorySegment b, int dim) {
    return switch (similarityFunction) {
      case EUCLIDEAN -> 1f / (1f + Similarity.l2I8(a, b));
      case DOT_PRODUCT -> 0.5f + Similarity.dotI8(a, b) / (float) (dim * (1 << 15));
      case COSINE -> (1f + Similarity.cosI8(a, b)) / 2f;
      case MAXIMUM_INNER_PRODUCT -> {
        float dot = Similarity.dotI8(a, b);
        yield dot < 0 ? 1f / (1f - dot) : dot + 1f;
      }
    };
  }
}
