package com.xgen.mongot.index.lucene.quantization;

import java.io.IOException;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.VectorScorer;
import org.apache.lucene.util.quantization.QuantizedByteVectorValues;

/**
 * {@link FloatVectorValues} view over binary-quantized float vectors that dequantizes each vector
 * on demand into its full-fidelity {@code float[]} form. Used by the approximate rescore path to
 * compare the raw query vector against dequantized segment vectors.
 */
public class DequantizedFloatVectorValues extends FloatVectorValues {

  private final QuantizedByteVectorValues quantizedVectorValues;
  private final VectorSimilarityFunction similarityFunction;

  /**
   * A reusable buffer that enables vectorValue() to skip array allocations. This is valid because
   * the contract of FloatVectorValues specifies that the caller is responsible for copying the
   * result of vectorValue() if it needs to reuse it.
   */
  private final float[] scratchBuffer;

  public DequantizedFloatVectorValues(
      QuantizedByteVectorValues quantizedVectorValues,
      VectorSimilarityFunction similarityFunction) {
    this.quantizedVectorValues = quantizedVectorValues;
    this.similarityFunction = similarityFunction;
    this.scratchBuffer = new float[quantizedVectorValues.dimension()];
  }

  @Override
  public int dimension() {
    return this.quantizedVectorValues.dimension();
  }

  @Override
  public int size() {
    return this.quantizedVectorValues.size();
  }

  @Override
  public float[] vectorValue(int ord) throws IOException {
    byte[] packedBits = this.quantizedVectorValues.vectorValue(ord);
    BinaryQuantizationUtils.dequantize(packedBits, this.scratchBuffer);
    return this.scratchBuffer;
  }

  @Override
  public FloatVectorValues copy() throws IOException {
    return new DequantizedFloatVectorValues(
        this.quantizedVectorValues.copy(), this.similarityFunction);
  }

  @Override
  public VectorScorer scorer(float[] query) throws IOException {
    DocIndexIterator itr = DequantizedFloatVectorValues.this.quantizedVectorValues.iterator();
    return new VectorScorer() {

      @Override
      public float score() throws IOException {
        return DequantizedFloatVectorValues.this.similarityFunction.compare(
            query, vectorValue(itr.index()));
      }

      @Override
      public DocIdSetIterator iterator() {
        return itr;
      }
    };
  }

  @Override
  public DocIndexIterator iterator() {
    return this.quantizedVectorValues.iterator();
  }
}
