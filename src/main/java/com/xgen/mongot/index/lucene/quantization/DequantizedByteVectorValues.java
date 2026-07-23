package com.xgen.mongot.index.lucene.quantization;

import java.io.IOException;
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.VectorScorer;
import org.apache.lucene.util.quantization.QuantizedByteVectorValues;

/**
 * {@link ByteVectorValues} view over binary-quantized byte vectors that dequantizes each vector on
 * demand into its full-fidelity {@code byte[]} form. Used by the approximate rescore path to
 * compare the raw query vector against dequantized segment vectors.
 */
public class DequantizedByteVectorValues extends ByteVectorValues {

  private final QuantizedByteVectorValues quantizedVectorValues;
  private final VectorSimilarityFunction similarityFunction;

  /**
   * A reusable buffer that enables vectorValue() to skip array allocations. This is valid because
   * the contract of ByteVectorValues specifies that the caller is responsible for copying the
   * result of vectorValue() if it needs to reuse it.
   */
  private final byte[] scratchBuffer;

  public DequantizedByteVectorValues(
      QuantizedByteVectorValues quantizedVectorValues,
      VectorSimilarityFunction similarityFunction) {
    this.quantizedVectorValues = quantizedVectorValues;
    this.similarityFunction = similarityFunction;
    this.scratchBuffer = new byte[quantizedVectorValues.dimension()];
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
  public byte[] vectorValue(int ord) throws IOException {
    byte[] packedBits = this.quantizedVectorValues.vectorValue(ord);
    BinaryQuantizationUtils.dequantize(packedBits, this.scratchBuffer);
    return this.scratchBuffer;
  }

  @Override
  public ByteVectorValues copy() throws IOException {
    return new DequantizedByteVectorValues(
        this.quantizedVectorValues.copy(), this.similarityFunction);
  }

  @Override
  public VectorScorer scorer(byte[] query) throws IOException {
    DocIndexIterator itr = DequantizedByteVectorValues.this.quantizedVectorValues.iterator();
    return new VectorScorer() {

      @Override
      public float score() throws IOException {
        return DequantizedByteVectorValues.this.similarityFunction.compare(
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
