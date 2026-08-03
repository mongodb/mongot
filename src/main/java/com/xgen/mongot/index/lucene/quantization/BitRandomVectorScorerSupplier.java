package com.xgen.mongot.index.lucene.quantization;

import com.xgen.mongot.index.lucene.util.VectorSearchUtil;
import java.io.IOException;
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.util.hnsw.RandomVectorScorerSupplier;
import org.apache.lucene.util.hnsw.UpdateableRandomVectorScorer;
import org.apache.lucene.util.hnsw.UpdateableRandomVectorScorer.AbstractUpdateableRandomVectorScorer;

public class BitRandomVectorScorerSupplier implements RandomVectorScorerSupplier {
  protected final ByteVectorValues vectorValues;
  protected final ByteVectorValues vectorValues1;
  protected final ByteVectorValues vectorValues2;

  public BitRandomVectorScorerSupplier(ByteVectorValues vectorValues) throws IOException {
    this.vectorValues = vectorValues.copy();
    this.vectorValues1 = vectorValues.copy();
    this.vectorValues2 = vectorValues.copy();
  }

  @Override
  public UpdateableRandomVectorScorer scorer() throws IOException {
    int bitDimensions = this.vectorValues2.dimension() * Byte.SIZE;
    // Each scorer copies the query vector out of vectorValues1's shared scratch buffer so that
    // scorers created from the same supplier cannot clobber each other's query. Sized by
    // getVectorByteLength(), not dimension(): for binary-quantized values the stored vectors are
    // bit-packed and shorter than the original dimension.
    byte[] query = new byte[this.vectorValues1.getVectorByteLength()];
    return new AbstractUpdateableRandomVectorScorer(this.vectorValues2) {
      @Override
      public void setScoringOrdinal(int node) throws IOException {
        System.arraycopy(
            BitRandomVectorScorerSupplier.this.vectorValues1.vectorValue(node),
            0,
            query,
            0,
            query.length);
      }

      @Override
      public float score(int node) throws IOException {
        return VectorSearchUtil.bitSimilarity(
            query,
            BitRandomVectorScorerSupplier.this.vectorValues2.vectorValue(node),
            bitDimensions);
      }
    };
  }

  @Override
  public RandomVectorScorerSupplier copy() throws IOException {
    return new BitRandomVectorScorerSupplier(this.vectorValues.copy());
  }
}
