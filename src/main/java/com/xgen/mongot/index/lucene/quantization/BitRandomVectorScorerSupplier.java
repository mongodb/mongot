package com.xgen.mongot.index.lucene.quantization;

import java.io.IOException;
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.util.hnsw.RandomVectorScorer;
import org.apache.lucene.util.hnsw.RandomVectorScorerSupplier;

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
  public RandomVectorScorer scorer(int ord) throws IOException {
    byte[] query = this.vectorValues1.vectorValue(ord);
    return new BitRandomVectorScorer(this.vectorValues2, query);
  }

  @Override
  public RandomVectorScorerSupplier copy() throws IOException {
    return new BitRandomVectorScorerSupplier(this.vectorValues.copy());
  }
}
