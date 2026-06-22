package com.xgen.mongot.index.lucene.extension;

import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;

/**
 * Custom implementation of Lucene's FieldType class to not be bound by have Lucene's limit on
 * vector dimensions. Since KnnFieldType can only override getters (not internal private fields), we
 * create Lucene's FieldType from it to make sure internal fields are also set in case future Lucene
 * versions would access them directly in FieldType class
 */
class KnnFieldType extends FieldType {

  private final int dimension;
  private final VectorEncoding vectorEncoding;
  private final VectorSimilarityFunction similarity;

  public KnnFieldType(int dimension, VectorEncoding encoding, VectorSimilarityFunction similarity) {
    this.dimension = dimension;
    this.vectorEncoding = encoding;
    this.similarity = similarity;
  }

  @Override
  public int vectorDimension() {
    return this.dimension;
  }

  @Override
  public VectorEncoding vectorEncoding() {
    return this.vectorEncoding;
  }

  @Override
  public VectorSimilarityFunction vectorSimilarityFunction() {
    return this.similarity;
  }
}
