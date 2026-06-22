package com.xgen.mongot.index.lucene.extension;

import com.xgen.mongot.index.definition.KnnVectorFieldDefinition;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;

/**
 * This class overrides Lucene's implementation in order to remove restrictive dimension limit. Our
 * own limit is enforced in {@link KnnVectorFieldDefinition}. See relevant discussions on github and
 * in the mailing list: https://github.com/apache/lucene/issues/11507
 * https://lists.apache.org/thread/pztxj14dtktd9c2fg5qppmxsybv8n4dd
 */
public class KnnFloatVectorField extends org.apache.lucene.document.KnnFloatVectorField {

  public KnnFloatVectorField(
      String name, float[] vector, VectorSimilarityFunction similarityFunction) {
    super(name, vector, createType(vector, similarityFunction));
  }

  private static FieldType createType(float[] vector, VectorSimilarityFunction similarityFunction) {
    var type =
        new FieldType(new KnnFieldType(vector.length, VectorEncoding.FLOAT32, similarityFunction));
    type.freeze();
    return type;
  }
}
