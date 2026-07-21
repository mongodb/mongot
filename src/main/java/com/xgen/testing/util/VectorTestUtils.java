package com.xgen.testing.util;

import java.io.IOException;
import java.util.random.RandomGenerator;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsReader;
import org.apache.lucene.codecs.perfield.PerFieldKnnVectorsFormat;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.util.VectorUtil;
import org.apache.lucene.util.quantization.QuantizedByteVectorValues;

public class VectorTestUtils {

  /** Create a random vector with floats in range [-1, 1]. */
  public static float[] createFloatVector(int dim) {
    float[] v = new float[dim];
    RandomGenerator rng = RandomGenerator.getDefault();

    for (int i = 0; i < v.length; ++i) {
      v[i] = rng.nextFloat(-1, 1);
    }

    return v;
  }

  /** Create a random byte vector with values in range [-128, 127]. */
  public static byte[] createByteVector(int dim) {
    byte[] v = new byte[dim];
    RandomGenerator rng = RandomGenerator.getDefault();

    for (int i = 0; i < v.length; ++i) {
      v[i] = (byte) rng.nextInt(Byte.MIN_VALUE, Byte.MAX_VALUE + 1);
    }

    return v;
  }

  /** Create a random unit vector. */
  public static float[] createUnitVector(int dim) {
    return VectorUtil.l2normalize(createFloatVector(dim));
  }

  /**
   * Builds a byte vector whose per-dimension sign encodes {@code id}: dimension j is positive iff
   * bit j of {@code id} is set. Binary quantization preserves sign, so the id survives quantization
   * and can be recovered from the stored vector via {@link #decodeId(byte[])} — letting a test
   * verify exactly which vector survived a merge. Requires {@code dim >= 32} to hold any int id.
   */
  public static byte[] idEncodingByteVector(int id, int dim) {
    byte[] v = new byte[dim];
    for (int j = 0; j < dim; j++) {
      v[j] = ((id >> j) & 1) == 1 ? (byte) 1 : (byte) -1;
    }
    return v;
  }

  /**
   * Float counterpart of {@link #idEncodingByteVector}; recovered via {@link #decodeId(float[])}.
   */
  public static float[] idEncodingFloatVector(int id, int dim) {
    float[] v = new float[dim];
    for (int j = 0; j < dim; j++) {
      v[j] = ((id >> j) & 1) == 1 ? 1f : -1f;
    }
    return v;
  }

  /** Inverse of {@link #idEncodingByteVector}: reads the id from the per-dimension signs. */
  public static int decodeId(byte[] vector) {
    int id = 0;
    for (int j = 0; j < vector.length; j++) {
      if (vector[j] > 0) {
        id |= 1 << j;
      }
    }
    return id;
  }

  /** Inverse of {@link #idEncodingFloatVector}: reads the id from the per-dimension signs. */
  public static int decodeId(float[] vector) {
    int id = 0;
    for (int j = 0; j < vector.length; j++) {
      if (vector[j] > 0f) {
        id |= 1 << j;
      }
    }
    return id;
  }

  /**
   * Gets the QuantizedByteVectorValues associated with a vector field.
   *
   * @throws IOException if the field does not exist or is not quantized.
   */
  public static QuantizedByteVectorValues getQuantizedReader(
      LeafReader reader, String luceneFieldName) throws IOException {
    // QuantizedByteVectorValues can't be instantiated other than by a CodecReader, so this
    // repeated casting is actually how Lucene tests QuantizedByteVectorValue classes internally.
    // This will break in future version of Lucene, so all access should be done through this method
    KnnVectorsReader knnVectorsReader = ((CodecReader) reader).getVectorReader();
    KnnVectorsReader forField =
        ((PerFieldKnnVectorsFormat.FieldsReader) knnVectorsReader).getFieldReader(luceneFieldName);
    return ((Lucene99HnswVectorsReader) forField).getQuantizedVectorValues(luceneFieldName);
  }
}
