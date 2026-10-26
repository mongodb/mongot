package com.xgen.mongot.index.lucene.util;

import com.xgen.mongot.index.definition.VectorSimilarity;
import com.xgen.mongot.index.version.IndexCapabilities;
import com.xgen.mongot.util.bson.FloatVector;
import com.xgen.mongot.util.bson.Vector;
import org.apache.lucene.util.VectorUtil;

/** Utility class for methods used in both vector indexing and search. */
public class VectorSearchUtil {

  private VectorSearchUtil() {
    // Do not instantiate
  }

  /**
   * Returns a similarity score of all bits in {@code x} and {@code y}
   *
   * <p>The score is computed as the negated Hamming distance scaled to the range [0, 1] where a
   * higher score implies a better match. This is equivalent to Euclidean distance for bit vectors,
   * but faster to compute.
   *
   * @param x - a bit vector densely packed into bytes, with zero padding
   * @param y - a bit vector densely packed into bytes, with zero padding
   * @param dim - the number of bits in the vectors (ignoring any padding)
   * @return a similarity score normalized to the range [0, 1]
   */
  public static float bitSimilarity(byte[] x, byte[] y, int dim) {
    return (dim - VectorUtil.xorBitCount(x, y)) / (float) dim;
  }

  /**
   * A vector paired with the similarity function it must be indexed or scored with. The two always
   * travel together: a normalized vector is only correct under the similarity returned with it.
   */
  public record NormalizedVector(Vector vector, VectorSimilarity similarity) {}

  /**
   * Pre-normalizes a vector for COSINE similarity when the index supports it, and returns it with
   * the similarity function to use at both index and query time.
   *
   * <p>When normalization applies (float32 vector, COSINE, {@link
   * IndexCapabilities#shouldNormalizeCosineVectors}), the result holds a normalized <b>copy</b> and
   * {@link VectorSimilarity#DOT_PRODUCT}. The argument is never mutated, so this is safe to call on
   * vectors shared across threads (query vectors shared by partition tasks, auto-embedding vectors
   * shared by documents with identical text). Otherwise the result holds {@code vector} itself,
   * uncopied, with {@code requestedSimilarity}.
   *
   * <p><b>Precondition:</b> callers must reject zero vectors first. A zero vector is returned
   * unchanged (l2normalize does not throw), but indexing or querying it under COSINE similarity
   * produces undefined results.
   *
   * @param vector - the vector to index or query
   * @param requestedSimilarity - The requested similarity function specified in the
   *     IndexDefinition, <b>not</b> a previously optimized one.
   * @param indexCapabilities - the IndexCapabilities for the index being indexed or queried
   */
  public static NormalizedVector normalizeIfNeeded(
      Vector vector, VectorSimilarity requestedSimilarity, IndexCapabilities indexCapabilities) {
    if (!shouldOptimize(vector.getVectorType(), requestedSimilarity, indexCapabilities)
        || !(vector instanceof FloatVector fv)) {
      return new NormalizedVector(vector, requestedSimilarity);
    }
    float[] copy = fv.getFloatVector().clone();
    VectorUtil.l2normalize(copy, false);
    return new NormalizedVector(
        Vector.fromFloats(copy, fv.getOriginalType()), VectorSimilarity.DOT_PRODUCT);
  }

  private static boolean shouldOptimize(
      Vector.VectorType vectorType,
      VectorSimilarity requestedSimilarity,
      IndexCapabilities indexCapabilities) {
    // Only normalize vectors on new indexes (version >= 5). We can't change the similarity
    // function on existing fields without breaking stored vector magnitudes.
    //
    // For scalar quantization: normalizing before quantization is safe and standard practice.
    // The quantization step sees unit-length vectors, which keeps quantile magnitudes consistent
    // with what the scorer expects. This is intentional.
    return vectorType == Vector.VectorType.FLOAT
        && requestedSimilarity == VectorSimilarity.COSINE
        && indexCapabilities.shouldNormalizeCosineVectors();
  }
}
