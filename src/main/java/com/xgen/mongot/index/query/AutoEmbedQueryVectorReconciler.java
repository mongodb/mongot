package com.xgen.mongot.index.query;

import com.xgen.mongot.index.definition.quantization.VectorAutoEmbedQuantization;
import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.bson.Vector;

/**
 * Validates a user-supplied queryVector against the dtype the auto-embedding index stored its
 * embeddings in ({@code float}/{@code binary} → float32, {@code scalar} → int8, {@code
 * binaryNoRescore} → packedBit). Query routing is by vector type, so a mismatch would silently
 * search an empty Lucene field. Only the matching dtype is accepted; others are rejected rather
 * than converted, since mongot cannot reproduce the provider's quantization and widening int8 to
 * float distorts scoring (badly for euclidean). The fallback of supplying your own embedding always
 * produces the matching dtype, so this is not a limitation.
 */
public final class AutoEmbedQueryVectorReconciler {

  private AutoEmbedQueryVectorReconciler() {}

  /** Returns the queryVector if its dtype matches the indexed encoding, else throws. */
  public static Vector reconcile(
      Vector queryVector, VectorAutoEmbedQuantization quantization, FieldPath path)
      throws InvalidQueryException {
    Vector.VectorType expected = expectedVectorType(quantization);
    if (queryVector.getVectorType() == expected) {
      return queryVector;
    }
    throw new InvalidQueryException(
        String.format(
            "queryVector of type %s cannot be used with autoEmbed quantization '%s' on path '%s',"
                + " which indexes %s embeddings. Provide %s, or use 'query' with text so the"
                + " query embedding is generated automatically.",
            dtypeName(queryVector.getVectorType()),
            quantization.getName(),
            path,
            dtypeName(expected),
            expectedInput(quantization)));
  }

  private static Vector.VectorType expectedVectorType(VectorAutoEmbedQuantization quantization) {
    return switch (quantization) {
      case FLOAT, BINARY -> Vector.VectorType.FLOAT;
      case SCALAR -> Vector.VectorType.BYTE;
      case BINARY_NO_RESCORE -> Vector.VectorType.BIT;
    };
  }

  private static String expectedInput(VectorAutoEmbedQuantization quantization) {
    return switch (quantization) {
      case FLOAT, BINARY -> "an array of numbers or a BSON vector of type float32";
      case SCALAR -> "a BSON vector of type int8";
      case BINARY_NO_RESCORE -> "a BSON vector of type packedBit";
    };
  }

  /** BSON dtype vocabulary a client sees, not the internal enum name. */
  private static String dtypeName(Vector.VectorType type) {
    return switch (type) {
      case FLOAT -> "float32";
      case FLOAT16 -> "float16";
      case BYTE -> "int8";
      case BIT -> "packedBit";
    };
  }
}
