package com.xgen.mongot.index.definition;

import com.xgen.mongot.index.definition.quantization.VectorQuantization;

/**
 * Estimates resident vector memory bytes from field configuration (dimensions, quantization) rather
 * than from on-disk file sizes. Used as a fallback when the on-disk estimate cannot be measured,
 * e.g. when all vector data is packed into Lucene compound (.cfs) files.
 */
public final class VectorMemoryEstimator {
  // Empirical compression ratios from float32 to each quantized on-heap representation
  // (MongoDB's uint1 binary quantization and Lucene's int7 scalar quantization).
  private static final double BINARY_FACTOR = 1.0 / 24.0;
  private static final double SCALAR_FACTOR = 1.0 / 3.75;
  private static final double NONE_FACTOR = 1.0;

  private VectorMemoryEstimator() {}

  /** Estimated resident bytes for one vector field, given the field's live document count. */
  public static long estimateFieldBytes(
      int numDimensions, VectorQuantization quantization, long numDocs) {
    long fullFidelityBytes = numDocs * numDimensions * Float.BYTES;
    double factor =
        switch (quantization) {
          case BINARY -> BINARY_FACTOR;
          case SCALAR -> SCALAR_FACTOR;
          case NONE -> NONE_FACTOR;
        };
    return (long) Math.ceil(fullFidelityBytes * factor);
  }

  /** Sum of per-field estimates across every vector field in the mapping. */
  public static long estimateTotalBytes(VectorIndexFieldMapping mapping, long numDocs) {
    return mapping.fieldMap().values().stream()
        .filter(VectorIndexFieldDefinition::isVectorField)
        .map(VectorIndexFieldDefinition::asVectorField)
        .mapToLong(
            field ->
                estimateFieldBytes(
                    field.specification().numDimensions(),
                    field.specification().quantization(),
                    numDocs))
        .sum();
  }
}
