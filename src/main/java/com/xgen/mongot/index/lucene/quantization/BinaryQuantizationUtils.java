package com.xgen.mongot.index.lucene.quantization;

import com.google.errorprone.annotations.Var;

/**
 * This class holds utility functions that are shared between indexing and query path for both
 * pre-quantized and auto-quantized bit vectors.
 */
class BinaryQuantizationUtils {

  /** Returns the minimum number of bytes required to store `numBits` bits. */
  public static int requiredBytes(int numBits) {
    return (numBits + 7) >> 3; // pads the array end to a whole byte
  }

  /**
   * Compress a vector by packing bits. Bits will be packed in order left-to-right. The final byte
   * of the compressed vector will contain padding bits if `raw.length` isn't a multiple of 8.
   *
   * @param raw the original uncompressed vector.
   * @param compressed A buffer to store the compressed result. The buffer size must be exactly
   *     ceil(raw.length / 8)
   */
  static void compressBytes(byte[] raw, byte[] compressed) {
    if (requiredBytes(raw.length) != compressed.length) {
      throw new IllegalArgumentException(
          "decompressed length "
              + raw.length
              + " does not match compressed length "
              + compressed.length);
    }
    @Var byte output = 0;
    @Var int outputBits = 0;
    @Var int ci = 0; // index into the compressed array
    for (byte b : raw) { // index into the raw array
      output <<= 1;
      output |= b; // for uint1, all bits except the lowest bit are zero and can be ignored
      if (++outputBits == 8) {
        outputBits = 0;
        compressed[ci++] = output; // write one byte
      }
    }
    if (outputBits > 0) {
      compressed[ci] = (byte) (output << (8 - outputBits)); // write final padded byte
    }
  }

  /**
   * Dequantizes a BitVector by expanding each non-padding bit into +/-1f and storing it in its
   * correspond slot in `output`.
   *
   * <p>Usage Note: when dequantizing many vectors, it is good practice to create one `output`
   * buffer and reuse it for each call.
   *
   * @param packedBitsWithPadding a BitVector densely packed into a byte[] with padding occurring at
   *     the end of the final byte
   * @param output a float buffer to store the result in. The buffer must have exactly one element
   *     for every non-padding bit in `packedBits`. The number of padding bits will be inferred from
   *     {@code output.length - packedBitsWithPadding.length}
   */
  public static void dequantize(byte[] packedBitsWithPadding, float[] output) {
    int n = packedBitsWithPadding.length;

    int loopBound = output.length / 8;
    @Var int out = 0;
    @Var int i;
    // This loop is unrolled to encourage the jvm to process whole byte at a time. See jmh benchmark
    for (i = 0; i < n && i < loopBound; ++i, out += 8) {
      byte b = packedBitsWithPadding[i];
      // expand each bit of the byte into consecutive floats
      output[out] = (b & 0x80) != 0 ? 1f : -1f;
      output[out + 1] = (b & 0x40) != 0 ? 1f : -1f;
      output[out + 2] = (b & 0x20) != 0 ? 1f : -1f;
      output[out + 3] = (b & 0x10) != 0 ? 1f : -1f;
      output[out + 4] = (b & 0x08) != 0 ? 1f : -1f;
      output[out + 5] = (b & 0x04) != 0 ? 1f : -1f;
      output[out + 6] = (b & 0x02) != 0 ? 1f : -1f;
      output[out + 7] = (b & 0x01) != 0 ? 1f : -1f;
    }

    // Scalar tail for last byte in case dimension is not multiple of 8
    for (byte b = packedBitsWithPadding[n - 1]; out < output.length; ++out) {
      output[out] = b < 0 ? 1f : -1f;
      b <<= 1;
    }
  }
}
