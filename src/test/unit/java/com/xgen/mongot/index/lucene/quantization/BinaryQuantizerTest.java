package com.xgen.mongot.index.lucene.quantization;

import org.apache.lucene.index.VectorSimilarityFunction;
import org.junit.Assert;
import org.junit.Test;

public class BinaryQuantizerTest {

  // This unit test is to check the behavior of various IEEE 754 floating-point constants.
  @Test
  public void quantize_unusualFloats_matchesLtSemantics() {
    BinaryQuantizer quantizer = BinaryQuantizer.INSTANCE;
    float[] input = {
      Float.NEGATIVE_INFINITY,
      Float.NaN,
      -Float.MAX_VALUE,
      -Float.MIN_VALUE,
      -0f,
      0f,
      Float.MIN_VALUE,
      Float.MAX_VALUE,
      Float.POSITIVE_INFINITY
    };
    byte[] output = new byte[input.length];

    quantizer.quantize(input, output, VectorSimilarityFunction.COSINE);

    Assert.assertArrayEquals(new byte[] {0, 0, 0, 0, 0, 0, 1, 1, 1}, output);
  }

  // This unit test is to check the behavior of the byte overload across various constants.
  @Test
  public void quantize_bytes_matchesGtZeroSemantics() {
    byte[] input = {Byte.MIN_VALUE, -1, 0, 1, (byte) 128L, (byte) -256L, Byte.MAX_VALUE};
    byte[] output = new byte[input.length];

    BinaryQuantizer.quantizeBytes(input, output);

    Assert.assertArrayEquals(new byte[] {0, 0, 0, 1, 0, 0, 1}, output);
  }
}
