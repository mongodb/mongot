package com.xgen.mongot.index.lucene.quantization;

import org.apache.lucene.index.VectorSimilarityFunction;
import org.junit.Assert;
import org.junit.Test;

public class BinaryQuantizerTest {

  // This unit test is to check the behavior of various IEEE 754 floating-point constants.
  @Test
  public void quantize_unusualFloats_matchesLtSemantics() {
    BinaryQuantizer quantizer = new BinaryQuantizer();
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
}
