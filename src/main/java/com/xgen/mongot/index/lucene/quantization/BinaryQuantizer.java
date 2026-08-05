/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// NOTE(corecursion): This file copied from Lucene's: ScalarQuantizer.java

package com.xgen.mongot.index.lucene.quantization;

import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.util.quantization.ScalarQuantizer;

/**
 * Will binary quantize float or byte vectors into 1-bit values. This is a lossy transformation.
 * Binary quantization works by capturing only the sign of each dimension. Unlike scalar
 * quantization, binary quantization is stateless: there is no confidence interval or quantiles to
 * calculate.
 */
public class BinaryQuantizer extends ScalarQuantizer {
  // Placeholder quantile values: quantiles are not used for binary quantization.
  // The ScalarQuantizer superclass computes scalar-quantization-specific parameters
  // from these values but they are unused here.
  static final float UNUSED_MIN_QUANTILE = 0f;
  static final float UNUSED_MAX_QUANTILE = 1f;

  /** Binary quantization is stateless, so a single shared instance serves all call sites. */
  public static final BinaryQuantizer INSTANCE = new BinaryQuantizer();

  private BinaryQuantizer() {
    super(UNUSED_MIN_QUANTILE, UNUSED_MAX_QUANTILE, (byte) 1);
  }

  @Override
  public float quantize(float[] src, byte[] dest, VectorSimilarityFunction similarityFunction) {
    if (dest != null) {
      assert src.length == dest.length;
      for (int i = 0; i < src.length; i++) {
        dest[i] = (src[i] > 0.0 ? (byte) 1 : (byte) 0);
      }
    }
    return 0f;
  }

  /**
   * Quantizes each byte-valued dimension into a single bit (1 if positive, else 0). Binary
   * quantization is stateless, so this is exposed as a static helper to avoid downcasting a {@link
   * ScalarQuantizer} at call sites.
   *
   * <p>note that this does not return a float correction constant because there is no correction
   * for binary quantization. Since the {@link #quantize} override from {@link ScalarQuantizer}
   * method is not used (and doesn't exist for bytes), passing 0 is not necessary.
   *
   * @param src source array of raw byte values
   * @param dest destination array of quantized bytes
   */
  public static void quantizeBytes(byte[] src, byte[] dest) {
    if (dest != null) {
      assert src.length == dest.length;
      for (int i = 0; i < src.length; i++) {
        dest[i] = (src[i] > 0 ? (byte) 1 : (byte) 0);
      }
    }
  }

  @Override
  public float recalculateCorrectiveOffset(
      byte[] quantizedVector,
      ScalarQuantizer oldQuantizer,
      VectorSimilarityFunction similarityFunction) {
    // Corrective offsets aren't necessary with single bit quantization, and they are not used for
    // scoring.
    return 0f;
  }
}
