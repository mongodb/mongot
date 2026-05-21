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

public class BinaryQuantizer extends ScalarQuantizer {
  // Placeholder quantile values: quantiles are not used for binary quantization.
  // The ScalarQuantizer superclass computes scalar-quantization-specific parameters
  // from these values but they are unused here.
  static final float UNUSED_MIN_QUANTILE = 0f;
  static final float UNUSED_MAX_QUANTILE = 1f;

  public BinaryQuantizer() {
    super(UNUSED_MIN_QUANTILE, UNUSED_MAX_QUANTILE, (byte) 1);
  }

  @Override
  public float quantize(float[] src, byte[] dest, VectorSimilarityFunction similarityFunction) {
    assert src.length == dest.length;
    if (dest != null) {
      for (int i = 0; i < src.length; i++) {
        dest[i] = (src[i] > 0.0 ? (byte) 1 : (byte) 0);
      }
    }
    return 0f;
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
