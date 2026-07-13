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

// NOTE(corecursion): This file copied from Lucene's FlatBitVectorsScorer.java because
// org.apache.lucene.codecs.bitvectors hasn't been exported from its module by Lucene.
// See: lucene/core/src/java/module-info.java
// Some code from Lucene99ScalarQuantizedVectorScorer.java also has been copied here whenever that
// class works correctly but the (apparently unused) FlatBitVectorsScorer throws an exception.

package com.xgen.mongot.index.lucene.quantization;

import java.io.IOException;
import org.apache.lucene.codecs.hnsw.FlatVectorsScorer;
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.util.hnsw.RandomVectorScorer;
import org.apache.lucene.util.hnsw.RandomVectorScorerSupplier;
import org.apache.lucene.util.quantization.QuantizedByteVectorValues;
import org.apache.lucene.util.quantization.ScalarQuantizer;

/** A vector scorer for scoring auto-quantized binary vectors stored in densely packed bytes. */
// NOTE(corecursion): For this file, disable Error Prone, NullAway, and CheckStyle.
@SuppressWarnings("all")
public class BinaryQuantizedFlatVectorsScorer implements FlatVectorsScorer {
  private final FlatVectorsScorer nonQuantizedDelegate;

  BinaryQuantizedFlatVectorsScorer(FlatVectorsScorer nonQuantizedDelegate) {
    this.nonQuantizedDelegate = nonQuantizedDelegate;
  }

  @Override
  public RandomVectorScorerSupplier getRandomVectorScorerSupplier(
      VectorSimilarityFunction similarityFunction, KnnVectorValues vectorValues)
      throws IOException {
    // [Changed from Lucene] Commented out this incorrect assertion from dead Lucene code.
    //    assert vectorValues instanceof RandomAccessVectorValues.Bytes;
    if (vectorValues instanceof ByteVectorValues byteVectorValues) {
      return new BitRandomVectorScorerSupplier(byteVectorValues);
    }
    // [Changed from Lucene] Changed throw IllegalArgumentException here to return a supplier.
    return this.nonQuantizedDelegate.getRandomVectorScorerSupplier(
        similarityFunction, vectorValues);
  }

  // [Changed from Lucene] Copied this function from Lucene99ScalarQuantizedVectorScorer.
  @Override
  public RandomVectorScorer getRandomVectorScorer(
      VectorSimilarityFunction similarityFunction, KnnVectorValues vectorValues, float[] query)
      throws IOException {
    if (vectorValues instanceof QuantizedByteVectorValues quantizedByteVectorValues) {
      ScalarQuantizer scalarQuantizer = quantizedByteVectorValues.getScalarQuantizer();
      assert scalarQuantizer != null && scalarQuantizer.getBits() == 1;

      // Quantize and pack query vector before scoring segment
      byte[] quantizedQuery = new byte[query.length];
      scalarQuantizer.quantize(query, quantizedQuery, similarityFunction);
      byte[] packedBits = new byte[BinaryQuantizationUtils.requiredBytes(query.length)];
      BinaryQuantizationUtils.compressBytes(quantizedQuery, packedBits);

      return new BitRandomVectorScorer(quantizedByteVectorValues, packedBits);
    }
    return this.nonQuantizedDelegate.getRandomVectorScorer(similarityFunction, vectorValues, query);
  }

  @Override
  public RandomVectorScorer getRandomVectorScorer(
      VectorSimilarityFunction similarityFunction, KnnVectorValues vectorValues, byte[] query)
      throws IOException {
    if (vectorValues instanceof QuantizedByteVectorValues quantizedByteVectorValues) {
      ScalarQuantizer scalarQuantizer = quantizedByteVectorValues.getScalarQuantizer();
      assert scalarQuantizer != null && scalarQuantizer.getBits() == 1;

      // Quantize and pack query vector before scoring segment
      byte[] quantizedQuery = new byte[query.length];
      // ScalarQuantizer has no byte[] quantize overload; use the stateless static helper
      // instead of downcasting to BinaryQuantizer.
      BinaryQuantizer.quantizeBytes(query, quantizedQuery);
      byte[] packedBits = new byte[BinaryQuantizationUtils.requiredBytes(query.length)];
      BinaryQuantizationUtils.compressBytes(quantizedQuery, packedBits);

      return new BitRandomVectorScorer(quantizedByteVectorValues, packedBits);
    }
    return this.nonQuantizedDelegate.getRandomVectorScorer(similarityFunction, vectorValues, query);
  }

  @Override
  public String toString() {
    return "BinaryQuantizedFlatVectorsScorer()";
  }
}
