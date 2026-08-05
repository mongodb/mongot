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

// NOTE(corecursion): This file copied from Lucene's: Lucene99ScalarQuantizedVectorsFormat.java

package com.xgen.mongot.index.lucene.quantization;

import java.io.IOException;
import java.util.Optional;
import org.apache.lucene.backward_codecs.lucene99.Lucene99ScalarQuantizedVectorsFormat;
import org.apache.lucene.codecs.hnsw.DefaultFlatVectorScorer;
import org.apache.lucene.codecs.hnsw.FlatVectorsFormat;
import org.apache.lucene.codecs.hnsw.FlatVectorsReader;
import org.apache.lucene.codecs.hnsw.FlatVectorsWriter;
import org.apache.lucene.codecs.lucene99.Lucene99FlatVectorsFormat;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;

/**
 * Flat vectors format for binary-quantized vectors, supporting quantization, storage, and retrieval
 * of both {@code float32} and {@code byte} encoded vector fields.
 *
 * @lucene.experimental
 */
public class Mongot01042BinaryQuantizedFlatVectorsFormat extends FlatVectorsFormat {

  public static final String QUANTIZED_VECTOR_COMPONENT = "QVEC";

  public static final String NAME = "Mongot01042BinaryQuantizedFlatVectorsFormat";

  static final int VERSION_START = 0;
  static final int VERSION_ADD_BITS = 1;
  static final int VERSION_CURRENT = VERSION_ADD_BITS;
  static final String META_CODEC_NAME = "Mongot01042BinaryQuantizedFlatVectorsFormatMeta";
  static final String VECTOR_DATA_CODEC_NAME = "Mongot01042BinaryQuantizedFlatVectorsFormatData";
  static final String META_EXTENSION = "vemq";
  static final String VECTOR_DATA_EXTENSION = "veq";

  private final FlatVectorsFormat rawVectorFormat;

  /** The minimum confidence interval */
  private static final float MINIMUM_CONFIDENCE_INTERVAL = 0.9f;

  /** Dynamic confidence interval */
  public static final float DYNAMIC_CONFIDENCE_INTERVAL = 0f;

  /**
   * Note: the confidence interval is a legacy field carried over from {@link
   * Lucene99ScalarQuantizedVectorsFormat}; binary quantization is stateless and does not use it for
   * scoring.
   */
  final Optional<Float> confidenceInterval;

  final BinaryQuantizedFlatVectorsScorer flatVectorScorer;

  /** Constructs a format using the given graph construction parameters. */
  public Mongot01042BinaryQuantizedFlatVectorsFormat() {
    super(NAME);
    this.confidenceInterval = Optional.of(1f);

    this.flatVectorScorer = new BinaryQuantizedFlatVectorsScorer(DefaultFlatVectorScorer.INSTANCE);
    this.rawVectorFormat = new Lucene99FlatVectorsFormat(this.flatVectorScorer);
  }

  public static float calculateDefaultConfidenceInterval(int vectorDimension) {
    return Math.max(MINIMUM_CONFIDENCE_INTERVAL, 1f - (1f / (vectorDimension + 1)));
  }

  @Override
  public String toString() {
    return NAME
        + "(name="
        + NAME
        + ", confidenceInterval="
        + this.confidenceInterval
        + ", flatVectorScorer="
        + this.flatVectorScorer
        + ", rawVectorFormat="
        + this.rawVectorFormat
        + ")";
  }

  // [Changed from Lucene] changed from returning Lucene99ScalarQuantizedVectorsWriter
  @Override
  public FlatVectorsWriter fieldsWriter(SegmentWriteState state) throws IOException {
    return new Mongot01042BinaryQuantizedFlatVectorsWriter(
        state,
        this.confidenceInterval,
        this.rawVectorFormat.fieldsWriter(state),
        this.flatVectorScorer);
  }

  // [Changed from Lucene] changed from returning Lucene99ScalarQuantizedVectorsReader
  @Override
  public FlatVectorsReader fieldsReader(SegmentReadState state) throws IOException {
    return new Mongot01042BinaryQuantizedFlatVectorsReader(
        state, this.rawVectorFormat.fieldsReader(state), this.flatVectorScorer);
  }
}
