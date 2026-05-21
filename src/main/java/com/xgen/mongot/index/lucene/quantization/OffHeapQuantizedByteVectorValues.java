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

// NOTE(corecursion): This file copied from Lucene's OffHeapQuantizedByteVectorValues.java.

package com.xgen.mongot.index.lucene.quantization;

import java.io.IOException;
import java.nio.ByteBuffer;
import javax.annotation.Nullable;
import org.apache.lucene.codecs.hnsw.FlatVectorsScorer;
import org.apache.lucene.codecs.lucene95.OrdToDocDISIReaderConfiguration;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.VectorScorer;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.hnsw.RandomVectorScorer;
import org.apache.lucene.util.packed.DirectMonotonicReader;
import org.apache.lucene.util.quantization.QuantizedByteVectorValues;
import org.apache.lucene.util.quantization.ScalarQuantizer;

/**
 * Read the quantized vector values and their score correction values from the index input. This
 * supports both iterated and random access.
 */
// NOTE(corecursion): For this file, disable CheckStyle, since it was largely copied from Lucene.
@SuppressWarnings("checkstyle")
public abstract class OffHeapQuantizedByteVectorValues extends QuantizedByteVectorValues {

  /** The number of bits in a single vector without padding. */
  protected final int dimension;

  /** The compressed (packed) vector size in bytes. */
  protected final int numBytes;

  /** Number of bytes stored per vector (packed bits + padding + score correction) */
  protected final int byteSize;

  /** The number of vectors for this field. */
  protected final int size;

  @Nullable protected final ScalarQuantizer scalarQuantizer;
  protected final VectorSimilarityFunction similarityFunction;
  protected final FlatVectorsScorer vectorsScorer;

  protected final IndexInput slice;
  protected final byte[] binaryValue;
  protected final ByteBuffer byteBuffer;
  protected int lastOrd = -1;
  protected final float[] scoreCorrectionConstant = new float[1];

  @Override
  public abstract OffHeapQuantizedByteVectorValues copy() throws IOException;

  OffHeapQuantizedByteVectorValues(
      int dimension,
      int size,
      @Nullable ScalarQuantizer scalarQuantizer,
      VectorSimilarityFunction similarityFunction,
      FlatVectorsScorer vectorsScorer,
      IndexInput slice) {
    this.dimension = dimension;
    this.size = size;
    this.slice = slice;
    this.scalarQuantizer = scalarQuantizer;
    this.numBytes = BinaryQuantizationUtils.requiredBytes(dimension);
    this.byteSize = this.numBytes + Float.BYTES;
    this.byteBuffer = ByteBuffer.allocate(this.numBytes);
    this.binaryValue = this.byteBuffer.array();
    this.similarityFunction = similarityFunction;
    this.vectorsScorer = vectorsScorer;
  }

  @Override
  @Nullable
  public ScalarQuantizer getScalarQuantizer() {
    return this.scalarQuantizer;
  }

  @Override
  public int dimension() {
    return this.dimension;
  }

  @Override
  public int size() {
    return this.size;
  }

  @Override
  public byte[] vectorValue(int targetOrd) throws IOException {
    if (this.lastOrd == targetOrd) {
      return this.binaryValue;
    }
    this.slice.seek((long) targetOrd * this.byteSize);
    this.slice.readBytes(this.byteBuffer.array(), this.byteBuffer.arrayOffset(), this.numBytes);
    this.slice.readFloats(this.scoreCorrectionConstant, 0, 1);
    this.lastOrd = targetOrd;
    return this.binaryValue;
  }

  @Override
  public float getScoreCorrectionConstant(int targetOrd) throws IOException {
    if (this.lastOrd == targetOrd) {
      return this.scoreCorrectionConstant[0];
    }
    this.slice.seek(((long) targetOrd * this.byteSize) + this.numBytes);
    this.slice.readFloats(this.scoreCorrectionConstant, 0, 1);
    return this.scoreCorrectionConstant[0];
  }

  @Override
  public IndexInput getSlice() {
    return this.slice;
  }

  @Override
  public int getVectorByteLength() {
    return this.numBytes;
  }

  public static OffHeapQuantizedByteVectorValues load(
      OrdToDocDISIReaderConfiguration configuration,
      int dimension,
      int size,
      @Nullable ScalarQuantizer scalarQuantizer,
      VectorSimilarityFunction similarityFunction,
      FlatVectorsScorer vectorsScorer,
      long quantizedVectorDataOffset,
      long quantizedVectorDataLength,
      IndexInput vectorData)
      throws IOException {
    if (configuration.isEmpty()) {
      return new EmptyOffHeapVectorValues(dimension, similarityFunction, vectorsScorer);
    }
    IndexInput bytesSlice =
        vectorData.slice(
            "quantized-vector-data", quantizedVectorDataOffset, quantizedVectorDataLength);
    if (configuration.isDense()) {
      return new DenseOffHeapVectorValues(
          dimension, size, scalarQuantizer, similarityFunction, vectorsScorer, bytesSlice);
    } else {
      return new SparseOffHeapVectorValues(
          configuration,
          dimension,
          size,
          scalarQuantizer,
          vectorData,
          similarityFunction,
          vectorsScorer,
          bytesSlice);
    }
  }

  /**
   * Dense vector values that are stored off-heap. This is the most common case when every doc has a
   * vector.
   */
  public static class DenseOffHeapVectorValues extends OffHeapQuantizedByteVectorValues {

    public DenseOffHeapVectorValues(
        int dimension,
        int size,
        @Nullable ScalarQuantizer scalarQuantizer,
        VectorSimilarityFunction similarityFunction,
        FlatVectorsScorer vectorsScorer,
        IndexInput slice) {
      super(dimension, size, scalarQuantizer, similarityFunction, vectorsScorer, slice);
    }

    @Override
    public DenseOffHeapVectorValues copy() throws IOException {
      return new DenseOffHeapVectorValues(
          this.dimension,
          this.size,
          this.scalarQuantizer,
          this.similarityFunction,
          this.vectorsScorer,
          this.slice.clone());
    }

    @Override
    public Bits getAcceptOrds(Bits acceptDocs) {
      return acceptDocs;
    }

    @Override
    public DocIndexIterator iterator() {
      return createDenseIterator();
    }

    @Override
    public VectorScorer scorer(float[] target) throws IOException {
      DenseOffHeapVectorValues copy = copy();
      DocIndexIterator iterator = copy.iterator();
      RandomVectorScorer vectorScorer =
          this.vectorsScorer.getRandomVectorScorer(this.similarityFunction, copy, target);
      return new VectorScorer() {
        @Override
        public float score() throws IOException {
          return vectorScorer.score(iterator.index());
        }

        @Override
        public DocIdSetIterator iterator() {
          return iterator;
        }
      };
    }
  }

  private static class SparseOffHeapVectorValues extends OffHeapQuantizedByteVectorValues {
    private final DirectMonotonicReader ordToDoc;
    // dataIn was used to init a new IndexedDIS for #randomAccess()
    private final IndexInput dataIn;
    private final OrdToDocDISIReaderConfiguration configuration;

    public SparseOffHeapVectorValues(
        OrdToDocDISIReaderConfiguration configuration,
        int dimension,
        int size,
        @Nullable ScalarQuantizer scalarQuantizer,
        IndexInput dataIn,
        VectorSimilarityFunction similarityFunction,
        FlatVectorsScorer vectorsScorer,
        IndexInput slice)
        throws IOException {
      super(dimension, size, scalarQuantizer, similarityFunction, vectorsScorer, slice);
      this.configuration = configuration;
      this.dataIn = dataIn;
      this.ordToDoc = configuration.getDirectMonotonicReader(dataIn);
    }

    @Override
    public SparseOffHeapVectorValues copy() throws IOException {
      return new SparseOffHeapVectorValues(
          this.configuration,
          this.dimension,
          this.size,
          this.scalarQuantizer,
          this.dataIn,
          this.similarityFunction,
          this.vectorsScorer,
          this.slice.clone());
    }

    @Override
    public int ordToDoc(int ord) {
      return (int) this.ordToDoc.get(ord);
    }

    @Nullable
    @Override
    public Bits getAcceptOrds(Bits acceptDocs) {
      if (acceptDocs == null) {
        return null;
      }
      return new Bits() {
        @Override
        public boolean get(int index) {
          return acceptDocs.get(ordToDoc(index));
        }

        @Override
        public int length() {
          return SparseOffHeapVectorValues.this.size;
        }
      };
    }

    @Override
    public DocIndexIterator iterator() {
      return createSparseIterator();
    }

    @Override
    public VectorScorer scorer(float[] target) throws IOException {
      SparseOffHeapVectorValues copy = copy();
      DocIndexIterator iterator = copy.iterator();
      RandomVectorScorer vectorScorer =
          this.vectorsScorer.getRandomVectorScorer(this.similarityFunction, copy, target);
      return new VectorScorer() {
        @Override
        public float score() throws IOException {
          return vectorScorer.score(iterator.index());
        }

        @Override
        public DocIdSetIterator iterator() {
          return iterator;
        }
      };
    }
  }

  private static class EmptyOffHeapVectorValues extends OffHeapQuantizedByteVectorValues {

    @SuppressWarnings("NullAway")
    public EmptyOffHeapVectorValues(
        int dimension,
        VectorSimilarityFunction similarityFunction,
        FlatVectorsScorer vectorsScorer) {
      super(dimension, 0, new BinaryQuantizer(), similarityFunction, vectorsScorer, null);
    }

    @Override
    public int dimension() {
      return super.dimension();
    }

    @Override
    public int size() {
      return 0;
    }

    @Override
    public EmptyOffHeapVectorValues copy() {
      throw new UnsupportedOperationException();
    }

    @Override
    public byte[] vectorValue(int targetOrd) {
      throw new UnsupportedOperationException();
    }

    @Override
    public int ordToDoc(int ord) {
      throw new UnsupportedOperationException();
    }

    @Nullable
    @Override
    public Bits getAcceptOrds(Bits acceptDocs) {
      return null;
    }

    @Nullable
    @Override
    public VectorScorer scorer(float[] target) {
      return null;
    }

    @Override
    public DocIndexIterator iterator() {
      return createDenseIterator();
    }
  }
}
