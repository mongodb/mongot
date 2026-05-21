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

// NOTE(corecursion): This file copied from Lucene's: Lucene99ScalarQuantizedVectorsReader.java

package com.xgen.mongot.index.lucene.quantization;

import static org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsReader.readSimilarityFunction;
import static org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsReader.readVectorEncoding;

import com.google.errorprone.annotations.Var;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.hnsw.FlatVectorsReader;
import org.apache.lucene.codecs.hnsw.FlatVectorsScorer;
import org.apache.lucene.codecs.lucene95.OrdToDocDISIReaderConfiguration;
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.VectorScorer;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.ReadAdvice;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.hnsw.RandomVectorScorer;
import org.apache.lucene.util.quantization.QuantizedVectorsReader;
import org.apache.lucene.util.quantization.ScalarQuantizer;

/**
 * Reads Scalar Quantized vectors from the index segments along with index data structures.
 *
 * @lucene.experimental
 */
public final class Mongot01042BinaryQuantizedFlatVectorsReader extends FlatVectorsReader
    implements QuantizedVectorsReader {

  private static final long SHALLOW_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(Mongot01042BinaryQuantizedFlatVectorsReader.class);

  private final Map<String, FieldEntry> fields = new HashMap<>();
  private final IndexInput quantizedVectorData;
  private final FlatVectorsReader rawVectorsReader;
  private final FieldInfos fieldInfos;

  /**
   * Reads Scalar Quantized vectors from the index segments along with index data structures.
   *
   * @param state Quantized segment state.
   * @param rawVectorsReader Unquantized vector reader for forwarding to if needed.
   * @param scorer Quantized vector scorer.
   * @throws IOException exceptions
   */
  public Mongot01042BinaryQuantizedFlatVectorsReader(
      SegmentReadState state, FlatVectorsReader rawVectorsReader, FlatVectorsScorer scorer)
      throws IOException {
    super(scorer);
    this.rawVectorsReader = rawVectorsReader;
    this.fieldInfos = state.fieldInfos;
    @Var int versionMeta = -1;
    String metaFileName =
        IndexFileNames.segmentFileName(
            state.segmentInfo.name,
            state.segmentSuffix,
            Mongot01042BinaryQuantizedFlatVectorsFormat.META_EXTENSION);
    @Var boolean success = false;
    try (ChecksumIndexInput meta = state.directory.openChecksumInput(metaFileName)) {
      @Var Throwable priorE = null;
      try {
        versionMeta =
            CodecUtil.checkIndexHeader(
                meta,
                Mongot01042BinaryQuantizedFlatVectorsFormat.META_CODEC_NAME,
                Mongot01042BinaryQuantizedFlatVectorsFormat.VERSION_START,
                Mongot01042BinaryQuantizedFlatVectorsFormat.VERSION_CURRENT,
                state.segmentInfo.getId(),
                state.segmentSuffix);
        readFields(meta, versionMeta, state.fieldInfos);
      } catch (Throwable exception) {
        priorE = exception;
      } finally {
        CodecUtil.checkFooter(meta, priorE);
      }
      this.quantizedVectorData =
          openDataInput(
              state,
              versionMeta,
              Mongot01042BinaryQuantizedFlatVectorsFormat.VECTOR_DATA_EXTENSION,
              Mongot01042BinaryQuantizedFlatVectorsFormat.VECTOR_DATA_CODEC_NAME,
              // Quantized vectors are accessed randomly from their node ID stored in the HNSW
              // graph.
              state.context.withReadAdvice(ReadAdvice.RANDOM));
      success = true;
    } finally {
      if (success == false) {
        IOUtils.closeWhileHandlingException(this);
      }
    }
  }

  private void readFields(ChecksumIndexInput meta, int versionMeta, FieldInfos infos)
      throws IOException {
    for (int fieldNumber = meta.readInt(); fieldNumber != -1; fieldNumber = meta.readInt()) {
      FieldInfo info = infos.fieldInfo(fieldNumber);
      if (info == null) {
        throw new CorruptIndexException("Invalid field number: " + fieldNumber, meta);
      }
      FieldEntry fieldEntry = readField(meta, versionMeta, info);
      validateFieldEntry(info, fieldEntry);
      this.fields.put(info.name, fieldEntry);
    }
  }

  static void validateFieldEntry(FieldInfo info, FieldEntry fieldEntry) {
    int dimension = info.getVectorDimension();
    if (dimension != fieldEntry.dimension) {
      throw new IllegalStateException(
          "Inconsistent vector dimension for field=\""
              + info.name
              + "\"; "
              + dimension
              + " != "
              + fieldEntry.dimension);
    }

    // [Changed from Lucene] Changed the quantizedVectorBytes calculation to work for bits 1-7.
    int vectorBits = dimension;
    int vectorBytes = (vectorBits + 7) >> 3; // pad to a full byte
    long quantizedVectorBytes = vectorBytes + Float.BYTES;
    long numQuantizedVectorBytes = Math.multiplyExact(quantizedVectorBytes, fieldEntry.size);
    if (numQuantizedVectorBytes != fieldEntry.vectorDataLength) {
      throw new IllegalStateException(
          "Quantized vector data length "
              + fieldEntry.vectorDataLength
              + " not matching size="
              + fieldEntry.size
              + " * (dim="
              + dimension
              + " + 4)"
              + " = "
              + numQuantizedVectorBytes);
    }
  }

  @Override
  public void checkIntegrity() throws IOException {
    this.rawVectorsReader.checkIntegrity();
    CodecUtil.checksumEntireFile(this.quantizedVectorData);
  }

  private FieldEntry getFieldEntry(String field) {
    FieldInfo info = this.fieldInfos.fieldInfo(field);
    FieldEntry fieldEntry;
    if (info == null || (fieldEntry = this.fields.get(info.name)) == null) {
      throw new IllegalArgumentException("field=\"" + field + "\" not found");
    }
    if (fieldEntry.vectorEncoding != VectorEncoding.FLOAT32) {
      throw new IllegalArgumentException(
          "field=\""
              + field
              + "\" is encoded as: "
              + fieldEntry.vectorEncoding
              + " expected: "
              + VectorEncoding.FLOAT32);
    }
    return fieldEntry;
  }

  @Override
  public FloatVectorValues getFloatVectorValues(String field) throws IOException {
    FieldEntry fieldEntry = getFieldEntry(field);
    FloatVectorValues rawVectorValues = this.rawVectorsReader.getFloatVectorValues(field);
    OffHeapQuantizedByteVectorValues quantizedByteVectorValues =
        OffHeapQuantizedByteVectorValues.load(
            fieldEntry.ordToDoc,
            fieldEntry.dimension,
            fieldEntry.size,
            fieldEntry.scalarQuantizer,
            fieldEntry.similarityFunction,
            this.vectorScorer,
            fieldEntry.vectorDataOffset,
            fieldEntry.vectorDataLength,
            this.quantizedVectorData);
    return new QuantizedVectorValues(rawVectorValues, quantizedByteVectorValues);
  }

  @Override
  public ByteVectorValues getByteVectorValues(String field) throws IOException {
    return this.rawVectorsReader.getByteVectorValues(field);
  }

  private static IndexInput openDataInput(
      SegmentReadState state,
      int versionMeta,
      String fileExtension,
      String codecName,
      IOContext context)
      throws IOException {
    String fileName =
        IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, fileExtension);
    IndexInput in = state.directory.openInput(fileName, context);
    @Var boolean success = false;
    try {
      int versionVectorData =
          CodecUtil.checkIndexHeader(
              in,
              codecName,
              Mongot01042BinaryQuantizedFlatVectorsFormat.VERSION_START,
              Mongot01042BinaryQuantizedFlatVectorsFormat.VERSION_CURRENT,
              state.segmentInfo.getId(),
              state.segmentSuffix);
      if (versionMeta != versionVectorData) {
        throw new CorruptIndexException(
            "Format versions mismatch: meta="
                + versionMeta
                + ", "
                + codecName
                + "="
                + versionVectorData,
            in);
      }
      CodecUtil.retrieveChecksum(in);
      success = true;
      return in;
    } finally {
      if (success == false) {
        IOUtils.closeWhileHandlingException(in);
      }
    }
  }

  @Override
  @Nullable
  public RandomVectorScorer getRandomVectorScorer(String field, float[] target) throws IOException {
    FieldEntry fieldEntry = getFieldEntry(field);
    if (fieldEntry.scalarQuantizer == null) {
      return this.rawVectorsReader.getRandomVectorScorer(field, target);
    }
    OffHeapQuantizedByteVectorValues vectorValues =
        OffHeapQuantizedByteVectorValues.load(
            fieldEntry.ordToDoc,
            fieldEntry.dimension,
            fieldEntry.size,
            fieldEntry.scalarQuantizer,
            fieldEntry.similarityFunction,
            this.vectorScorer,
            fieldEntry.vectorDataOffset,
            fieldEntry.vectorDataLength,
            this.quantizedVectorData);
    return this.vectorScorer.getRandomVectorScorer(
        fieldEntry.similarityFunction, vectorValues, target);
  }

  @Override
  public RandomVectorScorer getRandomVectorScorer(String field, byte[] target) throws IOException {
    return this.rawVectorsReader.getRandomVectorScorer(field, target);
  }

  @Override
  public void close() throws IOException {
    IOUtils.close(this.quantizedVectorData, this.rawVectorsReader);
  }

  @Override
  public long ramBytesUsed() {
    @Var long size = SHALLOW_SIZE;
    size +=
        RamUsageEstimator.sizeOfMap(
            this.fields, RamUsageEstimator.shallowSizeOfInstance(FieldEntry.class));
    size += this.rawVectorsReader.ramBytesUsed();
    return size;
  }

  private FieldEntry readField(IndexInput input, int versionMeta, FieldInfo info)
      throws IOException {
    VectorEncoding vectorEncoding = readVectorEncoding(input);
    VectorSimilarityFunction similarityFunction = readSimilarityFunction(input);
    if (similarityFunction != info.getVectorSimilarityFunction()) {
      throw new IllegalStateException(
          "Inconsistent vector similarity function for field=\""
              + info.name
              + "\"; "
              + similarityFunction
              + " != "
              + info.getVectorSimilarityFunction());
    }
    return new FieldEntry(input, versionMeta, vectorEncoding, info.getVectorSimilarityFunction());
  }

  // [Changed from Lucene] Modified function to return OffHeapQuantizedByteVectorValues.
  @Override
  @Nullable
  public OffHeapQuantizedByteVectorValues getQuantizedVectorValues(String fieldName)
      throws IOException {
    FieldEntry fieldEntry = getFieldEntry(fieldName);
    return OffHeapQuantizedByteVectorValues.load(
        fieldEntry.ordToDoc,
        fieldEntry.dimension,
        fieldEntry.size,
        fieldEntry.scalarQuantizer,
        fieldEntry.similarityFunction,
        this.vectorScorer,
        fieldEntry.vectorDataOffset,
        fieldEntry.vectorDataLength,
        this.quantizedVectorData);
  }

  @Override
  @Nullable
  public ScalarQuantizer getQuantizationState(String fieldName) {
    FieldEntry fieldEntry = getFieldEntry(fieldName);
    return fieldEntry.scalarQuantizer;
  }

  private static class FieldEntry implements Accountable {
    private static final long SHALLOW_SIZE =
        RamUsageEstimator.shallowSizeOfInstance(FieldEntry.class);
    final VectorSimilarityFunction similarityFunction;
    final VectorEncoding vectorEncoding;
    final int dimension;
    final long vectorDataOffset;
    final long vectorDataLength;
    @Nullable final BinaryQuantizer scalarQuantizer;
    final int size;
    final OrdToDocDISIReaderConfiguration ordToDoc;

    // [Changed from Lucene] changed bits = 7 to bits = 1 multiple times in this constructor
    FieldEntry(
        IndexInput input,
        int versionMeta,
        VectorEncoding vectorEncoding,
        VectorSimilarityFunction similarityFunction)
        throws IOException {
      this.similarityFunction = similarityFunction;
      this.vectorEncoding = vectorEncoding;
      this.vectorDataOffset = input.readVLong();
      this.vectorDataLength = input.readVLong();
      this.dimension = input.readVInt();
      this.size = input.readInt();
      if (this.size > 0) {
        if (versionMeta < Mongot01042BinaryQuantizedFlatVectorsFormat.VERSION_ADD_BITS) {
          input.readInt(); // confidenceInterval, unused
          input.readInt(); // minQuantile, unused
          input.readInt(); // maxQuantile, unused
          this.scalarQuantizer = new BinaryQuantizer();
        } else {
          input.readInt(); // confidenceInterval, unused
          input.readByte(); // bits
          input.readByte(); // compress
          input.readInt(); // minQuantile, unused
          input.readInt(); // maxQuantile, unused
          this.scalarQuantizer = new BinaryQuantizer();
        }
      } else {
        this.scalarQuantizer = null;
      }
      this.ordToDoc = OrdToDocDISIReaderConfiguration.fromStoredMeta(input, this.size);
    }

    @Override
    public long ramBytesUsed() {
      return SHALLOW_SIZE;
    }
  }

  private static final class QuantizedVectorValues extends FloatVectorValues {
    private final FloatVectorValues rawVectorValues;
    private final OffHeapQuantizedByteVectorValues quantizedVectorValues;

    QuantizedVectorValues(
        FloatVectorValues rawVectorValues, OffHeapQuantizedByteVectorValues quantizedVectorValues) {
      this.rawVectorValues = rawVectorValues;
      this.quantizedVectorValues = quantizedVectorValues;
    }

    @Override
    public int dimension() {
      return this.rawVectorValues.dimension();
    }

    @Override
    public int size() {
      return this.rawVectorValues.size();
    }

    @Override
    public float[] vectorValue(int ord) throws IOException {
      return this.rawVectorValues.vectorValue(ord);
    }

    @Override
    public FloatVectorValues copy() throws IOException {
      return new QuantizedVectorValues(
          this.rawVectorValues.copy(), this.quantizedVectorValues.copy());
    }

    @Override
    public VectorScorer scorer(float[] query) throws IOException {
      return this.quantizedVectorValues.scorer(query);
    }

    @Override
    public DocIndexIterator iterator() {
      return this.rawVectorValues.iterator();
    }
  }
}
