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

// NOTE(corecursion): This file copied from Lucene's: Lucene99ScalarQuantizedVectorsWriter.java

package com.xgen.mongot.index.lucene.quantization;

import static com.xgen.mongot.index.lucene.quantization.Mongot01042BinaryQuantizedFlatVectorsFormat.DYNAMIC_CONFIDENCE_INTERVAL;
import static com.xgen.mongot.index.lucene.quantization.Mongot01042BinaryQuantizedFlatVectorsFormat.QUANTIZED_VECTOR_COMPONENT;
import static com.xgen.mongot.index.lucene.quantization.Mongot01042BinaryQuantizedFlatVectorsFormat.calculateDefaultConfidenceInterval;
import static org.apache.lucene.codecs.KnnVectorsWriter.MergedVectorValues.hasVectorValues;
import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;
import static org.apache.lucene.util.RamUsageEstimator.shallowSizeOfInstance;

import com.google.errorprone.annotations.Var;
import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import javax.annotation.Nullable;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.hnsw.FlatFieldVectorsWriter;
import org.apache.lucene.codecs.hnsw.FlatVectorsScorer;
import org.apache.lucene.codecs.hnsw.FlatVectorsWriter;
import org.apache.lucene.codecs.lucene95.OrdToDocDISIReaderConfiguration;
import org.apache.lucene.index.DocIDMerger;
import org.apache.lucene.index.DocsWithFieldSet;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.index.KnnVectorValues.DocIndexIterator;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.Sorter;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.VectorScorer;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.hnsw.CloseableRandomVectorScorerSupplier;
import org.apache.lucene.util.hnsw.RandomVectorScorer;
import org.apache.lucene.util.hnsw.RandomVectorScorerSupplier;
import org.apache.lucene.util.quantization.QuantizedByteVectorValues;
import org.apache.lucene.util.quantization.ScalarQuantizer;

/**
 * Writes quantized vector values and metadata to index segments.
 *
 * @lucene.experimental
 */
public final class Mongot01042BinaryQuantizedFlatVectorsWriter extends FlatVectorsWriter {

  ////////// NOTE(corecursion): Copied from: Lucene99FlatVectorsFormat.java
  static final int DIRECT_MONOTONIC_BLOCK_SHIFT = 16;

  private static final long SHALLOW_RAM_BYTES_USED =
      shallowSizeOfInstance(Mongot01042BinaryQuantizedFlatVectorsWriter.class);

  private final SegmentWriteState segmentWriteState;

  private final List<FieldWriter> fields = new ArrayList<>();
  private final IndexOutput meta;
  private final IndexOutput quantizedVectorData;
  private final Optional<Float> confidenceInterval;
  private final FlatVectorsWriter rawVectorDelegate;
  private final int version;
  private boolean finished;

  public Mongot01042BinaryQuantizedFlatVectorsWriter(
      SegmentWriteState state,
      Optional<Float> confidenceInterval,
      FlatVectorsWriter rawVectorDelegate,
      FlatVectorsScorer scorer)
      throws IOException {
    this(
        state,
        Mongot01042BinaryQuantizedFlatVectorsFormat.VERSION_START,
        confidenceInterval,
        rawVectorDelegate,
        scorer);
    if (confidenceInterval.isPresent() && confidenceInterval.get() == 0) {
      throw new IllegalArgumentException("confidenceInterval cannot be set to zero");
    }
  }

  private Mongot01042BinaryQuantizedFlatVectorsWriter(
      SegmentWriteState state,
      int version,
      Optional<Float> confidenceInterval,
      FlatVectorsWriter rawVectorDelegate,
      FlatVectorsScorer scorer)
      throws IOException {
    super(scorer);
    this.confidenceInterval = confidenceInterval;
    this.version = version;
    this.segmentWriteState = state;
    String metaFileName =
        IndexFileNames.segmentFileName(
            state.segmentInfo.name,
            state.segmentSuffix,
            Mongot01042BinaryQuantizedFlatVectorsFormat.META_EXTENSION);

    String quantizedVectorDataFileName =
        IndexFileNames.segmentFileName(
            state.segmentInfo.name,
            state.segmentSuffix,
            Mongot01042BinaryQuantizedFlatVectorsFormat.VECTOR_DATA_EXTENSION);
    this.rawVectorDelegate = rawVectorDelegate;
    @Var boolean success = false;
    try {
      this.meta = state.directory.createOutput(metaFileName, state.context);
      this.quantizedVectorData =
          state.directory.createOutput(quantizedVectorDataFileName, state.context);

      CodecUtil.writeIndexHeader(
          this.meta,
          Mongot01042BinaryQuantizedFlatVectorsFormat.META_CODEC_NAME,
          version,
          state.segmentInfo.getId(),
          state.segmentSuffix);
      CodecUtil.writeIndexHeader(
          this.quantizedVectorData,
          Mongot01042BinaryQuantizedFlatVectorsFormat.VECTOR_DATA_CODEC_NAME,
          version,
          state.segmentInfo.getId(),
          state.segmentSuffix);
      success = true;
    } finally {
      if (success == false) {
        IOUtils.closeWhileHandlingException(this);
      }
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public FlatFieldVectorsWriter<?> addField(FieldInfo fieldInfo) throws IOException {
    FlatFieldVectorsWriter<?> delegate = this.rawVectorDelegate.addField(fieldInfo);
    if (fieldInfo.getVectorEncoding().equals(VectorEncoding.FLOAT32)) {
      // [Changed from Lucene] removed a check for even dimension from here, no longer required
      FieldWriter quantizedWriter =
          new FieldWriter(fieldInfo, (FlatFieldVectorsWriter<float[]>) delegate);
      this.fields.add(quantizedWriter);
    }
    return delegate;
  }

  @Override
  public void mergeOneField(FieldInfo fieldInfo, MergeState mergeState) throws IOException {
    this.rawVectorDelegate.mergeOneField(fieldInfo, mergeState);
    // Since we know we will not be searching for additional indexing, we can just write the
    // the vectors directly to the new segment.
    // No need to use temporary file as we don't have to re-open for reading
    if (fieldInfo.getVectorEncoding().equals(VectorEncoding.FLOAT32)) {
      ScalarQuantizer mergedQuantizationState = new BinaryQuantizer();
      MergedQuantizedVectorValues byteVectorValues =
          MergedQuantizedVectorValues.mergeQuantizedByteVectorValues(
              fieldInfo, mergeState, mergedQuantizationState);
      long vectorDataOffset = this.quantizedVectorData.alignFilePointer(Float.BYTES);
      DocsWithFieldSet docsWithField =
          writeQuantizedVectorData(this.quantizedVectorData, byteVectorValues);
      long vectorDataLength = this.quantizedVectorData.getFilePointer() - vectorDataOffset;
      writeMeta(
          fieldInfo,
          this.segmentWriteState.segmentInfo.maxDoc(),
          vectorDataOffset,
          vectorDataLength,
          this.confidenceInterval,
          BinaryQuantizer.UNUSED_MIN_QUANTILE,
          BinaryQuantizer.UNUSED_MAX_QUANTILE,
          docsWithField);
    }
  }

  @Override
  public void flush(int maxDoc, Sorter.DocMap sortMap) throws IOException {
    this.rawVectorDelegate.flush(maxDoc, sortMap);
    for (FieldWriter field : this.fields) {
      if (sortMap == null) {
        writeField(field, maxDoc);
      } else {
        writeSortingField(field, maxDoc, sortMap);
      }
      field.finish();
    }
  }

  @Override
  public void finish() throws IOException {
    if (this.finished) {
      throw new IllegalStateException("already finished");
    }
    this.finished = true;
    this.rawVectorDelegate.finish();
    if (this.meta != null) {
      // write end of fields marker
      this.meta.writeInt(-1);
      CodecUtil.writeFooter(this.meta);
    }
    if (this.quantizedVectorData != null) {
      CodecUtil.writeFooter(this.quantizedVectorData);
    }
  }

  @Override
  public long ramBytesUsed() {
    @Var long total = SHALLOW_RAM_BYTES_USED;
    for (FieldWriter field : this.fields) {
      total += field.ramBytesUsed();
    }
    return total;
  }

  private void writeField(FieldWriter fieldData, int maxDoc) throws IOException {
    // write vector values
    long vectorDataOffset = this.quantizedVectorData.alignFilePointer(Float.BYTES);
    writeQuantizedVectors(fieldData);
    long vectorDataLength = this.quantizedVectorData.getFilePointer() - vectorDataOffset;

    writeMeta(
        fieldData.fieldInfo,
        maxDoc,
        vectorDataOffset,
        vectorDataLength,
        this.confidenceInterval,
        BinaryQuantizer.UNUSED_MIN_QUANTILE,
        BinaryQuantizer.UNUSED_MAX_QUANTILE,
        fieldData.getDocsWithFieldSet());
  }

  private void writeMeta(
      FieldInfo field,
      int maxDoc,
      long vectorDataOffset,
      long vectorDataLength,
      Optional<Float> confidenceInterval,
      float lowerQuantile,
      float upperQuantile,
      DocsWithFieldSet docsWithField)
      throws IOException {
    this.meta.writeInt(field.number);
    this.meta.writeInt(field.getVectorEncoding().ordinal());
    this.meta.writeInt(field.getVectorSimilarityFunction().ordinal());
    this.meta.writeVLong(vectorDataOffset);
    this.meta.writeVLong(vectorDataLength);
    this.meta.writeVInt(field.getVectorDimension());
    int count = docsWithField.cardinality();
    this.meta.writeInt(count);
    if (count > 0) {
      assert Float.isFinite(lowerQuantile) && Float.isFinite(upperQuantile);
      if (this.version >= Mongot01042BinaryQuantizedFlatVectorsFormat.VERSION_ADD_BITS) {
        this.meta.writeInt(
            confidenceInterval.isEmpty() ? -1 : Float.floatToIntBits(confidenceInterval.get()));
        this.meta.writeByte((byte) 1); // bits
        this.meta.writeByte((byte) 1); // compress
      } else {
        assert confidenceInterval.isEmpty()
            || confidenceInterval.get() != DYNAMIC_CONFIDENCE_INTERVAL;
        this.meta.writeInt(
            Float.floatToIntBits(
                confidenceInterval.isEmpty()
                    ? calculateDefaultConfidenceInterval(field.getVectorDimension())
                    : confidenceInterval.get()));
      }
      this.meta.writeInt(Float.floatToIntBits(lowerQuantile));
      this.meta.writeInt(Float.floatToIntBits(upperQuantile));
    }
    // write docIDs
    OrdToDocDISIReaderConfiguration.writeStoredMeta(
        DIRECT_MONOTONIC_BLOCK_SHIFT,
        this.meta,
        this.quantizedVectorData,
        count,
        maxDoc,
        docsWithField);
  }

  private void writeQuantizedVectors(FieldWriter fieldData) throws IOException {
    ScalarQuantizer scalarQuantizer = new BinaryQuantizer();
    byte[] vector = new byte[fieldData.fieldInfo.getVectorDimension()];
    byte[] compressedVector = compressedArray(fieldData.fieldInfo.getVectorDimension());
    ByteBuffer offsetBuffer = ByteBuffer.allocate(Float.BYTES).order(ByteOrder.LITTLE_ENDIAN);
    for (float[] v : fieldData.getVectors()) {
      float offsetCorrection =
          scalarQuantizer.quantize(v, vector, fieldData.fieldInfo.getVectorSimilarityFunction());
      BinaryQuantizationUtils.compressBytes(vector, compressedVector);
      this.quantizedVectorData.writeBytes(compressedVector, compressedVector.length);
      offsetBuffer.putFloat(offsetCorrection);
      this.quantizedVectorData.writeBytes(offsetBuffer.array(), offsetBuffer.array().length);
      offsetBuffer.rewind();
    }
  }

  private void writeSortingField(FieldWriter fieldData, int maxDoc, Sorter.DocMap sortMap)
      throws IOException {
    int[] docIdOffsets = new int[sortMap.size()];
    @Var int offset = 1; // 0 means no vector for this (field, document)
    DocIdSetIterator iterator = fieldData.getDocsWithFieldSet().iterator();
    for (int docID = iterator.nextDoc();
        docID != DocIdSetIterator.NO_MORE_DOCS;
        docID = iterator.nextDoc()) {
      int newDocID = sortMap.oldToNew(docID);
      docIdOffsets[newDocID] = offset++;
    }
    DocsWithFieldSet newDocsWithField = new DocsWithFieldSet();
    int[] ordMap = new int[offset - 1]; // new ord to old ord
    @Var int ord = 0;
    @Var int doc = 0;
    for (int docIdOffset : docIdOffsets) {
      if (docIdOffset != 0) {
        ordMap[ord] = docIdOffset - 1;
        newDocsWithField.add(doc);
        ord++;
      }
      doc++;
    }

    // write vector values
    long vectorDataOffset = this.quantizedVectorData.alignFilePointer(Float.BYTES);
    writeSortedQuantizedVectors(fieldData, ordMap);
    long quantizedVectorLength = this.quantizedVectorData.getFilePointer() - vectorDataOffset;
    writeMeta(
        fieldData.fieldInfo,
        maxDoc,
        vectorDataOffset,
        quantizedVectorLength,
        this.confidenceInterval,
        BinaryQuantizer.UNUSED_MIN_QUANTILE,
        BinaryQuantizer.UNUSED_MAX_QUANTILE,
        newDocsWithField);
  }

  private void writeSortedQuantizedVectors(FieldWriter fieldData, int[] ordMap) throws IOException {
    ScalarQuantizer scalarQuantizer = new BinaryQuantizer();
    byte[] vector = new byte[fieldData.fieldInfo.getVectorDimension()];
    byte[] compressedVector = compressedArray(fieldData.fieldInfo.getVectorDimension());
    ByteBuffer offsetBuffer = ByteBuffer.allocate(Float.BYTES).order(ByteOrder.LITTLE_ENDIAN);
    for (int ordinal : ordMap) {
      float[] v = fieldData.getVectors().get(ordinal);
      float offsetCorrection =
          scalarQuantizer.quantize(v, vector, fieldData.fieldInfo.getVectorSimilarityFunction());
      BinaryQuantizationUtils.compressBytes(vector, compressedVector);
      this.quantizedVectorData.writeBytes(compressedVector, compressedVector.length);
      offsetBuffer.putFloat(offsetCorrection);
      this.quantizedVectorData.writeBytes(offsetBuffer.array(), offsetBuffer.array().length);
      offsetBuffer.rewind();
    }
  }

  // [Changed from Lucene] Coding standard required moving this function to here.
  @Override
  public CloseableRandomVectorScorerSupplier mergeOneFieldToIndex(
      FieldInfo fieldInfo, MergeState mergeState) throws IOException {
    if (fieldInfo.getVectorEncoding().equals(VectorEncoding.FLOAT32)) {
      // Simply merge the underlying delegate, which just copies the raw vector data to a new
      // segment file
      this.rawVectorDelegate.mergeOneField(fieldInfo, mergeState);
      ScalarQuantizer mergedQuantizationState = new BinaryQuantizer();
      return mergeOneFieldToIndex(
          this.segmentWriteState, fieldInfo, mergeState, mergedQuantizationState);
    }
    // We only merge the delegate, since the field type isn't float32, quantization wasn't
    // supported, so bypass it.
    return this.rawVectorDelegate.mergeOneFieldToIndex(fieldInfo, mergeState);
  }

  private ScalarQuantizedCloseableRandomVectorScorerSupplier mergeOneFieldToIndex(
      SegmentWriteState segmentWriteState,
      FieldInfo fieldInfo,
      MergeState mergeState,
      ScalarQuantizer mergedQuantizationState)
      throws IOException {
    if (segmentWriteState.infoStream.isEnabled(QUANTIZED_VECTOR_COMPONENT)) {
      segmentWriteState.infoStream.message(
          QUANTIZED_VECTOR_COMPONENT,
          "quantized field="
              + " confidenceInterval="
              + this.confidenceInterval
              + " minQuantile="
              + mergedQuantizationState.getLowerQuantile()
              + " maxQuantile="
              + mergedQuantizationState.getUpperQuantile());
    }
    long vectorDataOffset = this.quantizedVectorData.alignFilePointer(Float.BYTES);
    IndexOutput tempQuantizedVectorData =
        segmentWriteState.directory.createTempOutput(
            this.quantizedVectorData.getName(), "temp", segmentWriteState.context);
    @Var IndexInput quantizationDataInput = null;
    @Var boolean success = false;
    try {
      MergedQuantizedVectorValues byteVectorValues =
          MergedQuantizedVectorValues.mergeQuantizedByteVectorValues(
              fieldInfo, mergeState, mergedQuantizationState);
      DocsWithFieldSet docsWithField =
          writeQuantizedVectorData(tempQuantizedVectorData, byteVectorValues);
      CodecUtil.writeFooter(tempQuantizedVectorData);
      IOUtils.close(tempQuantizedVectorData);
      quantizationDataInput =
          segmentWriteState.directory.openInput(
              tempQuantizedVectorData.getName(), segmentWriteState.context);
      this.quantizedVectorData.copyBytes(
          quantizationDataInput, quantizationDataInput.length() - CodecUtil.footerLength());
      long vectorDataLength = this.quantizedVectorData.getFilePointer() - vectorDataOffset;
      CodecUtil.retrieveChecksum(quantizationDataInput);
      writeMeta(
          fieldInfo,
          segmentWriteState.segmentInfo.maxDoc(),
          vectorDataOffset,
          vectorDataLength,
          this.confidenceInterval,
          mergedQuantizationState.getLowerQuantile(),
          mergedQuantizationState.getUpperQuantile(),
          docsWithField);
      success = true;
      IndexInput finalQuantizationDataInput = quantizationDataInput;
      return new ScalarQuantizedCloseableRandomVectorScorerSupplier(
          () -> {
            IOUtils.close(finalQuantizationDataInput);
            segmentWriteState.directory.deleteFile(tempQuantizedVectorData.getName());
          },
          docsWithField.cardinality(),
          this.vectorsScorer.getRandomVectorScorerSupplier(
              fieldInfo.getVectorSimilarityFunction(),
              new OffHeapQuantizedByteVectorValues.DenseOffHeapVectorValues(
                  fieldInfo.getVectorDimension(),
                  docsWithField.cardinality(),
                  mergedQuantizationState,
                  fieldInfo.getVectorSimilarityFunction(),
                  this.vectorsScorer,
                  quantizationDataInput)));
    } finally {
      if (success == false) {
        IOUtils.closeWhileHandlingException(tempQuantizedVectorData, quantizationDataInput);
        IOUtils.deleteFilesIgnoringExceptions(
            segmentWriteState.directory, tempQuantizedVectorData.getName());
      }
    }
  }

  // [Changed from Lucene] compressedArray() rewritten completely
  /**
   * Allocate a byte array for holding a compressed vector for binary quantization. The size of the
   * array in bytes will be the dimension divided by 8 rounded up to the nearest whole byte if the
   * dimension isn't a multiple of 8.
   *
   * <p>When actively working with a quantized vector, Lucene always stores that quantized vector
   * unpacked in a byte array with one vector element per byte.
   *
   * <p>The rest of the time, if bits < 8, and if compress = true, vectors will be compressed to
   * save storage space, such as inside a .veq segment file, or in main memory when a vector isn't
   * actively in use, which is most of the time.
   *
   * @param dimension the number of bits in a quantized bit vector
   * @return a minimal empty byte array that can hold `dimension` packed bits.
   */
  private static byte[] compressedArray(int dimension) {
    return new byte[BinaryQuantizationUtils.requiredBytes(dimension)];
  }

  /**
   * Writes the vector values to the output and returns a set of documents that contains vectors.
   */
  public static DocsWithFieldSet writeQuantizedVectorData(
      IndexOutput output, QuantizedByteVectorValues quantizedByteVectorValues) throws IOException {
    DocsWithFieldSet docsWithField = new DocsWithFieldSet();
    byte[] compressedVector = compressedArray(quantizedByteVectorValues.dimension());
    DocIndexIterator iterator = quantizedByteVectorValues.iterator();
    for (int docV = iterator.nextDoc(); docV != NO_MORE_DOCS; docV = iterator.nextDoc()) {
      // write vector
      byte[] binaryValue = quantizedByteVectorValues.vectorValue(iterator.index());
      assert binaryValue.length == quantizedByteVectorValues.dimension()
          : "dim=" + quantizedByteVectorValues.dimension() + " len=" + binaryValue.length;
      BinaryQuantizationUtils.compressBytes(binaryValue, compressedVector);
      output.writeBytes(compressedVector, compressedVector.length);
      output.writeInt(
          Float.floatToIntBits(
              quantizedByteVectorValues.getScoreCorrectionConstant(iterator.index())));
      docsWithField.add(docV);
    }
    return docsWithField;
  }

  @Override
  public void close() throws IOException {
    IOUtils.close(this.meta, this.quantizedVectorData, this.rawVectorDelegate);
  }

  static class FieldWriter extends FlatFieldVectorsWriter<float[]> {
    private static final long SHALLOW_SIZE = shallowSizeOfInstance(FieldWriter.class);
    private final FieldInfo fieldInfo;
    private boolean finished;
    private final FlatFieldVectorsWriter<float[]> flatFieldVectorsWriter;

    @SuppressWarnings("unchecked")
    FieldWriter(FieldInfo fieldInfo, FlatFieldVectorsWriter<float[]> indexWriter) {
      super();
      this.fieldInfo = fieldInfo;
      this.flatFieldVectorsWriter = Objects.requireNonNull(indexWriter);
    }

    @Override
    public boolean isFinished() {
      return this.finished && this.flatFieldVectorsWriter.isFinished();
    }

    @Override
    public List<float[]> getVectors() {
      return this.flatFieldVectorsWriter.getVectors();
    }

    @Override
    public DocsWithFieldSet getDocsWithFieldSet() {
      return this.flatFieldVectorsWriter.getDocsWithFieldSet();
    }

    @Override
    public void finish() throws IOException {
      if (this.finished) {
        return;
      }
      assert this.flatFieldVectorsWriter.isFinished();
      this.finished = true;
    }

    @Override
    public long ramBytesUsed() {
      return SHALLOW_SIZE + this.flatFieldVectorsWriter.ramBytesUsed();
    }

    @Override
    public void addValue(int docID, float[] vectorValue) throws IOException {
      this.flatFieldVectorsWriter.addValue(docID, vectorValue);
    }

    @Override
    public float[] copyValue(float[] vectorValue) {
      throw new UnsupportedOperationException();
    }
  }

  static class QuantizedByteVectorValueSub extends DocIDMerger.Sub {
    private final QuantizedByteVectorValues values;
    private final KnnVectorValues.DocIndexIterator iterator;

    QuantizedByteVectorValueSub(MergeState.DocMap docMap, QuantizedByteVectorValues values) {
      super(docMap);
      this.values = values;
      this.iterator = values.iterator();
      assert this.iterator.docID() == -1;
    }

    @Override
    public int nextDoc() throws IOException {
      return this.iterator.nextDoc();
    }

    public int index() {
      return this.iterator.index();
    }
  }

  /** Returns a merged view over all the segment's {@link QuantizedByteVectorValues}. */
  static class MergedQuantizedVectorValues extends QuantizedByteVectorValues {
    public static MergedQuantizedVectorValues mergeQuantizedByteVectorValues(
        FieldInfo fieldInfo, MergeState mergeState, ScalarQuantizer scalarQuantizer)
        throws IOException {
      assert fieldInfo != null && fieldInfo.hasVectorValues();

      List<QuantizedByteVectorValueSub> subs = new ArrayList<>();
      for (int i = 0; i < mergeState.knnVectorsReaders.length; i++) {
        if (hasVectorValues(mergeState.fieldInfos[i], fieldInfo.name)) {
          FloatVectorValues toQuantize =
              mergeState.knnVectorsReaders[i].getFloatVectorValues(fieldInfo.name);
          QuantizedByteVectorValueSub sub =
              new QuantizedByteVectorValueSub(
                  mergeState.docMaps[i],
                  new QuantizedFloatVectorValues(
                      toQuantize, fieldInfo.getVectorSimilarityFunction(), scalarQuantizer));
          subs.add(sub);
        }
      }
      return new MergedQuantizedVectorValues(subs, mergeState);
    }

    private final List<QuantizedByteVectorValueSub> subs;
    private final DocIDMerger<QuantizedByteVectorValueSub> docIdMerger;
    private final int size;

    @Nullable private QuantizedByteVectorValueSub current;

    private MergedQuantizedVectorValues(
        List<QuantizedByteVectorValueSub> subs, MergeState mergeState) throws IOException {
      this.subs = subs;
      this.docIdMerger = DocIDMerger.of(subs, mergeState.needsIndexSort);
      @Var int totalSize = 0;
      for (QuantizedByteVectorValueSub sub : subs) {
        totalSize += sub.values.size();
      }
      this.size = totalSize;
    }

    @Override
    public byte[] vectorValue(int ord) throws IOException {
      if (this.current == null) {
        throw new NullPointerException("Current quantized vector is null");
      }
      return this.current.values.vectorValue(this.current.index());
    }

    @Override
    public int size() {
      return this.size;
    }

    @Override
    public int dimension() {
      return this.subs.get(0).values.dimension();
    }

    @Override
    public float getScoreCorrectionConstant(int ord) throws IOException {
      if (this.current == null) {
        throw new NullPointerException("Current quantized vector is null");
      }
      return this.current.values.getScoreCorrectionConstant(this.current.index());
    }

    @Override
    public DocIndexIterator iterator() {
      return new CompositeIterator();
    }

    private class CompositeIterator extends DocIndexIterator {
      private int docId;
      private int ord;

      public CompositeIterator() {
        this.docId = -1;
        this.ord = -1;
      }

      @Override
      public int index() {
        return this.ord;
      }

      @Override
      public int docID() {
        return this.docId;
      }

      @Override
      public int nextDoc() throws IOException {
        MergedQuantizedVectorValues.this.current =
            MergedQuantizedVectorValues.this.docIdMerger.next();
        if (MergedQuantizedVectorValues.this.current == null) {
          this.docId = NO_MORE_DOCS;
          this.ord = NO_MORE_DOCS;
        } else {
          this.docId = MergedQuantizedVectorValues.this.current.mappedDocID;
          ++this.ord;
        }
        return this.docId;
      }

      @Override
      public int advance(int target) throws IOException {
        throw new UnsupportedOperationException();
      }

      @Override
      public long cost() {
        return MergedQuantizedVectorValues.this.size;
      }
    }
  }

  static class QuantizedFloatVectorValues extends QuantizedByteVectorValues {
    private final FloatVectorValues values;
    private final ScalarQuantizer quantizer;
    private final byte[] quantizedVector;
    private float offsetValue = 0f;

    private final VectorSimilarityFunction vectorSimilarityFunction;

    public QuantizedFloatVectorValues(
        FloatVectorValues values,
        VectorSimilarityFunction vectorSimilarityFunction,
        ScalarQuantizer quantizer) {
      this.values = values;
      this.quantizer = quantizer;
      this.quantizedVector = new byte[values.dimension()];
      this.vectorSimilarityFunction = vectorSimilarityFunction;
    }

    @Override
    public int dimension() {
      return this.values.dimension();
    }

    @Override
    public int size() {
      return this.values.size();
    }

    @Override
    public VectorScorer scorer(float[] target) throws IOException {
      throw new UnsupportedOperationException();
    }

    private void quantize(float[] raw) throws IOException {
      this.offsetValue =
          this.quantizer.quantize(raw, this.quantizedVector, this.vectorSimilarityFunction);
    }

    @Override
    public float getScoreCorrectionConstant(int ord) throws IOException {
      return this.offsetValue;
    }

    @Override
    public byte[] vectorValue(int ord) throws IOException {
      quantize(this.values.vectorValue(ord));
      return this.quantizedVector;
    }

    @Override
    public DocIndexIterator iterator() {
      return this.values.iterator();
    }
  }

  static final class ScalarQuantizedCloseableRandomVectorScorerSupplier
      implements CloseableRandomVectorScorerSupplier {

    private final RandomVectorScorerSupplier supplier;
    private final Closeable onClose;
    private final int numVectors;

    ScalarQuantizedCloseableRandomVectorScorerSupplier(
        Closeable onClose, int numVectors, RandomVectorScorerSupplier supplier) {
      this.onClose = onClose;
      this.supplier = supplier;
      this.numVectors = numVectors;
    }

    @Override
    public RandomVectorScorer scorer(int ord) throws IOException {
      return this.supplier.scorer(ord);
    }

    @Override
    public RandomVectorScorerSupplier copy() throws IOException {
      return this.supplier.copy();
    }

    @Override
    public void close() throws IOException {
      this.onClose.close();
    }

    @Override
    public int totalVectorCount() {
      return this.numVectors;
    }
  }
}
