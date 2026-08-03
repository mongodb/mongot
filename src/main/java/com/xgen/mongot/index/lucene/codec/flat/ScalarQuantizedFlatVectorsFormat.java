package com.xgen.mongot.index.lucene.codec.flat;

import com.google.auto.service.AutoService;
import com.mongodb.lang.Nullable;
import com.xgen.mongot.index.definition.VectorFieldSpecification;
import java.io.IOException;
import org.apache.lucene.backward_codecs.lucene99.Lucene99ScalarQuantizedVectorsFormat;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.KnnVectorsWriter;
import org.apache.lucene.codecs.hnsw.FlatVectorsFormat;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;

/**
 * A {@link KnnVectorsFormat} that supports exhaustive vector search with scalar quantization.
 *
 * <p>This format does <b>NOT</b> build or use HNSW graph structures. Instead, it performs
 * exhaustive brute-force search by scanning all vectors in the index to find the k-nearest
 * neighbors.
 *
 * <p>The format delegates to Lucene's {@link Lucene99ScalarQuantizedVectorsFormat} for the
 * underlying storage and retrieval, which applies scalar quantization to reduce memory footprint
 *
 * @see KnnVectorsFormat
 * @see Lucene99ScalarQuantizedVectorsFormat
 */
@AutoService(KnnVectorsFormat.class)
public class ScalarQuantizedFlatVectorsFormat extends KnnVectorsFormat {

  private static final String NAME = "ScalarQuantizedFlatVectorsFormat";
  private final FlatVectorsFormat format;

  public ScalarQuantizedFlatVectorsFormat() {
    this(null, 7, false);
  }

  public ScalarQuantizedFlatVectorsFormat(
      @Nullable Float confidenceInterval, int bits, boolean compress) {
    super(NAME);
    this.format = new Lucene99ScalarQuantizedVectorsFormat(confidenceInterval, bits, compress);
  }

  @Override
  public KnnVectorsWriter fieldsWriter(SegmentWriteState state) throws IOException {
    return new FlatVectorsWriterDecorator(this.format.fieldsWriter(state));
  }

  @Override
  public KnnVectorsReader fieldsReader(SegmentReadState state) throws IOException {
    return new FullScanFlatVectorsReaderDecorator(this.format.fieldsReader(state));
  }

  @Override
  public int getMaxDimensions(String fieldName) {
    // [Changed from Lucene]
    // Changed the max allowed vector dimensions.
    return VectorFieldSpecification.MAX_DIMENSIONS;
  }
}
