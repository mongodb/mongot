package com.xgen.mongot.index.lucene.quantization;

import com.xgen.mongot.index.lucene.codec.LuceneCodec;
import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.perfield.PerFieldKnnVectorsFormat;

/**
 * A test-only codec that wires {@link Mongot01042HnswBinaryQuantizedVectorsFormat} in as the KNN
 * vectors format for every field, delegating all other sub-formats to {@link LuceneCodec}.
 *
 * <p>This lets tests write and read binary-quantized segments by injecting the format directly,
 * without routing through {@code LuceneCodec}'s per-field dispatch, enabling testing for new binary
 * quantization formats before rollout.
 */
public class Mongot01042BinaryQuantizedTestCodec extends FilterCodec {
  public Mongot01042BinaryQuantizedTestCodec() {
    super("Lucene99", new LuceneCodec());
  }

  @Override
  public KnnVectorsFormat knnVectorsFormat() {
    return new PerFieldKnnVectorsFormat() {
      @Override
      public KnnVectorsFormat getKnnVectorsFormatForField(String field) {
        return new Mongot01042HnswBinaryQuantizedVectorsFormat();
      }
    };
  }
}
