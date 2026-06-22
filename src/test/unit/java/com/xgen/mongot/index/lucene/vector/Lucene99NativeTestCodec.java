package com.xgen.mongot.index.lucene.vector;

import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.lucene101.Lucene101Codec;

/**
 * A test-only codec that wires {@link Lucene99NativeHnswVectorsFormat} into the standard {@link
 * Lucene101Codec}. Registered via SPI so that segments written with this codec can be read back by
 * name.
 */
public class Lucene99NativeTestCodec extends FilterCodec {
  public Lucene99NativeTestCodec() {
    super("Lucene99NativeHnsw", new Lucene101Codec());
  }

  @Override
  public KnnVectorsFormat knnVectorsFormat() {
    return new Lucene99NativeHnswVectorsFormat();
  }
}
