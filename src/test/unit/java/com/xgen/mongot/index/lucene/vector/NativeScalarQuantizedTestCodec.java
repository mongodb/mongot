package com.xgen.mongot.index.lucene.vector;

import org.apache.lucene.backward_codecs.lucene101.Lucene101Codec;
import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.KnnVectorsFormat;

/**
 * A test-only codec that wires {@link NativeHnswScalarQuantizedVectorsFormat} directly into the
 * standard {@link Lucene101Codec}.
 *
 * <p>Because {@code knnVectorsFormat()} returns the native format itself (rather than a {@link
 * org.apache.lucene.codecs.perfield.PerFieldKnnVectorsFormat}), reading a segment written with this
 * codec via {@link
 * org.apache.lucene.index.DirectoryReader#open(org.apache.lucene.index.IndexWriter)} resolves to
 * the native format -- and therefore to the flag-gated {@link NativeScalarQuantizedVectorsScorer}
 * -- without going through {@code KnnVectorsFormat.forName}. SPI name resolution does not favor the
 * shadowed name {@code Lucene99HnswScalarQuantizedVectorsFormat} in the unit-test classpath, so a
 * plain {@code PerFieldKnnVectorsFormat} read would silently resolve back to Lucene's stock scalar
 * format and never exercise the native path.
 */
public class NativeScalarQuantizedTestCodec extends FilterCodec {
  public NativeScalarQuantizedTestCodec() {
    super("NativeScalarQuantizedTest", new Lucene101Codec());
  }

  @Override
  public KnnVectorsFormat knnVectorsFormat() {
    return new NativeHnswScalarQuantizedVectorsFormat();
  }
}
