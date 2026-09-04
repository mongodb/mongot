package com.xgen.mongot.index.lucene.vector;

import java.io.IOException;
import org.apache.lucene.backward_codecs.lucene99.Lucene99ScalarQuantizedVectorsReader;
import org.apache.lucene.backward_codecs.lucene99.Lucene99ScalarQuantizedVectorsWriter;
import org.apache.lucene.codecs.hnsw.FlatVectorScorerUtil;
import org.apache.lucene.codecs.hnsw.FlatVectorsFormat;
import org.apache.lucene.codecs.hnsw.FlatVectorsReader;
import org.apache.lucene.codecs.hnsw.FlatVectorsScorer;
import org.apache.lucene.codecs.hnsw.FlatVectorsWriter;
import org.apache.lucene.codecs.lucene99.Lucene99FlatVectorsFormat;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;

/**
 * A {@link FlatVectorsFormat} for 7-bit scalar-quantized vectors that injects {@link
 * NativeScalarQuantizedVectorsScorer} into Lucene's stock scalar-quantized reader and writer.
 *
 * <p>The on-disk format is byte-identical to Lucene's {@code Lucene99ScalarQuantizedVectorsFormat}
 * (same codec name and storage layout): only the in-memory scorer differs. This means segments
 * interoperate with stock Lucene and native scoring can be enabled or disabled with no reindex.
 */
public class NativeScalarQuantizedFlatVectorsFormat extends FlatVectorsFormat {

  // Keep Lucene's codec name so the on-disk format is identical and segments interoperate.
  static final String NAME = "Lucene99ScalarQuantizedVectorsFormat";

  private static final byte BITS = 7;
  private static final boolean COMPRESS = false;

  private final FlatVectorsScorer scorer =
      new NativeScalarQuantizedVectorsScorer(
          FlatVectorScorerUtil.getLucene99ScalarQuantizedVectorsScorer());

  private final FlatVectorsFormat rawVectorFormat =
      new Lucene99FlatVectorsFormat(FlatVectorScorerUtil.getLucene99FlatVectorsScorer());

  public NativeScalarQuantizedFlatVectorsFormat() {
    super(NAME);
  }

  @Override
  public FlatVectorsWriter fieldsWriter(SegmentWriteState state) throws IOException {
    return new Lucene99ScalarQuantizedVectorsWriter(
        state, null, BITS, COMPRESS, this.rawVectorFormat.fieldsWriter(state), this.scorer);
  }

  @Override
  public FlatVectorsReader fieldsReader(SegmentReadState state) throws IOException {
    return new Lucene99ScalarQuantizedVectorsReader(
        state, this.rawVectorFormat.fieldsReader(state), this.scorer);
  }

  @Override
  public String toString() {
    return "NativeScalarQuantizedFlatVectorsFormat(name=" + NAME + ", scorer=" + this.scorer + ")";
  }
}
