package com.xgen.mongot.index.lucene.vector;

import com.google.errorprone.annotations.Immutable;
import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat;

/**
 * The write-format-dependent parameters shared by every HNSW vector format, grouped so call sites
 * cannot transpose the two ints and validated once here: every format constructor may treat any
 * {@code HnswWriteParams} instance as valid by construction.
 *
 * <p>Lives in this package (not next to {@code LuceneCodec.WriteFormat}, which carries an instance
 * per write format) so the format classes can depend on it without a codec package cycle.
 *
 * @param tinySegmentsThreshold segments with fewer vectors than this skip HNSW graph construction;
 *     see {@link Lucene99NativeHnswVectorsFormat#DEFAULT_HNSW_GRAPH_THRESHOLD}
 * @param graphWriteVersion the graph write version selecting the {@code .vem}/{@code .vex}
 *     encoding: {@link Lucene99HnswVectorsFormat#VERSION_START} (9.11-readable varint) or {@link
 *     Lucene99HnswVectorsFormat#VERSION_GROUPVARINT} (vanilla 10.4 group-varint)
 */
@Immutable
public record HnswWriteParams(int tinySegmentsThreshold, int graphWriteVersion) {

  public HnswWriteParams {
    if (tinySegmentsThreshold < 0) {
      throw new IllegalArgumentException(
          "tinySegmentsThreshold must be non-negative; tinySegmentsThreshold="
              + tinySegmentsThreshold);
    }
    if (graphWriteVersion < Lucene99HnswVectorsFormat.VERSION_START
        || graphWriteVersion > Lucene99HnswVectorsFormat.VERSION_GROUPVARINT) {
      throw new IllegalArgumentException(
          "graphWriteVersion must be between "
              + Lucene99HnswVectorsFormat.VERSION_START
              + " and "
              + Lucene99HnswVectorsFormat.VERSION_GROUPVARINT
              + "; graphWriteVersion="
              + graphWriteVersion);
    }
  }
}
