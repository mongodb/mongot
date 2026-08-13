package com.xgen.mongot.index.lucene.vector;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat;
import org.junit.Test;

/**
 * {@link HnswWriteParams} is the single validation point for the HNSW write parameters: every
 * format constructor treats an instance as valid by construction, so invalid values must be
 * rejected here — at codec construction, not at first flush inside an indexing thread.
 */
public class HnswWriteParamsTest {

  @Test
  public void constructor_acceptsEverySupportedGraphWriteVersion() {
    for (int version = Lucene99HnswVectorsFormat.VERSION_START;
        version <= Lucene99HnswVectorsFormat.VERSION_GROUPVARINT;
        version++) {
      HnswWriteParams params = new HnswWriteParams(0, version);
      assertThat(params.graphWriteVersion()).isEqualTo(version);
    }
  }

  @Test
  public void constructor_rejectsOutOfRangeGraphWriteVersion() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new HnswWriteParams(0, Lucene99HnswVectorsFormat.VERSION_START - 1));
    assertThrows(
        IllegalArgumentException.class,
        () -> new HnswWriteParams(0, Lucene99HnswVectorsFormat.VERSION_GROUPVARINT + 1));
  }

  @Test
  public void constructor_rejectsNegativeTinySegmentsThreshold() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new HnswWriteParams(-1, Lucene99HnswVectorsFormat.VERSION_START));
  }
}
