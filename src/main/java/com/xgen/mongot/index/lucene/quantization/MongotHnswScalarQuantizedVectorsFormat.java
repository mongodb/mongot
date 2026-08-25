package com.xgen.mongot.index.lucene.quantization;

import static com.xgen.mongot.index.lucene.vector.Lucene99NativeHnswVectorsFormat.DEFAULT_HNSW_GRAPH_THRESHOLD;
import static org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat.DEFAULT_BEAM_WIDTH;
import static org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat.DEFAULT_MAX_CONN;
import static org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat.DEFAULT_NUM_MERGE_WORKER;
import static org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat.MAXIMUM_BEAM_WIDTH;
import static org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat.MAXIMUM_MAX_CONN;

import com.xgen.mongot.index.lucene.vector.HnswWriteParams;
import java.io.IOException;
import org.apache.lucene.backward_codecs.lucene99.Lucene99HnswScalarQuantizedVectorsFormat;
import org.apache.lucene.backward_codecs.lucene99.Lucene99ScalarQuantizedVectorsFormat;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.KnnVectorsWriter;
import org.apache.lucene.codecs.hnsw.FlatVectorsFormat;
import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat;
import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsReader;
import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsWriter;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;

/**
 * Mongot's construction of the 7-bit scalar-quantized HNSW vector format ({@code KNN_F32_Q7}),
 * replicating {@link Lucene99HnswScalarQuantizedVectorsFormat} except that the {@link
 * Lucene99HnswVectorsWriter} is built with configurable {@link HnswWriteParams} (threshold 0 /
 * 9.11-varint under {@code WriteFormat.LUCENE99} so every segment gets a 9.11-readable graph; the
 * Lucene defaults under {@code WriteFormat.LUCENE104}). {@link
 * Lucene99HnswScalarQuantizedVectorsFormat} offers no way to override either parameter, hence this
 * wrapper.
 *
 * <p>Lucene 10.4's default threshold (100) skips the graph for smaller segments, writing {@code
 * numLevels = 0} metadata that pre-10.4 readers (mongot on Lucene 9.11.1 / 10.1.0) cannot handle:
 * their unguarded {@code getGraph()} throws NPE at query time, breaking rollback.
 *
 * <p>Takes the delegate's on-disk format name, so segments written here are resolved via SPI to the
 * real {@link Lucene99HnswScalarQuantizedVectorsFormat} on read. This class must therefore never be
 * registered with SPI itself, and its output must stay decodable by the delegate's reader:
 * byte-identical to the delegate's under {@code WriteFormat.LUCENE99} (readable by rolled-back
 * binaries), group-varint graphs under {@code WriteFormat.LUCENE104} (readable by the delegate's
 * version-widened reader on 10.4 binaries).
 */
public final class MongotHnswScalarQuantizedVectorsFormat extends KnnVectorsFormat {

  /** The delegate's on-disk format name, written into per-field metadata. */
  private static final String NAME = "Lucene99HnswScalarQuantizedVectorsFormat";

  /** The maximum number of connections to a node in the HNSW graph. */
  private final int maxConn;

  /** The size of the candidate queue maintained during graph construction. */
  private final int beamWidth;

  /** Tiny-segment graph threshold and graph write version; see {@link HnswWriteParams}. */
  private final HnswWriteParams hnswWriteParams;

  /**
   * The flat 7-bit scalar-quantized storage format, constructed exactly as {@link
   * Lucene99HnswScalarQuantizedVectorsFormat} does for mongot's parameters (bits=7, compress=false,
   * default confidence interval).
   */
  private final FlatVectorsFormat flatVectorsFormat;

  /** Constructs a format using default graph construction parameters */
  public MongotHnswScalarQuantizedVectorsFormat() {
    this(
        DEFAULT_MAX_CONN,
        DEFAULT_BEAM_WIDTH,
        new HnswWriteParams(DEFAULT_HNSW_GRAPH_THRESHOLD, Lucene99HnswVectorsFormat.VERSION_START));
  }

  /**
   * Constructs a format using the given graph construction parameters.
   *
   * @param maxConn the maximum number of connections to a node in the HNSW graph
   * @param beamWidth the size of the queue maintained during graph construction.
   * @param hnswWriteParams the tiny-segment graph threshold and graph write version
   */
  public MongotHnswScalarQuantizedVectorsFormat(
      int maxConn, int beamWidth, HnswWriteParams hnswWriteParams) {
    super(NAME);
    if (maxConn <= 0 || maxConn > MAXIMUM_MAX_CONN) {
      throw new IllegalArgumentException(
          "maxConn must be positive and less than or equal to "
              + MAXIMUM_MAX_CONN
              + "; maxConn="
              + maxConn);
    }
    if (beamWidth <= 0 || beamWidth > MAXIMUM_BEAM_WIDTH) {
      throw new IllegalArgumentException(
          "beamWidth must be positive and less than or equal to "
              + MAXIMUM_BEAM_WIDTH
              + "; beamWidth="
              + beamWidth);
    }
    this.maxConn = maxConn;
    this.beamWidth = beamWidth;
    this.flatVectorsFormat = new Lucene99ScalarQuantizedVectorsFormat(null, 7, false);
    this.hnswWriteParams = hnswWriteParams;
  }

  @Override
  public KnnVectorsWriter fieldsWriter(SegmentWriteState state) throws IOException {
    return new Lucene99HnswVectorsWriter(
        state,
        this.maxConn,
        this.beamWidth,
        this.flatVectorsFormat.fieldsWriter(state),
        DEFAULT_NUM_MERGE_WORKER,
        null,
        this.hnswWriteParams.tinySegmentsThreshold(),
        this.hnswWriteParams.graphWriteVersion());
  }

  @Override
  public KnnVectorsReader fieldsReader(SegmentReadState state) throws IOException {
    return new Lucene99HnswVectorsReader(state, this.flatVectorsFormat.fieldsReader(state));
  }

  @Override
  public int getMaxDimensions(String fieldName) {
    // Matches the delegate; the effective limit comes from LuceneCodec's PerFieldKnnVectorsFormat
    // override.
    return 1024;
  }

  @Override
  public String toString() {
    return "MongotHnswScalarQuantizedVectorsFormat(name="
        + NAME
        + ", maxConn="
        + this.maxConn
        + ", beamWidth="
        + this.beamWidth
        + ", flatVectorFormat="
        + this.flatVectorsFormat
        + ")";
  }
}
