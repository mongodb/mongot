package com.xgen.mongot.index.lucene.codec.flat;

import com.google.auto.service.AutoService;
import com.xgen.mongot.index.definition.VectorFieldSpecification;
import com.xgen.mongot.index.lucene.vector.NativeScalarQuantizedFlatVectorsFormat;
import java.io.IOException;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.KnnVectorsWriter;
import org.apache.lucene.codecs.hnsw.FlatVectorsFormat;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;

/**
 * A {@link KnnVectorsFormat} that supports exhaustive vector search with 7-bit scalar quantization,
 * accelerating scoring with native SIMD via {@link NativeScalarQuantizedFlatVectorsFormat} when the
 * {@code mongot.featureFlag.enableNativeScalarVectorScoring} flag is enabled and the platform is
 * accelerated (falling back to Lucene's Java scalar scorer otherwise).
 *
 * <p>Stored via the stock {@code Lucene99ScalarQuantizedVectorsFormat} layout (this format's own
 * name is {@code ScalarQuantizedFlatVectorsFormat}); only the in-memory scorer differs, so segments
 * interoperate and native scoring can be toggled with no reindex.
 *
 * @see KnnVectorsFormat
 * @see NativeScalarQuantizedFlatVectorsFormat
 */
@AutoService(KnnVectorsFormat.class)
public class ScalarQuantizedFlatVectorsFormat extends KnnVectorsFormat {

  private static final String NAME = "ScalarQuantizedFlatVectorsFormat";

  // Native Q7 scoring with Lucene's Java scalar scorer as the flag/acceleration-gated fallback. The
  // same FlatVectorsFormat already backs the HNSW Q7 path (NativeHnswScalarQuantizedVectorsFormat).
  private final FlatVectorsFormat format = new NativeScalarQuantizedFlatVectorsFormat();

  public ScalarQuantizedFlatVectorsFormat() {
    super(NAME);
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
