package com.xgen.mongot.index.lucene.codec.flat;

import com.google.auto.service.AutoService;
import com.xgen.mongot.index.definition.VectorFieldSpecification;
import com.xgen.mongot.index.lucene.vector.Lucene99NativeMemorySegmentVectorScorer;
import java.io.IOException;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.KnnVectorsWriter;
import org.apache.lucene.codecs.hnsw.FlatVectorScorerUtil;
import org.apache.lucene.codecs.hnsw.FlatVectorsFormat;
import org.apache.lucene.codecs.lucene99.Lucene99FlatVectorsFormat;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;

/**
 * A {@link KnnVectorsFormat} that supports exhaustive vector search for both float32 and byte
 * vector types, accelerating byte scoring with native SIMD via {@link
 * Lucene99NativeMemorySegmentVectorScorer} when the platform supports it (falling back to Lucene's
 * Java scorer for float vectors and on unaccelerated platforms).
 *
 * <p>Vectors are stored via Lucene's {@code Lucene99FlatVectorsFormat}, so the on-disk layout is
 * Lucene-identical (this format's own name is {@code Float32AndByteFlatVectorsFormat}); only the
 * in-memory scorer differs, so segments interoperate with no reindex. Byte scoring is always-on
 * natively when the platform is accelerated -- there is no feature flag. Q7 vectors are handled by
 * {@link ScalarQuantizedFlatVectorsFormat}, never here.
 *
 * @see KnnVectorsFormat
 * @see Lucene99FlatVectorsFormat
 * @see Lucene99NativeMemorySegmentVectorScorer
 */
@AutoService(KnnVectorsFormat.class)
public class Float32AndByteFlatVectorsFormat extends KnnVectorsFormat {

  private static final String NAME = "Float32AndByteFlatVectorsFormat";

  // Use the NativeMemorySegmentVectorScorer to accelerate int8 byte scoring when available; float
  // vectors and non-memory-segment inputs fall back to Lucene's Java scorer. The same composition
  // backs Lucene99NativeHnswVectorsFormat's flat storage.
  private final FlatVectorsFormat format =
      new Lucene99FlatVectorsFormat(
          new Lucene99NativeMemorySegmentVectorScorer(
              FlatVectorScorerUtil.getLucene99FlatVectorsScorer()));

  public Float32AndByteFlatVectorsFormat() {
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
