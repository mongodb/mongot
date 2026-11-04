package com.xgen.mongot.index.lucene.quantization;

import com.xgen.mongot.index.lucene.explain.knn.InstrumentableKnnByteVectorQuery;
import com.xgen.mongot.index.lucene.explain.tracing.Explain;
import java.io.IOException;
import java.util.Optional;
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.KnnByteVectorQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.VectorScorer;
import org.apache.lucene.util.quantization.QuantizedByteVectorValues;
import org.jetbrains.annotations.Nullable;

/** {@link BinaryQuantizedRescoreQuery} implementation backed by a byte-encoded KNN query. */
public class ByteBinaryQuantizedRescoreQuery implements BinaryQuantizedRescoreQuery {
  private final KnnByteVectorQuery query;

  public ByteBinaryQuantizedRescoreQuery(KnnByteVectorQuery query) {
    this.query = query;
  }

  @Override
  public String getField() {
    return this.query.getField();
  }

  @Nullable
  @Override
  public Query getFilter() {
    return this.query.getFilter();
  }

  @Override
  public VectorScorer approximateScorer(
      QuantizedByteVectorValues quantized, VectorSimilarityFunction similarityFunction)
      throws IOException {
    DequantizedByteVectorValues vectorValues =
        new DequantizedByteVectorValues(quantized, similarityFunction);
    return vectorValues.scorer(this.query.getTargetCopy());
  }

  /**
   * {@link ByteVectorValues#rescorer} is Lucene's contract for the highest-fidelity scorer: on the
   * mixed-source view returned by {@link Mongot01042BinaryQuantizedFlatVectorsReader} it routes to
   * the raw (full-precision) values, which carry the field's similarity function from their {@code
   * FieldInfo}. Their scorer dispatches through the format's {@code flatVectorScorer} — whose
   * non-quantized delegate is {@link
   * com.xgen.mongot.index.lucene.vector.Lucene99NativeMemorySegmentVectorScorer}, picking up native
   * SIMD scoring transparently when the segment is memory-mapped.
   */
  @Override
  public Optional<VectorScorer> fullFidelityScorer(LeafReader leafReader) throws IOException {
    @Nullable ByteVectorValues vectorValues = leafReader.getByteVectorValues(this.query.getField());
    if (vectorValues == null) {
      return Optional.empty();
    }
    return Optional.ofNullable(vectorValues.rescorer(this.query.getTargetCopy()));
  }

  @Override
  public void examineResultsAfterRescoring(TopDocs rescoredDocs) {
    if (Explain.isEnabled()
        && this.query instanceof InstrumentableKnnByteVectorQuery instrumentableQuery) {
      instrumentableQuery.examineResultsAfterRescoring(rescoredDocs);
    }
  }
}
