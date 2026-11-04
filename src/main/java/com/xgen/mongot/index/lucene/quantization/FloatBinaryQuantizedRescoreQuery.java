package com.xgen.mongot.index.lucene.quantization;

import com.xgen.mongot.index.lucene.explain.knn.InstrumentableKnnFloatVectorQuery;
import com.xgen.mongot.index.lucene.explain.tracing.Explain;
import java.io.IOException;
import java.util.Optional;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.KnnFloatVectorQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.VectorScorer;
import org.apache.lucene.util.quantization.QuantizedByteVectorValues;
import org.jetbrains.annotations.Nullable;

/** {@link BinaryQuantizedRescoreQuery} implementation backed by a float-encoded KNN query. */
public class FloatBinaryQuantizedRescoreQuery implements BinaryQuantizedRescoreQuery {

  private final KnnFloatVectorQuery query;

  public FloatBinaryQuantizedRescoreQuery(KnnFloatVectorQuery query) {
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
    DequantizedFloatVectorValues vectorValues =
        new DequantizedFloatVectorValues(quantized, similarityFunction);
    return vectorValues.scorer(this.query.getTargetCopy());
  }

  /**
   * {@link FloatVectorValues#rescorer} is Lucene's contract for the highest-fidelity scorer: on the
   * mixed-source view returned by {@link Mongot01042BinaryQuantizedFlatVectorsReader} it routes to
   * the raw (full-precision) values, which carry the field's similarity function from their {@code
   * FieldInfo}.
   */
  @Override
  public Optional<VectorScorer> fullFidelityScorer(LeafReader leafReader) throws IOException {
    @Nullable
    FloatVectorValues vectorValues = leafReader.getFloatVectorValues(this.query.getField());
    if (vectorValues == null) {
      return Optional.empty();
    }
    return Optional.ofNullable(vectorValues.rescorer(this.query.getTargetCopy()));
  }

  @Override
  public void examineResultsAfterRescoring(TopDocs rescoredDocs) {
    if (Explain.isEnabled()
        && this.query instanceof InstrumentableKnnFloatVectorQuery instrumentableQuery) {
      instrumentableQuery.examineResultsAfterRescoring(rescoredDocs);
    }
  }
}
