package com.xgen.mongot.index.lucene.quantization;

import com.xgen.mongot.index.lucene.explain.knn.InstrumentableKnnFloatVectorQuery;
import com.xgen.mongot.index.lucene.explain.tracing.Explain;
import java.io.IOException;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.DocIdSetIterator;
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

  @Override
  public VectorScorer fullFidelityScorer(
      LeafReader leafReader, VectorSimilarityFunction similarityFunction) throws IOException {
    FloatVectorValues vectorValues = leafReader.getFloatVectorValues(this.query.getField());
    FloatVectorValues copy = vectorValues.copy();
    FloatVectorValues.DocIndexIterator iterator = copy.iterator();

    float[] target = this.query.getTargetCopy();
    return new VectorScorer() {

      @Override
      public float score() throws IOException {
        return similarityFunction.compare(target, copy.vectorValue(iterator.index()));
      }

      @Override
      public DocIdSetIterator iterator() {
        return iterator;
      }
    };
  }

  @Override
  public void examineResultsAfterRescoring(TopDocs rescoredDocs) {
    if (Explain.isEnabled()
        && this.query instanceof InstrumentableKnnFloatVectorQuery instrumentableQuery) {
      instrumentableQuery.examineResultsAfterRescoring(rescoredDocs);
    }
  }
}
