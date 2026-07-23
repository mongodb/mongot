package com.xgen.mongot.index.lucene.quantization;

import com.xgen.mongot.index.lucene.explain.knn.InstrumentableKnnByteVectorQuery;
import com.xgen.mongot.index.lucene.explain.tracing.Explain;
import java.io.IOException;
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.DocIdSetIterator;
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

  @Override
  public VectorScorer fullFidelityScorer(
      LeafReader leafReader, VectorSimilarityFunction similarityFunction) throws IOException {
    ByteVectorValues vectorValues = leafReader.getByteVectorValues(this.query.getField());
    ByteVectorValues copy = vectorValues.copy();
    ByteVectorValues.DocIndexIterator iterator = copy.iterator();

    byte[] target = this.query.getTargetCopy();
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
        && this.query instanceof InstrumentableKnnByteVectorQuery instrumentableQuery) {
      instrumentableQuery.examineResultsAfterRescoring(rescoredDocs);
    }
  }
}
