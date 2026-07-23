package com.xgen.mongot.index.lucene.quantization;

import com.xgen.mongot.util.Check;
import java.io.IOException;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.KnnByteVectorQuery;
import org.apache.lucene.search.KnnFloatVectorQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.VectorScorer;
import org.apache.lucene.util.quantization.QuantizedByteVectorValues;
import org.jetbrains.annotations.Nullable;

/**
 * Encoding-agnostic view of the KNN query being rescored by {@link BinaryQuantizedVectorRescorer}.
 *
 * <p>Lucene's {@code AbstractKnnVectorQuery} (the common supertype of {@code KnnFloatVectorQuery}
 * and {@code KnnByteVectorQuery}) is package-private. This wraps the extracted Lucene query into
 * the matching implementation ({@code FloatBinaryQuantizedRescoreQuery} or {@code
 * ByteBinaryQuantizedRescoreQuery}). The rescorer operates purely against this type. All
 * encoding-specific behavior lives behind the scorer factory methods.
 */
public interface BinaryQuantizedRescoreQuery {

  /**
   * Wraps the raw Lucene KNN query in the encoding-specific rescore-query view. Dispatches on the
   * concrete {@code Knn*VectorQuery} subtype to select {@link FloatBinaryQuantizedRescoreQuery} or
   * {@link ByteBinaryQuantizedRescoreQuery}.
   *
   * @param query the extracted Lucene KNN query
   * @return the matching rescore-query view
   */
  static BinaryQuantizedRescoreQuery from(Query query) {
    return switch (query) {
      case KnnFloatVectorQuery floatQuery -> new FloatBinaryQuantizedRescoreQuery(floatQuery);
      case KnnByteVectorQuery byteQuery -> new ByteBinaryQuantizedRescoreQuery(byteQuery);
      default ->
          Check.unreachable(
              "Unrecognized vector query shape — expected KnnFloatVectorQuery or"
                  + " KnnByteVectorQuery");
    };
  }

  /** The vector field this query targets. */
  String getField();

  /** The optional child filter carried by the query, or {@code null} if none. */
  @Nullable
  Query getFilter();

  /**
   * Builds a scorer for the approximate rescore, which compares the raw query vector against
   * dequantized binary vectors. The query is <em>not</em> quantized on this path.
   *
   * <p>Because the class for a vector's dequantized vector values is not type agnostic, it is
   * deferred to the implementation to wrap {@code quantized} in its respective dequantized vector
   * value class.
   *
   * @param quantized the segment's quantized byte vector values
   * @param similarityFunction the field's similarity function
   * @return the scorer for the approximate rescore.
   */
  VectorScorer approximateScorer(
      QuantizedByteVectorValues quantized, VectorSimilarityFunction similarityFunction)
      throws IOException;

  /**
   * Builds a scorer for the full-fidelity rescore, which compares the raw query vector against the
   * segment's full-precision stored vectors.
   *
   * @param leafReader the segment reader to load full-precision vectors from
   * @param similarityFunction the field's similarity function
   * @return the full fidelity scorer based on the full-precision vectors and similarity function.
   */
  VectorScorer fullFidelityScorer(
      LeafReader leafReader, VectorSimilarityFunction similarityFunction) throws IOException;

  /**
   * Reports the final rescored results to the wrapped query's explain instrumentation, if it
   * supports one. No-op if explain is disabled or the wrapped query isn't instrumentable.
   */
  void examineResultsAfterRescoring(TopDocs rescoredDocs);
}
