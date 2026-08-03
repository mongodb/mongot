package com.xgen.mongot.index.lucene.query.custom;

import com.xgen.mongot.featureflag.Feature;
import com.xgen.mongot.featureflag.FeatureFlags;
import com.xgen.mongot.index.IndexMetricsUpdater;
import java.io.IOException;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.QueryTimeout;
import org.apache.lucene.search.AcceptDocs;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.KnnFloatVectorQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.search.knn.KnnCollectorManager;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.BitSetIterator;
import org.jetbrains.annotations.Nullable;

/**
 * A specialized implementation of Lucene's KnnFloatVectorQuery to integrate with Mongot's metrics
 * and serve as a potential extension point for future optimizations.
 */
public class MongotKnnFloatQuery extends KnnFloatVectorQuery {

  protected final IndexMetricsUpdater.QueryingMetricsUpdater metrics;
  protected final boolean hasFilter;
  private final FeatureFlags flags;

  /**
   * The default value of max connections for the bottom layer of the HNSW graph. We don't have
   * access to configured HnswOptions at query time, so we select the minimum valid value, which
   * biases the cost estimate toward HNSW search.
   */
  public static final double DEFAULT_M = 16;

  /**
   * This coefficient scales the estimate of work required to search the bottom layer of the graph
   * in terms of M. A larger value of M increases fan-out, requiring more scoring per hop, but
   * decreases the diameter of the graph, requiring fewer hops per search.
   */
  public static final double M_FACTOR = (DEFAULT_M - 1) / Math.log(DEFAULT_M);

  private static final double FILTER_CORRELATION_COEFF = .69;

  /** A convenience overload for creating an unfiltered KNN query. */
  public MongotKnnFloatQuery(
      IndexMetricsUpdater.QueryingMetricsUpdater metrics, String field, float[] target, int k) {
    this(metrics, FeatureFlags.getDefault(), field, target, k, null);
  }

  /**
   * Find the k nearest documents to the target vector according to the vectors in the given field.
   * target vector.
   *
   * @param metrics - a metrics updater that holds counters relating to query execution.
   * @param field – the lucene field name that has been indexed as a KnnFloatVectorField.
   * @param target – the target query vector
   * @param k – the number of documents to find
   * @param filter – an optional filter applied before the vector search, or null if the search is
   *     unfiltered.
   * @throws IllegalArgumentException – if k is less than 1
   */
  public MongotKnnFloatQuery(
      IndexMetricsUpdater.QueryingMetricsUpdater metrics,
      FeatureFlags flags,
      String field,
      float[] target,
      int k,
      @Nullable Query filter) {
    super(field, target, k, filter);
    this.metrics = metrics;
    this.flags = flags;
    this.hasFilter = filter != null;
  }

  boolean shouldUseFullScan(LeafReaderContext context, int visitedLimit) throws IOException {
    @Nullable FloatVectorValues vectors = context.reader().getFloatVectorValues(this.field);
    if (vectors == null || vectors.size() == 0) {
      return false;
    }
    int numVectors = vectors.size();
    int effectiveK = Math.min(this.k, numVectors);

    double fullScanCost = Math.clamp(visitedLimit - 1, 0, numVectors);

    if (fullScanCost <= effectiveK) {
      return true;
    }

    double selectivity = fullScanCost / numVectors;
    double hnswCost = expectedVectorsVisited(numVectors, effectiveK, selectivity);

    return fullScanCost < hnswCost;
  }

  static double expectedVectorsVisited(int n, int k, double selectivity) {
    double expectedVectorsVisited =
        .038 * M_FACTOR * k * Math.log(n) / Math.pow(selectivity, FILTER_CORRELATION_COEFF);
    return expectedUniqueSamples(n, expectedVectorsVisited);
  }

  /**
   * Returns the expected number of unique samples found when sampling `numSamples` items from a
   * universe of `n` items.
   */
  static double expectedUniqueSamples(int n, double numSamples) {
    return n * (1 - Math.exp(numSamples * Math.log1p(-1.0 / n)));
  }

  @Override
  protected TopDocs approximateSearch(
      LeafReaderContext context,
      AcceptDocs acceptDocs,
      int visitedLimit,
      KnnCollectorManager knnCollectorManager)
      throws IOException {
    // Note: visitedLimit = acceptDocs.cardinality() + 1 if using filtered search, otherwise is
    // Integer.MAX_VALUE.

    TopDocs result;
    if (acceptDocs.bits() instanceof BitSet bits && shouldUseFullScan(context, visitedLimit)) {

      // If flag is disabled, we still do hnsw and record whether full scan would have been better.
      if (this.flags.isEnabled(Feature.KNN_FULL_SCAN_HEURISTIC)) {
        // Note: Must not call exactSearch directly for correct explain reporting.
        return fullScanHeuristicSearch(context, new BitSetIterator(bits, visitedLimit - 1));
      } else {
        result = super.approximateSearch(context, acceptDocs, visitedLimit, knnCollectorManager);
        this.metrics.recordFallbackHeuristicResult(result.totalHits.relation());
      }

    } else {
      result = super.approximateSearch(context, acceptDocs, visitedLimit, knnCollectorManager);
    }

    var searchMode =
        result.totalHits.relation() == TotalHits.Relation.EQUAL_TO
            ? IndexMetricsUpdater.KnnSearchMode.APPROXIMATE
            : IndexMetricsUpdater.KnnSearchMode.FALLBACK_TO_EXACT;
    this.metrics.incrementKnnSearchMode(searchMode);

    // Record visited nodes - totalHits.value equals visited nodes right after approximate search
    this.metrics.recordVectorSearchVisitedNodes(
        result.totalHits.value(), this.hasFilter, IndexMetricsUpdater.KnnSearchMode.APPROXIMATE);
    // Record visited nodes per segment (for both approximate and fallback-to-exact)
    this.metrics.recordVectorSearchVisitedNodesPerSegment(
        result.totalHits.value(), this.hasFilter, searchMode);

    return result;
  }

  @Override
  protected TopDocs exactSearch(
      LeafReaderContext context, DocIdSetIterator acceptIterator, QueryTimeout queryTimeout)
      throws IOException {
    TopDocs result = super.exactSearch(context, acceptIterator, queryTimeout);

    // Record visited nodes - totalHits.value equals visited nodes right after exact search
    this.metrics.recordVectorSearchVisitedNodes(
        result.totalHits.value(), this.hasFilter, IndexMetricsUpdater.KnnSearchMode.EXACT);

    return result;
  }

  /**
   * Same as exactSearch, but only called by our full scan search heuristic. This allows it to be
   * instrumented separately.
   */
  protected TopDocs fullScanHeuristicSearch(LeafReaderContext context, BitSetIterator acceptDocs)
      throws IOException {
    TopDocs result = super.exactSearch(context, acceptDocs, null);

    // Record visited nodes - totalHits.value equals visited nodes right after exact search
    this.metrics.incrementKnnSearchMode(IndexMetricsUpdater.KnnSearchMode.FULL_SCAN);
    this.metrics.recordVectorSearchVisitedNodes(
        result.totalHits.value(), this.hasFilter, IndexMetricsUpdater.KnnSearchMode.FULL_SCAN);

    return result;
  }
}
