package com.xgen.mongot.index.lucene.explain.knn;

import com.xgen.mongot.featureflag.FeatureFlags;
import com.xgen.mongot.index.IndexMetricsUpdater;
import com.xgen.mongot.index.lucene.query.custom.MongotKnnFloatQuery;
import java.io.IOException;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.QueryTimeout;
import org.apache.lucene.search.AcceptDocs;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.knn.KnnCollectorManager;
import org.apache.lucene.util.BitSetIterator;
import org.jetbrains.annotations.Nullable;

/**
 * A decorator over MongotKnnFloatQuery to intercept calls to 'approximateSearch' and 'exactSearch'
 * for instrumentation purposes while maintaining metrics tracking from the parent class.
 */
public class InstrumentableKnnFloatVectorQuery extends MongotKnnFloatQuery {

  private final KnnInstrumentationHelper instrumentationHelper;

  public InstrumentableKnnFloatVectorQuery(
      IndexMetricsUpdater.QueryingMetricsUpdater metrics,
      FeatureFlags flags,
      KnnInstrumentationHelper instrumentationHelper,
      String field,
      float[] target,
      int k,
      @Nullable Query filter) {
    super(metrics, flags, field, target, k, filter);
    this.instrumentationHelper = instrumentationHelper;
  }

  @Override
  protected KnnCollectorManager getKnnCollectorManager(int k, IndexSearcher searcher) {
    return new InstrumentedTopKnnCollectorManager(k, searcher, this.instrumentationHelper);
  }

  @Override
  protected TopDocs approximateSearch(
      LeafReaderContext context,
      AcceptDocs acceptDocs,
      int visitedLimit,
      KnnCollectorManager knnCollectorManager)
      throws IOException {

    return this.instrumentationHelper.meteredApproximateSearch(
        context,
        acceptDocs.bits(),
        () -> super.approximateSearch(context, acceptDocs, visitedLimit, knnCollectorManager));
  }

  @Override
  protected TopDocs exactSearch(
      LeafReaderContext context, DocIdSetIterator acceptIterator, QueryTimeout queryTimeout)
      throws IOException {

    return this.instrumentationHelper.meteredExactSearch(
        context, acceptIterator, () -> super.exactSearch(context, acceptIterator, queryTimeout));
  }

  @Override
  protected TopDocs fullScanHeuristicSearch(LeafReaderContext context, BitSetIterator acceptDocs)
      throws IOException {
    return this.instrumentationHelper.meteredFullScanHeuristicSearch(
        context, acceptDocs, () -> super.fullScanHeuristicSearch(context, acceptDocs));
  }

  @Override
  protected TopDocs mergeLeafResults(TopDocs[] perLeafResults) {
    return this.instrumentationHelper.meteredMergeLeafResults(
        perLeafResults, () -> super.mergeLeafResults(perLeafResults));
  }

  /**
   * Reports the final rescored results to the instrumentation helper so that tracing docs dropped
   * during rescoring can be marked. Called by {@code BinaryQuantizedVectorRescorer} after two-stage
   * rescoring completes.
   *
   * @param rescoredDocs the final rescored top docs
   */
  public void examineResultsAfterRescoring(TopDocs rescoredDocs) {
    this.instrumentationHelper.examineResultsAfterRescoring(rescoredDocs);
  }
}
