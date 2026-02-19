package com.xgen.mongot.index.lucene.explain.knn;

import com.xgen.mongot.index.IndexMetricsUpdater;
import com.xgen.mongot.index.lucene.query.custom.MongotKnnByteQuery;
import java.io.IOException;
import javax.annotation.Nullable;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.QueryTimeout;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.knn.KnnCollectorManager;
import org.apache.lucene.util.Bits;

/**
 * A decorator over MongotKnnByteQuery to intercept calls to 'approximateSearch' and 'exactSearch'
 * for instrumentation purposes while maintaining metrics tracking from the parent class.
 */
public class InstrumentableKnnByteVectorQuery extends MongotKnnByteQuery {

  private final KnnInstrumentationHelper instrumentationHelper;

  public InstrumentableKnnByteVectorQuery(
      IndexMetricsUpdater.QueryingMetricsUpdater metrics,
      KnnInstrumentationHelper instrumentationHelper,
      String field,
      byte[] target,
      int k) {
    super(metrics, field, target, k);
    this.instrumentationHelper = instrumentationHelper;
  }

  public InstrumentableKnnByteVectorQuery(
      IndexMetricsUpdater.QueryingMetricsUpdater metrics,
      KnnInstrumentationHelper instrumentationHelper,
      String field,
      byte[] target,
      int k,
      Query filter) {
    super(metrics, field, target, k, filter);
    this.instrumentationHelper = instrumentationHelper;
  }

  @Override
  protected KnnCollectorManager getKnnCollectorManager(int k, IndexSearcher searcher) {
    return new InstrumentedTopKnnCollectorManager(k, searcher, this.instrumentationHelper);
  }

  @Override
  protected TopDocs approximateSearch(
      LeafReaderContext context,
      @Nullable Bits acceptDocs,
      int visitedLimit,
      KnnCollectorManager knnCollectorManager)
      throws IOException {

    return this.instrumentationHelper.meteredApproximateSearch(
        context,
        acceptDocs,
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
  protected TopDocs mergeLeafResults(TopDocs[] perLeafResults) {
    return this.instrumentationHelper.meteredMergeLeafResults(
        perLeafResults, () -> super.mergeLeafResults(perLeafResults));
  }
}
