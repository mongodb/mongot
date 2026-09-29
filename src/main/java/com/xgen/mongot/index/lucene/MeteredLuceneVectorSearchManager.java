package com.xgen.mongot.index.lucene;

import static com.xgen.mongot.index.IndexMetricsUpdater.QueryingMetricsUpdater;

import com.xgen.mongot.index.definition.quantization.VectorQuantization;
import com.xgen.mongot.index.query.InvalidQueryException;
import com.xgen.mongot.index.query.operators.ApproximateVectorSearchCriteria;
import com.xgen.mongot.index.query.operators.VectorSearchCriteria;
import com.xgen.mongot.metrics.Timed;
import java.io.IOException;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;

/**
 * Measures time spent on the actual Lucene search (topDocs collection part) for the vectorSearch
 * and collects addition metrics
 */
public class MeteredLuceneVectorSearchManager<T> implements LuceneSearchManager<T> {

  private final LuceneSearchManager<T> searchManager;
  private final QueryingMetricsUpdater metricsUpdater;
  private final VectorSearchCriteria criteria;
  private final VectorQuantization fieldQuantization;

  public MeteredLuceneVectorSearchManager(
      QueryingMetricsUpdater metricsUpdater,
      LuceneSearchManager<T> searchManager,
      VectorSearchCriteria criteria,
      VectorQuantization fieldQuantization) {
    this.searchManager = searchManager;
    this.metricsUpdater = metricsUpdater;
    this.criteria = criteria;
    this.fieldQuantization = fieldQuantization;
  }

  @Override
  public T initialSearch(LuceneIndexSearcherReference searcherReference, int batchSize)
      throws IOException, InvalidQueryException {

    if (this.criteria instanceof ApproximateVectorSearchCriteria approximateCriteria) {
      var metric =
          switch (this.fieldQuantization) {
            case VectorQuantization.NONE -> this.metricsUpdater.getNumCandidatesUnquantized();
            case VectorQuantization.SCALAR -> this.metricsUpdater.getNumCandidatesScalarQuantized();
            case VectorQuantization.BINARY -> this.metricsUpdater.getNumCandidatesBinaryQuantized();
          };
      metric.record(approximateCriteria.getNumCandidates());
    }
    this.metricsUpdater.getLimitPerQuery().record(this.criteria.limit());

    return Timed.<T, IOException, InvalidQueryException>supplier2(
        this.metricsUpdater.getVectorSearchInitialTopDocsLatencyTimer(),
        () -> this.searchManager.initialSearch(searcherReference, batchSize));
  }

  @Override
  public TopDocs getMoreTopDocs(
      LuceneIndexSearcherReference searcherReference, ScoreDoc lastScoreDoc, int batchSize)
      throws IOException {
    return Timed.supplier(
        this.metricsUpdater.getVectorSearchGetMoreTopDocsLatencyTimer(),
        () -> this.searchManager.getMoreTopDocs(searcherReference, lastScoreDoc, batchSize));
  }
}
