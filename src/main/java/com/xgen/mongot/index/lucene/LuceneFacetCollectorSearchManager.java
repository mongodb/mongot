package com.xgen.mongot.index.lucene;

import com.xgen.mongot.index.lucene.explain.explainers.CollectorTimingFeatureExplainer;
import com.xgen.mongot.index.lucene.explain.profiler.ProfileCollectorManager;
import com.xgen.mongot.index.lucene.explain.timing.ExplainTimings;
import com.xgen.mongot.index.lucene.explain.tracing.Explain;
import com.xgen.mongot.index.lucene.searcher.LuceneIndexSearcher;
import com.xgen.mongot.index.query.sort.SequenceToken;
import java.io.IOException;
import java.util.Optional;
import org.apache.lucene.facet.FacetsCollector;
import org.apache.lucene.facet.FacetsCollectorManager;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.MultiCollectorManager;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TopDocs;

/**
 * The LuceneFacetCollectorSearchManager is used to manage the execution of a single lucene search.
 * It is used by the {@link LuceneSearchIndexReader} to run an initial search and generate
 * FacetCollectorQueryInfo with the info necessary to initialize a batch producer with the search
 * results, and either to build full {@link com.xgen.mongot.index.MetaResults} or initialize a
 * {@link LuceneFacetCollectorMetaBatchProducer} to produce intermediate meta buckets.
 */
class LuceneFacetCollectorSearchManager
    extends AbstractLuceneSearchManager<LuceneFacetCollectorSearchManager.FacetCollectorQueryInfo> {

  public static class FacetCollectorQueryInfo extends QueryInfo {
    public final FacetsCollector collector;

    public FacetCollectorQueryInfo(
        TopDocs topDocs, FacetsCollector collector, boolean luceneExhausted) {
      super(topDocs, luceneExhausted);
      this.collector = collector;
    }
  }

  public LuceneFacetCollectorSearchManager(
      Query luceneQuery, Optional<Sort> luceneSort, Optional<SequenceToken> searchAfter) {
    super(luceneQuery, luceneSort, searchAfter);
  }

  @Override
  public FacetCollectorQueryInfo initialSearch(
      LuceneIndexSearcherReference searcherReference, int batchSize) throws IOException {

    LuceneIndexSearcher searcher = searcherReference.getIndexSearcher();

    var readerLimit = Math.max(1, searcher.getIndexReader().maxDoc());
    var searchLimit = Math.min(readerLimit, batchSize);

    var searchCollectorManager = createCollectorManager(searchLimit, Integer.MAX_VALUE);

    var results =
        searcher.search(
            this.getLuceneQuery(),
            new MultiCollectorManager(getFacetsCollectorManager(), searchCollectorManager));

    var facetsCollector = (FacetsCollector) results[0];
    var topDocs = (TopDocs) results[1];

    maybePopulateScores(searcher, topDocs.scoreDocs);
    return new FacetCollectorQueryInfo(
        topDocs, facetsCollector, topDocs.scoreDocs.length < batchSize);
  }

  private CollectorManager<?, FacetsCollector> getFacetsCollectorManager() {
    FacetsCollectorManager facetsCollectorManager = new FacetsCollectorManager();

    if (!Explain.isEnabled()) {
      return facetsCollectorManager;
    }

    ExplainTimings facetCollectorExplainTimings =
        Explain.getQueryInfo()
            .map(
                queryInfo ->
                    queryInfo
                        .getFeatureExplainer(
                            CollectorTimingFeatureExplainer.class,
                            CollectorTimingFeatureExplainer::new)
                        .getFacetCollectorTimings())
            .orElseThrow();

    return new ProfileCollectorManager<>(facetsCollectorManager, facetCollectorExplainTimings);
  }
}
