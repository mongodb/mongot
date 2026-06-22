package com.xgen.mongot.index.lucene;

import com.xgen.mongot.index.lucene.searcher.LuceneIndexSearcher;
import com.xgen.mongot.index.query.InvalidQueryException;
import com.xgen.mongot.index.query.sort.SequenceToken;
import java.io.IOException;
import java.util.Optional;
import org.apache.lucene.facet.DrillSideways.DrillSidewaysResult;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TopDocs;

/**
 * The LuceneFacetOptimizedDrillSidewaysSearchManager is used to manage the execution of a single
 * lucene search of an optimized drill sideways faceted search. It is used by the {@link
 * LuceneSearchIndexReader} to run an initial search and generate
 * OptimizedDrillSidewaysFacetCollectorQueryInfo with the info necessary to initialize a batch
 * producer with the search results, and either to build full {@link
 * com.xgen.mongot.index.MetaResults} or initialize a {@link LuceneFacetCollectorMetaBatchProducer}
 * to produce intermediate meta buckets.
 */
class LuceneFacetOptimizedDrillSidewaysSearchManager
    extends AbstractLuceneSearchManager<
        LuceneFacetOptimizedDrillSidewaysSearchManager
            .OptimizedDrillSidewaysFacetCollectorQueryInfo> {

  public static class OptimizedDrillSidewaysFacetCollectorQueryInfo extends QueryInfo {
    public final DrillSidewaysResult drillSidewaysResult;

    public OptimizedDrillSidewaysFacetCollectorQueryInfo(
        TopDocs topDocs, DrillSidewaysResult drillSidewaysResult, boolean luceneExhausted) {
      super(topDocs, luceneExhausted);
      this.drillSidewaysResult = drillSidewaysResult;
    }
  }

  private final LuceneDrillSideways luceneDrillSideways;

  public LuceneFacetOptimizedDrillSidewaysSearchManager(
      Query luceneQuery,
      LuceneDrillSideways luceneDrillSideways,
      Optional<Sort> luceneSort,
      Optional<SequenceToken> searchAfter) {
    super(luceneQuery, luceneSort, searchAfter);
    this.luceneDrillSideways = luceneDrillSideways;
  }

  @Override
  public OptimizedDrillSidewaysFacetCollectorQueryInfo initialSearch(
      LuceneIndexSearcherReference searcherReference, int batchSize)
      throws IOException, InvalidQueryException {

    LuceneIndexSearcher searcher = searcherReference.getIndexSearcher();

    var readerLimit = Math.max(1, searcher.getIndexReader().maxDoc());
    var searchLimit = Math.min(readerLimit, batchSize);

    var searchCollectorManager = createCollectorManager(searchLimit, Integer.MAX_VALUE);

    var drillSidewaysResult =
        this.luceneDrillSideways
            .drillSideways()
            .searchSafe(this.luceneDrillSideways.drillDownQuery(), searchCollectorManager);

    var topDocs = drillSidewaysResult.collectorResult;

    maybePopulateScores(searcher, topDocs.scoreDocs);
    return new OptimizedDrillSidewaysFacetCollectorQueryInfo(
        topDocs, drillSidewaysResult, topDocs.scoreDocs.length < batchSize);
  }
}
