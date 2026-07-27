package com.xgen.mongot.index.lucene;

import com.google.common.annotations.VisibleForTesting;
import com.xgen.mongot.index.lucene.LuceneAggregationPlanResolver.AggregationPlan;
import com.xgen.mongot.index.lucene.facet.TokenFacetsCardinalityLimitExceededException;
import com.xgen.mongot.index.lucene.facet.TokenFacetsStateCache;
import com.xgen.mongot.index.lucene.facet.TokenSsdvFacetState;
import com.xgen.mongot.index.lucene.searcher.LuceneIndexSearcher;
import com.xgen.mongot.index.query.aggregation.AggregationContext;
import com.xgen.mongot.index.query.aggregation.AggregationFieldResolver;
import com.xgen.mongot.index.query.aggregation.AggregationResult;
import com.xgen.mongot.index.query.aggregation.TopAggregationSpec;
import com.xgen.mongot.index.query.sort.SequenceToken;
import com.xgen.mongot.util.Check;
import java.io.IOException;
import java.util.Optional;
import org.apache.lucene.search.MultiCollectorManager;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TopDocs;

/**
 * The LuceneAggregationSearchManager is used to manage the execution of a single lucene search. It
 * is used by the {@link LuceneSearchIndexReader} to run an initial search and generate
 * AggregationQueryInfo with the info necessary to initialize a batch producer with the search
 * results, and either to build full {@link com.xgen.mongot.index.MetaResults} or produce
 * intermediate meta buckets.
 */
public class LuceneAggregationSearchManager
    extends AbstractLuceneSearchManager<LuceneAggregationSearchManager.AggregationQueryInfo> {

  private final TopAggregationSpec rootSpec;
  private final AggregationFieldResolver fieldResolver;

  public static class AggregationQueryInfo extends QueryInfo {
    private final AggregationResult aggregationResult;

    public AggregationQueryInfo(
        TopDocs topDocs, boolean luceneExhausted, AggregationResult aggregationResult) {
      super(topDocs, luceneExhausted);
      this.aggregationResult = aggregationResult;
    }

    public AggregationResult getAggregationResult() {
      return this.aggregationResult;
    }
  }

  public LuceneAggregationSearchManager(
      Query luceneQuery,
      Optional<Sort> luceneSort,
      Optional<SequenceToken> searchAfter,
      AggregationPlan aggregationPlan) {
    super(luceneQuery, luceneSort, searchAfter);
    this.rootSpec = aggregationPlan.rootSpec();
    this.fieldResolver = aggregationPlan.fieldResolver();
  }

  @Override
  public AggregationQueryInfo initialSearch(
      LuceneIndexSearcherReference searcherReference, int batchSize) throws IOException {
    LuceneIndexSearcher searcher = searcherReference.getIndexSearcher();

    AggregationContext context =
        AggregationContext.create(
            searcher.getIndexReader(),
            Optional.of(this.fieldResolver),
            cachedGlobalValueLookup(searcher));

    int readerLimit = Math.max(1, searcher.getIndexReader().maxDoc());
    int searchLimit = Math.min(readerLimit, batchSize);

    var searchCollectorManager = createCollectorManager(searchLimit, Integer.MAX_VALUE);

    AccumulatorCollectorManager accCM = new AccumulatorCollectorManager(this.rootSpec, context);

    var results =
        searcher.search(
            this.getLuceneQuery(), new MultiCollectorManager(accCM, searchCollectorManager));

    AggregationResult aggResult = Check.instanceOf(results[0], AggregationResult.class);
    TopDocs topDocs = Check.instanceOf(results[1], TopDocs.class);
    maybePopulateScores(searcher, topDocs.scoreDocs);

    return new AggregationQueryInfo(topDocs, topDocs.scoreDocs.length < batchSize, aggResult);
  }

  /**
   * Serves top-level doc values for terms aggregations out of the searcher's {@link
   * com.xgen.mongot.index.lucene.facet.TokenFacetsStateCache}, so the expensive global OrdinalMap
   * is built once per (reader, field) rather than on every query. Returns empty — letting the
   * accumulator build an uncached view, matching previous behavior — when the cache is disabled or
   * when the shared-tier facet cardinality limit would reject the field (aggregations do not
   * enforce that limit).
   */
  @VisibleForTesting
  static AggregationContext.CachedGlobalValueLookup cachedGlobalValueLookup(
      LuceneIndexSearcher searcher) {
    return luceneField -> {
      Optional<TokenFacetsStateCache> cache = searcher.getTokenFacetsStateCache();
      if (cache.isEmpty()) {
        return Optional.empty();
      }
      Optional<TokenSsdvFacetState> state;
      try {
        state = cache.get().get(luceneField);
      } catch (TokenFacetsCardinalityLimitExceededException e) {
        return Optional.empty();
      }
      if (state.isEmpty()) {
        return Optional.empty();
      }
      return Optional.of(state.get().getDocValues());
    };
  }
}
