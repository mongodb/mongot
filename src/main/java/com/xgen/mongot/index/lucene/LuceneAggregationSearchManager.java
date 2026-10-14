package com.xgen.mongot.index.lucene;

import com.google.common.annotations.VisibleForTesting;
import com.xgen.mongot.index.SearchAggregationMetricsReporter;
import com.xgen.mongot.index.lucene.LuceneAggregationPlanResolver.AggregationPlan;
import com.xgen.mongot.index.lucene.explain.explainers.AggregationFeatureExplainer;
import com.xgen.mongot.index.lucene.explain.profiler.ProfileCollectorManager;
import com.xgen.mongot.index.lucene.explain.timing.ExplainTimings;
import com.xgen.mongot.index.lucene.explain.tracing.Explain;
import com.xgen.mongot.index.lucene.facet.TokenFacetsCardinalityLimitExceededException;
import com.xgen.mongot.index.lucene.facet.TokenFacetsStateCache;
import com.xgen.mongot.index.lucene.facet.TokenSsdvFacetState;
import com.xgen.mongot.index.lucene.searcher.LuceneIndexSearcher;
import com.xgen.mongot.index.query.aggregation.AggregationContext;
import com.xgen.mongot.index.query.aggregation.AggregationFieldResolver;
import com.xgen.mongot.index.query.aggregation.AggregationHeapAccountant;
import com.xgen.mongot.index.query.aggregation.AggregationResult;
import com.xgen.mongot.index.query.aggregation.TopAggregationSpec;
import com.xgen.mongot.index.query.sort.SequenceToken;
import com.xgen.mongot.util.Check;
import java.io.IOException;
import java.util.Optional;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.CollectorManager;
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

  /**
   * The query's global sort, borrowed by {@code first}/{@code last}, and the same sort inverted,
   * which {@code last} collects under. Held here rather than exposed on the shared search-manager
   * base class, which has no other reason to.
   */
  private final Optional<Sort> luceneSort;

  private final Optional<Sort> invertedLuceneSort;

  private final int indexPartitionId;

  // True only when this reduce is the outermost count merge, so it may prune to top-N before
  // materializing. Intermediate (partition or shard) results must keep full cardinality.
  private final boolean applyLimitOnReduce;

  private final SearchAggregationMetricsReporter metricsReporter;

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
      Optional<Sort> invertedLuceneSort,
      Optional<SequenceToken> searchAfter,
      AggregationPlan aggregationPlan,
      int indexPartitionId,
      boolean applyLimitOnReduce,
      SearchAggregationMetricsReporter metricsReporter) {
    super(luceneQuery, luceneSort, searchAfter);
    this.luceneSort = luceneSort;
    this.invertedLuceneSort = invertedLuceneSort;
    this.rootSpec = aggregationPlan.rootSpec();
    this.fieldResolver = aggregationPlan.fieldResolver();
    this.indexPartitionId = indexPartitionId;
    this.applyLimitOnReduce = applyLimitOnReduce;
    this.metricsReporter = ExplainAwareSearchAggregationMetricsReporter.wrap(metricsReporter);
  }

  @Override
  public AggregationQueryInfo initialSearch(
      LuceneIndexSearcherReference searcherReference, int batchSize) throws IOException {
    LuceneIndexSearcher searcher = searcherReference.getIndexSearcher();

    AggregationHeapAccountant heapAccountant = new AggregationHeapAccountant();
    AggregationContext context =
        AggregationContext.create(
            searcher.getIndexReader(),
            Optional.of(this.fieldResolver),
            cachedGlobalValueLookup(searcher),
            // first/last borrow the query's global sort to order the documents in a bucket, and
            // the partition id to order documents that the sort leaves tied. last collects under
            // the inverted sort, since a top-docs collector only ever keeps a top document.
            this.luceneSort,
            this.invertedLuceneSort,
            this.indexPartitionId,
            Explain.atVerbosity(Explain.Verbosity.EXECUTION_STATS),
            heapAccountant);

    int readerLimit = Math.max(1, searcher.getIndexReader().maxDoc());
    int searchLimit = Math.min(readerLimit, batchSize);

    var searchCollectorManager = createCollectorManager(searchLimit, Integer.MAX_VALUE);

    // Phase-level only: the reporter wraps the whole collector fan-out (per-slice collection plus
    // the slice reduce that materializes each slice's buckets), so it costs one pair of clock
    // reads per query rather than a read on the per-document collect path. Process metrics and
    // explain share this one call.
    Object[] results =
        this.metricsReporter.time(
            SearchAggregationMetricsReporter.Phase.COLLECTION,
            () ->
                searcher.search(
                    this.getLuceneQuery(),
                    new MultiCollectorManager(
                        getAccumulatorCollectorManager(context), searchCollectorManager)));

    // One sample per successful partition search: the context total already sums grouping nodes
    // and slices. Recording after time() so a thrown collection does not emit a partial sample.
    this.metricsReporter.recordObservedBuckets(context.observedBuckets());
    this.metricsReporter.recordObservedHeapBytes(heapAccountant.currentBytes());

    AggregationResult aggResult = Check.instanceOf(results[0], AggregationResult.class);
    TopDocs topDocs = Check.instanceOf(results[1], TopDocs.class);
    maybePopulateScores(searcher, topDocs.scoreDocs);

    return new AggregationQueryInfo(topDocs, topDocs.scoreDocs.length < batchSize, aggResult);
  }

  /**
   * Profiling the accumulator manager on its own separates the aggregation tree's share of
   * collection from the query iteration and the topdocs collector that the phase timer lumps
   * together. Costs a clock read per matched document, so only when the query is explained, and not
   * a process-level Micrometer timer: {@link SearchAggregationMetricsReporter} records the phase
   * wall time around {@code searcher.search} instead.
   */
  private CollectorManager<? extends Collector, AggregationResult> getAccumulatorCollectorManager(
      AggregationContext context) {
    AccumulatorCollectorManager accumulatorCollectorManager =
        new AccumulatorCollectorManager(
            this.rootSpec,
            context,
            AggregationFeatureExplainer::recordNodeStats,
            this.applyLimitOnReduce);

    Optional<ExplainTimings> accumulatorProfile =
        AggregationFeatureExplainer.currentAccumulatorProfile();
    if (accumulatorProfile.isEmpty()) {
      return accumulatorCollectorManager;
    }

    return new ProfileCollectorManager<>(accumulatorCollectorManager, accumulatorProfile.get());
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
