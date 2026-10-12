package com.xgen.mongot.index.lucene;

import static com.xgen.mongot.util.Check.checkState;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.flogger.FluentLogger;
import com.google.errorprone.annotations.Var;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import com.xgen.mongot.cursor.batch.BatchSizeStrategy;
import com.xgen.mongot.cursor.batch.QueryCursorOptions;
import com.xgen.mongot.featureflag.Feature;
import com.xgen.mongot.featureflag.FeatureFlags;
import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlagRegistry;
import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlags;
import com.xgen.mongot.index.BatchProducer;
import com.xgen.mongot.index.CountMetaBatchProducer;
import com.xgen.mongot.index.IndexMetricsUpdater;
import com.xgen.mongot.index.MeteredBatchProducer;
import com.xgen.mongot.index.ReaderClosedException;
import com.xgen.mongot.index.SearchAggregationMetricsReporter;
import com.xgen.mongot.index.SearchIndexReader;
import com.xgen.mongot.index.SearchResult;
import com.xgen.mongot.index.VectorSearchResult;
import com.xgen.mongot.index.analyzer.AnalyzerRegistry;
import com.xgen.mongot.index.definition.SearchFieldDefinitionResolver;
import com.xgen.mongot.index.definition.SearchIndexDefinition;
import com.xgen.mongot.index.lucene.LuceneAggregationSearchManager.AggregationQueryInfo;
import com.xgen.mongot.index.lucene.LuceneFacetCollectorSearchManager.FacetCollectorQueryInfo;
import com.xgen.mongot.index.lucene.LuceneSearchManager.QueryInfo;
import com.xgen.mongot.index.lucene.config.LuceneConfig;
import com.xgen.mongot.index.lucene.explain.explainers.FacetFeatureExplainer;
import com.xgen.mongot.index.lucene.explain.tracing.Explain;
import com.xgen.mongot.index.lucene.facet.TokenFacetsStateCache;
import com.xgen.mongot.index.lucene.field.FieldName;
import com.xgen.mongot.index.lucene.field.FieldValue;
import com.xgen.mongot.index.lucene.quantization.BinaryQuantizedVectorRescorer;
import com.xgen.mongot.index.lucene.query.LuceneSearchQueryFactoryDistributor;
import com.xgen.mongot.index.lucene.query.pushdown.project.ProjectFactory;
import com.xgen.mongot.index.lucene.query.pushdown.project.ProjectSpec;
import com.xgen.mongot.index.lucene.query.sort.IndexSortUtils;
import com.xgen.mongot.index.lucene.query.util.DisableBulkScorerQuery;
import com.xgen.mongot.index.lucene.searcher.LuceneIndexSearcher;
import com.xgen.mongot.index.lucene.searcher.LuceneSearcherManager;
import com.xgen.mongot.index.query.CollectorQuery;
import com.xgen.mongot.index.query.InvalidQueryException;
import com.xgen.mongot.index.query.MaterializedVectorSearchQuery;
import com.xgen.mongot.index.query.OperatorQuery;
import com.xgen.mongot.index.query.Pagination;
import com.xgen.mongot.index.query.Query;
import com.xgen.mongot.index.query.QueryOptimizationFlags;
import com.xgen.mongot.index.query.ReturnScope;
import com.xgen.mongot.index.query.SearchQuery;
import com.xgen.mongot.index.query.VectorSearchQuery;
import com.xgen.mongot.index.query.aggregation.ShardMergeAliasPlan;
import com.xgen.mongot.index.query.collectors.AggregationCollector;
import com.xgen.mongot.index.query.collectors.AggregationValidator;
import com.xgen.mongot.index.query.collectors.Collector;
import com.xgen.mongot.index.query.collectors.DrillSidewaysInfoBuilder;
import com.xgen.mongot.index.query.collectors.DrillSidewaysInfoBuilder.DrillSidewaysInfo;
import com.xgen.mongot.index.query.collectors.DrillSidewaysInfoBuilder.DrillSidewaysInfo.QueryOptimizationStatus;
import com.xgen.mongot.index.query.collectors.FacetCollector;
import com.xgen.mongot.index.query.counts.Count;
import com.xgen.mongot.index.query.operators.Operator;
import com.xgen.mongot.index.query.sort.SequenceToken;
import com.xgen.mongot.index.query.sort.SortSpec;
import com.xgen.mongot.index.query.sort.UserFieldSortOptions;
import com.xgen.mongot.index.synonym.SynonymRegistry;
import com.xgen.mongot.index.version.IndexFormatVersion;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.CheckedStream;
import com.xgen.mongot.util.bson.Vector;
import com.xgen.mongot.util.concurrent.LockGuard;
import com.xgen.mongot.util.concurrent.NamedExecutorService;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.facet.DrillSideways.DrillSidewaysResult;
import org.apache.lucene.facet.FacetsCollector;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.CollectionStatistics;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;

/** This class reads from a single Lucene index. */
@SuppressWarnings("GuardedBy") // Uses LockGuard instead
public class LuceneSearchIndexReader implements SearchIndexReader {
  private static final FluentLogger FLOGGER = FluentLogger.forEnclosingClass();

  private final DynamicFeatureFlagRegistry dynamicFeatureFlagRegistry;

  private final LuceneSearchQueryFactoryDistributor queryFactory;

  /**
   * These exclusive/shared locks allow for many different threads to use the LuceneIndexReader
   * concurrently (locking in the shared mode), but locks out all other threads if a thread is
   * close()-ing the LuceneIndexReader.
   *
   * <p>This enables good performance during the common case (i.e. LuceneIndexReader is open, many
   * threads accessing it), while preventing race conditions while shutting down (e.g. returning
   * from close() (and subsequently having the SearcherManager closed) while query() is being run on
   * a different thread, resulting in query() trying to use a closed SearcherManager.
   */
  private final ReentrantReadWriteLock.WriteLock shutdownExclusiveLock;

  private final ReentrantReadWriteLock.ReadLock shutdownSharedLock;

  private final SearchIndexDefinition indexDefinition;

  /**
   * In order to do anything with either SearcherManager, you must acquire the shutdownSharedLock to
   * ensure that the LuceneIndexReader (and subsequently the SearcherManager) will not be closed()
   * while processing.
   */
  @GuardedBy("shutdownSharedLock")
  private final LuceneSearcherManager searcherManager;

  private final LuceneHighlighterContext highlighterContext;

  private final LuceneFacetContext facetContext;

  private final Optional<NamedExecutorService> concurrentSearchExecutor;

  private final LuceneMetaResultsBuilder metaResultsBuilder;

  /**
   * In order to read closed's value you must acquire at least the shutdownSharedLock.
   *
   * <p>In order to flip closed's value you must acquire the shutdownExclusiveLock.
   *
   * <p>Note that @GuardedBy is not capable of expressing something being guarded by either of two
   * locks, so we do not annotate this variable.
   */
  private boolean closed;

  private final IndexMetricsUpdater.QueryingMetricsUpdater queryingMetricsUpdater;

  private final LuceneSearchManagerFactory luceneSearchManagerFactory;

  private final int indexPartitionId;
  private final FeatureFlags featureFlags;

  /**
   * Plan-time cap on a search aggregation query's estimated total bucket count.
   * LuceneConfig.DEFAULT_AGGREGATION_MAX_BUCKETS is used if not configured.
   */
  private final int aggregationMaxBuckets;

  private LuceneSearchIndexReader(
      SearchIndexDefinition indexDefinition,
      LuceneSearchQueryFactoryDistributor queryFactory,
      LuceneSearcherManager searcherManager,
      LuceneHighlighterContext highlighterContext,
      LuceneFacetContext facetContext,
      IndexMetricsUpdater.QueryingMetricsUpdater queryingMetricsUpdater,
      LuceneSearchManagerFactory luceneSearchManagerFactory,
      Optional<NamedExecutorService> concurrentSearchExecutor,
      Optional<Integer> aggregationMaxBuckets,
      int indexPartitionId,
      FeatureFlags featureFlags,
      DynamicFeatureFlagRegistry dynamicFeatureFlagRegistry) {
    this.indexDefinition = indexDefinition;
    this.queryFactory = queryFactory;
    this.searcherManager = searcherManager;
    this.highlighterContext = highlighterContext;
    this.facetContext = facetContext;
    this.queryingMetricsUpdater = queryingMetricsUpdater;
    this.luceneSearchManagerFactory = luceneSearchManagerFactory;
    this.concurrentSearchExecutor = concurrentSearchExecutor;
    this.metaResultsBuilder =
        new LuceneMetaResultsBuilder(facetContext, concurrentSearchExecutor, queryFactory);
    this.indexPartitionId = indexPartitionId;
    this.featureFlags = featureFlags;
    this.dynamicFeatureFlagRegistry = dynamicFeatureFlagRegistry;
    this.aggregationMaxBuckets =
        aggregationMaxBuckets.orElse(LuceneConfig.DEFAULT_AGGREGATION_MAX_BUCKETS);

    ReentrantReadWriteLock shutdownLock = new ReentrantReadWriteLock(true);
    this.shutdownExclusiveLock = shutdownLock.writeLock();
    this.shutdownSharedLock = shutdownLock.readLock();

    this.closed = false;
  }

  @VisibleForTesting
  static LuceneSearchIndexReader create(
      LuceneSearchQueryFactoryDistributor queryFactory,
      LuceneSearcherManager searcherManager,
      SearchIndexDefinition indexDefinition,
      LuceneHighlighterContext highlighterContext,
      LuceneFacetContext facetContext,
      IndexMetricsUpdater.QueryingMetricsUpdater queryingMetricsUpdater,
      LuceneSearchManagerFactory luceneSearchManagerFactory,
      Optional<NamedExecutorService> concurrentSearchExecutor,
      Optional<Integer> aggregationMaxBuckets,
      int indexPartitionId,
      FeatureFlags featureFlags,
      DynamicFeatureFlagRegistry dynamicFeatureFlagRegistry) {

    return new LuceneSearchIndexReader(
        indexDefinition,
        queryFactory,
        searcherManager,
        highlighterContext,
        facetContext,
        queryingMetricsUpdater,
        luceneSearchManagerFactory,
        concurrentSearchExecutor,
        aggregationMaxBuckets,
        indexPartitionId,
        featureFlags,
        dynamicFeatureFlagRegistry);
  }

  static LuceneSearchIndexReader create(
      LuceneSearcherManager searcherManager,
      SearchIndexDefinition indexDefinition,
      IndexFormatVersion indexFormatVersion,
      AnalyzerRegistry analyzerRegistry,
      SynonymRegistry synonymRegistry,
      Analyzer indexAnalyzer,
      IndexMetricsUpdater.QueryingMetricsUpdater queryingMetricsUpdater,
      Optional<NamedExecutorService> concurrentSearchExecutor,
      Optional<NamedExecutorService> concurrentVectorRescoringExecutor,
      boolean enableTextOperatorNewSynonymsSyntax,
      Optional<Integer> aggregationMaxBuckets,
      int indexPartitionId,
      FeatureFlags featureFlags,
      DynamicFeatureFlagRegistry dynamicFeatureFlagRegistry) {
    LuceneSearchQueryFactoryDistributor queryFactory =
        LuceneSearchQueryFactoryDistributor.create(
            indexDefinition,
            indexFormatVersion,
            analyzerRegistry,
            synonymRegistry,
            queryingMetricsUpdater,
            enableTextOperatorNewSynonymsSyntax,
            featureFlags,
            dynamicFeatureFlagRegistry);
    SearchFieldDefinitionResolver fieldDefinitionResolver =
        indexDefinition.createFieldDefinitionResolver(indexFormatVersion);

    LuceneHighlighterContext highlighterContext =
        new LuceneHighlighterContext(fieldDefinitionResolver, indexAnalyzer);
    LuceneFacetContext facetContext =
        new LuceneFacetContext(
            fieldDefinitionResolver, indexDefinition.getIndexCapabilities(indexFormatVersion));
    return create(
        queryFactory,
        searcherManager,
        indexDefinition,
        highlighterContext,
        facetContext,
        queryingMetricsUpdater,
        new LuceneSearchManagerFactory(
            fieldDefinitionResolver,
            new BinaryQuantizedVectorRescorer(concurrentVectorRescoringExecutor),
            queryingMetricsUpdater),
        concurrentSearchExecutor,
        aggregationMaxBuckets,
        indexPartitionId,
        featureFlags,
        dynamicFeatureFlagRegistry);
  }

  SearchAggregationMetricsReporter getSearchAggregationMetricsReporter() {
    return this.queryingMetricsUpdater.getSearchAggregationMetricsReporter();
  }

  @Override
  public SearchProducerAndMetaResults query(
      Query query,
      QueryCursorOptions queryCursorOptions,
      BatchSizeStrategy batchSizeStrategy,
      QueryOptimizationFlags queryOptimizationFlags)
      throws IOException, InvalidQueryException, InterruptedException {
    if (query.returnScope().isEmpty()
        && query.returnStoredSource()
        && this.indexDefinition.getStoredSource().isAllExcluded()) {
      throw new InvalidQueryException(
          "storedSource is not configured for this index. "
              + "For queries on 'embeddedDocument' fields with 'storedSource', "
              + "the 'returnScope' option must be populated");
    }
    try (LockGuard ignored = LockGuard.with(this.shutdownSharedLock)) {
      ensureOpen("query");
      var searcherReference = createSearcherReference(query.concurrent());
      try {
        return query(
            query,
            searcherReference,
            queryCursorOptions,
            batchSizeStrategy,
            queryOptimizationFlags);
      } catch (Exception e) {
        searcherReference.close();
        throw e;
      }
    }
  }

  private SearchProducerAndMetaResults query(
      Query query,
      LuceneIndexSearcherReference searcherReference,
      QueryCursorOptions queryCursorOptions,
      BatchSizeStrategy batchSizeStrategy,
      QueryOptimizationFlags queryOptimizationFlags)
      throws InvalidQueryException, IOException, InterruptedException {

    LuceneIndexSearcher indexSearcher = searcherReference.getIndexSearcher();
    return switch (query) {
      case VectorSearchQuery vectorSearchQuery -> {
        // This path is only for $vectorSearch over $search index. No need to support auto-embed
        // text queries here.
        Vector vector = Check.isPresent(vectorSearchQuery.criteria().queryVector(), "queryVector");
        var materializedQuery = new MaterializedVectorSearchQuery(vectorSearchQuery, vector);
        // Vector search query over search index
        org.apache.lucene.search.Query luceneQuery =
            Explain.isEnabled()
                ? this.queryFactory.createVectorSearchExplainQuery(
                    materializedQuery, indexSearcher.getIndexReader())
                : this.queryFactory.createVectorQuery(
                    materializedQuery, indexSearcher.getIndexReader());
        yield vectorSearchQuery(
            vectorSearchQuery, luceneQuery, searcherReference, batchSizeStrategy);
      }
      case SearchQuery searchQuery -> {
        Operator operator = getOperator(searchQuery);
        @Var
        org.apache.lucene.search.Query luceneQuery =
            Explain.isEnabled()
                ? this.queryFactory.createSearchExplainQuery(
                    operator,
                    indexSearcher.getIndexReader(),
                    searchQuery.returnScope(),
                    queryOptimizationFlags)
                : this.queryFactory.createQuery(
                    operator,
                    indexSearcher.getIndexReader(),
                    searchQuery.returnScope(),
                    queryOptimizationFlags);

        Optional<LuceneUnifiedHighlighter> unifiedHighlighter =
            this.highlighterContext.getHighlighterIfPresent(
                indexSearcher,
                searchQuery.highlight(),
                luceneQuery,
                operator,
                searchQuery.returnScope(),
                queryOptimizationFlags);

        Optional<LuceneScoreDetailsManager> scoreDetailsManager =
            LuceneScoreDetailsManager.getScoreDetailsManagerIfPresent(
                searchQuery.scoreDetails(), luceneQuery, queryOptimizationFlags);

        Optional<SequenceToken> searchAfter =
            searchQuery.pagination().map(Pagination::sequenceToken);

        Optional<Sort> indexSort =
            this.featureFlags.isEnabled(Feature.SORTED_INDEX)
                ? IndexSortUtils.extractFirstIndexSort(indexSearcher.getIndexReader())
                : Optional.empty();

        Optional<Sort> luceneSort =
            this.queryFactory.createSort(
                searchQuery.sortSpec(),
                searchAfter,
                indexSearcher.getFieldToSortableTypesMapping(),
                searchQuery.returnScope(),
                queryOptimizationFlags,
                indexSort,
                searchQuery.sortDrivesAggregation(),
                indexSearcher.getIndexReader(),
                embeddedScopedSortEnabled(searchQuery));
        trackIndexSortMetrics(luceneSort, indexSort);

        luceneQuery =
            maybeDisableBulkScorerForSort(luceneQuery, luceneSort, this.dynamicFeatureFlagRegistry);

        yield switch (searchQuery) {
          case OperatorQuery operatorQuery ->
              operatorQuery(
                  operatorQuery,
                  luceneQuery,
                  searcherReference,
                  batchSizeStrategy,
                  unifiedHighlighter,
                  scoreDetailsManager,
                  luceneSort,
                  searchAfter,
                  queryCursorOptions,
                  queryOptimizationFlags);
          case CollectorQuery collectorQuery ->
              collectorQuery(
                  collectorQuery,
                  luceneQuery,
                  searcherReference,
                  batchSizeStrategy,
                  unifiedHighlighter,
                  scoreDetailsManager,
                  luceneSort,
                  searchAfter,
                  queryCursorOptions,
                  queryOptimizationFlags);
        };
      }
    };
  }

  @Override
  public SearchProducerAndMetaProducer intermediateQuery(
      SearchQuery query,
      QueryCursorOptions queryCursorOptions,
      BatchSizeStrategy batchSizeStrategy,
      QueryOptimizationFlags queryOptimizationFlags)
      throws IOException, InvalidQueryException, InterruptedException {
    if (query.returnScope().isEmpty()
        && query.returnStoredSource()
        && this.indexDefinition.getStoredSource().isAllExcluded()) {
      throw new InvalidQueryException(
          "storedSource is not configured for this index. "
              + "For queries on 'embeddedDocument' fields with 'storedSource', "
              + "the 'returnScope' option must be populated");
    }
    try (LockGuard ignored = LockGuard.with(this.shutdownSharedLock)) {
      ensureOpen("query");
      var searcherReference = createSearcherReference(query.concurrent());
      try {
        return intermediateQuery(
            query,
            searcherReference,
            queryCursorOptions,
            batchSizeStrategy,
            queryOptimizationFlags);
      } catch (Exception e) {
        searcherReference.close();
        throw e;
      }
    }
  }

  private SearchProducerAndMetaProducer intermediateQuery(
      SearchQuery query,
      LuceneIndexSearcherReference searcherReference,
      QueryCursorOptions queryCursorOptions,
      BatchSizeStrategy batchSizeStrategy,
      QueryOptimizationFlags queryOptimizationFlags)
      throws InvalidQueryException, IOException, InterruptedException {
    Operator operator = getOperator(query);
    LuceneIndexSearcher indexSearcher = searcherReference.getIndexSearcher();

    @Var
    org.apache.lucene.search.Query luceneQuery =
        Explain.isEnabled()
            ? this.queryFactory.createSearchExplainQuery(
                operator,
                indexSearcher.getIndexReader(),
                query.returnScope(),
                queryOptimizationFlags)
            : this.queryFactory.createQuery(
                operator,
                indexSearcher.getIndexReader(),
                query.returnScope(),
                queryOptimizationFlags);
    Optional<LuceneUnifiedHighlighter> unifiedHighlighter =
        this.highlighterContext.getHighlighterIfPresent(
            indexSearcher,
            query.highlight(),
            luceneQuery,
            operator,
            query.returnScope(),
            queryOptimizationFlags);

    Optional<LuceneScoreDetailsManager> scoreDetailsManager =
        LuceneScoreDetailsManager.getScoreDetailsManagerIfPresent(
            query.scoreDetails(), luceneQuery, queryOptimizationFlags);

    Optional<SequenceToken> searchAfter = query.pagination().map(Pagination::sequenceToken);

    Optional<Sort> indexSort =
        this.featureFlags.isEnabled(Feature.SORTED_INDEX)
            ? IndexSortUtils.extractFirstIndexSort(indexSearcher.getIndexReader())
            : Optional.empty();

    Optional<Sort> luceneSort =
        this.queryFactory.createSort(
            query.sortSpec(),
            searchAfter,
            indexSearcher.getFieldToSortableTypesMapping(),
            query.returnScope(),
            queryOptimizationFlags,
            indexSort,
            query.sortDrivesAggregation(),
            indexSearcher.getIndexReader(),
            embeddedScopedSortEnabled(query));
    trackIndexSortMetrics(luceneSort, indexSort);

    luceneQuery =
        maybeDisableBulkScorerForSort(luceneQuery, luceneSort, this.dynamicFeatureFlagRegistry);

    return switch (query) {
      case OperatorQuery operatorQuery ->
          intermediateOperatorQuery(
              operatorQuery,
              luceneQuery,
              searcherReference,
              batchSizeStrategy,
              unifiedHighlighter,
              scoreDetailsManager,
              luceneSort,
              searchAfter,
              queryCursorOptions,
              queryOptimizationFlags);
      case CollectorQuery collectorQuery ->
          intermediateCollectorQuery(
              collectorQuery,
              luceneQuery,
              searcherReference,
              batchSizeStrategy,
              unifiedHighlighter,
              scoreDetailsManager,
              luceneSort,
              searchAfter,
              queryCursorOptions,
              queryOptimizationFlags);
    };
  }

  /** returns number of mongodb docs in indexes with embedded documents. */
  @Override
  public long getNumEmbeddedRootDocuments() throws IOException, ReaderClosedException {
    try (LockGuard ignored = LockGuard.with(this.shutdownSharedLock)) {
      if (this.closed) {
        throw ReaderClosedException.create("getNumEmbeddedRootDocuments");
      }
      try (var searcherReference = createSearcherReference(false)) {
        if (this.featureFlags.isEnabled(Feature.ACCURATE_NUM_EMBEDDED_ROOT_DOCS_METRIC)) {
          return searcherReference
              .getIndexSearcher()
              .count(
                  new TermQuery(
                      new Term(
                          FieldName.MetaField.EMBEDDED_ROOT.getLuceneFieldName(),
                          FieldValue.EMBEDDED_ROOT_FIELD_VALUE)));
        }
        return Optional.ofNullable(
                searcherReference
                    .getIndexSearcher()
                    .collectionStatistics(FieldName.MetaField.EMBEDDED_ROOT.getLuceneFieldName()))
            .map(CollectionStatistics::docCount)
            .orElse(0L);
      }
    }
  }

  @Override
  public List<FieldInfos> getFieldInfos() throws IOException, ReaderClosedException {
    try (LockGuard ignored = LockGuard.with(this.shutdownSharedLock)) {
      if (this.closed) {
        throw ReaderClosedException.create("getFieldInfos");
      }
      try (var searcherReference = createSearcherReference(false)) {
        return List.of(
            FieldInfos.getMergedFieldInfos(searcherReference.getIndexSearcher().getIndexReader()));
      }
    }
  }

  @Override
  public void refresh() throws IOException {
    try (LockGuard ignored = LockGuard.with(this.shutdownSharedLock)) {
      ensureOpen("refresh");
      this.searcherManager.maybeRefreshBlocking();
    }
  }

  @Override
  public void open() {
    try (LockGuard ignored = LockGuard.with(this.shutdownExclusiveLock)) {
      this.closed = false;
    }
  }

  /**
   * Must be called after ensuring all LuceneSearchResultBatchIterators returned by this
   * LuceneIndexReader will not be iterated anymore.
   */
  @Override
  public void close() {
    try (LockGuard ignored = LockGuard.with(this.shutdownExclusiveLock)) {
      this.closed = true;
    }
  }

  private SearchProducerAndMetaResults vectorSearchQuery(
      VectorSearchQuery query,
      org.apache.lucene.search.Query luceneQuery,
      LuceneIndexSearcherReference searcherReference,
      BatchSizeStrategy batchSizeStrategy)
      throws IOException, InvalidQueryException {

    LuceneSearchManager<QueryInfo> operatorSearchManager =
        this.luceneSearchManagerFactory.newVectorQueryManager(luceneQuery, query.criteria());

    var queryInfo =
        operatorSearchManager.initialSearch(
            searcherReference, batchSizeStrategy.adviseNextBatchSize());

    // TODO(CLOUDP-280897): remove count from interacting with vector search at all, we will not
    // surface count for vectorsearch.
    return new SearchProducerAndMetaResults(
        vectorSearchBatchProducer(
            operatorSearchManager,
            queryInfo.topDocs,
            queryInfo.luceneExhausted,
            batchSizeStrategy,
            searcherReference),
        this.metaResultsBuilder.getCountMetaResults(queryInfo.topDocs, Count.DEFAULT.type()));
  }

  private SearchProducerAndMetaResults operatorQuery(
      OperatorQuery query,
      org.apache.lucene.search.Query luceneQuery,
      LuceneIndexSearcherReference searcherReference,
      BatchSizeStrategy batchSizeStrategy,
      Optional<LuceneUnifiedHighlighter> unifiedHighlighter,
      Optional<LuceneScoreDetailsManager> scoreDetailsManager,
      Optional<Sort> luceneSort,
      Optional<SequenceToken> searchAfter,
      QueryCursorOptions queryCursorOptions,
      QueryOptimizationFlags queryOptimizationFlags)
      throws IOException, InvalidQueryException {

    LuceneSearchManager<QueryInfo> operatorSearchManager =
        this.luceneSearchManagerFactory.newOperatorManager(
            query, luceneQuery, luceneSort, searchAfter);

    var queryInfo =
        operatorSearchManager.initialSearch(
            searcherReference, batchSizeStrategy.adviseNextBatchSize());

    return new SearchProducerAndMetaResults(
        searchBatchProducer(
            query,
            operatorSearchManager,
            queryInfo.topDocs,
            queryInfo.luceneExhausted,
            batchSizeStrategy,
            searcherReference,
            unifiedHighlighter,
            scoreDetailsManager,
            queryCursorOptions,
            queryOptimizationFlags),
        this.metaResultsBuilder.getCountMetaResults(queryInfo.topDocs, query.count().type()));
  }

  /**
   * Evaluated per query (not cached) so a runtime toggle of the dynamic flag takes effect on
   * existing readers, matching how {@code DRILL_SIDEWAYS_FACETING} is evaluated.
   */
  private boolean embeddedScopedFacetsEnabled() {
    return this.dynamicFeatureFlagRegistry.evaluateClusterInvariant(
        DynamicFeatureFlags.ENABLE_EMBEDDED_SCOPED_FACET);
  }

  /**
   * Evaluated per query so a runtime toggle of the requested-bucket cap takes effect on existing
   * readers.
   */
  private boolean enforceRequestedBucketLimit() {
    return this.dynamicFeatureFlagRegistry.evaluateClusterInvariant(
        DynamicFeatureFlags.ENABLE_SEARCH_AGGREGATION_REQUESTED_BUCKET_LIMIT);
  }

  /** Evaluated per query so a runtime toggle takes effect, mirroring scoped facets. */
  private boolean embeddedScopedSortEnabled(SearchQuery query) {
    return query.rawSortSpec().stream()
            .flatMap(sort -> sort.getSortFields().stream())
            .anyMatch(
                field ->
                    field.options() instanceof UserFieldSortOptions options
                        && options.embeddedScope().isPresent())
        && this.dynamicFeatureFlagRegistry.evaluateClusterInvariant(
            DynamicFeatureFlags.ENABLE_EMBEDDED_SCOPED_SORT);
  }

  /**
   * Schema-checks the collector, lowers it once, and (when the DFF is on) rejects trees whose
   * requested bucket space exceeds {@link
   * com.xgen.mongot.index.query.aggregation.TopAggregationSpec#MAX_REQUESTED_BUCKETS} using that
   * already-built spec.
   */
  private LuceneAggregationPlanResolver.AggregationPlan resolveValidatedAggregationPlan(
      AggregationCollector aggregationCollector, CollectorQuery query)
      throws InvalidQueryException {
    try {
      AggregationValidator.validateAggregationQuery(
          aggregationCollector,
          this.facetContext.getFieldDefinitionResolver(),
          query.returnScope().map(ReturnScope::path),
          this.featureFlags.isEnabled(Feature.SEARCH_AGGREGATION));
      LuceneAggregationPlanResolver.AggregationPlan aggregationPlan =
          LuceneAggregationPlanResolver.resolveAggregationPlan(
              this.facetContext, aggregationCollector, query.returnStoredSource());
      if (enforceRequestedBucketLimit()) {
        AggregationValidator.validateRequestedBuckets(aggregationPlan.rootSpec());
      }
      return aggregationPlan;
    } catch (AggregationValidator.AggregationValidationException e) {
      throw new InvalidQueryException(e.getMessage());
    }
  }

  private static void rejectScopedFacetsWithDrillSideways(FacetCollector facetCollector)
      throws InvalidQueryException {
    boolean hasScopedFacet =
        facetCollector.facetDefinitions().values().stream()
            .anyMatch(def -> def.embeddedDocument().isPresent());
    if (hasScopedFacet) {
      throw new InvalidQueryException(
          "embeddedDocument-scoped facets are not supported together with drill-sideways "
              + "(doesNotAffect) faceting. Remove the doesNotAffect option from the operator, or "
              + "the embeddedDocument block from the facet.");
    }
  }

  @VisibleForTesting
  SearchProducerAndMetaResults collectorQuery(
      CollectorQuery query,
      org.apache.lucene.search.Query luceneQuery,
      LuceneIndexSearcherReference searcherReference,
      BatchSizeStrategy batchSizeStrategy,
      Optional<LuceneUnifiedHighlighter> unifiedHighlighter,
      Optional<LuceneScoreDetailsManager> scoreDetailsManager,
      Optional<Sort> luceneSort,
      Optional<SequenceToken> searchAfter,
      QueryCursorOptions queryCursorOptions,
      QueryOptimizationFlags queryOptimizationFlags)
      throws IOException, InvalidQueryException, InterruptedException {

    switch (query.collector()) {
      case FacetCollector facetCollector -> {
        this.facetContext.validateQuery(facetCollector, query.returnScope());

        @Var Optional<DrillSidewaysInfo> drillSidewaysInfo = Optional.empty();

        // Check the dynamic feature flag
        boolean drillSidewaysEnabled =
            this.dynamicFeatureFlagRegistry.evaluateClusterInvariant(
                DynamicFeatureFlags.DRILL_SIDEWAYS_FACETING.getName(),
                DynamicFeatureFlags.DRILL_SIDEWAYS_FACETING.getFallback());

        if (drillSidewaysEnabled) {
          drillSidewaysInfo = facetCollector.drillSidewaysInfo();
        }

        if (drillSidewaysInfo.isPresent()) {
          // Scoped facets are computed independently of the main operator, so they must not be
          // routed through the drill-sideways path (which would bypass the scoped counter and its
          // feature-flag gate, silently producing document-indexed counts). Reject the combination.
          rejectScopedFacetsWithDrillSideways(facetCollector);
          QueryOptimizationStatus optimizationStatus = drillSidewaysInfo.get().optimizationStatus();
          if (optimizationStatus == QueryOptimizationStatus.GENERIC) {
            return genericDrillSidewaysQuery(
                facetCollector,
                query,
                luceneQuery,
                searcherReference,
                batchSizeStrategy,
                unifiedHighlighter,
                scoreDetailsManager,
                luceneSort,
                searchAfter,
                queryCursorOptions,
                query.returnScope(),
                queryOptimizationFlags);
          } else if (optimizationStatus == QueryOptimizationStatus.OPTIMIZABLE) {
            return optimizedDrillSidewaysFacetCollectorQuery(
                facetCollector,
                query,
                luceneQuery,
                searcherReference,
                batchSizeStrategy,
                unifiedHighlighter,
                scoreDetailsManager,
                luceneSort,
                searchAfter,
                queryCursorOptions,
                query.returnScope(),
                queryOptimizationFlags);
          }
        }
      }
      case AggregationCollector aggregationCollector -> {
        return aggregationCollectorQuery(
            aggregationCollector,
            query,
            luceneQuery,
            searcherReference,
            batchSizeStrategy,
            unifiedHighlighter,
            scoreDetailsManager,
            luceneSort,
            searchAfter,
            queryCursorOptions,
            queryOptimizationFlags);
      }
    }

    return facetCollectorQuery(
        query,
        luceneQuery,
        searcherReference,
        batchSizeStrategy,
        unifiedHighlighter,
        scoreDetailsManager,
        luceneSort,
        searchAfter,
        queryCursorOptions,
        queryOptimizationFlags);
  }

  @VisibleForTesting
  SearchProducerAndMetaResults genericDrillSidewaysQuery(
      FacetCollector facetCollector,
      CollectorQuery query,
      org.apache.lucene.search.Query luceneQuery,
      LuceneIndexSearcherReference searcherReference,
      BatchSizeStrategy batchSizeStrategy,
      Optional<LuceneUnifiedHighlighter> unifiedHighlighter,
      Optional<LuceneScoreDetailsManager> scoreDetailsManager,
      Optional<Sort> luceneSort,
      Optional<SequenceToken> searchAfter,
      QueryCursorOptions queryCursorOptions,
      Optional<ReturnScope> returnScope,
      QueryOptimizationFlags queryOptimizationFlags)
      throws IOException, InvalidQueryException {

    DrillSidewaysInfo drillSidewaysInfo = facetCollector.drillSidewaysInfo().get();
    Map<String, Optional<Operator>> facetToOperator = drillSidewaysInfo.facetOperators();
    Map<String, org.apache.lucene.search.Query> facetToFacetQueries = new HashMap<>();
    for (String facet : facetToOperator.keySet()) {
      Optional<Operator> operator = facetToOperator.get(facet);
      org.apache.lucene.search.Query facetQuery =
          operator.isPresent()
              ? this.queryFactory.createQuery(
                  operator.get(),
                  searcherReference.getIndexSearcher().getIndexReader(),
                  returnScope,
                  queryOptimizationFlags)
              : new MatchAllDocsQuery();
      facetToFacetQueries.put(facet, facetQuery);
    }
    Map<String, LuceneDrillSideways> facetToDrillSidewaysFacetQueries =
        GenericDrillSidewaysFactory.from(
            facetToFacetQueries,
            facetCollector,
            searcherReference.getIndexSearcher(),
            this.facetContext,
            returnScope);

    LuceneSearchManager<
            LuceneFacetGenericDrillSidewaysSearchManager
                .GenericDrillSidewaysResultFacetCollectorQueryInfo>
        facetSearchManager =
            this.luceneSearchManagerFactory.newFacetGenericDrillSidewaysCollectorManager(
                luceneQuery,
                facetToDrillSidewaysFacetQueries,
                luceneSort,
                searchAfter,
                query.concurrent(),
                this.concurrentSearchExecutor,
                this.dynamicFeatureFlagRegistry);
    var collectorQueryInfo =
        facetSearchManager.initialSearch(
            searcherReference, batchSizeStrategy.adviseNextBatchSize());

    return new SearchProducerAndMetaResults(
        searchBatchProducer(
            query,
            facetSearchManager,
            collectorQueryInfo.topDocs,
            collectorQueryInfo.luceneExhausted,
            batchSizeStrategy,
            searcherReference,
            unifiedHighlighter,
            scoreDetailsManager,
            queryCursorOptions,
            queryOptimizationFlags),
        this.metaResultsBuilder.buildDrillSidewaysFacetMetaResults(
            searcherReference,
            collectorQueryInfo.topDocs,
            query,
            facetName -> Optional.ofNullable(collectorQueryInfo.facetResults.get(facetName))));
  }

  @VisibleForTesting
  SearchProducerAndMetaResults optimizedDrillSidewaysFacetCollectorQuery(
      FacetCollector facetCollector,
      CollectorQuery query,
      org.apache.lucene.search.Query luceneQuery,
      LuceneIndexSearcherReference searcherReference,
      BatchSizeStrategy batchSizeStrategy,
      Optional<LuceneUnifiedHighlighter> unifiedHighlighter,
      Optional<LuceneScoreDetailsManager> scoreDetailsManager,
      Optional<Sort> luceneSort,
      Optional<SequenceToken> searchAfter,
      QueryCursorOptions queryCursorOptions,
      Optional<ReturnScope> returnScope,
      QueryOptimizationFlags queryOptimizationFlags)
      throws IOException, InvalidQueryException {

    // Create Lucene queries for each facet
    Map<String, org.apache.lucene.search.Query> facetToDrillDownQueries;
    Optional<DrillSidewaysInfoBuilder.DrillSidewaysInfo> drillSidewaysInfoOptional =
        facetCollector.drillSidewaysInfo();
    if (drillSidewaysInfoOptional.isPresent()) {
      facetToDrillDownQueries =
          CheckedStream.from(
                  drillSidewaysInfoOptional.get().facetOperators().entrySet().stream()
                      .filter(entry -> entry.getValue().isPresent())) // Filter out empty entries
              .<String, org.apache.lucene.search.Query, RuntimeException, IOException,
                  InvalidQueryException>
                  collectToMapCheckedBiValueException(
                      Map.Entry::getKey,
                      entry ->
                          this.queryFactory.createQuery(
                              entry.getValue().get(),
                              searcherReference.getIndexSearcher().getIndexReader(),
                              returnScope,
                              queryOptimizationFlags));
    } else {
      throw new IllegalStateException("facetCollector.drillSidewaysInfo() is not present.");
    }

    org.apache.lucene.search.Query baseQuery =
        facetCollector.drillSidewaysInfo().get().preFilter().isPresent()
            ? this.queryFactory.createQuery(
                facetCollector.drillSidewaysInfo().get().preFilter().get(),
                searcherReference.getIndexSearcher().getIndexReader(),
                returnScope,
                queryOptimizationFlags)
            : new MatchAllDocsQuery();

    var luceneDrillSideways =
        OptimizedDrillSidewaysFactory.from(
            baseQuery,
            facetToDrillDownQueries,
            facetCollector,
            searcherReference.getIndexSearcher(),
            this.facetContext,
            returnScope,
            query.concurrent(),
            this.concurrentSearchExecutor,
            this.dynamicFeatureFlagRegistry);

    LuceneSearchManager<
            LuceneFacetOptimizedDrillSidewaysSearchManager
                .OptimizedDrillSidewaysFacetCollectorQueryInfo>
        facetSearchManager =
            this.luceneSearchManagerFactory.newOptimizedDrillSidewaysCollectorManager(
                luceneQuery, luceneDrillSideways, luceneSort, searchAfter);
    var collectorQueryInfo =
        facetSearchManager.initialSearch(
            searcherReference, batchSizeStrategy.adviseNextBatchSize());

    return new SearchProducerAndMetaResults(
        searchBatchProducer(
            query,
            facetSearchManager,
            collectorQueryInfo.topDocs,
            collectorQueryInfo.luceneExhausted,
            batchSizeStrategy,
            searcherReference,
            unifiedHighlighter,
            scoreDetailsManager,
            queryCursorOptions,
            queryOptimizationFlags),
        this.metaResultsBuilder.buildDrillSidewaysFacetMetaResults(
            searcherReference,
            collectorQueryInfo.topDocs,
            query,
            facetName -> Optional.ofNullable(collectorQueryInfo.drillSidewaysResult)));
  }

  @VisibleForTesting
  SearchProducerAndMetaResults facetCollectorQuery(
      CollectorQuery query,
      org.apache.lucene.search.Query luceneQuery,
      LuceneIndexSearcherReference searcherReference,
      BatchSizeStrategy batchSizeStrategy,
      Optional<LuceneUnifiedHighlighter> unifiedHighlighter,
      Optional<LuceneScoreDetailsManager> scoreDetailsManager,
      Optional<Sort> luceneSort,
      Optional<SequenceToken> searchAfter,
      QueryCursorOptions queryCursorOptions,
      QueryOptimizationFlags queryOptimizationFlags)
      throws IOException, InterruptedException, InvalidQueryException {
    LuceneSearchManager<FacetCollectorQueryInfo> facetSearchManager =
        this.luceneSearchManagerFactory.newFacetCollectorManager(
            luceneQuery, luceneSort, searchAfter);

    var collectorQueryInfo =
        facetSearchManager.initialSearch(
            searcherReference, batchSizeStrategy.adviseNextBatchSize());

    return new SearchProducerAndMetaResults(
        searchBatchProducer(
            query,
            facetSearchManager,
            collectorQueryInfo.topDocs,
            collectorQueryInfo.luceneExhausted,
            batchSizeStrategy,
            searcherReference,
            unifiedHighlighter,
            scoreDetailsManager,
            queryCursorOptions,
            queryOptimizationFlags),
        this.metaResultsBuilder.buildFacetMetaResults(
            searcherReference,
            collectorQueryInfo.topDocs,
            collectorQueryInfo.collector,
            query,
            query.concurrent(),
            embeddedScopedFacetsEnabled()));
  }

  @VisibleForTesting
  SearchProducerAndMetaResults aggregationCollectorQuery(
      AggregationCollector aggregationCollector,
      CollectorQuery query,
      org.apache.lucene.search.Query luceneQuery,
      LuceneIndexSearcherReference searcherReference,
      BatchSizeStrategy batchSizeStrategy,
      Optional<LuceneUnifiedHighlighter> unifiedHighlighter,
      Optional<LuceneScoreDetailsManager> scoreDetailsManager,
      Optional<Sort> luceneSort,
      Optional<SequenceToken> searchAfter,
      QueryCursorOptions queryCursorOptions,
      QueryOptimizationFlags queryOptimizationFlags)
      throws IOException, InterruptedException, InvalidQueryException {
    LuceneAggregationPlanResolver.AggregationPlan aggregationPlan =
        resolveValidatedAggregationPlan(aggregationCollector, query);
    validateAggregationBucketLimit(aggregationCollector, searcherReference);
    // Always 0 here: a partitioned index routes aggregation through
    // intermediateAggregationCollectorQuery instead, so this path only ever serves a
    // single-partition index.
    LuceneSearchManager<AggregationQueryInfo> aggregationSearchManager =
        this.luceneSearchManagerFactory.newAggregationManager(
            luceneQuery,
            luceneSort,
            // The same sort inverted, for $last; see AggregationContext#getInvertedSort for why
            // the token and the index sort are left out.
            this.queryFactory.createSort(
                query.sortSpec().map(SortSpec::invert),
                Optional.empty(),
                searcherReference.getIndexSearcher().getFieldToSortableTypesMapping(),
                query.returnScope(),
                queryOptimizationFlags,
                Optional.empty(),
                query.sortDrivesAggregation(),
                searcherReference.getIndexSearcher().getIndexReader(),
                embeddedScopedSortEnabled(query)),
            searchAfter,
            aggregationPlan,
            this.indexPartitionId,
            // This result is never count-merged with another, so the reduce may prune to top-N
            // before resolving terms.
            /* applyLimitOnReduce= */ true);

    AggregationQueryInfo collectorQueryInfo =
        aggregationSearchManager.initialSearch(
            searcherReference, batchSizeStrategy.adviseNextBatchSize());

    return new SearchProducerAndMetaResults(
        searchBatchProducer(
            query,
            aggregationSearchManager,
            collectorQueryInfo.topDocs,
            collectorQueryInfo.luceneExhausted,
            batchSizeStrategy,
            searcherReference,
            unifiedHighlighter,
            scoreDetailsManager,
            queryCursorOptions,
            queryOptimizationFlags),
        this.metaResultsBuilder.buildAggregationMetaResults(
            searcherReference,
            collectorQueryInfo.topDocs,
            collectorQueryInfo.getAggregationResult(),
            query,
            query.concurrent()));
  }

  private SearchProducerAndMetaProducer intermediateAggregationCollectorQuery(
      AggregationCollector aggregationCollector,
      CollectorQuery query,
      org.apache.lucene.search.Query luceneQuery,
      LuceneIndexSearcherReference searcherReference,
      BatchSizeStrategy batchSizeStrategy,
      Optional<LuceneUnifiedHighlighter> unifiedHighlighter,
      Optional<LuceneScoreDetailsManager> scoreDetailsManager,
      Optional<Sort> luceneSort,
      Optional<SequenceToken> searchAfter,
      QueryCursorOptions queryCursorOptions,
      QueryOptimizationFlags queryOptimizationFlags)
      throws IOException, InterruptedException, InvalidQueryException {
    LuceneAggregationPlanResolver.AggregationPlan aggregationPlan =
        resolveValidatedAggregationPlan(aggregationCollector, query);
    validateAggregationBucketLimit(aggregationCollector, searcherReference);
    // The reader's own partition, since a partitioned index has one reader per partition and
    // fans out over them. first/last need it because doc ids only order documents within a
    // single partition.
    LuceneSearchManager<AggregationQueryInfo> aggregationSearchManager =
        this.luceneSearchManagerFactory.newAggregationManager(
            luceneQuery,
            luceneSort,
            // The same sort inverted, for $last; see AggregationContext#getInvertedSort for why
            // the token and the index sort are left out.
            this.queryFactory.createSort(
                query.sortSpec().map(SortSpec::invert),
                Optional.empty(),
                searcherReference.getIndexSearcher().getFieldToSortableTypesMapping(),
                query.returnScope(),
                queryOptimizationFlags,
                Optional.empty(),
                query.sortDrivesAggregation(),
                searcherReference.getIndexSearcher().getIndexReader(),
                embeddedScopedSortEnabled(query)),
            searchAfter,
            aggregationPlan,
            this.indexPartitionId,
            // Intermediate results are count-merged across partitions or shards afterwards, so
            // they must carry full cardinality.
            /* applyLimitOnReduce= */ false);

    AggregationQueryInfo collectorQueryInfo =
        aggregationSearchManager.initialSearch(
            searcherReference, batchSizeStrategy.adviseNextBatchSize());

    checkState(
        collectorQueryInfo.topDocs.totalHits.relation() == TotalHits.Relation.EQUAL_TO,
        "AggregationIntermediateBatchProducer requires an exact count.");

    List<Integer> sortDirections =
        query.sortSpec().map(LuceneSearchIndexReader::aggregationSortDirections).orElse(List.of());
    ShardMergeAliasPlan aliasPlan =
        ShardMergeAliasPlan.from(aggregationPlan.rootSpec(), sortDirections);

    BatchProducer intermediateProducer =
        new AggregationIntermediateBatchProducer(
            collectorQueryInfo.getAggregationResult(),
            collectorQueryInfo.topDocs.totalHits.value(),
            aliasPlan);

    return new SearchProducerAndMetaProducer(
        searchBatchProducer(
            query,
            aggregationSearchManager,
            collectorQueryInfo.topDocs,
            collectorQueryInfo.luceneExhausted,
            batchSizeStrategy,
            searcherReference,
            unifiedHighlighter,
            scoreDetailsManager,
            queryCursorOptions,
            queryOptimizationFlags),
        intermediateProducer);
  }

  private SearchProducerAndMetaProducer intermediateOperatorQuery(
      OperatorQuery query,
      org.apache.lucene.search.Query luceneQuery,
      LuceneIndexSearcherReference searcherReference,
      BatchSizeStrategy batchSizeStrategy,
      Optional<LuceneUnifiedHighlighter> unifiedHighlighter,
      Optional<LuceneScoreDetailsManager> scoreDetailsManager,
      Optional<Sort> luceneSort,
      Optional<SequenceToken> searchAfter,
      QueryCursorOptions queryCursorOptions,
      QueryOptimizationFlags queryOptimizationFlags)
      throws IOException, InvalidQueryException {

    LuceneSearchManager<QueryInfo> searchManager =
        this.luceneSearchManagerFactory.newOperatorManager(
            query, luceneQuery, luceneSort, searchAfter);

    var queryInfo =
        searchManager.initialSearch(searcherReference, batchSizeStrategy.adviseNextBatchSize());

    return new SearchProducerAndMetaProducer(
        searchBatchProducer(
            query,
            searchManager,
            queryInfo.topDocs,
            queryInfo.luceneExhausted,
            batchSizeStrategy,
            searcherReference,
            unifiedHighlighter,
            scoreDetailsManager,
            queryCursorOptions,
            queryOptimizationFlags),
        new CountMetaBatchProducer(queryInfo.topDocs.totalHits.value()));
  }

  private SearchProducerAndMetaProducer intermediateCollectorQuery(
      CollectorQuery query,
      org.apache.lucene.search.Query luceneQuery,
      LuceneIndexSearcherReference searcherReference,
      BatchSizeStrategy batchSizeStrategy,
      Optional<LuceneUnifiedHighlighter> unifiedHighlighter,
      Optional<LuceneScoreDetailsManager> scoreDetailsManager,
      Optional<Sort> luceneSort,
      Optional<SequenceToken> searchAfter,
      QueryCursorOptions queryCursorOptions,
      QueryOptimizationFlags queryOptimizationFlags)
      throws IOException, InvalidQueryException, InterruptedException {

    switch (query.collector()) {
      case FacetCollector facetCollector -> {
        this.facetContext.validateQuery(facetCollector, query.returnScope());
        @Var Optional<DrillSidewaysInfo> drillSidewaysInfo = Optional.empty();

        // Check the dynamic feature flag
        boolean drillSidewaysEnabled =
            this.dynamicFeatureFlagRegistry.evaluateClusterInvariant(
                DynamicFeatureFlags.DRILL_SIDEWAYS_FACETING.getName(),
                DynamicFeatureFlags.DRILL_SIDEWAYS_FACETING.getFallback());

        if (drillSidewaysEnabled) {
          drillSidewaysInfo = facetCollector.drillSidewaysInfo();
        }

        if (drillSidewaysInfo.isPresent()) {
          // Scoped facets are computed independently of the main operator, so they must not be
          // routed through the drill-sideways path (which would bypass the scoped counter and its
          // feature-flag gate, silently producing document-indexed counts). Reject the combination.
          rejectScopedFacetsWithDrillSideways(facetCollector);
          QueryOptimizationStatus optimizationStatus = drillSidewaysInfo.get().optimizationStatus();

          if (optimizationStatus == QueryOptimizationStatus.OPTIMIZABLE) {
            return optimizedDrillSidewaysIntermediateQuery(
                facetCollector,
                query,
                luceneQuery,
                searcherReference,
                batchSizeStrategy,
                unifiedHighlighter,
                scoreDetailsManager,
                luceneSort,
                searchAfter,
                queryCursorOptions,
                query.returnScope(),
                queryOptimizationFlags);
          } else if (optimizationStatus == QueryOptimizationStatus.GENERIC) {
            return genericDrillSidewaysIntermediateQuery(
                facetCollector,
                query,
                luceneQuery,
                searcherReference,
                batchSizeStrategy,
                unifiedHighlighter,
                scoreDetailsManager,
                luceneSort,
                searchAfter,
                queryCursorOptions,
                query.returnScope(),
                queryOptimizationFlags);
          }
        }
      }
      case AggregationCollector aggregationCollector -> {
        return intermediateAggregationCollectorQuery(
            aggregationCollector,
            query,
            luceneQuery,
            searcherReference,
            batchSizeStrategy,
            unifiedHighlighter,
            scoreDetailsManager,
            luceneSort,
            searchAfter,
            queryCursorOptions,
            queryOptimizationFlags);
      }
    }
    return intermediateFacetCollectorQuery(
        query,
        luceneQuery,
        searcherReference,
        batchSizeStrategy,
        unifiedHighlighter,
        scoreDetailsManager,
        luceneSort,
        searchAfter,
        queryCursorOptions,
        queryOptimizationFlags);
  }

  private SearchProducerAndMetaProducer genericDrillSidewaysIntermediateQuery(
      FacetCollector facetCollector,
      CollectorQuery query,
      org.apache.lucene.search.Query luceneQuery,
      LuceneIndexSearcherReference searcherReference,
      BatchSizeStrategy batchSizeStrategy,
      Optional<LuceneUnifiedHighlighter> unifiedHighlighter,
      Optional<LuceneScoreDetailsManager> scoreDetailsManager,
      Optional<Sort> luceneSort,
      Optional<SequenceToken> searchAfter,
      QueryCursorOptions queryCursorOptions,
      Optional<ReturnScope> returnScope,
      QueryOptimizationFlags queryOptimizationFlags)
      throws IOException, InvalidQueryException, InterruptedException {
    Map<String, Optional<Operator>> facetOperators =
        facetCollector.drillSidewaysInfo().get().facetOperators();
    Map<String, org.apache.lucene.search.Query> facetQueries = new HashMap<>();
    for (String key : facetOperators.keySet()) {
      Optional<Operator> operator = facetOperators.get(key);
      org.apache.lucene.search.Query facetQuery =
          operator.isPresent()
              ? this.queryFactory.createQuery(
                  operator.get(),
                  searcherReference.getIndexSearcher().getIndexReader(),
                  returnScope,
                  queryOptimizationFlags)
              : new MatchAllDocsQuery();
      facetQueries.put(key, facetQuery);
    }
    Map<String, LuceneDrillSideways> facetToLuceneDrillSideways =
        GenericDrillSidewaysFactory.from(
            facetQueries,
            facetCollector,
            searcherReference.getIndexSearcher(),
            this.facetContext,
            returnScope);

    LuceneSearchManager<
            LuceneFacetGenericDrillSidewaysSearchManager
                .GenericDrillSidewaysResultFacetCollectorQueryInfo>
        facetSearchManager =
            this.luceneSearchManagerFactory.newFacetGenericDrillSidewaysCollectorManager(
                luceneQuery,
                facetToLuceneDrillSideways,
                luceneSort,
                searchAfter,
                query.concurrent(),
                this.concurrentSearchExecutor,
                this.dynamicFeatureFlagRegistry);
    LuceneFacetGenericDrillSidewaysSearchManager.GenericDrillSidewaysResultFacetCollectorQueryInfo
        collectorQueryInfo =
            facetSearchManager.initialSearch(
                searcherReference, batchSizeStrategy.adviseNextBatchSize());

    return new SearchProducerAndMetaProducer(
        searchBatchProducer(
            query,
            facetSearchManager,
            collectorQueryInfo.topDocs,
            collectorQueryInfo.luceneExhausted,
            batchSizeStrategy,
            searcherReference,
            unifiedHighlighter,
            scoreDetailsManager,
            queryCursorOptions,
            queryOptimizationFlags),
        drillSidewaysFacetMetaBatchProducer(
            query,
            this.facetContext,
            searcherReference,
            collectorQueryInfo.topDocs,
            collectorQueryInfo.facetResults));
  }

  private SearchProducerAndMetaProducer optimizedDrillSidewaysIntermediateQuery(
      FacetCollector facetCollector,
      CollectorQuery query,
      org.apache.lucene.search.Query luceneQuery,
      LuceneIndexSearcherReference searcherReference,
      BatchSizeStrategy batchSizeStrategy,
      Optional<LuceneUnifiedHighlighter> unifiedHighlighter,
      Optional<LuceneScoreDetailsManager> scoreDetailsManager,
      Optional<Sort> luceneSort,
      Optional<SequenceToken> searchAfter,
      QueryCursorOptions queryCursorOptions,
      Optional<ReturnScope> returnScope,
      QueryOptimizationFlags queryOptimizationFlags)
      throws IOException, InvalidQueryException, InterruptedException {

    // Create Lucene queries for each facet
    Map<String, org.apache.lucene.search.Query> facetToDrillDownQueries;
    Optional<DrillSidewaysInfoBuilder.DrillSidewaysInfo> drillSidewaysInfoOptional =
        facetCollector.drillSidewaysInfo();
    if (drillSidewaysInfoOptional.isPresent()) {
      facetToDrillDownQueries =
          CheckedStream.from(
                  drillSidewaysInfoOptional.get().facetOperators().entrySet().stream()
                      .filter(entry -> entry.getValue().isPresent())) // Filter out empty entries
              .<String, org.apache.lucene.search.Query, RuntimeException, IOException,
                  InvalidQueryException>
                  collectToMapCheckedBiValueException(
                      Map.Entry::getKey,
                      entry ->
                          this.queryFactory.createQuery(
                              entry.getValue().get(),
                              searcherReference.getIndexSearcher().getIndexReader(),
                              returnScope,
                              queryOptimizationFlags));
    } else {
      throw new IllegalStateException("facetCollector.drillSidewaysInfo() is not present.");
    }

    org.apache.lucene.search.Query baseQuery =
        facetCollector.drillSidewaysInfo().get().preFilter().isPresent()
            ? this.queryFactory.createQuery(
                facetCollector.drillSidewaysInfo().get().preFilter().get(),
                searcherReference.getIndexSearcher().getIndexReader(),
                returnScope,
                queryOptimizationFlags)
            : new MatchAllDocsQuery();

    var luceneDrillSideways =
        OptimizedDrillSidewaysFactory.from(
            baseQuery,
            facetToDrillDownQueries,
            facetCollector,
            searcherReference.getIndexSearcher(),
            this.facetContext,
            returnScope,
            query.concurrent(),
            Optional.empty(),
            this.dynamicFeatureFlagRegistry);

    LuceneSearchManager<
            LuceneFacetOptimizedDrillSidewaysSearchManager
                .OptimizedDrillSidewaysFacetCollectorQueryInfo>
        facetSearchManager =
            this.luceneSearchManagerFactory.newOptimizedDrillSidewaysCollectorManager(
                luceneQuery, luceneDrillSideways, luceneSort, searchAfter);
    var collectorQueryInfo =
        facetSearchManager.initialSearch(
            searcherReference, batchSizeStrategy.adviseNextBatchSize());

    Optional<FacetFeatureExplainer> facetExplainer =
        LuceneFacetResultUtil.getFacetFeatureExplainer();

    return new SearchProducerAndMetaProducer(
        searchBatchProducer(
            query,
            facetSearchManager,
            collectorQueryInfo.topDocs,
            collectorQueryInfo.luceneExhausted,
            batchSizeStrategy,
            searcherReference,
            unifiedHighlighter,
            scoreDetailsManager,
            queryCursorOptions,
            queryOptimizationFlags),
        LuceneFacetDrillSidewaysMetaBatchProducerFactory.create(
            query,
            this.facetContext,
            searcherReference,
            collectorQueryInfo.topDocs,
            facetName -> Optional.ofNullable(collectorQueryInfo.drillSidewaysResult),
            facetExplainer));
  }

  private SearchProducerAndMetaProducer intermediateFacetCollectorQuery(
      CollectorQuery query,
      org.apache.lucene.search.Query luceneQuery,
      LuceneIndexSearcherReference searcherReference,
      BatchSizeStrategy batchSizeStrategy,
      Optional<LuceneUnifiedHighlighter> unifiedHighlighter,
      Optional<LuceneScoreDetailsManager> scoreDetailsManager,
      Optional<Sort> luceneSort,
      Optional<SequenceToken> searchAfter,
      QueryCursorOptions queryCursorOptions,
      QueryOptimizationFlags queryOptimizationFlags)
      throws IOException, InterruptedException, InvalidQueryException {
    LuceneSearchManager<FacetCollectorQueryInfo> searchManager =
        this.luceneSearchManagerFactory.newFacetCollectorManager(
            luceneQuery, luceneSort, searchAfter);

    var collectorQueryInfo =
        searchManager.initialSearch(searcherReference, batchSizeStrategy.adviseNextBatchSize());

    // The SearchProducer is the owner of the searcherReference and in charge of closing it.
    // metaBatchProducer() uses the searcherReference here to initialize the MetaProducer, but does
    // not store it.
    return new SearchProducerAndMetaProducer(
        searchBatchProducer(
            query,
            searchManager,
            collectorQueryInfo.topDocs,
            collectorQueryInfo.luceneExhausted,
            batchSizeStrategy,
            searcherReference,
            unifiedHighlighter,
            scoreDetailsManager,
            queryCursorOptions,
            queryOptimizationFlags),
        facetMetaBatchProducer(
            query,
            this.facetContext,
            searcherReference,
            collectorQueryInfo.topDocs,
            collectorQueryInfo.collector,
            this.concurrentSearchExecutor));
  }

  private BatchProducer searchBatchProducer(
      SearchQuery query,
      LuceneSearchManager<? extends QueryInfo> searchManager,
      TopDocs initDocs,
      boolean luceneExhausted,
      BatchSizeStrategy batchSizeStrategy,
      LuceneIndexSearcherReference searcherReference,
      Optional<LuceneUnifiedHighlighter> unifiedHighlighter,
      Optional<LuceneScoreDetailsManager> scoreDetailsManager,
      QueryCursorOptions queryCursorOptions,
      QueryOptimizationFlags queryOptimizationFlags) {

    var spec = new ProjectSpec(query.returnStoredSource(), this.indexDefinition.getStoredSource());
    return new MeteredBatchProducer(
        queryOptimizationFlags.omitSearchDocumentResults()
            ? new EmptySearchBatchProducer(Optional.of(searcherReference))
            : new LuceneSearchBatchProducer(
                searcherReference,
                searchManager,
                initDocs,
                luceneExhausted,
                unifiedHighlighter,
                scoreDetailsManager,
                batchSizeStrategy,
                ProjectFactory.build(spec, searcherReference.getIndexSearcher().getIndexReader()),
                this.queryingMetricsUpdater,
                queryCursorOptions,
                query
                    .sortSpec()
                    .map(s -> s instanceof com.xgen.mongot.index.query.sort.Sort)
                    .orElse(false),
                query.pagination().map(Pagination::sequenceToken),
                query.returnScope().isPresent(),
                SearchResult::new,
                this.indexPartitionId,
                this.indexDefinition.getSort().isPresent()),
        this.queryingMetricsUpdater);
  }

  private BatchProducer vectorSearchBatchProducer(
      LuceneSearchManager<QueryInfo> searchManager,
      TopDocs initDocs,
      boolean luceneExhausted,
      BatchSizeStrategy batchSizeStrategy,
      LuceneIndexSearcherReference searcherReference) {

    var spec = new ProjectSpec(false, this.indexDefinition.getStoredSource());
    return new MeteredBatchProducer(
        new LuceneSearchBatchProducer(
            searcherReference,
            searchManager,
            initDocs,
            luceneExhausted,
            Optional.empty(),
            Optional.empty(),
            batchSizeStrategy,
            ProjectFactory.build(spec, searcherReference.getIndexSearcher().getIndexReader()),
            this.queryingMetricsUpdater,
            QueryCursorOptions.empty(),
            false,
            Optional.empty(),
            false,
            VectorSearchResult::create,
            this.indexPartitionId,
            false),
        this.queryingMetricsUpdater);
  }

  private BatchProducer facetMetaBatchProducer(
      CollectorQuery collectorQuery,
      LuceneFacetContext facetContext,
      LuceneIndexSearcherReference searcherReference,
      TopDocs topDocs,
      FacetsCollector facetsCollector,
      Optional<NamedExecutorService> concurrentSearchExecutor)
      throws IOException, InterruptedException, InvalidQueryException {

    return LuceneFacetCollectorMetaBatchProducerFactory.create(
        collectorQuery,
        facetContext,
        searcherReference,
        topDocs,
        facetsCollector,
        concurrentSearchExecutor,
        this.queryFactory,
        embeddedScopedFacetsEnabled());
  }

  private BatchProducer drillSidewaysFacetMetaBatchProducer(
      CollectorQuery collectorQuery,
      LuceneFacetContext facetContext,
      LuceneIndexSearcherReference searcherReference,
      TopDocs topDocs,
      Map<String, DrillSidewaysResult> facetToDrillSidewaysResult)
      throws IOException, InterruptedException, InvalidQueryException {

    Optional<FacetFeatureExplainer> facetExplainer =
        LuceneFacetResultUtil.getFacetFeatureExplainer();
    return LuceneFacetDrillSidewaysMetaBatchProducerFactory.create(
        collectorQuery,
        facetContext,
        searcherReference,
        topDocs,
        facetName -> Optional.ofNullable(facetToDrillSidewaysResult.get(facetName)),
        facetExplainer);
  }

  @GuardedBy("shutdownSharedLock")
  private void ensureOpen(String methodName) {
    checkState(!this.closed, "cannot call %s() after to close()", methodName);
  }

  @GuardedBy("shutdownSharedLock")
  private LuceneIndexSearcherReference createSearcherReference(boolean concurrentQuery)
      throws IOException {
    return concurrentQuery && this.concurrentSearchExecutor.isPresent()
        ? LuceneIndexSearcherReference.create(
            this.searcherManager,
            this.concurrentSearchExecutor.get(),
            this.queryingMetricsUpdater,
            this.featureFlags)
        : LuceneIndexSearcherReference.create(
            this.searcherManager, this.queryingMetricsUpdater, this.featureFlags);
  }

  @VisibleForTesting
  void trackIndexSortMetrics(Optional<Sort> luceneSort, Optional<Sort> indexSort) {
    // track Index sort metrics
    if (luceneSort.isPresent()
        && indexSort.isPresent()
        && IndexSortUtils.usesIndexSort(luceneSort.get(), indexSort.get())) {
      this.queryingMetricsUpdater.getBenefitFromIndexSortCounter().increment();
    }
  }

  /**
   * Mitigates a Lucene 10.4 {@code DenseConjunctionBulkScorer} regression on sorted searches. When
   * a (non-relevance) {@code sort} is present, the search is collected by a {@code
   * TopFieldCollector} whose competitive iterator makes sort-without-sorted-index cheap by pruning
   * non-competitive docs. Lucene 10.4's {@code DenseConjunctionBulkScorer} streams every matching
   * doc through a per-window bitset, defeating that pruning and causing over-collection. Wrapping
   * the top-level query in {@link DisableBulkScorerQuery} forces {@code Weight.DefaultBulkScorer},
   * which honors the collector's competitive iterator, restoring the Lucene 10.1 path. Gated by
   * {@link DynamicFeatureFlags#DISABLE_BULK_SCORER_QUERY_FOR_SORT}.
   */
  @VisibleForTesting
  static org.apache.lucene.search.Query maybeDisableBulkScorerForSort(
      org.apache.lucene.search.Query luceneQuery,
      Optional<Sort> luceneSort,
      DynamicFeatureFlagRegistry dynamicFeatureFlagRegistry) {
    if (luceneSort.isPresent()
        && dynamicFeatureFlagRegistry.evaluateClusterInvariant(
            DynamicFeatureFlags.DISABLE_BULK_SCORER_QUERY_FOR_SORT)) {
      return new DisableBulkScorerQuery(luceneQuery);
    }
    return luceneQuery;
  }

  private static Operator getOperator(SearchQuery query) {
    return switch (query) {
      case OperatorQuery operatorQuery -> operatorQuery.operator();
      case CollectorQuery collectorQuery -> getCollectorOperator(collectorQuery.collector());
    };
  }

  private static Operator getCollectorOperator(Collector collector) {
    return switch (collector) {
      case FacetCollector facetCollector -> facetCollector.operator();
      case AggregationCollector aggregationCollector -> aggregationCollector.operator();
    };
  }

  /**
   * Returns whether the MAX_STRING_FACET_CARDINALITY_METRIC feature flag is enabled.
   *
   * @return true if the feature flag is enabled, false otherwise
   */
  public boolean maxFacetCardinalityMetricEnabled() {
    return this.featureFlags.isEnabled(Feature.MAX_STRING_FACET_CARDINALITY_METRIC);
  }

  /**
   * Plan-time bucket-limit guard (gated by the {@code enableSearchAggregationBucketLimit} dynamic
   * feature flag): rejects the aggregation before any bucket state is allocated when the estimated
   * total bucket count exceeds {@code aggregationMaxBuckets}. The estimate assumes every unique
   * grouping value survives the filter, so it over-rejects rather than risking an OOM.
   */
  private void validateAggregationBucketLimit(
      AggregationCollector aggregationCollector, LuceneIndexSearcherReference searcherReference)
      throws IOException, InvalidQueryException {
    if (!this.dynamicFeatureFlagRegistry.evaluateClusterInvariant(
        DynamicFeatureFlags.SEARCH_AGGREGATION_BUCKET_LIMIT)) {
      return;
    }
    try {
      AggregationValidator.validateBucketLimit(
          aggregationCollector,
          searcherReference.getIndexSearcher().getIndexReader(),
          this.aggregationMaxBuckets);
    } catch (AggregationValidator.AggregationValidationException e) {
      getSearchAggregationMetricsReporter().recordBucketLimitRejection();
      throw new InvalidQueryException(e.getMessage());
    }
  }

  /**
   * Returns the maximum cardinality across all stringFacet fields for the per index metric.
   *
   * @return the maximum cardinality, or 0 if no stringFacet fields are indexed or if the reader is
   *     closed
   */
  public int getMaxStringFacetCardinality() {
    try (LockGuard ignored = LockGuard.with(this.shutdownSharedLock)) {
      if (!maxFacetCardinalityMetricEnabled()) {
        return 0;
      }
      if (this.closed) {
        return 0;
      }
      try {
        LuceneIndexSearcher searcher = this.searcherManager.acquire();
        try {
          return searcher
              .getFacetsState()
              .map(
                  state ->
                      state.getPrefixToOrdRange().values().stream()
                          // get cardinality of each field using the range of ordinal numbers
                          // representing the unique values for a facet field
                          .mapToInt(ordRange -> ordRange.end() - ordRange.start() + 1)
                          .max()
                          .orElse(0))
              .orElse(0);
        } finally {
          this.searcherManager.release(searcher);
        }
      } catch (IOException e) {
        FLOGGER.atWarning().atMostEvery(5, TimeUnit.MINUTES).withCause(e).log(
            "IOException while computing max facet cardinality, returning 0");
        return 0;
      }
    }
  }

  /**
   * Snapshot of this partition's token-facets ordinal-map cache. Acquires the live searcher once so
   * callers can read heap, entries, and hit/miss/eviction without five separate acquires. Returns
   * {@link TokenFacetsStateCache.Snapshot#EMPTY} when the cache is disabled or the reader is
   * closed.
   */
  @Override
  public TokenFacetsStateCache.Snapshot getTokenFacetsStateCacheSnapshot() {
    try (LockGuard ignored = LockGuard.with(this.shutdownSharedLock)) {
      if (this.closed) {
        return TokenFacetsStateCache.Snapshot.EMPTY;
      }
      try {
        LuceneIndexSearcher searcher = this.searcherManager.acquire();
        try {
          return searcher
              .getTokenFacetsStateCache()
              .map(TokenFacetsStateCache::snapshot)
              .orElse(TokenFacetsStateCache.Snapshot.EMPTY);
        } finally {
          this.searcherManager.release(searcher);
        }
      } catch (IOException e) {
        FLOGGER.atWarning().atMostEvery(5, TimeUnit.MINUTES).withCause(e).log(
            "IOException while reading token facets state cache metrics, returning empty snapshot");
        return TokenFacetsStateCache.Snapshot.EMPTY;
      }
    }
  }

  private static List<Integer> aggregationSortDirections(SortSpec sortSpec) {
    return sortSpec.getSortFields().stream()
        .map(sortField -> sortField.options().order().intValue)
        .toList();
  }
}
