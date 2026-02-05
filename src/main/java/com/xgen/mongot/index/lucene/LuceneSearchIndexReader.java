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
import com.xgen.mongot.index.SearchIndexReader;
import com.xgen.mongot.index.SearchResult;
import com.xgen.mongot.index.VectorSearchResult;
import com.xgen.mongot.index.analyzer.AnalyzerRegistry;
import com.xgen.mongot.index.definition.SearchFieldDefinitionResolver;
import com.xgen.mongot.index.definition.SearchIndexDefinition;
import com.xgen.mongot.index.lucene.LuceneFacetCollectorSearchManager.FacetCollectorQueryInfo;
import com.xgen.mongot.index.lucene.LuceneSearchManager.QueryInfo;
import com.xgen.mongot.index.lucene.explain.explainers.FacetFeatureExplainer;
import com.xgen.mongot.index.lucene.explain.tracing.Explain;
import com.xgen.mongot.index.lucene.field.FieldName;
import com.xgen.mongot.index.lucene.field.FieldValue;
import com.xgen.mongot.index.lucene.quantization.BinaryQuantizedVectorRescorer;
import com.xgen.mongot.index.lucene.query.LuceneSearchQueryFactoryDistributor;
import com.xgen.mongot.index.lucene.query.pushdown.project.ProjectFactory;
import com.xgen.mongot.index.lucene.query.pushdown.project.ProjectSpec;
import com.xgen.mongot.index.lucene.query.sort.IndexSortUtils;
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
import com.xgen.mongot.index.query.collectors.Collector;
import com.xgen.mongot.index.query.collectors.DrillSidewaysInfoBuilder;
import com.xgen.mongot.index.query.collectors.DrillSidewaysInfoBuilder.DrillSidewaysInfo;
import com.xgen.mongot.index.query.collectors.DrillSidewaysInfoBuilder.DrillSidewaysInfo.QueryOptimizationStatus;
import com.xgen.mongot.index.query.collectors.FacetCollector;
import com.xgen.mongot.index.query.counts.Count;
import com.xgen.mongot.index.query.operators.Operator;
import com.xgen.mongot.index.query.sort.SequenceToken;
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

  private LuceneSearchIndexReader(
      SearchIndexDefinition indexDefinition,
      LuceneSearchQueryFactoryDistributor queryFactory,
      LuceneSearcherManager searcherManager,
      LuceneHighlighterContext highlighterContext,
      LuceneFacetContext facetContext,
      IndexMetricsUpdater.QueryingMetricsUpdater queryingMetricsUpdater,
      LuceneSearchManagerFactory luceneSearchManagerFactory,
      Optional<NamedExecutorService> concurrentSearchExecutor,
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
    this.metaResultsBuilder = new LuceneMetaResultsBuilder(facetContext, concurrentSearchExecutor);
    this.indexPartitionId = indexPartitionId;
    this.featureFlags = featureFlags;
    this.dynamicFeatureFlagRegistry = dynamicFeatureFlagRegistry;

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
            featureFlags);
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
        indexPartitionId,
        featureFlags,
        dynamicFeatureFlagRegistry);
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
                indexSort);
        trackIndexSortMetrics(luceneSort, indexSort);

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
            indexSort);
    trackIndexSortMetrics(luceneSort, indexSort);

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

  @Override
  public long getRequiredMemoryForVectorData() throws ReaderClosedException {
    return 0L;
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
            query.concurrent()));
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
        new CountMetaBatchProducer(queryInfo.topDocs.totalHits.value));
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
                this.indexPartitionId),
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
            this.indexPartitionId),
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
        concurrentSearchExecutor);
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
        && IndexSortUtils.canBenefitFromIndexSort(luceneSort.get(), indexSort.get())) {
      this.queryingMetricsUpdater.getBenefitFromIndexSortCounter().increment();
    }
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
                          .mapToInt(ordRange -> ordRange.end - ordRange.start + 1)
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
}
