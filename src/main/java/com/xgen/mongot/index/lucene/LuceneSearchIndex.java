package com.xgen.mongot.index.lucene;

import static com.xgen.mongot.util.Check.checkState;

import com.google.common.annotations.VisibleForTesting;
import com.xgen.mongot.featureflag.Feature;
import com.xgen.mongot.featureflag.FeatureFlags;
import com.xgen.mongot.index.IndexUnavailableException;
import com.xgen.mongot.index.InitialSyncIndexUnavailableException;
import com.xgen.mongot.index.SearchIndex;
import com.xgen.mongot.index.analyzer.AnalyzerRegistry;
import com.xgen.mongot.index.analyzer.definition.AnalyzerDefinition;
import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.definition.SearchIndexDefinition;
import com.xgen.mongot.index.lucene.backing.IndexBackingStrategy;
import com.xgen.mongot.index.lucene.config.LuceneConfig;
import com.xgen.mongot.index.lucene.merge.InstrumentedConcurrentMergeScheduler;
import com.xgen.mongot.index.lucene.searcher.QueryCacheProvider;
import com.xgen.mongot.index.lucene.synonym.LuceneSynonymRegistry;
import com.xgen.mongot.index.status.IndexStatus;
import com.xgen.mongot.index.synonym.SynonymRegistry;
import com.xgen.mongot.index.version.IndexFormatVersion;
import com.xgen.mongot.metrics.PerIndexMetricsFactory;
import com.xgen.mongot.util.AtomicDirectoryRemover;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.concurrent.NamedExecutorService;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.lucene.index.MergePolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LuceneSearchIndex implements SearchIndex {
  private static final Logger LOG = LoggerFactory.getLogger(LuceneSearchIndex.class);
  private final SearchIndexDefinition definition;
  private final AtomicReference<IndexStatus> statusRef;
  private final SearchIndexProperties searchIndexProperties;
  private volatile boolean closed;

  /** The properties used by the search index at runtime. */
  static class SearchIndexProperties {
    LuceneSynonymRegistry synonymRegistry;
    AnalyzerRegistry analyzerRegistry;
    IndexBackingStrategy indexBackingStrategy;
    InstrumentedConcurrentMergeScheduler mergeScheduler;
    MergePolicy mergePolicy;
    QueryCacheProvider queryCacheProvider;
    double ramBufferSizeMb;
    Optional<Integer> fieldLimit;
    Optional<Integer> docsLimit;
    boolean enableTextOperatorNewSynonymsSyntax;
    boolean enableFacetingOverTokenFields;
    Optional<Integer> tokenFacetingCardinalityLimit;
    IndexFormatVersion indexFormatVersion;
    PerIndexMetricsFactory metricsFactory;
    Optional<NamedExecutorService> concurrentSearchExecutor;
    Optional<NamedExecutorService> concurrentVectorRescoringExecutor;
    Executor metricRefreshExecutor;

    @VisibleForTesting
    SearchIndexProperties(
        LuceneSynonymRegistry synonymRegistry,
        AnalyzerRegistry analyzerRegistry,
        IndexBackingStrategy indexBackingStrategy,
        InstrumentedConcurrentMergeScheduler mergeScheduler,
        MergePolicy mergePolicy,
        QueryCacheProvider queryCacheProvider,
        double ramBufferSizeMb,
        Optional<Integer> fieldLimit,
        Optional<Integer> docsLimit,
        boolean enableTextOperatorNewSynonymsSyntax,
        boolean enableFacetingOverTokenFields,
        Optional<Integer> tokenFacetingCardinalityLimit,
        IndexFormatVersion indexFormatVersion,
        PerIndexMetricsFactory metricsFactory,
        Optional<NamedExecutorService> concurrentSearchExecutor,
        Optional<NamedExecutorService> concurrentVectorRescoringExecutor,
        Executor metricRefreshExecutor) {
      this.synonymRegistry = synonymRegistry;
      this.analyzerRegistry = analyzerRegistry;
      this.indexBackingStrategy = indexBackingStrategy;
      this.mergeScheduler = mergeScheduler;
      this.mergePolicy = mergePolicy;
      this.queryCacheProvider = queryCacheProvider;
      this.ramBufferSizeMb = ramBufferSizeMb;
      this.fieldLimit = fieldLimit;
      this.docsLimit = docsLimit;
      this.enableTextOperatorNewSynonymsSyntax = enableTextOperatorNewSynonymsSyntax;
      this.enableFacetingOverTokenFields = enableFacetingOverTokenFields;
      this.tokenFacetingCardinalityLimit = tokenFacetingCardinalityLimit;
      this.indexFormatVersion = indexFormatVersion;
      this.metricsFactory = metricsFactory;
      this.concurrentSearchExecutor = concurrentSearchExecutor;
      this.concurrentVectorRescoringExecutor = concurrentVectorRescoringExecutor;
      this.metricRefreshExecutor = metricRefreshExecutor;
    }
  }

  LuceneSearchIndex(
      SearchIndexDefinition definition,
      AtomicReference<IndexStatus> statusRef,
      SearchIndexProperties searchIndexProperties)
      throws IOException {
    this.definition = definition;
    this.statusRef = statusRef;
    this.searchIndexProperties = searchIndexProperties;

    this.closed = false;
  }

  /** This constructor initializes the searcher managers and synonym registry. */
  private static LuceneSearchIndex create(
      InstrumentedConcurrentMergeScheduler mergeScheduler,
      MergePolicy mergePolicy,
      QueryCacheProvider queryCacheProvider,
      double ramBufferSizeMb,
      Optional<Integer> fieldLimit,
      Optional<Integer> docsLimit,
      Optional<Integer> maxDocumentsPerSynonymCollection,
      boolean enableTextOperatorNewSynonymsSyntax,
      boolean enableFacetingOverTokenFields,
      Optional<Integer> tokenFacetingCardinalityLimit,
      SearchIndexDefinition indexDefinition,
      IndexFormatVersion indexFormatVersion,
      AnalyzerRegistry analyzerRegistry,
      PerIndexMetricsFactory metricsFactory,
      IndexBackingStrategy indexBackingStrategy,
      Optional<NamedExecutorService> concurrentSearchExecutor,
      Optional<NamedExecutorService> concurrentVectorRescoringExecutor,
      Executor metricRefreshExecutor,
      IndexStatus initialIndexStatus)
      throws IOException {
    checkOverriddenAnalyzersSatisfied(indexDefinition, analyzerRegistry);
    AtomicReference<IndexStatus> statusRef = new AtomicReference<>(initialIndexStatus);

    LuceneSynonymRegistry synonymRegistry =
        LuceneSynonymRegistry.create(
            analyzerRegistry, indexDefinition.getSynonymMap(), maxDocumentsPerSynonymCollection);
    var index =
        new LuceneSearchIndex(
            indexDefinition,
            statusRef,
            new SearchIndexProperties(
                synonymRegistry,
                analyzerRegistry,
                indexBackingStrategy,
                mergeScheduler,
                mergePolicy,
                queryCacheProvider,
                ramBufferSizeMb,
                fieldLimit,
                docsLimit,
                enableTextOperatorNewSynonymsSyntax,
                enableFacetingOverTokenFields,
                tokenFacetingCardinalityLimit,
                indexFormatVersion,
                metricsFactory,
                concurrentSearchExecutor,
                concurrentVectorRescoringExecutor,
                metricRefreshExecutor));
    return index;
  }

  /**
   * Creates a LuceneIndex that is backed by the disk (as opposed to memory). This is the only
   * production code entry point for creating this class.
   */
  static LuceneSearchIndex createDiskBacked(
      Path indexPath,
      Path metadataPath,
      LuceneConfig config,
      FeatureFlags featureFlags,
      InstrumentedConcurrentMergeScheduler mergeScheduler,
      MergePolicy mergePolicy,
      QueryCacheProvider queryCacheProvider,
      ScheduledExecutorService refreshExecutor,
      Optional<NamedExecutorService> concurrentSearchExecutor,
      Optional<NamedExecutorService> concurrentVectorRescoringExecutor,
      SearchIndexDefinition indexDefinition,
      IndexFormatVersion indexFormatVersion,
      AnalyzerRegistry analyzerRegistry,
      AtomicDirectoryRemover directoryRemover,
      PerIndexMetricsFactory metricsFactory,
      Executor metricRefreshExecutor)
      throws IOException {
    return create(
        mergeScheduler,
        mergePolicy,
        queryCacheProvider,
        config.ramBufferSizeMb(),
        config.fieldLimit(),
        config.docsLimit(),
        config.maxDocumentsPerSynonymCollection(),
        config.enableTextOperatorNewSynonymsSyntax(),
        featureFlags.isEnabled(Feature.FACETING_OVER_TOKEN_FIELDS),
        config.tokenFacetingCardinalityLimit(),
        indexDefinition,
        indexFormatVersion,
        analyzerRegistry,
        metricsFactory,
        IndexBackingStrategyFactory.diskBacked(
            refreshExecutor,
            config.refreshInterval(),
            directoryRemover,
            indexPath,
            metadataPath,
            metricsFactory,
            indexDefinition.getNumPartitions()),
        concurrentSearchExecutor,
        concurrentVectorRescoringExecutor,
        metricRefreshExecutor,
        featureFlags.isEnabled(Feature.INITIAL_INDEX_STATUS_UNKNOWN)
            ? IndexStatus.unknown()
            : IndexStatus.notStarted());
  }

  private static void checkOverriddenAnalyzersSatisfied(
      SearchIndexDefinition definition, AnalyzerRegistry analyzerRegistry) {
    Set<String> requiredAnalyzers = definition.getNonStockAnalyzerNames();
    Set<String> providedAnalyzers =
        analyzerRegistry.getAnalyzerDefinitions().stream()
            .map(AnalyzerDefinition::name)
            .collect(Collectors.toSet());
    // We only need required analyzers to be provided, not provided analyzers to be required.
    Check.checkState(
        providedAnalyzers.containsAll(requiredAnalyzers),
        "analyzer registry does not contain all defined overridden analyzers [%s != %s]",
        requiredAnalyzers,
        providedAnalyzers);
  }

  @Override
  public SearchIndexDefinition getDefinition() {
    return this.definition;
  }

  @Override
  public void setStatus(IndexStatus status) {
    this.statusRef.set(status);
  }

  @Override
  public IndexStatus getStatus() {
    return this.statusRef.get();
  }

  @Override
  public SynonymRegistry getSynonymRegistry() {
    return this.searchIndexProperties.synonymRegistry;
  }

  @Override
  public synchronized void drop() throws IOException {
    LOG.atInfo().addKeyValue("indexId", this.definition.getIndexId()).log("Dropping index");
    checkState(this.closed, "Index must be closed prior to calling drop()");
    this.searchIndexProperties.indexBackingStrategy.releaseResources();
  }

  @Override
  public synchronized void close() throws IOException {
    if (this.closed) {
      return;
    }
    LOG.atInfo().addKeyValue("indexId", this.definition.getIndexId()).log("Closing index");
    // Immediately flip the flag so other calls see that the Index is closed.
    this.closed = true;
  }

  @Override
  public synchronized boolean isClosed() {
    return this.closed;
  }

  @Override
  public boolean isCompatibleWith(IndexDefinition indexDefinition) {
    return indexDefinition.getType() == IndexDefinition.Type.SEARCH;
  }

  @Override
  public void throwIfUnavailableForQuerying() throws IndexUnavailableException {
    IndexStatus.StatusCode statusCode = this.getStatus().getStatusCode();
    switch (statusCode) {
      case INITIAL_SYNC ->
          throw new InitialSyncIndexUnavailableException(
              String.format(
                  "cannot query search index %s while in state %s", this.definition, statusCode));
      case UNKNOWN, NOT_STARTED, FAILED ->
          throw new IndexUnavailableException(
              String.format(
                  "cannot query search index %s while in state %s", this.definition, statusCode));
      case STEADY, STALE, RECOVERING_TRANSIENT, RECOVERING_NON_TRANSIENT, DOES_NOT_EXIST -> {
        // do nothing
      }
    }
  }

  public SearchIndexProperties getSearchIndexProperties() {
    return this.searchIndexProperties;
  }

  public Supplier<IndexStatus> getStatusRef() {
    return this.statusRef::get;
  }
}
