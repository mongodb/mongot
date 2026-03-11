package com.xgen.mongot.index.lucene;

import static com.xgen.mongot.index.definition.IndexDefinitionGeneration.Type;

import com.google.common.annotations.VisibleForTesting;
import com.xgen.mongot.featureflag.Feature;
import com.xgen.mongot.featureflag.FeatureFlags;
import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlagRegistry;
import com.xgen.mongot.index.Index;
import com.xgen.mongot.index.IndexFactory;
import com.xgen.mongot.index.IndexMetricsUpdater;
import com.xgen.mongot.index.InitializedIndex;
import com.xgen.mongot.index.analyzer.AnalyzerRegistry;
import com.xgen.mongot.index.analyzer.AnalyzerRegistryFactory;
import com.xgen.mongot.index.analyzer.InvalidAnalyzerDefinitionException;
import com.xgen.mongot.index.definition.IndexDefinitionGeneration;
import com.xgen.mongot.index.definition.IndexSortValidator;
import com.xgen.mongot.index.definition.SearchIndexDefinition;
import com.xgen.mongot.index.definition.VectorIndexDefinition;
import com.xgen.mongot.index.lucene.blobstore.LuceneIndexSnapshotter;
import com.xgen.mongot.index.lucene.blobstore.LuceneIndexSnapshotterManager;
import com.xgen.mongot.index.lucene.config.LuceneConfig;
import com.xgen.mongot.index.lucene.directory.ByteReadCollector;
import com.xgen.mongot.index.lucene.directory.EnvironmentVariantPerfConfig;
import com.xgen.mongot.index.lucene.directory.IndexDirectoryFactory;
import com.xgen.mongot.index.lucene.directory.IndexDirectoryHelper;
import com.xgen.mongot.index.lucene.merge.DiskUtilizationAwareMergePolicy;
import com.xgen.mongot.index.lucene.merge.InstrumentedConcurrentMergeScheduler;
import com.xgen.mongot.index.lucene.merge.MergePolicyFactory;
import com.xgen.mongot.index.lucene.searcher.QueryCacheProvider;
import com.xgen.mongot.index.version.GenerationId;
import com.xgen.mongot.metrics.MeterAndFtdcRegistry;
import com.xgen.mongot.metrics.MetricsFactory;
import com.xgen.mongot.metrics.PerIndexMetricsFactory;
import com.xgen.mongot.monitor.DiskMonitor;
import com.xgen.mongot.monitor.Gate;
import com.xgen.mongot.util.AtomicDirectoryRemover;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.CollectionUtils;
import com.xgen.mongot.util.Crash;
import com.xgen.mongot.util.FileUtils;
import com.xgen.mongot.util.concurrent.Executors;
import com.xgen.mongot.util.concurrent.MeteredCallerRunsPolicy;
import com.xgen.mongot.util.concurrent.NamedExecutorService;
import com.xgen.mongot.util.concurrent.NamedScheduledExecutorService;
import io.micrometer.core.instrument.MeterRegistry;
import java.io.IOException;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.lucene.index.MergePolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import oshi.SystemInfo;

public class LuceneIndexFactory implements IndexFactory {
  private static final Logger LOG = LoggerFactory.getLogger(LuceneIndexFactory.class);
  private static final long CACHE_WARMER_MAX_UPTIME_MINUTES = 30L; // VM likely rebooted

  /** Pre-built plumbing objects shared by all LuceneIndexFactory variants. */
  protected record IndexFactoryContext(
      AtomicDirectoryRemover indexRemover,
      AnalyzerRegistryFactory analyzerRegistryFactory,
      InstrumentedConcurrentMergeScheduler mergeScheduler,
      MergePolicy mergePolicy,
      Optional<MergePolicy> vectorMergePolicy,
      QueryCacheProvider queryCacheProvider,
      NamedScheduledExecutorService refreshExecutor,
      Optional<NamedExecutorService> concurrentSearchExecutor,
      Optional<NamedExecutorService> concurrentVectorRescoringExecutor,
      MetricsFactory metricsFactory,
      IndexDirectoryHelper indexDirectoryHelper) {

    public static IndexFactoryContext create(
        LuceneConfig config,
        FeatureFlags featureFlags,
        MeterAndFtdcRegistry meterAndFtdcRegistry,
        AnalyzerRegistryFactory analyzerRegistryFactory,
        DiskMonitor diskMonitor)
        throws IOException {
      var meterRegistry = meterAndFtdcRegistry.meterRegistry();

      var mergeScheduler = getInstrumentedConcurrentMergeScheduler(config, meterRegistry);

      Gate mergeGate = DiskUtilizationAwareMergePolicy.createMergeGate(config, diskMonitor);
      // Pass the merge gate to the scheduler for disk-based pause/resume support
      if (featureFlags.isEnabled(Feature.CANCEL_MERGE)) {
        mergeScheduler.setMergeGate(mergeGate);
      }
      MergePolicy mergePolicy =
          MergePolicyFactory.createMergePolicy(config, mergeGate, meterRegistry);
      QueryCacheProvider queryCacheProvider =
          featureFlags.isEnabled(Feature.INSTRUMENTED_QUERY_CACHE)
              ? new QueryCacheProvider.MeteredQueryCacheProvider(meterRegistry)
              : new QueryCacheProvider.DefaultQueryCacheProvider();
      Optional<MergePolicy> vectorMergePolicy =
          MergePolicyFactory.createVectorMergePolicy(
              config, featureFlags, mergeGate, meterRegistry);

      var refreshExecutor =
          Executors.fixedSizeThreadScheduledExecutor(
              "index-refresh", config.refreshExecutorThreads(), meterRegistry);

      Optional<NamedExecutorService> concurrentSearchExecutor =
          config.enableConcurrentSearch()
              ? Optional.of(
                  Executors.fixedSizeThreadPool(
                      "concurrent-search",
                      config.concurrentSearchExecutorThreads(),
                      config.concurrentSearchExecutorQueueSize(),
                      new MeteredCallerRunsPolicy(
                          meterRegistry.counter("rejectedConcurrentSearchExecutionCount")),
                      meterRegistry))
              : Optional.empty();

      Optional<NamedExecutorService> concurrentVectorRescoringExecutor =
          config.enableConcurrentSearch()
              ? Optional.of(
                  Executors.fixedSizeThreadPool(
                      "concurrent-vector-rescoring",
                      config.concurrentVectorRescoringExecutorThreads(),
                      config.concurrentVectorRescoringExecutorQueueSize(),
                      new MeteredCallerRunsPolicy(
                          meterRegistry.counter("rejectedConcurrentVectorRescoringExecutionCount")),
                      meterRegistry))
              : Optional.empty();

      var metricsFactory = new MetricsFactory("indexFactory", meterRegistry);
      var indexDirectoryHelper = IndexDirectoryHelper.create(config.dataPath(), metricsFactory);

      return new IndexFactoryContext(
          indexDirectoryHelper.getIndexRemover(),
          analyzerRegistryFactory,
          mergeScheduler,
          mergePolicy,
          vectorMergePolicy,
          queryCacheProvider,
          refreshExecutor,
          concurrentSearchExecutor,
          concurrentVectorRescoringExecutor,
          metricsFactory,
          indexDirectoryHelper);
    }

    private static InstrumentedConcurrentMergeScheduler getInstrumentedConcurrentMergeScheduler(
        LuceneConfig config, MeterRegistry meterRegistry) {
      long cancelMergeTimeout =
          config
              .cancelMergePerThreadTimeoutMs()
              .orElse(
                  InstrumentedConcurrentMergeScheduler.DEFAULT_CANCEL_MERGE_PER_THREAD_TIMEOUT_MS);
      long cancelAllMergesTimeout =
          config
              .cancelAllMergesPerThreadTimeoutMs()
              .orElse(
                  InstrumentedConcurrentMergeScheduler
                      .DEFAULT_CANCEL_ALL_MERGES_PER_THREAD_TIMEOUT_MS);
      var mergeScheduler =
          new InstrumentedConcurrentMergeScheduler(
              meterRegistry, cancelMergeTimeout, cancelAllMergesTimeout);
      mergeScheduler.setMaxMergesAndThreads(config.numMaxMerges(), config.numMaxMergeThreads());
      return mergeScheduler;
    }
  }

  protected final LuceneConfig config;
  protected final AtomicDirectoryRemover indexRemover;
  private final AnalyzerRegistryFactory analyzerRegistryFactory;
  protected final InstrumentedConcurrentMergeScheduler mergeScheduler;
  protected final MergePolicy mergePolicy;
  private final Optional<MergePolicy> vectorMergePolicy;
  private final QueryCacheProvider queryCacheProvider;
  protected final NamedScheduledExecutorService refreshExecutor;
  private final Optional<NamedExecutorService> concurrentSearchExecutor;
  private final Optional<NamedExecutorService> concurrentVectorRescoringExecutor;
  private final Optional<LuceneIndexSnapshotterManager> luceneIndexSnapshotterManager;
  private final Optional<ByteReadCollector> byteReadCollector;

  protected final MeterAndFtdcRegistry meterAndFtdcRegistry;
  private final MetricsFactory metricsFactory;
  protected final IndexDirectoryHelper indexDirectoryHelper;
  protected final FeatureFlags featureFlags;
  private final DynamicFeatureFlagRegistry dynamicFeatureFlagRegistry;
  private final EnvironmentVariantPerfConfig environmentVariantPerfConfig;
  private final Optional<SystemInfo> systemInfo;
  private final AtomicLong cacheWarmerTotalMilliseconds;
  private volatile boolean cacheWarmerAlreadyDisabled;

  @VisibleForTesting
  protected LuceneIndexFactory(
      LuceneConfig config,
      AtomicDirectoryRemover indexRemover,
      AnalyzerRegistryFactory analyzerRegistryFactory,
      InstrumentedConcurrentMergeScheduler mergeScheduler,
      MergePolicy mergePolicy,
      Optional<MergePolicy> vectorMergePolicy,
      QueryCacheProvider queryCacheProvider,
      NamedScheduledExecutorService refreshExecutor,
      Optional<NamedExecutorService> concurrentSearchExecutor,
      Optional<NamedExecutorService> concurrentVectorRescoringExecutor,
      Optional<LuceneIndexSnapshotterManager> luceneSnapshotterManager,
      MeterAndFtdcRegistry meterAndFtdcRegistry,
      MetricsFactory metricsFactory,
      IndexDirectoryHelper indexDirectoryHelper,
      FeatureFlags featureFlags,
      DynamicFeatureFlagRegistry dynamicFeatureFlagRegistry,
      EnvironmentVariantPerfConfig environmentVariantPerfConfig,
      Optional<SystemInfo> systemInfo) {
    this.config = config;
    this.indexRemover = indexRemover;
    this.analyzerRegistryFactory = analyzerRegistryFactory;
    this.mergeScheduler = mergeScheduler;
    this.mergePolicy = mergePolicy;
    this.vectorMergePolicy = vectorMergePolicy;
    this.queryCacheProvider = queryCacheProvider;
    this.refreshExecutor = refreshExecutor;
    this.concurrentSearchExecutor = concurrentSearchExecutor;
    this.concurrentVectorRescoringExecutor = concurrentVectorRescoringExecutor;
    this.luceneIndexSnapshotterManager = luceneSnapshotterManager;
    this.meterAndFtdcRegistry = meterAndFtdcRegistry;
    this.metricsFactory = metricsFactory;
    this.indexDirectoryHelper = indexDirectoryHelper;
    this.featureFlags = featureFlags;
    this.dynamicFeatureFlagRegistry = dynamicFeatureFlagRegistry;
    this.environmentVariantPerfConfig = environmentVariantPerfConfig;
    if (this.environmentVariantPerfConfig.isByteReadInstrumentationEnabled()) {
      this.byteReadCollector = Optional.of(new ByteReadCollector(metricsFactory));
    } else {
      this.byteReadCollector = Optional.empty();
    }
    this.systemInfo = systemInfo;
    this.cacheWarmerTotalMilliseconds =
        this.metricsFactory.timeGauge(
            "cacheWarmerTotalMilliseconds", new AtomicLong(), AtomicLong::doubleValue);
    this.cacheWarmerAlreadyDisabled = false;
  }

  LuceneIndexFactory(
      LuceneConfig config,
      AtomicDirectoryRemover indexRemover,
      AnalyzerRegistryFactory analyzerRegistryFactory,
      InstrumentedConcurrentMergeScheduler mergeScheduler,
      MergePolicy mergePolicy,
      Optional<MergePolicy> vectorMergePolicy,
      QueryCacheProvider queryCacheProvider,
      NamedScheduledExecutorService refreshExecutor,
      Optional<NamedExecutorService> concurrentSearchExecutor,
      Optional<NamedExecutorService> concurrentVectorRescoringExecutor,
      Optional<LuceneIndexSnapshotterManager> luceneSnapshotterManager,
      MeterAndFtdcRegistry meterAndFtdcRegistry,
      MetricsFactory metricsFactory,
      IndexDirectoryHelper indexDirectoryHelper,
      FeatureFlags featureFlags,
      DynamicFeatureFlagRegistry dynamicFeatureFlagRegistry,
      EnvironmentVariantPerfConfig environmentVariantPerfConfig) {
    this(
        config,
        indexRemover,
        analyzerRegistryFactory,
        mergeScheduler,
        mergePolicy,
        vectorMergePolicy,
        queryCacheProvider,
        refreshExecutor,
        concurrentSearchExecutor,
        concurrentVectorRescoringExecutor,
        luceneSnapshotterManager,
        meterAndFtdcRegistry,
        metricsFactory,
        indexDirectoryHelper,
        featureFlags,
        dynamicFeatureFlagRegistry,
        environmentVariantPerfConfig,
        LuceneIndexFactory.tryNewSystemInfo());
  }

  @VisibleForTesting
  protected LuceneIndexFactory(
      LuceneConfig config,
      IndexFactoryContext ctx,
      Optional<LuceneIndexSnapshotterManager> snapshotterManager,
      MeterAndFtdcRegistry meterAndFtdcRegistry,
      FeatureFlags featureFlags,
      DynamicFeatureFlagRegistry dynamicFeatureFlagRegistry,
      EnvironmentVariantPerfConfig environmentVariantPerfConfig,
      Optional<SystemInfo> systemInfo) {
    this(
        config,
        ctx.indexRemover(),
        ctx.analyzerRegistryFactory(),
        ctx.mergeScheduler(),
        ctx.mergePolicy(),
        ctx.vectorMergePolicy(),
        ctx.queryCacheProvider(),
        ctx.refreshExecutor(),
        ctx.concurrentSearchExecutor(),
        ctx.concurrentVectorRescoringExecutor(),
        snapshotterManager,
        meterAndFtdcRegistry,
        ctx.metricsFactory(),
        ctx.indexDirectoryHelper(),
        featureFlags,
        dynamicFeatureFlagRegistry,
        environmentVariantPerfConfig,
        systemInfo);
  }

  /** Creates LuceneIndexFactory. */
  public static LuceneIndexFactory fromConfig(
      LuceneConfig config,
      FeatureFlags featureFlags,
      DynamicFeatureFlagRegistry dynamicFeatureFlagRegistry,
      EnvironmentVariantPerfConfig environmentVariantPerfConfig,
      MeterAndFtdcRegistry meterAndFtdcRegistry,
      Optional<LuceneIndexSnapshotterManager> snapshotterManager,
      AnalyzerRegistryFactory analyzerRegistryFactory,
      DiskMonitor diskMonitor,
      Optional<SystemInfo> systemInfo)
      throws IOException {
    var ctx =
        IndexFactoryContext.create(
            config, featureFlags, meterAndFtdcRegistry, analyzerRegistryFactory, diskMonitor);
    return new LuceneIndexFactory(
        config,
        ctx,
        snapshotterManager,
        meterAndFtdcRegistry,
        featureFlags,
        dynamicFeatureFlagRegistry,
        environmentVariantPerfConfig,
        systemInfo);
  }

  /** Creates LuceneIndexFactory. */
  public static LuceneIndexFactory fromConfig(
      LuceneConfig config,
      FeatureFlags featureFlags,
      DynamicFeatureFlagRegistry dynamicFeatureFlagRegistry,
      EnvironmentVariantPerfConfig environmentVariantPerfConfig,
      MeterAndFtdcRegistry meterAndFtdcRegistry,
      Optional<LuceneIndexSnapshotterManager> snapshotterManager,
      AnalyzerRegistryFactory analyzerRegistryFactory,
      DiskMonitor diskMonitor)
      throws IOException {
    return LuceneIndexFactory.fromConfig(
        config,
        featureFlags,
        dynamicFeatureFlagRegistry,
        environmentVariantPerfConfig,
        meterAndFtdcRegistry,
        snapshotterManager,
        analyzerRegistryFactory,
        diskMonitor,
        LuceneIndexFactory.tryNewSystemInfo());
  }

  protected static Optional<SystemInfo> tryNewSystemInfo() {
    try {
      return Optional.of(new SystemInfo());
    } catch (Exception | LinkageError e) {
      LOG.atWarn()
          .setCause(e)
          .log("failed to create OSHI SystemInfo (ignored because this is optional)");
      return Optional.empty();
    }
  }

  /** Must be called after all associated indexes are closed. */
  @Override
  public void close() {
    LOG.info("Shutting down.");
    Executors.shutdownOrFail(this.refreshExecutor);
    this.concurrentSearchExecutor.ifPresent(Executors::shutdownOrFail);

    // Cancel all ongoing merges across all indices before closing the scheduler.
    // This is an optimization to speed up shutdown - if it fails, we log a warning
    // and continue. The subsequent mergeScheduler.close() will still wait for
    // merges to complete.
    // This behavior is controlled by the CANCEL_MERGE feature flag.
    if (this.featureFlags.isEnabled(Feature.CANCEL_MERGE)) {
      LOG.info("Cancelling all ongoing merges across all indices for shutdown.");
      try {
        this.mergeScheduler.cancelAllMerges();
      } catch (Exception e) {
        LOG.warn("Failed to cancel all merges during shutdown, continuing with shutdown anyway", e);
      }
    }

    Crash.because("failed to close merge scheduler").ifThrows(() -> this.mergeScheduler.close());
    this.metricsFactory.close();
  }

  @Override
  public Index getIndex(IndexDefinitionGeneration definitionGeneration)
      throws InvalidAnalyzerDefinitionException, IOException {

    LOG.atInfo()
        .addKeyValue("indexId", definitionGeneration.getGenerationId().indexId)
        .addKeyValue("generationId", definitionGeneration.getGenerationId())
        .log("creating index");

    var metricsFactory =
        new PerIndexMetricsFactory(
            IndexMetricsUpdater.NAMESPACE,
            this.meterAndFtdcRegistry,
            definitionGeneration.getGenerationId());

    if (definitionGeneration.getType() == Type.VECTOR) {
      VectorIndexDefinition vectorDef =
          definitionGeneration.getIndexDefinition().asVectorDefinition();
      if (vectorDef.isCustomVectorEngineIndex()) {
        return createCustomVectorEngineIndex(vectorDef, definitionGeneration, metricsFactory);
      }
      return LuceneVectorIndex.createDiskBacked(
          this.indexDirectoryHelper.getIndexDirectoryPath(definitionGeneration),
          this.indexDirectoryHelper.getIndexMetadataPath(definitionGeneration),
          this.config,
          this.featureFlags,
          this.mergeScheduler,
          this.vectorMergePolicy.orElse(this.mergePolicy),
          this.queryCacheProvider,
          this.refreshExecutor,
          this.concurrentSearchExecutor,
          this.concurrentVectorRescoringExecutor,
          vectorDef,
          definitionGeneration.generation().indexFormatVersion,
          this.indexRemover,
          metricsFactory);
    }

    SearchIndexDefinition searchDefinition =
        definitionGeneration.getIndexDefinition().asSearchDefinition();
    if (searchDefinition.getSort().isPresent()) {
      IndexSortValidator.checkSortedIndexEnabled(searchDefinition, this.featureFlags);
    }
    AnalyzerRegistry analyzerRegistry =
        this.analyzerRegistryFactory.create(
            CollectionUtils.concat(
                definitionGeneration.asSearch().definition().analyzerDefinitions(),
                searchDefinition.getAnalyzers()),
            this.featureFlags.isEnabled(Feature.TRUNCATE_AUTOCOMPLETE_TOKENS));

    boolean hasVectorField =
        searchDefinition.getMappings().fields().values().stream()
            .anyMatch(field -> field.vectorFieldSpecification().isPresent());
    return LuceneSearchIndex.createDiskBacked(
        this.indexDirectoryHelper.getIndexDirectoryPath(definitionGeneration),
        this.indexDirectoryHelper.getIndexMetadataPath(definitionGeneration),
        this.config,
        this.featureFlags,
        this.mergeScheduler,
        hasVectorField ? this.vectorMergePolicy.orElse(this.mergePolicy) : this.mergePolicy,
        this.queryCacheProvider,
        this.refreshExecutor,
        this.concurrentSearchExecutor,
        this.concurrentVectorRescoringExecutor,
        searchDefinition,
        definitionGeneration.generation().indexFormatVersion,
        analyzerRegistry,
        this.indexRemover,
        metricsFactory);
  }

  protected Index createCustomVectorEngineIndex(
      VectorIndexDefinition vectorDef,
      IndexDefinitionGeneration definitionGeneration,
      PerIndexMetricsFactory metricsFactory) {
    throw new UnsupportedOperationException(
        "Custom vector engine indexes are not supported in the community build");
  }

  protected InitializedIndex initializeCustomVectorEngineIndex(
      Index index, GenerationId generationId, IndexDirectoryFactory directoryFactory)
      throws IOException {
    throw new UnsupportedOperationException(
        "Custom vector engine indexes are not supported in the community build");
  }

  @Override
  public InitializedIndex getInitializedIndex(
      Index index, IndexDefinitionGeneration definitionGeneration) throws IOException {
    try {
      FileUtils.mkdirIfNotExist(
          this.indexDirectoryHelper.getIndexMetadataPath(definitionGeneration));
      FileUtils.mkdirIfNotExist(
          this.indexDirectoryHelper.getIndexDirectoryPath(definitionGeneration));
    } catch (IOException e) {
      throw new IllegalStateException(e.getMessage());
    }

    Optional<LuceneIndexSnapshotter> luceneIndexSnapshotter =
        this.luceneIndexSnapshotterManager.flatMap(
            manager -> manager.get(definitionGeneration.getGenerationId()));
    var directoryFactory =
        new IndexDirectoryFactory(
            this.indexDirectoryHelper,
            definitionGeneration,
            this.config,
            this.byteReadCollector,
            this.isCacheWarmerEnabled(),
            Optional.of(this.cacheWarmerTotalMilliseconds));
    GenerationId generationId = definitionGeneration.getGenerationId();

    if (definitionGeneration.getType() == Type.VECTOR) {
      VectorIndexDefinition vectorDef =
          definitionGeneration.getIndexDefinition().asVectorDefinition();
      if (vectorDef.isCustomVectorEngineIndex()) {
        return initializeCustomVectorEngineIndex(index, generationId, directoryFactory);
      }
      var luceneVectorIndex = Check.instanceOf(index, LuceneVectorIndex.class);
      return InitializedLuceneVectorIndex.create(
          luceneVectorIndex,
          generationId,
          directoryFactory,
          this.indexDirectoryHelper,
          luceneIndexSnapshotter,
          this.featureFlags);
    } else {
      var luceneSearchIndex = Check.instanceOf(index, LuceneSearchIndex.class);
      return InitializedLuceneSearchIndex.create(
          luceneSearchIndex,
          generationId,
          directoryFactory,
          this.indexDirectoryHelper,
          luceneIndexSnapshotter,
          this.featureFlags,
          this.dynamicFeatureFlagRegistry);
    }
  }

  boolean isCacheWarmerEnabled() {
    if (this.cacheWarmerAlreadyDisabled) {
      return false; // Decide very quickly after the cache warmer becomes disabled one time.
    }
    if (!LuceneIndexFactory.isCacheWarmerEnabled(this.featureFlags, this.systemInfo)) {
      this.cacheWarmerAlreadyDisabled = true;
      return false;
    }
    return true;
  }

  @VisibleForTesting
  static boolean isCacheWarmerEnabled(FeatureFlags featureFlags, Optional<SystemInfo> systemInfo) {
    try {
      if (!featureFlags.isEnabled(Feature.CACHE_WARMER)) {
        return false;
      }

      if (systemInfo.isEmpty()) {
        return false;
      }

      long uptimeSeconds = systemInfo.get().getOperatingSystem().getSystemUptime();
      long maxSeconds =
          Duration.ofMinutes(LuceneIndexFactory.CACHE_WARMER_MAX_UPTIME_MINUTES).toSeconds();
      if (uptimeSeconds > maxSeconds) {
        long uptimeMinutes = Duration.ofSeconds(uptimeSeconds + 59L).toMinutes();
        LOG.atInfo()
            .addKeyValue("uptimeSeconds", uptimeSeconds)
            .addKeyValue(
                "CACHE_WARMER_MAX_UPTIME_MINUTES",
                LuceneIndexFactory.CACHE_WARMER_MAX_UPTIME_MINUTES)
            .log(
                "Cache Warmer: disabled because system uptime {} minutes exceeds {} minutes",
                uptimeMinutes,
                LuceneIndexFactory.CACHE_WARMER_MAX_UPTIME_MINUTES);
        return false;
      }

      return true;
    } catch (Exception | LinkageError e) {
      LOG.atWarn()
          .setCause(e)
          .log("Cache Warmer: failure (ignored because this is only an optimization)");
      return false;
    }
  }
}
