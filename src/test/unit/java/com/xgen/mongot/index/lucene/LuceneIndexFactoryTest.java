package com.xgen.mongot.index.lucene;

import static com.google.common.truth.Truth.assertThat;
import static com.xgen.mongot.index.version.Generation.CURRENT;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.xgen.mongot.featureflag.Feature;
import com.xgen.mongot.featureflag.FeatureFlags;
import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlagRegistry;
import com.xgen.mongot.index.EncodedUserData;
import com.xgen.mongot.index.InitializedIndex;
import com.xgen.mongot.index.MeteredIndexWriter;
import com.xgen.mongot.index.analyzer.AnalyzerRegistry;
import com.xgen.mongot.index.analyzer.AnalyzerRegistryFactory;
import com.xgen.mongot.index.analyzer.InvalidAnalyzerDefinitionException;
import com.xgen.mongot.index.definition.SearchIndexDefinitionGeneration;
import com.xgen.mongot.index.definition.SearchIndexVectorFieldDefinition;
import com.xgen.mongot.index.definition.VectorFieldSpecification;
import com.xgen.mongot.index.definition.VectorIndexDefinitionGeneration;
import com.xgen.mongot.index.definition.VectorIndexingAlgorithm;
import com.xgen.mongot.index.definition.VectorSimilarity;
import com.xgen.mongot.index.definition.quantization.VectorQuantization;
import com.xgen.mongot.index.lucene.config.LuceneConfig;
import com.xgen.mongot.index.lucene.directory.EnvironmentVariantPerfConfig;
import com.xgen.mongot.index.lucene.directory.IndexDirectoryHelper;
import com.xgen.mongot.index.lucene.merge.InstrumentedConcurrentMergeScheduler;
import com.xgen.mongot.index.lucene.merge.VectorMergePolicy;
import com.xgen.mongot.index.lucene.searcher.QueryCacheProvider;
import com.xgen.mongot.index.lucene.writer.SingleLuceneIndexWriter;
import com.xgen.mongot.index.query.sort.UserFieldSortOptions;
import com.xgen.mongot.index.version.Generation;
import com.xgen.mongot.index.version.IndexFormatVersion;
import com.xgen.mongot.metrics.MeterAndFtdcRegistry;
import com.xgen.mongot.metrics.MetricsFactory;
import com.xgen.mongot.monitor.NoOpDiskMonitor;
import com.xgen.mongot.replication.mongodb.common.AutoEmbeddingCommitMetadata;
import com.xgen.mongot.replication.mongodb.common.IndexCommitUserData;
import com.xgen.mongot.util.AtomicDirectoryRemover;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.concurrent.Executors;
import com.xgen.testing.TestUtils;
import com.xgen.testing.mongot.index.definition.DocumentFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.FieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.SearchIndexDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.SearchIndexDefinitionGenerationBuilder;
import com.xgen.testing.mongot.index.definition.VectorIndexDefinitionGenerationBuilder;
import com.xgen.testing.mongot.index.lucene.LuceneConfigBuilder;
import com.xgen.testing.mongot.index.query.sort.SortFieldBuilder;
import com.xgen.testing.mongot.index.query.sort.SortSpecBuilder;
import com.xgen.testing.mongot.mock.index.SearchIndex;
import com.xgen.testing.mongot.mock.index.VectorIndex;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Optional;
import org.apache.lucene.index.MergePolicy;
import org.bson.types.ObjectId;
import org.junit.Assert;
import org.junit.Test;
import oshi.SystemInfo;
import oshi.software.os.OperatingSystem;

public class LuceneIndexFactoryTest {
  @Test
  public void testIndexDirectoryChosenByGenerationId()
      throws InvalidAnalyzerDefinitionException, IOException {
    var cfg = LuceneConfigBuilder.builder().tempDataPath().build();
    var path = cfg.dataPath();
    var indexId = new ObjectId("111122223333444455556666");
    try (LuceneIndexFactory factory =
        LuceneIndexFactory.fromConfig(
            cfg,
            FeatureFlags.getDefault(),
            new DynamicFeatureFlagRegistry(
                Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty()),
            EnvironmentVariantPerfConfig.getDefault(),
            MeterAndFtdcRegistry.createWithSimpleRegistries(),
            Optional.empty(),
            AnalyzerRegistry.factory(),
            new NoOpDiskMonitor())) {

      // the first generation id is unique and not encoded with the generation counters for
      // backwards compatibility
      assertGenerationStoredAtPath(
          factory, indexId, Generation.FIRST, path.resolve("111122223333444455556666"));

      var attemptIncremented = Generation.FIRST.nextAttempt();
      assertGenerationStoredAtPath(
          factory, indexId, attemptIncremented, path.resolve("111122223333444455556666_f5_u0_a1"));

      var userIncremented = Generation.FIRST.incrementUser();
      assertGenerationStoredAtPath(
          factory, indexId, userIncremented, path.resolve("111122223333444455556666_f5_u1_a0"));

      var formatIncremented =
          new Generation(
              Generation.FIRST.userIndexVersion,
              IndexFormatVersion.create(Generation.FIRST.formatVersion.versionNumber() + 1));
      assertGenerationStoredAtPath(
          factory, indexId, formatIncremented, path.resolve("111122223333444455556666_f6_u0_a0"));
    }
  }

  private void assertGenerationStoredAtPath(
      LuceneIndexFactory factory, ObjectId indexId, Generation generation, Path path)
      throws IOException, InvalidAnalyzerDefinitionException {
    var definition = SearchIndex.mockSearchDefinition(indexId);
    SearchIndexDefinitionGeneration definitionGeneration =
        SearchIndexDefinitionGenerationBuilder.create(
            definition, generation, Collections.emptyList());
    var index = factory.getIndex(definitionGeneration);
    Assert.assertFalse(path.toFile().exists());
    InitializedIndex initializedIndex = factory.getInitializedIndex(index, definitionGeneration);
    Assert.assertTrue(path.toFile().exists());
    initializedIndex.close();
    Assert.assertTrue(path.toFile().exists());
    index.drop();
  }

  @Test
  public void testSharedMergeSchedulerPropertiesSetUp()
      throws IOException, InvalidAnalyzerDefinitionException {
    LuceneIndexFactory factory =
        LuceneIndexFactory.fromConfig(
            LuceneConfigBuilder.builder().tempDataPath().build(),
            FeatureFlags.getDefault(),
            new DynamicFeatureFlagRegistry(
                Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty()),
            EnvironmentVariantPerfConfig.getDefault(),
            MeterAndFtdcRegistry.createWithSimpleRegistries(),
            Optional.empty(),
            AnalyzerRegistry.factory(),
            new NoOpDiskMonitor());

    SearchIndexDefinitionGeneration definitionGeneration1 =
        SearchIndexDefinitionGenerationBuilder.create(
            SearchIndex.mockSearchDefinition(new ObjectId()), CURRENT, Collections.emptyList());
    LuceneSearchIndex index1 =
        Check.instanceOf(factory.getIndex(definitionGeneration1), LuceneSearchIndex.class);

    InitializedIndex initializedIndex = factory.getInitializedIndex(index1, definitionGeneration1);
    SearchIndexDefinitionGeneration definitionGeneration2 =
        SearchIndexDefinitionGenerationBuilder.create(
            SearchIndex.mockSearchDefinition(new ObjectId()), CURRENT, Collections.emptyList());
    var index2 = Check.instanceOf(factory.getIndex(definitionGeneration2), LuceneSearchIndex.class);
    InitializedIndex initializedIndex2 = factory.getInitializedIndex(index2, definitionGeneration2);
    var index1MergeScheduler =
        ((SingleLuceneIndexWriter) ((MeteredIndexWriter) initializedIndex.getWriter()).getWrapped())
            .getLuceneWriter()
            .getConfig()
            .getMergeScheduler();
    var index2MergeScheduler =
        ((SingleLuceneIndexWriter)
                ((MeteredIndexWriter) initializedIndex2.getWriter()).getWrapped())
            .getLuceneWriter()
            .getConfig()
            .getMergeScheduler();
    assertThat(index1MergeScheduler)
        .isInstanceOf(InstrumentedConcurrentMergeScheduler.PerIndexPartitionMergeScheduler.class);
    assertThat(index2MergeScheduler)
        .isInstanceOf(InstrumentedConcurrentMergeScheduler.PerIndexPartitionMergeScheduler.class);
    Assert.assertNotSame(index1MergeScheduler, index2MergeScheduler);
    Assert.assertSame(
        ((InstrumentedConcurrentMergeScheduler.PerIndexPartitionMergeScheduler)
                index1MergeScheduler)
            .getIn(),
        ((InstrumentedConcurrentMergeScheduler.PerIndexPartitionMergeScheduler)
                index2MergeScheduler)
            .getIn());
    factory.close();
  }

  private MergePolicy getUnderlyingMergePolicy(InitializedIndex index) {
    return ((SingleLuceneIndexWriter) ((MeteredIndexWriter) index.getWriter()).getWrapped())
        .getLuceneWriter()
        .getConfig()
        .getMergePolicy();
  }

  @Test
  public void testVectorMergePolicy() throws IOException, InvalidAnalyzerDefinitionException {
    LuceneIndexFactory factory =
        LuceneIndexFactory.fromConfig(
            LuceneConfigBuilder.builder()
                .tempDataPath()
                .vectorMergePolicyConfig(
                    new LuceneConfig.VectorMergePolicyConfig(100, 1024, 2048, 1024, 2048))
                .build(),
            FeatureFlags.getDefault(),
            new DynamicFeatureFlagRegistry(
                Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty()),
            EnvironmentVariantPerfConfig.getDefault(),
            MeterAndFtdcRegistry.createWithSimpleRegistries(),
            Optional.empty(),
            AnalyzerRegistry.factory(),
            new NoOpDiskMonitor());

    VectorIndexDefinitionGeneration definitionGeneration1 =
        VectorIndexDefinitionGenerationBuilder.builder()
            .definition(VectorIndex.mockVectorDefinition(new ObjectId()))
            .generation(CURRENT)
            .build();
    var index1 = factory.getIndex(definitionGeneration1);

    VectorIndexDefinitionGeneration definitionGeneration2 =
        VectorIndexDefinitionGenerationBuilder.builder()
            .definition(VectorIndex.mockVectorDefinition(new ObjectId()))
            .generation(CURRENT)
            .build();
    var index2 = factory.getIndex(definitionGeneration2);
    var mergePolicy1 =
        getUnderlyingMergePolicy(factory.getInitializedIndex(index1, definitionGeneration1));
    var mergePolicy2 =
        getUnderlyingMergePolicy(factory.getInitializedIndex(index2, definitionGeneration2));
    Assert.assertSame(mergePolicy1, mergePolicy2);
    assertThat(mergePolicy1).isInstanceOf(VectorMergePolicy.class);
  }

  @Test
  public void testNestedSearchVectorUsesVectorMergePolicy()
      throws IOException, InvalidAnalyzerDefinitionException {
    var config =
        LuceneConfigBuilder.builder()
            .tempDataPath()
            .vectorMergePolicyConfig(
                new LuceneConfig.VectorMergePolicyConfig(100, 1024, 2048, 1024, 2048))
            .build();
    try (LuceneIndexFactory factory =
        LuceneIndexFactory.fromConfig(
            config,
            FeatureFlags.getDefault(),
            new DynamicFeatureFlagRegistry(
                Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty()),
            EnvironmentVariantPerfConfig.getDefault(),
            MeterAndFtdcRegistry.createWithSimpleRegistries(),
            Optional.empty(),
            AnalyzerRegistry.factory(),
            new NoOpDiskMonitor())) {
      var vectorSpecification =
          new VectorFieldSpecification(
              512,
              VectorSimilarity.COSINE,
              VectorQuantization.NONE,
              new VectorIndexingAlgorithm.HnswIndexingAlgorithm());
      var mappings =
          DocumentFieldDefinitionBuilder.builder()
              .dynamic(false)
              .field(
                  "_autoEmbed",
                  FieldDefinitionBuilder.builder()
                      .document(
                          DocumentFieldDefinitionBuilder.builder()
                              .dynamic(false)
                              .field(
                                  "content",
                                  FieldDefinitionBuilder.builder()
                                      .searchIndexVectorField(
                                          new SearchIndexVectorFieldDefinition(vectorSpecification))
                                      .build())
                              .build())
                      .build())
              .build();
      var definition =
          SearchIndexDefinitionBuilder.builder().defaultMetadata().mappings(mappings).build();
      SearchIndexDefinitionGeneration definitionGeneration =
          SearchIndexDefinitionGenerationBuilder.create(
              definition, CURRENT, Collections.emptyList());

      var index = factory.getIndex(definitionGeneration);
      InitializedIndex initializedIndex = factory.getInitializedIndex(index, definitionGeneration);

      assertThat(getUnderlyingMergePolicy(initializedIndex)).isInstanceOf(VectorMergePolicy.class);
      initializedIndex.close();
      index.drop();
    }
  }

  @Test
  public void testGracefulShutdownOfInternalComponents() throws IOException {
    var mergeScheduler = mock(InstrumentedConcurrentMergeScheduler.class);
    SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
    var refreshExecutor = spy(Executors.singleThreadScheduledExecutor("test", 1, meterRegistry));
    var concurrentExecutor = spy(Executors.singleThreadScheduledExecutor("test", 1, meterRegistry));
    var metricRefreshExecutor =
        spy(Executors.fixedSizeThreadPool("metric-refresh", 1, meterRegistry));
    var metricsFactory = spy(new MetricsFactory("indexFactory", meterRegistry));
    var factory =
        new LuceneIndexFactory(
            LuceneConfigBuilder.builder().tempDataPath().build(),
            mock(AtomicDirectoryRemover.class),
            mock(AnalyzerRegistryFactory.class),
            mergeScheduler,
            mock(MergePolicy.class),
            Optional.empty(),
            new QueryCacheProvider.DefaultQueryCacheProvider(),
            refreshExecutor,
            Optional.of(concurrentExecutor),
            Optional.empty(),
            Optional.of(metricRefreshExecutor),
            Optional.empty(),
            MeterAndFtdcRegistry.createWithSimpleRegistries(),
            metricsFactory,
            new IndexDirectoryHelper(
                TestUtils.getTempFolderPath(),
                mock(MetricsFactory.class),
                mock(AtomicDirectoryRemover.class)),
            FeatureFlags.getDefault(),
            new DynamicFeatureFlagRegistry(
                Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty()),
            EnvironmentVariantPerfConfig.getDefault());

    factory.close();

    verify(refreshExecutor, times(1)).shutdown();
    verify(concurrentExecutor, times(1)).shutdown();
    verify(metricRefreshExecutor, times(1)).shutdown();
    verify(mergeScheduler, times(1)).close();
    verify(metricsFactory, times(1)).close();
  }

  @Test
  public void testGetIndexWithSortWhenFeatureDisabledThrowsException()
      throws InvalidAnalyzerDefinitionException, IOException {
    // Create feature flags with SORTED_INDEX disabled
    FeatureFlags featureFlagsWithSortDisabled =
        FeatureFlags.withDefaults().disable(Feature.SORTED_INDEX).build();

    try (LuceneIndexFactory factory =
        LuceneIndexFactory.fromConfig(
            LuceneConfigBuilder.builder().tempDataPath().build(),
            featureFlagsWithSortDisabled,
            new DynamicFeatureFlagRegistry(
                Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty()),
            EnvironmentVariantPerfConfig.getDefault(),
            MeterAndFtdcRegistry.createWithSimpleRegistries(),
            Optional.empty(),
            AnalyzerRegistry.factory(),
            new NoOpDiskMonitor())) {

      // Create a search definition with sort using builder
      var sortSpec =
          SortSpecBuilder.builder()
              .sortField(
                  SortFieldBuilder.builder()
                      .path("field1")
                      .sortOption(UserFieldSortOptions.DEFAULT_ASC)
                      .build())
              .buildSort();

      var definitionWithSort =
          SearchIndexDefinitionBuilder.builder()
              .defaultMetadata()
              .mappings(SearchIndex.mockSearchDefinition(new ObjectId()).getMappings())
              .sort(sortSpec)
              .build();

      SearchIndexDefinitionGeneration definitionGeneration =
          SearchIndexDefinitionGenerationBuilder.create(
              definitionWithSort, CURRENT, Collections.emptyList());

      // Expect IllegalArgumentException when trying to create index with sort
      IllegalArgumentException exception =
          Assert.assertThrows(
              IllegalArgumentException.class, () -> factory.getIndex(definitionGeneration));

      assertThat(exception.getMessage())
          .contains(
              "Sort configuration is not supported, " + "Feature flag SORTED_INDEX is not enabled");
    }
  }

  @Test
  public void testGetIndexWithSortWhenFeatureDisabledSucceed()
      throws InvalidAnalyzerDefinitionException, IOException {
    // Create feature flags with SORTED_INDEX enabled
    FeatureFlags featureFlagsWithSortDisabled =
        FeatureFlags.withDefaults().enable(Feature.SORTED_INDEX).build();

    try (LuceneIndexFactory factory =
        LuceneIndexFactory.fromConfig(
            LuceneConfigBuilder.builder().tempDataPath().build(),
            featureFlagsWithSortDisabled,
            new DynamicFeatureFlagRegistry(
                Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty()),
            EnvironmentVariantPerfConfig.getDefault(),
            MeterAndFtdcRegistry.createWithSimpleRegistries(),
            Optional.empty(),
            AnalyzerRegistry.factory(),
            new NoOpDiskMonitor())) {

      // Create a search definition with sort using builder
      var sortSpec =
          SortSpecBuilder.builder()
              .sortField(
                  SortFieldBuilder.builder()
                      .path("field1")
                      .sortOption(UserFieldSortOptions.DEFAULT_ASC)
                      .build())
              .buildSort();

      var definitionWithSort =
          SearchIndexDefinitionBuilder.builder()
              .defaultMetadata()
              .mappings(SearchIndex.mockSearchDefinition(new ObjectId()).getMappings())
              .sort(sortSpec)
              .build();

      SearchIndexDefinitionGeneration definitionGeneration =
          SearchIndexDefinitionGenerationBuilder.create(
              definitionWithSort, CURRENT, Collections.emptyList());

      var index = factory.getIndex(definitionGeneration);
      assertThat(index).isNotNull();
      assertThat(index.getDefinition().asSearchDefinition().getSort().get()).isEqualTo(sortSpec);
    }
  }

  @Test
  public void testQueryCacheProviderIsMeteredWhenFeatureFlagEnabled()
      throws InvalidAnalyzerDefinitionException, IOException {
    var cfg = LuceneConfigBuilder.builder().tempDataPath().build();
    FeatureFlags featureFlags =
        FeatureFlags.withDefaults().enable(Feature.INSTRUMENTED_QUERY_CACHE).build();
    VectorIndexDefinitionGeneration definitionGeneration =
        VectorIndexDefinitionGenerationBuilder.builder()
            .definition(VectorIndex.mockVectorDefinition(new ObjectId()))
            .generation(CURRENT)
            .build();
    try (LuceneIndexFactory factory =
        LuceneIndexFactory.fromConfig(
            cfg,
            featureFlags,
            new DynamicFeatureFlagRegistry(
                Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty()),
            EnvironmentVariantPerfConfig.getDefault(),
            MeterAndFtdcRegistry.createWithSimpleRegistries(),
            Optional.empty(),
            AnalyzerRegistry.factory(),
            new NoOpDiskMonitor())) {

      LuceneVectorIndex index1 =
          Check.instanceOf(factory.getIndex(definitionGeneration), LuceneVectorIndex.class);

      Check.instanceOf(
          index1.getVectorIndexProperties().queryCacheProvider,
          QueryCacheProvider.MeteredQueryCacheProvider.class);
    }
  }

  @Test
  public void testQueryCacheProviderIsDefaultWhenFeatureFlagDisabled()
      throws InvalidAnalyzerDefinitionException, IOException {
    var cfg = LuceneConfigBuilder.builder().tempDataPath().build();
    FeatureFlags featureFlags =
        FeatureFlags.withDefaults().disable(Feature.INSTRUMENTED_QUERY_CACHE).build();
    VectorIndexDefinitionGeneration definitionGeneration =
        VectorIndexDefinitionGenerationBuilder.builder()
            .definition(VectorIndex.mockVectorDefinition(new ObjectId()))
            .generation(CURRENT)
            .build();
    try (LuceneIndexFactory factory =
        LuceneIndexFactory.fromConfig(
            cfg,
            featureFlags,
            new DynamicFeatureFlagRegistry(
                Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty()),
            EnvironmentVariantPerfConfig.getDefault(),
            MeterAndFtdcRegistry.createWithSimpleRegistries(),
            Optional.empty(),
            AnalyzerRegistry.factory(),
            new NoOpDiskMonitor())) {

      LuceneVectorIndex index1 =
          Check.instanceOf(factory.getIndex(definitionGeneration), LuceneVectorIndex.class);

      Check.instanceOf(
          index1.getVectorIndexProperties().queryCacheProvider,
          QueryCacheProvider.DefaultQueryCacheProvider.class);
    }
  }

  @Test
  public void isCacheWarmerEnabled_whenUptimeWithinThreshold_returnsTrue() {
    FeatureFlags featureFlags = FeatureFlags.withDefaults().enable(Feature.CACHE_WARMER).build();
    Optional<SystemInfo> systemInfo = Optional.of(mock(SystemInfo.class));
    OperatingSystem operatingSystem = mock(OperatingSystem.class);
    when(systemInfo.get().getOperatingSystem()).thenReturn(operatingSystem);
    when(operatingSystem.getSystemUptime()).thenReturn(30L * 60L);
    assertThat(LuceneIndexFactory.isCacheWarmerEnabled(featureFlags, systemInfo)).isTrue();
  }

  @Test
  public void isCacheWarmerEnabled_whenUptimeExceedsThreshold_returnsFalse() {
    FeatureFlags featureFlags = FeatureFlags.withDefaults().enable(Feature.CACHE_WARMER).build();
    Optional<SystemInfo> systemInfo = Optional.of(mock(SystemInfo.class));
    OperatingSystem operatingSystem = mock(OperatingSystem.class);
    when(systemInfo.get().getOperatingSystem()).thenReturn(operatingSystem);
    when(operatingSystem.getSystemUptime()).thenReturn(30L * 60L + 1L);
    assertThat(LuceneIndexFactory.isCacheWarmerEnabled(featureFlags, systemInfo)).isFalse();
  }

  @Test
  public void isCacheWarmerEnabled_whenFeatureFlagDisabled_returnsFalse() {
    FeatureFlags featureFlags = FeatureFlags.withDefaults().disable(Feature.CACHE_WARMER).build();
    Optional<SystemInfo> systemInfo = Optional.of(mock(SystemInfo.class));
    OperatingSystem operatingSystem = mock(OperatingSystem.class);
    when(systemInfo.get().getOperatingSystem()).thenReturn(operatingSystem);
    when(operatingSystem.getSystemUptime()).thenReturn(30L * 60L);
    assertThat(LuceneIndexFactory.isCacheWarmerEnabled(featureFlags, systemInfo)).isFalse();
  }

  @Test
  public void isCacheWarmerEnabled_whenSystemInfoThrows_returnsFalse() {
    FeatureFlags featureFlags = FeatureFlags.withDefaults().enable(Feature.CACHE_WARMER).build();
    Optional<SystemInfo> systemInfo = Optional.of(mock(SystemInfo.class));
    when(systemInfo.get().getOperatingSystem()).thenThrow(new RuntimeException("native error"));
    assertThat(LuceneIndexFactory.isCacheWarmerEnabled(featureFlags, systemInfo)).isFalse();
  }

  @Test
  public void isCacheWarmerEnabled_onceDisabled_remainsDisabledWithoutRequeryingSystemInfo()
      throws InvalidAnalyzerDefinitionException, IOException {
    FeatureFlags featureFlags = FeatureFlags.withDefaults().enable(Feature.CACHE_WARMER).build();
    Optional<SystemInfo> systemInfo = Optional.of(mock(SystemInfo.class));
    OperatingSystem operatingSystem = mock(OperatingSystem.class);
    when(systemInfo.get().getOperatingSystem()).thenReturn(operatingSystem);
    when(operatingSystem.getSystemUptime()).thenReturn(30L * 60L + 1L);

    try (LuceneIndexFactory factory =
        LuceneIndexFactory.fromConfig(
            LuceneConfigBuilder.builder().tempDataPath().build(),
            featureFlags,
            new DynamicFeatureFlagRegistry(
                Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty()),
            EnvironmentVariantPerfConfig.getDefault(),
            MeterAndFtdcRegistry.createWithSimpleRegistries(),
            Optional.empty(),
            AnalyzerRegistry.factory(),
            new NoOpDiskMonitor(),
            systemInfo,
            false)) {

      assertThat(factory.isCacheWarmerEnabled()).isFalse();
      assertThat(factory.isCacheWarmerEnabled()).isFalse();

      verify(systemInfo.get(), times(1)).getOperatingSystem();
    }
  }

  @Test
  public void isCacheWarmerEnabled_whileEnabled_continuesCheckingSystemInfo()
      throws InvalidAnalyzerDefinitionException, IOException {
    FeatureFlags featureFlags = FeatureFlags.withDefaults().enable(Feature.CACHE_WARMER).build();
    Optional<SystemInfo> systemInfo = Optional.of(mock(SystemInfo.class));
    OperatingSystem operatingSystem = mock(OperatingSystem.class);
    when(systemInfo.get().getOperatingSystem()).thenReturn(operatingSystem);
    when(operatingSystem.getSystemUptime()).thenReturn(0L);

    try (LuceneIndexFactory factory =
        LuceneIndexFactory.fromConfig(
            LuceneConfigBuilder.builder().tempDataPath().build(),
            featureFlags,
            new DynamicFeatureFlagRegistry(
                Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty()),
            EnvironmentVariantPerfConfig.getDefault(),
            MeterAndFtdcRegistry.createWithSimpleRegistries(),
            Optional.empty(),
            AnalyzerRegistry.factory(),
            new NoOpDiskMonitor(),
            systemInfo,
            false)) {

      assertThat(factory.isCacheWarmerEnabled()).isTrue();
      assertThat(factory.isCacheWarmerEnabled()).isTrue();

      verify(systemInfo.get(), times(2)).getOperatingSystem();
    }
  }

  @Test
  public void readPersistedCommitUserData_whenIndexNeverPersisted_returnsEmpty() throws Exception {
    var cfg = LuceneConfigBuilder.builder().tempDataPath().build();
    try (LuceneIndexFactory factory = openFactory(cfg)) {
      SearchIndexDefinitionGeneration gen =
          SearchIndexDefinitionGenerationBuilder.create(
              SearchIndex.mockSearchDefinition(new ObjectId()), CURRENT, Collections.emptyList());
      assertThat(factory.readPersistedCommitUserData(gen)).isEmpty();
    }
  }

  @Test
  public void readPersistedCommitUserData_singlePartition_roundTripsAutoEmbeddingMetadata()
      throws Exception {
    var cfg = LuceneConfigBuilder.builder().tempDataPath().build();
    try (LuceneIndexFactory factory = openFactory(cfg)) {
      SearchIndexDefinitionGeneration gen =
          SearchIndexDefinitionGenerationBuilder.create(
              SearchIndex.mockSearchDefinition(new ObjectId()), CURRENT, Collections.emptyList());
      commitQueryableAutoEmbedding(factory, gen);
      assertThat(readAutoEmbedding(factory, gen)).hasValue(new AutoEmbeddingCommitMetadata(true));
    }
  }

  @Test
  public void readPersistedCommitUserData_multiPartition_readsPartitionZeroCommit()
      throws Exception {
    var cfg = LuceneConfigBuilder.builder().tempDataPath().build();
    try (LuceneIndexFactory factory = openFactory(cfg)) {
      var definition =
          SearchIndexDefinitionBuilder.builder()
              .defaultMetadata()
              .mappings(SearchIndex.mockSearchDefinition(new ObjectId()).getMappings())
              .numPartitions(2)
              .build();
      SearchIndexDefinitionGeneration gen =
          SearchIndexDefinitionGenerationBuilder.create(
              definition, CURRENT, Collections.emptyList());
      commitQueryableAutoEmbedding(factory, gen);
      // The index directory itself has no segments file: partitions live in "0"/"1"
      // subdirectories, so this only succeeds because the read targets partition 0.
      assertThat(readAutoEmbedding(factory, gen)).hasValue(new AutoEmbeddingCommitMetadata(true));
    }
  }

  @Test
  public void readPersistedCommitUserData_whenCommitCorrupt_returnsEmptyWithoutThrowing()
      throws Exception {
    var cfg = LuceneConfigBuilder.builder().tempDataPath().build();
    try (LuceneIndexFactory factory = openFactory(cfg)) {
      SearchIndexDefinitionGeneration gen =
          SearchIndexDefinitionGenerationBuilder.create(
              SearchIndex.mockSearchDefinition(new ObjectId()), CURRENT, Collections.emptyList());
      commitQueryableAutoEmbedding(factory, gen);
      // Overwrite the segments file with garbage so Lucene fails to parse the commit. The read must
      // swallow the failure (IOException or an unchecked exception from a corrupt SegmentInfos) and
      // report absent rather than propagate.
      overwriteSegmentsFilesWithGarbage(cfg.dataPath());
      assertThat(factory.readPersistedCommitUserData(gen)).isEmpty();
    }
  }

  @Test
  public void readPersistedCommitUserData_whenDirectoryHasNoCommit_returnsEmpty() throws Exception {
    var cfg = LuceneConfigBuilder.builder().tempDataPath().build();
    try (LuceneIndexFactory factory = openFactory(cfg)) {
      SearchIndexDefinitionGeneration gen =
          SearchIndexDefinitionGenerationBuilder.create(
              SearchIndex.mockSearchDefinition(new ObjectId()), CURRENT, Collections.emptyList());
      commitQueryableAutoEmbedding(factory, gen);
      // A present but commit-less directory (all index files gone) must also degrade to absent.
      deleteAllRegularFiles(cfg.dataPath());
      assertThat(factory.readPersistedCommitUserData(gen)).isEmpty();
    }
  }

  @Test
  public void readPersistedCommitUserData_whenCommitHasNoUserData_returnsEmpty() throws Exception {
    var cfg = LuceneConfigBuilder.builder().tempDataPath().build();
    try (LuceneIndexFactory factory = openFactory(cfg)) {
      SearchIndexDefinitionGeneration gen =
          SearchIndexDefinitionGenerationBuilder.create(
              SearchIndex.mockSearchDefinition(new ObjectId()), CURRENT, Collections.emptyList());
      // A real commit that carries no user data ("{}", e.g. an older-schema commit) must read back
      // as absent, not as a present-but-empty payload: empty uniformly means "no persisted signal".
      var index = factory.getIndex(gen);
      InitializedIndex initializedIndex = factory.getInitializedIndex(index, gen);
      initializedIndex.getWriter().commit(EncodedUserData.EMPTY);
      initializedIndex.close();
      assertThat(factory.readPersistedCommitUserData(gen)).isEmpty();
    }
  }

  @Test
  public void deleteGenerationData_removesDataCreatedByTheDefinitionFulPath()
      throws InvalidAnalyzerDefinitionException, IOException {
    // Pins the definition-free delete path to the definition-ful create path: whatever
    // getIndex/getInitializedIndex put on disk for a generation must be exactly what
    // deleteGenerationData removes given only that generation's identity. If the two ever derive
    // paths differently, quarantined-index cleanup would silently leave data behind.
    var cfg = LuceneConfigBuilder.builder().tempDataPath().build();
    try (LuceneIndexFactory factory = openFactory(cfg)) {
      SearchIndexDefinitionGeneration gen =
          SearchIndexDefinitionGenerationBuilder.create(
              SearchIndex.mockSearchDefinition(new ObjectId()), CURRENT, Collections.emptyList());
      var index = factory.getIndex(gen);
      InitializedIndex initializedIndex = factory.getInitializedIndex(index, gen);
      initializedIndex.close();
      index.close();

      String name = IndexDirectoryHelper.getNameForGeneration(gen.getGenerationId());
      Path dataPath = cfg.dataPath().resolve(name);
      Path metadataPath = cfg.dataPath().resolve("indexMetadata").resolve(name);
      assertThat(Files.exists(dataPath)).isTrue();
      assertThat(Files.exists(metadataPath)).isTrue();

      // Note: no index.drop() here -- the point is that deletion succeeds without one.
      factory.deleteGenerationData(gen.getGenerationId());

      assertThat(Files.exists(dataPath)).isFalse();
      assertThat(Files.exists(metadataPath)).isFalse();
    }
  }

  @Test
  public void deleteGenerationData_missingData_isNoOp()
      throws InvalidAnalyzerDefinitionException, IOException {
    // A quarantined entry may name a generation whose data was already removed before a crash.
    var cfg = LuceneConfigBuilder.builder().tempDataPath().build();
    try (LuceneIndexFactory factory = openFactory(cfg)) {
      SearchIndexDefinitionGeneration gen =
          SearchIndexDefinitionGenerationBuilder.create(
              SearchIndex.mockSearchDefinition(new ObjectId()), CURRENT, Collections.emptyList());

      factory.deleteGenerationData(gen.getGenerationId());
    }
  }

  private static LuceneIndexFactory openFactory(LuceneConfig cfg)
      throws InvalidAnalyzerDefinitionException, IOException {
    return LuceneIndexFactory.fromConfig(
        cfg,
        FeatureFlags.getDefault(),
        new DynamicFeatureFlagRegistry(
            Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty()),
        EnvironmentVariantPerfConfig.getDefault(),
        MeterAndFtdcRegistry.createWithSimpleRegistries(),
        Optional.empty(),
        AnalyzerRegistry.factory(),
        new NoOpDiskMonitor());
  }

  private static void commitQueryableAutoEmbedding(
      LuceneIndexFactory factory, SearchIndexDefinitionGeneration gen)
      throws InvalidAnalyzerDefinitionException, IOException {
    var index = factory.getIndex(gen);
    InitializedIndex initializedIndex = factory.getInitializedIndex(index, gen);
    IndexCommitUserData userData =
        IndexCommitUserData.fromEncodedData(
                EncodedUserData.fromString("{\"backendIndexVersion\": {\"$numberInt\": \"123\"}}"),
                Optional.of(gen.getGenerationId()))
            .withAutoEmbeddingMetadata(new AutoEmbeddingCommitMetadata(true));
    initializedIndex.getWriter().commit(userData.toEncodedData());
    initializedIndex.close();
  }

  private static Optional<AutoEmbeddingCommitMetadata> readAutoEmbedding(
      LuceneIndexFactory factory, SearchIndexDefinitionGeneration gen) {
    return factory
        .readPersistedCommitUserData(gen)
        .map(
            encoded ->
                IndexCommitUserData.fromEncodedData(encoded, Optional.of(gen.getGenerationId())))
        .flatMap(IndexCommitUserData::autoEmbeddingMetadata);
  }

  private static void overwriteSegmentsFilesWithGarbage(Path root) throws IOException {
    try (var stream = Files.walk(root)) {
      for (Path file :
          stream
              .filter(Files::isRegularFile)
              .filter(file -> file.getFileName().toString().startsWith("segments"))
              .toList()) {
        Files.write(file, new byte[] {0, 1, 2, 3, 4, 5, 6, 7});
      }
    }
  }

  private static void deleteAllRegularFiles(Path root) throws IOException {
    try (var stream = Files.walk(root)) {
      for (Path file : stream.filter(Files::isRegularFile).toList()) {
        Files.delete(file);
      }
    }
  }
}
