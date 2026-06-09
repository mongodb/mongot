package com.xgen.mongot.index.lucene;

import static com.xgen.testing.mongot.mock.index.VectorIndex.MOCK_INDEX_DEFINITION_GENERATION;
import static com.xgen.testing.mongot.mock.index.VectorIndex.MOCK_INDEX_GENERATION_ID;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.google.common.collect.ImmutableList;
import com.xgen.mongot.featureflag.FeatureFlags;
import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlagRegistry;
import com.xgen.mongot.index.DocumentEvent;
import com.xgen.mongot.index.DocumentMetadata;
import com.xgen.mongot.index.EncodedUserData;
import com.xgen.mongot.index.IndexClosedException;
import com.xgen.mongot.index.MeteredVectorIndexReader;
import com.xgen.mongot.index.ReaderClosedException;
import com.xgen.mongot.index.lucene.backing.IndexBackingStrategy;
import com.xgen.mongot.index.lucene.directory.IndexDirectoryFactory;
import com.xgen.mongot.index.lucene.directory.IndexDirectoryHelper;
import com.xgen.mongot.index.lucene.merge.InstrumentedConcurrentMergeScheduler;
import com.xgen.mongot.index.lucene.searcher.QueryCacheProvider;
import com.xgen.mongot.index.lucene.writer.SingleLuceneIndexWriter;
import com.xgen.mongot.index.status.IndexStatus;
import com.xgen.mongot.index.version.IndexFormatVersion;
import com.xgen.mongot.metrics.MeterAndFtdcRegistry;
import com.xgen.mongot.metrics.MetricsFactory;
import com.xgen.mongot.metrics.PerIndexMetricsFactory;
import com.xgen.mongot.replication.mongodb.common.IndexCommitUserData;
import com.xgen.mongot.util.AtomicDirectoryRemover;
import com.xgen.mongot.util.BsonUtils;
import com.xgen.mongot.util.DirectorySize;
import com.xgen.mongot.util.concurrent.NamedExecutorService;
import com.xgen.mongot.util.concurrent.NamedScheduledExecutorService;
import com.xgen.testing.TestUtils;
import com.xgen.testing.mongot.index.IndexMetricsUpdaterBuilder;
import com.xgen.testing.mongot.index.lucene.LuceneConfigBuilder;
import com.xgen.testing.mongot.mock.index.IndexMetricsSupplier;
import com.xgen.testing.mongot.mock.index.SearchIndex;
import com.xgen.testing.mongot.mock.index.VectorIndex;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.TieredMergePolicy;
import org.apache.lucene.search.ReferenceManager;
import org.apache.lucene.store.Directory;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.junit.Assert;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

public class InitializedLuceneVectorIndexTest {
  @Test
  public void testIndexSizeMetric() throws Exception {
    var temporaryFolder = TestUtils.getTempFolder();
    var path = temporaryFolder.getRoot().toPath();
    var directoryFactory = createDirectoryFactory(path);
    var indexPath = directoryFactory.getPath();

    LuceneVectorIndex index =
        LuceneVectorIndex.createDiskBacked(
            indexPath,
            directoryFactory.getMetadataPath(),
            directoryFactory.getConfig(),
            FeatureFlags.getDefault(),
            new InstrumentedConcurrentMergeScheduler(new SimpleMeterRegistry()),
            new TieredMergePolicy(),
            mock(QueryCacheProvider.class),
            mock(NamedScheduledExecutorService.class),
            Optional.empty(),
            Optional.empty(),
            VectorIndex.MOCK_INDEX_DEFINITION_GENERATION.getIndexDefinition(),
            MOCK_INDEX_DEFINITION_GENERATION.generation().indexFormatVersion,
            new AtomicDirectoryRemover(TestUtils.getTempFolder().getRoot().toPath()),
            VectorIndex.mockMetricsFactory());
    InitializedLuceneVectorIndex initializedIndex =
        InitializedLuceneVectorIndex.create(
            index,
            MOCK_INDEX_GENERATION_ID,
            directoryFactory,
            mock(IndexDirectoryHelper.class),
            Optional.empty(),
            FeatureFlags.getDefault(),
            DynamicFeatureFlagRegistry.empty(),
            () -> false);
    Assert.assertEquals(
        DirectorySize.of(indexPath.toFile()),
        initializedIndex.getMetricsUpdater().getMetrics().indexingMetrics().indexSize().toBytes());
  }

  @Test
  public void testInitializeIndexFails() throws Exception {
    var path = TestUtils.getTempFolder().getRoot().toPath();
    var directoryFactory = createDirectoryFactory(path);
    var indexPath = directoryFactory.getPath();
    var metadataPath = directoryFactory.getMetadataPath();
    var indexDirectoryHelper =
        spy(
            IndexDirectoryHelper.create(
                path, new MetricsFactory("test", new SimpleMeterRegistry())));

    LuceneVectorIndex index =
        spy(
            LuceneVectorIndex.createDiskBacked(
                indexPath,
                metadataPath,
                directoryFactory.getConfig(),
                FeatureFlags.getDefault(),
                new InstrumentedConcurrentMergeScheduler(new SimpleMeterRegistry()),
                new TieredMergePolicy(),
                mock(QueryCacheProvider.class),
                mock(NamedScheduledExecutorService.class),
                Optional.empty(),
                Optional.empty(),
                VectorIndex.MOCK_INDEX_DEFINITION_GENERATION.getIndexDefinition(),
                MOCK_INDEX_DEFINITION_GENERATION.generation().indexFormatVersion,
                indexDirectoryHelper.getIndexRemover(),
                new PerIndexMetricsFactory(
                    "test",
                    MeterAndFtdcRegistry.createWithSimpleRegistries(),
                    MOCK_INDEX_DEFINITION_GENERATION.getGenerationId())));
    doThrow(RuntimeException.class).doCallRealMethod().when(index).getVectorIndexProperties();
    assertThrows(
        RuntimeException.class,
        () ->
            InitializedLuceneVectorIndex.create(
                index,
                SearchIndex.MOCK_INDEX_GENERATION_ID,
                directoryFactory,
                indexDirectoryHelper,
                Optional.empty(),
                FeatureFlags.getDefault(),
                DynamicFeatureFlagRegistry.empty(),
                () -> false));
    verify(indexDirectoryHelper, times(1))
        .attemptToRecoverUnreadableIndex(any(), any(), any(), any(), any());
    verify(index, times(1)).getVectorIndexProperties();
    assertFalse(indexPath.toFile().exists());
    assertFalse(metadataPath.toFile().exists());
    // Drop the Index and ensure the directory was deleted.
    index.close();
    index.drop();
    assertFalse(indexPath.toFile().exists());
    assertFalse(metadataPath.toFile().exists());
  }

  @Test
  public void testInitializeIndexFailsWhenUnableToRecover() throws Exception {
    var temporaryFolder = TestUtils.getTempFolder();
    var path = temporaryFolder.getRoot().toPath();
    IndexDirectoryHelper indexDirectoryHelper =
        spy(
            IndexDirectoryHelper.create(
                path, new MetricsFactory("test", new SimpleMeterRegistry())));
    var directoryFactory = createDirectoryFactory(path);
    var indexPath = directoryFactory.getPath();
    var metadataPath = directoryFactory.getMetadataPath();

    doThrow(IOException.class)
        .when(indexDirectoryHelper)
        .attemptToRecoverUnreadableIndex(any(), any(), any(), any(), any());
    LuceneVectorIndex index =
        spy(
            LuceneVectorIndex.createDiskBacked(
                indexPath,
                metadataPath,
                directoryFactory.getConfig(),
                FeatureFlags.getDefault(),
                new InstrumentedConcurrentMergeScheduler(new SimpleMeterRegistry()),
                new TieredMergePolicy(),
                mock(QueryCacheProvider.class),
                mock(NamedScheduledExecutorService.class),
                Optional.empty(),
                Optional.empty(),
                VectorIndex.MOCK_INDEX_DEFINITION_GENERATION.getIndexDefinition(),
                MOCK_INDEX_DEFINITION_GENERATION.generation().indexFormatVersion,
                indexDirectoryHelper.getIndexRemover(),
                new PerIndexMetricsFactory(
                    "test",
                    MeterAndFtdcRegistry.createWithSimpleRegistries(),
                    MOCK_INDEX_DEFINITION_GENERATION.getGenerationId())));
    doThrow(RuntimeException.class).when(index).getVectorIndexProperties();
    assertThrows(
        IOException.class,
        () ->
            InitializedLuceneVectorIndex.create(
                index,
                SearchIndex.MOCK_INDEX_GENERATION_ID,
                directoryFactory,
                indexDirectoryHelper,
                Optional.empty(),
                FeatureFlags.getDefault(),
                DynamicFeatureFlagRegistry.empty(),
                () -> false));

    verify(indexDirectoryHelper, times(1))
        .attemptToRecoverUnreadableIndex(any(), any(), any(), any(), any());
    verify(index, times(1)).getVectorIndexProperties();
    Assert.assertFalse(indexPath.toFile().exists());
    Assert.assertFalse(metadataPath.toFile().exists());
    // Drop the Index
    index.close();
    index.drop();
    assertFalse(indexPath.toFile().exists());
    assertFalse(metadataPath.toFile().exists());
  }

  @Test
  public void testInitializeIndexFailsThenRecovers() throws Exception {
    var temporaryFolder = TestUtils.getTempFolder();
    var path = temporaryFolder.getRoot().toPath();
    var indexDirectoryHelper =
        spy(
            IndexDirectoryHelper.create(
                path, new MetricsFactory("test", new SimpleMeterRegistry())));
    var directoryFactory = createDirectoryFactory(path);
    var indexPath = directoryFactory.getPath();
    var metadataPath = directoryFactory.getMetadataPath();

    doReturn(true)
        .when(indexDirectoryHelper)
        .attemptToRecoverUnreadableIndex(any(), any(), any(), any(), any());

    LuceneVectorIndex index =
        spy(
            LuceneVectorIndex.createDiskBacked(
                indexPath,
                metadataPath,
                directoryFactory.getConfig(),
                FeatureFlags.getDefault(),
                new InstrumentedConcurrentMergeScheduler(new SimpleMeterRegistry()),
                new TieredMergePolicy(),
                mock(QueryCacheProvider.class),
                mock(NamedScheduledExecutorService.class),
                Optional.empty(),
                Optional.empty(),
                VectorIndex.MOCK_INDEX_DEFINITION_GENERATION.getIndexDefinition(),
                MOCK_INDEX_DEFINITION_GENERATION.generation().indexFormatVersion,
                indexDirectoryHelper.getIndexRemover(),
                new PerIndexMetricsFactory(
                    "test",
                    MeterAndFtdcRegistry.createWithSimpleRegistries(),
                    MOCK_INDEX_DEFINITION_GENERATION.getGenerationId())));
    doThrow(RuntimeException.class).doCallRealMethod().when(index).getVectorIndexProperties();
    InitializedLuceneVectorIndex vectorIndex =
        InitializedLuceneVectorIndex.create(
            index,
            SearchIndex.MOCK_INDEX_GENERATION_ID,
            directoryFactory,
            indexDirectoryHelper,
            Optional.empty(),
            FeatureFlags.getDefault(),
            DynamicFeatureFlagRegistry.empty(),
            () -> false);
    assertEquals(SearchIndex.MOCK_INDEX_GENERATION_ID, vectorIndex.getGenerationId());
    verify(indexDirectoryHelper, times(1))
        .attemptToRecoverUnreadableIndex(any(), any(), any(), any(), any());
    verify(index, times(2)).getVectorIndexProperties();
    // Ensure the directory exists after creating the Index.
    Assert.assertTrue(indexPath.toFile().exists());
    Assert.assertTrue(metadataPath.toFile().exists());
    // Drop the Index and ensure the directory was deleted.
    index.close();
    index.drop();
    assertFalse(indexPath.toFile().exists());
    assertFalse(metadataPath.toFile().exists());
  }

  @Test
  public void testInitializeIndexOnClosedIndex() throws Exception {
    var temporaryFolder = TestUtils.getTempFolder();
    var path = temporaryFolder.getRoot().toPath();
    var meterRegistry = new SimpleMeterRegistry();
    var metricsFactory = new MetricsFactory("test", meterRegistry);
    var indexDirectoryHelper = spy(IndexDirectoryHelper.create(path, metricsFactory));
    var directoryFactory = createDirectoryFactory(path);
    var indexPath = directoryFactory.getPath();
    var metadataPath = directoryFactory.getMetadataPath();

    LuceneVectorIndex index =
        spy(
            LuceneVectorIndex.createDiskBacked(
                indexPath,
                metadataPath,
                directoryFactory.getConfig(),
                FeatureFlags.getDefault(),
                new InstrumentedConcurrentMergeScheduler(meterRegistry),
                new TieredMergePolicy(),
                mock(QueryCacheProvider.class),
                mock(NamedScheduledExecutorService.class),
                Optional.empty(),
                Optional.empty(),
                VectorIndex.MOCK_INDEX_DEFINITION_GENERATION.getIndexDefinition(),
                MOCK_INDEX_DEFINITION_GENERATION.generation().indexFormatVersion,
                indexDirectoryHelper.getIndexRemover(),
                new PerIndexMetricsFactory(
                    "test",
                    MeterAndFtdcRegistry.createWithSimpleRegistries(),
                    MOCK_INDEX_DEFINITION_GENERATION.getGenerationId())));
    // Simulate an error that happens only the first time the index is created.
    doThrow(RuntimeException.class).doCallRealMethod().when(index).getVectorIndexProperties();
    index.close();
    index.setStatus(IndexStatus.doesNotExist(IndexStatus.Reason.INDEX_DROPPED));
    InitializedLuceneVectorIndex vectorIndex =
        InitializedLuceneVectorIndex.create(
            index,
            SearchIndex.MOCK_INDEX_GENERATION_ID,
            directoryFactory,
            indexDirectoryHelper,
            Optional.empty(),
            FeatureFlags.getDefault(),
            DynamicFeatureFlagRegistry.empty(),
            () -> false);
    assertEquals(SearchIndex.MOCK_INDEX_GENERATION_ID, vectorIndex.getGenerationId());
    verify(indexDirectoryHelper).attemptToRecoverUnreadableIndex(any(), any(), any(), any(), any());
    verify(index, times(2)).getVectorIndexProperties();
    // Drop the Index and ensure the directory was deleted.
    index.close();
    index.drop();
    assertFalse(indexPath.toFile().exists());
    assertFalse(metadataPath.toFile().exists());
    Assert.assertThrows(IndexClosedException.class, vectorIndex::getWriter);
    Assert.assertEquals(1.0, metricsFactory.get("unreadableDroppedIndexes").counter().count(), 0);
  }

  @Test
  public void testGracefulShutdownOfInternalComponents() throws IOException {
    var directory = mock(Directory.class);
    var writer = mock(SingleLuceneIndexWriter.class);
    var reader = mock(MeteredVectorIndexReader.class);
    var refresher = mock(PeriodicLuceneIndexRefresher.class);
    ImmutableList<ReferenceManager<?>> searcherManagers =
        ImmutableList.of(
            spy(new LuceneVectorIndexTest.NoopSearcherManager()),
            spy(new LuceneVectorIndexTest.NoopSearcherManager()));

    var index =
        new LuceneVectorIndex(
            VectorIndex.MOCK_INDEX_DEFINITION_GENERATION.getIndexDefinition(),
            new AtomicReference<>(IndexStatus.steady()),
            mock(LuceneVectorIndex.VectorIndexProperties.class));
    InitializedLuceneVectorIndex initializedIndex =
        new InitializedLuceneVectorIndex(
            MOCK_INDEX_DEFINITION_GENERATION.getGenerationId(),
            index,
            List.of(directory),
            writer,
            reader,
            refresher,
            searcherManagers,
            IndexMetricsUpdaterBuilder.builder()
                .metricsFactory(SearchIndex.mockMetricsFactory())
                .indexMetricsSupplier(IndexMetricsSupplier.mockEmptyIndexMetricsSupplier())
                .build());
    initializedIndex.close();
    initializedIndex.close(); // ensure that short circuit prevents repeated close()
    // calls for internals

    var shutdownSequence =
        inOrder(
            refresher, reader, writer, directory, searcherManagers.get(0), searcherManagers.get(1));
    shutdownSequence.verify(refresher, times(1)).close();
    shutdownSequence.verify(reader, times(1)).close();
    for (var manager : searcherManagers) {
      shutdownSequence
          .verify((LuceneVectorIndexTest.NoopSearcherManager) manager, times(1))
          .afterClose();
    }
    shutdownSequence.verify(writer).close();
    shutdownSequence.verify(directory, times(1)).close();
  }

  @Test
  public void testIndexSupplierReportedMetrics() throws Exception {
    var metricsFactory = SearchIndex.mockMetricsFactory();
    var index = createIndex(metricsFactory);
    InitializedLuceneVectorIndex initializedIndex = createInitializedIndex(index, false);
    var numDocsGauge =
        metricsFactory
            .get(LuceneSearchIndexMetricValuesSupplier.MetricNames.NUM_LUCENE_DOCS)
            .gauge();
    var numFieldsGauge =
        metricsFactory
            .get(LuceneSearchIndexMetricValuesSupplier.MetricNames.NUM_LUCENE_FIELDS)
            .gauge();
    Assert.assertEquals(0, numDocsGauge.value(), 0.1);
    // Do not read numFieldsGauge here: NUM_LUCENE_FIELDS is cached by CachedGauge, so the first
    // read would cache 0 and the post-update assertion would fail. First read after adding the doc.

    // Add 1 document.
    BsonDocument bsonDoc =
        new BsonDocument()
            .append(
                index.getDefinition().getIndexId().toString(),
                new BsonDocument("_id", new BsonInt32(2)));
    var writer = initializedIndex.getWriter();
    var document = BsonUtils.documentToRaw(bsonDoc);
    writer.updateIndex(
        DocumentEvent.createInsert(
            DocumentMetadata.fromMetadataNamespace(
                Optional.of(document), index.getDefinition().getIndexId()),
            document));

    Assert.assertEquals(1, numDocsGauge.value(), 0.1);
    Assert.assertTrue(numFieldsGauge.value() > 0);
  }

  @Test
  public void testClear() throws IOException, ReaderClosedException {
    var directory = mock(Directory.class);
    var writer = mock(SingleLuceneIndexWriter.class);
    var reader = mock(MeteredVectorIndexReader.class);
    var refresher = mock(PeriodicLuceneIndexRefresher.class);
    ImmutableList<ReferenceManager<?>> searcherManagers =
        ImmutableList.of(
            spy(new LuceneVectorIndexTest.NoopSearcherManager()),
            spy(new LuceneVectorIndexTest.NoopSearcherManager()));

    var index =
        new LuceneVectorIndex(
            VectorIndex.MOCK_INDEX_DEFINITION_GENERATION.getIndexDefinition(),
            new AtomicReference<>(IndexStatus.steady()),
            getIndexProperties());

    InitializedLuceneVectorIndex initializedIndex =
        new InitializedLuceneVectorIndex(
            MOCK_INDEX_DEFINITION_GENERATION.getGenerationId(),
            index,
            List.of(directory),
            writer,
            reader,
            refresher,
            searcherManagers,
            IndexMetricsUpdaterBuilder.builder()
                .metricsFactory(SearchIndex.mockMetricsFactory())
                .indexMetricsSupplier(IndexMetricsSupplier.mockEmptyIndexMetricsSupplier())
                .build());

    var commitData = IndexCommitUserData.createExceeded("test").toEncodedData();
    initializedIndex.clear(commitData);

    var clearSequence = inOrder(refresher, reader, writer);
    clearSequence.verify(reader, times(1)).close();
    clearSequence.verify(writer).deleteAll(commitData);
    clearSequence.verify(reader, times(1)).open();
    clearSequence.verify(reader, times(1)).refresh();

    initializedIndex.clear(commitData); // check calling clear again does not throw
  }

  @Test
  public void testClearClearsMetadata() throws IOException {
    TemporaryFolder temporaryFolder = TestUtils.getTempFolder();
    Path folderPath = temporaryFolder.getRoot().toPath();
    var indexDirectoryHelper = IndexDirectoryHelper.create(folderPath, mock(MetricsFactory.class));
    var directoryFactory = createDirectoryFactory(folderPath);
    var indexPath = directoryFactory.getPath();
    var metadataPath = directoryFactory.getMetadataPath();
    var index =
        LuceneVectorIndex.createDiskBacked(
            indexPath,
            metadataPath,
            directoryFactory.getConfig(),
            FeatureFlags.getDefault(),
            new InstrumentedConcurrentMergeScheduler(new SimpleMeterRegistry()),
            new TieredMergePolicy(),
            mock(QueryCacheProvider.class),
            mock(NamedScheduledExecutorService.class),
            Optional.empty(),
            Optional.empty(),
            VectorIndex.MOCK_INDEX_DEFINITION_GENERATION.getIndexDefinition(),
            MOCK_INDEX_DEFINITION_GENERATION.generation().indexFormatVersion,
            indexDirectoryHelper.getIndexRemover(),
            VectorIndex.mockMetricsFactory());
    InitializedLuceneVectorIndex initializedIndex =
        InitializedLuceneVectorIndex.create(
            index,
            MOCK_INDEX_GENERATION_ID,
            directoryFactory,
            indexDirectoryHelper,
            Optional.empty(),
            FeatureFlags.getDefault(),
            DynamicFeatureFlagRegistry.empty(),
            () -> false);

    // Ensure metadata is cleared but not index directory.
    var commitData = IndexCommitUserData.createExceeded("test").toEncodedData();
    com.xgen.mongot.util.FileUtils.atomicallyReplace(
        directoryFactory.getMetadataPath().resolve("file1"), "contents");

    Assert.assertTrue(metadataPath.toFile().listFiles().length > 0);
    Assert.assertFalse(
        Arrays.stream(metadataPath.toFile().listFiles()).allMatch(File::isDirectory));
    Assert.assertTrue(indexPath.toFile().listFiles().length > 0);
    initializedIndex.clear(commitData);
    Assert.assertTrue(Arrays.stream(metadataPath.toFile().listFiles()).allMatch(File::isDirectory));
    Assert.assertTrue(indexPath.toFile().listFiles().length > 0);

    initializedIndex.clear(commitData); // check calling clear again does not throw
    initializedIndex.close();
    initializedIndex.drop();
  }

  @Test
  public void testClearFailsOnCloseIndex() throws IOException {
    var writer = mock(SingleLuceneIndexWriter.class);
    var reader = mock(MeteredVectorIndexReader.class);
    var refresher = mock(PeriodicLuceneIndexRefresher.class);
    ImmutableList<ReferenceManager<?>> searcherManagers =
        ImmutableList.of(
            spy(new LuceneVectorIndexTest.NoopSearcherManager()),
            spy(new LuceneVectorIndexTest.NoopSearcherManager()));
    var index =
        new LuceneVectorIndex(
            VectorIndex.MOCK_INDEX_DEFINITION_GENERATION.getIndexDefinition(),
            new AtomicReference<>(IndexStatus.steady()),
            getIndexProperties());

    InitializedLuceneVectorIndex initializedIndex =
        new InitializedLuceneVectorIndex(
            VectorIndex.MOCK_INDEX_DEFINITION_GENERATION.getGenerationId(),
            index,
            List.of(mock(Directory.class)),
            writer,
            reader,
            refresher,
            searcherManagers,
            IndexMetricsUpdaterBuilder.builder()
                .metricsFactory(SearchIndex.mockMetricsFactory())
                .indexMetricsSupplier(IndexMetricsSupplier.mockEmptyIndexMetricsSupplier())
                .build());

    initializedIndex.close();
    Assert.assertThrows(
        IllegalStateException.class, () -> initializedIndex.clear(EncodedUserData.EMPTY));
  }

  @Test
  public void testCanAccessStatsUpdaterAfterClose() throws Exception {
    // Create a temporary directory we can make an index in.
    var directoryFactory = createDirectoryFactory();
    var indexPath = directoryFactory.getPath();
    var metadataPath = directoryFactory.getMetadataPath();

    var index =
        LuceneVectorIndex.createDiskBacked(
            indexPath,
            metadataPath,
            directoryFactory.getConfig(),
            FeatureFlags.getDefault(),
            new InstrumentedConcurrentMergeScheduler(new SimpleMeterRegistry()),
            new TieredMergePolicy(),
            mock(QueryCacheProvider.class),
            mock(NamedScheduledExecutorService.class),
            Optional.empty(),
            Optional.empty(),
            VectorIndex.MOCK_INDEX_DEFINITION_GENERATION.getIndexDefinition(),
            MOCK_INDEX_DEFINITION_GENERATION.generation().indexFormatVersion,
            new AtomicDirectoryRemover(TestUtils.getTempFolder().getRoot().toPath()),
            VectorIndex.mockMetricsFactory());
    InitializedLuceneVectorIndex initializedLuceneVectorIndex =
        InitializedLuceneVectorIndex.create(
            index,
            MOCK_INDEX_GENERATION_ID,
            directoryFactory,
            mock(IndexDirectoryHelper.class),
            Optional.empty(),
            FeatureFlags.getDefault(),
            DynamicFeatureFlagRegistry.empty(),
            () -> false);
    // Ensure the directory exists after creating the Index.
    Assert.assertTrue(indexPath.toFile().exists());
    Assert.assertTrue(metadataPath.toFile().exists());

    // Drop the Index and ensure the directory was deleted.
    index.close();
    index.drop();

    // should not throw an exception when called
    Assert.assertNotNull(initializedLuceneVectorIndex.getMetrics());
  }

  private LuceneVectorIndex createIndex(PerIndexMetricsFactory metricsFactory) throws IOException {
    var temporaryFolder = TestUtils.getTempFolder();
    var indexPath = temporaryFolder.getRoot().toPath().resolve("indexData");
    var metadataPath = temporaryFolder.getRoot().toPath().resolve("indexMapping");
    var config = LuceneConfigBuilder.builder().dataPath(indexPath).build();

    return LuceneVectorIndex.createDiskBacked(
        indexPath,
        metadataPath,
        config,
        FeatureFlags.getDefault(),
        new InstrumentedConcurrentMergeScheduler(new SimpleMeterRegistry()),
        new TieredMergePolicy(),
        mock(QueryCacheProvider.class),
        Mockito.mock(NamedScheduledExecutorService.class),
        Optional.of(Mockito.mock(NamedExecutorService.class)),
        Optional.of(Mockito.mock(NamedExecutorService.class)),
        VectorIndex.MOCK_INDEX_DEFINITION_GENERATION.getIndexDefinition(),
        VectorIndex.MOCK_INDEX_DEFINITION_GENERATION.generation().indexFormatVersion,
        new AtomicDirectoryRemover(TestUtils.getTempFolder().getRoot().toPath()),
        metricsFactory);
  }

  private InitializedLuceneVectorIndex createInitializedIndex(
      LuceneVectorIndex index, boolean nrtCacheEnabled) throws IOException {
    var path = TestUtils.getTempFolder().getRoot().toPath();
    var indexPath = path.resolve("indexGeneration");
    var metadataPath = path.resolve("indexMapping/indexGeneration");
    var luceneConfig =
        LuceneConfigBuilder.builder().dataPath(path).nrtCacheEnabled(nrtCacheEnabled).build();
    var directoryFactory =
        new IndexDirectoryFactory(indexPath, metadataPath, luceneConfig, 1, Optional.empty());
    return InitializedLuceneVectorIndex.create(
        index,
        SearchIndex.MOCK_INDEX_GENERATION_ID,
        directoryFactory,
        mock(IndexDirectoryHelper.class),
        Optional.empty(),
        FeatureFlags.getDefault(),
        DynamicFeatureFlagRegistry.empty(),
        () -> false);
  }

  private LuceneVectorIndex.VectorIndexProperties getIndexProperties() {
    return new LuceneVectorIndex.VectorIndexProperties(
        mock(IndexBackingStrategy.class),
        mock(InstrumentedConcurrentMergeScheduler.class),
        mock(MergePolicy.class),
        mock(QueryCacheProvider.class),
        10.0,
        Optional.empty(),
        Optional.empty(),
        IndexFormatVersion.CURRENT,
        mock(PerIndexMetricsFactory.class),
        Optional.empty(),
        Optional.empty());
  }

  private IndexDirectoryFactory createDirectoryFactory(Path path) {
    var luceneConfig = LuceneConfigBuilder.builder().dataPath(path).build();
    var indexPath = path.resolve("indexGeneration");
    var metadataPath = path.resolve("indexMapping/indexGeneration");
    return new IndexDirectoryFactory(indexPath, metadataPath, luceneConfig, 1, Optional.empty());
  }

  private IndexDirectoryFactory createDirectoryFactory() throws IOException {
    var temporaryFolder = TestUtils.getTempFolder();
    var folderPath = temporaryFolder.getRoot().toPath();
    return createDirectoryFactory(folderPath);
  }
}
