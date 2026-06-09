package com.xgen.mongot.index.lucene;

import static com.xgen.testing.mongot.mock.index.SearchIndex.MOCK_INDEX_DEFINITION_GENERATION;
import static com.xgen.testing.mongot.mock.index.SearchIndex.MOCK_INDEX_GENERATION_ID;
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
import com.google.common.util.concurrent.testing.TestingExecutors;
import com.xgen.mongot.featureflag.FeatureFlags;
import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlagRegistry;
import com.xgen.mongot.index.DocumentEvent;
import com.xgen.mongot.index.DocumentMetadata;
import com.xgen.mongot.index.IndexClosedException;
import com.xgen.mongot.index.MeteredSearchIndexReader;
import com.xgen.mongot.index.ReaderClosedException;
import com.xgen.mongot.index.analyzer.AnalyzerRegistry;
import com.xgen.mongot.index.lucene.backing.IndexBackingStrategy;
import com.xgen.mongot.index.lucene.codec.LuceneCodec;
import com.xgen.mongot.index.lucene.directory.IndexDirectoryFactory;
import com.xgen.mongot.index.lucene.directory.IndexDirectoryHelper;
import com.xgen.mongot.index.lucene.merge.InstrumentedConcurrentMergeScheduler;
import com.xgen.mongot.index.lucene.searcher.QueryCacheProvider;
import com.xgen.mongot.index.lucene.synonym.LuceneSynonymRegistry;
import com.xgen.mongot.index.lucene.util.LuceneDocumentIdEncoder;
import com.xgen.mongot.index.lucene.writer.SingleLuceneIndexWriter;
import com.xgen.mongot.index.status.IndexStatus;
import com.xgen.mongot.index.version.IndexFormatVersion;
import com.xgen.mongot.metrics.MetricsFactory;
import com.xgen.mongot.metrics.PerIndexMetricsFactory;
import com.xgen.mongot.replication.mongodb.common.IndexCommitUserData;
import com.xgen.mongot.util.AtomicDirectoryRemover;
import com.xgen.mongot.util.BsonUtils;
import com.xgen.mongot.util.DirectorySize;
import com.xgen.mongot.util.concurrent.Executors;
import com.xgen.mongot.util.concurrent.NamedExecutorService;
import com.xgen.mongot.util.concurrent.NamedScheduledExecutorService;
import com.xgen.testing.TestUtils;
import com.xgen.testing.mongot.index.IndexMetricsUpdaterBuilder;
import com.xgen.testing.mongot.index.analyzer.AnalyzerRegistryBuilder;
import com.xgen.testing.mongot.index.definition.SearchIndexDefinitionBuilder;
import com.xgen.testing.mongot.index.lucene.LuceneConfigBuilder;
import com.xgen.testing.mongot.mock.index.IndexMetricsSupplier;
import com.xgen.testing.mongot.mock.index.SearchIndex;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.TieredMergePolicy;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ReferenceManager;
import org.apache.lucene.store.Directory;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.RawBsonDocument;
import org.junit.Assert;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

public class InitializedLuceneSearchIndexTest {
  @Test
  public void testIndexSizeMetric() throws Exception {
    var directoryFactory = createDirectoryFactory();
    var meterRegistry = new SimpleMeterRegistry();

    LuceneSearchIndex index =
        LuceneSearchIndex.createDiskBacked(
            directoryFactory.getPath(),
            directoryFactory.getMetadataPath(),
            directoryFactory.getConfig(),
            FeatureFlags.getDefault(),
            new InstrumentedConcurrentMergeScheduler(meterRegistry),
            new TieredMergePolicy(),
            new QueryCacheProvider.DefaultQueryCacheProvider(),
            Executors.namedExecutor(
                "refresh", TestingExecutors.noOpScheduledExecutor(), meterRegistry),
            Optional.of(
                Executors.namedExecutor(
                    "refresh", TestingExecutors.noOpScheduledExecutor(), meterRegistry)),
            Optional.of(
                Executors.namedExecutor(
                    "rescoring", TestingExecutors.noOpScheduledExecutor(), meterRegistry)),
            MOCK_INDEX_DEFINITION_GENERATION.getIndexDefinition(),
            MOCK_INDEX_DEFINITION_GENERATION.generation().indexFormatVersion,
            AnalyzerRegistryBuilder.empty(),
            new AtomicDirectoryRemover(TestUtils.getTempFolder().getRoot().toPath()),
            SearchIndex.mockMetricsFactory(),
            Runnable::run);
    var initializedIndex =
        InitializedLuceneSearchIndex.create(
            index,
            MOCK_INDEX_GENERATION_ID,
            directoryFactory,
            mock(IndexDirectoryHelper.class),
            Optional.empty(),
            FeatureFlags.getDefault(),
            new DynamicFeatureFlagRegistry(
                Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty()),
            () -> false);

    Assert.assertEquals(
        DirectorySize.of(directoryFactory.getPath().toFile()),
        initializedIndex.getMetricsUpdater().getMetrics().indexingMetrics().indexSize().toBytes());
    initializedIndex.close();
    initializedIndex.drop();
  }

  @Test
  public void testGracefulShutdownOfInternalComponents() throws IOException {
    var directory = mock(Directory.class);
    var writer = mock(SingleLuceneIndexWriter.class);
    var reader = mock(MeteredSearchIndexReader.class);
    var refresher = mock(PeriodicLuceneIndexRefresher.class);
    ImmutableList<ReferenceManager<?>> searcherManagers =
        ImmutableList.of(spy(new NoopSearcherManager()), spy(new NoopSearcherManager()));
    var index =
        new LuceneSearchIndex(
            SearchIndexDefinitionBuilder.VALID_INDEX,
            new AtomicReference<>(IndexStatus.steady()),
            getIndexProperties());
    var initializedIndex =
        new InitializedLuceneSearchIndex(
            MOCK_INDEX_GENERATION_ID,
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
      shutdownSequence.verify((NoopSearcherManager) manager, times(1)).afterClose();
    }
    shutdownSequence.verify(writer).close();
    shutdownSequence.verify(directory, times(1)).close();
  }

  @Test
  public void testClear() throws IOException, ReaderClosedException {
    var directory = mock(Directory.class);
    var writer = mock(SingleLuceneIndexWriter.class);
    var reader = mock(MeteredSearchIndexReader.class);
    var refresher = mock(PeriodicLuceneIndexRefresher.class);

    ImmutableList<ReferenceManager<?>> searcherManagers =
        ImmutableList.of(spy(new NoopSearcherManager()), spy(new NoopSearcherManager()));
    var index =
        new LuceneSearchIndex(
            SearchIndexDefinitionBuilder.VALID_INDEX,
            new AtomicReference<>(IndexStatus.steady()),
            getIndexProperties());
    var initializedIndex =
        new InitializedLuceneSearchIndex(
            MOCK_INDEX_GENERATION_ID,
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
    initializedIndex.close();
    initializedIndex.drop();
  }

  @Test
  public void testClearClearsMetadata() throws IOException {
    TemporaryFolder temporaryFolder = TestUtils.getTempFolder();
    Path folderPath = temporaryFolder.getRoot().toPath();
    SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
    MetricsFactory metricsFactory = new MetricsFactory("test", meterRegistry);
    var indexDirectoryHelper = IndexDirectoryHelper.create(folderPath, metricsFactory);
    var directoryFactory = createDirectoryFactory(folderPath);
    var indexPath = directoryFactory.getPath();
    var luceneConfig = directoryFactory.getConfig();
    Path metadataPath = directoryFactory.getMetadataPath();
    LuceneSearchIndex index =
        LuceneSearchIndex.createDiskBacked(
            indexPath,
            metadataPath,
            luceneConfig,
            FeatureFlags.getDefault(),
            new InstrumentedConcurrentMergeScheduler(meterRegistry),
            new TieredMergePolicy(),
            new QueryCacheProvider.DefaultQueryCacheProvider(),
            Executors.namedExecutor(
                "refresh", TestingExecutors.noOpScheduledExecutor(), meterRegistry),
            Optional.of(
                Executors.namedExecutor(
                    "refresh", TestingExecutors.noOpScheduledExecutor(), meterRegistry)),
            Optional.of(
                Executors.namedExecutor(
                    "rescoring", TestingExecutors.noOpScheduledExecutor(), meterRegistry)),
            MOCK_INDEX_DEFINITION_GENERATION.getIndexDefinition(),
            MOCK_INDEX_DEFINITION_GENERATION.generation().indexFormatVersion,
            AnalyzerRegistryBuilder.empty(),
            indexDirectoryHelper.getIndexRemover(),
            SearchIndex.mockMetricsFactory(),
            Runnable::run);
    var initializedIndex =
        InitializedLuceneSearchIndex.create(
            index,
            MOCK_INDEX_GENERATION_ID,
            directoryFactory,
            indexDirectoryHelper,
            Optional.empty(),
            FeatureFlags.getDefault(),
            new DynamicFeatureFlagRegistry(
                Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty()),
            () -> false);

    // Ensure metadata is cleared but not index directory.
    var commitData = IndexCommitUserData.createExceeded("test").toEncodedData();
    com.xgen.mongot.util.FileUtils.atomicallyReplace(metadataPath.resolve("file1"), "contents");

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
  public void testInitializeIndexFails() throws Exception {
    var path = TestUtils.getTempFolder().getRoot().toPath();
    var directoryFactory = createDirectoryFactory(path);
    var indexPath = directoryFactory.getPath();
    var metadataPath = directoryFactory.getMetadataPath();
    IndexDirectoryHelper indexDirectoryHelper =
        spy(
            IndexDirectoryHelper.create(
                path, new MetricsFactory("test", new SimpleMeterRegistry())));

    SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
    AtomicDirectoryRemover directoryRemover = indexDirectoryHelper.getIndexRemover();
    LuceneSearchIndex index =
        spy(
            LuceneSearchIndex.createDiskBacked(
                indexPath,
                metadataPath,
                LuceneConfigBuilder.builder().dataPath(path).build(),
                FeatureFlags.getDefault(),
                new InstrumentedConcurrentMergeScheduler(meterRegistry),
                new TieredMergePolicy(),
                new QueryCacheProvider.DefaultQueryCacheProvider(),
                Executors.namedExecutor(
                    "refresh", TestingExecutors.noOpScheduledExecutor(), meterRegistry),
                Optional.of(
                    Executors.namedExecutor(
                        "refresh", TestingExecutors.noOpScheduledExecutor(), meterRegistry)),
                Optional.of(
                    Executors.namedExecutor(
                        "rescoring", TestingExecutors.noOpScheduledExecutor(), meterRegistry)),
                MOCK_INDEX_DEFINITION_GENERATION.getIndexDefinition(),
                MOCK_INDEX_DEFINITION_GENERATION.generation().indexFormatVersion,
                AnalyzerRegistryBuilder.empty(),
                directoryRemover,
                SearchIndex.mockMetricsFactory(),
                Runnable::run));
    doThrow(RuntimeException.class).when(index).getSearchIndexProperties();
    assertThrows(
        RuntimeException.class,
        () ->
            InitializedLuceneSearchIndex.create(
                index,
                MOCK_INDEX_GENERATION_ID,
                directoryFactory,
                indexDirectoryHelper,
                Optional.empty(),
                FeatureFlags.getDefault(),
                new DynamicFeatureFlagRegistry(
                    Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty()),
                () -> false));
    verify(indexDirectoryHelper, times(1))
        .attemptToRecoverUnreadableIndex(any(), any(), any(), any(), any());
    verify(index, times(1)).getSearchIndexProperties();
    assertFalse(indexPath.toFile().exists());
    assertFalse(metadataPath.toFile().exists());
    // Drop the Index and ensure the directory was deleted.
    index.close();
    index.drop();
    assertFalse(indexPath.toFile().exists());
    assertFalse(metadataPath.toFile().exists());
  }

  @Test
  public void testInitializeIndexFailsThenRecovers() throws Exception {
    TemporaryFolder temporaryFolder = TestUtils.getTempFolder();
    Path folderPath = temporaryFolder.getRoot().toPath();
    SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
    IndexDirectoryHelper indexDirectoryHelper =
        spy(
            IndexDirectoryHelper.create(
                folderPath, new MetricsFactory("test", new SimpleMeterRegistry())));

    doReturn(true)
        .when(indexDirectoryHelper)
        .attemptToRecoverUnreadableIndex(any(), any(), any(), any(), any());
    var directoryFactory = createDirectoryFactory(folderPath);
    var indexPath = directoryFactory.getPath();
    LuceneSearchIndex index =
        spy(
            LuceneSearchIndex.createDiskBacked(
                indexPath,
                directoryFactory.getMetadataPath(),
                directoryFactory.getConfig(),
                FeatureFlags.getDefault(),
                new InstrumentedConcurrentMergeScheduler(meterRegistry),
                new TieredMergePolicy(),
                new QueryCacheProvider.DefaultQueryCacheProvider(),
                Executors.namedExecutor(
                    "refresh", TestingExecutors.noOpScheduledExecutor(), meterRegistry),
                Optional.of(
                    Executors.namedExecutor(
                        "refresh", TestingExecutors.noOpScheduledExecutor(), meterRegistry)),
                Optional.of(
                    Executors.namedExecutor(
                        "rescoring", TestingExecutors.noOpScheduledExecutor(), meterRegistry)),
                MOCK_INDEX_DEFINITION_GENERATION.getIndexDefinition(),
                MOCK_INDEX_DEFINITION_GENERATION.generation().indexFormatVersion,
                AnalyzerRegistryBuilder.empty(),
                new AtomicDirectoryRemover(TestUtils.getTempFolder().getRoot().toPath()),
                SearchIndex.mockMetricsFactory(),
                Runnable::run));
    doThrow(RuntimeException.class).doCallRealMethod().when(index).getSearchIndexProperties();

    InitializedLuceneSearchIndex initializedLuceneSearchIndex =
        InitializedLuceneSearchIndex.create(
            index,
            MOCK_INDEX_GENERATION_ID,
            directoryFactory,
            indexDirectoryHelper,
            Optional.empty(),
            FeatureFlags.getDefault(),
            new DynamicFeatureFlagRegistry(
                Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty()),
            () -> false);
    assertEquals(MOCK_INDEX_GENERATION_ID, initializedLuceneSearchIndex.getGenerationId());
    // Ensure the directory exists after creating the Index.
    Assert.assertTrue(indexPath.toFile().exists());
    verify(indexDirectoryHelper, times(1))
        .attemptToRecoverUnreadableIndex(any(), any(), any(), any(), any());
    verify(index, times(2)).getSearchIndexProperties();

    // Drop the Index and ensure the directory was deleted.
    index.close();
    index.drop();
    assertFalse(indexPath.toFile().exists());
  }

  @Test
  public void testInitializeIndexOnClosedIndex() throws Exception {
    TemporaryFolder temporaryFolder = TestUtils.getTempFolder();
    Path folderPath = temporaryFolder.getRoot().toPath();
    SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
    MetricsFactory metricsFactory = new MetricsFactory("test", meterRegistry);

    var indexDirectoryHelper = spy(IndexDirectoryHelper.create(folderPath, metricsFactory));
    var directoryFactory = createDirectoryFactory(folderPath);
    var indexPath = directoryFactory.getPath();
    var luceneConfig = directoryFactory.getConfig();
    LuceneSearchIndex index =
        spy(
            LuceneSearchIndex.createDiskBacked(
                indexPath,
                directoryFactory.getMetadataPath(),
                luceneConfig,
                FeatureFlags.getDefault(),
                new InstrumentedConcurrentMergeScheduler(meterRegistry),
                new TieredMergePolicy(),
                new QueryCacheProvider.DefaultQueryCacheProvider(),
                Executors.namedExecutor(
                    "refresh", TestingExecutors.noOpScheduledExecutor(), meterRegistry),
                Optional.of(
                    Executors.namedExecutor(
                        "refresh", TestingExecutors.noOpScheduledExecutor(), meterRegistry)),
                Optional.of(
                    Executors.namedExecutor(
                        "rescoring", TestingExecutors.noOpScheduledExecutor(), meterRegistry)),
                MOCK_INDEX_DEFINITION_GENERATION.getIndexDefinition(),
                MOCK_INDEX_DEFINITION_GENERATION.generation().indexFormatVersion,
                AnalyzerRegistryBuilder.empty(),
                new AtomicDirectoryRemover(TestUtils.getTempFolder().getRoot().toPath()),
                SearchIndex.mockMetricsFactory(),
                Runnable::run));
    // Simulate an error that happens only the first time the index is created.
    doThrow(RuntimeException.class).doCallRealMethod().when(index).getSearchIndexProperties();
    index.close();
    index.setStatus(IndexStatus.doesNotExist(IndexStatus.Reason.INDEX_DROPPED));
    InitializedLuceneSearchIndex initializedLuceneSearchIndex =
        InitializedLuceneSearchIndex.create(
            index,
            MOCK_INDEX_GENERATION_ID,
            directoryFactory,
            indexDirectoryHelper,
            Optional.empty(),
            FeatureFlags.getDefault(),
            new DynamicFeatureFlagRegistry(
                Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty()),
            () -> false);
    assertEquals(MOCK_INDEX_GENERATION_ID, initializedLuceneSearchIndex.getGenerationId());
    // Ensure the directory exists after creating the Index.
    Assert.assertTrue(indexPath.toFile().exists());
    verify(indexDirectoryHelper).attemptToRecoverUnreadableIndex(any(), any(), any(), any(), any());
    verify(index, times(2)).getSearchIndexProperties();

    // Drop the Index and ensure the directory was deleted.
    index.close();
    index.drop();
    assertFalse(indexPath.toFile().exists());
    Assert.assertThrows(IndexClosedException.class, initializedLuceneSearchIndex::getWriter);
    Assert.assertEquals(1.0, metricsFactory.get("unreadableDroppedIndexes").counter().count(), 0);
  }

  @Test
  public void testInitializeIndexFailsWhenUnableToRecover() throws Exception {
    TemporaryFolder temporaryFolder = TestUtils.getTempFolder();
    Path path = temporaryFolder.getRoot().toPath();
    SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
    IndexDirectoryHelper indexDirectoryHelper =
        spy(
            IndexDirectoryHelper.create(
                path, new MetricsFactory("test", new SimpleMeterRegistry())));

    doThrow(IOException.class)
        .when(indexDirectoryHelper)
        .attemptToRecoverUnreadableIndex(any(), any(), any(), any(), any());

    var directoryFactory = createDirectoryFactory(path);
    var indexPath = directoryFactory.getPath();
    var luceneConfig = directoryFactory.getConfig();
    var index =
        spy(
            LuceneSearchIndex.createDiskBacked(
                indexPath,
                directoryFactory.getMetadataPath(),
                luceneConfig,
                FeatureFlags.getDefault(),
                new InstrumentedConcurrentMergeScheduler(meterRegistry),
                new TieredMergePolicy(),
                new QueryCacheProvider.DefaultQueryCacheProvider(),
                Executors.namedExecutor(
                    "refresh", TestingExecutors.noOpScheduledExecutor(), meterRegistry),
                Optional.of(
                    Executors.namedExecutor(
                        "refresh", TestingExecutors.noOpScheduledExecutor(), meterRegistry)),
                Optional.of(
                    Executors.namedExecutor(
                        "rescoring", TestingExecutors.noOpScheduledExecutor(), meterRegistry)),
                MOCK_INDEX_DEFINITION_GENERATION.getIndexDefinition(),
                MOCK_INDEX_DEFINITION_GENERATION.generation().indexFormatVersion,
                AnalyzerRegistryBuilder.empty(),
                indexDirectoryHelper.getIndexRemover(),
                SearchIndex.mockMetricsFactory(),
                Runnable::run));
    doThrow(RuntimeException.class).when(index).getSearchIndexProperties();
    assertThrows(
        IOException.class,
        () ->
            InitializedLuceneSearchIndex.create(
                index,
                MOCK_INDEX_GENERATION_ID,
                directoryFactory,
                indexDirectoryHelper,
                Optional.empty(),
                FeatureFlags.getDefault(),
                new DynamicFeatureFlagRegistry(
                    Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty()),
                () -> false));
    verify(indexDirectoryHelper, times(1))
        .attemptToRecoverUnreadableIndex(any(), any(), any(), any(), any());
    verify(index, times(1)).getSearchIndexProperties();
    Assert.assertFalse(indexPath.toFile().exists());
    // Drop the Index
    index.close();
    index.drop();
    assertFalse(indexPath.toFile().exists());
  }

  @Test
  public void testCanAccessStatsUpdaterAfterClose() throws Exception {
    // Create a temporary directory we can make an index in.
    TemporaryFolder temporaryFolder = TestUtils.getTempFolder();
    Path folderPath = temporaryFolder.getRoot().toPath();
    SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
    IndexDirectoryHelper indexDirectoryHelper =
        spy(IndexDirectoryHelper.create(folderPath, new MetricsFactory("test", meterRegistry)));

    var directoryFactory = createDirectoryFactory(folderPath);
    var luceneConfig = directoryFactory.getConfig();
    LuceneSearchIndex index =
        LuceneSearchIndex.createDiskBacked(
            folderPath.resolve("testCanAccessStatsUpdaterAfterClose"),
            folderPath.resolve("indexMapping/testCanAccessStatsUpdaterAfterClose"),
            luceneConfig,
            FeatureFlags.getDefault(),
            new InstrumentedConcurrentMergeScheduler(meterRegistry),
            new TieredMergePolicy(),
            new QueryCacheProvider.DefaultQueryCacheProvider(),
            Mockito.mock(NamedScheduledExecutorService.class),
            Optional.of(Mockito.mock(NamedExecutorService.class)),
            Optional.of(Mockito.mock(NamedExecutorService.class)),
            MOCK_INDEX_DEFINITION_GENERATION.getIndexDefinition(),
            MOCK_INDEX_DEFINITION_GENERATION.generation().indexFormatVersion,
            AnalyzerRegistryBuilder.empty(),
            indexDirectoryHelper.getIndexRemover(),
            SearchIndex.mockMetricsFactory(),
            Runnable::run);

    var initializedIndex =
        InitializedLuceneSearchIndex.create(
            index,
            MOCK_INDEX_GENERATION_ID,
            directoryFactory,
            indexDirectoryHelper,
            Optional.empty(),
            FeatureFlags.getDefault(),
            new DynamicFeatureFlagRegistry(
                Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty()),
            () -> false);

    // Ensure the directory exists after creating the Index.
    Assert.assertTrue(folderPath.toFile().exists());

    // Drop the Index and ensure the directory was deleted.
    index.close();
    index.drop();

    // should not throw an exception when called
    Assert.assertNotNull(initializedIndex.getMetrics());
  }

  @Test
  public void testInitializeIndexFailsDueToIncompatibleCodecThenRecovers() throws Exception {
    TemporaryFolder temporaryFolder = TestUtils.getTempFolder();
    Path folderPath = temporaryFolder.getRoot().toPath();
    SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
    MetricsFactory metricsFactory = new MetricsFactory("test", meterRegistry);
    IndexDirectoryHelper indexDirectoryHelper =
        spy(
            new IndexDirectoryHelper(
                folderPath,
                metricsFactory,
                new AtomicDirectoryRemover(TestUtils.getTempFolder().getRoot().toPath())));
    var directoryFactory = createDirectoryFactory(folderPath);
    var directory = directoryFactory.create(0);
    var indexPath = directoryFactory.getPath();
    var metadataPath = directoryFactory.getMetadataPath();

    // Create an IndexWriter with an unregistered Codec.
    IndexWriter writer =
        new IndexWriter(directory, new IndexWriterConfig().setCodec(new LuceneCodec("Lucene9999")));

    // Add a document using the IndexWriter.
    BsonDocument bson = new BsonDocument().append("_id", new BsonInt32(0));
    Document doc = new Document();
    doc.add(
        LuceneDocumentIdEncoder.documentIdField(LuceneDocumentIdEncoder.encodeDocumentId(bson)));
    doc.add(new StringField("name", "test", Field.Store.NO));
    writer.addDocument(doc);
    writer.commit();
    writer.close();

    Assert.assertEquals(
        0.0,
        metricsFactory
            .get(
                "unreadableIndexRecoveries",
                Tags.of(Tag.of("unreadableIndexCause", "incompatibleCodec")))
            .counter()
            .count(),
        0);

    LuceneSearchIndex index =
        spy(
            LuceneSearchIndex.createDiskBacked(
                indexPath,
                metadataPath,
                directoryFactory.getConfig(),
                FeatureFlags.getDefault(),
                new InstrumentedConcurrentMergeScheduler(meterRegistry),
                new TieredMergePolicy(),
                new QueryCacheProvider.DefaultQueryCacheProvider(),
                Executors.namedExecutor(
                    "refresh", TestingExecutors.noOpScheduledExecutor(), meterRegistry),
                Optional.of(
                    Executors.namedExecutor(
                        "refresh", TestingExecutors.noOpScheduledExecutor(), meterRegistry)),
                Optional.of(
                    Executors.namedExecutor(
                        "rescoring", TestingExecutors.noOpScheduledExecutor(), meterRegistry)),
                MOCK_INDEX_DEFINITION_GENERATION.getIndexDefinition(),
                MOCK_INDEX_DEFINITION_GENERATION.generation().indexFormatVersion,
                AnalyzerRegistryBuilder.empty(),
                new AtomicDirectoryRemover(TestUtils.getTempFolder().getRoot().toPath()),
                SearchIndex.mockMetricsFactory(),
                Runnable::run));
    InitializedLuceneSearchIndex.create(
        index,
        MOCK_INDEX_GENERATION_ID,
        directoryFactory,
        indexDirectoryHelper,
        Optional.empty(),
        FeatureFlags.getDefault(),
        new DynamicFeatureFlagRegistry(
            Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty()),
        () -> false);
    verify(indexDirectoryHelper, Mockito.times(1))
        .attemptToRecoverUnreadableIndex(any(), any(), any(), any(), any());
    Assert.assertEquals(
        1.0,
        metricsFactory
            .get(
                "unreadableIndexRecoveries",
                Tags.of(Tag.of("unreadableIndexCause", "incompatibleCodec")))
            .counter()
            .count(),
        0);
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
  public void testIndexSupplierReportedMetrics() throws Exception {
    var metricsFactory = SearchIndex.mockMetricsFactory();
    LuceneSearchIndex index = createIndex(metricsFactory);
    InitializedLuceneSearchIndex initializedIndex = createInitializedIndex(index, false);

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
        new BsonDocument(
                index.getDefinition().getIndexId().toString(),
                new BsonDocument("_id", new BsonInt32(2)))
            .append("_id", new BsonInt32(2));
    var writer = initializedIndex.getWriter();
    RawBsonDocument document = BsonUtils.documentToRaw(bsonDoc);
    writer.updateIndex(
        DocumentEvent.createInsert(
            DocumentMetadata.fromMetadataNamespace(
                Optional.of(document), index.getDefinition().getIndexId()),
            document));

    Assert.assertEquals(1, numDocsGauge.value(), 0.1);
    Assert.assertTrue(numFieldsGauge.value() > 0);
  }

  private LuceneSearchIndex createIndex(PerIndexMetricsFactory metricsFactory) throws IOException {
    var temporaryFolder = TestUtils.getTempFolder();
    var indexPath = temporaryFolder.getRoot().toPath().resolve("indexData");
    var metadataPath = temporaryFolder.getRoot().toPath().resolve("indexMapping");
    var config = LuceneConfigBuilder.builder().dataPath(indexPath).build();
    SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
    return LuceneSearchIndex.createDiskBacked(
        indexPath,
        metadataPath,
        config,
        FeatureFlags.getDefault(),
        new InstrumentedConcurrentMergeScheduler(meterRegistry),
        new TieredMergePolicy(),
        new QueryCacheProvider.DefaultQueryCacheProvider(),
        Mockito.mock(NamedScheduledExecutorService.class),
        Optional.of(Mockito.mock(NamedExecutorService.class)),
        Optional.of(Mockito.mock(NamedExecutorService.class)),
        MOCK_INDEX_DEFINITION_GENERATION.getIndexDefinition(),
        MOCK_INDEX_DEFINITION_GENERATION.generation().indexFormatVersion,
        AnalyzerRegistryBuilder.empty(),
        new AtomicDirectoryRemover(TestUtils.getTempFolder().getRoot().toPath()),
        metricsFactory,
        Runnable::run);
  }

  private InitializedLuceneSearchIndex createInitializedIndex(
      LuceneSearchIndex index, boolean nrtCacheEnabled) throws IOException {
    var path = TestUtils.getTempFolder().getRoot().toPath();
    var indexPath = path.resolve("indexGeneration");
    var metadataPath = path.resolve("indexMapping/indexGeneration");
    var luceneConfig =
        LuceneConfigBuilder.builder().dataPath(path).nrtCacheEnabled(nrtCacheEnabled).build();
    var directoryFactory =
        new IndexDirectoryFactory(indexPath, metadataPath, luceneConfig, 1, Optional.empty());
    return InitializedLuceneSearchIndex.create(
        index,
        MOCK_INDEX_GENERATION_ID,
        directoryFactory,
        mock(IndexDirectoryHelper.class),
        Optional.empty(),
        FeatureFlags.getDefault(),
        new DynamicFeatureFlagRegistry(
            Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty()),
        () -> false);
  }

  private LuceneSearchIndex.SearchIndexProperties getIndexProperties() {
    return new LuceneSearchIndex.SearchIndexProperties(
        mock(LuceneSynonymRegistry.class),
        mock(AnalyzerRegistry.class),
        mock(IndexBackingStrategy.class),
        mock(InstrumentedConcurrentMergeScheduler.class),
        mock(MergePolicy.class),
        mock(QueryCacheProvider.class),
        10.0,
        Optional.empty(),
        Optional.empty(),
        false,
        false,
        Optional.empty(),
        IndexFormatVersion.CURRENT,
        mock(PerIndexMetricsFactory.class),
        Optional.empty(),
        Optional.empty(),
        Runnable::run);
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

  public static class NoopSearcherManager extends ReferenceManager<IndexSearcher> {

    public NoopSearcherManager() {
      this.current = mock(IndexSearcher.class);
    }

    @Override
    protected void decRef(IndexSearcher reference) {}

    @Override
    protected IndexSearcher refreshIfNeeded(IndexSearcher referenceToRefresh) {
      return null;
    }

    @Override
    protected boolean tryIncRef(IndexSearcher reference) {
      return false;
    }

    @Override
    protected int getRefCount(IndexSearcher reference) {
      return 0;
    }

    @Override
    public void afterClose() throws IOException {
      super.afterClose();
    }
  }
}
