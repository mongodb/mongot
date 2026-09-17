package com.xgen.mongot.index.lucene;

import static com.xgen.testing.mongot.mock.index.VectorIndex.MOCK_INDEX_DEFINITION_GENERATION;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

import com.xgen.mongot.featureflag.FeatureFlags;
import com.xgen.mongot.index.IndexUnavailableException;
import com.xgen.mongot.index.InitialSyncIndexUnavailableException;
import com.xgen.mongot.index.lucene.config.LuceneConfig;
import com.xgen.mongot.index.lucene.merge.InstrumentedConcurrentMergeScheduler;
import com.xgen.mongot.index.lucene.searcher.QueryCacheProvider;
import com.xgen.mongot.index.status.IndexStatus;
import com.xgen.mongot.index.status.StaleStatusReason;
import com.xgen.mongot.metrics.PerIndexMetricsFactory;
import com.xgen.mongot.util.AtomicDirectoryRemover;
import com.xgen.mongot.util.concurrent.NamedExecutorService;
import com.xgen.mongot.util.concurrent.NamedScheduledExecutorService;
import com.xgen.testing.ConcurrencyTestUtils;
import com.xgen.testing.TestUtils;
import com.xgen.testing.mongot.index.lucene.LuceneConfigBuilder;
import com.xgen.testing.mongot.mock.index.SearchIndex;
import com.xgen.testing.mongot.mock.index.VectorIndex;
import io.micrometer.core.instrument.search.MeterNotFoundException;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import org.apache.lucene.index.TieredMergePolicy;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ReferenceManager;
import org.bson.BsonTimestamp;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

public class LuceneVectorIndexTest {

  @Test
  public void testDrop() throws Exception {
    // Create a temporary directory we can make an index in.
    var temporaryFolder = TestUtils.getTempFolder();
    var indexPath = temporaryFolder.getRoot().toPath();
    var metadataPath = temporaryFolder.getRoot().toPath().resolve("indexMapping");

    SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
    LuceneVectorIndex index =
        LuceneVectorIndex.createDiskBacked(
            indexPath,
            metadataPath,
            LuceneConfigBuilder.builder().dataPath(indexPath).build(),
            FeatureFlags.getDefault(),
            new InstrumentedConcurrentMergeScheduler(meterRegistry),
            new TieredMergePolicy(),
            new QueryCacheProvider.DefaultQueryCacheProvider(),
            mock(NamedScheduledExecutorService.class),
            Optional.empty(),
            Optional.empty(),
            VectorIndex.MOCK_INDEX_DEFINITION_GENERATION.getIndexDefinition(),
            MOCK_INDEX_DEFINITION_GENERATION.generation().formatVersion.asLucene(),
            new AtomicDirectoryRemover(TestUtils.getTempFolder().getRoot().toPath()),
            VectorIndex.mockMetricsFactory());

    // Ensure the directory exists after creating the Index.
    Assert.assertTrue(indexPath.toFile().exists());

    // Drop the Index and ensure the directory was deleted.
    index.close();
    index.drop();
    Assert.assertFalse(indexPath.toFile().exists());

    index.drop(); // shouldn't throw if already dropped
  }

  @Test
  public void testIndexBackingStrategy() throws IOException {
    var temporaryFolder = TestUtils.getTempFolder();
    var path = temporaryFolder.getRoot().toPath();

    var backingStrategy =
        IndexBackingStrategyFactory.diskBacked(
            Mockito.mock(NamedScheduledExecutorService.class),
            LuceneConfigBuilder.builder().dataPath(path).build().refreshInterval(),
            new AtomicDirectoryRemover(TestUtils.getTempFolder().getRoot().toPath()),
            path,
            path.resolve("indexMapping"),
            VectorIndex.mockMetricsFactory(),
            1);

    backingStrategy.releaseResources();
    Assert.assertEquals(
        "should handle calling getIndexSize() after releaseResources() gracefully",
        0,
        backingStrategy.getDiskStats().totalFileByteSize());
  }

  @Test
  public void testIndexBackingStrategyConcurrently() throws Exception {
    var temporaryFolder = TestUtils.getTempFolder();
    var path = temporaryFolder.getRoot().toPath();

    CountDownLatch firstReadyToStart = new CountDownLatch(1);
    CountDownLatch firstShouldStart = new CountDownLatch(1);

    var directoryRemover =
        spy(new AtomicDirectoryRemover(TestUtils.getTempFolder().getRoot().toPath()));
    Answer<?> answer =
        invocation -> {
          firstReadyToStart.countDown();
          firstShouldStart.await();
          return invocation.callRealMethod();
        };
    doAnswer(answer).when(directoryRemover).deleteDirectory(any());

    var backingStrategy =
        IndexBackingStrategyFactory.diskBacked(
            Mockito.mock(NamedScheduledExecutorService.class),
            LuceneConfigBuilder.builder().dataPath(path).build().refreshInterval(),
            directoryRemover,
            path,
            path.resolve("indexMapping"),
            VectorIndex.mockMetricsFactory(),
            1);

    ConcurrencyTestUtils.assertCannotBeInvokedConcurrently(
        backingStrategy::releaseResources,
        firstReadyToStart,
        firstShouldStart,
        backingStrategy::getDiskStats);
  }

  @Test
  public void testGetDefinitions() throws Exception {
    // Create a temporary directory we can make an index in.
    var temporaryFolder = TestUtils.getTempFolder();
    var indexPath = temporaryFolder.getRoot().toPath().resolve("indexData");
    var metadataPath = temporaryFolder.getRoot().toPath().resolve("indexMapping");

    SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
    var index =
        LuceneVectorIndex.createDiskBacked(
            indexPath,
            metadataPath,
            LuceneConfigBuilder.builder().dataPath(indexPath).build(),
            FeatureFlags.getDefault(),
            new InstrumentedConcurrentMergeScheduler(meterRegistry),
            new TieredMergePolicy(),
            new QueryCacheProvider.DefaultQueryCacheProvider(),
            mock(NamedScheduledExecutorService.class),
            Optional.empty(),
            Optional.empty(),
            VectorIndex.MOCK_INDEX_DEFINITION_GENERATION.getIndexDefinition(),
            MOCK_INDEX_DEFINITION_GENERATION.generation().formatVersion.asLucene(),
            new AtomicDirectoryRemover(TestUtils.getTempFolder().getRoot().toPath()),
            VectorIndex.mockMetricsFactory());

    // it is not necessary for these definitions to be the same object, but better test this way
    // since we do not define equality for these objects.
    Assert.assertSame(
        VectorIndex.MOCK_INDEX_DEFINITION_GENERATION.getIndexDefinition(), index.getDefinition());
    index.close();
    index.drop();
  }

  @Test
  public void testIndexSupplierAndWriterNotInitialized() throws Exception {
    var metricsFactory = SearchIndex.mockMetricsFactory();
    createIndex(metricsFactory);
    Assert.assertThrows(
        MeterNotFoundException.class,
        () ->
            metricsFactory
                .get(LuceneSearchIndexMetricValuesSupplier.MetricNames.NUM_LUCENE_DOCS)
                .gauge());
  }

  @Test
  public void testThrowsWhenIndexIsNotAvailableForQuerying() {
    Assert.assertThrows(
        IndexUnavailableException.class,
        () -> createIndexWithStatus(IndexStatus.notStarted()).throwIfUnavailableForQuerying());
    Assert.assertThrows(
        InitialSyncIndexUnavailableException.class,
        () -> createIndexWithStatus(IndexStatus.initialSync()).throwIfUnavailableForQuerying());
    Assert.assertThrows(
        IndexUnavailableException.class,
        () -> createIndexWithStatus(IndexStatus.failed("test")).throwIfUnavailableForQuerying());
  }

  @Test // should not throw an exception
  public void testThrowsWhenIndexIsAvailableForQuerying()
      throws IOException, IndexUnavailableException {
    List<IndexStatus> statuses =
        Arrays.asList(
            IndexStatus.steady(),
            IndexStatus.stale(StaleStatusReason.DOCS_EXCEEDED.formatMessage(), new BsonTimestamp()),
            IndexStatus.recoveringNonTransient(new BsonTimestamp()),
            IndexStatus.doesNotExist(IndexStatus.Reason.COLLECTION_NOT_FOUND));
    for (IndexStatus indexStatus : statuses) {
      LuceneVectorIndex indexWithStatus = createIndexWithStatus(indexStatus);
      indexWithStatus.throwIfUnavailableForQuerying();
      indexWithStatus.close();
    }
  }

  private LuceneVectorIndex createIndexWithStatus(IndexStatus status) throws IOException {
    var index = createIndex(SearchIndex.mockMetricsFactory());
    index.setStatus(status);
    return index;
  }

  private LuceneVectorIndex createIndex(PerIndexMetricsFactory metricsFactory) throws IOException {
    var temporaryFolder = TestUtils.getTempFolder();
    var indexPath = temporaryFolder.getRoot().toPath().resolve("indexData");
    var metadataPath = temporaryFolder.getRoot().toPath().resolve("indexMapping");
    LuceneConfig config = LuceneConfigBuilder.builder().dataPath(indexPath).build();

    SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
    return LuceneVectorIndex.createDiskBacked(
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
        VectorIndex.MOCK_INDEX_DEFINITION_GENERATION.getIndexDefinition(),
        VectorIndex.MOCK_INDEX_DEFINITION_GENERATION.generation().formatVersion.asLucene(),
        new AtomicDirectoryRemover(TestUtils.getTempFolder().getRoot().toPath()),
        metricsFactory);
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
