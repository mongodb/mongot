package com.xgen.mongot.index.lucene;

import static com.xgen.mongot.index.lucene.InstrumentedConcurrentMergeSchedulerTest.createMergeScheduler;
import static com.xgen.testing.mongot.mock.index.SearchIndex.MOCK_INDEX_DEFINITION;
import static com.xgen.testing.mongot.mock.index.SearchIndex.MOCK_INDEX_DEFINITION_GENERATION;
import static com.xgen.testing.mongot.mock.index.SearchIndex.MOCK_INDEX_GENERATION_ID;
import static org.mockito.ArgumentMatchers.any;

import com.google.common.collect.ImmutableList;
import com.xgen.mongot.featureflag.FeatureFlags;
import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlagRegistry;
import com.xgen.mongot.index.EncodedUserData;
import com.xgen.mongot.index.analyzer.wrapper.LuceneAnalyzer;
import com.xgen.mongot.index.lucene.merge.InstrumentedConcurrentMergeScheduler;
import com.xgen.mongot.index.lucene.searcher.LuceneSearcherFactory;
import com.xgen.mongot.index.lucene.searcher.LuceneSearcherManager;
import com.xgen.mongot.index.lucene.searcher.QueryCacheProvider;
import com.xgen.mongot.index.lucene.writer.SingleLuceneIndexWriter;
import com.xgen.mongot.index.status.IndexStatus;
import com.xgen.mongot.metrics.PerIndexMetricsFactory;
import com.xgen.testing.TestUtils;
import com.xgen.testing.mongot.index.analyzer.AnalyzerRegistryBuilder;
import com.xgen.testing.mongot.mock.index.SearchIndex;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.lucene.index.TieredMergePolicy;
import org.apache.lucene.search.ReferenceManager;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.junit.Assert;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

public class PeriodicLuceneIndexRefresherTest {

  @Test
  public void refreshesOnlyWhenInSteadyState() throws Exception {
    LuceneSearcherManager searcherManager = getSearcherManager();
    CountDownLatch refreshed = new CountDownLatch(1);
    AtomicInteger refreshCount = new AtomicInteger();
    searcherManager.addListener(
        new ReferenceManager.RefreshListener() {
          @Override
          public void beforeRefresh() {
            try {
              refreshed.countDown();
              refreshCount.getAndIncrement();
            } catch (Exception e) {
              throw new RuntimeException(e);
            }
          }

          @Override
          public void afterRefresh(boolean didRefresh) {}
        });

    AtomicReference<IndexStatus> statusRef = new AtomicReference<>(IndexStatus.notStarted());

    new PeriodicLuceneIndexRefresher(
        Executors.newSingleThreadScheduledExecutor(),
        Duration.ofMillis(50),
        ImmutableList.of(searcherManager),
        statusRef::get,
        Mockito.mock(PerIndexMetricsFactory.class));

    // The refresher should not refresh while in NOT_STARTED.
    Assert.assertFalse(refreshed.await(500, TimeUnit.MILLISECONDS));

    // Changing from NOT_STARTED to STEADY should make the refresher start refreshing.
    statusRef.set(IndexStatus.steady());
    refreshed.await(500, TimeUnit.MILLISECONDS);

    // Changing out of STEADY to INITIAL_SYNC should stop the refresher from refreshing.
    statusRef.set(IndexStatus.initialSync());

    // It's possible that we are racing with a current refresh, so we allow the final number of
    // refreshes to potentially be one more than the count after we set the index status to
    // INITIAL_SYNC;
    int postChangeRefreshCount = refreshCount.get();
    Thread.sleep(500);

    int postSleepRefreshCount = refreshCount.get();
    Assert.assertTrue(
        postSleepRefreshCount == postChangeRefreshCount
            || postSleepRefreshCount == postChangeRefreshCount + 1);
  }

  @Test
  public void testRefreshesAreTimed() throws Exception {
    LuceneSearcherManager searcherManager = getSearcherManager();
    CountDownLatch refreshed = new CountDownLatch(2);
    searcherManager.addListener(
        new ReferenceManager.RefreshListener() {
          @Override
          public void beforeRefresh() {
            try {
              refreshed.countDown();
            } catch (Exception e) {
              throw new RuntimeException(e);
            }
          }

          @Override
          public void afterRefresh(boolean didRefresh) {}
        });

    Timer timer = new SimpleMeterRegistry().timer("testTimer");
    var metricsFactory = Mockito.mock(PerIndexMetricsFactory.class);
    Mockito.when(metricsFactory.perIndexTimer(any())).thenReturn(timer);

    new PeriodicLuceneIndexRefresher(
        Executors.newSingleThreadScheduledExecutor(),
        Duration.ofMillis(50),
        ImmutableList.of(searcherManager),
        () -> IndexStatus.steady(),
        metricsFactory);

    Assert.assertTrue(refreshed.await(5, TimeUnit.SECONDS));
    Assert.assertTrue(timer.totalTime(TimeUnit.MILLISECONDS) >= 0);
    Assert.assertTrue(timer.count() >= 1);
  }

  private static LuceneSearcherManager getSearcherManager() throws Exception {
    SingleLuceneIndexWriter writer = getIndexWriter();
    return LuceneSearcherManager.create(
        writer.getLuceneWriter(),
        new LuceneSearcherFactory(
            MOCK_INDEX_DEFINITION,
            false,
            new QueryCacheProvider.DefaultQueryCacheProvider(),
            Optional.empty(),
            SearchIndex.mockQueryMetricsUpdater(MOCK_INDEX_DEFINITION.getType())),
        SearchIndex.mockMetricsFactory());
  }

  private static SingleLuceneIndexWriter getIndexWriter() throws Exception {
    TemporaryFolder temporaryFolder = TestUtils.getTempFolder();
    Directory directory = new MMapDirectory(temporaryFolder.getRoot().toPath());

    var luceneIndexWriter =
        SingleLuceneIndexWriter.createForSearchIndex(
            directory,
            createMergeScheduler(
                new InstrumentedConcurrentMergeScheduler(new SimpleMeterRegistry()),
                MOCK_INDEX_GENERATION_ID,
                0,
                1,
                false,
                directory),
            new TieredMergePolicy(),
            16D,
            Optional.empty(),
            Optional.empty(),
            LuceneAnalyzer.indexAnalyzer(MOCK_INDEX_DEFINITION, AnalyzerRegistryBuilder.empty()),
            MOCK_INDEX_DEFINITION.createFieldDefinitionResolver(
                MOCK_INDEX_DEFINITION_GENERATION.generation().indexFormatVersion),
            SearchIndex.mockIndexingMetricsUpdater(MOCK_INDEX_DEFINITION.getType()),
            Optional.empty(),
            FeatureFlags.getDefault(),
            DynamicFeatureFlagRegistry.empty(),
            false);

    luceneIndexWriter.commit(EncodedUserData.EMPTY);

    return luceneIndexWriter;
  }
}
