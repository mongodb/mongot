package com.xgen.mongot.util;

import static com.google.common.truth.Truth.assertThat;

import com.google.common.flogger.util.Checks;
import com.google.errorprone.annotations.Var;
import com.xgen.mongot.featureflag.Feature;
import com.xgen.mongot.featureflag.FeatureFlags;
import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.lucene.LuceneIndexSearcherReference;
import com.xgen.mongot.index.lucene.searcher.LuceneIndexSearcher;
import com.xgen.mongot.index.lucene.searcher.LuceneSearcherFactory;
import com.xgen.mongot.index.lucene.searcher.LuceneSearcherManager;
import com.xgen.mongot.index.lucene.searcher.QueryCacheProvider.DefaultQueryCacheProvider;
import com.xgen.testing.mongot.mock.index.SearchIndex;
import io.micrometer.core.instrument.Counter;
import java.io.Closeable;
import java.io.IOException;
import java.lang.ref.WeakReference;
import java.lang.reflect.Field;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.ReferenceManager;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.junit.Test;

public class CloseablePhantomCleanerTest {

  private static final Counter NO_OP =
      new Counter() {
        @Override
        public void increment(double amount) {
        }

        @Override
        public double count() {
          return 0;
        }

        @Override
        public Id getId() {
          return null;
        }
      };

  private static class TestCountDownCloseable implements Closeable {

    static class Action implements CloseablePhantomCleaner.NoRefCloseable {

      private final CountDownLatch latch;

      public Action(CountDownLatch latch) {
        this.latch = latch;
      }

      @Override
      public void close() throws IOException {
        this.latch.countDown();
      }
    }

    public final CountDownLatch latch = new CountDownLatch(1);
    private final Closeable cleanable;

    public TestCountDownCloseable() {
      this.cleanable =
          CloseablePhantomCleaner.register(
              this, new CloseablePhantomCleaner.CleanerThreadAction(NO_OP, new Action(this.latch)));
    }

    @Override
    public void close() throws IOException {
      this.cleanable.close();
    }
  }

  private static class TestCounterCloseable implements Closeable {

    static class Action implements CloseablePhantomCleaner.NoRefCloseable {

      private final AtomicInteger counter;

      public Action(AtomicInteger counter) {
        this.counter = counter;
      }

      @Override
      public void close() throws IOException {
        this.counter.incrementAndGet();
      }
    }

    public final AtomicInteger counter = new AtomicInteger();

    private final Closeable cleanable;

    public TestCounterCloseable() {
      this.cleanable =
          CloseablePhantomCleaner.register(
              this,
              new CloseablePhantomCleaner.CleanerThreadAction(NO_OP, new Action(this.counter)));
    }

    @Override
    public void close() throws IOException {
      this.cleanable.close();
    }
  }

  @Test
  public void testPhantomObjectsAreCleanedUp() {
    @Var var closeable = new TestCountDownCloseable();
    var latch = closeable.latch;

    Checks.checkState(latch.getCount() == 1, "latch should not have counted down");

    // make closeable phantom reachable
    closeable = null;
    Condition.await()
        .atMost(Duration.ofSeconds(10))
        .withPollingInterval(Duration.ofMillis(100))
        .until(
            () -> {
              System.gc();
              return latch.getCount() == 0;
            });

    Checks.checkState(latch.getCount() == 0, "latch should have counted down");
  }

  @Test
  public void testCleanActionIsCalledExactlyOnce() throws IOException {
    @Var var closeable = new TestCounterCloseable();
    var counter = closeable.counter;
    Checks.checkState(counter.get() == 0, "counter is not 0");

    // Close the closeable to increment the counter to 1
    closeable.close();
    Checks.checkState(counter.get() == 1, "counter is not 1");

    // make closeable phantom reachable
    closeable = null;

    // Trigger GC a few times
    for (int i = 0; i < 10; ++i) {
      System.gc();
      try {
        Thread.sleep(10);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }

    // make sure the cleanable is not called again
    Checks.checkState(counter.get() == 1, "counter is not 1 after GC");
  }

  @Test
  public void testPhantomLuceneIndexSearcherReferenceCleanUp() throws Exception {
    try (var directory = new ByteBuffersDirectory()) {
      try (var writer = new IndexWriter(directory, new IndexWriterConfig())) {
        var manager =
            new LuceneSearcherManager(
                writer,
                new LuceneSearcherFactory(
                    SearchIndex.MOCK_INDEX_DEFINITION,
                    false,
                    new DefaultQueryCacheProvider(),
                    Optional.empty(),
                    SearchIndex.mockQueryMetricsUpdater(IndexDefinition.Type.SEARCH)),
                SearchIndex.mockMetricsFactory());
        var metricsUpdater = SearchIndex.mockQueryMetricsUpdater(IndexDefinition.Type.SEARCH);

        // Get initial index reader ref count.
        var initialSearcher = manager.acquire();
        var reader = initialSearcher.getIndexReader();
        // minus one to account for the above acquire
        var initialRefCount = reader.getRefCount() - 1;
        manager.release(initialSearcher);

        @SuppressWarnings("UnusedVariable")
        @Var
        var searcherReference =
            LuceneIndexSearcherReference.create(
                manager,
                metricsUpdater,
                FeatureFlags.withDefaults().enable(Feature.PHANTOM_REFERENCE_CLEANUP).build());

        // make it phantom and validate the search ref can be closed by detecting the added
        // counter metrics.
        Checks.checkState(
            metricsUpdater.getPhantomSearcherCleanupCounter().count() == 0, "counter is not 0");

        searcherReference = null;
        Condition.await()
            .atMost(Duration.ofSeconds(10))
            .withPollingInterval(Duration.ofMillis(100))
            .until(
                () -> {
                  System.gc();
                  return metricsUpdater.getPhantomSearcherCleanupCounter().count() == 1;
                });

        // verify ref count decreased
        var finalSearcher = manager.acquire();
        // minus one to account for the above acquire
        var finalRefCount = finalSearcher.getIndexReader().getRefCount() - 1;
        manager.release(finalSearcher);
        Checks.checkState(finalRefCount == initialRefCount, "ref count did not decrease");
      }
    }
  }

  @Test
  public void testPhantomLuceneIndexSearcherReferenceCleanUpWithExplicitClose() throws Exception {
    try (var directory = new ByteBuffersDirectory()) {
      try (var writer = new IndexWriter(directory, new IndexWriterConfig())) {
        var manager =
            new LuceneSearcherManager(
                writer,
                new LuceneSearcherFactory(
                    SearchIndex.MOCK_INDEX_DEFINITION,
                    false,
                    new DefaultQueryCacheProvider(),
                    Optional.empty(),
                    SearchIndex.mockQueryMetricsUpdater(IndexDefinition.Type.SEARCH)),
                SearchIndex.mockMetricsFactory());
        var metricsUpdater = SearchIndex.mockQueryMetricsUpdater(IndexDefinition.Type.SEARCH);

        // Get initial index reader ref count.
        var initialSearcher = manager.acquire();
        var reader = initialSearcher.getIndexReader();
        // minus one to account for the above acquire
        var initialRefCount = reader.getRefCount() - 1;
        manager.release(initialSearcher);

        var searcherReference =
            LuceneIndexSearcherReference.create(
                manager,
                metricsUpdater,
                FeatureFlags.withDefaults().enable(Feature.PHANTOM_REFERENCE_CLEANUP).build());

        Checks.checkState(
            metricsUpdater.getPhantomSearcherCleanupCounter().count() == 0, "counter is not 0");

        // explicitly close the searcher reference
        searcherReference.close();
        // Trigger GC a few times
        for (int i = 0; i < 10; ++i) {
          System.gc();
          try {
            Thread.sleep(10);
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
        }
        Checks.checkState(
            metricsUpdater.getPhantomSearcherCleanupCounter().count() == 0, "counter is not 0");

        // verify ref count decreased
        var finalSearcher = manager.acquire();
        // minus one to account for the above acquire
        var finalRefCount = finalSearcher.getIndexReader().getRefCount() - 1;
        manager.release(finalSearcher);
        Checks.checkState(finalRefCount == initialRefCount, "ref count did not decrease");
      }
    }
  }

  // CLOUDP-374952: In the old implementation, LuceneIndexSearcherReference#Cleaning holds weak
  // reerence to LuceneIndexSearcher. After index refresh, the LuceneIndexSearcher is replaced with
  // a new one. This will cause the LuceneIndexSearcherReference close skipped due to the searcher
  // become phantom.
  @Test
  public void testPhantomLuceneIndexSearcherReferenceCleanUpWithIndexRefresh() throws Exception {
    try (var directory = new ByteBuffersDirectory()) {
      try (var writer = new IndexWriter(directory, new IndexWriterConfig())) {
        AtomicLong releaseCounter = new AtomicLong();
        var manager =
            new LuceneSearcherManager(
                writer,
                new LuceneSearcherFactory(
                    SearchIndex.MOCK_INDEX_DEFINITION,
                    false,
                    new DefaultQueryCacheProvider(),
                    Optional.empty(),
                    SearchIndex.mockQueryMetricsUpdater(IndexDefinition.Type.SEARCH)),
                SearchIndex.mockMetricsFactory()) {
              @Override
              protected void decRef(LuceneIndexSearcher reference) throws IOException {
                super.decRef(reference);
                releaseCounter.incrementAndGet();
              }

            };
        var metricsUpdater = SearchIndex.mockQueryMetricsUpdater(IndexDefinition.Type.SEARCH);

        @Var
        var searcherReference =
            LuceneIndexSearcherReference.create(
                manager,
                metricsUpdater,
                FeatureFlags.withDefaults().enable(Feature.PHANTOM_REFERENCE_CLEANUP).build());

        // create a weak ref to track when the searcher is GC'd
        WeakReference<LuceneIndexSearcher> weakSearcher = new WeakReference<>(
            searcherReference.getIndexSearcher());

        // Null out current to mimic the index refresh behavior
        Field currentField = ReferenceManager.class.getDeclaredField("current");
        currentField.setAccessible(true);
        currentField.set(manager, null);

        searcherReference = null;

        Condition.await()
            .atMost(Duration.ofSeconds(10))
            .withPollingInterval(Duration.ofMillis(100))
            .until(
              () -> {
                System.gc();
                return weakSearcher.get() == null;
              });

        // Make sure the searcher is released
        assertThat(releaseCounter.get()).isEqualTo(1);
      }
    }
  }
}
