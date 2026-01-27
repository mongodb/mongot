package com.xgen.mongot.index.lucene;

import com.google.common.flogger.FluentLogger;
import com.xgen.mongot.featureflag.Feature;
import com.xgen.mongot.featureflag.FeatureFlags;
import com.xgen.mongot.index.IndexMetricsUpdater;
import com.xgen.mongot.index.lucene.explain.ProfilingIndexSearcher;
import com.xgen.mongot.index.lucene.explain.explainers.CollectorTimingFeatureExplainer;
import com.xgen.mongot.index.lucene.explain.explainers.MetadataFeatureExplainer;
import com.xgen.mongot.index.lucene.explain.profiler.QueryProfilerFeatureExplainer;
import com.xgen.mongot.index.lucene.explain.tracing.Explain;
import com.xgen.mongot.index.lucene.searcher.ConcurrentIndexSearcher;
import com.xgen.mongot.index.lucene.searcher.LuceneIndexSearcher;
import com.xgen.mongot.index.lucene.searcher.LuceneSearcherManager;
import com.xgen.mongot.util.CloseablePhantomCleaner;
import io.opentelemetry.context.Context;
import java.io.Closeable;
import java.io.IOException;
import java.lang.ref.WeakReference;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import org.apache.lucene.search.ReferenceManager;

/**
 * LuceneIndexSearcherReference couples together an IndexSearcher with the SearcherManager or other
 * manager that created it/is responsible for its lifecycle. LuceneIndexSearcherReference
 * automatically releases the IndexSearcher when closing the LuceneIndexSearcherReference.
 *
 * <p>This allows for IDEs to warn that the IndexSearcher wasn't released, and also allows for code
 * like the following to be written which will automatically release the searcher: <code>
 * try(final LuceneIndexSearcherReference searcherReference = getReference()) {
 * useSearcher(searcherReference.getIndexSearcher());
 * }
 * </code>
 */
public class LuceneIndexSearcherReference implements Closeable {

  /**
   * Cleanup handler for releasing IndexSearcher references when the parent
   * LuceneIndexSearcherReference is garbage collected.
   *
   * <p><b>Uses strong references</b> to prevent resource leaks: When the SearcherManager
   * refreshes (rotates to a new searcher), the old searcher would become weakly-reachable
   * and could be GC'd before this cleanup runs. Strong references ensure the searcher
   * remains alive until {@code manager.release(searcher)} is called, preventing leaks.
   */
  private record Cleaning(
      WeakReference<ReferenceManager<LuceneIndexSearcher>> searcherManager,
      LuceneIndexSearcher indexSearcher)
      implements CloseablePhantomCleaner.NoRefCloseable {

    Cleaning(
        ReferenceManager<LuceneIndexSearcher> searcherManager,
        LuceneIndexSearcher indexSearcher) {
      this(new WeakReference<>(searcherManager), indexSearcher);
    }

    private static final FluentLogger FLOGGER = FluentLogger.forEnclosingClass();

    @Override
    public void close() throws IOException {
      Optional<ReferenceManager<LuceneIndexSearcher>> maybeManager =
          Optional.ofNullable(this.searcherManager.get());
      if (maybeManager.isPresent()) {
        LuceneIndexSearcherReference.releaseSearcher(maybeManager.get(), this.indexSearcher);
      } else {
        FLOGGER.atWarning()
            .atMostEvery(10, TimeUnit.SECONDS)
            .log("Skip releasing searcher because manager or searcher become phantom");
      }
    }
  }

  private final LuceneIndexSearcher indexSearcher;
  private final Closeable cleaning;

  private static void releaseSearcher(
      ReferenceManager<LuceneIndexSearcher> searcherManager,
      LuceneIndexSearcher indexSearcher)
      throws IOException {
    searcherManager.release(indexSearcher);
  }

  private LuceneIndexSearcherReference(
      ReferenceManager<LuceneIndexSearcher> searcherManager,
      LuceneIndexSearcher indexSearcher,
      FeatureFlags featureFlags,
      IndexMetricsUpdater.QueryingMetricsUpdater metricsUpdater) {

    this.indexSearcher = indexSearcher;

    this.cleaning = featureFlags.isEnabled(Feature.PHANTOM_REFERENCE_CLEANUP)
        ? CloseablePhantomCleaner.register(
            this,
            new CloseablePhantomCleaner.CleanerThreadAction(
                metricsUpdater.getPhantomSearcherCleanupCounter(),
                new Cleaning(searcherManager, indexSearcher)))
        : () -> releaseSearcher(searcherManager, indexSearcher);
  }

  public static LuceneIndexSearcherReference create(
      LuceneSearcherManager searcherManager,
      IndexMetricsUpdater.QueryingMetricsUpdater metricsUpdater,
      FeatureFlags featureFlags)
      throws IOException {
    var initialSearcher = searcherManager.acquire();
    var searcher =
        Explain.getQueryInfo()
            .map(
                info ->
                    (LuceneIndexSearcher)
                        new ProfilingIndexSearcher(
                            initialSearcher,
                            info.getFeatureExplainer(
                                    QueryProfilerFeatureExplainer.class,
                                    () -> new QueryProfilerFeatureExplainer(metricsUpdater))
                                .getQueryProfiler(),
                            info.getFeatureExplainer(
                                CollectorTimingFeatureExplainer.class,
                                CollectorTimingFeatureExplainer::new),
                            info.getFeatureExplainer(
                                MetadataFeatureExplainer.class, MetadataFeatureExplainer::new)))
            .orElse(initialSearcher);
    return new LuceneIndexSearcherReference(
        searcherManager, searcher, featureFlags, metricsUpdater);
  }

  public static LuceneIndexSearcherReference create(
      LuceneSearcherManager searcherManager,
      Executor executor,
      IndexMetricsUpdater.QueryingMetricsUpdater metricsUpdater,
      FeatureFlags featureFlags)
      throws IOException {
    var initialSearcher = searcherManager.acquire();
    var searcher =
        Explain.getQueryInfo()
            .map(
                info ->
                    (LuceneIndexSearcher)
                        new ProfilingIndexSearcher(
                            initialSearcher,
                            info.getFeatureExplainer(
                                    QueryProfilerFeatureExplainer.class,
                                    () -> new QueryProfilerFeatureExplainer(metricsUpdater))
                                .getQueryProfiler(),
                            info.getFeatureExplainer(
                                CollectorTimingFeatureExplainer.class,
                                CollectorTimingFeatureExplainer::new),
                            info.getFeatureExplainer(
                                MetadataFeatureExplainer.class, MetadataFeatureExplainer::new),
                            Context.current().wrap(Explain.maybeWrap(executor))))
            .orElseGet(
                () ->
                    new ConcurrentIndexSearcher(initialSearcher, Context.current().wrap(executor)));
    return new LuceneIndexSearcherReference(
        searcherManager, searcher, featureFlags, metricsUpdater);
  }

  public LuceneIndexSearcher getIndexSearcher() {
    return this.indexSearcher;
  }

  @Override
  public void close() throws IOException {
    this.cleaning.close();
  }
}
