package com.xgen.mongot.index.lucene;

import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import com.xgen.mongot.index.status.IndexStatus;
import com.xgen.mongot.index.status.IndexStatus.StatusCode;
import com.xgen.mongot.metrics.PerIndexMetricsFactory;
import com.xgen.mongot.util.Crash;
import com.xgen.mongot.util.VerboseRunnable;
import io.micrometer.core.instrument.Timer;
import java.io.Closeable;
import java.time.Duration;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.apache.lucene.search.ReferenceManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class PeriodicLuceneIndexRefresher implements Closeable, VerboseRunnable {

  private static final Logger LOG = LoggerFactory.getLogger(PeriodicLuceneIndexRefresher.class);

  private final ImmutableList<ReferenceManager<?>> searcherManagers;
  private final Supplier<IndexStatus> indexStatusRef;
  private final PerIndexMetricsFactory metricsFactory;
  private final Timer timer;
  private final ScheduledFuture<?> scheduledRefresh;

  @GuardedBy("this")
  private boolean shutdown = false;

  PeriodicLuceneIndexRefresher(
      ScheduledExecutorService executor,
      Duration interval,
      ImmutableList<ReferenceManager<?>> searcherManagers,
      Supplier<IndexStatus> indexStatusRef,
      PerIndexMetricsFactory metricsFactory) {
    this.searcherManagers = searcherManagers;
    this.indexStatusRef = indexStatusRef;
    this.metricsFactory = metricsFactory;
    this.timer =
        metricsFactory.getProcessMetricsFactory().timer("refreshDurations", TimeUnit.MILLISECONDS);
    this.scheduledRefresh =
        executor.scheduleWithFixedDelay(this, 0, interval.toMillis(), TimeUnit.MILLISECONDS);
  }

  /**
   * Close the PeriodicLuceneIndexRefresher and cancel its scheduled refresh.
   *
   * <p>This method does not return until it is guaranteed that the searcher managers will not be
   * used again. It is synchronized with refreshIfNeeded(), so if a refresh is running, close()
   * blocks until it finishes. Any later run sees shutdown set and returns without refreshing.
   */
  @Override
  public synchronized void close() {
    this.shutdown = true;
    // Mocked executors in tests return null from scheduleWithFixedDelay.
    if (this.scheduledRefresh != null) {
      this.scheduledRefresh.cancel(false);
    }
    this.metricsFactory.close();
  }

  @Override
  public void verboseRun() {
    refreshIfNeeded();
  }

  @Override
  public Logger getLogger() {
    return LOG;
  }

  private synchronized void refreshIfNeeded() {
    if (this.shutdown) {
      return;
    }
    if (this.indexStatusRef.get().getStatusCode() == StatusCode.STEADY) {
      for (var searcherManager : this.searcherManagers) {
        this.timer.record(
            () ->
                Crash.because("failed to refresh searcher manager")
                    .ifThrows(searcherManager::maybeRefreshBlocking));
      }
    }
  }
}
