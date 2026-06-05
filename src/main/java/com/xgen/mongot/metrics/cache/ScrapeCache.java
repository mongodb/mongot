package com.xgen.mongot.metrics.cache;

import com.xgen.mongot.util.concurrent.Executors;
import com.xgen.mongot.util.concurrent.NamedExecutorService;
import com.xgen.mongot.util.concurrent.NamedScheduledExecutorService;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import java.time.Duration;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Caches Prometheus scrape responses, decoupling metrics calculation from the metrics reading path.
 *
 * <p>Operates in two modes:
 *
 * <ul>
 *   <li>Live mode: each {@link #get(Duration)} computes inline by calling the registry, bounded by
 *       the caller-supplied timeout. On timeout, transitions to cached mode.
 *   <li>Cached mode: a background scheduled task refreshes the cache on a fixed cadence; {@link
 *       #get(Duration)} serves the cached metrics verbatim. Returns to live mode after the sticky
 *       window expires.
 * </ul>
 */
public class ScrapeCache {
  private static final Logger LOG = LoggerFactory.getLogger(ScrapeCache.class);

  static final String COMPUTE_EXECUTOR_NAME = "scrape-cache-compute";
  static final String REFRESH_EXECUTOR_NAME = "scrape-cache-refresh";

  public static final String COMPUTED_AT_METRIC_NAME = "metrics_computed_at_seconds";
  public static final String COMPUTED_IN_BACKGROUND_METRIC_NAME = "metrics_computed_in_background";

  /** Predefined timeout for callers that want an unbounded scrape. */
  public static final Duration NO_TIMEOUT = Duration.ofMillis(Long.MAX_VALUE);

  private final ScrapeCacheConfig config;
  private final IntervalThrottle throttle;

  // Original source for the metrics — every cache scrape ultimately delegates to registry.scrape().
  private final PrometheusMeterRegistry registry;
  // Cached metrics
  private final AtomicReference<String> cachedMetrics = new AtomicReference<>();

  // 0 indicates ScrapeCache serves live metrics
  private static final long LIVE_MODE = 0L;
  // When not 0, indicates when the cached mode expires
  private final AtomicLong cachedModeExpiresAtMs = new AtomicLong(LIVE_MODE);
  // Future for the background compute task
  private final AtomicReference<ScheduledFuture<?>> backgroundComputeFuture =
      new AtomicReference<>();
  // Shared live-mode scrape. Concurrent callers wait on the same Future instead of queueing
  // duplicate registry.scrape() calls behind the single compute thread.
  private @Nullable Future<String> liveComputeFuture;
  private final NamedExecutorService liveComputeExecutor;
  private final NamedScheduledExecutorService backgroundComputeExecutor;

  public ScrapeCache(
      PrometheusMeterRegistry registry, ScrapeCacheConfig config, IntervalThrottle throttle) {
    this.registry = registry;
    this.config = config;
    this.throttle = throttle;
    this.liveComputeExecutor = Executors.fixedSizeThreadPool(COMPUTE_EXECUTOR_NAME, 1, registry);
    this.backgroundComputeExecutor =
        Executors.singleThreadScheduledExecutor(REFRESH_EXECUTOR_NAME, registry);

    Gauge.builder(COMPUTED_AT_METRIC_NAME, () -> System.currentTimeMillis() / 1000.0)
        .description("Unix timestamp in seconds at which the cached metrics were last computed")
        .register(registry);

    Gauge.builder(
            COMPUTED_IN_BACKGROUND_METRIC_NAME,
            () -> Thread.currentThread().getName().contains(REFRESH_EXECUTOR_NAME) ? 1.0 : 0.0)
        .description("1 if computed in cached mode (background); 0 if computed in live mode")
        .register(registry);
  }

  /**
   * Returns fresh or cached metrics depending on live/cached mode.
   *
   * <p>In live mode, calls {@code registry.scrape()} bounded by {@code timeout}; on timeout,
   * switches to cached mode and serves periodically refreshed metrics.
   */
  public String get(Duration timeout) {
    if (this.throttle.shouldThrottle()) {
      LOG.atDebug().log("scrape throttled, serving cached metrics");
      return getCachedMetrics();
    }
    // Exit on the read path so the background thread keeps cachedMetrics warm until exit. If the
    // subsequent live scrape() times out, the fallback snapshot is at most one refresh old.
    exitCachedModeIfExpired();

    if (isInCachedMode()) {
      return getCachedMetrics();
    }

    try {
      String metrics = computeWithTimeout(timeout);
      this.cachedMetrics.set(metrics);
      return metrics;
    } catch (TimeoutException e) {
      LOG.atInfo()
          .addKeyValue("timeoutMs", timeout.toMillis())
          .log("Scrape exceeded timeout; entering cached mode and serving cached metrics");
      enterCachedModeIfNeeded();
      return getCachedMetrics();
    }
  }

  private String computeWithTimeout(Duration timeout) throws TimeoutException {
    Future<String> future = getOrStartLiveComputeFuture();
    try {
      return future.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      LOG.error("Registry threw error while scraping metrics", cause);
      if (cause instanceof RuntimeException) {
        throw (RuntimeException) cause;
      }
      if (cause instanceof Error) {
        throw (Error) cause;
      }
      throw new RuntimeException(cause);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Interrupted while scraping metrics", e);
    }
  }

  private synchronized Future<String> getOrStartLiveComputeFuture() {
    if (this.liveComputeFuture != null && !this.liveComputeFuture.isDone()) {
      return this.liveComputeFuture;
    }
    FutureTask<String> task =
        new FutureTask<>(this.registry::scrape) {
          @Override
          protected void done() {
            clearLiveComputeFuture(this);
          }
        };
    this.liveComputeExecutor.execute(task);
    this.liveComputeFuture = task;
    return task;
  }

  private synchronized void clearLiveComputeFuture(Future<String> task) {
    if (this.liveComputeFuture == task) {
      this.liveComputeFuture = null;
    }
  }

  private boolean isInCachedMode() {
    return this.cachedModeExpiresAtMs.get() != LIVE_MODE;
  }

  private void exitCachedModeIfExpired() {
    if (!isInCachedMode()) {
      return;
    }
    boolean cachedModeExpired = System.currentTimeMillis() >= this.cachedModeExpiresAtMs.get();
    if (cachedModeExpired) {
      exitCachedMode();
    }
  }

  private synchronized void exitCachedMode() {
    if (!isInCachedMode()) {
      return;
    }
    this.cachedModeExpiresAtMs.set(LIVE_MODE);
    cancelRefreshFuture();
  }

  private void cancelRefreshFuture() {
    ScheduledFuture<?> f = this.backgroundComputeFuture.getAndSet(null);
    if (f != null) {
      f.cancel(false);
    }
  }

  private synchronized void enterCachedModeIfNeeded() {
    long expires =
        System.currentTimeMillis()
            + TimeUnit.SECONDS.toMillis(this.config.slowModeStickyDurationSec());
    if (!this.cachedModeExpiresAtMs.compareAndSet(LIVE_MODE, expires)) {
      return;
    }
    ScheduledFuture<?> f =
        this.backgroundComputeExecutor.scheduleWithFixedDelay(
            this::refreshCachedMetrics,
            0,
            this.config.backgroundRefreshIntervalSec(),
            TimeUnit.SECONDS);
    this.backgroundComputeFuture.set(f);
  }

  private void refreshCachedMetrics() {
    try {
      this.cachedMetrics.set(this.registry.scrape());
    } catch (Throwable t) {
      LOG.error("Background scrape cache refresh failed; switching to live mode", t);
    }
  }

  private String getCachedMetrics() {
    String metrics = this.cachedMetrics.get();
    return metrics != null ? metrics : "";
  }

  public void close() {
    Executors.shutdownOrFail(this.backgroundComputeExecutor);
    Executors.shutdownOrFail(this.liveComputeExecutor);
  }
}
