package com.xgen.mongot.metrics.prometheus;

import com.google.common.annotations.VisibleForTesting;
import com.sun.net.httpserver.HttpServer;
import com.xgen.mongot.featureflag.Feature;
import com.xgen.mongot.featureflag.FeatureFlags;
import com.xgen.mongot.metrics.cache.IntervalThrottle;
import com.xgen.mongot.metrics.cache.ScrapeCache;
import com.xgen.mongot.metrics.cache.ScrapeCacheConfig;
import com.xgen.mongot.util.retry.FixedBackoffPolicy;
import com.xgen.mongot.util.retry.Retry;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.config.MeterFilter;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import io.micrometer.prometheusmetrics.PrometheusNamingConvention;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Exports an endpoint that Prometheus can pull metrics from. */
public class PrometheusServer {
  private static final Logger LOG = LoggerFactory.getLogger(PrometheusServer.class);
  protected static final String ENDPOINT_PATH = "/metrics";
  public static final int DEFAULT_PORT = 9946;

  protected static final String SCRAPING_TIMER_NAME = "prometheus-server-scraping-timer";
  protected static final Tags SCRAPING_TIMER_TAGS = Tags.of("timeUnit", "milliseconds");

  // Retry transient server-creation failures for a bounded period before failing bootstrap.
  private static final Duration BIND_RETRY_TIMEOUT = Duration.ofMinutes(1);
  private static final Duration BIND_RETRY_INTERVAL = Duration.ofSeconds(1);
  private static final int BIND_MAX_RETRIES =
      Math.toIntExact(BIND_RETRY_TIMEOUT.dividedBy(BIND_RETRY_INTERVAL));
  private static final int MAX_SUPPRESSED_RETRY_FAILURES = 3;

  // See https://prometheus.io/docs/instrumenting/exposition_formats/#basic-info
  // Matches our version of micrometer:
  // https://github.com/micrometer-metrics/micrometer/blob/1.15.x/implementations/micrometer-registry-prometheus/src/main/java/io/micrometer/prometheusmetrics/PrometheusMeterRegistry.java#L655
  private static final String PROMETHEUS_SCRAPE_CONTENT_TYPE =
      "text/plain; version=0.0.4; charset=utf-8";

  private final HttpServer server;
  private final PrometheusMeterRegistry prometheusMeterRegistry;

  // Serves cached registry scrape output per request.
  private final Optional<ScrapeCache> scrapeCache;

  private PrometheusServer(
      HttpServer server,
      PrometheusMeterRegistry prometheusMeterRegistry,
      Optional<ScrapeCache> scrapeCache) {
    this.server = server;
    this.prometheusMeterRegistry = prometheusMeterRegistry;
    this.scrapeCache = scrapeCache;
  }

  public PrometheusMeterRegistry getPrometheusMeterRegistry() {
    return this.prometheusMeterRegistry;
  }

  public InetSocketAddress getAddress() {
    return this.server.getAddress();
  }

  /** Starts the Prometheus server. */
  public static PrometheusServer start(
      InetSocketAddress address,
      Iterable<Tag> commonTags,
      List<MeterFilter> filters,
      FeatureFlags featureFlags,
      ScrapeCacheConfig scrapeCacheConfig) {
    return startInternal(
        address,
        new PrometheusMeterRegistry(PrometheusConfig.DEFAULT),
        commonTags,
        filters,
        featureFlags,
        scrapeCacheConfig,
        /* preWarmCache= */ true,
        Optional.empty());
  }

  /** Test-visible entry point. */
  @VisibleForTesting
  static PrometheusServer startInternal(
      InetSocketAddress address,
      PrometheusMeterRegistry prometheusRegistry,
      Iterable<Tag> commonTags,
      List<MeterFilter> filters,
      FeatureFlags featureFlags,
      ScrapeCacheConfig scrapeCacheConfig,
      boolean preWarmCache,
      Optional<Executor> httpExecutor) {
    prometheusRegistry.config().namingConvention(new MongotPrometheusNamingConvention());
    prometheusRegistry.config().commonTags(commonTags);
    filters.forEach(filter -> prometheusRegistry.config().meterFilter(filter));

    Timer scrapingTimer =
        Timer.builder(SCRAPING_TIMER_NAME)
            .tags(SCRAPING_TIMER_TAGS)
            .publishPercentiles(0.5, 0.75, 0.9, 0.99)
            .register(prometheusRegistry);

    Optional<ScrapeCache> scrapeCache =
        featureFlags.isEnabled(Feature.METRICS_CACHE)
            ? Optional.of(
                new ScrapeCache(
                    prometheusRegistry,
                    scrapeCacheConfig,
                    new IntervalThrottle(
                        Clock.systemUTC(),
                        Duration.ofSeconds(scrapeCacheConfig.throttleIntervalSec()))))
            : Optional.empty();
    Supplier<String> scrapeSupplier;
    if (scrapeCache.isPresent()) {
      ScrapeCache c = scrapeCache.get();
      Duration httpScrapeTimeout = Duration.ofSeconds(scrapeCacheConfig.scrapeTimeoutSec());
      scrapeSupplier = () -> c.get(httpScrapeTimeout);
    } else {
      scrapeSupplier = prometheusRegistry::scrape;
    }

    LOG.atInfo()
        .addKeyValue("address", address)
        .addKeyValue("commonTags", commonTags)
        .addKeyValue("numMeterFilters", filters.size())
        .addKeyValue("cacheEnabled", scrapeCache.isPresent())
        .log("Starting and configuring prometheus");

    try {
      HttpServer server =
          createHttpServerWithRetry(
              address,
              BIND_RETRY_INTERVAL,
              BIND_MAX_RETRIES,
              candidateAddress -> HttpServer.create(candidateAddress, 0));
      server.createContext(
          ENDPOINT_PATH,
          httpExchange -> {
            @Nullable String response = scrapingTimer.record(scrapeSupplier);
            byte[] bytes = response != null ? response.getBytes() : "".getBytes();
            httpExchange.getResponseHeaders().add("Content-Type", PROMETHEUS_SCRAPE_CONTENT_TYPE);
            httpExchange.sendResponseHeaders(200, bytes.length);
            try (OutputStream os = httpExchange.getResponseBody()) {
              os.write(bytes);
            }
          });
      server.setExecutor(httpExecutor.orElse(null));
      server.start();
      // Eager pre-warm with no timeout so the first /metrics request doesn't pay cold-start cost.
      if (preWarmCache && scrapeCache.isPresent()) {
        LOG.atInfo().log("Triggering scrape cache pre-warm");
        scrapeCache.get().preWarm();
      } else {
        LOG.atInfo()
            .addKeyValue("preWarmCache", preWarmCache)
            .addKeyValue("cacheEnabled", scrapeCache.isPresent())
            .log("Skipping scrape cache pre-warm");
      }
      return new PrometheusServer(server, prometheusRegistry, scrapeCache);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Creates the {@link HttpServer}, retrying {@link IOException}s a bounded number of times.
   *
   * <p>A transient failure during startup should not crash bootstrap. Retrying the checked failures
   * exposed by {@link HttpServer#create} lets mongot recover once the failure clears.
   */
  @VisibleForTesting
  static HttpServer createHttpServerWithRetry(
      InetSocketAddress address, Duration retryInterval, int maxRetries, HttpServerBinder binder)
      throws IOException {
    AtomicInteger retryCount = new AtomicInteger();
    List<IOException> previousFailures = new ArrayList<>(MAX_SUPPRESSED_RETRY_FAILURES);

    try {
      HttpServer server =
          Retry.withBackoff(
                  FixedBackoffPolicy.builder().delay(retryInterval).maxRetries(maxRetries).build())
              .onRetry(
                  failure -> {
                    int currentRetry = retryCount.incrementAndGet();
                    if (currentRetry == 1) {
                      LOG.atWarn()
                          .addKeyValue("address", address)
                          .addKeyValue("maxRetries", maxRetries)
                          .addKeyValue("retryIntervalMs", retryInterval.toMillis())
                          .setCause(failure)
                          .log("Failed to create Prometheus endpoint, retrying");
                    }
                    if (previousFailures.size() < MAX_SUPPRESSED_RETRY_FAILURES) {
                      previousFailures.add((IOException) failure);
                    }
                  })
              .get(() -> binder.bind(address), IOException.class);
      if (retryCount.get() > 0) {
        LOG.atInfo()
            .addKeyValue("address", address)
            .addKeyValue("attempts", retryCount.get() + 1)
            .log("Successfully bound Prometheus endpoint after retrying");
      }
      return server;
    } catch (IOException e) {
      previousFailures.stream().filter(failure -> failure != e).forEach(e::addSuppressed);
      LOG.atError()
          .addKeyValue("address", address)
          .addKeyValue("attempts", retryCount.get() + 1)
          .setCause(e)
          .log("Failed to create Prometheus endpoint after retrying; giving up");
      throw e;
    }
  }

  @FunctionalInterface
  interface HttpServerBinder {
    HttpServer bind(InetSocketAddress address) throws IOException;
  }

  public void shutdown() {
    LOG.info("Shutting down Prometheus endpoint...");
    this.server.stop(0);
    this.scrapeCache.ifPresent(ScrapeCache::close);
  }

  private static class MongotPrometheusNamingConvention extends PrometheusNamingConvention {
    @Override
    public String name(String name, Meter.Type type, @Nullable String baseUnit) {
      return String.format("%s_%s", "mongot", super.name(name, type, baseUnit));
    }
  }
}
