package com.xgen.mongot.metrics.cache;

import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import java.time.Clock;
import java.time.Duration;
import org.junit.Assert;
import org.junit.Test;

public class ScrapeCacheTest {

  private static final Duration THROTTLE_INTERVAL = Duration.ofSeconds(30);

  private static IntervalThrottle defaultThrottle() {
    return new IntervalThrottle(Clock.systemUTC(), THROTTLE_INTERVAL);
  }

  @Test
  public void testGetReturnsSupplerValue() {
    PrometheusMeterRegistry registry =
        new PrometheusMeterRegistry(PrometheusConfig.DEFAULT) {
          @Override
          public String scrape() {
            return "data";
          }
        };
    ScrapeCache cache =
        new ScrapeCache(registry, ScrapeCacheConfig.getDefault(), defaultThrottle());
    try {
      Assert.assertEquals("data", cache.get(ScrapeCache.NO_TIMEOUT));
    } finally {
      cache.close();
    }
  }

  @Test
  public void testGetPropagatesException() {
    RuntimeException ex = new RuntimeException("boom");
    PrometheusMeterRegistry registry =
        new PrometheusMeterRegistry(PrometheusConfig.DEFAULT) {
          @Override
          public String scrape() {
            throw ex;
          }
        };
    ScrapeCache cache =
        new ScrapeCache(registry, ScrapeCacheConfig.getDefault(), defaultThrottle());
    try {
      RuntimeException thrown =
          Assert.assertThrows(RuntimeException.class, () -> cache.get(ScrapeCache.NO_TIMEOUT));
      Assert.assertSame(ex, thrown);
    } finally {
      cache.close();
    }
  }
}
