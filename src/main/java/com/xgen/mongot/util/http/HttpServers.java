package com.xgen.mongot.util.http;

import com.google.common.annotations.VisibleForTesting;
import com.sun.net.httpserver.HttpServer;
import com.xgen.mongot.util.retry.FixedBackoffPolicy;
import com.xgen.mongot.util.retry.Retry;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Creates {@link HttpServer}s, retrying transient bind failures a bounded number of times so a
 * transient failure during startup does not crash bootstrap.
 */
public final class HttpServers {
  private static final Logger LOG = LoggerFactory.getLogger(HttpServers.class);

  // Retry transient server-creation failures for a bounded period before failing bootstrap.
  private static final Duration BIND_RETRY_TIMEOUT = Duration.ofMinutes(1);
  private static final Duration BIND_RETRY_INTERVAL = Duration.ofSeconds(1);
  private static final int BIND_MAX_RETRIES =
      Math.toIntExact(BIND_RETRY_TIMEOUT.dividedBy(BIND_RETRY_INTERVAL));
  private static final int MAX_SUPPRESSED_RETRY_FAILURES = 3;

  private HttpServers() {}

  /**
   * Creates a plain-HTTP {@link HttpServer} at {@code address}, retrying transient bind failures a
   * bounded number of times before failing.
   *
   * @param endpointName how the endpoint is identified in log messages, e.g. {@code "Prometheus"}
   */
  public static HttpServer createWithRetry(InetSocketAddress address, String endpointName)
      throws IOException {
    return createWithRetry(
        address,
        BIND_RETRY_INTERVAL,
        BIND_MAX_RETRIES,
        endpointName,
        addr -> HttpServer.create(addr, 0));
  }

  /**
   * Creates an {@link HttpServer} via {@code binder}, retrying transient bind failures a bounded
   * number of times before failing.
   *
   * @param endpointName how the endpoint is identified in log messages, e.g. {@code "Prometheus"}
   */
  public static HttpServer createWithRetry(
      InetSocketAddress address, String endpointName, HttpServerBinder binder) throws IOException {
    return createWithRetry(address, BIND_RETRY_INTERVAL, BIND_MAX_RETRIES, endpointName, binder);
  }

  /**
   * Creates the {@link HttpServer}, retrying {@link IOException}s a bounded number of times.
   *
   * <p>A transient failure during startup should not crash bootstrap. Retrying the checked failures
   * exposed by {@link HttpServer#create} lets mongot recover once the failure clears.
   */
  @VisibleForTesting
  static HttpServer createWithRetry(
      InetSocketAddress address,
      Duration retryInterval,
      int maxRetries,
      String endpointName,
      HttpServerBinder binder)
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
                          .log("Failed to create %s endpoint, retrying".formatted(endpointName));
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
            .log("Successfully bound %s endpoint after retrying".formatted(endpointName));
      }
      return server;
    } catch (IOException e) {
      previousFailures.stream().filter(failure -> failure != e).forEach(e::addSuppressed);
      LOG.atError()
          .addKeyValue("address", address)
          .addKeyValue("attempts", retryCount.get() + 1)
          .setCause(e)
          .log("Failed to create %s endpoint after retrying; giving up".formatted(endpointName));
      throw e;
    }
  }

  @FunctionalInterface
  public interface HttpServerBinder {
    HttpServer bind(InetSocketAddress address) throws IOException;
  }
}
