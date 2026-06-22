package com.xgen.mongot.util.retry;

import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;
import net.jodah.failsafe.RetryPolicy;

public interface BackoffPolicy {
  /** Apply retry parameters specific to the policy. */
  <T> RetryPolicy<T> applyParameters(RetryPolicy<T> retryPolicy);

  /**
   * Returns the delay to wait before the next attempt, given the number of consecutive failures
   * observed so far. Returns {@link Duration#ZERO} when {@code consecutiveFailures} is
   * non-positive.
   */
  Duration delayFor(int consecutiveFailures);

  /**
   * Returns {@code base} scaled by a uniform random factor in {@code [1 - jitter, 1 + jitter]}.
   * When {@code jitter} is zero, returns {@code base} unchanged.
   */
  default Duration applyJitter(Duration base, double jitter) {
    if (jitter == 0.0) {
      return base;
    }
    double factor = 1.0 + ThreadLocalRandom.current().nextDouble(-jitter, jitter);
    long nanos = (long) (base.toNanos() * factor);
    return Duration.ofNanos(Math.max(nanos, 0L));
  }
}
