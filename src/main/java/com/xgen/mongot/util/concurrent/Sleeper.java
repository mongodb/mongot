package com.xgen.mongot.util.concurrent;

import java.time.Duration;

/**
 * Sleeps the calling thread. Exists so that code with a retry backoff can be unit tested without
 * spending real wall-clock time: production code uses {@link #DEFAULT}, while tests inject {@link
 * #NO_OP} (or a recording implementation that asserts on the requested durations).
 */
@FunctionalInterface
public interface Sleeper {

  /** Sleeps for the given duration. Durations of zero or less return immediately. */
  void sleep(Duration duration) throws InterruptedException;

  /** Sleeps the calling thread via {@link Thread#sleep(long)}. */
  Sleeper DEFAULT =
      duration -> {
        if (duration.isZero() || duration.isNegative()) {
          return;
        }
        Thread.sleep(duration.toMillis());
      };

  /** Returns immediately without sleeping. For tests. */
  Sleeper NO_OP = duration -> {};
}
