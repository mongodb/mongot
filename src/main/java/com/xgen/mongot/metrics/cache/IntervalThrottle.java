package com.xgen.mongot.metrics.cache;

import java.time.Clock;
import java.time.Duration;

/**
 * Limits how often something can run. {@link #shouldThrottle()} returns true (skip) if it was
 * called less than {@code interval} ago, otherwise it returns false and allows the call through.
 */
public class IntervalThrottle {
  private final Clock clock;
  private final long intervalMs;
  // Earliest time the next call may proceed.
  private long nextAllowedAtMs;

  public IntervalThrottle(Clock clock, Duration interval) {
    this.clock = clock;
    this.intervalMs = interval.toMillis();
    // Allow the first call immediately.
    this.nextAllowedAtMs = clock.millis();
  }

  /** Returns true to throttle (skip) the call, or false to allow it. */
  public synchronized boolean shouldThrottle() {
    long now = this.clock.millis();
    if (now < this.nextAllowedAtMs) {
      return true;
    }
    this.nextAllowedAtMs = now + this.intervalMs;
    return false;
  }
}
