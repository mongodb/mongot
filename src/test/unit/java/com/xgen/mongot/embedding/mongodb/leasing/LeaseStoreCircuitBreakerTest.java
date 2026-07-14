package com.xgen.mongot.embedding.mongodb.leasing;

import static com.google.common.truth.Truth.assertThat;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link LeaseStoreCircuitBreaker}. */
@RunWith(JUnit4.class)
public class LeaseStoreCircuitBreakerTest {

  private static final int THRESHOLD = 3;
  private static final Duration BASE_COOLDOWN = Duration.ofSeconds(30);
  private static final Duration MAX_COOLDOWN = Duration.ofMinutes(5);

  private Counter openedCounter;
  private AtomicLong stalledGauge;
  private AtomicLong nowMs;
  private LeaseStoreCircuitBreaker breaker;

  @Before
  public void setUp() {
    this.openedCounter = new SimpleMeterRegistry().counter("circuitBreakerOpened");
    this.stalledGauge = new AtomicLong(0);
    this.nowMs = new AtomicLong(0);
    this.breaker =
        new LeaseStoreCircuitBreaker(
            THRESHOLD,
            BASE_COOLDOWN,
            MAX_COOLDOWN,
            this.openedCounter,
            this.stalledGauge,
            this.nowMs::get);
  }

  @Test
  public void opensOnlyAtThreshold_andStoreReachableResetsTheStreak() {
    assertThat(this.breaker.allowAttempt()).isTrue();
    assertThat(this.breaker.isOpen()).isFalse();
    assertThat(this.stalledGauge.get()).isEqualTo(0);

    // Below-threshold failures don't open; a store-reachable signal clears the streak, so the two
    // failures that follow (four total) still don't open it.
    this.breaker.recordStallFailure();
    this.breaker.recordStallFailure();
    this.breaker.recordStoreReachable();
    this.breaker.recordStallFailure();
    this.breaker.recordStallFailure();
    assertThat(this.breaker.isOpen()).isFalse();
    assertThat(this.openedCounter.count()).isEqualTo(0.0);

    // The THRESHOLD-th consecutive failure opens it: attempts suppressed, gauge and counter set.
    this.breaker.recordStallFailure();
    assertThat(this.breaker.isOpen()).isTrue();
    assertThat(this.breaker.allowAttempt()).isFalse();
    assertThat(this.stalledGauge.get()).isEqualTo(1);
    assertThat(this.openedCounter.count()).isEqualTo(1.0);
  }

  @Test
  public void halfOpenProbeSucceedsAndCloses() {
    open();

    // Suppressed until the cooldown elapses, then a single probe is permitted.
    this.nowMs.set(BASE_COOLDOWN.toMillis() - 1);
    assertThat(this.breaker.allowAttempt()).isFalse();
    this.nowMs.set(BASE_COOLDOWN.toMillis());
    assertThat(this.breaker.allowAttempt()).isTrue();

    // A store-reachable probe result closes the breaker.
    this.breaker.recordStoreReachable();
    assertThat(this.breaker.isOpen()).isFalse();
    assertThat(this.stalledGauge.get()).isEqualTo(0);
    assertThat(this.breaker.allowAttempt()).isTrue();
  }

  @Test
  public void halfOpenGrantsOnlyOneProbeAtATime() {
    open();
    this.nowMs.set(BASE_COOLDOWN.toMillis());
    // First caller gets the single probe; a second caller at the same instant (e.g. the heartbeat
    // thread) is denied, so concurrent w:majority acquire writes can't storm the stalled primary.
    assertThat(this.breaker.allowAttempt()).isTrue();
    assertThat(this.breaker.allowAttempt()).isFalse();
  }

  @Test
  public void halfOpenProbeFailureBacksOffExponentiallyUpToCap() {
    open();

    // First failed probe doubles the cooldown to 2 * base.
    this.nowMs.set(BASE_COOLDOWN.toMillis());
    assertThat(this.breaker.allowAttempt()).isTrue();
    this.breaker.recordStallFailure();
    long doubledBoundary = BASE_COOLDOWN.toMillis() + 2 * BASE_COOLDOWN.toMillis();
    this.nowMs.set(doubledBoundary - 1);
    assertThat(this.breaker.allowAttempt()).isFalse();
    this.nowMs.set(doubledBoundary);
    assertThat(this.breaker.allowAttempt()).isTrue();

    // Repeated failed probes saturate at the max cooldown, never longer.
    for (int i = 0; i < 20; i++) {
      this.nowMs.addAndGet(MAX_COOLDOWN.toMillis());
      assertThat(this.breaker.allowAttempt()).isTrue();
      this.breaker.recordStallFailure();
    }
    long lastOpenedAt = this.nowMs.get();
    this.nowMs.set(lastOpenedAt + MAX_COOLDOWN.toMillis() - 1);
    assertThat(this.breaker.allowAttempt()).isFalse();
    this.nowMs.set(lastOpenedAt + MAX_COOLDOWN.toMillis());
    assertThat(this.breaker.allowAttempt()).isTrue();

    // Re-opening from failed probes never re-increments the opened counter or clears the gauge.
    assertThat(this.openedCounter.count()).isEqualTo(1.0);
    assertThat(this.stalledGauge.get()).isEqualTo(1);
  }

  private void open() {
    for (int i = 0; i < THRESHOLD; i++) {
      this.breaker.recordStallFailure();
    }
    assertThat(this.breaker.isOpen()).isTrue();
  }
}
