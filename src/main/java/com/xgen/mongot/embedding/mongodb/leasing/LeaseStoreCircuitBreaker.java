package com.xgen.mongot.embedding.mongodb.leasing;

import com.google.common.annotations.VisibleForTesting;
import io.micrometer.core.instrument.Counter;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Backs off AutoEmbedding leadership-acquisition writes when the customer majority-commit point is
 * stalled, so relaxing the follower poll off linearizable does not turn a silent orphan into a
 * {@code w:majority} acquire storm against an already-saturated primary.
 *
 * <p>Only ever suppresses acquisition (never grants it), so it fails safe: a wrongly-open breaker
 * merely delays failover (bounded by the cooldown, retried by a half-open probe), never
 * split-brain. Trips on stall-class failures and resets on any store response (commit or OCC loss),
 * so ordinary contention and genuine orphans — where acquisition succeeds — never open it.
 * Thread-safe.
 */
final class LeaseStoreCircuitBreaker {
  private static final Logger LOG = LoggerFactory.getLogger(LeaseStoreCircuitBreaker.class);

  private final int failureThreshold;
  private final long baseCooldownMs;
  private final long maxCooldownMs;
  private final LongSupplier clockMs;
  private final Counter openedCounter;
  private final AtomicLong stalledGauge;

  // Consecutive stall-class failures while closed; any store response resets it.
  private int consecutiveStallFailures = 0;
  // Re-open count in the current stall episode; drives the cooldown backoff.
  private int openStreak = 0;
  // Epoch ms until which attempts are suppressed. A granted half-open probe reserves the next
  // window here, so only one probe runs at a time even across threads.
  private long openUntilMs = 0;
  private boolean open = false;

  LeaseStoreCircuitBreaker(
      int failureThreshold,
      Duration baseCooldown,
      Duration maxCooldown,
      Counter openedCounter,
      AtomicLong stalledGauge) {
    this(
        failureThreshold,
        baseCooldown,
        maxCooldown,
        openedCounter,
        stalledGauge,
        System::currentTimeMillis);
  }

  @VisibleForTesting
  LeaseStoreCircuitBreaker(
      int failureThreshold,
      Duration baseCooldown,
      Duration maxCooldown,
      Counter openedCounter,
      AtomicLong stalledGauge,
      LongSupplier clockMs) {
    this.failureThreshold = failureThreshold;
    this.baseCooldownMs = baseCooldown.toMillis();
    this.maxCooldownMs = maxCooldown.toMillis();
    this.openedCounter = openedCounter;
    this.stalledGauge = stalledGauge;
    this.clockMs = clockMs;
  }

  /**
   * Whether an acquisition attempt may proceed. Grants exactly one half-open probe per cooldown by
   * reserving the next window, so concurrent callers stay suppressed until the probe reports back
   * (or the reserved window elapses, self-healing a probe that never reports).
   */
  synchronized boolean allowAttempt() {
    if (!this.open) {
      return true;
    }
    long now = this.clockMs.getAsLong();
    if (now < this.openUntilMs) {
      return false;
    }
    // Half-open: grant one probe; reserve the next window so no concurrent caller probes.
    this.openUntilMs = now + currentCooldownMs();
    return true;
  }

  /**
   * Records a stall-class failure (store unreachable / commit stalled) from acquire or renew. Each
   * call is one fully-retried operation (MongoClientOperationExecutor already retries ~6x
   * internally), so the threshold counts retried operations, not raw socket errors.
   */
  synchronized void recordStallFailure() {
    if (this.open) {
      // Failed half-open probe: extend the cooldown with backoff.
      long cooldownMs = openWithBackoff();
      LOG.atInfo()
          .addKeyValue("cooldownMs", cooldownMs)
          .addKeyValue("openStreak", this.openStreak)
          .log("Lease store circuit breaker probe failed - extending cooldown");
      return;
    }
    if (++this.consecutiveStallFailures >= this.failureThreshold) {
      this.openStreak = 0;
      this.open = true;
      long cooldownMs = openWithBackoff();
      this.openedCounter.increment();
      this.stalledGauge.set(1);
      LOG.atWarn()
          .addKeyValue("consecutiveStallFailures", this.consecutiveStallFailures)
          .addKeyValue("cooldownMs", cooldownMs)
          .log("Lease store circuit breaker opened - suspected customer commit-point stall");
    }
  }

  /** Records that the store responded (committed write or clean OCC loss): resets and closes. */
  synchronized void recordStoreReachable() {
    this.consecutiveStallFailures = 0;
    if (this.open) {
      this.open = false;
      this.openStreak = 0;
      this.openUntilMs = 0;
      this.stalledGauge.set(0);
      LOG.atInfo().log("Lease store circuit breaker closed - lease store reachable again");
    }
  }

  private long openWithBackoff() {
    long cooldownMs = currentCooldownMs();
    this.openUntilMs = this.clockMs.getAsLong() + cooldownMs;
    this.openStreak++;
    return cooldownMs;
  }

  private long currentCooldownMs() {
    // base * 2^openStreak, capped; shift bounded to avoid overflow before the cap applies.
    return Math.min(this.baseCooldownMs << Math.min(this.openStreak, 20), this.maxCooldownMs);
  }

  @VisibleForTesting
  synchronized boolean isOpen() {
    return this.open;
  }
}
