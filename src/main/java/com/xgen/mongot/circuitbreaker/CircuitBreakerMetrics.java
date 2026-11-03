package com.xgen.mongot.circuitbreaker;

import com.xgen.mongot.metrics.MetricsFactory;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.concurrent.atomic.AtomicLong;

/**
 * What the circuit breaker reports.
 *
 * <p>In shadow mode these counters are the entire product of the feature: they are what turns the
 * starting thresholds into calibrated values.
 */
public final class CircuitBreakerMetrics {

  private final AtomicLong totalReservedBytes;
  private final Counter refused;
  private final Counter wouldHaveRefused;
  private final Counter cancelled;
  private final Counter wouldHaveCancelled;
  private final Counter breakerErrors;
  private final Counter reservedBytesCorrected;
  private final AtomicLong inFlightQueries;
  private final AtomicLong untrackedBytes;
  private final AtomicLong effectiveLimitBytes;
  private final AtomicLong untrackedHeapSampleCount;

  public CircuitBreakerMetrics(MeterRegistry meterRegistry) {
    MetricsFactory factory = new MetricsFactory("circuitBreaker", meterRegistry);
    this.totalReservedBytes = factory.numGauge("totalReservedBytes");
    this.inFlightQueries = factory.numGauge("inFlightQueries");
    this.untrackedBytes = factory.numGauge("untrackedBytes");
    this.effectiveLimitBytes = factory.numGauge("effectiveLimitBytes");
    this.untrackedHeapSampleCount = factory.numGauge("untrackedHeapSampleCount");
    this.refused = factory.counter("refused");
    this.wouldHaveRefused = factory.counter("wouldHaveRefused");
    this.cancelled = factory.counter("cancelled");
    this.wouldHaveCancelled = factory.counter("wouldHaveCancelled");
    this.breakerErrors = factory.counter("breakerErrors");
    this.reservedBytesCorrected = factory.counter("reservedBytesCorrected");
  }

  /** The node-wide total of reserved bytes, which the registry reads and writes in place. */
  AtomicLong totalReservedBytes() {
    return this.totalReservedBytes;
  }

  /** The count of registered running queries, which the registry keeps current. */
  AtomicLong inFlightQueries() {
    return this.inFlightQueries;
  }

  void recordRefused() {
    this.refused.increment();
  }

  void recordWouldHaveRefused() {
    this.wouldHaveRefused.increment();
  }

  void recordCancelled() {
    this.cancelled.increment();
  }

  void recordWouldHaveCancelled() {
    this.wouldHaveCancelled.increment();
  }

  void recordBreakerError() {
    this.breakerErrors.increment();
  }

  void recordReservedBytesCorrected() {
    this.reservedBytesCorrected.increment();
  }

  void recordUntrackedBytes(long bytes) {
    this.untrackedBytes.set(bytes);
  }

  void recordEffectiveLimitBytes(long bytes) {
    this.effectiveLimitBytes.set(bytes);
  }

  /** How many GC samples the untracked heap window holds. */
  void recordUntrackedHeapSampleCount(int count) {
    this.untrackedHeapSampleCount.set(count);
  }
}
