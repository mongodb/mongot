package com.xgen.mongot.server.grpc;

import com.xgen.mongot.metrics.ServerStatusDataExtractor.LoadSheddingMeterData;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;

/**
 * Prometheus counters for search-envoy overload-retry attempts that mongot rejects, on the {@code
 * OVERLOAD_RETRY_SIGNAL} flag path. Each rejection is tagged by envoy attempt and outcome: {@code
 * rejected} (envoy may retry on another host) or {@code exhausted} (retry budget spent, error
 * surfaced to the driver).
 *
 * <p>The attempt tag is clamped to the retry budget to keep Prometheus cardinality bounded — a raw
 * {@code x-envoy-attempt-count} from a misbehaving caller could otherwise spawn a series per
 * distinct value, worst during overload. The counter is registered only while the flag is on; a
 * host with the flag off emits no {@code loadShedding.envoyRetry} series.
 */
final class EnvoyRetryMetrics {

  // The retry budget that bounds the attempt tag, from the per-request
  // x-envoy-max-attempt-count header.
  private final int maxAttempts;
  private final MeterRegistry meterRegistry;

  EnvoyRetryMetrics(MeterRegistry meterRegistry, int maxAttempts) {
    this.meterRegistry = meterRegistry;
    this.maxAttempts = maxAttempts;
  }

  /** Records a rejection that envoy may still retry (attempt budget not yet exhausted). */
  void recordRejected(int attempt) {
    // A retriable attempt is, by definition, below the retry budget. Clamp to [1, max] so
    // out-of-range values (0, negatives, or values >= max from a misbehaving caller) collapse to
    // a bounded label instead of spawning unbounded time series.
    int label = Math.min(Math.max(attempt, 1), this.maxAttempts);
    counter(label, LoadSheddingMeterData.ENVOY_RETRY_REJECTED).increment();
  }

  /** Records a rejection on the final attempt, after envoy's retry budget is spent. */
  void recordExhausted(int attempt) {
    // The exhausted outcome is, by definition, the budget-exhausting (final) attempt, so it
    // always coincides with the max attempt. Label it as such regardless of the raw value to
    // keep the attempt tag bounded.
    counter(this.maxAttempts, LoadSheddingMeterData.ENVOY_RETRY_EXHAUSTED).increment();
  }

  private Counter counter(int attempt, String outcome) {
    return this.meterRegistry.counter(
        LoadSheddingMeterData.ENVOY_RETRY,
        LoadSheddingMeterData.ENVOY_RETRY_ATTEMPT_TAG,
        String.valueOf(attempt),
        LoadSheddingMeterData.ENVOY_RETRY_OUTCOME_TAG,
        outcome);
  }
}
