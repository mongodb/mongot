package com.xgen.mongot.server.grpc;

import static com.google.common.truth.Truth.assertThat;

import com.xgen.mongot.metrics.ServerStatusDataExtractor.LoadSheddingMeterData;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.Test;

public class EnvoyRetryMetricsTest {

  private static Counter counter(MeterRegistry registry, int attempt, String outcome) {
    return registry.counter(
        LoadSheddingMeterData.ENVOY_RETRY,
        LoadSheddingMeterData.ENVOY_RETRY_ATTEMPT_TAG,
        String.valueOf(attempt),
        LoadSheddingMeterData.ENVOY_RETRY_OUTCOME_TAG,
        outcome);
  }

  @Test
  public void recordRejected_incrementsRejectedSeriesPerAttempt() {
    var registry = new SimpleMeterRegistry();
    var metrics = new EnvoyRetryMetrics(registry, 3);

    metrics.recordRejected(1);
    metrics.recordRejected(1);
    metrics.recordRejected(2);

    assertThat(counter(registry, 1, LoadSheddingMeterData.ENVOY_RETRY_REJECTED).count())
        .isEqualTo(2.0);
    assertThat(counter(registry, 2, LoadSheddingMeterData.ENVOY_RETRY_REJECTED).count())
        .isEqualTo(1.0);
    assertThat(counter(registry, 1, LoadSheddingMeterData.ENVOY_RETRY_EXHAUSTED).count())
        .isEqualTo(0.0);
  }

  @Test
  public void recordExhausted_incrementsExhaustedSeriesOnly() {
    var registry = new SimpleMeterRegistry();
    var metrics = new EnvoyRetryMetrics(registry, 3);

    metrics.recordExhausted(3);

    assertThat(counter(registry, 3, LoadSheddingMeterData.ENVOY_RETRY_EXHAUSTED).count())
        .isEqualTo(1.0);
    assertThat(counter(registry, 3, LoadSheddingMeterData.ENVOY_RETRY_REJECTED).count())
        .isEqualTo(0.0);
  }

  @Test
  public void record_isIdempotentForSameAttemptAndOutcome() {
    var registry = new SimpleMeterRegistry();
    var metrics = new EnvoyRetryMetrics(registry, 3);

    // Two EnvoyRetryMetrics instances against the same registry share the underlying counter.
    var metricsAgain = new EnvoyRetryMetrics(registry, 3);
    metrics.recordRejected(1);
    metricsAgain.recordRejected(1);

    assertThat(counter(registry, 1, LoadSheddingMeterData.ENVOY_RETRY_REJECTED).count())
        .isEqualTo(2.0);
  }

  @Test
  public void recordRejected_clampsOutOfRangeValuesToBoundedLabel() {
    var registry = new SimpleMeterRegistry();
    var metrics = new EnvoyRetryMetrics(registry, 3);

    // 0 / negatives collapse to the first-attempt bucket (envoy's missing-header default is 1).
    metrics.recordRejected(0);
    metrics.recordRejected(-1);
    metrics.recordRejected(Integer.MIN_VALUE);
    assertThat(counter(registry, 1, LoadSheddingMeterData.ENVOY_RETRY_REJECTED).count())
        .isEqualTo(3.0);

    // Arbitrarily large values collapse to the max-attempt bucket; no per-value series are created.
    metrics.recordRejected(999);
    metrics.recordRejected(Integer.MAX_VALUE);
    assertThat(counter(registry, 3, LoadSheddingMeterData.ENVOY_RETRY_REJECTED).count())
        .isEqualTo(2.0);

    // The only rejected series that exist are within the budget {1, 2, 3}.
    var rejectedCounters =
        registry.find(LoadSheddingMeterData.ENVOY_RETRY).counters().stream()
            .filter(
                c ->
                    LoadSheddingMeterData.ENVOY_RETRY_REJECTED.equals(
                        c.getId().getTag(LoadSheddingMeterData.ENVOY_RETRY_OUTCOME_TAG)))
            .toList();
    assertThat(rejectedCounters).hasSize(2); // attempt=1 and attempt=3 only
  }

  @Test
  public void recordExhausted_alwaysLabelsAsMaxAttemptRegardlessOfRawValue() {
    var registry = new SimpleMeterRegistry();
    var metrics = new EnvoyRetryMetrics(registry, 3);

    metrics.recordExhausted(3);
    metrics.recordExhausted(999);
    metrics.recordExhausted(Integer.MAX_VALUE);
    metrics.recordExhausted(0);
    metrics.recordExhausted(-5);

    assertThat(counter(registry, 3, LoadSheddingMeterData.ENVOY_RETRY_EXHAUSTED).count())
        .isEqualTo(5.0);

    // Exactly one exhausted series exists (the max-attempt bucket), regardless of raw values.
    var exhaustedCounters =
        registry.find(LoadSheddingMeterData.ENVOY_RETRY).counters().stream()
            .filter(
                c ->
                    LoadSheddingMeterData.ENVOY_RETRY_EXHAUSTED.equals(
                        c.getId().getTag(LoadSheddingMeterData.ENVOY_RETRY_OUTCOME_TAG)))
            .toList();
    assertThat(exhaustedCounters).hasSize(1);
  }

  @Test
  public void record_usesInjectedBudgetNotHardcodedValue() {
    // Proves the attempt tag follows the configured retry budget rather than a literal 3, so a
    // future MMS-owned / envoy-provided max-attempt plugs in via the constructor.
    var registry = new SimpleMeterRegistry();
    var metrics = new EnvoyRetryMetrics(registry, 5);

    metrics.recordRejected(4);
    metrics.recordRejected(999);
    metrics.recordExhausted(999);

    assertThat(counter(registry, 4, LoadSheddingMeterData.ENVOY_RETRY_REJECTED).count())
        .isEqualTo(1.0);
    assertThat(counter(registry, 5, LoadSheddingMeterData.ENVOY_RETRY_REJECTED).count())
        .isEqualTo(1.0);
    assertThat(counter(registry, 5, LoadSheddingMeterData.ENVOY_RETRY_EXHAUSTED).count())
        .isEqualTo(1.0);
  }
}
