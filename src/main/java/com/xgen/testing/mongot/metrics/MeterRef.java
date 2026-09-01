package com.xgen.testing.mongot.metrics;

import com.xgen.mongot.metrics.MetricsFactory;

/**
 * A meter identified by the {@link MetricsFactory} it is registered in and its metric name.
 *
 * <p>Renders as the fully qualified name, so the subject description matches the {@code meter name}
 * fact of a failure.
 */
record MeterRef(MetricsFactory metricsFactory, String metricName) {
  @Override
  public String toString() {
    return "\"%s\"".formatted(this.metricsFactory.metricName(this.metricName));
  }
}
