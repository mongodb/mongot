package com.xgen.testing.mongot.metrics;

import static com.google.common.truth.Truth.assertAbout;

import com.xgen.mongot.metrics.MetricsFactory;
import io.micrometer.core.instrument.Tags;

/** Entry points for asserting on the counters registered through a {@link MetricsFactory}. */
public final class CounterSubjects {

  /**
   * Asserts on the single counter with the given name.
   *
   * <p>No tags are used unless they are defined using {@link
   * SingleCounterEmptyTags#withTags(Tags)}.
   */
  public static SingleCounterEmptyTags assertThatCounter(
      MetricsFactory metricsFactory, String metricName) {
    return assertAbout(SingleCounterSubject.factory())
        .that(new MeterRef(metricsFactory, metricName));
  }

  /**
   * Asserts on the set of counters with the given name.
   *
   * <p>All counters with the given name are considered unless they are filtered by tags using
   * {@link MultiCounterEmptyTags#containingTags(Tags)}.
   */
  public static MultiCounterEmptyTags assertThatCounters(
      MetricsFactory metricsFactory, String metricName) {
    return assertAbout(MultiCounterSubject.factory())
        .that(new MeterRef(metricsFactory, metricName));
  }

  private CounterSubjects() {}
}
