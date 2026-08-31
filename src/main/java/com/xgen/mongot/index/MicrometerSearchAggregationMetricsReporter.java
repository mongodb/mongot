package com.xgen.mongot.index;

import com.xgen.mongot.metrics.MetricsFactory;
import com.xgen.mongot.metrics.Timed;
import com.xgen.mongot.util.functionalinterfaces.CheckedSupplier;
import io.micrometer.core.instrument.Timer;
import java.util.concurrent.TimeUnit;

/**
 * Process-level Micrometer implementation of {@link SearchAggregationMetricsReporter}. Timer
 * registration happens in {@link #create}, not in the constructor.
 */
public final class MicrometerSearchAggregationMetricsReporter
    implements SearchAggregationMetricsReporter {

  private final Timer collectionTimer;
  private final Timer partitionMergeTimer;

  /**
   * Returns a no-op reporter when search aggregation is disabled so the process factory does not
   * export permanently-zero series. Otherwise registers the collection and partition-merge timers
   * and returns a recording reporter.
   */
  public static SearchAggregationMetricsReporter create(
      MetricsFactory processMetricsFactory, boolean searchAggregationEnabled) {
    if (!searchAggregationEnabled) {
      return SearchAggregationMetricsReporter.noop();
    }
    return new MicrometerSearchAggregationMetricsReporter(
        processMetricsFactory.timer(
            SearchAggregationMetricsReporter.COLLECTION_LATENCY_TIMER, TimeUnit.MILLISECONDS),
        processMetricsFactory.timer(
            SearchAggregationMetricsReporter.PARTITION_MERGE_LATENCY_TIMER, TimeUnit.MILLISECONDS));
  }

  private MicrometerSearchAggregationMetricsReporter(
      Timer collectionTimer, Timer partitionMergeTimer) {
    this.collectionTimer = collectionTimer;
    this.partitionMergeTimer = partitionMergeTimer;
  }

  @Override
  public <T, E extends Exception> T time(Phase phase, CheckedSupplier<T, E> body) throws E {
    return Timed.supplier(timerFor(phase), body);
  }

  private Timer timerFor(Phase phase) {
    return switch (phase) {
      case COLLECTION -> this.collectionTimer;
      case PARTITION_MERGE -> this.partitionMergeTimer;
    };
  }
}
