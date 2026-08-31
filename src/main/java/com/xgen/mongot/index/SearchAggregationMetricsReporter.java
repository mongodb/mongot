package com.xgen.mongot.index;

import com.xgen.mongot.util.functionalinterfaces.CheckedSupplier;

/**
 * Process-level search-aggregation phase timings. {@link
 * IndexMetricsUpdater.QueryingMetricsUpdater} creates the implementation and exposes it through
 * {@link IndexMetricsUpdater.QueryingMetricsUpdater#getSearchAggregationMetricsReporter()}; callers
 * time phases through this type rather than holding individual Micrometer instruments.
 */
public interface SearchAggregationMetricsReporter {

  /** Process-level timer names registered when search aggregation is enabled. */
  String COLLECTION_LATENCY_TIMER = "aggregationCollectionLatencies";

  String PARTITION_MERGE_LATENCY_TIMER = "aggregationPartitionMergeLatencies";

  enum Phase {
    COLLECTION,
    PARTITION_MERGE
  }

  /**
   * Runs {@code body} and, on success, records its wall time on the process timer for {@code
   * phase}. A thrown exception is propagated and does not record a sample, matching {@link
   * com.xgen.mongot.metrics.Timed}.
   */
  <T, E extends Exception> T time(Phase phase, CheckedSupplier<T, E> body) throws E;

  /**
   * Reporter that runs {@code body} and records nothing. Useful in tests that do not assert
   * metrics.
   */
  static SearchAggregationMetricsReporter noop() {
    return Noop.INSTANCE;
  }

  enum Noop implements SearchAggregationMetricsReporter {
    INSTANCE;

    @Override
    public <T, E extends Exception> T time(Phase phase, CheckedSupplier<T, E> body) throws E {
      return body.get();
    }
  }
}
