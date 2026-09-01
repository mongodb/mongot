package com.xgen.mongot.index.lucene;

import com.xgen.mongot.index.SearchAggregationMetricsReporter;
import com.xgen.mongot.index.lucene.explain.explainers.AggregationFeatureExplainer;
import com.xgen.mongot.index.lucene.explain.timing.ExplainTimings;
import com.xgen.mongot.index.query.aggregation.Operator;
import com.xgen.mongot.index.query.collectors.FacetDefinition;
import com.xgen.mongot.util.functionalinterfaces.CheckedSupplier;
import com.xgen.mongot.util.timers.InvocationCountingTimer;

/**
 * Decorates a process-metrics {@link SearchAggregationMetricsReporter} so one {@link #time} call
 * feeds both Micrometer and the query's aggregation explain section. Explain recording is a no-op
 * when the query is not being explained.
 */
final class ExplainAwareSearchAggregationMetricsReporter
    implements SearchAggregationMetricsReporter {

  private final SearchAggregationMetricsReporter delegate;

  private ExplainAwareSearchAggregationMetricsReporter(SearchAggregationMetricsReporter delegate) {
    this.delegate = delegate;
  }

  static SearchAggregationMetricsReporter wrap(SearchAggregationMetricsReporter delegate) {
    if (delegate instanceof ExplainAwareSearchAggregationMetricsReporter) {
      return delegate;
    }
    return new ExplainAwareSearchAggregationMetricsReporter(delegate);
  }

  @Override
  public <T, E extends Exception> T time(Phase phase, CheckedSupplier<T, E> body) throws E {
    try (var unused =
        new InvocationCountingTimer.AutocloseableOptional<>(
            AggregationFeatureExplainer.currentPhaseProfile()
                .map(t -> t.split(explainType(phase))))) {
      return this.delegate.time(phase, body);
    }
  }

  @Override
  public void recordGroupType(FacetDefinition.Type groupType) {
    this.delegate.recordGroupType(groupType);
  }

  @Override
  public void recordMetricType(Operator metricType) {
    this.delegate.recordMetricType(metricType);
  }

  @Override
  public void recordNesting(int depth) {
    this.delegate.recordNesting(depth);
  }

  @Override
  public void recordRequestedBuckets(long requestedBuckets, long maxPathBuckets) {
    this.delegate.recordRequestedBuckets(requestedBuckets, maxPathBuckets);
  }

  private static ExplainTimings.Type explainType(Phase phase) {
    return switch (phase) {
      case COLLECTION -> ExplainTimings.Type.AGGREGATION_COLLECT;
      case PARTITION_MERGE -> ExplainTimings.Type.AGGREGATION_MERGE;
    };
  }
}
