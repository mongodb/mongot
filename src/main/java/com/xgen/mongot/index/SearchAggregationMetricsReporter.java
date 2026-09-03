package com.xgen.mongot.index;

import com.xgen.mongot.index.query.aggregation.Operator;
import com.xgen.mongot.index.query.collectors.FacetDefinition;
import com.xgen.mongot.util.functionalinterfaces.CheckedSupplier;

/**
 * Process-level search-aggregation metrics. {@link IndexMetricsUpdater.QueryingMetricsUpdater}
 * creates the implementation and exposes it through {@link
 * IndexMetricsUpdater.QueryingMetricsUpdater#getSearchAggregationMetricsReporter()}. Callers record
 * through this type rather than holding individual Micrometer instruments.
 *
 * <p>Phase timings and the observed-bucket histogram are execution-time. Group-type and metric-type
 * counters, nesting, and requested-bucket histograms are plan-time, recorded once per aggregation
 * query from {@link QueryMetricsRecorder}.
 */
public interface SearchAggregationMetricsReporter {

  /** Process-level timer names registered when search aggregation is enabled. */
  String COLLECTION_LATENCY_TIMER = "aggregationCollectionLatencies";

  String PARTITION_MERGE_LATENCY_TIMER = "aggregationPartitionMergeLatencies";

  /**
   * Tag prefix for the grouping type of a search-aggregation facet node. Counters share the query
   * {@code feature} timeseries ({@code name=aggregation_group_<type>}).
   */
  String AGGREGATION_GROUP_TAG_PREFIX = "aggregation_group_";

  /**
   * Tag prefix for the metric type of a search-aggregation {@code aggregations} leaf. Counters
   * share the query {@code feature} timeseries ({@code name=aggregation_metric_<type>}).
   */
  String AGGREGATION_METRIC_TAG_PREFIX = "aggregation_metric_";

  /** Histogram of the nesting depth of a search-aggregation facet tree, one sample per query. */
  String NESTING_HISTOGRAM = "aggregationNestingPerQuery";

  /**
   * Histogram of a query's total requested bucket space: the product of {@code numBuckets} down
   * each root-to-leaf path, summed over paths. One sample per aggregation query.
   */
  String REQUESTED_BUCKETS_HISTOGRAM = "aggregationRequestedBucketsPerQuery";

  /**
   * Histogram of the single largest root-to-leaf requested bucket space in a query. Read against
   * {@link #REQUESTED_BUCKETS_HISTOGRAM}, which cannot tell ten paths of 100 buckets apart from one
   * runaway path of 1000.
   */
  String MAX_PATH_BUCKETS_HISTOGRAM = "aggregationMaxPathBucketsPerQuery";

  /**
   * Histogram of grouping buckets instantiated by one aggregation partition search, summed over
   * grouping nodes and search slices. Read against {@link #REQUESTED_BUCKETS_HISTOGRAM}: observed
   * can exceed requested by orders of magnitude when cardinality explodes.
   */
  String OBSERVED_BUCKETS_HISTOGRAM = "aggregationObservedBuckets";

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
   * Increments the process counter for {@code groupType}. Callers should invoke this once per
   * distinct grouping type in a query, not once per node, so the series counts queries using a
   * grouping type.
   */
  void recordGroupType(FacetDefinition.Type groupType);

  /**
   * Increments the process counter for {@code metricType}. Callers should invoke this once per
   * distinct metric type in a query, not once per leaf, so the series counts queries using a metric
   * type.
   */
  void recordMetricType(Operator metricType);

  /** Records the nesting depth of a single aggregation query's deepest facet tree. */
  void recordNesting(int depth);

  /**
   * Records a single aggregation query's requested bucket space: {@code requestedBuckets} summed
   * over root-to-leaf paths, and {@code maxPathBuckets} for the largest single path.
   */
  void recordRequestedBuckets(long requestedBuckets, long maxPathBuckets);

  /**
   * Records grouping buckets instantiated by one aggregation partition search. One sample per
   * successful collection, not per node: callers sum across grouping nodes and slices first.
   */
  void recordObservedBuckets(long observedBuckets);

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

    @Override
    public void recordGroupType(FacetDefinition.Type groupType) {
      // Intentionally empty: tests and disabled aggregations record nothing.
    }

    @Override
    public void recordMetricType(Operator metricType) {
      // Intentionally empty: tests and disabled aggregations record nothing.
    }

    @Override
    public void recordNesting(int depth) {
      // Intentionally empty: tests and disabled aggregations record nothing.
    }

    @Override
    public void recordRequestedBuckets(long requestedBuckets, long maxPathBuckets) {
      // Intentionally empty: tests and disabled aggregations record nothing.
    }

    @Override
    public void recordObservedBuckets(long observedBuckets) {
      // Intentionally empty: tests and disabled aggregations record nothing.
    }
  }
}
