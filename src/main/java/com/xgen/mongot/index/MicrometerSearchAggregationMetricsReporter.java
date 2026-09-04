package com.xgen.mongot.index;

import com.google.common.base.CaseFormat;
import com.xgen.mongot.index.IndexMetricsUpdater.QueryingMetricsUpdater.QueryFeaturesMetricsUpdater;
import com.xgen.mongot.index.query.aggregation.Operator;
import com.xgen.mongot.index.query.collectors.FacetDefinition;
import com.xgen.mongot.metrics.MetricsFactory;
import com.xgen.mongot.metrics.Timed;
import com.xgen.mongot.util.Enums;
import com.xgen.mongot.util.functionalinterfaces.CheckedSupplier;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Process-level Micrometer implementation of {@link SearchAggregationMetricsReporter}. Instrument
 * registration happens in {@link #create}, not in the constructor.
 */
public final class MicrometerSearchAggregationMetricsReporter
    implements SearchAggregationMetricsReporter {

  /**
   * Shared bounds for both requested-bucket histograms. Wider than {@code
   * totalFacetBucketsPerQuery}: a nested aggregation's ask is a product down each path, so it
   * reaches values a flat facet list cannot.
   *
   * <p>Micrometer SLO histograms always include a {@code +Inf} bucket, so samples above 1M
   * (including saturated {@code Long.MAX_VALUE}) are counted there rather than in the 1M band.
   */
  private static final double[] REQUESTED_BUCKETS_BOUNDS = {
    10, 100, 1000, 10_000, 100_000, 1_000_000
  };

  private static final double[] NESTING_BOUNDS = {1, 2, 3, 4};

  /**
   * Same first six bands as the requested-bucket histograms so the two line up on a dashboard; two
   * extra decades because observed can exceed requested by orders of magnitude.
   */
  private static final double[] OBSERVED_BUCKETS_BOUNDS = {
    10, 100, 1000, 10_000, 100_000, 1_000_000, 10_000_000, 100_000_000
  };

  private final Timer collectionTimer;
  private final Timer partitionMergeTimer;
  private final Map<FacetDefinition.Type, Counter> groupCounters;
  private final Map<Operator, Counter> metricCounters;
  private final DistributionSummary nestingHistogram;
  private final DistributionSummary requestedBucketsHistogram;
  private final DistributionSummary maxPathBucketsHistogram;
  private final Counter bucketLimitRejectionsCounter;
  private final DistributionSummary observedBucketsHistogram;

  /**
   * Returns a no-op reporter when search aggregation is disabled so the process factory does not
   * export permanently-zero series. Otherwise registers timers, group-type and metric-type
   * counters, nesting, requested-bucket, and observed-bucket histograms.
   */
  public static SearchAggregationMetricsReporter create(
      MetricsFactory processMetricsFactory, boolean searchAggregationEnabled) {
    if (!searchAggregationEnabled) {
      return SearchAggregationMetricsReporter.noop();
    }
    return new MicrometerSearchAggregationMetricsReporter(
        processMetricsFactory.timer(COLLECTION_LATENCY_TIMER, TimeUnit.MILLISECONDS),
        processMetricsFactory.timer(PARTITION_MERGE_LATENCY_TIMER, TimeUnit.MILLISECONDS),
        featureCounters(
            processMetricsFactory, FacetDefinition.Type.class, AGGREGATION_GROUP_TAG_PREFIX),
        featureCounters(processMetricsFactory, Operator.class, AGGREGATION_METRIC_TAG_PREFIX),
        processMetricsFactory.histogram(NESTING_HISTOGRAM, NESTING_BOUNDS),
        processMetricsFactory.histogram(REQUESTED_BUCKETS_HISTOGRAM, REQUESTED_BUCKETS_BOUNDS),
        processMetricsFactory.histogram(MAX_PATH_BUCKETS_HISTOGRAM, REQUESTED_BUCKETS_BOUNDS),
        processMetricsFactory.counter(BUCKET_LIMIT_REJECTIONS_COUNTER),
        processMetricsFactory.histogram(OBSERVED_BUCKETS_HISTOGRAM, OBSERVED_BUCKETS_BOUNDS));
  }

  private MicrometerSearchAggregationMetricsReporter(
      Timer collectionTimer,
      Timer partitionMergeTimer,
      Map<FacetDefinition.Type, Counter> groupCounters,
      Map<Operator, Counter> metricCounters,
      DistributionSummary nestingHistogram,
      DistributionSummary requestedBucketsHistogram,
      DistributionSummary maxPathBucketsHistogram,
      Counter bucketLimitRejectionsCounter,
      DistributionSummary observedBucketsHistogram) {
    this.collectionTimer = collectionTimer;
    this.partitionMergeTimer = partitionMergeTimer;
    this.groupCounters = groupCounters;
    this.metricCounters = metricCounters;
    this.nestingHistogram = nestingHistogram;
    this.requestedBucketsHistogram = requestedBucketsHistogram;
    this.maxPathBucketsHistogram = maxPathBucketsHistogram;
    this.bucketLimitRejectionsCounter = bucketLimitRejectionsCounter;
    this.observedBucketsHistogram = observedBucketsHistogram;
  }

  @Override
  public <T, E extends Exception> T time(Phase phase, CheckedSupplier<T, E> body) throws E {
    return Timed.supplier(timerFor(phase), body);
  }

  @Override
  public void recordGroupType(FacetDefinition.Type groupType) {
    this.groupCounters.get(groupType).increment();
  }

  @Override
  public void recordMetricType(Operator metricType) {
    this.metricCounters.get(metricType).increment();
  }

  @Override
  public void recordNesting(int depth) {
    this.nestingHistogram.record(depth);
  }

  @Override
  public void recordRequestedBuckets(long requestedBuckets, long maxPathBuckets) {
    this.requestedBucketsHistogram.record(requestedBuckets);
    this.maxPathBucketsHistogram.record(maxPathBuckets);
  }

  @Override
  public void recordBucketLimitRejection() {
    this.bucketLimitRejectionsCounter.increment();
  }

  @Override
  public void recordObservedBuckets(long observedBuckets) {
    this.observedBucketsHistogram.record(observedBuckets);
  }

  private Timer timerFor(Phase phase) {
    return switch (phase) {
      case COLLECTION -> this.collectionTimer;
      case PARTITION_MERGE -> this.partitionMergeTimer;
    };
  }

  private static <T extends Enum<T>> Map<T, Counter> featureCounters(
      MetricsFactory processMetricsFactory, Class<T> enumClass, String tagPrefix) {
    Map<T, Counter> counters = new EnumMap<>(enumClass);
    for (T value : enumClass.getEnumConstants()) {
      counters.put(
          value,
          processMetricsFactory.counter(
              QueryFeaturesMetricsUpdater.METRIC_NAME,
              Tags.of(
                  QueryFeaturesMetricsUpdater.NAME_TAG,
                  tagPrefix + Enums.convertNameTo(CaseFormat.LOWER_CAMEL, value))));
    }
    return counters;
  }
}
