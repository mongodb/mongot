package com.xgen.mongot.index;

import static com.google.common.truth.Truth.assertThat;
import static com.xgen.mongot.index.SearchAggregationMetricsReporter.AGGREGATION_GROUP_TAG_PREFIX;
import static com.xgen.mongot.index.SearchAggregationMetricsReporter.AGGREGATION_METRIC_TAG_PREFIX;
import static com.xgen.mongot.index.SearchAggregationMetricsReporter.BUCKET_LIMIT_REJECTIONS_COUNTER;
import static com.xgen.mongot.index.SearchAggregationMetricsReporter.COLLECTION_LATENCY_TIMER;
import static com.xgen.mongot.index.SearchAggregationMetricsReporter.MAX_PATH_BUCKETS_HISTOGRAM;
import static com.xgen.mongot.index.SearchAggregationMetricsReporter.NESTING_HISTOGRAM;
import static com.xgen.mongot.index.SearchAggregationMetricsReporter.OBSERVED_BUCKETS_HISTOGRAM;
import static com.xgen.mongot.index.SearchAggregationMetricsReporter.OBSERVED_HEAP_BYTES_HISTOGRAM;
import static com.xgen.mongot.index.SearchAggregationMetricsReporter.PARTITION_MERGE_LATENCY_TIMER;
import static com.xgen.mongot.index.SearchAggregationMetricsReporter.REQUESTED_BUCKETS_HISTOGRAM;
import static org.junit.Assert.assertThrows;

import com.google.common.base.CaseFormat;
import com.xgen.mongot.index.IndexMetricsUpdater.QueryingMetricsUpdater.QueryFeaturesMetricsUpdater;
import com.xgen.mongot.index.definition.SearchIndexCapabilities;
import com.xgen.mongot.index.query.collectors.FacetDefinition;
import com.xgen.mongot.metrics.PerIndexMetricsFactory;
import com.xgen.mongot.util.Enums;
import com.xgen.testing.mongot.mock.index.SearchIndex;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.search.MeterNotFoundException;
import org.junit.Test;

public class SearchAggregationMetricsReporterTest {

  @Test
  public void queryingMetricsUpdater_timesCollection_recordsCollectionTimerOnly() {
    PerIndexMetricsFactory metricsFactory = SearchIndex.mockMetricsFactory();
    SearchAggregationMetricsReporter reporter =
        new IndexMetricsUpdater.QueryingMetricsUpdater(metricsFactory)
            .getSearchAggregationMetricsReporter();

    String result = reporter.time(SearchAggregationMetricsReporter.Phase.COLLECTION, () -> "ok");

    assertThat(result).isEqualTo("ok");
    assertThat(timerCount(metricsFactory, COLLECTION_LATENCY_TIMER)).isEqualTo(1);
    assertThat(timerCount(metricsFactory, PARTITION_MERGE_LATENCY_TIMER)).isEqualTo(0);
  }

  @Test
  public void queryingMetricsUpdater_timesPartitionMerge_recordsMergeTimerOnly() {
    PerIndexMetricsFactory metricsFactory = SearchIndex.mockMetricsFactory();
    SearchAggregationMetricsReporter reporter =
        new IndexMetricsUpdater.QueryingMetricsUpdater(metricsFactory)
            .getSearchAggregationMetricsReporter();

    reporter.time(SearchAggregationMetricsReporter.Phase.PARTITION_MERGE, () -> null);

    assertThat(timerCount(metricsFactory, COLLECTION_LATENCY_TIMER)).isEqualTo(0);
    assertThat(timerCount(metricsFactory, PARTITION_MERGE_LATENCY_TIMER)).isEqualTo(1);
  }

  @Test
  public void queryingMetricsUpdater_bodyThrows_doesNotRecordTimer() {
    PerIndexMetricsFactory metricsFactory = SearchIndex.mockMetricsFactory();
    SearchAggregationMetricsReporter reporter =
        new IndexMetricsUpdater.QueryingMetricsUpdater(metricsFactory)
            .getSearchAggregationMetricsReporter();

    assertThrows(
        IllegalStateException.class,
        () ->
            reporter.time(
                SearchAggregationMetricsReporter.Phase.COLLECTION,
                () -> {
                  throw new IllegalStateException("boom");
                }));

    assertThat(timerCount(metricsFactory, COLLECTION_LATENCY_TIMER)).isEqualTo(0);
  }

  @Test
  public void recordBucketLimitRejection_incrementsProcessCounter() {
    PerIndexMetricsFactory metricsFactory = SearchIndex.mockMetricsFactory();
    SearchAggregationMetricsReporter reporter =
        new IndexMetricsUpdater.QueryingMetricsUpdater(metricsFactory)
            .getSearchAggregationMetricsReporter();

    reporter.recordBucketLimitRejection();
    reporter.recordBucketLimitRejection();

    assertThat(
            metricsFactory
                .getProcessMetricsFactory()
                .get(BUCKET_LIMIT_REJECTIONS_COUNTER)
                .counter()
                .count())
        .isEqualTo(2.0);
  }

  @Test
  public void queryingMetricsUpdater_returnsSameReporterInstance() {
    var updater = new IndexMetricsUpdater.QueryingMetricsUpdater(SearchIndex.mockMetricsFactory());

    assertThat(updater.getSearchAggregationMetricsReporter())
        .isSameInstanceAs(updater.getSearchAggregationMetricsReporter());
  }

  @Test
  public void noop_runsBodyWithoutThrowing() {
    String result =
        SearchAggregationMetricsReporter.noop()
            .time(SearchAggregationMetricsReporter.Phase.COLLECTION, () -> "ok");

    assertThat(result).isEqualTo("ok");
  }

  @Test
  public void create_searchAggregationDisabled_doesNotRegisterTimersAndStillRunsBody() {
    PerIndexMetricsFactory metricsFactory = SearchIndex.mockMetricsFactory();
    SearchAggregationMetricsReporter reporter =
        new IndexMetricsUpdater.QueryingMetricsUpdater(
                metricsFactory,
                IndexTypeData.IndexTypeTag.TAG_SEARCH,
                SearchIndexCapabilities.CURRENT_FEATURE_VERSION,
                true,
                /* isSearchAggregationEnabled= */ false)
            .getSearchAggregationMetricsReporter();

    String result = reporter.time(SearchAggregationMetricsReporter.Phase.COLLECTION, () -> "ok");

    assertThat(result).isEqualTo("ok");
    assertThrows(
        MeterNotFoundException.class,
        () -> metricsFactory.getProcessMetricsFactory().get(COLLECTION_LATENCY_TIMER).timer());
    assertThrows(
        MeterNotFoundException.class,
        () -> metricsFactory.getProcessMetricsFactory().get(PARTITION_MERGE_LATENCY_TIMER).timer());
    assertThrows(
        MeterNotFoundException.class,
        () -> metricsFactory.getProcessMetricsFactory().get(REQUESTED_BUCKETS_HISTOGRAM).summary());
    assertThrows(
        MeterNotFoundException.class,
        () -> metricsFactory.getProcessMetricsFactory().get(MAX_PATH_BUCKETS_HISTOGRAM).summary());
    assertThrows(
        MeterNotFoundException.class,
        () -> metricsFactory.getProcessMetricsFactory().get(OBSERVED_BUCKETS_HISTOGRAM).summary());
    assertThrows(
        MeterNotFoundException.class,
        () ->
            metricsFactory.getProcessMetricsFactory().get(OBSERVED_HEAP_BYTES_HISTOGRAM).summary());
    assertThrows(
        MeterNotFoundException.class,
        () -> groupCounter(metricsFactory, FacetDefinition.Type.STRING));
    assertThrows(
        MeterNotFoundException.class,
        () ->
            metricCounter(metricsFactory, com.xgen.mongot.index.query.aggregation.Operator.COUNT));
    assertThrows(
        MeterNotFoundException.class,
        () -> metricsFactory.getProcessMetricsFactory().get(NESTING_HISTOGRAM).summary());
  }

  @Test
  public void recordGroupType_incrementsOnlyThatType() {
    PerIndexMetricsFactory metricsFactory = SearchIndex.mockMetricsFactory();
    SearchAggregationMetricsReporter reporter =
        new IndexMetricsUpdater.QueryingMetricsUpdater(metricsFactory)
            .getSearchAggregationMetricsReporter();

    reporter.recordGroupType(FacetDefinition.Type.STRING);
    reporter.recordGroupType(FacetDefinition.Type.STRING);
    reporter.recordGroupType(FacetDefinition.Type.NUMBER);

    assertThat(groupCounter(metricsFactory, FacetDefinition.Type.STRING)).isEqualTo(2.0);
    assertThat(groupCounter(metricsFactory, FacetDefinition.Type.NUMBER)).isEqualTo(1.0);
    assertThat(groupCounter(metricsFactory, FacetDefinition.Type.DATE)).isEqualTo(0.0);
  }

  @Test
  public void recordMetricType_incrementsOnlyThatType() {
    PerIndexMetricsFactory metricsFactory = SearchIndex.mockMetricsFactory();
    SearchAggregationMetricsReporter reporter =
        new IndexMetricsUpdater.QueryingMetricsUpdater(metricsFactory)
            .getSearchAggregationMetricsReporter();

    reporter.recordMetricType(com.xgen.mongot.index.query.aggregation.Operator.SUM);
    reporter.recordMetricType(com.xgen.mongot.index.query.aggregation.Operator.SUM);
    reporter.recordMetricType(com.xgen.mongot.index.query.aggregation.Operator.COUNT);

    assertThat(metricCounter(metricsFactory, com.xgen.mongot.index.query.aggregation.Operator.SUM))
        .isEqualTo(2.0);
    assertThat(
            metricCounter(metricsFactory, com.xgen.mongot.index.query.aggregation.Operator.COUNT))
        .isEqualTo(1.0);
    assertThat(metricCounter(metricsFactory, com.xgen.mongot.index.query.aggregation.Operator.AVG))
        .isEqualTo(0.0);
  }

  @Test
  public void recordNesting_recordsDepth() {
    PerIndexMetricsFactory metricsFactory = SearchIndex.mockMetricsFactory();
    SearchAggregationMetricsReporter reporter =
        new IndexMetricsUpdater.QueryingMetricsUpdater(metricsFactory)
            .getSearchAggregationMetricsReporter();

    reporter.recordNesting(2);

    assertThat(summaryMax(metricsFactory, NESTING_HISTOGRAM)).isEqualTo(2);
    assertThat(summaryCount(metricsFactory, NESTING_HISTOGRAM)).isEqualTo(1);
  }

  @Test
  public void recordRequestedBuckets_recordsTotalAndMaxPath() {
    PerIndexMetricsFactory metricsFactory = SearchIndex.mockMetricsFactory();
    SearchAggregationMetricsReporter reporter =
        new IndexMetricsUpdater.QueryingMetricsUpdater(metricsFactory)
            .getSearchAggregationMetricsReporter();

    reporter.recordRequestedBuckets(102, 100);

    assertThat(summaryMax(metricsFactory, REQUESTED_BUCKETS_HISTOGRAM)).isEqualTo(102);
    assertThat(summaryMax(metricsFactory, MAX_PATH_BUCKETS_HISTOGRAM)).isEqualTo(100);
    assertThat(summaryCount(metricsFactory, REQUESTED_BUCKETS_HISTOGRAM)).isEqualTo(1);
    assertThat(summaryCount(metricsFactory, MAX_PATH_BUCKETS_HISTOGRAM)).isEqualTo(1);
  }

  @Test
  public void recordObservedBuckets_recordsOneSample() {
    PerIndexMetricsFactory metricsFactory = SearchIndex.mockMetricsFactory();
    SearchAggregationMetricsReporter reporter =
        new IndexMetricsUpdater.QueryingMetricsUpdater(metricsFactory)
            .getSearchAggregationMetricsReporter();

    reporter.recordObservedBuckets(4200);

    assertThat(summaryMax(metricsFactory, OBSERVED_BUCKETS_HISTOGRAM)).isEqualTo(4200);
    assertThat(summaryCount(metricsFactory, OBSERVED_BUCKETS_HISTOGRAM)).isEqualTo(1);
  }

  @Test
  public void recordObservedHeapBytes_recordsOneSample() {
    PerIndexMetricsFactory metricsFactory = SearchIndex.mockMetricsFactory();
    SearchAggregationMetricsReporter reporter =
        new IndexMetricsUpdater.QueryingMetricsUpdater(metricsFactory)
            .getSearchAggregationMetricsReporter();

    reporter.recordObservedHeapBytes(1 << 20);

    assertThat(summaryMax(metricsFactory, OBSERVED_HEAP_BYTES_HISTOGRAM)).isEqualTo(1 << 20);
    assertThat(summaryCount(metricsFactory, OBSERVED_HEAP_BYTES_HISTOGRAM)).isEqualTo(1);
  }

  @Test
  public void noop_recordMethodsDoNotThrow() {
    SearchAggregationMetricsReporter.noop().recordGroupType(FacetDefinition.Type.STRING);
    SearchAggregationMetricsReporter.noop()
        .recordMetricType(com.xgen.mongot.index.query.aggregation.Operator.COUNT);
    SearchAggregationMetricsReporter.noop().recordNesting(1);
    SearchAggregationMetricsReporter.noop().recordRequestedBuckets(10, 5);
    SearchAggregationMetricsReporter.noop().recordObservedBuckets(7);
    SearchAggregationMetricsReporter.noop().recordObservedHeapBytes(1024);
  }

  private static long timerCount(PerIndexMetricsFactory metricsFactory, String name) {
    return metricsFactory.getProcessMetricsFactory().get(name).timer().count();
  }

  private static long summaryCount(PerIndexMetricsFactory metricsFactory, String name) {
    return metricsFactory.getProcessMetricsFactory().get(name).summary().count();
  }

  private static double summaryMax(PerIndexMetricsFactory metricsFactory, String name) {
    return metricsFactory.getProcessMetricsFactory().get(name).summary().max();
  }

  private static double groupCounter(
      PerIndexMetricsFactory metricsFactory, FacetDefinition.Type type) {
    return featureCounter(
        metricsFactory,
        AGGREGATION_GROUP_TAG_PREFIX + Enums.convertNameTo(CaseFormat.LOWER_CAMEL, type));
  }

  private static double metricCounter(
      PerIndexMetricsFactory metricsFactory,
      com.xgen.mongot.index.query.aggregation.Operator operator) {
    return featureCounter(
        metricsFactory,
        AGGREGATION_METRIC_TAG_PREFIX + Enums.convertNameTo(CaseFormat.LOWER_CAMEL, operator));
  }

  private static double featureCounter(PerIndexMetricsFactory metricsFactory, String nameTag) {
    return metricsFactory
        .getProcessMetricsFactory()
        .get(
            QueryFeaturesMetricsUpdater.METRIC_NAME,
            Tags.of(QueryFeaturesMetricsUpdater.NAME_TAG, nameTag))
        .counter()
        .count();
  }
}
