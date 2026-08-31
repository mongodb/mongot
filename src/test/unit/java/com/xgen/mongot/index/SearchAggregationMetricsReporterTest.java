package com.xgen.mongot.index;

import static com.google.common.truth.Truth.assertThat;
import static com.xgen.mongot.index.SearchAggregationMetricsReporter.COLLECTION_LATENCY_TIMER;
import static com.xgen.mongot.index.SearchAggregationMetricsReporter.PARTITION_MERGE_LATENCY_TIMER;
import static org.junit.Assert.assertThrows;

import com.xgen.mongot.index.definition.SearchIndexCapabilities;
import com.xgen.mongot.metrics.PerIndexMetricsFactory;
import com.xgen.testing.mongot.mock.index.SearchIndex;
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
  }

  private static long timerCount(PerIndexMetricsFactory metricsFactory, String name) {
    return metricsFactory.getProcessMetricsFactory().get(name).timer().count();
  }
}
