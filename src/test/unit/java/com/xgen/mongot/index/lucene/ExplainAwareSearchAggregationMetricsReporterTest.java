package com.xgen.mongot.index.lucene;

import static com.google.common.truth.Truth.assertThat;
import static com.xgen.mongot.index.SearchAggregationMetricsReporter.COLLECTION_LATENCY_TIMER;
import static org.junit.Assert.assertThrows;

import com.xgen.mongot.index.IndexMetricsUpdater;
import com.xgen.mongot.index.SearchAggregationMetricsReporter;
import com.xgen.mongot.index.lucene.explain.tracing.Explain;
import com.xgen.mongot.metrics.PerIndexMetricsFactory;
import com.xgen.testing.mongot.mock.index.SearchIndex;
import java.util.Optional;
import org.junit.Test;

public class ExplainAwareSearchAggregationMetricsReporterTest {

  @Test
  public void wrap_alreadyWrapped_returnsSameInstance() {
    SearchAggregationMetricsReporter wrapped =
        ExplainAwareSearchAggregationMetricsReporter.wrap(SearchAggregationMetricsReporter.noop());

    assertThat(ExplainAwareSearchAggregationMetricsReporter.wrap(wrapped))
        .isSameInstanceAs(wrapped);
  }

  @Test
  public void time_withoutExplain_recordsProcessTimerOnly() {
    PerIndexMetricsFactory metricsFactory = SearchIndex.mockMetricsFactory();
    SearchAggregationMetricsReporter reporter =
        ExplainAwareSearchAggregationMetricsReporter.wrap(
            new IndexMetricsUpdater.QueryingMetricsUpdater(metricsFactory)
                .getSearchAggregationMetricsReporter());

    reporter.time(SearchAggregationMetricsReporter.Phase.COLLECTION, () -> "ok");

    assertThat(timerCount(metricsFactory, COLLECTION_LATENCY_TIMER)).isEqualTo(1);
  }

  @Test
  public void time_withExplain_recordsCollectExplainAndProcessTimer() throws Exception {
    PerIndexMetricsFactory metricsFactory = SearchIndex.mockMetricsFactory();
    SearchAggregationMetricsReporter reporter =
        ExplainAwareSearchAggregationMetricsReporter.wrap(
            new IndexMetricsUpdater.QueryingMetricsUpdater(metricsFactory)
                .getSearchAggregationMetricsReporter());

    try (var unused =
        Explain.setup(Optional.of(Explain.Verbosity.EXECUTION_STATS), Optional.empty())) {
      reporter.time(SearchAggregationMetricsReporter.Phase.COLLECTION, () -> "ok");

      var aggregation = Explain.collect().orElseThrow().aggregation().orElseThrow();
      assertThat(aggregation.collect()).isPresent();
      assertThat(aggregation.partitionMerge()).isEmpty();
    }

    assertThat(timerCount(metricsFactory, COLLECTION_LATENCY_TIMER)).isEqualTo(1);
  }

  @Test
  public void time_withExplain_recordsPartitionMergeExplain() throws Exception {
    SearchAggregationMetricsReporter reporter =
        ExplainAwareSearchAggregationMetricsReporter.wrap(SearchAggregationMetricsReporter.noop());

    try (var unused =
        Explain.setup(Optional.of(Explain.Verbosity.EXECUTION_STATS), Optional.empty())) {
      reporter.time(SearchAggregationMetricsReporter.Phase.PARTITION_MERGE, () -> null);

      var aggregation = Explain.collect().orElseThrow().aggregation().orElseThrow();
      assertThat(aggregation.partitionMerge()).isPresent();
      assertThat(aggregation.collect()).isEmpty();
    }
  }

  @Test
  public void time_bodyThrows_recordsExplainButNotProcessTimer() throws Exception {
    PerIndexMetricsFactory metricsFactory = SearchIndex.mockMetricsFactory();
    SearchAggregationMetricsReporter reporter =
        ExplainAwareSearchAggregationMetricsReporter.wrap(
            new IndexMetricsUpdater.QueryingMetricsUpdater(metricsFactory)
                .getSearchAggregationMetricsReporter());

    try (var unused =
        Explain.setup(Optional.of(Explain.Verbosity.EXECUTION_STATS), Optional.empty())) {
      assertThrows(
          IllegalStateException.class,
          () ->
              reporter.time(
                  SearchAggregationMetricsReporter.Phase.COLLECTION,
                  () -> {
                    throw new IllegalStateException("boom");
                  }));

      var aggregation = Explain.collect().orElseThrow().aggregation().orElseThrow();
      assertThat(aggregation.collect()).isPresent();
    }

    assertThat(timerCount(metricsFactory, COLLECTION_LATENCY_TIMER)).isEqualTo(0);
  }

  private static long timerCount(PerIndexMetricsFactory metricsFactory, String name) {
    return metricsFactory.getProcessMetricsFactory().get(name).timer().count();
  }
}
