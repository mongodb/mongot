package com.xgen.mongot.index;

import static com.google.common.truth.Truth.assertThat;
import static com.xgen.mongot.index.IndexMetricsUpdater.QueryingMetricsUpdater.QueryFeaturesMetricsUpdater.KNN_BETA_FILTER_TAG_PREFIX;
import static com.xgen.mongot.index.IndexMetricsUpdater.QueryingMetricsUpdater.QueryFeaturesMetricsUpdater.SEARCH_VECTOR_SEARCH_FILTER_TAG_PREFIX;
import static com.xgen.mongot.index.IndexMetricsUpdater.QueryingMetricsUpdater.QueryFeaturesMetricsUpdater.SORT_NO_DATA_NAME_TAG_PREFIX;
import static com.xgen.mongot.index.IndexTypeData.INDEX_TYPE_TAG_NAME;
import static com.xgen.mongot.index.IndexTypeData.IndexTypeTag.TAG_VECTOR_SEARCH_AUTO_EMBEDDING;
import static com.xgen.testing.TestUtils.assertThrows;
import static com.xgen.testing.mongot.mock.index.SearchIndex.mockAutoEmbeddingVectorSearchDefinition;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

import com.google.common.base.CaseFormat;
import com.xgen.mongot.index.IndexMetricsUpdater.IndexingMetricsUpdater;
import com.xgen.mongot.index.definition.SearchIndexCapabilities;
import com.xgen.mongot.index.definition.VectorIndexCapabilities;
import com.xgen.mongot.index.query.collectors.Collector;
import com.xgen.mongot.index.query.operators.Operator;
import com.xgen.mongot.index.query.operators.TextOperator;
import com.xgen.mongot.index.query.operators.VectorSearchCriteria;
import com.xgen.mongot.index.query.scores.Score;
import com.xgen.mongot.index.query.sort.NullEmptySortPosition;
import com.xgen.mongot.index.status.IndexStatus;
import com.xgen.mongot.metrics.MeterAndFtdcRegistry;
import com.xgen.mongot.metrics.PerIndexMetricsFactory;
import com.xgen.mongot.util.Enums;
import com.xgen.testing.mongot.index.IndexMetricsBuilder;
import com.xgen.testing.mongot.index.IndexMetricsUpdaterBuilder;
import com.xgen.testing.mongot.index.definition.SearchIndexDefinitionBuilder;
import com.xgen.testing.mongot.index.version.GenerationIdBuilder;
import com.xgen.testing.mongot.metrics.micrometer.PercentilesBuilder;
import com.xgen.testing.mongot.metrics.micrometer.SerializableTimerBuilder;
import com.xgen.testing.mongot.mock.index.IndexMetricsSupplier;
import com.xgen.testing.mongot.mock.index.SearchIndex;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.search.MeterNotFoundException;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.bson.BsonTimestamp;
import org.bson.types.ObjectId;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.mockito.Mockito;

@RunWith(Suite.class)
@Suite.SuiteClasses(
    value = {
      TestIndexMetricsUpdater.TestClass.class,
      TestIndexMetricsUpdater.TestClass.TestIndexingMetricsUpdater.class,
      TestIndexMetricsUpdater.TestClass.TestQueryingMetricsUpdater.class,
      TestIndexMetricsUpdater.TestClass.TestQueryingMetricsUpdater.TestQueryFeaturesMetricsUpdater
          .class,
    })
public class TestIndexMetricsUpdater {
  private static PerIndexMetricsFactory mockMetricsFactory(String namespace) {
    return new PerIndexMetricsFactory(
        namespace, MeterAndFtdcRegistry.createWithSimpleRegistries(), GenerationIdBuilder.create());
  }

  public static class TestClass {
    @Test
    public void testIndexMetrics() {
      var indexDefinition =
          SearchIndexDefinitionBuilder.builder().defaultMetadata().dynamicMapping().build();
      var mockedIndexMetricsSupplier = IndexMetricsSupplier.mockEmptyIndexMetricsSupplier();
      var indexMetricsUpdater =
          IndexMetricsUpdaterBuilder.builder()
              .metricsFactory(mockMetricsFactory(IndexMetricsUpdater.NAMESPACE))
              .indexDefinition(indexDefinition)
              .indexMetricsSupplier(mockedIndexMetricsSupplier)
              .build();

      var mockedIndexMetricsUpdater = spy(indexMetricsUpdater);
      var mockedIndexingMetricsUpdater = spy(indexMetricsUpdater.getIndexingMetricsUpdater());
      Mockito.doReturn(new ReplicationOpTimeInfo(10L, 20L, 10L))
          .when(mockedIndexingMetricsUpdater)
          .getReplicationOpTimeInfo();
      Mockito.doReturn(mockedIndexingMetricsUpdater)
          .when(mockedIndexMetricsUpdater)
          .getIndexingMetricsUpdater();

      mockedIndexMetricsUpdater.getQueryingMetricsUpdater().getFailedQueryCounter().increment();

      var queryMetrics =
          IndexMetricsBuilder.QueryingMetricsBuilder.builder()
              .failedQueryCount(1)
              .searchResultBatchLatency(
                  SerializableTimerBuilder.builder()
                      .timeUnit(TimeUnit.MILLISECONDS)
                      .percentiles(PercentilesBuilder.builder().zeroPercentiles().build())
                      .build())
              .tokenFacetsStateRefresh(
                  SerializableTimerBuilder.builder()
                      .timeUnit(TimeUnit.MILLISECONDS)
                      .percentiles(PercentilesBuilder.builder().zeroPercentiles().build())
                      .build())
              .build();

      var expected =
          IndexMetricsBuilder.builder()
              .indexDefinition(indexDefinition)
              .indexingMetrics(
                  IndexMetricsBuilder.IndexingMetricsBuilder.builder()
                      .replicationLagMs(10)
                      .replicationOpTime(new BsonTimestamp(10L))
                      .build())
              .queryingMetrics(queryMetrics)
              .build();
      var result = mockedIndexMetricsUpdater.getMetrics();
      Assert.assertEquals(expected, result);
    }

    public static class TestIndexingMetricsUpdater {
      @Test
      public void testIndexingMetricsSuppliers() {
        var mockedIndexMetricsSupplier = mock(IndexMetricValuesSupplier.class);
        Mockito.doReturn(new DocCounts(25, 24, 17, 25L))
            .when(mockedIndexMetricsSupplier)
            .getDocCounts();
        Mockito.doReturn(2).when(mockedIndexMetricsSupplier).getNumFields();
        Mockito.doAnswer((ignored) -> IndexStatus.steady())
            .when(mockedIndexMetricsSupplier)
            .getIndexStatus();
        Mockito.doReturn(1000L).when(mockedIndexMetricsSupplier).computeIndexSize();
        Mockito.doReturn(500L).when(mockedIndexMetricsSupplier).getRequiredMemoryForVectorData();

        var indexingMetricsUpdater =
            IndexMetricsUpdaterBuilder.IndexingMetricsUpdaterBuilder.builder()
                .metricsFactory(mockMetricsFactory(IndexingMetricsUpdater.NAMESPACE))
                .build();
        var mockedIndexingMetricsUpdater = spy(indexingMetricsUpdater);
        Mockito.doReturn(new ReplicationOpTimeInfo(10L, 20L, 10L))
            .when(mockedIndexingMetricsUpdater)
            .getReplicationOpTimeInfo();

        var result = mockedIndexingMetricsUpdater.getMetrics(mockedIndexMetricsSupplier);
        var expected =
            IndexMetricsBuilder.IndexingMetricsBuilder.builder()
                .replicationOpTime(new BsonTimestamp(10L))
                .replicationLagMs(10L)
                .numDocs(25)
                .numLuceneMaxDocs(24)
                .maxLuceneMaxDocs(17)
                .numMongoDbDocs(25)
                .indexSize(1000)
                .requiredMemory(500)
                .numFields(2)
                .build();

        Assert.assertEquals(expected, result);
      }

      @Test
      public void testReplicationLagPresentScenario() {
        var factory = mockMetricsFactory(IndexingMetricsUpdater.NAMESPACE);
        var indexingMetricsUpdater =
            IndexMetricsUpdaterBuilder.IndexingMetricsUpdaterBuilder.builder()
                .metricsFactory(factory)
                .build();

        indexingMetricsUpdater
            .getReplicationOpTimeInfo()
            .update(new BsonTimestamp(1, 0).getValue(), new BsonTimestamp(2, 0).getValue());

        Assert.assertTrue(
            indexingMetricsUpdater
                .getMetrics(IndexMetricsSupplier.mockEmptyIndexMetricsSupplier())
                .replicationLagMs()
                .isPresent());
        // it should also be reported to the meter registry
        Gauge gauge = factory.get(IndexingMetricsUpdater.REPLICATION_LAG_MILLISECONDS_NAME).gauge();
        Assert.assertEquals(1000, gauge.value(), 0.1d);
      }

      @Test
      public void testReplicationLagMetricsSetAndUnset() {
        var factory = mockMetricsFactory(IndexingMetricsUpdater.NAMESPACE);
        var indexingMetricsUpdater =
            IndexMetricsUpdaterBuilder.IndexingMetricsUpdaterBuilder.builder()
                .metricsFactory(factory)
                .build();

        indexingMetricsUpdater
            .getReplicationOpTimeInfo()
            .update(new BsonTimestamp(1, 0).getValue(), new BsonTimestamp(2, 0).getValue());
        indexingMetricsUpdater.getReplicationOpTimeInfo().unset(() -> true);
        Assert.assertTrue(indexingMetricsUpdater.getReplicationOpTimeInfo().snapshot().isEmpty());

        // should report as 0 to meter registry if its not present
        Gauge gauge = factory.get(IndexingMetricsUpdater.REPLICATION_LAG_MILLISECONDS_NAME).gauge();
        Assert.assertEquals(Double.NaN, gauge.value(), 0.1d);

        indexingMetricsUpdater.getReplicationOpTimeInfo().update(20L, 40L);
        Assert.assertTrue(indexingMetricsUpdater.getReplicationOpTimeInfo().snapshot().isPresent());
      }

      @Test
      public void testIndexFeatureVersionFourEnabledTags() {
        var metricsFactory = SearchIndex.mockMetricsFactory();
        var indexingMetricsUpdater =
            IndexMetricsUpdaterBuilder.IndexingMetricsUpdaterBuilder.builder()
                .metricsFactory(metricsFactory)
                .build();
        indexingMetricsUpdater.getInitialSyncExceptionCounter().increment();
        var initialSyncExceptions =
            metricsFactory.get("initialSyncExceptions").tag("indexFeatureVersion", "4").counter();
        assertThat(initialSyncExceptions.count()).isEqualTo(1);

        indexingMetricsUpdater.getSteadyStateExceptionCounter().increment();
        var steadyStateExceptions =
            metricsFactory.get("steadyStateExceptions").tag("indexFeatureVersion", "4").counter();
        assertThat(steadyStateExceptions.count()).isEqualTo(1);
      }

      @Test
      public void testReplicationOpTimeInfoGauges() {
        var factory = mockMetricsFactory(IndexingMetricsUpdater.NAMESPACE);
        var indexingMetricsUpdater =
            IndexMetricsUpdaterBuilder.IndexingMetricsUpdaterBuilder.builder()
                .metricsFactory(factory)
                .build();

        ReplicationOpTimeInfo.Snapshot opTimeInfo =
            new ReplicationOpTimeInfo(
                    new BsonTimestamp(1, 0).getValue(), new BsonTimestamp(2, 0).getValue())
                .snapshotOrThrow();

        indexingMetricsUpdater
            .getReplicationOpTimeInfo()
            .update(opTimeInfo.replicationOpTime(), opTimeInfo.maxPossibleReplicationOpTime());
        Assert.assertEquals(
            opTimeInfo.replicationOpTime(), factory.get("replicationOpTime").gauge().value(), 0.0);
        Assert.assertEquals(
            opTimeInfo.maxPossibleReplicationOpTime(),
            factory.get("maxPossibleReplicationOpTime").gauge().value(),
            0.0);
        Assert.assertEquals(
            opTimeInfo.replicationLagMs(), factory.get("replicationLagMs").gauge().value(), 0.0);
      }

      @Test
      public void testRecordDocumentSizeBytes() {
        int mb = 1024 * 1024;
        var metricsFactory = mockMetricsFactory(IndexingMetricsUpdater.NAMESPACE);
        IndexingMetricsUpdater metricsUpdater =
            IndexMetricsUpdaterBuilder.IndexingMetricsUpdaterBuilder.builder()
                .metricsFactory(metricsFactory)
                .build();

        metricsUpdater.recordDocumentSizeBytes(11 * mb);
        IndexingMetricsUpdater.CHANGE_STREAM_EVENT_SIZE_THRESHOLDS.forEach(
            threshold -> {
              if (threshold.toMebi() < 11) {
                Assert.assertEquals(
                    1,
                    metricsFactory
                        .get(IndexingMetricsUpdater.LARGE_CHANGE_STREAM_EVENTS)
                        .tag("threshold", threshold.toMebi() + "MiB")
                        .counter()
                        .count(),
                    0);
              } else {
                Assert.assertEquals(
                    0,
                    metricsFactory
                        .get(IndexingMetricsUpdater.LARGE_CHANGE_STREAM_EVENTS)
                        .tag("threshold", threshold.toMebi() + "MiB")
                        .counter()
                        .count(),
                    0);
              }
            });
      }

      @Test
      public void testIncDocumentEventTypeCount() {
        var metricsFactory = mockMetricsFactory(IndexingMetricsUpdater.NAMESPACE);
        IndexingMetricsUpdater metricsUpdater =
            IndexMetricsUpdaterBuilder.IndexingMetricsUpdaterBuilder.builder()
                .metricsFactory(metricsFactory)
                .build();

        metricsUpdater.getDocumentEventTypeCounter(DocumentEvent.EventType.INSERT).increment();
        Assert.assertEquals(
            1,
            metricsFactory
                .get(Enums.convertNameTo(CaseFormat.LOWER_CAMEL, DocumentEvent.EventType.INSERT))
                .counter()
                .count(),
            0);
      }
    }

    public static class TestQueryingMetricsUpdater {
      @Test
      public void testQueryingMetrics() {
        var metricsFactory = SearchIndex.mockMetricsFactory();
        var queryingMetricsUpdater = new IndexMetricsUpdater.QueryingMetricsUpdater(metricsFactory);
        queryingMetricsUpdater.getFailedQueryCounter().increment();

        var result = queryingMetricsUpdater.getMetrics();
        var expected =
            IndexMetricsBuilder.QueryingMetricsBuilder.builder().failedQueryCount(1).build();

        Assert.assertEquals(expected, result);
      }

      @Test
      public void testIndexFeatureVersionFourEnabledTags() {
        var metricsFactory = SearchIndex.mockMetricsFactory();
        var queryingMetricsUpdater = new IndexMetricsUpdater.QueryingMetricsUpdater(metricsFactory);
        queryingMetricsUpdater.getFailedQueryCounter().increment();
        var failedQueriesCounter =
            metricsFactory.get("failedQueries").tag("indexFeatureVersion", "4").counter();
        assertThat(failedQueriesCounter.count()).isEqualTo(1);

        queryingMetricsUpdater.getTotalQueryCounter().increment();
        var totalQueryCounter =
            metricsFactory.get("totalQueries").tag("indexFeatureVersion", "4").counter();
        assertThat(totalQueryCounter.count()).isEqualTo(1);

        queryingMetricsUpdater.getInternallyFailedQueryCounter().increment();
        var internallyFailedQueryCounter =
            metricsFactory.get("internallyFailedQueries").tag("indexFeatureVersion", "4").counter();
        assertThat(internallyFailedQueryCounter.count()).isEqualTo(1);

        queryingMetricsUpdater.getSearchResultBatchLatencyTimer().record(Duration.ofMillis(100));
        var searchBatchLatencies =
            metricsFactory
                .get("searchResultBatchLatencies")
                .tag("indexFeatureVersion", "4")
                .timer();
        assertThat(searchBatchLatencies.totalTime(TimeUnit.MILLISECONDS)).isEqualTo(100);

        var vectorIndexQueryingMetricsUpdater =
            new IndexMetricsUpdater.QueryingMetricsUpdater(
                metricsFactory,
                IndexTypeData.IndexTypeTag.TAG_VECTOR_SEARCH,
                VectorIndexCapabilities.CURRENT_FEATURE_VERSION,
                true);
        vectorIndexQueryingMetricsUpdater
            .getVectorResultLatencyTimer()
            .record(Duration.ofMillis(100));
        var vectorBatchLatencies =
            metricsFactory.get("vectorResultLatencies").tag("indexFeatureVersion", "4").timer();
        assertThat(vectorBatchLatencies.totalTime(TimeUnit.MILLISECONDS)).isEqualTo(100);
      }

      @Test
      public void testIndexFeatureVersionFourDisabledTags() {
        var metricsFactory = SearchIndex.mockMetricsFactory();
        var queryingMetricsUpdater =
            new IndexMetricsUpdater.QueryingMetricsUpdater(
                metricsFactory,
                IndexTypeData.IndexTypeTag.TAG_SEARCH,
                SearchIndexCapabilities.CURRENT_FEATURE_VERSION,
                false);
        queryingMetricsUpdater.getFailedQueryCounter().increment();
        assertThrows(
            "No meters have the required tag 'indexFeatureVersion'.",
            MeterNotFoundException.class,
            () -> metricsFactory.get("failedQueries").tag("indexFeatureVersion", "4").counter());

        queryingMetricsUpdater.getTotalQueryCounter().increment();
        assertThrows(
            "No meters have the required tag 'indexFeatureVersion'.",
            MeterNotFoundException.class,
            () -> metricsFactory.get("totalQueries").tag("indexFeatureVersion", "4").counter());

        queryingMetricsUpdater.getInternallyFailedQueryCounter().increment();
        assertThrows(
            "No meters have the required tag 'indexFeatureVersion'.",
            MeterNotFoundException.class,
            () ->
                metricsFactory
                    .get("internallyFailedQueries")
                    .tag("indexFeatureVersion", "4")
                    .counter());

        queryingMetricsUpdater.getSearchResultBatchLatencyTimer().record(Duration.ofMillis(100));
        assertThrows(
            "No meters have the required tag 'indexFeatureVersion'.",
            MeterNotFoundException.class,
            () ->
                metricsFactory
                    .get("searchResultBatchLatencies")
                    .tag("indexFeatureVersion", "4")
                    .timer());

        var vectorIndexQueryingMetricsUpdater =
            new IndexMetricsUpdater.QueryingMetricsUpdater(
                metricsFactory,
                IndexTypeData.IndexTypeTag.TAG_VECTOR_SEARCH,
                VectorIndexCapabilities.CURRENT_FEATURE_VERSION,
                false);
        vectorIndexQueryingMetricsUpdater
            .getVectorResultLatencyTimer()
            .record(Duration.ofMillis(100));
        assertThrows(
            "No meters have the required tag 'indexFeatureVersion'.",
            MeterNotFoundException.class,
            () ->
                metricsFactory
                    .get("vectorResultLatencies")
                    .tag("indexFeatureVersion", "4")
                    .timer());
      }

      @Test
      public void testRecordDynamicFeatureFlagLatencyTimer() {
        var metricsFactory = SearchIndex.mockMetricsFactory();
        var queryingMetricsUpdater = new IndexMetricsUpdater.QueryingMetricsUpdater(metricsFactory);

        // Use a scope to record feature flag evaluations
        try (var scope = DynamicFeatureFlagsMetricsRecorder.setup()) {
          // Record some feature flag evaluations
          DynamicFeatureFlagsMetricsRecorder.recordEvaluation("test-flag-1", true);
          DynamicFeatureFlagsMetricsRecorder.recordEvaluation("test-flag-2", false);

          // Record the latency with feature flag tags
          long durationNs = TimeUnit.MILLISECONDS.toNanos(50);
          queryingMetricsUpdater.recordDynamicFeatureFlagLatencyTimer(durationNs);

          // Verify the timers were recorded with the correct tags (one per flag)
          var timer1 =
              metricsFactory
                  .get("dynamicFeatureFlagLatencies")
                  .tag(DynamicFeatureFlagsMetricsRecorder.FEATURE_FLAG_NAME_TAG, "test-flag-1")
                  .tag(DynamicFeatureFlagsMetricsRecorder.EVALUATION_RESULT_TAG, "true")
                  .timer();
          assertThat(timer1.count()).isEqualTo(1);
          assertThat(timer1.totalTime(TimeUnit.MILLISECONDS)).isEqualTo(50);

          var timer2 =
              metricsFactory
                  .get("dynamicFeatureFlagLatencies")
                  .tag(DynamicFeatureFlagsMetricsRecorder.FEATURE_FLAG_NAME_TAG, "test-flag-2")
                  .tag(DynamicFeatureFlagsMetricsRecorder.EVALUATION_RESULT_TAG, "false")
                  .timer();
          assertThat(timer2.count()).isEqualTo(1);
          assertThat(timer2.totalTime(TimeUnit.MILLISECONDS)).isEqualTo(50);
        }

        // Outside the scope, getTagsPerFlag returns empty (context cleaned up automatically)
        var tagsAfterScope = DynamicFeatureFlagsMetricsRecorder.getTagsPerFlag();
        assertThat(tagsAfterScope.size()).isEqualTo(0);
      }

      @Test
      public void testRecordDynamicFeatureFlagLatencyTimer_noScope() {
        var metricsFactory = SearchIndex.mockMetricsFactory();
        var queryingMetricsUpdater = new IndexMetricsUpdater.QueryingMetricsUpdater(metricsFactory);

        // Record the latency without a scope (no feature flags captured)
        long durationNs = TimeUnit.MILLISECONDS.toNanos(25);
        queryingMetricsUpdater.recordDynamicFeatureFlagLatencyTimer(durationNs);

        // Verify no timer was recorded when there are no feature flags evaluated
        assertThrows(
            "No meter with name 'index.stats.dynamicFeatureFlagLatencies' was found.",
            MeterNotFoundException.class,
            () -> metricsFactory.get("dynamicFeatureFlagLatencies").timer());
      }

      @Test
      public void testRecordDynamicFeatureFlagLatencyTimer_multipleRecordings() {
        var metricsFactory = SearchIndex.mockMetricsFactory();
        var queryingMetricsUpdater = new IndexMetricsUpdater.QueryingMetricsUpdater(metricsFactory);

        // First recording with flag-a=true in its own scope
        try (var scope1 = DynamicFeatureFlagsMetricsRecorder.setup()) {
          DynamicFeatureFlagsMetricsRecorder.recordEvaluation("flag-a", true);
          queryingMetricsUpdater.recordDynamicFeatureFlagLatencyTimer(
              TimeUnit.MILLISECONDS.toNanos(10));
        }

        // Second recording with flag-a=false in its own scope
        try (var scope2 = DynamicFeatureFlagsMetricsRecorder.setup()) {
          DynamicFeatureFlagsMetricsRecorder.recordEvaluation("flag-a", false);
          queryingMetricsUpdater.recordDynamicFeatureFlagLatencyTimer(
              TimeUnit.MILLISECONDS.toNanos(20));
        }

        // Verify first timer (flag-a with evaluationResult=true)
        var timerTrue =
            metricsFactory
                .get("dynamicFeatureFlagLatencies")
                .tag(DynamicFeatureFlagsMetricsRecorder.FEATURE_FLAG_NAME_TAG, "flag-a")
                .tag(DynamicFeatureFlagsMetricsRecorder.EVALUATION_RESULT_TAG, "true")
                .timer();
        assertThat(timerTrue.count()).isEqualTo(1);
        assertThat(timerTrue.totalTime(TimeUnit.MILLISECONDS)).isEqualTo(10);

        // Verify second timer (flag-a with evaluationResult=false)
        var timerFalse =
            metricsFactory
                .get("dynamicFeatureFlagLatencies")
                .tag(DynamicFeatureFlagsMetricsRecorder.FEATURE_FLAG_NAME_TAG, "flag-a")
                .tag(DynamicFeatureFlagsMetricsRecorder.EVALUATION_RESULT_TAG, "false")
                .timer();
        assertThat(timerFalse.count()).isEqualTo(1);
        assertThat(timerFalse.totalTime(TimeUnit.MILLISECONDS)).isEqualTo(20);
      }

      public static class TestQueryFeaturesMetricsUpdater {
        @Test
        public void testQueryFeaturesMetrics() {
          var metricsFactory = SearchIndex.mockMetricsFactory();
          var metricsUpdater =
              new IndexMetricsUpdater.QueryingMetricsUpdater.QueryFeaturesMetricsUpdater(
                  metricsFactory);
          metricsUpdater.getCollectorTypeCounter(Collector.Type.FACET).increment();

          var result = metricsUpdater.getMetrics();
          var expected =
              IndexMetricsBuilder.QueryingMetricsBuilder.QueryFeaturesMetricsBuilder.builder()
                  .collectorTypeCount(Collector.Type.FACET, 1)
                  .build();

          Assert.assertEquals(expected, result);
        }

        @Test
        public void testIncCollectorTypeCount() {
          var metricsFactory = SearchIndex.mockMetricsFactory();
          IndexMetricsUpdater.QueryingMetricsUpdater.QueryFeaturesMetricsUpdater metricsUpdater =
              new IndexMetricsUpdater.QueryingMetricsUpdater.QueryFeaturesMetricsUpdater(
                  metricsFactory);
          metricsUpdater.getCollectorTypeCounter(Collector.Type.FACET).increment();

          var counter =
              metricsFactory
                  .get(
                      IndexMetricsUpdater.QueryingMetricsUpdater.QueryFeaturesMetricsUpdater
                          .METRIC_NAME)
                  .tag(
                      IndexMetricsUpdater.QueryingMetricsUpdater.QueryFeaturesMetricsUpdater
                          .NAME_TAG,
                      Enums.convertNameTo(CaseFormat.LOWER_CAMEL, Collector.Type.FACET))
                  .counter();
          Assert.assertEquals(1, counter.count(), 0);
        }

        @Test
        public void testIncOperatorTypeCount() {
          var metricsFactory = mockMetricsFactory("index.stats.query.features");
          IndexMetricsUpdater.QueryingMetricsUpdater.QueryFeaturesMetricsUpdater metricsUpdater =
              IndexMetricsUpdaterBuilder.QueryingMetricsUpdaterBuilder
                  .QueryFeaturesMetricsUpdaterBuilder.builder()
                  .metricsFactory(metricsFactory)
                  .build();
          metricsUpdater.getOperatorTypeCounter(Operator.Type.AUTOCOMPLETE).increment();
          var counter =
              metricsFactory
                  .get(
                      IndexMetricsUpdater.QueryingMetricsUpdater.QueryFeaturesMetricsUpdater
                          .METRIC_NAME)
                  .tag(
                      IndexMetricsUpdater.QueryingMetricsUpdater.QueryFeaturesMetricsUpdater
                          .NAME_TAG,
                      Enums.convertNameTo(CaseFormat.LOWER_CAMEL, Operator.Type.AUTOCOMPLETE))
                  .counter();
          Assert.assertEquals(1, counter.count(), 0);
        }

        @Test
        public void testIncScoreTypeCount() {
          var metricsFactory = mockMetricsFactory("index.stats.query.features");
          IndexMetricsUpdater.QueryingMetricsUpdater.QueryFeaturesMetricsUpdater metricsUpdater =
              IndexMetricsUpdaterBuilder.QueryingMetricsUpdaterBuilder
                  .QueryFeaturesMetricsUpdaterBuilder.builder()
                  .metricsFactory(metricsFactory)
                  .build();
          metricsUpdater.getScoreTypeCounter(Score.Type.CONSTANT).increment();
          var counter =
              metricsFactory
                  .get(
                      IndexMetricsUpdater.QueryingMetricsUpdater.QueryFeaturesMetricsUpdater
                          .METRIC_NAME)
                  .tag(
                      IndexMetricsUpdater.QueryingMetricsUpdater.QueryFeaturesMetricsUpdater
                          .NAME_TAG,
                      Enums.convertNameTo(CaseFormat.LOWER_CAMEL, Score.Type.CONSTANT))
                  .counter();
          Assert.assertEquals(1, counter.count(), 0);
        }

        @Test
        public void testIncTextMatchCriteriaCount() {
          var metricsFactory = mockMetricsFactory("index.stats.query.features");
          IndexMetricsUpdater.QueryingMetricsUpdater.QueryFeaturesMetricsUpdater metricsUpdater =
              IndexMetricsUpdaterBuilder.QueryingMetricsUpdaterBuilder
                  .QueryFeaturesMetricsUpdaterBuilder.builder()
                  .metricsFactory(metricsFactory)
                  .build();
          metricsUpdater.getTextMatchCriteriaCounter(TextOperator.MatchCriteria.ANY).increment();
          var counter =
              metricsFactory
                  .get(
                      IndexMetricsUpdater.QueryingMetricsUpdater.QueryFeaturesMetricsUpdater
                          .METRIC_NAME)
                  .tag(
                      IndexMetricsUpdater.QueryingMetricsUpdater.QueryFeaturesMetricsUpdater
                          .NAME_TAG,
                      Enums.convertNameTo(CaseFormat.LOWER_CAMEL, TextOperator.MatchCriteria.ANY))
                  .counter();
          Assert.assertEquals(1, counter.count(), 0);
        }

        @Test
        public void testIncNoDataSortPositionCount() {
          var metricsFactory = mockMetricsFactory("index.stats.query.features");
          IndexMetricsUpdater.QueryingMetricsUpdater.QueryFeaturesMetricsUpdater metricsUpdater =
              IndexMetricsUpdaterBuilder.QueryingMetricsUpdaterBuilder
                  .QueryFeaturesMetricsUpdaterBuilder.builder()
                  .metricsFactory(metricsFactory)
                  .build();

          metricsUpdater.getNoDataSortPositionCounter(NullEmptySortPosition.HIGHEST).increment();
          metricsUpdater.getNoDataSortPositionCounter(NullEmptySortPosition.HIGHEST).increment();
          metricsUpdater.getNoDataSortPositionCounter(NullEmptySortPosition.LOWEST).increment();

          var noDataHighestCounter =
              metricsFactory
                  .get(
                      IndexMetricsUpdater.QueryingMetricsUpdater.QueryFeaturesMetricsUpdater
                          .METRIC_NAME)
                  .tag(
                      IndexMetricsUpdater.QueryingMetricsUpdater.QueryFeaturesMetricsUpdater
                          .NAME_TAG,
                      SORT_NO_DATA_NAME_TAG_PREFIX
                          + Enums.convertNameTo(
                              CaseFormat.LOWER_CAMEL, NullEmptySortPosition.HIGHEST))
                  .counter();

          var noDataLowestCounter =
              metricsFactory
                  .get(
                      IndexMetricsUpdater.QueryingMetricsUpdater.QueryFeaturesMetricsUpdater
                          .METRIC_NAME)
                  .tag(
                      IndexMetricsUpdater.QueryingMetricsUpdater.QueryFeaturesMetricsUpdater
                          .NAME_TAG,
                      SORT_NO_DATA_NAME_TAG_PREFIX
                          + Enums.convertNameTo(
                              CaseFormat.LOWER_CAMEL, NullEmptySortPosition.LOWEST))
                  .counter();

          Assert.assertEquals(2, noDataHighestCounter.count(), 2);
          Assert.assertEquals(2, noDataLowestCounter.count(), 1);
        }

        @Test
        public void testVectorQueryTypeFeaturesMetrics() {
          var metricsFactory = SearchIndex.mockMetricsFactory();
          var metricsUpdater =
              new IndexMetricsUpdater.QueryingMetricsUpdater.QueryFeaturesMetricsUpdater(
                  metricsFactory);
          metricsUpdater
              .getVectorSearchQueryTypeCounter(VectorSearchCriteria.Type.APPROXIMATE)
              .increment();
          metricsUpdater
              .getVectorSearchQueryTypeCounter(VectorSearchCriteria.Type.EXACT)
              .increment(2.0);

          var result = metricsUpdater.getMetrics();
          var expected =
              IndexMetricsBuilder.QueryingMetricsBuilder.QueryFeaturesMetricsBuilder.builder()
                  .vectorSearchQueryTypeCount(VectorSearchCriteria.Type.APPROXIMATE, 1)
                  .vectorSearchQueryTypeCount(VectorSearchCriteria.Type.EXACT, 2)
                  .build();

          Assert.assertEquals(expected, result);
        }

        @Test
        public void testVectorSearchFilterOperatorTypeCounterMap() {
          var metricsFactory = SearchIndex.mockMetricsFactory();
          IndexMetricsUpdater.QueryingMetricsUpdater.QueryFeaturesMetricsUpdater metricsUpdater =
              new IndexMetricsUpdater.QueryingMetricsUpdater.QueryFeaturesMetricsUpdater(
                  metricsFactory);
          metricsUpdater
              .getSearchVectorSearchFilterOperatorTypeCounterMap(Operator.Type.GEO_WITHIN)
              .increment();

          var counter =
              metricsFactory
                  .get(
                      IndexMetricsUpdater.QueryingMetricsUpdater.QueryFeaturesMetricsUpdater
                          .METRIC_NAME)
                  .tag(
                      IndexMetricsUpdater.QueryingMetricsUpdater.QueryFeaturesMetricsUpdater
                          .NAME_TAG,
                      SEARCH_VECTOR_SEARCH_FILTER_TAG_PREFIX
                          + Enums.convertNameTo(CaseFormat.LOWER_CAMEL, Operator.Type.GEO_WITHIN))
                  .counter();

          Assert.assertEquals(1, counter.count(), 0);
        }

        @Test
        public void testKnnBetaFilterOperatorTypeCounterMap() {
          var metricsFactory = SearchIndex.mockMetricsFactory();
          IndexMetricsUpdater.QueryingMetricsUpdater.QueryFeaturesMetricsUpdater metricsUpdater =
              new IndexMetricsUpdater.QueryingMetricsUpdater.QueryFeaturesMetricsUpdater(
                  metricsFactory);
          metricsUpdater
              .getKnnBetaFilterOperatorTypeCounterMap(Operator.Type.GEO_WITHIN)
              .increment();

          var counter =
              metricsFactory
                  .get(
                      IndexMetricsUpdater.QueryingMetricsUpdater.QueryFeaturesMetricsUpdater
                          .METRIC_NAME)
                  .tag(
                      IndexMetricsUpdater.QueryingMetricsUpdater.QueryFeaturesMetricsUpdater
                          .NAME_TAG,
                      KNN_BETA_FILTER_TAG_PREFIX
                          + Enums.convertNameTo(CaseFormat.LOWER_CAMEL, Operator.Type.GEO_WITHIN))
                  .counter();

          Assert.assertEquals(1, counter.count(), 0);
        }
      }
    }

    @Test
    public void testClose() {
      var meterAndFtdcRegistry = MeterAndFtdcRegistry.createWithSimpleRegistries();
      var meterRegistry = meterAndFtdcRegistry.meterRegistry();
      var metricsFactory =
          new PerIndexMetricsFactory(
              IndexMetricsUpdater.NAMESPACE, meterAndFtdcRegistry, GenerationIdBuilder.create());
      IndexMetricsUpdater metricsUpdater =
          IndexMetricsUpdaterBuilder.builder()
              .metricsFactory(metricsFactory)
              .indexMetricsSupplier(IndexMetricsSupplier.mockEmptyIndexMetricsSupplier())
              .build();
      Assert.assertTrue(meterRegistry.getMeters().size() > 0);
      metricsUpdater.close();
      // All per-index metrics should be removed; per process metrics may remain.
      Assert.assertEquals(
          0,
          meterRegistry.getMeters().stream()
              .filter(
                  m ->
                      m.getId().getTags().stream()
                              .filter(t -> t.getKey().equals("generationId logString"))
                              .count()
                          > 0)
              .count());
    }

    @Test
    public void testReplicationMetricsIndexTypeTag() {
      var meterAndFtdcRegistry = MeterAndFtdcRegistry.createWithSimpleRegistries();
      var meterRegistry = meterAndFtdcRegistry.meterRegistry();
      var metricsFactory =
          new PerIndexMetricsFactory(
              IndexMetricsUpdater.NAMESPACE, meterAndFtdcRegistry, GenerationIdBuilder.create());
      IndexMetricsUpdater vectorSearchIndexMetricsUpdater =
          IndexMetricsUpdaterBuilder.builder()
              .metricsFactory(metricsFactory)
              .indexMetricsSupplier(IndexMetricsSupplier.mockEmptyIndexMetricsSupplier())
              .indexDefinition(mockAutoEmbeddingVectorSearchDefinition(new ObjectId()))
              .build();
      vectorSearchIndexMetricsUpdater.close();
      var replicationMetricNamespace =
          IndexMetricsUpdater.NAMESPACE
              + "."
              + IndexMetricsUpdater.ReplicationMetricsUpdater.NAMESPACE;
      var replicationMetersList =
          meterRegistry.getMeters().stream()
              .filter(m -> m.getId().getName().startsWith(replicationMetricNamespace))
              .collect(Collectors.toList());
      // Asserts that the replication metrics all have the right auto embeddings index tag when used
      // for a auto embeddings vector index
      Assert.assertTrue(
          replicationMetersList.stream()
              .allMatch(
                  m ->
                      m.getId()
                          .getTag(INDEX_TYPE_TAG_NAME)
                          .equals(TAG_VECTOR_SEARCH_AUTO_EMBEDDING.tagValue)));
      Assert.assertTrue(replicationMetersList.size() > 0);
    }
  }
}
