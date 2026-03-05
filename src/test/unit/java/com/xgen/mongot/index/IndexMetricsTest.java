package com.xgen.mongot.index;

import static com.xgen.testing.BsonDeserializationTestSuite.fromDocument;
import static com.xgen.testing.BsonSerializationTestSuite.fromEncodable;

import com.xgen.mongot.index.definition.SearchIndexDefinition;
import com.xgen.mongot.index.query.operators.TextOperator;
import com.xgen.mongot.index.query.scores.Score;
import com.xgen.mongot.index.query.sort.NullEmptySortPosition;
import com.xgen.testing.BsonDeserializationTestSuite;
import com.xgen.testing.BsonSerializationTestSuite;
import com.xgen.testing.mongot.index.IndexMetricsBuilder;
import com.xgen.testing.mongot.index.definition.DocumentFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.SearchIndexDefinitionBuilder;
import com.xgen.testing.mongot.metrics.micrometer.PercentilesBuilder;
import com.xgen.testing.mongot.metrics.micrometer.SerializableTimerBuilder;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.bson.BsonTimestamp;
import org.bson.types.ObjectId;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses(
    value = {
      IndexMetricsTest.TestIndexMetricsDeserialization.class,
      IndexMetricsTest.TestIndexMetricsSerialization.class,
      IndexMetricsTest.TestIndexingMetricsSerialization.class,
      IndexMetricsTest.TestQueryingMetricsSerialization.class,
      IndexMetricsTest.TestQueryFeaturesMetricsSerialization.class,
    })
public class IndexMetricsTest {

  private static final String RESOURCES_PATH = "src/test/unit/resources/index";

  @RunWith(Parameterized.class)
  public static class TestIndexMetricsDeserialization {
    private static final String SUITE_NAME = "index-metrics-deserialization";
    private static final BsonDeserializationTestSuite<IndexMetrics> TEST_SUITE =
        fromDocument(RESOURCES_PATH, SUITE_NAME, IndexMetrics::fromBson);

    private final BsonDeserializationTestSuite.TestSpecWrapper<IndexMetrics> testSpec;

    public TestIndexMetricsDeserialization(
        BsonDeserializationTestSuite.TestSpecWrapper<IndexMetrics> testSpec) {
      this.testSpec = testSpec;
    }

    /** Test data. */
    @Parameterized.Parameters(name = "{0}")
    public static Iterable<BsonDeserializationTestSuite.TestSpecWrapper<IndexMetrics>> data() {
      return TEST_SUITE.withExamples(simple());
    }

    @Test
    public void runTest() throws Exception {
      TEST_SUITE.runTest(this.testSpec);
    }

    private static BsonDeserializationTestSuite.ValidSpec<IndexMetrics> simple() {
      return BsonDeserializationTestSuite.TestSpec.valid("simple", IndexMetricsBuilder.sample());
    }
  }

  @RunWith(Parameterized.class)
  public static class TestIndexingMetricsSerialization {
    private static final String SUITE_NAME = "indexing-metrics-serialization";
    private static final BsonSerializationTestSuite<IndexMetrics.IndexingMetrics> TEST_SUITE =
        fromEncodable(RESOURCES_PATH, SUITE_NAME);

    private final BsonSerializationTestSuite.TestSpec<IndexMetrics.IndexingMetrics> testSpec;

    public TestIndexingMetricsSerialization(
        BsonSerializationTestSuite.TestSpec<IndexMetrics.IndexingMetrics> testSpec) {
      this.testSpec = testSpec;
    }

    /** Test data. */
    @Parameterized.Parameters(name = "{0}")
    public static Iterable<BsonSerializationTestSuite.TestSpec<IndexMetrics.IndexingMetrics>>
        data() {
      return List.of(indexingStatsSerialization());
    }

    @Test
    public void runTest() throws Exception {
      TEST_SUITE.runTest(this.testSpec);
    }

    private static BsonSerializationTestSuite.TestSpec<IndexMetrics.IndexingMetrics>
        indexingStatsSerialization() {
      return BsonSerializationTestSuite.TestSpec.create(
          "indexingMetricsSerialization",
          IndexMetricsBuilder.IndexingMetricsBuilder.builder()
              .documentEventTypeCount(DocumentEvent.EventType.INSERT, 50)
              .initialSyncExceptionCount(1)
              .steadyStateExceptionCount(2)
              .replicationOpTime(new BsonTimestamp(20, 0))
              .replicationLagMs(20)
              .totalBytesProcessed(10.0)
              .numDocs(23)
              .numLuceneMaxDocs(24)
              .maxLuceneMaxDocs(19)
              .numMongoDbDocs(25)
              .indexSize(2L)
              .vectorFieldSize(0L)
              .requiredMemory(500000000L)
              .numFields(30)
              .batchIndexingTimer(
                  SerializableTimerBuilder.builder()
                      .timeUnit(TimeUnit.NANOSECONDS)
                      .count(2L)
                      .totalTime(150.0)
                      .max(100.0)
                      .mean(75.0)
                      .percentiles(
                          PercentilesBuilder.builder()
                              .percentile50(75.0)
                              .percentile75(100.0)
                              .percentile90(100.0)
                              .percentile99(100.0)
                              .build())
                      .build())
              .build());
    }
  }

  @RunWith(Parameterized.class)
  public static class TestQueryingMetricsSerialization {
    private static final String SUITE_NAME = "querying-metrics-serialization";
    private static final BsonSerializationTestSuite<IndexMetrics.QueryingMetrics> TEST_SUITE =
        fromEncodable(RESOURCES_PATH, SUITE_NAME);

    private final BsonSerializationTestSuite.TestSpec<IndexMetrics.QueryingMetrics> testSpec;

    public TestQueryingMetricsSerialization(
        BsonSerializationTestSuite.TestSpec<IndexMetrics.QueryingMetrics> testSpec) {
      this.testSpec = testSpec;
    }

    /** Test data. */
    @Parameterized.Parameters(name = "{0}")
    public static Iterable<BsonSerializationTestSuite.TestSpec<IndexMetrics.QueryingMetrics>>
        data() {
      return List.of(queryingStatsSerialization());
    }

    @Test
    public void runTest() throws Exception {
      TEST_SUITE.runTest(this.testSpec);
    }

    private static BsonSerializationTestSuite.TestSpec<IndexMetrics.QueryingMetrics>
        queryingStatsSerialization() {
      IndexMetrics.QueryingMetrics.QueryFeaturesMetrics queryFeaturesMetrics =
          IndexMetricsBuilder.QueryingMetricsBuilder.QueryFeaturesMetricsBuilder.builder()
              .fuzzyCount(5)
              .synonymCount(6)
              .textSynonymCount(4)
              .textSynonymsWithoutMatchCriteriaCount(3)
              .phraseSynonymCount(2)
              .highlightingCount(7)
              .wildcardPathCount(8)
              .concurrentCount(10)
              .returnStoredSourceCount(2)
              .sequenceTokenCount(4)
              .requireSequenceTokensCount(1)
              .scoreDetailsCount(1)
              .sortCount(3)
              .trackingCount(11)
              .returnScopeCount(12)
              .build();

      return BsonSerializationTestSuite.TestSpec.create(
          "queryingMetricsSerialization",
          IndexMetricsBuilder.QueryingMetricsBuilder.builder()
              .totalQueryCount(5)
              .failedQueryCount(3)
              .lenientFailureCount(4)
              .queryFeaturesMetrics(queryFeaturesMetrics)
              .searchGetMoreCommandCount(4)
              .searchResultBatchLatency(
                  SerializableTimerBuilder.builder()
                      .timeUnit(TimeUnit.MILLISECONDS)
                      .count(2L)
                      .totalTime(8.0)
                      .max(4.0)
                      .mean(2.0)
                      .percentiles(
                          PercentilesBuilder.builder()
                              .percentile50(2.0)
                              .percentile75(2.0)
                              .percentile90(2.0)
                              .percentile99(1.0)
                              .build())
                      .build())
              .tokenFacetsStateRefresh(
                  SerializableTimerBuilder.builder()
                      .timeUnit(TimeUnit.MILLISECONDS)
                      .count(2L)
                      .totalTime(8.0)
                      .max(4.0)
                      .mean(2.0)
                      .percentiles(
                          PercentilesBuilder.builder()
                              .percentile50(2.0)
                              .percentile75(2.0)
                              .percentile90(2.0)
                              .percentile99(3.0)
                              .build())
                      .build())
              .stringFacetsStateRefresh(
                  SerializableTimerBuilder.builder()
                      .timeUnit(TimeUnit.MILLISECONDS)
                      .count(2L)
                      .totalTime(8.0)
                      .max(4.0)
                      .mean(2.0)
                      .percentiles(
                          PercentilesBuilder.builder()
                              .percentile50(2.0)
                              .percentile75(2.0)
                              .percentile90(2.0)
                              .percentile99(3.0)
                              .build())
                      .build())
              .build());
    }
  }

  @RunWith(Parameterized.class)
  public static class TestQueryFeaturesMetricsSerialization {
    private static final String SUITE_NAME = "query-features-metrics-serialization";
    private static final BsonSerializationTestSuite<
            IndexMetrics.QueryingMetrics.QueryFeaturesMetrics>
        TEST_SUITE = fromEncodable(RESOURCES_PATH, SUITE_NAME);

    private final BsonSerializationTestSuite.TestSpec<
            IndexMetrics.QueryingMetrics.QueryFeaturesMetrics>
        testSpec;

    public TestQueryFeaturesMetricsSerialization(
        BsonSerializationTestSuite.TestSpec<IndexMetrics.QueryingMetrics.QueryFeaturesMetrics>
            testSpec) {
      this.testSpec = testSpec;
    }

    /** Test data. */
    @Parameterized.Parameters(name = "{0}")
    public static Iterable<
            BsonSerializationTestSuite.TestSpec<IndexMetrics.QueryingMetrics.QueryFeaturesMetrics>>
        data() {
      return List.of(queryFeaturesStatsSerialization());
    }

    @Test
    public void runTest() throws Exception {
      TEST_SUITE.runTest(this.testSpec);
    }

    private static BsonSerializationTestSuite.TestSpec<
            IndexMetrics.QueryingMetrics.QueryFeaturesMetrics>
        queryFeaturesStatsSerialization() {
      return BsonSerializationTestSuite.TestSpec.create(
          "queryFeaturesMetricsSerialization",
          IndexMetricsBuilder.QueryingMetricsBuilder.QueryFeaturesMetricsBuilder.builder()
              .fuzzyCount(1)
              .synonymCount(3)
              .textSynonymCount(1)
              .phraseSynonymCount(1)
              .textSynonymsWithoutMatchCriteriaCount(1)
              .highlightingCount(1)
              .wildcardPathCount(1)
              .scoreTypeCount(Score.Type.CONSTANT, 1)
              .textMatchCriteriaCount(TextOperator.MatchCriteria.ANY, 1)
              .concurrentCount(10)
              .returnStoredSourceCount(2)
              .sequenceTokenCount(4)
              .requireSequenceTokensCount(1)
              .scoreDetailsCount(1)
              .sortCount(3)
              .noDataSortPositionCount(NullEmptySortPosition.HIGHEST, 3)
              .noDataSortPositionCount(NullEmptySortPosition.LOWEST, 1)
              .trackingCount(11)
              .returnScopeCount(12)
              .build());
    }
  }

  @RunWith(Parameterized.class)
  public static class TestIndexMetricsSerialization {
    private static final String SUITE_NAME = "index-metrics-serialization";
    private static final BsonSerializationTestSuite<IndexMetrics> TEST_SUITE =
        fromEncodable(RESOURCES_PATH, SUITE_NAME);

    private final BsonSerializationTestSuite.TestSpec<IndexMetrics> testSpec;

    public TestIndexMetricsSerialization(
        BsonSerializationTestSuite.TestSpec<IndexMetrics> testSpec) {
      this.testSpec = testSpec;
    }

    /** Test data. */
    @Parameterized.Parameters(name = "{0}")
    public static Iterable<BsonSerializationTestSuite.TestSpec<IndexMetrics>> data() {
      return List.of(indexStatsSerialization());
    }

    @Test
    public void runTest() throws Exception {
      TEST_SUITE.runTest(this.testSpec);
    }

    private static BsonSerializationTestSuite.TestSpec<IndexMetrics> indexStatsSerialization() {
      IndexMetrics.QueryingMetrics.QueryFeaturesMetrics queryFeaturesMetrics =
          IndexMetricsBuilder.QueryingMetricsBuilder.QueryFeaturesMetricsBuilder.builder()
              .fuzzyCount(5)
              .synonymCount(6)
              .textSynonymCount(4)
              .textSynonymsWithoutMatchCriteriaCount(3)
              .phraseSynonymCount(2)
              .highlightingCount(7)
              .wildcardPathCount(8)
              .concurrentCount(10)
              .returnStoredSourceCount(2)
              .sequenceTokenCount(4)
              .requireSequenceTokensCount(1)
              .scoreDetailsCount(1)
              .sortCount(3)
              .trackingCount(11)
              .returnScopeCount(12)
              .build();

      IndexMetrics.QueryingMetrics queryingMetrics =
          IndexMetricsBuilder.QueryingMetricsBuilder.builder()
              .totalQueryCount(5)
              .failedQueryCount(3)
              .lenientFailureCount(4)
              .queryFeaturesMetrics(queryFeaturesMetrics)
              .searchGetMoreCommandCount(4)
              .searchResultBatchLatency(
                  SerializableTimerBuilder.builder()
                      .timeUnit(TimeUnit.MILLISECONDS)
                      .count(2L)
                      .totalTime(8.0)
                      .max(4.0)
                      .mean(2.0)
                      .percentiles(
                          PercentilesBuilder.builder()
                              .percentile50(2.0)
                              .percentile75(2.0)
                              .percentile90(2.0)
                              .percentile99(1.0)
                              .build())
                      .build())
              .tokenFacetsStateRefresh(
                  SerializableTimerBuilder.builder()
                      .timeUnit(TimeUnit.MILLISECONDS)
                      .count(2L)
                      .totalTime(8.0)
                      .max(4.0)
                      .mean(2.0)
                      .percentiles(
                          PercentilesBuilder.builder()
                              .percentile50(2.0)
                              .percentile75(2.0)
                              .percentile90(2.0)
                              .percentile99(3.0)
                              .build())
                      .build())
              .stringFacetsStateRefresh(
                  SerializableTimerBuilder.builder()
                      .timeUnit(TimeUnit.MILLISECONDS)
                      .count(2L)
                      .totalTime(8.0)
                      .max(4.0)
                      .mean(2.0)
                      .percentiles(
                          PercentilesBuilder.builder()
                              .percentile50(2.0)
                              .percentile75(2.0)
                              .percentile90(2.0)
                              .percentile99(3.0)
                              .build())
                      .build())
              .build();

      IndexMetrics.IndexingMetrics indexingMetrics =
          IndexMetricsBuilder.IndexingMetricsBuilder.builder()
              .documentEventTypeCount(DocumentEvent.EventType.INSERT, 50)
              .initialSyncExceptionCount(1)
              .steadyStateExceptionCount(2)
              .replicationOpTime(new BsonTimestamp(20, 0))
              .replicationLagMs(20)
              .totalBytesProcessed(10.0)
              .numDocs(23)
              .numLuceneMaxDocs(24)
              .maxLuceneMaxDocs(19)
              .numMongoDbDocs(25)
              .indexSize(2L)
              .vectorFieldSize(0L)
              .requiredMemory(500000000L)
              .numFields(30)
              .batchIndexingTimer(
                  SerializableTimerBuilder.builder()
                      .timeUnit(TimeUnit.NANOSECONDS)
                      .count(2L)
                      .totalTime(150.0)
                      .max(100.0)
                      .mean(75.0)
                      .percentiles(
                          PercentilesBuilder.builder()
                              .percentile50(75.0)
                              .percentile75(100.0)
                              .percentile90(100.0)
                              .percentile99(100.0)
                              .build())
                      .build())
              .build();

      SearchIndexDefinition indexDefinition =
          SearchIndexDefinitionBuilder.builder()
              .indexId(new ObjectId("507f191e810c19729de860ea"))
              .name("index")
              .database("database")
              .lastObservedCollectionName("collection")
              .collectionUuid(UUID.fromString("eb6c40ca-f25e-47e8-b48c-02a05b64a5aa"))
              .mappings(DocumentFieldDefinitionBuilder.builder().dynamic(true).build())
              .build();

      return BsonSerializationTestSuite.TestSpec.create(
          "indexMetricsSerialization",
          IndexMetricsBuilder.builder()
              .indexingMetrics(indexingMetrics)
              .queryingMetrics(queryingMetrics)
              .indexDefinition(indexDefinition)
              .build());
    }
  }
}
