package com.xgen.testing.mongot.index;

import com.xgen.mongot.index.DocumentEvent;
import com.xgen.mongot.index.DocumentEvent.EventType;
import com.xgen.mongot.index.IndexMetrics;
import com.xgen.mongot.index.definition.SearchIndexDefinition;
import com.xgen.mongot.index.lucene.field.FieldName;
import com.xgen.mongot.index.query.collectors.Collector;
import com.xgen.mongot.index.query.operators.Operator;
import com.xgen.mongot.index.query.operators.TextOperator;
import com.xgen.mongot.index.query.operators.VectorSearchCriteria;
import com.xgen.mongot.index.query.scores.Score;
import com.xgen.mongot.index.query.sort.NullEmptySortPosition;
import com.xgen.mongot.index.status.IndexStatus;
import com.xgen.mongot.metrics.micrometer.Percentiles;
import com.xgen.mongot.metrics.micrometer.SerializableTimer;
import com.xgen.mongot.util.Bytes;
import com.xgen.mongot.util.Check;
import com.xgen.testing.mongot.index.IndexMetricsBuilder.QueryingMetricsBuilder.QueryFeaturesMetricsBuilder;
import com.xgen.testing.mongot.index.definition.DocumentFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.SearchIndexDefinitionBuilder;
import com.xgen.testing.mongot.metrics.micrometer.PercentilesBuilder;
import com.xgen.testing.mongot.metrics.micrometer.SerializableTimerBuilder;
import java.util.EnumMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.bson.BsonTimestamp;
import org.bson.types.ObjectId;

public class IndexMetricsBuilder {

  private IndexMetrics.IndexingMetrics indexingMetrics = IndexingMetricsBuilder.builder().build();
  private IndexMetrics.QueryingMetrics queryingMetrics = QueryingMetricsBuilder.builder().build();
  private Optional<SearchIndexDefinition> indexDefinition = Optional.empty();
  private IndexStatus indexStatus = IndexStatus.notStarted();

  public static IndexMetricsBuilder builder() {
    return new IndexMetricsBuilder();
  }

  public IndexMetricsBuilder indexingMetrics(IndexMetrics.IndexingMetrics indexingMetrics) {
    this.indexingMetrics = indexingMetrics;
    return this;
  }

  public IndexMetricsBuilder queryingMetrics(IndexMetrics.QueryingMetrics queryingMetrics) {
    this.queryingMetrics = queryingMetrics;
    return this;
  }

  public IndexMetricsBuilder indexDefinition(SearchIndexDefinition indexDefinition) {
    this.indexDefinition = Optional.of(indexDefinition);
    return this;
  }

  public IndexMetricsBuilder indexStatus(IndexStatus indexStatus) {
    this.indexStatus = indexStatus;
    return this;
  }

  public IndexMetrics build() {
    Check.isPresent(this.indexDefinition, "indexDefinition");
    return new IndexMetrics(
        IndexMetrics.IndexMetadata.create(this.indexDefinition.get(), this.indexStatus),
        this.indexingMetrics,
        this.queryingMetrics);
  }

  /** Create a sample IndexMetrics. */
  public static IndexMetrics sample() {
    var queryFeaturesMetrics =
        QueryFeaturesMetricsBuilder.builder()
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
            .explainCount(3)
            .sortCount(3)
            .trackingCount(11)
            .returnScopeCount(12)
            .build();

    var queryingMetrics =
        QueryingMetricsBuilder.builder()
            .totalQueryCount(5)
            .failedQueryCount(3)
            .lenientFailureCount(4)
            .searchGetMoreCommandCount(4)
            .searchResultBatchLatency(
                SerializableTimerBuilder.builder()
                    .timeUnit(TimeUnit.MILLISECONDS)
                    .count(2)
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
            .queryFeaturesMetrics(queryFeaturesMetrics)
            .build();

    var indexingMetrics =
        IndexingMetricsBuilder.builder()
            .documentEventTypeCount(EventType.INSERT, 50)
            .initialSyncExceptionCount(1)
            .steadyStateExceptionCount(2)
            .replicationOpTime(new BsonTimestamp(20, 0))
            .replicationLagMs(20)
            .totalBytesProcessed(10.0)
            .numDocs(100)
            .numMongoDbDocs(100L)
            .numLuceneMaxDocs(100)
            .maxLuceneMaxDocs(100)
            .numFields(20)
            .indexSize(5L)
            .vectorFieldSize(0L)
            .requiredMemory(500_000_000L)
            .batchIndexingTimer(
                SerializableTimerBuilder.builder()
                    .timeUnit(TimeUnit.NANOSECONDS)
                    .count(2)
                    .totalTime(100.0)
                    .max(80.0)
                    .mean(50.0)
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

    return new IndexMetrics(
        IndexMetrics.IndexMetadata.create(indexDefinition, IndexStatus.notStarted()),
        indexingMetrics,
        queryingMetrics);
  }

  public static class QueryingMetricsBuilder {
    private double totalQueryCount = 0.0;
    private double failedQueryCount = 0.0;
    private double lenientFailureCount = 0.0;
    private double searchGetMoreCommandCount = 0.0;
    private SerializableTimer searchResultBatchLatency =
        SerializableTimerBuilder.builder()
            .timeUnit(TimeUnit.MILLISECONDS)
            .percentiles(
                new Percentiles(
                    Optional.empty(),
                    Optional.of(0d),
                    Optional.of(0d),
                    Optional.of(0d),
                    Optional.of(0d)))
            .build();
    private SerializableTimer tokenFacetsStateRefresh =
        SerializableTimerBuilder.builder()
            .timeUnit(TimeUnit.MILLISECONDS)
            .percentiles(
                new Percentiles(
                    Optional.empty(),
                    Optional.of(0d),
                    Optional.of(0d),
                    Optional.of(0d),
                    Optional.of(0d)))
            .build();
    private IndexMetrics.QueryingMetrics.QueryFeaturesMetrics queryFeaturesMetrics =
        QueryFeaturesMetricsBuilder.builder().build();

    public static QueryingMetricsBuilder builder() {
      return new QueryingMetricsBuilder();
    }

    public QueryingMetricsBuilder totalQueryCount(double totalQueryCount) {
      this.totalQueryCount = totalQueryCount;
      return this;
    }

    public QueryingMetricsBuilder failedQueryCount(double failedQueryCount) {
      this.failedQueryCount = failedQueryCount;
      return this;
    }

    public QueryingMetricsBuilder lenientFailureCount(double lenientFailureCount) {
      this.lenientFailureCount = lenientFailureCount;
      return this;
    }

    public QueryingMetricsBuilder searchGetMoreCommandCount(double searchGetMoreCommandCount) {
      this.searchGetMoreCommandCount = searchGetMoreCommandCount;
      return this;
    }

    public QueryingMetricsBuilder searchResultBatchLatency(
        SerializableTimer searchResultBatchLatency) {
      this.searchResultBatchLatency = searchResultBatchLatency;
      return this;
    }

    public QueryingMetricsBuilder tokenFacetsStateRefresh(
        SerializableTimer tokenFacetsStateRefresh) {
      this.tokenFacetsStateRefresh = tokenFacetsStateRefresh;
      return this;
    }

    public QueryingMetricsBuilder queryFeaturesMetrics(
        IndexMetrics.QueryingMetrics.QueryFeaturesMetrics queryFeaturesMetrics) {
      this.queryFeaturesMetrics = queryFeaturesMetrics;
      return this;
    }

    public IndexMetrics.QueryingMetrics build() {
      return new IndexMetrics.QueryingMetrics(
          this.totalQueryCount,
          this.failedQueryCount,
          this.lenientFailureCount,
          this.searchGetMoreCommandCount,
          this.searchResultBatchLatency,
          Optional.of(this.tokenFacetsStateRefresh),
          this.queryFeaturesMetrics);
    }

    public static class QueryFeaturesMetricsBuilder {

      private final Map<Collector.Type, Double> collectorTypeCountMap =
          initEnumMap(Collector.Type.class);
      private final Map<Operator.Type, Double> operatorTypeCountMap =
          initEnumMap(Operator.Type.class);
      private final Map<Score.Type, Double> scoreTypeCountMap = initEnumMap(Score.Type.class);
      private final Map<TextOperator.MatchCriteria, Double> textMatchCriteriaCountMap =
          initEnumMap(TextOperator.MatchCriteria.class);
      private final Map<NullEmptySortPosition, Double> noDataSortPositionCountMap =
          initEnumMap(NullEmptySortPosition.class);
      private final Map<VectorSearchCriteria.Type, Double> vectorSearchQueryTypeCountMap =
          initEnumMap(VectorSearchCriteria.Type.class);
      private final Map<Operator.Type, Double> knnBetaFilterOperatorTypeCounterMap =
          initEnumMap(Operator.Type.class);
      private final Map<Operator.Type, Double> searchVectorSearchFilterOperatorTypeCounterMap =
          initEnumMap(Operator.Type.class);
      private double synonymCount = 0.0;
      private double textSynonymCount = 0.0;
      private double textSynonymsWithoutMatchCriteriaCount = 0.0;
      private double phraseSynonymCount = 0.0;
      private double fuzzyCount = 0.0;
      private double wildcardPathCount = 0.0;
      private double highlightingCount = 0.0;
      private double concurrentCount = 0.0;
      private double returnStoredSourceCount = 0.0;
      private double sequenceTokenCount = 0.0;
      private double requireSequenceTokensCount = 0.0;
      private double scoreDetailsCount = 0.0;
      private double sortCount = 0.0;
      private double trackingCount = 0.0;
      private double returnScopeCount = 0.0;
      private double explainCount = 0.0;

      public static QueryFeaturesMetricsBuilder builder() {
        return new QueryFeaturesMetricsBuilder();
      }

      public QueryFeaturesMetricsBuilder synonymCount(double synonymCount) {
        this.synonymCount = synonymCount;
        return this;
      }

      public QueryFeaturesMetricsBuilder phraseSynonymCount(double phraseSynonymCount) {
        this.phraseSynonymCount = phraseSynonymCount;
        return this;
      }

      public QueryFeaturesMetricsBuilder textSynonymCount(double textSynonymCount) {
        this.textSynonymCount = textSynonymCount;
        return this;
      }

      public QueryFeaturesMetricsBuilder textSynonymsWithoutMatchCriteriaCount(
          double textSynonymsWithoutMatchCriteriaCount) {
        this.textSynonymsWithoutMatchCriteriaCount = textSynonymsWithoutMatchCriteriaCount;
        return this;
      }

      public QueryFeaturesMetricsBuilder sequenceTokenCount(double sequenceTokenCount) {
        this.sequenceTokenCount = sequenceTokenCount;
        return this;
      }

      public QueryFeaturesMetricsBuilder requireSequenceTokensCount(
          double requireSequenceTokensCount) {
        this.requireSequenceTokensCount = requireSequenceTokensCount;
        return this;
      }

      public QueryFeaturesMetricsBuilder scoreDetailsCount(double scoreDetailsCount) {
        this.scoreDetailsCount = scoreDetailsCount;
        return this;
      }

      public QueryFeaturesMetricsBuilder fuzzyCount(double fuzzyCount) {
        this.fuzzyCount = fuzzyCount;
        return this;
      }

      public QueryFeaturesMetricsBuilder wildcardPathCount(double wildcardPathCount) {
        this.wildcardPathCount = wildcardPathCount;
        return this;
      }

      public QueryFeaturesMetricsBuilder highlightingCount(double highlightingCount) {
        this.highlightingCount = highlightingCount;
        return this;
      }

      public QueryFeaturesMetricsBuilder concurrentCount(double concurrentCount) {
        this.concurrentCount = concurrentCount;
        return this;
      }

      public QueryFeaturesMetricsBuilder returnStoredSourceCount(double returnStoredSourceCount) {
        this.returnStoredSourceCount = returnStoredSourceCount;
        return this;
      }

      public QueryFeaturesMetricsBuilder sortCount(double sortCount) {
        this.sortCount = sortCount;
        return this;
      }

      public QueryFeaturesMetricsBuilder trackingCount(double trackingCount) {
        this.trackingCount = trackingCount;
        return this;
      }

      public QueryFeaturesMetricsBuilder returnScopeCount(double returnScopeCount) {
        this.returnScopeCount = returnScopeCount;
        return this;
      }

      public QueryFeaturesMetricsBuilder explainCount(double explainCount) {
        this.explainCount = explainCount;
        return this;
      }

      public QueryFeaturesMetricsBuilder collectorTypeCount(
          Collector.Type collectorType, double count) {
        this.collectorTypeCountMap.put(collectorType, count);
        return this;
      }

      public QueryFeaturesMetricsBuilder operatorTypeCount(
          Operator.Type operatorType, double count) {
        this.operatorTypeCountMap.put(operatorType, count);
        return this;
      }

      public QueryFeaturesMetricsBuilder scoreTypeCount(Score.Type scoreType, double count) {
        this.scoreTypeCountMap.put(scoreType, count);
        return this;
      }

      public QueryFeaturesMetricsBuilder textMatchCriteriaCount(
          TextOperator.MatchCriteria matchCriteria, double count) {
        this.textMatchCriteriaCountMap.put(matchCriteria, count);
        return this;
      }

      public QueryFeaturesMetricsBuilder noDataSortPositionCount(
          NullEmptySortPosition nullEmptySortPosition, double count) {
        this.noDataSortPositionCountMap.put(nullEmptySortPosition, count);
        return this;
      }

      public QueryFeaturesMetricsBuilder vectorSearchQueryTypeCount(
          VectorSearchCriteria.Type vectorSearchType, double count) {
        this.vectorSearchQueryTypeCountMap.put(vectorSearchType, count);
        return this;
      }

      public QueryFeaturesMetricsBuilder knnBetaFilterOperatorTypeCount(
          Operator.Type operatorType, double count) {
        this.knnBetaFilterOperatorTypeCounterMap.put(operatorType, count);
        return this;
      }

      public IndexMetrics.QueryingMetrics.QueryFeaturesMetrics build() {

        return new IndexMetrics.QueryingMetrics.QueryFeaturesMetrics(
            this.collectorTypeCountMap,
            this.operatorTypeCountMap,
            this.scoreTypeCountMap,
            this.textMatchCriteriaCountMap,
            this.noDataSortPositionCountMap,
            this.vectorSearchQueryTypeCountMap,
            this.knnBetaFilterOperatorTypeCounterMap,
            this.searchVectorSearchFilterOperatorTypeCounterMap,
            this.synonymCount,
            this.textSynonymCount,
            this.textSynonymsWithoutMatchCriteriaCount,
            this.phraseSynonymCount,
            this.fuzzyCount,
            this.wildcardPathCount,
            this.highlightingCount,
            this.concurrentCount,
            this.returnStoredSourceCount,
            this.sequenceTokenCount,
            this.requireSequenceTokensCount,
            this.scoreDetailsCount,
            this.explainCount,
            this.sortCount,
            this.trackingCount,
            this.returnScopeCount);
      }
    }
  }

  public static class IndexingMetricsBuilder {
    private final Map<EventType, Double> documentEventTypeCountMap = initEnumMap(EventType.class);
    private double initialSyncExceptionCount = 0.0;
    private double steadyStateExceptionCount = 0.0;
    private Optional<BsonTimestamp> replicationOpTime = Optional.empty();
    private Optional<Long> replicationLagMs = Optional.empty();
    private double totalBytesProcessed = 0.0;
    private long numDocs = 0;
    private int numLuceneMaxDocs = 0;
    private int maxLuceneMaxDocs = 0;
    private long numMongoDbDocs = 0;
    private Bytes indexSize = Bytes.ofBytes(0L);
    private Bytes vectorFieldSize = Bytes.ofBytes(0L);
    private int numFields = 0;
    private final Map<FieldName.TypeField, Double> numFieldsPerDatatype =
        initEnumMap(FieldName.TypeField.class);

    private SerializableTimer batchIndexingTimer = SerializableTimerBuilder.builder().build();

    private Bytes requiredMemory = Bytes.ofBytes(0L);

    public static IndexingMetricsBuilder builder() {
      return new IndexingMetricsBuilder();
    }

    public IndexingMetricsBuilder replicationOpTime(BsonTimestamp replicationOpTime) {
      this.replicationOpTime = Optional.of(replicationOpTime);
      return this;
    }

    public IndexingMetricsBuilder replicationLagMs(long replicationLagMs) {
      this.replicationLagMs = Optional.of(replicationLagMs);
      return this;
    }

    public IndexingMetricsBuilder totalBytesProcessed(double totalBytesProcessed) {
      this.totalBytesProcessed = totalBytesProcessed;
      return this;
    }

    public IndexingMetricsBuilder numDocs(long numDocs) {
      this.numDocs = numDocs;
      return this;
    }

    public IndexingMetricsBuilder numLuceneMaxDocs(int numLuceneMaxDocs) {
      this.numLuceneMaxDocs = numLuceneMaxDocs;
      return this;
    }

    public IndexingMetricsBuilder maxLuceneMaxDocs(int maxLuceneMaxDocs) {
      this.maxLuceneMaxDocs = maxLuceneMaxDocs;
      return this;
    }

    public IndexingMetricsBuilder numMongoDbDocs(long numMongoDbDocs) {
      this.numMongoDbDocs = numMongoDbDocs;
      return this;
    }

    public IndexingMetricsBuilder indexSize(long indexSize) {
      this.indexSize = Bytes.ofBytes(indexSize);
      return this;
    }

    public IndexingMetricsBuilder vectorFieldSize(long vectorFieldSize) {
      this.vectorFieldSize = Bytes.ofBytes(vectorFieldSize);
      return this;
    }

    public IndexingMetricsBuilder requiredMemory(long requiredMemory) {
      this.requiredMemory = Bytes.ofBytes(requiredMemory);
      return this;
    }

    public IndexingMetricsBuilder numFields(int numFields) {
      this.numFields = numFields;
      return this;
    }

    public IndexingMetricsBuilder numFieldsPerDatatype(
        FieldName.TypeField typeField, double count) {
      this.numFieldsPerDatatype.put(typeField, count);
      return this;
    }

    public IndexingMetricsBuilder initialSyncExceptionCount(double initialSyncExceptionCount) {
      this.initialSyncExceptionCount = initialSyncExceptionCount;
      return this;
    }

    public IndexingMetricsBuilder steadyStateExceptionCount(double steadyStateExceptionCount) {
      this.steadyStateExceptionCount = steadyStateExceptionCount;
      return this;
    }

    public IndexingMetricsBuilder batchIndexingTimer(SerializableTimer batchIndexingTimer) {
      this.batchIndexingTimer = batchIndexingTimer;
      return this;
    }

    public IndexingMetricsBuilder documentEventTypeCount(
        DocumentEvent.EventType eventType, double count) {
      this.documentEventTypeCountMap.put(eventType, count);
      return this;
    }

    public IndexMetrics.IndexingMetrics build() {
      return new IndexMetrics.IndexingMetrics(
          this.documentEventTypeCountMap,
          this.initialSyncExceptionCount,
          this.steadyStateExceptionCount,
          this.replicationOpTime,
          this.replicationLagMs,
          this.totalBytesProcessed,
          this.numDocs,
          this.numLuceneMaxDocs,
          this.maxLuceneMaxDocs,
          this.numMongoDbDocs,
          this.indexSize,
          this.vectorFieldSize,
          this.numFields,
          this.numFieldsPerDatatype,
          Optional.of(this.batchIndexingTimer),
          this.requiredMemory);
    }
  }

  private static <T extends Enum<T>> Map<T, Double> initEnumMap(Class<T> enumClass) {
    return Stream.of(enumClass.getEnumConstants())
        .collect(
            Collectors.toMap(
                type -> type,
                type -> 0.0,
                (oldVal, newVal) -> Check.unreachable(),
                () -> new EnumMap<>(enumClass)));
  }
}
