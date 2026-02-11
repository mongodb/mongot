package com.xgen.mongot.index;

import static com.xgen.mongot.util.Check.checkArg;

import com.google.common.base.CaseFormat;
import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.lucene.field.FieldName;
import com.xgen.mongot.index.query.collectors.Collector;
import com.xgen.mongot.index.query.operators.Operator;
import com.xgen.mongot.index.query.operators.TextOperator;
import com.xgen.mongot.index.query.operators.VectorSearchCriteria;
import com.xgen.mongot.index.query.scores.Score;
import com.xgen.mongot.index.query.sort.NullEmptySortPosition;
import com.xgen.mongot.index.status.IndexStatus;
import com.xgen.mongot.metrics.micrometer.SerializableTimer;
import com.xgen.mongot.util.Bytes;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.Enums;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonTimestamp;

public record IndexMetrics(
    com.xgen.mongot.index.IndexMetrics.IndexMetadata indexMetadata,
    com.xgen.mongot.index.IndexMetrics.IndexingMetrics indexingMetrics,
    com.xgen.mongot.index.IndexMetrics.QueryingMetrics queryingMetrics)
    implements DocumentEncodable {

  private static class Fields {

    private static final Field.Required<QueryingMetrics> QUERYING_METRICS =
        Field.builder("query")
            .classField(QueryingMetrics::fromBson)
            .allowUnknownFields()
            .required();
  }

  static IndexMetrics create(
      IndexDefinition indexDefinition,
      IndexStatus indexStatus,
      IndexingMetrics indexingMetrics,
      QueryingMetrics queryingMetrics) {
    return new IndexMetrics(
        IndexMetadata.create(indexDefinition, indexStatus), indexingMetrics, queryingMetrics);
  }

  @Override
  public BsonDocument toBson() {
    BsonDocument indexStats = new BsonDocument();
    indexStats.putAll(this.indexMetadata.toBson());
    indexStats.putAll(this.indexingMetrics.toBson());
    indexStats.putAll(
        BsonDocumentBuilder.builder().field(Fields.QUERYING_METRICS, this.queryingMetrics).build());
    return indexStats;
  }

  public static IndexMetrics fromBson(DocumentParser parser) throws BsonParseException {
    return new IndexMetrics(
        IndexMetadata.fromBson(parser),
        IndexingMetrics.fromBson(parser),
        parser.getField(Fields.QUERYING_METRICS).unwrap());
  }

  public record IndexMetadata(
      UUID collectionUuid,
      String lastObservedCollectionName,
      String dbName,
      IndexStatus.StatusCode indexStatus)
      implements DocumentEncodable {

    private static class Fields {

      private static final Field.Required<UUID> COLLECTION_UUID =
          Field.builder("collectionUuid").uuidField().encodeAsString().required();

      private static final Field.Required<String> LAST_OBSERVED_COLL_NAME =
          Field.builder("lastObservedCollectionName").stringField().required();

      private static final Field.Required<String> DB_NAME =
          Field.builder("dbName").stringField().required();

      private static final Field.Required<IndexStatus.StatusCode> INDEX_STATUS =
          Field.builder("indexStatus")
              .enumField(IndexStatus.StatusCode.class)
              .asCamelCase()
              .required();
    }

    public static IndexMetadata create(IndexDefinition indexDefinition, IndexStatus indexStatus) {
      return new IndexMetadata(
          indexDefinition.getCollectionUuid(),
          indexDefinition.getLastObservedCollectionName(),
          indexDefinition.getDatabase(),
          indexStatus.getStatusCode());
    }

    @Override
    public BsonDocument toBson() {
      return BsonDocumentBuilder.builder()
          .field(Fields.COLLECTION_UUID, this.collectionUuid)
          .field(Fields.DB_NAME, this.dbName)
          .field(Fields.LAST_OBSERVED_COLL_NAME, this.lastObservedCollectionName)
          .field(Fields.INDEX_STATUS, this.indexStatus)
          .build();
    }

    public static IndexMetadata fromBson(DocumentParser parser) throws BsonParseException {
      return new IndexMetadata(
          parser.getField(Fields.COLLECTION_UUID).unwrap(),
          parser.getField(Fields.LAST_OBSERVED_COLL_NAME).unwrap(),
          parser.getField(Fields.DB_NAME).unwrap(),
          parser.getField(Fields.INDEX_STATUS).unwrap());
    }
  }

  public record IndexingMetrics(
      Map<DocumentEvent.EventType, Double> appliedDocumentEventTypeCountMap,
      double initialSyncExceptionCount,
      double steadyStateExceptionCount,
      Optional<BsonTimestamp> replicationOpTime,
      Optional<Long> replicationLagMs,
      double totalBytesProcessed,
      long numDocs,
      long numLuceneMaxDocs,
      int maxLuceneMaxDocs,
      long numMongoDbDocs,
      Bytes indexSize,
      Bytes vectorFieldSize,
      int numFields,
      Map<FieldName.TypeField, Double> numFieldsPerDatatype,
      Optional<SerializableTimer> batchIndexingTimer,
      Bytes requiredMemory)
      implements DocumentEncodable {

    private static class Fields {

      private static final Field.Required<BsonDocument> APPLIED_DOC_EVENT_TYPE_COUNT =
          Field.builder("appliedDocumentEventTypeCount").documentField().required();

      private static final Field.Required<Double> INITIAL_SYNC_EXCEPTION_COUNT =
          Field.builder("initialSyncExceptionCount").doubleField().required();

      private static final Field.Required<Double> STEADY_STATE_EXCEPTION_COUNT =
          Field.builder("steadyStateExceptionCount").doubleField().required();

      private static final Field.Optional<BsonTimestamp> REPLICATION_OP_TIME =
          Field.builder("replicationOpTime").bsonTimestampField().optional().noDefault();

      private static final Field.Optional<Long> REPLICATION_LAG_MS =
          Field.builder("replicationLagMs").longField().optional().noDefault();

      private static final Field.Required<Double> TOTAL_BYTES_PROCESSED =
          Field.builder("totalBytesProcessed").doubleField().required();

      private static final Field.Required<Long> NUM_DOCS =
          Field.builder("numDocs").longField().required();

      private static final Field.Required<Long> NUM_LUCENE_MAX_DOCS =
          Field.builder("numLuceneMaxDocs").longField().required();

      private static final Field.Required<Integer> MAX_LUCENE_MAX_DOCS =
          Field.builder("maxLuceneMaxDocs").intField().required();

      private static final Field.Required<Long> NUM_MONGO_DB_DOCS =
          Field.builder("numMongoDbDocs").longField().required();

      private static final Field.Required<Long> INDEX_SIZE =
          Field.builder("indexSizeBytes").longField().required();

      // Optional with default so that indexStats in the downgrade test succeeds.
      // This value is currently deprecated and will always be set to 0 to pass downgrade tests
      private static final Field.WithDefault<Long> VECTOR_FIELD_SIZE_DEPRECATED =
          Field.builder("vectorFieldSizeBytes").longField().optional().withDefault(0L);

      private static final Field.Required<Integer> NUM_FIELDS =
          Field.builder("numFields").intField().required();

      private static final Field.WithDefault<BsonDocument> NUM_FIELDS_PER_DATATYPE =
          Field.builder("numFieldsPerDatatype")
              .documentField()
              .optional()
              .withDefault(statsMapToBson(statsMapWithDefault(FieldName.TypeField.class, 0.0)));

      // Records the duration to finish indexing work for a whole IndexingSchedulerBatch
      private static final Field.Optional<SerializableTimer> BATCH_INDEXING_TIMER =
          Field.builder("batchIndexingTimer")
              .classField(SerializableTimer::fromBson)
              .disallowUnknownFields()
              .optional()
              .noDefault();

      private static final Field.WithDefault<Long> REQUIRED_MEMORY =
          Field.builder("requiredMemoryBytes").longField().optional().withDefault(0L);
    }

    static IndexingMetrics create(
        Map<DocumentEvent.EventType, Counter> documentEventTypeCounterMap,
        Counter initialSyncExceptionCounter,
        Counter steadyStateExceptionCounter,
        Optional<ReplicationOpTimeInfo.Snapshot> opTimeInfoSnapshot,
        Counter totalBytesProcessedCounter,
        long indexSize,
        long vectorFieldSize,
        int numFields,
        Map<FieldName.TypeField, Double> numFieldsPerDatatype,
        long numDocs,
        long numLuceneMaxDocs,
        int maxLuceneMaxDocs,
        long numMongoDbDocs,
        Timer batchIndexingTimer,
        long requiredMemory) {
      return new IndexingMetrics(
          getCurrentStatsFromCounters(DocumentEvent.EventType.class, documentEventTypeCounterMap),
          initialSyncExceptionCounter.count(),
          steadyStateExceptionCounter.count(),
          opTimeInfoSnapshot.map(info -> new BsonTimestamp(info.replicationOpTime())),
          opTimeInfoSnapshot.map(ReplicationOpTimeInfo.Snapshot::replicationLagMs),
          totalBytesProcessedCounter.count(),
          numDocs,
          numLuceneMaxDocs,
          maxLuceneMaxDocs,
          numMongoDbDocs,
          Bytes.ofBytes(indexSize),
          Bytes.ofBytes(vectorFieldSize),
          numFields,
          getCurrentStatsFromDouble(FieldName.TypeField.class, numFieldsPerDatatype),
          Optional.of(SerializableTimer.create(batchIndexingTimer)),
          Bytes.ofBytes(requiredMemory));
    }

    @Override
    public BsonDocument toBson() {
      return BsonDocumentBuilder.builder()
          .field(Fields.REPLICATION_OP_TIME, this.replicationOpTime)
          .field(Fields.REPLICATION_LAG_MS, this.replicationLagMs)
          .field(Fields.TOTAL_BYTES_PROCESSED, this.totalBytesProcessed)
          .field(Fields.NUM_DOCS, this.numDocs)
          .field(Fields.NUM_LUCENE_MAX_DOCS, this.numLuceneMaxDocs)
          .field(Fields.MAX_LUCENE_MAX_DOCS, this.maxLuceneMaxDocs)
          .field(Fields.NUM_MONGO_DB_DOCS, this.numMongoDbDocs)
          .field(Fields.INDEX_SIZE, this.indexSize.toBytes())
          .field(Fields.VECTOR_FIELD_SIZE_DEPRECATED, 0L)
          .field(Fields.NUM_FIELDS, this.numFields)
          .field(Fields.NUM_FIELDS_PER_DATATYPE, statsMapToBson(this.numFieldsPerDatatype))
          .field(
              Fields.APPLIED_DOC_EVENT_TYPE_COUNT,
              statsMapToBson(this.appliedDocumentEventTypeCountMap))
          .field(Fields.INITIAL_SYNC_EXCEPTION_COUNT, this.initialSyncExceptionCount)
          .field(Fields.STEADY_STATE_EXCEPTION_COUNT, this.steadyStateExceptionCount)
          .field(Fields.BATCH_INDEXING_TIMER, this.batchIndexingTimer)
          .field(Fields.REQUIRED_MEMORY, this.requiredMemory.toBytes())
          .build();
    }

    public static IndexingMetrics fromBson(DocumentParser parser) throws BsonParseException {
      return new IndexingMetrics(
          bsonToStatsMap(
              parser.getField(Fields.APPLIED_DOC_EVENT_TYPE_COUNT).unwrap(),
              DocumentEvent.EventType.class),
          parser.getField(Fields.INITIAL_SYNC_EXCEPTION_COUNT).unwrap(),
          parser.getField(Fields.STEADY_STATE_EXCEPTION_COUNT).unwrap(),
          parser.getField(Fields.REPLICATION_OP_TIME).unwrap(),
          parser.getField(Fields.REPLICATION_LAG_MS).unwrap(),
          parser.getField(Fields.TOTAL_BYTES_PROCESSED).unwrap(),
          parser.getField(Fields.NUM_DOCS).unwrap(),
          parser.getField(Fields.NUM_LUCENE_MAX_DOCS).unwrap(),
          parser.getField(Fields.MAX_LUCENE_MAX_DOCS).unwrap(),
          parser.getField(Fields.NUM_MONGO_DB_DOCS).unwrap(),
          Bytes.ofBytes(parser.getField(Fields.INDEX_SIZE).unwrap()),
          Bytes.ofBytes(parser.getField(Fields.VECTOR_FIELD_SIZE_DEPRECATED).unwrap()),
          parser.getField(Fields.NUM_FIELDS).unwrap(),
          bsonToStatsMap(
              parser.getField(Fields.NUM_FIELDS_PER_DATATYPE).unwrap(), FieldName.TypeField.class),
          parser.getField(Fields.BATCH_INDEXING_TIMER).unwrap(),
          Bytes.ofBytes(parser.getField(Fields.REQUIRED_MEMORY).unwrap()));
    }
  }

  public record QueryingMetrics(
      double totalQueryCount,
      double failedQueryCount,
      double lenientFailureCount,
      double searchGetMoreCommandCount,
      SerializableTimer searchResultBatchLatencyStats,
      Optional<SerializableTimer> tokenFacetsStateRefreshLatency,
      QueryFeaturesMetrics queryFeaturesMetrics)
      implements DocumentEncodable {

    private static class Fields {

      private static final Field.Required<Double> TOTAL_QUERIES_COUNT =
          Field.builder("totalQueriesCount").doubleField().required();

      private static final Field.Required<Double> FAILED_QUERIES_COUNT =
          Field.builder("failedQueriesCount").doubleField().required();

      private static final Field.Required<Double> LENIENT_FAILURES_COUNT =
          Field.builder("lenientFailureCount").doubleField().required();

      // TODO(CLOUDP-110899): remove searchGetMoreCommandCount
      private static final Field.Required<Double> SEARCH_GET_MORE_COMMAND_COUNT =
          Field.builder("searchGetMoreCommandCount").doubleField().required();

      /**
       * Time spent on retrieving data from Lucene in scope of a single request from mongod (e.g.
       * search or getMore commands)
       */
      private static final Field.Required<SerializableTimer> SEARCH_GET_RESULT_BATCH_LATENCY =
          Field.builder("searchResultBatchLatency")
              .classField(SerializableTimer::fromBson)
              .disallowUnknownFields()
              .required();

      /** Time spent on refresh facets on token fields when index searcher refreshes */
      private static final Field.Optional<SerializableTimer> TOKEN_FACET_STATE_REFRESH_LATENCY =
          Field.builder("tokenFacetsStateRefreshLatency")
              .classField(SerializableTimer::fromBson)
              .disallowUnknownFields()
              .optional()
              .noDefault();
    }

    static QueryingMetrics create(
        Counter totalQueryCounter,
        Counter failedQueryCounter,
        Counter lenientFailureCounter,
        Counter searchGetMoreCommandCounter,
        Timer searchResultBatchLatencyTimer,
        Timer tokenFacetsStateRefreshLatencyTimer,
        QueryFeaturesMetrics queryFeaturesMetrics) {

      return new QueryingMetrics(
          totalQueryCounter.count(),
          failedQueryCounter.count(),
          lenientFailureCounter.count(),
          searchGetMoreCommandCounter.count(),
          SerializableTimer.create(searchResultBatchLatencyTimer),
          Optional.of(SerializableTimer.create(tokenFacetsStateRefreshLatencyTimer)),
          queryFeaturesMetrics);
    }

    @Override
    public BsonDocument toBson() {
      BsonDocument queryingStats =
          BsonDocumentBuilder.builder()
              .field(Fields.TOTAL_QUERIES_COUNT, this.totalQueryCount)
              .field(Fields.FAILED_QUERIES_COUNT, this.failedQueryCount)
              .field(Fields.LENIENT_FAILURES_COUNT, this.lenientFailureCount)
              .field(Fields.SEARCH_GET_MORE_COMMAND_COUNT, this.searchGetMoreCommandCount)
              .field(Fields.SEARCH_GET_RESULT_BATCH_LATENCY, this.searchResultBatchLatencyStats)
              .field(Fields.TOKEN_FACET_STATE_REFRESH_LATENCY, this.tokenFacetsStateRefreshLatency)
              .build();

      queryingStats.putAll(this.queryFeaturesMetrics.toBson());

      return queryingStats;
    }

    public static QueryingMetrics fromBson(DocumentParser parser) throws BsonParseException {
      return new QueryingMetrics(
          parser.getField(Fields.TOTAL_QUERIES_COUNT).unwrap(),
          parser.getField(Fields.FAILED_QUERIES_COUNT).unwrap(),
          parser.getField(Fields.LENIENT_FAILURES_COUNT).unwrap(),
          parser.getField(Fields.SEARCH_GET_MORE_COMMAND_COUNT).unwrap(),
          parser.getField(Fields.SEARCH_GET_RESULT_BATCH_LATENCY).unwrap(),
          parser.getField(Fields.TOKEN_FACET_STATE_REFRESH_LATENCY).unwrap(),
          QueryFeaturesMetrics.fromBson(parser));
    }

    public record QueryFeaturesMetrics(
        Map<Collector.Type, Double> collectorTypeCountMap,
        Map<Operator.Type, Double> operatorTypeCountMap,
        Map<Score.Type, Double> scoreTypeCountMap,
        Map<TextOperator.MatchCriteria, Double> textMatchCriteriaCountMap,
        Map<NullEmptySortPosition, Double> noDataSortPositionCountMap,
        Map<VectorSearchCriteria.Type, Double> vectorSearchQueryTypeCountMap,
        Map<Operator.Type, Double> knnBetaFilterOperatorTypeCounterMap,
        Map<Operator.Type, Double> searchVectorSearchFilterOperatorTypeCounterMap,
        double synonymCount,
        double textSynonymCount,
        double textSynonymsWithoutMatchCriteriaCount,
        double phraseSynonymCount,
        double fuzzyCount,
        double wildcardPathCount,
        double highlightingCount,
        double concurrentCount,
        double returnStoredSourceCount,
        double sequenceTokenCount,
        double requireSequenceTokensCount,
        double scoreDetailsCount,
        double explainCount,
        double sortCount,
        double trackingCount,
        double returnScopeCount)
        implements DocumentEncodable {

      private static class Fields {

        public static final Field.Required<Double> SEQUENCE_TOKEN_COUNT =
            Field.builder("sequenceTokenCount").doubleField().required();

        public static final Field.WithDefault<Double> REQUIRE_SEQUENCE_TOKENS_COUNT =
            Field.builder("requireSequenceTokensCount").doubleField().optional().withDefault(0.0);

        public static final Field.WithDefault<Double> SCORE_DETAILS_COUNT =
            Field.builder("scoreDetails").doubleField().optional().withDefault(0.0);

        public static final Field.WithDefault<Double> EXPLAIN_COUNT =
            Field.builder("explainCount").doubleField().optional().withDefault(0.0);

        // TODO(CLOUDP-272535): Stop populating this. It duplicates
        // textSynonymCount + phraseSynomymCount
        private static final Field.Required<Double> SYNONYM_COUNT =
            Field.builder("synonymCount").doubleField().required();

        private static final Field.WithDefault<Double> TEXT_SYNONYM_COUNT =
            Field.builder("textSynonymCount").doubleField().optional().withDefault(0.0);

        private static final Field.WithDefault<Double> PHRASE_SYNONYM_COUNT =
            Field.builder("phraseSynonymCount").doubleField().optional().withDefault(0.0);

        private static final Field.WithDefault<Double> TEXT_DEPRECATED_SYNONYM_COUNT =
            Field.builder("textSynonymsWithoutMatchCriteriaCount")
                .doubleField()
                .optional()
                .withDefault(0.0);

        private static final Field.Required<Double> FUZZY_COUNT =
            Field.builder("fuzzyCount").doubleField().required();

        private static final Field.Required<Double> WILDCARD_PATH_COUNT =
            Field.builder("wildcardPathCount").doubleField().required();

        private static final Field.Required<Double> HIGHLIGHTING_COUNT =
            Field.builder("highlightingCount").doubleField().required();

        private static final Field.Required<Double> CONCURRENT_COUNT =
            Field.builder("concurrentCount").doubleField().required();

        private static final Field.WithDefault<Double> RETURN_STORED_SOURCE_COUNT =
            Field.builder("returnStoredSourceCount").doubleField().optional().withDefault(0.0);

        private static final Field.Required<Double> SORT_COUNT =
            Field.builder("sortCount").doubleField().required();

        private static final Field.Required<Double> TRACKING_COUNT =
            Field.builder("trackingCount").doubleField().required();

        private static final Field.WithDefault<Double> RETURNSCOPE_COUNT =
            Field.builder("returnScopeCount").doubleField().optional().withDefault(0.0);

        private static final Field.Required<BsonDocument> COLLECTOR_TYPE_COUNT =
            Field.builder("collectorTypeCount").documentField().required();

        private static final Field.Required<BsonDocument> OPERATOR_TYPE_COUNT =
            Field.builder("operatorTypeCount").documentField().required();

        private static final Field.WithDefault<BsonDocument> TEXT_MATCH_CRITERIA_COUNT =
            Field.builder("textMatchCriteriaCount")
                .documentField()
                .optional()
                .withDefault(
                    statsMapToBson(statsMapWithDefault(TextOperator.MatchCriteria.class, 0.0)));

        private static final Field.WithDefault<BsonDocument> NO_DATA_SORT_POSITION_COUNT =
            Field.builder("noDataSortPositionCount")
                .documentField()
                .optional()
                .withDefault(statsMapToBson(statsMapWithDefault(NullEmptySortPosition.class, 0.0)));

        private static final Field.Required<BsonDocument> SCORE_TYPE_COUNT =
            Field.builder("scoreTypeCount").documentField().required();

        private static final Field.WithDefault<BsonDocument> VECTOR_QUERY_TYPE_COUNT =
            Field.builder("vectorQueryTypeCount")
                .documentField()
                .optional()
                .withDefault(
                    statsMapToBson(statsMapWithDefault(VectorSearchCriteria.Type.class, 0.0)));

        private static final Field.WithDefault<BsonDocument> KNN_BETA_FILTER_OPERATOR_TYPE_COUNT =
            Field.builder("knnBetaFilterOperatorTypeCount")
                .documentField()
                .optional()
                .withDefault(statsMapToBson(statsMapWithDefault(Operator.Type.class, 0.0)));

        private static final Field.WithDefault<BsonDocument>
            SEARCH_VECTOR_SEARCH_FILTER_OPERATOR_TYPE_COUNT =
                Field.builder("searchVectorSearchFilterOperatorTypeCount")
                    .documentField()
                    .optional()
                    .withDefault(statsMapToBson(statsMapWithDefault(Operator.Type.class, 0.0)));
      }

      static QueryFeaturesMetrics create(
          Map<Collector.Type, Counter> collectorTypeCounterMap,
          Map<Operator.Type, Counter> operatorTypeCounterMap,
          Map<Score.Type, Counter> scoreTypeCounterMap,
          Map<TextOperator.MatchCriteria, Counter> textMatchCriteriaCounterMap,
          Map<NullEmptySortPosition, Counter> noDataSortPositionCountMap,
          Map<VectorSearchCriteria.Type, Counter> vectorSearchQueryCounterMap,
          Map<Operator.Type, Counter> knnBetaFilterOperatorTypeCounterMap,
          Map<Operator.Type, Counter> searchVectorSearchFilterOperatorTypeCounterMap,
          Counter synonymCounter,
          Counter textSynonymCounter,
          Counter textSynonymsWithoutMatchCriteriaCounter,
          Counter phraseSynonymCounter,
          Counter fuzzyCounter,
          Counter wildcardPathCounter,
          Counter highlightingCounter,
          Counter concurrentCounter,
          Counter returnStoredSourceCounter,
          Counter sequenceTokenCounter,
          Counter scoreDetailsCounter,
          Counter explainCounter,
          Counter sortCounter,
          Counter trackingCounter,
          Counter requireSequenceTokensCounter,
          Counter returnScopeCounter) {
        return new QueryFeaturesMetrics(
            getCurrentStatsFromCounters(Collector.Type.class, collectorTypeCounterMap),
            getCurrentStatsFromCounters(Operator.Type.class, operatorTypeCounterMap),
            getCurrentStatsFromCounters(Score.Type.class, scoreTypeCounterMap),
            getCurrentStatsFromCounters(
                TextOperator.MatchCriteria.class, textMatchCriteriaCounterMap),
            getCurrentStatsFromCounters(NullEmptySortPosition.class, noDataSortPositionCountMap),
            getCurrentStatsFromCounters(
                VectorSearchCriteria.Type.class, vectorSearchQueryCounterMap),
            getCurrentStatsFromCounters(Operator.Type.class, knnBetaFilterOperatorTypeCounterMap),
            getCurrentStatsFromCounters(
                Operator.Type.class, searchVectorSearchFilterOperatorTypeCounterMap),
            synonymCounter.count(),
            textSynonymCounter.count(),
            textSynonymsWithoutMatchCriteriaCounter.count(),
            phraseSynonymCounter.count(),
            fuzzyCounter.count(),
            wildcardPathCounter.count(),
            highlightingCounter.count(),
            concurrentCounter.count(),
            returnStoredSourceCounter.count(),
            sequenceTokenCounter.count(),
            requireSequenceTokensCounter.count(),
            scoreDetailsCounter.count(),
            explainCounter.count(),
            sortCounter.count(),
            trackingCounter.count(),
            returnScopeCounter.count());
      }

      @Override
      public BsonDocument toBson() {
        return BsonDocumentBuilder.builder()
            .field(Fields.SYNONYM_COUNT, this.synonymCount)
            .field(Fields.TEXT_SYNONYM_COUNT, this.textSynonymCount)
            .field(Fields.TEXT_DEPRECATED_SYNONYM_COUNT, this.textSynonymsWithoutMatchCriteriaCount)
            .field(Fields.PHRASE_SYNONYM_COUNT, this.phraseSynonymCount)
            .field(Fields.FUZZY_COUNT, this.fuzzyCount)
            .field(Fields.WILDCARD_PATH_COUNT, this.wildcardPathCount)
            .field(Fields.HIGHLIGHTING_COUNT, this.highlightingCount)
            .field(Fields.HIGHLIGHTING_COUNT, this.highlightingCount)
            .field(Fields.CONCURRENT_COUNT, this.concurrentCount)
            .field(Fields.RETURN_STORED_SOURCE_COUNT, this.returnStoredSourceCount)
            .field(Fields.SEQUENCE_TOKEN_COUNT, this.sequenceTokenCount)
            .field(Fields.REQUIRE_SEQUENCE_TOKENS_COUNT, this.requireSequenceTokensCount)
            .field(Fields.SCORE_DETAILS_COUNT, this.scoreDetailsCount)
            .field(Fields.EXPLAIN_COUNT, this.explainCount)
            .field(Fields.SORT_COUNT, this.sortCount)
            .field(Fields.TRACKING_COUNT, this.trackingCount)
            .field(Fields.RETURNSCOPE_COUNT, this.returnScopeCount)
            .field(Fields.COLLECTOR_TYPE_COUNT, statsMapToBson(this.collectorTypeCountMap))
            .field(Fields.OPERATOR_TYPE_COUNT, statsMapToBson(this.operatorTypeCountMap))
            .field(Fields.SCORE_TYPE_COUNT, statsMapToBson(this.scoreTypeCountMap))
            .field(Fields.TEXT_MATCH_CRITERIA_COUNT, statsMapToBson(this.textMatchCriteriaCountMap))
            .field(
                Fields.NO_DATA_SORT_POSITION_COUNT, statsMapToBson(this.noDataSortPositionCountMap))
            .field(
                Fields.VECTOR_QUERY_TYPE_COUNT, statsMapToBson(this.vectorSearchQueryTypeCountMap))
            .field(
                Fields.KNN_BETA_FILTER_OPERATOR_TYPE_COUNT,
                statsMapToBson(this.knnBetaFilterOperatorTypeCounterMap))
            .field(
                Fields.SEARCH_VECTOR_SEARCH_FILTER_OPERATOR_TYPE_COUNT,
                statsMapToBson(this.searchVectorSearchFilterOperatorTypeCounterMap))
            .build();
      }

      public static QueryFeaturesMetrics fromBson(DocumentParser parser) throws BsonParseException {

        return new QueryFeaturesMetrics(
            bsonToStatsMap(
                parser.getField(Fields.COLLECTOR_TYPE_COUNT).unwrap(), Collector.Type.class),
            bsonToStatsMap(
                parser.getField(Fields.OPERATOR_TYPE_COUNT).unwrap(), Operator.Type.class),
            bsonToStatsMap(parser.getField(Fields.SCORE_TYPE_COUNT).unwrap(), Score.Type.class),
            bsonToStatsMap(
                parser.getField(Fields.TEXT_MATCH_CRITERIA_COUNT).unwrap(),
                TextOperator.MatchCriteria.class),
            bsonToStatsMap(
                parser.getField(Fields.NO_DATA_SORT_POSITION_COUNT).unwrap(),
                NullEmptySortPosition.class),
            bsonToStatsMap(
                parser.getField(Fields.VECTOR_QUERY_TYPE_COUNT).unwrap(),
                VectorSearchCriteria.Type.class),
            bsonToStatsMap(
                parser.getField(Fields.KNN_BETA_FILTER_OPERATOR_TYPE_COUNT).unwrap(),
                Operator.Type.class),
            bsonToStatsMap(
                parser.getField(Fields.SEARCH_VECTOR_SEARCH_FILTER_OPERATOR_TYPE_COUNT).unwrap(),
                Operator.Type.class),
            parser.getField(Fields.SYNONYM_COUNT).unwrap(),
            parser.getField(Fields.TEXT_SYNONYM_COUNT).unwrap(),
            parser.getField(Fields.TEXT_DEPRECATED_SYNONYM_COUNT).unwrap(),
            parser.getField(Fields.PHRASE_SYNONYM_COUNT).unwrap(),
            parser.getField(Fields.FUZZY_COUNT).unwrap(),
            parser.getField(Fields.WILDCARD_PATH_COUNT).unwrap(),
            parser.getField(Fields.HIGHLIGHTING_COUNT).unwrap(),
            parser.getField(Fields.CONCURRENT_COUNT).unwrap(),
            parser.getField(Fields.RETURN_STORED_SOURCE_COUNT).unwrap(),
            parser.getField(Fields.SEQUENCE_TOKEN_COUNT).unwrap(),
            parser.getField(Fields.REQUIRE_SEQUENCE_TOKENS_COUNT).unwrap(),
            parser.getField(Fields.SCORE_DETAILS_COUNT).unwrap(),
            parser.getField(Fields.EXPLAIN_COUNT).unwrap(),
            parser.getField(Fields.SORT_COUNT).unwrap(),
            parser.getField(Fields.TRACKING_COUNT).unwrap(),
            parser.getField(Fields.RETURNSCOPE_COUNT).unwrap());
      }
    }
  }

  private static <T extends Enum<T>> Map<T, Double> getCurrentStatsFromDouble(
      Class<T> enumClass, Map<T, Double> enumMap) {
    checkArg(enumClass.isEnum(), "enumClass '%s' must be an Enum", enumClass);
    return Arrays.stream(enumClass.getEnumConstants())
        .collect(
            Collectors.toMap(
                Function.identity(),
                key -> enumMap.getOrDefault(key, 0.0),
                (a, b) -> Check.unreachable("Duplicate enum key should not occur"),
                () -> new EnumMap<>(enumClass)));
  }

  private static <T extends Enum<T>> Map<T, Double> getCurrentStatsFromCounters(
      Class<T> enumClass, Map<T, Counter> enumMap) {
    checkArg(enumClass.isEnum(), "enumClass '%s' must be an Enum", enumClass);
    return enumMap.entrySet().stream()
        .collect(
            Collectors.toMap(
                Map.Entry::getKey,
                entry -> entry.getValue().count(),
                (prevCount, newCount) -> Check.unreachable("Duplicate enum key should not occur"),
                () -> new EnumMap<>(enumClass)));
  }

  private static <T extends Enum<T>> BsonDocument statsMapToBson(Map<T, Double> currentStats) {
    return currentStats.entrySet().stream()
        .collect(
            Collectors.toMap(
                entry -> Enums.convertNameTo(CaseFormat.LOWER_CAMEL, entry.getKey()),
                entry -> new BsonDouble(entry.getValue()),
                (prevCountBson, newCountBson) -> Check.unreachable(
                    "Duplicate enum key should not occur"),
                BsonDocument::new));
  }

  private static <T extends Enum<T>> Map<T, Double> bsonToStatsMap(
      BsonDocument document, Class<T> enumClass) {
    checkArg(enumClass.isEnum(), "provided class is not Enum");
    return Stream.of(enumClass.getEnumConstants())
        .collect(
            Collectors.toMap(
                type -> type,
                type ->
                    document
                        .getDouble(
                            // Downgrade compatibility tests will first run the newer version code
                            // and then the older version code. the newer version code consists of
                            // hasAncestor and hasRoot operator invocation stats within
                            // operatorTypeCountMap, and such stats will be persisted in admin db.
                            // As older version code does not have hasAncestor and hasRoot operator
                            // types, getDouble will throw exception for these types.
                            // In order to bypass that for downgrade tests,
                            // we will return default value 0.0 for these operator types.
                            Enums.convertNameTo(CaseFormat.LOWER_CAMEL, type), new BsonDouble(0.0))
                        .getValue(),
                (oldName, newName) -> Check.unreachable("Duplicate enum key should not occur"),
                () -> new EnumMap<>(enumClass)));
  }

  private static <T extends Enum<T>> Map<T, Double> statsMapWithDefault(
      Class<T> enumClass, Double defaultValue) {
    checkArg(enumClass.isEnum(), "provided class is not Enum");
    return Stream.of(enumClass.getEnumConstants())
        .collect(
            Collectors.toMap(
                type -> type,
                type -> defaultValue,
                (oldName, newName) -> Check.unreachable("Duplicate enum key should not occur"),
                () -> new EnumMap<>(enumClass)));
  }
}
