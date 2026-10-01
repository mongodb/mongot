package com.xgen.mongot.config.provider.community;

import com.xgen.mongot.util.DeploymentEnvironment;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import java.util.Optional;
import org.bson.BsonDocument;

/**
 * The {@code autoEmbedding} block of the mongot config file. Tunes auto-embedding materialized
 * views.
 */
public record CommunityAutoEmbeddingConfig(Optional<MaterializedViewConfig> materializedViewConfig)
    implements DocumentEncodable {

  static class Fields {
    public static final Field.Optional<MaterializedViewConfig> MATERIALIZED_VIEW_CONFIG =
        Field.builder("materializedView")
            .classField(MaterializedViewConfig::fromBson)
            .disallowUnknownFields()
            .edition(DeploymentEnvironment.COMMUNITY)
            .optional()
            .noDefault();
  }

  public static CommunityAutoEmbeddingConfig fromBson(DocumentParser parser)
      throws BsonParseException {
    return new CommunityAutoEmbeddingConfig(
        parser.getField(Fields.MATERIALIZED_VIEW_CONFIG).unwrap());
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder()
        .field(Fields.MATERIALIZED_VIEW_CONFIG, this.materializedViewConfig)
        .build();
  }

  public record MaterializedViewConfig(
      Optional<Integer> numConcurrentChangeStreams,
      Optional<Integer> numIndexingThreads,
      Optional<Integer> numEmbeddingThreads,
      Optional<Integer> numConcurrentInitialSyncs,
      Optional<Integer> matViewWriterMaxConnections,
      Optional<Integer> maxInFlightEmbeddingGetMores,
      Optional<Integer> embeddingGetMoreBatchSize,
      Optional<Integer> changeStreamMaxTimeMs,
      Optional<Integer> changeStreamCursorMaxTimeSec,
      Optional<Integer> numChangeStreamDecodingThreads,
      Optional<Integer> requestRateLimitBackoffMs,
      Optional<Integer> mvWriteRateLimitRps,
      Optional<Integer> embeddingProviderRpsLimit,
      Optional<Integer> globalMemoryBudgetHeapPercent,
      Optional<Integer> perBatchMemoryBudgetHeapPercent)
      implements DocumentEncodable {

    static class Fields {
      public static final Field.Optional<Integer> NUM_CONCURRENT_CHANGE_STREAMS =
          Field.builder("numConcurrentChangeStreams")
              .intField()
              .mustBePositive()
              .edition(DeploymentEnvironment.COMMUNITY)
              .optional()
              .noDefault();

      public static final Field.Optional<Integer> NUM_INDEXING_THREADS =
          Field.builder("numIndexingThreads")
              .intField()
              .mustBePositive()
              .edition(DeploymentEnvironment.COMMUNITY)
              .optional()
              .noDefault();

      public static final Field.Optional<Integer> NUM_EMBEDDING_THREADS =
          Field.builder("numEmbeddingThreads")
              .intField()
              .mustBePositive()
              .edition(DeploymentEnvironment.COMMUNITY)
              .optional()
              .noDefault();

      public static final Field.Optional<Integer> NUM_CONCURRENT_INITIAL_SYNCS =
          Field.builder("numConcurrentInitialSyncs")
              .intField()
              .mustBePositive()
              .edition(DeploymentEnvironment.COMMUNITY)
              .optional()
              .noDefault();

      public static final Field.Optional<Integer> MAT_VIEW_WRITER_MAX_CONNECTIONS =
          Field.builder("matViewWriterMaxConnections")
              .intField()
              .mustBePositive()
              .edition(DeploymentEnvironment.COMMUNITY)
              .optional()
              .noDefault();

      public static final Field.Optional<Integer> MAX_IN_FLIGHT_EMBEDDING_GET_MORES =
          Field.builder("maxInFlightEmbeddingGetMores")
              .intField()
              .mustBePositive()
              .edition(DeploymentEnvironment.COMMUNITY)
              .optional()
              .noDefault();

      public static final Field.Optional<Integer> EMBEDDING_GET_MORE_BATCH_SIZE =
          Field.builder("embeddingGetMoreBatchSize")
              .intField()
              .mustBePositive()
              .edition(DeploymentEnvironment.COMMUNITY)
              .optional()
              .noDefault();

      public static final Field.Optional<Integer> CHANGE_STREAM_MAX_TIME_MS =
          Field.builder("changeStreamMaxTimeMs")
              .intField()
              .mustBePositive()
              .edition(DeploymentEnvironment.COMMUNITY)
              .optional()
              .noDefault();

      public static final Field.Optional<Integer> CHANGE_STREAM_CURSOR_MAX_TIME_SEC =
          Field.builder("changeStreamCursorMaxTimeSec")
              .intField()
              .mustBePositive()
              .edition(DeploymentEnvironment.COMMUNITY)
              .optional()
              .noDefault();

      public static final Field.Optional<Integer> NUM_CHANGE_STREAM_DECODING_THREADS =
          Field.builder("numChangeStreamDecodingThreads")
              .intField()
              .mustBePositive()
              .edition(DeploymentEnvironment.COMMUNITY)
              .optional()
              .noDefault();

      public static final Field.Optional<Integer> REQUEST_RATE_LIMIT_BACKOFF_MS =
          Field.builder("requestRateLimitBackoffMs")
              .intField()
              .mustBePositive()
              .edition(DeploymentEnvironment.COMMUNITY)
              .optional()
              .noDefault();

      public static final Field.Optional<Integer> MV_WRITE_RATE_LIMIT_RPS =
          Field.builder("mvWriteRateLimitRps")
              .intField()
              .mustBePositive()
              .edition(DeploymentEnvironment.COMMUNITY)
              .optional()
              .noDefault();

      public static final Field.Optional<Integer> EMBEDDING_PROVIDER_RPS_LIMIT =
          Field.builder("embeddingProviderRpsLimit")
              .intField()
              .mustBePositive()
              .edition(DeploymentEnvironment.COMMUNITY)
              .optional()
              .noDefault();

      public static final Field.Optional<Integer> GLOBAL_MEMORY_BUDGET_HEAP_PERCENT =
          Field.builder("globalMemoryBudgetHeapPercent")
              .intField()
              .mustBePositive()
              .validate(
                  v ->
                      v > 100
                          ? Optional.of("must be between 1 and 100 (inclusive), got " + v)
                          : Optional.empty())
              .edition(DeploymentEnvironment.COMMUNITY)
              .optional()
              .noDefault();

      public static final Field.Optional<Integer> PER_BATCH_MEMORY_BUDGET_HEAP_PERCENT =
          Field.builder("perBatchMemoryBudgetHeapPercent")
              .intField()
              .mustBePositive()
              .validate(
                  v ->
                      v > 100
                          ? Optional.of("must be between 1 and 100 (inclusive), got " + v)
                          : Optional.empty())
              .edition(DeploymentEnvironment.COMMUNITY)
              .optional()
              .noDefault();
    }

    public static MaterializedViewConfig fromBson(DocumentParser parser) throws BsonParseException {
      return new MaterializedViewConfig(
          parser.getField(Fields.NUM_CONCURRENT_CHANGE_STREAMS).unwrap(),
          parser.getField(Fields.NUM_INDEXING_THREADS).unwrap(),
          parser.getField(Fields.NUM_EMBEDDING_THREADS).unwrap(),
          parser.getField(Fields.NUM_CONCURRENT_INITIAL_SYNCS).unwrap(),
          parser.getField(Fields.MAT_VIEW_WRITER_MAX_CONNECTIONS).unwrap(),
          parser.getField(Fields.MAX_IN_FLIGHT_EMBEDDING_GET_MORES).unwrap(),
          parser.getField(Fields.EMBEDDING_GET_MORE_BATCH_SIZE).unwrap(),
          parser.getField(Fields.CHANGE_STREAM_MAX_TIME_MS).unwrap(),
          parser.getField(Fields.CHANGE_STREAM_CURSOR_MAX_TIME_SEC).unwrap(),
          parser.getField(Fields.NUM_CHANGE_STREAM_DECODING_THREADS).unwrap(),
          parser.getField(Fields.REQUEST_RATE_LIMIT_BACKOFF_MS).unwrap(),
          parser.getField(Fields.MV_WRITE_RATE_LIMIT_RPS).unwrap(),
          parser.getField(Fields.EMBEDDING_PROVIDER_RPS_LIMIT).unwrap(),
          parser.getField(Fields.GLOBAL_MEMORY_BUDGET_HEAP_PERCENT).unwrap(),
          parser.getField(Fields.PER_BATCH_MEMORY_BUDGET_HEAP_PERCENT).unwrap());
    }

    @Override
    public BsonDocument toBson() {
      return BsonDocumentBuilder.builder()
          .field(Fields.NUM_CONCURRENT_CHANGE_STREAMS, this.numConcurrentChangeStreams)
          .field(Fields.NUM_INDEXING_THREADS, this.numIndexingThreads)
          .field(Fields.NUM_EMBEDDING_THREADS, this.numEmbeddingThreads)
          .field(Fields.NUM_CONCURRENT_INITIAL_SYNCS, this.numConcurrentInitialSyncs)
          .field(Fields.MAT_VIEW_WRITER_MAX_CONNECTIONS, this.matViewWriterMaxConnections)
          .field(Fields.MAX_IN_FLIGHT_EMBEDDING_GET_MORES, this.maxInFlightEmbeddingGetMores)
          .field(Fields.EMBEDDING_GET_MORE_BATCH_SIZE, this.embeddingGetMoreBatchSize)
          .field(Fields.CHANGE_STREAM_MAX_TIME_MS, this.changeStreamMaxTimeMs)
          .field(Fields.CHANGE_STREAM_CURSOR_MAX_TIME_SEC, this.changeStreamCursorMaxTimeSec)
          .field(Fields.NUM_CHANGE_STREAM_DECODING_THREADS, this.numChangeStreamDecodingThreads)
          .field(Fields.REQUEST_RATE_LIMIT_BACKOFF_MS, this.requestRateLimitBackoffMs)
          .field(Fields.MV_WRITE_RATE_LIMIT_RPS, this.mvWriteRateLimitRps)
          .field(Fields.EMBEDDING_PROVIDER_RPS_LIMIT, this.embeddingProviderRpsLimit)
          .field(Fields.GLOBAL_MEMORY_BUDGET_HEAP_PERCENT, this.globalMemoryBudgetHeapPercent)
          .field(Fields.PER_BATCH_MEMORY_BUDGET_HEAP_PERCENT, this.perBatchMemoryBudgetHeapPercent)
          .build();
    }
  }
}
