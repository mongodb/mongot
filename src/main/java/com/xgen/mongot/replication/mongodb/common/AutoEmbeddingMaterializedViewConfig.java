package com.xgen.mongot.replication.mongodb.common;

import com.google.common.annotations.VisibleForTesting;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.Runtime;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.Field;
import com.xgen.mongot.util.bson.parser.Value;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import org.bson.BsonDocument;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class AutoEmbeddingMaterializedViewConfig extends CommonReplicationConfig
    implements DocumentEncodable {

  private static final Logger LOG =
      LoggerFactory.getLogger(AutoEmbeddingMaterializedViewConfig.class);

  private static final int DEFAULT_CHANGE_STREAM_MAX_TIME_MS = 1000;
  private static final int DEFAULT_NUM_INITIAL_SYNCS = 1;
  private static final int DEFAULT_CHANGE_STREAM_CURSOR_MAX_TIME_SEC =
      Math.toIntExact(Duration.ofMinutes(30).toSeconds());
  private static final int DEFAULT_REQUEST_RATE_LIMIT_BACKOFF_MS = 100;

  /**
   * The number of steady state change streams that are allowed to have outstanding getMores issued
   * at any given time.
   */
  public final int numConcurrentChangeStreams;

  /** The number of indexing threads to use in the EmbeddingIndexingWorkScheduler. */
  public final int numIndexingThreads;

  /** The number of change-stream batches decoding threads. Used in steady-state replication. */
  public final int numChangeStreamDecodingThreads;

  /**
   * The time period in milliseconds to wait between transient resumes during initial-sync and
   * steady-state on mongod overload error.
   */
  public final int requestRateLimitBackoffMs;

  /**
   * The batch size (in number of documents) to use for getMore operations on auto-embedding
   * indexes. If this optional is empty, the default batch size will be used <a
   * href="https://www.mongodb.com/docs/manual/reference/command/getMore/#command-fields">(16MiB)</a>.
   */
  public final Optional<Integer> embeddingGetMoreBatchSize;

  /**
   * This is the most up-to-date Materialized View Schema Version supported in Mongots. Not setting
   * it will disable all schema upgrades.
   */
  public final Optional<Integer> materializedViewSchemaVersion;

  /**
   * The maximum number of in-flight getMores allowed for auto-embedding indexes. Used only in
   * synchronous steady-state flow.
   */
  public final int maxInFlightEmbeddingGetMores;

  /**
   * The maximum amount of time in milliseconds that any given steady state change stream getMore
   * should be allowed to take.
   */
  public final int changeStreamMaxTimeMs;

  /**
   * The maximum amount of time in seconds that any given steady state change stream cursor should
   * be allowed to stay open. Used only in synchronous steady-state flow.
   */
  public final int changeStreamCursorMaxTimeSec;

  /**
   * The maximum number of auto-embedding indexes that are allowed to run initial sync concurrently.
   */
  public final int maxConcurrentEmbeddingInitialSyncs;

  private AutoEmbeddingMaterializedViewConfig(
      boolean pauseAllInitialSyncs,
      List<ObjectId> pauseInitialSyncOnIndexIds,
      List<String> excludedChangestreamFields,
      boolean matchCollectionUuidForUpdateLookup,
      boolean enableSplitLargeChangeStreamEvents,
      int numConcurrentChangeStreams,
      int numIndexingThreads,
      int changeStreamMaxTimeMs,
      int changeStreamCursorMaxTimeSec,
      int numChangeStreamDecodingThreads,
      int requestRateLimitBackoffMs,
      int maxConcurrentEmbeddingInitialSyncs,
      int maxInFlightEmbeddingGetMores,
      Optional<Integer> embeddingGetMoreBatchSize,
      Optional<Integer> materializedViewSchemaVersion) {
    super(
        pauseAllInitialSyncs,
        pauseInitialSyncOnIndexIds,
        enableSplitLargeChangeStreamEvents,
        excludedChangestreamFields,
        matchCollectionUuidForUpdateLookup);
    this.maxConcurrentEmbeddingInitialSyncs = maxConcurrentEmbeddingInitialSyncs;
    this.numConcurrentChangeStreams = numConcurrentChangeStreams;
    this.numIndexingThreads = numIndexingThreads;
    this.changeStreamMaxTimeMs = changeStreamMaxTimeMs;
    this.changeStreamCursorMaxTimeSec = changeStreamCursorMaxTimeSec;
    this.numChangeStreamDecodingThreads = numChangeStreamDecodingThreads;
    this.maxInFlightEmbeddingGetMores = maxInFlightEmbeddingGetMores;
    this.embeddingGetMoreBatchSize = embeddingGetMoreBatchSize;
    this.requestRateLimitBackoffMs = requestRateLimitBackoffMs;
    this.materializedViewSchemaVersion = materializedViewSchemaVersion;
  }

  /**
   * Creates a new AutoEmbeddingMaterializedViewReplicationConfig, deriving defaults for any options
   * that are not supplied.
   */
  public static AutoEmbeddingMaterializedViewConfig create(
      GlobalReplicationConfig globalReplicationConfig,
      Optional<Integer> optionalNumConcurrentChangeStreams,
      Optional<Integer> optionalNumIndexingThreads,
      Optional<Integer> optionalChangeStreamMaxTimeMs,
      Optional<Integer> optionalChangeStreamCursorMaxTimeSec,
      Optional<Integer> optionalNumChangeStreamDecodingThreads,
      Optional<Integer> optionalRequestRateLimitBackoffMs,
      Optional<Integer> optionalMaxConcurrentEmbeddingInitialSyncs,
      Optional<Integer> optionalMaxInFlightEmbeddingGetMores,
      Optional<Integer> embeddingGetMoreBatchSize,
      Optional<Integer> materializedViewSchemaVersion) {
    return create(
        Runtime.INSTANCE,
        globalReplicationConfig,
        optionalNumConcurrentChangeStreams,
        optionalNumIndexingThreads,
        optionalChangeStreamMaxTimeMs,
        optionalChangeStreamCursorMaxTimeSec,
        optionalNumChangeStreamDecodingThreads,
        optionalRequestRateLimitBackoffMs,
        optionalMaxConcurrentEmbeddingInitialSyncs,
        optionalMaxInFlightEmbeddingGetMores,
        embeddingGetMoreBatchSize,
        materializedViewSchemaVersion);
  }

  /** Used for testing. The above create() method should be called instead. */
  @VisibleForTesting
  static AutoEmbeddingMaterializedViewConfig create(
      Runtime runtime,
      GlobalReplicationConfig globalReplicationConfig,
      Optional<Integer> optionalNumConcurrentChangeStreams,
      Optional<Integer> optionalNumIndexingThreads,
      Optional<Integer> optionalChangeStreamMaxTimeMs,
      Optional<Integer> optionalChangeStreamCursorMaxTimeSec,
      Optional<Integer> optionalNumChangeStreamDecodingThreads,
      Optional<Integer> optionalRequestRateLimitBackoffMs,
      Optional<Integer> optionalMaxConcurrentEmbeddingInitialSyncs,
      Optional<Integer> optionalMaxInFlightEmbeddingGetMores,
      Optional<Integer> embeddingGetMoreBatchSize,
      Optional<Integer> materializedViewSchemaVersion) {

    int maxConcurrentEmbeddingInitialSyncs =
        getMaxConcurrentEmbeddingInitialSyncsWithDefault(
            runtime, optionalMaxConcurrentEmbeddingInitialSyncs);
    Check.argIsPositive(maxConcurrentEmbeddingInitialSyncs, "maxConcurrentEmbeddingInitialSyncs");

    int numConcurrentChangeStreams =
        getNumConcurrentChangeStreamsWithDefault(runtime, optionalNumConcurrentChangeStreams);
    Check.argIsPositive(numConcurrentChangeStreams, "numConcurrentChangeStreams");

    int numIndexingThreads = getNumIndexingThreadsWithDefault(runtime, optionalNumIndexingThreads);
    Check.argIsPositive(numIndexingThreads, "numIndexingThreads");

    int changeStreamMaxTimeMs = getChangeStreamMaxTimeMsWithDefault(optionalChangeStreamMaxTimeMs);
    Check.argIsPositive(changeStreamMaxTimeMs, "changeStreamMaxTimeMs");

    int changeStreamCursorMaxTimeSec =
        getChangeStreamCursorMaxTimeSecWithDefault(optionalChangeStreamCursorMaxTimeSec);
    Check.argIsPositive(changeStreamCursorMaxTimeSec, "changeStreamCursorMaxTimeSec");

    int numChangeStreamDecodingThreads =
        getNumChangeStreamDecodingThreadsWithDefault(
            runtime, optionalNumChangeStreamDecodingThreads);
    Check.argIsPositive(numChangeStreamDecodingThreads, "numChangeStreamDecodingThreads");

    int maxInFlightEmbeddingGetMores =
        getMaxInFlightEmbeddingGetMoresWithDefault(
            optionalMaxInFlightEmbeddingGetMores, numConcurrentChangeStreams);
    Check.argIsPositive(maxInFlightEmbeddingGetMores, "maxInFlightEmbeddingGetMores");

    embeddingGetMoreBatchSize.ifPresent(
        value -> Check.argIsPositive(value, "embeddingGetMoreBatchSize"));

    int requestRateLimitBackoffMs =
        getRequestRateLimitBackoffMsWithDefault(optionalRequestRateLimitBackoffMs);
    Check.argIsPositive(requestRateLimitBackoffMs, "requestRateLimitBackoffMs");

    return new AutoEmbeddingMaterializedViewConfig(
        globalReplicationConfig.pauseAllInitialSyncs(),
        globalReplicationConfig.pauseInitialSyncOnIndexIds(),
        globalReplicationConfig.excludedChangestreamFields(),
        globalReplicationConfig.matchCollectionUuidForUpdateLookup(),
        globalReplicationConfig.enableSplitLargeChangeStreamEvents(),
        numConcurrentChangeStreams,
        numIndexingThreads,
        changeStreamMaxTimeMs,
        changeStreamCursorMaxTimeSec,
        numChangeStreamDecodingThreads,
        requestRateLimitBackoffMs,
        maxConcurrentEmbeddingInitialSyncs,
        maxInFlightEmbeddingGetMores,
        embeddingGetMoreBatchSize,
        materializedViewSchemaVersion);
  }

  /**
   * Creates a new AutoEmbeddingMaterializedViewReplicationConfig, defaulting to empty options for
   * everything, can be used for community and local dev environments.
   */
  public static AutoEmbeddingMaterializedViewConfig getDefault() {
    return create(
        defaultGlobalReplicationConfig(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty());
  }

  /** Used for testing. The above getDefault() method should be called instead. */
  @VisibleForTesting
  static AutoEmbeddingMaterializedViewConfig getDefaultWithRuntime(Runtime runtime) {
    return create(
        runtime,
        defaultGlobalReplicationConfig(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty());
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder()
        .field(Fields.PAUSE_ALL_INITIAL_SYNCS, this.pauseAllInitialSyncs)
        .field(Fields.PAUSE_INITIAL_SYNC_ON_INDEX_IDS, this.pauseInitialSyncOnIndexIds)
        .field(
            Fields.ENABLE_SPLIT_LARGE_CHANGE_STREAM_EVENTS, this.enableSplitLargeChangeStreamEvents)
        .fieldOmitDefaultValue(Fields.EXCLUDED_CHANGESTREAM_FIELDS, this.excludedChangestreamFields)
        .field(
            Fields.MATCH_COLLECTION_UUID_FOR_UPDATE_LOOKUP, this.matchCollectionUuidForUpdateLookup)
        .field(Fields.NUM_CONCURRENT_CHANGE_STREAMS, this.numConcurrentChangeStreams)
        .field(Fields.NUM_INDEXING_THREADS, this.numIndexingThreads)
        .field(Fields.CHANGE_STREAM_MAX_TIME_MS, this.changeStreamMaxTimeMs)
        .field(
            Fields.CHANGE_STREAM_CURSOR_MAX_TIME_SEC,
            Optional.of(this.changeStreamCursorMaxTimeSec))
        .field(
            Fields.NUM_CHANGE_STREAM_DECODING_THREADS,
            Optional.of(this.numChangeStreamDecodingThreads))
        .field(Fields.REQUEST_RATE_LIMIT_BACKOFF_MS, Optional.of(this.requestRateLimitBackoffMs))
        .field(
            Fields.MAX_CONCURRENT_EMBEDDING_INITIAL_SYNCS, this.maxConcurrentEmbeddingInitialSyncs)
        .field(Fields.MAX_IN_FLIGHT_EMBEDDING_GET_MORES, this.maxInFlightEmbeddingGetMores)
        .field(Fields.EMBEDDING_GET_MORE_BATCH_SIZE, this.embeddingGetMoreBatchSize)
        .field(Fields.MATERIALIZED_VIEW_SCHEMA_VERSION, this.materializedViewSchemaVersion)
        .build();
  }

  @Override
  public int getNumConcurrentInitialSyncs() {
    // Should be same as maxConcurrentEmbeddingInitialSyncs.
    return this.maxConcurrentEmbeddingInitialSyncs;
  }

  @Override
  public int getNumConcurrentChangeStreams() {
    return this.numConcurrentChangeStreams;
  }

  @Override
  public int getNumIndexingThreads() {
    return this.numIndexingThreads;
  }

  @Override
  public int getChangeStreamMaxTimeMs() {
    return this.changeStreamMaxTimeMs;
  }

  @Override
  public int getChangeStreamCursorMaxTimeSec() {
    return this.changeStreamCursorMaxTimeSec;
  }

  @Override
  public int getNumChangeStreamDecodingThreads() {
    return this.numChangeStreamDecodingThreads;
  }

  @Override
  public int getRequestRateLimitBackoffMs() {
    return this.requestRateLimitBackoffMs;
  }

  private static int getMaxConcurrentEmbeddingInitialSyncsWithDefault(
      Runtime runtime, Optional<Integer> optionalMaxConcurrentEmbeddingInitialSyncs) {
    return optionalMaxConcurrentEmbeddingInitialSyncs.orElseGet(
        () -> {
          int maxConcurrentEmbeddingInitialSyncs =
              Math.min(runtime.getNumCpus(), DEFAULT_NUM_INITIAL_SYNCS);
          LOG.info(
              "maxConcurrentEmbeddingInitialSyncs not configured, defaulting to {}.",
              maxConcurrentEmbeddingInitialSyncs);
          return maxConcurrentEmbeddingInitialSyncs;
        });
  }

  private static int getNumConcurrentChangeStreamsWithDefault(
      Runtime runtime, Optional<Integer> optionalNumConcurrentChangeStreams) {
    return optionalNumConcurrentChangeStreams.orElseGet(
        () -> {
          int numConcurrentChangeStreams = runtime.getNumCpus() * 2;
          LOG.info(
              "numConcurrentChangeStreams not configured, defaulting to {}.",
              numConcurrentChangeStreams);
          return numConcurrentChangeStreams;
        });
  }

  private static int getNumIndexingThreadsWithDefault(
      Runtime runtime, Optional<Integer> optionalNumIndexingThreads) {
    return optionalNumIndexingThreads.orElseGet(
        () -> {
          int numIndexingThreads = Math.max(1, Math.floorDiv(runtime.getNumCpus(), 2));
          LOG.info("numIndexingThreads not configured, defaulting to {}.", numIndexingThreads);
          return numIndexingThreads;
        });
  }

  private static int getChangeStreamMaxTimeMsWithDefault(
      Optional<Integer> optionalChangeStreamMaxTimeMs) {
    return optionalChangeStreamMaxTimeMs.orElseGet(
        () -> {
          int changeStreamMaxTimeMs = DEFAULT_CHANGE_STREAM_MAX_TIME_MS;
          LOG.info(
              "changeStreamMaxTimeMs not configured, defaulting to {}.", changeStreamMaxTimeMs);
          return changeStreamMaxTimeMs;
        });
  }

  private static int getChangeStreamCursorMaxTimeSecWithDefault(
      Optional<Integer> optionalChangeStreamCursorMaxTimeSec) {
    return optionalChangeStreamCursorMaxTimeSec.orElseGet(
        () -> {
          int changeStreamCursorMaxTimeSec = DEFAULT_CHANGE_STREAM_CURSOR_MAX_TIME_SEC;
          LOG.info(
              "changeStreamCursorMaxTimeSec not configured, defaulting to {}.",
              changeStreamCursorMaxTimeSec);
          return changeStreamCursorMaxTimeSec;
        });
  }

  private static int getNumChangeStreamDecodingThreadsWithDefault(
      Runtime runtime, Optional<Integer> optionalNumChangeStreamDecodingThreads) {
    return optionalNumChangeStreamDecodingThreads.orElseGet(
        () -> {
          int numDecodingThreads = Math.max(1, Math.floorDiv(runtime.getNumCpus(), 2));
          LOG.info(
              "numChangeStreamDecodingThreads not configured, defaulting to {}.",
              numDecodingThreads);
          return numDecodingThreads;
        });
  }

  private static int getMaxInFlightEmbeddingGetMoresWithDefault(
      Optional<Integer> optionalMaxInFlightEmbeddingGetMores, int numConcurrentChangeStreams) {
    return optionalMaxInFlightEmbeddingGetMores.orElseGet(
        () -> Math.max(1, numConcurrentChangeStreams / 4));
  }

  private static int getRequestRateLimitBackoffMsWithDefault(
      Optional<Integer> optionalRequestRateLimitBackoffMs) {
    return optionalRequestRateLimitBackoffMs.orElseGet(
        () -> {
          LOG.info(
              "requestRateLimitBackoffMs not configured, defaulting to {}.",
              DEFAULT_REQUEST_RATE_LIMIT_BACKOFF_MS);
          return DEFAULT_REQUEST_RATE_LIMIT_BACKOFF_MS;
        });
  }

  private static class Fields {

    private static final Field.Required<Integer> NUM_CONCURRENT_CHANGE_STREAMS =
        Field.builder("numConcurrentChangeStreams").intField().mustBePositive().required();

    private static final Field.Required<Integer> NUM_INDEXING_THREADS =
        Field.builder("numIndexingThreads").intField().mustBePositive().required();

    private static final Field.Required<Integer> CHANGE_STREAM_MAX_TIME_MS =
        Field.builder("changeStreamMaxTimeMs").intField().mustBePositive().required();

    private static final Field.Optional<Integer> CHANGE_STREAM_CURSOR_MAX_TIME_SEC =
        Field.builder("changeStreamCursorMaxTimeSec")
            .intField()
            .mustBePositive()
            .optional()
            .noDefault();

    private static final Field.Optional<Integer> NUM_CHANGE_STREAM_DECODING_THREADS =
        Field.builder("numChangeStreamDecodingThreads")
            .intField()
            .mustBePositive()
            .optional()
            .noDefault();

    private static final Field.Required<Boolean> PAUSE_ALL_INITIAL_SYNCS =
        Field.builder("pauseAllInitialSyncs").booleanField().required();

    private static final Field.Required<List<ObjectId>> PAUSE_INITIAL_SYNC_ON_INDEX_IDS =
        Field.builder("pauseInitialSyncOnIndexIds")
            .listOf(Value.builder().objectIdValue().required())
            .required();

    private static final Field.WithDefault<List<String>> EXCLUDED_CHANGESTREAM_FIELDS =
        Field.builder("excludedChangestreamFields")
            .listOf(Value.builder().stringValue().required())
            .optional()
            .withDefault(List.of());

    private static final Field.Required<Boolean> MATCH_COLLECTION_UUID_FOR_UPDATE_LOOKUP =
        Field.builder("matchCollectionUUIDForUpdateLookup").booleanField().required();

    private static final Field.Required<Boolean> ENABLE_SPLIT_LARGE_CHANGE_STREAM_EVENTS =
        Field.builder("enableSplitLargeChangeStreamEvents").booleanField().required();

    private static final Field.Optional<Integer> REQUEST_RATE_LIMIT_BACKOFF_MS =
        Field.builder("requestRateLimitBackoffMs")
            .intField()
            .mustBePositive()
            .optional()
            .noDefault();

    private static final Field.Required<Integer> MAX_CONCURRENT_EMBEDDING_INITIAL_SYNCS =
        Field.builder("maxConcurrentEmbeddingInitialSyncs").intField().mustBePositive().required();

    private static final Field.Required<Integer> MAX_IN_FLIGHT_EMBEDDING_GET_MORES =
        Field.builder("maxInFlightEmbeddingGetMores").intField().mustBePositive().required();

    private static final Field.Optional<Integer> EMBEDDING_GET_MORE_BATCH_SIZE =
        Field.builder("embeddingGetMoreBatchSize")
            .intField()
            .mustBePositive()
            .optional()
            .noDefault();

    private static final Field.Optional<Integer> MATERIALIZED_VIEW_SCHEMA_VERSION =
        Field.builder("materializedViewSchemaVersion")
            .intField()
            .mustBeNonNegative()
            .optional()
            .noDefault();
  }
}
