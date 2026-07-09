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

public final class MongoDbReplicationConfig extends CommonReplicationConfig
    implements DocumentEncodable {

  private static final Logger LOG = LoggerFactory.getLogger(MongoDbReplicationConfig.class);

  private static final int DEFAULT_CHANGE_STREAM_MAX_TIME_MS = 1000;
  private static final int DEFAULT_NUM_INITIAL_SYNCS = 2;
  private static final int DEFAULT_NUM_CONCURRENT_SYNONYM_SYNCS = 2;
  private static final int DEFAULT_CHANGE_STREAM_CURSOR_MAX_TIME_SEC =
      Math.toIntExact(Duration.ofMinutes(30).toSeconds());
  private static final int DEFAULT_REQUEST_RATE_LIMIT_BACKOFF_MS = 100;

  /**
   * Default optime-lag threshold, in seconds, above which a change-stream task is considered
   * high-priority by the {@code OptimeLagPriorityStrategy}, when priority scheduling is enabled and
   * {@link SteadyStateSchedulingMethod#OPTIME_LAG} is selected.
   */
  public static final int DEFAULT_STEADY_STATE_PRIORITY_OPTIME_THRESHOLD_SECONDS = 60;

  /**
   * Default number of preferred-queue tasks the {@code DualQueueExecutor} runs per regular-queue
   * task, when priority scheduling is enabled.
   */
  public static final int DEFAULT_STEADY_STATE_PRIORITY_RATIO = 1;

  /**
   * Default priority strategy variant: FIFO. Used when {@code steadyStateSchedulingMethod} is not
   * configured at all. A separate fallback to {@code FIFO} for an invalid value lives in {@code
   * MmsConfig}'s {@code STEADY_STATE_SCHEDULING_METHOD} field definition. If this default has to be
   * changed, both sites need to be updated.
   */
  public static final SteadyStateSchedulingMethod DEFAULT_STEADY_STATE_SCHEDULING_METHOD =
      SteadyStateSchedulingMethod.FIFO;

  /** The number of initial syncs that are allowed to be run concurrently. */
  public final int numConcurrentInitialSyncs;

  /**
   * The number of steady state change streams that are allowed to have outstanding getMores issued
   * at any given time.
   */
  public final int numConcurrentChangeStreams;

  /** The number of indexing threads to use in the scheduler. */
  public final int numIndexingThreads;

  /** The number of synonym sync operations that are allowed to be run concurrently. */
  public final int numConcurrentSynonymSyncs;

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

  /** The number of change-stream batches decoding threads. Used in steady-state replication. */
  public final int numChangeStreamDecodingThreads;

  /**
   * Optional override of the change stream projection mode. If this optional is empty (default),
   * mongot will select the mode automatically based on heuristics.
   */
  public Optional<Boolean> enableSteadyStateChangeStreamProjection;

  /**
   * The maximum number of in-flight getMores allowed for auto-embedding indexes. Used only in
   * synchronous steady-state flow. If this is greater than numConcurrentChangeStreams, the number
   * of in-flight getMores for auto-embedding indexes will not be limited.
   */
  public final int maxInFlightEmbeddingGetMores;

  /**
   * The maximum number of auto-embedding indexes that are allowed to run initial sync concurrently.
   * If this is greater than numConcurrentInitialSyncs, the number of concurrent initial syncs for
   * auto-embedding indexes will not be limited.
   */
  public final int maxConcurrentEmbeddingInitialSyncs;

  /**
   * The batch size (in number of documents) to use for getMore operations on auto-embedding
   * indexes. If this optional is empty, the default batch size will be used <a
   * href="https://www.mongodb.com/docs/manual/reference/command/getMore/#command-fields">(16MiB)</a>.
   */
  public final Optional<Integer> embeddingGetMoreBatchSize;

  /**
   * The time period in milliseconds to wait between transient resumes during initial-sync and
   * steady-state on mongod overload error.
   */
  public final int requestRateLimitBackoffMs;

  /**
   * Which priority strategy variant, if any, classifies change-stream tasks as high-priority, used
   * only when {@code Feature.CHANGE_STREAM_PRIORITY_SCHEDULING} is enabled.
   */
  public final SteadyStateSchedulingMethod steadyStateSchedulingMethod;

  /**
   * The number of preferred-queue tasks the {@code DualQueueExecutor} runs per regular-queue task,
   * used only when {@code Feature.CHANGE_STREAM_PRIORITY_SCHEDULING} is enabled.
   */
  public final int steadyStatePriorityRatio;

  /**
   * The optime-lag threshold, in seconds, used by the {@code OptimeLagPriorityStrategy} to classify
   * a change-stream task as high-priority, used only when {@link #steadyStateSchedulingMethod} is
   * {@link SteadyStateSchedulingMethod#OPTIME_LAG}.
   */
  public final int steadyStatePriorityOptimeThresholdSeconds;

  private MongoDbReplicationConfig(
      int numConcurrentInitialSyncs,
      int numConcurrentChangeStreams,
      int numIndexingThreads,
      int changeStreamMaxTimeMs,
      int numConcurrentSynonymSyncs,
      int changeStreamCursorMaxTimeSec,
      int numChangeStreamDecodingThreads,
      Optional<Boolean> enableSteadyStateChangeStreamProjection,
      boolean pauseAllInitialSyncs,
      List<ObjectId> pauseInitialSyncOnIndexIds,
      // TODO(CLOUDP-360914): Remove following 3 parameters after shutting down type:text indexes
      int maxInFlightEmbeddingGetMores,
      int maxConcurrentEmbeddingInitialSyncs,
      Optional<Integer> embeddingGetMoreBatchSize,
      List<String> excludedChangestreamFields,
      boolean matchCollectionUuidForUpdateLookup,
      boolean enableSplitLargeChangeStreamEvents,
      boolean splitLargeChangeStreamEventsForInitialSync,
      int requestRateLimitBackoffMs,
      int steadyStatePriorityOptimeThresholdSeconds,
      SteadyStateSchedulingMethod steadyStateSchedulingMethod,
      int steadyStatePriorityRatio) {
    super(
        pauseAllInitialSyncs,
        pauseInitialSyncOnIndexIds,
        enableSplitLargeChangeStreamEvents,
        splitLargeChangeStreamEventsForInitialSync,
        excludedChangestreamFields,
        matchCollectionUuidForUpdateLookup);
    this.numConcurrentInitialSyncs = numConcurrentInitialSyncs;
    this.numConcurrentChangeStreams = numConcurrentChangeStreams;
    this.numIndexingThreads = numIndexingThreads;
    this.changeStreamMaxTimeMs = changeStreamMaxTimeMs;
    this.numConcurrentSynonymSyncs = numConcurrentSynonymSyncs;
    this.changeStreamCursorMaxTimeSec = changeStreamCursorMaxTimeSec;
    this.numChangeStreamDecodingThreads = numChangeStreamDecodingThreads;
    this.enableSteadyStateChangeStreamProjection = enableSteadyStateChangeStreamProjection;
    this.maxInFlightEmbeddingGetMores = maxInFlightEmbeddingGetMores;
    this.maxConcurrentEmbeddingInitialSyncs = maxConcurrentEmbeddingInitialSyncs;
    this.embeddingGetMoreBatchSize = embeddingGetMoreBatchSize;
    this.requestRateLimitBackoffMs = requestRateLimitBackoffMs;
    this.steadyStatePriorityOptimeThresholdSeconds = steadyStatePriorityOptimeThresholdSeconds;
    this.steadyStateSchedulingMethod = steadyStateSchedulingMethod;
    this.steadyStatePriorityRatio = steadyStatePriorityRatio;
  }

  /**
   * Creates a new MongoDbReplicationConfig, deriving defaults for any options that are not
   * supplied.
   */
  public static MongoDbReplicationConfig create(
      GlobalReplicationConfig globalReplicationConfig,
      Optional<Integer> optionalNumConcurrentInitialSyncs,
      Optional<Integer> optionalNumConcurrentChangeStreams,
      Optional<Integer> optionalNumIndexingThreads,
      Optional<Integer> optionalChangeStreamMaxTimeMs,
      Optional<Integer> optionalNumConcurrentSynonymSyncs,
      Optional<Integer> optionalChangeStreamCursorMaxTimeSec,
      Optional<Integer> optionalNumChangeStreamDecodingThreads,
      Optional<Boolean> enableSteadyStateChangeStreamProjection,
      // TODO(CLOUDP-360914): Remove those embedding parameters after shutdown type:text indexes
      Optional<Integer> optionalMaxInFlightEmbeddingGetMores,
      Optional<Integer> optionalMaxConcurrentEmbeddingInitialSyncs,
      Optional<Integer> embeddingGetMoreBatchSize,
      Optional<Integer> requestRateLimitBackoffMs,
      Optional<Integer> optionalSteadyStatePriorityOptimeThresholdSeconds,
      Optional<SteadyStateSchedulingMethod> optionalSteadyStateSchedulingMethod,
      Optional<Integer> optionalSteadyStatePriorityRatio) {
    return create(
        Runtime.INSTANCE,
        globalReplicationConfig,
        optionalNumConcurrentInitialSyncs,
        optionalNumConcurrentChangeStreams,
        optionalNumIndexingThreads,
        optionalChangeStreamMaxTimeMs,
        optionalNumConcurrentSynonymSyncs,
        optionalChangeStreamCursorMaxTimeSec,
        optionalNumChangeStreamDecodingThreads,
        enableSteadyStateChangeStreamProjection,
        optionalMaxInFlightEmbeddingGetMores,
        optionalMaxConcurrentEmbeddingInitialSyncs,
        embeddingGetMoreBatchSize,
        requestRateLimitBackoffMs,
        optionalSteadyStatePriorityOptimeThresholdSeconds,
        optionalSteadyStateSchedulingMethod,
        optionalSteadyStatePriorityRatio);
  }

  /** Used for testing. The above create() method should be called instead. */
  @VisibleForTesting
  public static MongoDbReplicationConfig create(
      Runtime runtime,
      GlobalReplicationConfig globalReplicationConfig,
      Optional<Integer> optionalNumConcurrentInitialSyncs,
      Optional<Integer> optionalNumConcurrentChangeStreams,
      Optional<Integer> optionalNumIndexingThreads,
      Optional<Integer> optionalChangeStreamMaxTimeMs,
      Optional<Integer> optionalNumConcurrentSynonymSyncs,
      Optional<Integer> optionalChangeStreamCursorMaxTimeSec,
      Optional<Integer> optionalNumChangeStreamDecodingThreads,
      Optional<Boolean> enableSteadyStateChangeStreamProjection,
      Optional<Integer> optionalMaxInFlightEmbeddingGetMores,
      Optional<Integer> optionalMaxConcurrentEmbeddingInitialSyncs,
      Optional<Integer> embeddingGetMoreBatchSize,
      Optional<Integer> optionalRequestRateLimitBackoffMs,
      Optional<Integer> optionalSteadyStatePriorityOptimeThresholdSeconds,
      Optional<SteadyStateSchedulingMethod> optionalSteadyStateSchedulingMethod,
      Optional<Integer> optionalSteadyStatePriorityRatio) {
    int numConcurrentInitialSyncs =
        getNumConcurrentInitialSyncsWithDefault(runtime, optionalNumConcurrentInitialSyncs);
    Check.argIsPositive(numConcurrentInitialSyncs, "numConcurrentInitialSyncs");

    int numConcurrentChangeStreams =
        getNumConcurrentChangeStreamsWithDefault(runtime, optionalNumConcurrentChangeStreams);
    Check.argIsPositive(numConcurrentChangeStreams, "numConcurrentChangeStreams");

    int numIndexingThreads = getNumIndexingThreadsWithDefault(runtime, optionalNumIndexingThreads);
    Check.argIsPositive(numIndexingThreads, "numIndexingThreads");

    int changeStreamMaxTimeMs = getChangeStreamMaxTimeMsWithDefault(optionalChangeStreamMaxTimeMs);
    Check.argIsPositive(changeStreamMaxTimeMs, "changeStreamMaxTimeMs");

    int numConcurrentSynonymSyncs =
        optionalNumConcurrentSynonymSyncs.orElse(DEFAULT_NUM_CONCURRENT_SYNONYM_SYNCS);
    Check.argIsPositive(numConcurrentSynonymSyncs, "numConcurrentSynonymSyncs");

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

    int maxConcurrentEmbeddingInitialSyncs =
        getMaxConcurrentEmbeddingInitialSyncsWithDefault(
            optionalMaxConcurrentEmbeddingInitialSyncs, numConcurrentInitialSyncs);
    Check.argIsPositive(maxConcurrentEmbeddingInitialSyncs, "maxConcurrentEmbeddingInitialSyncs");

    embeddingGetMoreBatchSize.ifPresent(
        value -> Check.argIsPositive(value, "embeddingGetMoreBatchSize"));

    int requestRateLimitBackoffMs =
        getRequestRateLimitBackoffMsWithDefault(optionalRequestRateLimitBackoffMs);
    Check.argIsPositive(requestRateLimitBackoffMs, "requestRateLimitBackoffMs");

    int steadyStatePriorityOptimeThresholdSeconds =
        optionalSteadyStatePriorityOptimeThresholdSeconds.orElse(
            DEFAULT_STEADY_STATE_PRIORITY_OPTIME_THRESHOLD_SECONDS);
    Check.argIsPositive(
        steadyStatePriorityOptimeThresholdSeconds, "steadyStatePriorityOptimeThresholdSeconds");

    SteadyStateSchedulingMethod steadyStateSchedulingMethod =
        optionalSteadyStateSchedulingMethod.orElse(DEFAULT_STEADY_STATE_SCHEDULING_METHOD);

    int steadyStatePriorityRatio =
        optionalSteadyStatePriorityRatio.orElse(DEFAULT_STEADY_STATE_PRIORITY_RATIO);
    Check.argIsPositive(steadyStatePriorityRatio, "steadyStatePriorityRatio");

    return new MongoDbReplicationConfig(
        numConcurrentInitialSyncs,
        numConcurrentChangeStreams,
        numIndexingThreads,
        changeStreamMaxTimeMs,
        numConcurrentSynonymSyncs,
        changeStreamCursorMaxTimeSec,
        numChangeStreamDecodingThreads,
        enableSteadyStateChangeStreamProjection,
        globalReplicationConfig.pauseAllInitialSyncs(),
        globalReplicationConfig.pauseInitialSyncOnIndexIds(),
        maxInFlightEmbeddingGetMores,
        maxConcurrentEmbeddingInitialSyncs,
        embeddingGetMoreBatchSize,
        globalReplicationConfig.excludedChangestreamFields(),
        globalReplicationConfig.matchCollectionUuidForUpdateLookup(),
        globalReplicationConfig.enableSplitLargeChangeStreamEvents(),
        globalReplicationConfig.splitLargeChangeStreamEventsForInitialSync(),
        requestRateLimitBackoffMs,
        steadyStatePriorityOptimeThresholdSeconds,
        steadyStateSchedulingMethod,
        steadyStatePriorityRatio);
  }

  /** Creates a new MongoDbReplicationConfig, defaulting to empty options for everything. */
  public static MongoDbReplicationConfig getDefault() {
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
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty());
  }

  private static int getNumConcurrentInitialSyncsWithDefault(
      Runtime runtime, Optional<Integer> optionalNumConcurrentInitialSyncs) {
    return optionalNumConcurrentInitialSyncs.orElseGet(
        () -> {
          int numConcurrentInitialSyncs = Math.min(runtime.getNumCpus(), DEFAULT_NUM_INITIAL_SYNCS);
          LOG.info(
              "numConcurrentInitialSyncs not configured, defaulting to {}.",
              numConcurrentInitialSyncs);
          return numConcurrentInitialSyncs;
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

  private static int getMaxConcurrentEmbeddingInitialSyncsWithDefault(
      Optional<Integer> optionalMaxConcurrentEmbeddingInitialSyncs, int numConcurrentInitialSyncs) {
    return optionalMaxConcurrentEmbeddingInitialSyncs.orElseGet(
        () -> Math.max(1, numConcurrentInitialSyncs / 2));
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

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder()
        .field(Fields.NUM_CONCURRENT_CHANGE_STREAMS, this.numConcurrentChangeStreams)
        .field(Fields.NUM_CONCURRENT_INITIAL_SYNCS, this.numConcurrentInitialSyncs)
        .field(Fields.NUM_INDEXING_THREADS, this.numIndexingThreads)
        .field(Fields.CHANGE_STREAM_MAX_TIME_MS, this.changeStreamMaxTimeMs)
        .field(Fields.NUM_CONCURRENT_SYNONYM_SYNCS, this.numConcurrentSynonymSyncs)
        .field(
            Fields.CHANGE_STREAM_CURSOR_MAX_TIME_SEC,
            Optional.of(this.changeStreamCursorMaxTimeSec))
        .field(
            Fields.NUM_CHANGE_STREAM_DECODING_THREADS,
            Optional.of(this.numChangeStreamDecodingThreads))
        .field(
            Fields.ENABLE_STEADY_STATE_CHANGE_STREAM_PROJECTION,
            this.enableSteadyStateChangeStreamProjection)
        .field(Fields.PAUSE_ALL_INITIAL_SYNCS, this.pauseAllInitialSyncs)
        .field(Fields.PAUSE_INITIAL_SYNC_ON_INDEX_IDS, this.pauseInitialSyncOnIndexIds)
        .field(Fields.MAX_IN_FLIGHT_EMBEDDING_GET_MORES, this.maxInFlightEmbeddingGetMores)
        .field(
            Fields.MAX_CONCURRENT_EMBEDDING_INITIAL_SYNCS, this.maxConcurrentEmbeddingInitialSyncs)
        .field(Fields.EMBEDDING_GET_MORE_BATCH_SIZE, this.embeddingGetMoreBatchSize)
        .fieldOmitDefaultValue(Fields.EXCLUDED_CHANGESTREAM_FIELDS, this.excludedChangestreamFields)
        .field(
            Fields.MATCH_COLLECTION_UUID_FOR_UPDATE_LOOKUP, this.matchCollectionUuidForUpdateLookup)
        .field(
            Fields.ENABLE_SPLIT_LARGE_CHANGE_STREAM_EVENTS, this.enableSplitLargeChangeStreamEvents)
        .field(
            Fields.SPLIT_LARGE_CHANGE_STREAM_EVENTS_FOR_INITIAL_SYNC,
            this.splitLargeChangeStreamEventsForInitialSync)
        .field(Fields.REQUEST_RATE_LIMIT_BACKOFF_MS, Optional.of(this.requestRateLimitBackoffMs))
        .field(
            Fields.STEADY_STATE_PRIORITY_OPTIME_THRESHOLD_SECONDS,
            Optional.of(this.steadyStatePriorityOptimeThresholdSeconds))
        .field(Fields.STEADY_STATE_SCHEDULING_METHOD, Optional.of(this.steadyStateSchedulingMethod))
        .field(Fields.STEADY_STATE_PRIORITY_RATIO, Optional.of(this.steadyStatePriorityRatio))
        .build();
  }

  @Override
  public int getNumConcurrentInitialSyncs() {
    return this.numConcurrentInitialSyncs;
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

  @Override
  public Type getReplicationType() {
    return Type.DEFAULT;
  }

  public int getSteadyStatePriorityOptimeThresholdSeconds() {
    return this.steadyStatePriorityOptimeThresholdSeconds;
  }

  public SteadyStateSchedulingMethod getSteadyStateSchedulingMethod() {
    return this.steadyStateSchedulingMethod;
  }

  public int getSteadyStatePriorityRatio() {
    return this.steadyStatePriorityRatio;
  }

  private static class Fields {
    private static final Field.Required<Integer> NUM_CONCURRENT_INITIAL_SYNCS =
        Field.builder("numConcurrentInitialSyncs").intField().mustBePositive().required();

    private static final Field.Required<Integer> NUM_CONCURRENT_CHANGE_STREAMS =
        Field.builder("numConcurrentChangeStreams").intField().mustBePositive().required();

    private static final Field.Required<Integer> NUM_INDEXING_THREADS =
        Field.builder("numIndexingThreads").intField().mustBePositive().required();

    private static final Field.Required<Integer> CHANGE_STREAM_MAX_TIME_MS =
        Field.builder("changeStreamMaxTimeMs").intField().mustBePositive().required();

    private static final Field.Required<Integer> NUM_CONCURRENT_SYNONYM_SYNCS =
        Field.builder("numConcurrentSynonymSyncs").intField().mustBePositive().required();

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

    private static final Field.Optional<Boolean> ENABLE_STEADY_STATE_CHANGE_STREAM_PROJECTION =
        Field.builder("enableSteadyStateChangeStreamProjection")
            .booleanField()
            .optional()
            .noDefault();

    private static final Field.Required<Boolean> PAUSE_ALL_INITIAL_SYNCS =
        Field.builder("pauseAllInitialSyncs").booleanField().required();

    private static final Field.Required<List<ObjectId>> PAUSE_INITIAL_SYNC_ON_INDEX_IDS =
        Field.builder("pauseInitialSyncOnIndexIds")
            .listOf(Value.builder().objectIdValue().required())
            .required();

    private static final Field.Required<Integer> MAX_IN_FLIGHT_EMBEDDING_GET_MORES =
        Field.builder("maxInFlightEmbeddingGetMores").intField().mustBePositive().required();

    private static final Field.Required<Integer> MAX_CONCURRENT_EMBEDDING_INITIAL_SYNCS =
        Field.builder("maxConcurrentEmbeddingInitialSyncs").intField().mustBePositive().required();

    private static final Field.Optional<Integer> EMBEDDING_GET_MORE_BATCH_SIZE =
        Field.builder("embeddingGetMoreBatchSize")
            .intField()
            .mustBePositive()
            .optional()
            .noDefault();

    private static final Field.WithDefault<List<String>> EXCLUDED_CHANGESTREAM_FIELDS =
        Field.builder("excludedChangestreamFields")
            .listOf(Value.builder().stringValue().required())
            .optional()
            .withDefault(List.of());

    private static final Field.Required<Boolean> MATCH_COLLECTION_UUID_FOR_UPDATE_LOOKUP =
        Field.builder("matchCollectionUUIDForUpdateLookup").booleanField().required();

    private static final Field.Required<Boolean> ENABLE_SPLIT_LARGE_CHANGE_STREAM_EVENTS =
        Field.builder("enableSplitLargeChangeStreamEvents").booleanField().required();

    private static final Field.Required<Boolean> SPLIT_LARGE_CHANGE_STREAM_EVENTS_FOR_INITIAL_SYNC =
        Field.builder("splitLargeChangeStreamEventsForInitialSync").booleanField().required();

    private static final Field.Optional<Integer> REQUEST_RATE_LIMIT_BACKOFF_MS =
        Field.builder("requestRateLimitBackoffMs")
            .intField()
            .mustBePositive()
            .optional()
            .noDefault();

    private static final Field.Optional<Integer> STEADY_STATE_PRIORITY_OPTIME_THRESHOLD_SECONDS =
        Field.builder("steadyStatePriorityOptimeThresholdSeconds")
            .intField()
            .mustBePositive()
            .optional()
            .noDefault();

    private static final Field.Optional<SteadyStateSchedulingMethod>
        STEADY_STATE_SCHEDULING_METHOD =
            Field.builder("steadyStateSchedulingMethod")
                .enumField(SteadyStateSchedulingMethod.class)
                .asUpperUnderscore()
                .optional()
                .noDefault();

    private static final Field.Optional<Integer> STEADY_STATE_PRIORITY_RATIO =
        Field.builder("steadyStatePriorityRatio")
            .intField()
            .mustBePositive()
            .optional()
            .noDefault();
  }
}
