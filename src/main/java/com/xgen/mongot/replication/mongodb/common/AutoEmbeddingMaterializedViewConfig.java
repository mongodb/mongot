package com.xgen.mongot.replication.mongodb.common;

import com.google.common.annotations.VisibleForTesting;
import com.xgen.mongot.embedding.AutoEmbeddingMemoryBudget;
import com.xgen.mongot.embedding.providers.configs.EmbeddingServiceConfig;
import com.xgen.mongot.embedding.providers.congestion.AimdCongestionControl;
import com.xgen.mongot.embedding.providers.congestion.AimdCongestionControl.CongestionControlParams;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.Runtime;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.Field;
import com.xgen.mongot.util.bson.parser.Value;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.Set;
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
  private static final int DEFAULT_MAT_VIEW_WRITER_MAX_CONNECTIONS = 4;
  private static final int MAX_MAT_VIEW_WRITER_MAX_CONNECTIONS = 16;
  private static final long DEFAULT_MATERIALIZED_VIEW_NAME_FORMAT_VERSION = 1;
  private static final long DEFAULT_LEASE_MANAGER_HEARTBEAT_INTERVAL_MS =
      Duration.ofSeconds(30).toMillis();
  private static final long DEFAULT_MATERIALIZED_VIEW_STATUS_REFRESH_INTERVAL_MS =
      Duration.ofSeconds(30).toMillis();
  private static final long DEFAULT_MATERIALIZED_VIEW_OPTIME_UPDATE_INTERVAL_MS =
      Duration.ofSeconds(10).toMillis();
  private static final long DEFAULT_MATERIALIZED_VIEW_WRITER_TIMEOUT_MS =
      Duration.ofSeconds(60).toMillis();

  /**
   * Default memory budget as a percentage of JVM heap. The global default is 100% (unbounded). The
   * per-batch default is 50%, which limits peak memory from a single batch without restricting
   * overall throughput. Setting either to 100% disables that budget entirely.
   */
  static final int DEFAULT_GLOBAL_MEMORY_BUDGET_HEAP_PERCENT = 100;

  static final int DEFAULT_PER_BATCH_MEMORY_BUDGET_HEAP_PERCENT = 50;

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

  /** The maximum number of materialized view bulk write commits per second allowed on this node. */
  public final Optional<Integer> mvWriteRateLimitRps;

  /**
   * Node-level rate limit (RPS) for embedding provider API calls. Merged with per-model values
   * using min so every value acts as an upper bound.
   */
  public final Optional<Integer> embeddingProviderRpsLimit;

  /**
   * AIMD congestion control parameters for flex-tier / embedding provider rate limiting (collection
   * scan workloads). When empty, {@link AimdCongestionControl} defaults apply at runtime.
   */
  public final Optional<CongestionControlParams> congestionControl;

  /**
   * Service tiers for which Voyage flex tier is used (when deployment is Atlas). When empty, only
   * {@link EmbeddingServiceConfig.ServiceTier#COLLECTION_SCAN} uses Voyage flex tier.
   */
  public final Optional<Set<EmbeddingServiceConfig.ServiceTier>> flexTierWorkloads;

  /** The maximum number of connections to the materialized view */
  public final int matViewWriterMaxConnections;

  /**
   * Thread pool size for embedding provider work (e.g. {@link
   * com.xgen.mongot.embedding.providers.EmbeddingServiceManager}). When not set in config, defaults
   * to {@code max(1, runtime.getNumCpus())} at bootstrap (independent of Lucene indexing thread
   * counts).
   */
  public final int numEmbeddingThreads;

  /**
   * Indicates the default Materialized View Collection name format version, used for collection
   * resolver as a fallback if indexDefinition has no name format version.
   */
  public final long defaultMaterializedViewNameFormatVersion;

  /**
   * Backoff after errors that trigger a materialized-view resync (initial sync re-queue). When
   * empty, materialized view replication uses {@link
   * com.xgen.mongot.replication.mongodb.ReplicationIndexManager#DEFAULT_RESYNC_BACKOFF}.
   */
  public final Optional<Duration> resyncBackoff;

  /**
   * Backoff after transient steady-state replication errors. When empty, materialized view
   * replication uses {@link
   * com.xgen.mongot.replication.mongodb.ReplicationIndexManager#DEFAULT_TRANSIENT_BACKOFF}.
   */
  public final Optional<Duration> transientBackoff;

  /**
   * The global auto-embedding memory budget as a percentage of JVM max heap, shared across all
   * auto-embedding indexes on this mongot. Setting this to 100 disables the budget (unbounded
   * mode). Defaults to {@link #DEFAULT_GLOBAL_MEMORY_BUDGET_HEAP_PERCENT} (unbounded).
   */
  public final int globalMemoryBudgetHeapPercent;

  /**
   * The per-batch auto-embedding memory budget as a percentage of JVM max heap. Limits embedding
   * memory held by a single materialized-view batch at a time; the batch is split into sub-batches
   * that are flushed sequentially when this limit is active. Setting this to 100 disables the
   * budget (unbounded mode). Defaults to {@link #DEFAULT_PER_BATCH_MEMORY_BUDGET_HEAP_PERCENT}.
   */
  public final int perBatchMemoryBudgetHeapPercent;

  /** The interval in milliseconds at which the lease manager heartbeat task runs. */
  public final long leaseManagerHeartbeatIntervalMs;

  /**
   * The interval in milliseconds at which the materialized view manager status refresh task runs.
   */
  public final long materializedViewStatusRefreshIntervalMs;

  /** The interval in milliseconds at which the materialized view index optime is updated. */
  public final long materializedViewOptimeUpdateIntervalMs;

  /**
   * Socket read timeout (milliseconds) for the materialized view writer MongoClient (see {@link
   * com.xgen.mongot.embedding.mongodb.common.AutoEmbeddingMongoClient}). Stored as {@code long}
   * like other interval fields; validated to fit {@code int} because the driver {@code
   * socketTimeoutMs} setting is int-valued.
   */
  public final long materializedViewWriterSocketTimeoutMs;

  private AutoEmbeddingMaterializedViewConfig(
      boolean pauseAllInitialSyncs,
      List<ObjectId> pauseInitialSyncOnIndexIds,
      List<String> excludedChangestreamFields,
      boolean matchCollectionUuidForUpdateLookup,
      boolean enableSplitLargeChangeStreamEvents,
      boolean splitLargeChangeStreamEventsForInitialSync,
      int numConcurrentChangeStreams,
      int numIndexingThreads,
      int changeStreamMaxTimeMs,
      int changeStreamCursorMaxTimeSec,
      int numChangeStreamDecodingThreads,
      int requestRateLimitBackoffMs,
      int maxConcurrentEmbeddingInitialSyncs,
      int maxInFlightEmbeddingGetMores,
      int matViewWriterMaxConnections,
      int numEmbeddingThreads,
      Optional<Integer> embeddingGetMoreBatchSize,
      Optional<Integer> materializedViewSchemaVersion,
      Optional<Integer> mvWriteRateLimitRps,
      Optional<Integer> embeddingProviderRpsLimit,
      Optional<CongestionControlParams> congestionControl,
      Optional<Set<EmbeddingServiceConfig.ServiceTier>> flexTierWorkloads,
      Optional<Duration> resyncBackoff,
      Optional<Duration> transientBackoff,
      long defaultMaterializedViewNameFormatVersion,
      int globalMemoryBudgetHeapPercent,
      int perBatchMemoryBudgetHeapPercent,
      long leaseManagerHeartbeatIntervalMs,
      long materializedViewStatusRefreshIntervalMs,
      long materializedViewOptimeUpdateIntervalMs,
      long materializedViewWriterSocketTimeoutMs) {
    super(
        pauseAllInitialSyncs,
        pauseInitialSyncOnIndexIds,
        enableSplitLargeChangeStreamEvents,
        splitLargeChangeStreamEventsForInitialSync,
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
    this.mvWriteRateLimitRps = mvWriteRateLimitRps;
    this.embeddingProviderRpsLimit = embeddingProviderRpsLimit;
    this.congestionControl = congestionControl;
    this.flexTierWorkloads = flexTierWorkloads;
    this.matViewWriterMaxConnections = matViewWriterMaxConnections;
    this.numEmbeddingThreads = numEmbeddingThreads;
    this.resyncBackoff = resyncBackoff;
    this.transientBackoff = transientBackoff;
    this.defaultMaterializedViewNameFormatVersion = defaultMaterializedViewNameFormatVersion;
    this.globalMemoryBudgetHeapPercent = globalMemoryBudgetHeapPercent;
    this.perBatchMemoryBudgetHeapPercent = perBatchMemoryBudgetHeapPercent;
    this.leaseManagerHeartbeatIntervalMs = leaseManagerHeartbeatIntervalMs;
    this.materializedViewStatusRefreshIntervalMs = materializedViewStatusRefreshIntervalMs;
    this.materializedViewOptimeUpdateIntervalMs = materializedViewOptimeUpdateIntervalMs;
    this.materializedViewWriterSocketTimeoutMs = materializedViewWriterSocketTimeoutMs;
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
      Optional<Integer> optionalMatViewWriterMaxConnections,
      Optional<Integer> optionalNumEmbeddingThreads,
      Optional<Integer> embeddingGetMoreBatchSize,
      Optional<Integer> materializedViewSchemaVersion,
      Optional<Integer> mvWriteRateLimitRps,
      Optional<Integer> embeddingProviderRpsLimit,
      Optional<CongestionControlParams> congestionControl,
      Optional<Set<EmbeddingServiceConfig.ServiceTier>> flexTierWorkloads,
      Optional<Long> optionalResyncBackoffMs,
      Optional<Long> optionalTransientBackoffMs,
      Optional<Long> defaultMaterializedViewNameFormatVersion,
      Optional<Integer> globalMemoryBudgetHeapPercent,
      Optional<Integer> perBatchMemoryBudgetHeapPercent,
      Optional<Long> optionalLeaseManagerHeartbeatIntervalMs,
      Optional<Long> optionalMaterializedViewStatusRefreshIntervalMs,
      Optional<Long> optionalMaterializedViewOptimeUpdateIntervalMs,
      Optional<Long> optionalMaterializedViewWriterSocketTimeoutMs) {
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
        optionalMatViewWriterMaxConnections,
        optionalNumEmbeddingThreads,
        embeddingGetMoreBatchSize,
        materializedViewSchemaVersion,
        mvWriteRateLimitRps,
        embeddingProviderRpsLimit,
        congestionControl,
        flexTierWorkloads,
        optionalResyncBackoffMs,
        optionalTransientBackoffMs,
        defaultMaterializedViewNameFormatVersion,
        globalMemoryBudgetHeapPercent,
        perBatchMemoryBudgetHeapPercent,
        optionalLeaseManagerHeartbeatIntervalMs,
        optionalMaterializedViewStatusRefreshIntervalMs,
        optionalMaterializedViewOptimeUpdateIntervalMs,
        optionalMaterializedViewWriterSocketTimeoutMs);
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
      Optional<Integer> optionalMatViewWriterMaxConnections,
      Optional<Integer> optionalNumEmbeddingThreads,
      Optional<Integer> embeddingGetMoreBatchSize,
      Optional<Integer> materializedViewSchemaVersion,
      Optional<Integer> mvWriteRateLimitRps,
      Optional<Integer> embeddingProviderRpsLimit,
      Optional<CongestionControlParams> congestionControl,
      Optional<Set<EmbeddingServiceConfig.ServiceTier>> flexTierWorkloads,
      Optional<Long> optionalResyncBackoffMs,
      Optional<Long> optionalTransientBackoffMs,
      Optional<Long> optionalMaterializedViewNameFormatVersion,
      Optional<Integer> optionalGlobalMemoryBudgetHeapPercent,
      Optional<Integer> optionalPerBatchMemoryBudgetHeapPercent,
      Optional<Long> optionalLeaseManagerHeartbeatIntervalMs,
      Optional<Long> optionalMaterializedViewStatusRefreshIntervalMs,
      Optional<Long> optionalMaterializedViewOptimeUpdateIntervalMs,
      Optional<Long> optionalMaterializedViewWriterSocketTimeoutMs) {

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

    int matViewWriterMaxConnections =
        getMatViewWriterMaxConnectionsWithDefault(optionalMatViewWriterMaxConnections);
    Check.argIsPositive(matViewWriterMaxConnections, "matViewWriterMaxConnections");
    Check.checkArg(
        matViewWriterMaxConnections <= MAX_MAT_VIEW_WRITER_MAX_CONNECTIONS,
        "matViewWriterMaxConnections must be at most %s, got %s",
        MAX_MAT_VIEW_WRITER_MAX_CONNECTIONS,
        matViewWriterMaxConnections);

    int numEmbeddingThreads =
        getNumEmbeddingThreadsWithDefault(runtime, optionalNumEmbeddingThreads);

    embeddingGetMoreBatchSize.ifPresent(
        value -> Check.argIsPositive(value, "embeddingGetMoreBatchSize"));

    int requestRateLimitBackoffMs =
        getRequestRateLimitBackoffMsWithDefault(optionalRequestRateLimitBackoffMs);
    Check.argIsPositive(requestRateLimitBackoffMs, "requestRateLimitBackoffMs");

    mvWriteRateLimitRps.ifPresent(value -> Check.argIsPositive(value, "mvWriteRateLimitRps"));
    embeddingProviderRpsLimit.ifPresent(
        value -> {
          Check.argIsPositive(value, "embeddingProviderRpsLimit");
          Check.checkArg(
              value <= EmbeddingServiceConfig.MAX_RPS_PER_PROVIDER,
              "embeddingProviderRpsLimit must be at most %s, got %s",
              EmbeddingServiceConfig.MAX_RPS_PER_PROVIDER,
              value);
        });

    congestionControl.ifPresent(
        c -> {
          Check.argIsPositive(c.initialCwnd(), "congestionControl.initialCwnd");
          Check.argIsPositive(c.slowStartThreshold(), "congestionControl.slowStartThreshold");
          Check.argIsPositive(c.linearIncrease(), "congestionControl.linearIncrease");
          Check.argInInclusiveRange(
              c.multiplicativeDecrease(), 0.0, 1.0, "congestionControl.multiplicativeDecrease");
          Check.argIsPositive(c.idleTimeoutMillis(), "congestionControl.idleTimeoutMillis");
        });

    long defaultMaterializedViewNameFormatVersion =
        getMaterializedViewNameFormatVersionWithDefault(optionalMaterializedViewNameFormatVersion);

    Optional<Duration> resyncBackoff =
        optionalResyncBackoffMs.map(
            ms -> {
              Check.checkArg(ms > 0, "resyncBackoffMs must be positive, got %s", ms);
              return Duration.ofMillis(ms);
            });
    Optional<Duration> transientBackoff =
        optionalTransientBackoffMs.map(
            ms -> {
              Check.checkArg(ms > 0, "transientBackoffMs must be positive, got %s", ms);
              return Duration.ofMillis(ms);
            });
    int globalMemoryBudgetHeapPercent =
        getMemoryBudgetHeapPercentWithDefault(
            optionalGlobalMemoryBudgetHeapPercent,
            "globalMemoryBudgetHeapPercent",
            DEFAULT_GLOBAL_MEMORY_BUDGET_HEAP_PERCENT);
    int perBatchMemoryBudgetHeapPercent =
        getMemoryBudgetHeapPercentWithDefault(
            optionalPerBatchMemoryBudgetHeapPercent,
            "perBatchMemoryBudgetHeapPercent",
            DEFAULT_PER_BATCH_MEMORY_BUDGET_HEAP_PERCENT);

    long leaseManagerHeartbeatIntervalMs =
        getIntervalMsWithDefault(
            optionalLeaseManagerHeartbeatIntervalMs,
            "leaseManagerHeartbeatIntervalMs",
            DEFAULT_LEASE_MANAGER_HEARTBEAT_INTERVAL_MS);
    long materializedViewStatusRefreshIntervalMs =
        getIntervalMsWithDefault(
            optionalMaterializedViewStatusRefreshIntervalMs,
            "materializedViewStatusRefreshIntervalMs",
            DEFAULT_MATERIALIZED_VIEW_STATUS_REFRESH_INTERVAL_MS);
    long materializedViewOptimeUpdateIntervalMs =
        getIntervalMsWithDefault(
            optionalMaterializedViewOptimeUpdateIntervalMs,
            "materializedViewOptimeUpdateIntervalMs",
            DEFAULT_MATERIALIZED_VIEW_OPTIME_UPDATE_INTERVAL_MS);

    long materializedViewWriterSocketTimeoutMs =
        optionalMaterializedViewWriterSocketTimeoutMs.orElse(
            DEFAULT_MATERIALIZED_VIEW_WRITER_TIMEOUT_MS);
    Check.checkArg(
        materializedViewWriterSocketTimeoutMs > 0
            && materializedViewWriterSocketTimeoutMs <= Integer.MAX_VALUE,
        "materializedViewWriterSocketTimeoutMs must be in (0, Integer.MAX_VALUE], got %s",
        materializedViewWriterSocketTimeoutMs);

    return new AutoEmbeddingMaterializedViewConfig(
        globalReplicationConfig.pauseAllInitialSyncs(),
        globalReplicationConfig.pauseInitialSyncOnIndexIds(),
        globalReplicationConfig.excludedChangestreamFields(),
        globalReplicationConfig.matchCollectionUuidForUpdateLookup(),
        globalReplicationConfig.enableSplitLargeChangeStreamEvents(),
        globalReplicationConfig.splitLargeChangeStreamEventsForInitialSync(),
        numConcurrentChangeStreams,
        numIndexingThreads,
        changeStreamMaxTimeMs,
        changeStreamCursorMaxTimeSec,
        numChangeStreamDecodingThreads,
        requestRateLimitBackoffMs,
        maxConcurrentEmbeddingInitialSyncs,
        maxInFlightEmbeddingGetMores,
        matViewWriterMaxConnections,
        numEmbeddingThreads,
        embeddingGetMoreBatchSize,
        materializedViewSchemaVersion,
        mvWriteRateLimitRps,
        embeddingProviderRpsLimit,
        congestionControl,
        flexTierWorkloads,
        resyncBackoff,
        transientBackoff,
        defaultMaterializedViewNameFormatVersion,
        globalMemoryBudgetHeapPercent,
        perBatchMemoryBudgetHeapPercent,
        leaseManagerHeartbeatIntervalMs,
        materializedViewStatusRefreshIntervalMs,
        materializedViewOptimeUpdateIntervalMs,
        materializedViewWriterSocketTimeoutMs);
  }

  private static long getIntervalMsWithDefault(
      Optional<Long> optionalIntervalMs, String name, long defaultMs) {
    long value =
        optionalIntervalMs.orElseGet(
            () -> {
              LOG.info("{} not configured, defaulting to {}ms.", name, defaultMs);
              return defaultMs;
            });
    if (value <= 0) {
      LOG.warn(
          "{} must be positive, got {}. Falling back to default {}ms.", name, value, defaultMs);
      return defaultMs;
    }
    return value;
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
        .field(Fields.MAT_VIEW_WRITER_MAX_CONNECTIONS, this.matViewWriterMaxConnections)
        .field(Fields.NUM_EMBEDDING_THREADS, this.numEmbeddingThreads)
        .field(Fields.MAX_IN_FLIGHT_EMBEDDING_GET_MORES, this.maxInFlightEmbeddingGetMores)
        .field(Fields.EMBEDDING_GET_MORE_BATCH_SIZE, this.embeddingGetMoreBatchSize)
        .field(Fields.MATERIALIZED_VIEW_SCHEMA_VERSION, this.materializedViewSchemaVersion)
        .field(Fields.MV_WRITE_RATE_LIMIT_RPS, this.mvWriteRateLimitRps)
        .field(Fields.EMBEDDING_PROVIDER_RPS_LIMIT, this.embeddingProviderRpsLimit)
        .field(Fields.CONGESTION_CONTROL, this.congestionControl)
        .field(
            Fields.FLEX_TIER_WORKLOADS,
            this.flexTierWorkloads.map(AutoEmbeddingMaterializedViewConfig::sortedFlexTierList))
        .field(Fields.RESYNC_BACKOFF_MS, this.resyncBackoff.map(Duration::toMillis))
        .field(Fields.TRANSIENT_BACKOFF_MS, this.transientBackoff.map(Duration::toMillis))
        .fieldOmitDefaultValue(
            Fields.MATERIALIZED_VIEW_NAME_FORMAT_VERSION,
            this.defaultMaterializedViewNameFormatVersion)
        .fieldOmitDefaultValue(
            Fields.GLOBAL_MEMORY_BUDGET_HEAP_PERCENT, this.globalMemoryBudgetHeapPercent)
        .fieldOmitDefaultValue(
            Fields.PER_BATCH_MEMORY_BUDGET_HEAP_PERCENT, this.perBatchMemoryBudgetHeapPercent)
        .field(Fields.LEASE_MANAGER_HEARTBEAT_INTERVAL_MS, this.leaseManagerHeartbeatIntervalMs)
        .field(
            Fields.MATERIALIZED_VIEW_STATUS_REFRESH_INTERVAL_MS,
            this.materializedViewStatusRefreshIntervalMs)
        .field(
            Fields.MATERIALIZED_VIEW_OPTIME_UPDATE_INTERVAL_MS,
            this.materializedViewOptimeUpdateIntervalMs)
        .fieldOmitDefaultValue(
            Fields.MATERIALIZED_VIEW_WRITER_SOCKET_TIMEOUT_MS,
            this.materializedViewWriterSocketTimeoutMs)
        .build();
  }

  /**
   * Emits flex tiers in enum declaration order so BSON is stable across parse/serialize (unlike
   * {@link List#copyOf(Set)} on a hash set).
   */
  private static List<EmbeddingServiceConfig.ServiceTier> sortedFlexTierList(
      Set<EmbeddingServiceConfig.ServiceTier> tiers) {
    return tiers.stream().sorted().toList();
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

  @Override
  public Type getReplicationType() {
    return Type.AUTO_EMBEDDING;
  }

  public Optional<Integer> getMvWriteRateLimitRps() {
    return this.mvWriteRateLimitRps;
  }

  public Optional<Integer> getEmbeddingProviderRpsLimit() {
    return this.embeddingProviderRpsLimit;
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
          int numIndexingThreads = Math.max(1, runtime.getNumCpus());
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

  private static int getMatViewWriterMaxConnectionsWithDefault(
      Optional<Integer> optionalMatViewWriterMaxConnections) {
    return optionalMatViewWriterMaxConnections.orElseGet(
        () -> {
          LOG.info(
              "matViewWriterMaxConnections not configured, defaulting to {}.",
              DEFAULT_MAT_VIEW_WRITER_MAX_CONNECTIONS);
          return DEFAULT_MAT_VIEW_WRITER_MAX_CONNECTIONS;
        });
  }

  private static int getNumEmbeddingThreadsWithDefault(
      Runtime runtime, Optional<Integer> optionalNumEmbeddingThreads) {
    optionalNumEmbeddingThreads.ifPresent(n -> Check.argIsPositive(n, "numEmbeddingThreads"));
    int numEmbeddingThreads =
        optionalNumEmbeddingThreads.orElseGet(
            () -> {
              int derived = Math.max(1, runtime.getNumCpus());
              LOG.info(
                  "numEmbeddingThreads not configured, defaulting to {} (max(1, getNumCpus()))"
                      + ".",
                  derived);
              return derived;
            });
    Check.argIsPositive(numEmbeddingThreads, "numEmbeddingThreads");
    return numEmbeddingThreads;
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

  private static long getMaterializedViewNameFormatVersionWithDefault(
      Optional<Long> optionalMaterializedViewNameFormatVersion) {
    return optionalMaterializedViewNameFormatVersion.orElseGet(
        () -> {
          LOG.info(
              "materializedViewNameFormatVersion not configured, defaulting to {}.",
              DEFAULT_MATERIALIZED_VIEW_NAME_FORMAT_VERSION);
          return DEFAULT_MATERIALIZED_VIEW_NAME_FORMAT_VERSION;
        });
  }

  private static int getMemoryBudgetHeapPercentWithDefault(
      Optional<Integer> optionalHeapPercent, String fieldName, int defaultValue) {
    int heapPercent = optionalHeapPercent.orElse(defaultValue);
    Check.checkArg(
        heapPercent >= 1 && heapPercent <= 100,
        "%s must be between 1 and 100 (inclusive), got %s",
        fieldName,
        heapPercent);
    if (optionalHeapPercent.isEmpty()) {
      LOG.info("{} not configured, defaulting to {}%.", fieldName, defaultValue);
    }
    return heapPercent;
  }

  /**
   * Creates an {@link AutoEmbeddingMemoryBudget} from this config's global budget percent using the
   * given runtime to determine max heap size.
   */
  public AutoEmbeddingMemoryBudget createGlobalMemoryBudget(Runtime runtime) {
    return AutoEmbeddingMemoryBudget.fromHeapPercent(this.globalMemoryBudgetHeapPercent, runtime);
  }

  /**
   * Returns the per-batch memory budget in bytes derived from the configured heap percent and the
   * given runtime's max heap size. Returns {@link Long#MAX_VALUE} when the budget is set to 100%
   * (unbounded).
   */
  public long getPerBatchMemoryBudgetBytes(Runtime runtime) {
    if (this.perBatchMemoryBudgetHeapPercent >= 100) {
      return Long.MAX_VALUE;
    }
    return runtime.getMaxHeapSize().toBytes() * this.perBatchMemoryBudgetHeapPercent / 100;
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

    private static final Field.Required<Integer> MAT_VIEW_WRITER_MAX_CONNECTIONS =
        Field.builder("matViewWriterMaxConnections").intField().mustBePositive().required();

    private static final Field.Required<Integer> NUM_EMBEDDING_THREADS =
        Field.builder("numEmbeddingThreads").intField().mustBePositive().required();

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

    private static final Field.Optional<Integer> MV_WRITE_RATE_LIMIT_RPS =
        Field.builder("mvWriteRateLimitRps").intField().mustBePositive().optional().noDefault();

    private static final Field.Optional<Integer> EMBEDDING_PROVIDER_RPS_LIMIT =
        Field.builder("embeddingProviderRpsLimit")
            .intField()
            .mustBePositive()
            .optional()
            .noDefault();

    private static final Field.Optional<CongestionControlParams> CONGESTION_CONTROL =
        Field.builder("congestionControl")
            .classField(CongestionControlParams::fromBson)
            .allowUnknownFields()
            .optional()
            .noDefault();

    private static final Field.Optional<List<EmbeddingServiceConfig.ServiceTier>>
        FLEX_TIER_WORKLOADS =
            Field.builder("flexTierWorkloads")
                .listOf(
                    Value.builder()
                        .enumValue(EmbeddingServiceConfig.ServiceTier.class)
                        .asUpperUnderscore()
                        .required())
                .optional()
                .noDefault();

    private static final Field.WithDefault<Long> MATERIALIZED_VIEW_NAME_FORMAT_VERSION =
        Field.builder("defaultMaterializedViewNameFormatVersion")
            .longField()
            .mustBeNonNegative()
            .optional()
            .withDefault(DEFAULT_MATERIALIZED_VIEW_NAME_FORMAT_VERSION);

    private static final Field.Optional<Long> RESYNC_BACKOFF_MS =
        Field.builder("resyncBackoffMs").longField().mustBePositive().optional().noDefault();

    private static final Field.Optional<Long> TRANSIENT_BACKOFF_MS =
        Field.builder("transientBackoffMs").longField().mustBePositive().optional().noDefault();

    private static final Field.WithDefault<Integer> GLOBAL_MEMORY_BUDGET_HEAP_PERCENT =
        Field.builder("globalMemoryBudgetHeapPercent")
            .intField()
            .mustBePositive()
            .optional()
            .withDefault(DEFAULT_GLOBAL_MEMORY_BUDGET_HEAP_PERCENT);

    private static final Field.WithDefault<Integer> PER_BATCH_MEMORY_BUDGET_HEAP_PERCENT =
        Field.builder("perBatchMemoryBudgetHeapPercent")
            .intField()
            .mustBePositive()
            .optional()
            .withDefault(DEFAULT_PER_BATCH_MEMORY_BUDGET_HEAP_PERCENT);

    private static final Field.WithDefault<Long> LEASE_MANAGER_HEARTBEAT_INTERVAL_MS =
        Field.builder("leaseManagerHeartbeatIntervalMs")
            .longField()
            .mustBePositive()
            .optional()
            .withDefault(DEFAULT_LEASE_MANAGER_HEARTBEAT_INTERVAL_MS);

    private static final Field.WithDefault<Long> MATERIALIZED_VIEW_STATUS_REFRESH_INTERVAL_MS =
        Field.builder("materializedViewStatusRefreshIntervalMs")
            .longField()
            .mustBePositive()
            .optional()
            .withDefault(DEFAULT_MATERIALIZED_VIEW_STATUS_REFRESH_INTERVAL_MS);

    private static final Field.WithDefault<Long> MATERIALIZED_VIEW_OPTIME_UPDATE_INTERVAL_MS =
        Field.builder("materializedViewOptimeUpdateIntervalMs")
            .longField()
            .mustBePositive()
            .optional()
            .withDefault(DEFAULT_MATERIALIZED_VIEW_OPTIME_UPDATE_INTERVAL_MS);

    private static final Field.WithDefault<Long> MATERIALIZED_VIEW_WRITER_SOCKET_TIMEOUT_MS =
        Field.builder("materializedViewWriterSocketTimeoutMs")
            .longField()
            .mustBePositive()
            .optional()
            .withDefault(DEFAULT_MATERIALIZED_VIEW_WRITER_TIMEOUT_MS);
  }
}
