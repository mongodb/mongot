package com.xgen.mongot.featureflag;

/**
 * Enumeration of all feature flags supported by Mongot.
 *
 * <p>Each feature flag has a name (used for BSON serialization) and a default state. Feature flags
 * are used to control experimental or optional functionality in the system.
 *
 * <p>Feature flags are typically managed through a {@link FeatureFlags}, which provides a type-safe
 * way to check if a feature is enabled and to create custom configurations.
 *
 * <p><b>Naming Conventions:</b>
 *
 * <p>When adding new features, follow these naming guidelines:
 *
 * <ul>
 *   <li><b>Enum constant name:</b> Use UPPER_SNAKE_CASE and name the feature for what it is,
 *       without the "enable" prefix. For example, use {@code FACETING_OVER_TOKEN_FIELDS} instead of
 *       {@code ENABLE_FACETING_OVER_TOKEN_FIELDS}.
 *   <li><b>BSON field name:</b> Use camelCase and name the feature for what it is. The "enable"
 *       prefix may be included in the BSON name for backward compatibility with existing
 *       configurations, but new features should omit it. For example, use {@code
 *       "facetingOverTokenFields"} instead of {@code "enableFacetingOverTokenFields"}.
 * </ul>
 *
 * <p>Example usage:
 *
 * <pre>{@code
 * // Check if a feature is enabled
 * FeatureFlags config = FeatureFlags.getDefault();
 * if (config.isEnabled(Feature.FACETING_OVER_TOKEN_FIELDS)) {
 *   // Use the feature
 * }
 *
 * // Create a config with specific features enabled
 * FeatureFlags customConfig = FeatureFlags.withDefaults()
 *     .enable(Feature.FACETING_OVER_TOKEN_FIELDS)
 *     .enable(Feature.NEW_EMBEDDED_SEARCH_CAPABILITIES)
 *     .build();
 * }</pre>
 *
 * @see FeatureFlags
 * @see Feature.State
 */
public enum Feature {
  STALE_STATE_TRANSITION("enableStaleStateTransition", State.DISABLED),
  RETAIN_FAILED_INDEX_DATA_ON_DISK("retainFailedIndexDataOnDisk", State.DISABLED),
  RETAIN_FAILED_INITIAL_SYNC_DATA_ON_DISK("retainFailedInitialSyncDataOnDisk", State.DISABLED),
  REMOVE_ABSENT_INDEXES_BEFORE_INITIALIZATION(
      "removeAbsentIndexesBeforeInitialization", State.DISABLED),
  FACETING_OVER_TOKEN_FIELDS("enableFacetingOverTokenFields", State.DISABLED),
  GZIP_CONF_CALL_REQUEST_COMPRESSION("enableGzipConfCallRequestCompression", State.DISABLED),
  INDEX_OR_DOC_VALUES_QUERY_FOR_IN_OPERATOR(
      "enableIndexOrDocValuesQueryForInOperator", State.DISABLED),
  NEW_EMBEDDED_SEARCH_CAPABILITIES("enableNewEmbeddedSearchCapabilities", State.DISABLED),
  SHUT_DOWN_REPLICATION_WHEN_COLLECTION_NOT_FOUND(
      "shutDownReplicationWhenCollectionNotFound", State.DISABLED),
  MAAS_METRICS("enableMaasMetrics", State.DISABLED),
  INTERNAL_MAAS_METRICS("enableInternalMaasMetrics", State.DISABLED),
  ACCURATE_NUM_EMBEDDED_ROOT_DOCS_METRIC("enableAccurateNumEmbeddedRootDocsMetric", State.DISABLED),
  NON_ZERO_ATTEMPT_UPLOADS("enableNonZeroAttemptUploads", State.DISABLED),
  INDEX_FEATURE_VERSION_FOUR("indexFeatureVersionFour", State.DISABLED),
  FLOOR_SEGMENT_MB("enableFloorSegmentMB", State.DISABLED),
  RESTART_WITH_NO_CONF_CALL("restartWithNoConfCall", State.DISABLED),
  LOSSLESS_JSON_CODEC("losslessJsonCodec", State.DISABLED),
  SORTED_INDEX("enableSortedIndex", State.DISABLED),
  TRUNCATE_AUTOCOMPLETE_TOKENS("enableTruncateAutocompleteTokens", State.DISABLED),
  ENABLE_VALIDATION_OF_RETURN_STORED_SOURCE("enableValidationOfReturnStoredSource", State.ENABLED),
  VECTOR_STORED_SOURCE("vectorStoredSource", State.ENABLED),
  INSTRUMENTED_QUERY_CACHE("instrumentedQueryCache", State.DISABLED),
  PHANTOM_REFERENCE_CLEANUP("phantomReferenceCleanup", State.DISABLED),
  FTDC_EXECUTOR_METRICS_TO_PROMETHEUS("ftdcExecutorMetricsToPrometheus", State.DISABLED),
  MAX_STRING_FACET_CARDINALITY_METRIC("enableMaxStringFacetCardinalityMetric", State.DISABLED),
  INDEX_SIZE_QUANTIZATION_METRICS("enableIndexSizeQuantizationMetrics", State.DISABLED),
  LOGGABLE_DOCUMENT_ID("mongotLoggableDocumentId", State.DISABLED),
  CACHE_WARMER("cacheWarmer", State.DISABLED),
  CONCURRENT_INDEX_PARTITION_SEARCH("enableConcurrentIndexPartitionSearch", State.DISABLED),
  CANCEL_MERGE("enableCancelMerge", State.DISABLED),
  KNN_FULL_SCAN_HEURISTIC("enableKnnFullScanHeuristic", State.DISABLED),
  VECTORX_INDEX_CREATION_ALLOWED("vectorxIndexCreationAllowed", State.DISABLED),
  // Gates automatic derivation of $vectorSearch numCandidates when the request omits it.
  AUTO_NUM_CANDIDATES("enableAutoNumCandidates", State.DISABLED),
  OVERLOAD_RETRY_SIGNAL("overloadRetrySignal", State.DISABLED),
  NESTED_VECTOR("nestedVector", State.ENABLED),
  QUERY_MEMORY_ATTRIBUTION_METRICS("queryMemoryAttributionMetrics", State.ENABLED),
  METRICS_CACHE("metricsCache", State.DISABLED),
  MERGE_ATTRIBUTION_METRICS("mergeAttributionMetrics", State.ENABLED),
  LIFECYCLE_ATTRIBUTION_METRICS("lifecycleAttributionMetrics", State.ENABLED),
  FCIS_CONSISTENT_SNAPSHOTS_LEADERSHIP("fcisConsistentSnapshotsLeadership", State.DISABLED),
  FCIS_CONSISTENT_SNAPSHOTS_DOWNLOAD("fcisConsistentSnapshotsDownload", State.DISABLED),
  FCIS_INDEX_DATA_CONDITIONAL_WRITES("fcisIndexDataConditionalWrites", State.DISABLED),
  SEARCH_AGGREGATION("searchAggregation", State.DISABLED),
  BYTE_BINARY_QUANTIZATION("byteBinaryQuantization", State.DISABLED),
  // Kill-switch for the CLOUDP-416232 one-time stale-internal-search-checkpoint reset guard in
  // ReplicationIndexManager. Enabled by default; disable to skip the reset without a rollback.
  RESET_STALE_INTERNAL_SEARCH_CHECKPOINT("resetStaleInternalSearchCheckpoint", State.ENABLED),
  CHANGE_STREAM_PRIORITY_SCHEDULING("enableChangeStreamPriorityScheduling", State.DISABLED),
  CHANGE_STREAM_ACTIVITY_OBSERVER("changeStreamActivityObserver", State.DISABLED),
  PARALLEL_CHANGE_STREAM_READERS("enableParallelChangeStreamReaders", State.DISABLED),
  // Retries a steady-state getMore rejected by Mongotune ingress rate limiting (code 462).
  RETRY_THROTTLED_GETMORE_ON_SAME_CURSOR("retryThrottledGetMoreOnSameCursor", State.DISABLED),
  // Kill-switch for the CLOUDP-429731 mmap bloom read path. Disabled by default, so bloom bitsets
  // are read in place from the memory-mapped .blm and occupy OS page cache rather than JVM heap.
  // Enable, and restart, to fall back to copying each segment's bitset onto the heap at open. The
  // .blm is byte-identical either way, so flipping this needs no reindex in either direction.
  BLOOM_FILTER_ON_HEAP("bloomFilterOnHeap", State.DISABLED),
  // Writes new segments in the native Lucene 10.4 on-disk format instead of the 9.11.1 format,
  // and stamps newly created indexes with indexCreatedVersionMajor 10 (index-wide, permanent).
  // Enabling is one-way for rollback purposes: anything written under the flag is unreadable by
  // binaries carrying Lucene < 10.4, and a created-10 index stays that way even after the flag is
  // disabled — only a re-index clears it. Do not enable until the fleet minimum carries 10.4 jars.
  LUCENE_104_CODEC("lucene104Codec", State.DISABLED),
  // Quarantines a config journal entry whose index definition cannot be parsed instead of
  // failing startup, so that one malformed definition does not take every index on the host
  // down with it. Disabled by default: until it is enabled the journal is still read
  // fail-fast, exactly as before.
  QUARANTINE_UNPARSEABLE_INDEX("quarantineUnparseableIndex", State.DISABLED),
  // Kill switch for the compat gate. Enabling stops the conf call enforcing either registry, so a
  // gated member is neither rejected nor carry-forward-checked, without needing a rollback. Named
  // for the off action so the gate stays on by default and MMS can turn it off by writing true,
  // the same shape every other flag piped through MongotConfigSvc.configFeatureFlags uses.
  FORCE_DISABLE_COMPAT_GATE("forceDisableCompatGate", State.DISABLED),
  // Constructs blobstore snapshotter resources at startup even when blobstore uploads and downloads
  // are disabled, so a later runtime request can flip them on without restarting mongot.
  ALWAYS_BOOTSTRAP_SNAPSHOTTER_RESOURCES("alwaysBootstrapSnapshotterResources", State.DISABLED),
  // CLOUDP-447390: escalates the AE resync id-order tripwire from REPORT (metric + warning,
  // default) to FAIL — aborting the resync on the first source _id ordering inversion.
  AE_RESYNC_ID_ORDER_TRIPWIRE_FAIL_FAST("aeResyncIdOrderTripwireFailFast", State.DISABLED),
  // CLOUDP-449412: selects which AE resync classification acts — the sort-merge (default) or the
  // per-window hash join. The other one always runs in the background as a cheap, set-based shadow
  // (no compareDocuments). When the two disagree, resyncMergeClassificationMismatches is
  // incremented
  // and the index id plus the first few differing _ids are logged.
  AE_RESYNC_HASH_JOIN("aeResyncHashJoin", State.DISABLED);

  private final String name;
  private final State defaultState;

  /**
   * Constructs a Feature with the given name and default state.
   *
   * @param name the BSON field name for this feature
   * @param defaultState the default state of this feature
   */
  Feature(String name, State defaultState) {
    this.name = name;
    this.defaultState = defaultState;
  }

  /**
   * Returns the BSON field name for this feature.
   *
   * <p>This name is used when serializing and deserializing feature configurations to/from BSON.
   *
   * @return the BSON field name
   */
  public String getName() {
    return this.name;
  }

  /**
   * Returns the default state of this feature.
   *
   * <p>When a new {@link FeatureFlags} is created without explicitly setting this feature, it will
   * use this default state.
   *
   * @return the default state
   */
  public State getDefaultState() {
    return this.defaultState;
  }

  /**
   * Enumeration representing the possible states of a feature flag.
   *
   * <p>A feature can be either {@link #ENABLED} or {@link #DISABLED}.
   */
  public enum State {
    /** Feature is enabled and active. */
    ENABLED,
    /** Feature is disabled and inactive. */
    DISABLED
  }
}
