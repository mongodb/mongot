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
  REMOVE_ABSENT_INDEXES_BEFORE_INITIALIZATION(
      "removeAbsentIndexesBeforeInitialization", State.DISABLED),
  FACETING_OVER_TOKEN_FIELDS("enableFacetingOverTokenFields", State.DISABLED),
  GZIP_CONF_CALL_REQUEST_COMPRESSION("enableGzipConfCallRequestCompression", State.DISABLED),
  INDEX_OR_DOC_VALUES_QUERY_FOR_IN_OPERATOR(
      "enableIndexOrDocValuesQueryForInOperator", State.DISABLED),
  NEW_EMBEDDED_SEARCH_CAPABILITIES("enableNewEmbeddedSearchCapabilities", State.DISABLED),
  SHUT_DOWN_REPLICATION_WHEN_COLLECTION_NOT_FOUND(
      "shutDownReplicationWhenCollectionNotFound", State.DISABLED),
  INITIAL_INDEX_STATUS_UNKNOWN("initialIndexStatusUnknown", State.ENABLED),
  MAAS_METRICS("enableMaasMetrics", State.DISABLED),
  ACCURATE_NUM_EMBEDDED_ROOT_DOCS_METRIC("enableAccurateNumEmbeddedRootDocsMetric", State.DISABLED),
  NON_ZERO_ATTEMPT_UPLOADS("enableNonZeroAttemptUploads", State.DISABLED),
  INDEX_FEATURE_VERSION_FOUR("indexFeatureVersionFour", State.DISABLED),
  FLOOR_SEGMENT_MB("enableFloorSegmentMB", State.DISABLED),
  RESTART_WITH_NO_CONF_CALL("restartWithNoConfCall", State.DISABLED),
  SORTED_INDEX("enableSortedIndex", State.DISABLED),
  TRUNCATE_AUTOCOMPLETE_TOKENS("enableTruncateAutocompleteTokens", State.DISABLED),
  ENABLE_VALIDATION_OF_RETURN_STORED_SOURCE("enableValidationOfReturnStoredSource", State.DISABLED),
  VECTOR_STORED_SOURCE("vectorStoredSource", State.DISABLED),
  INSTRUMENTED_QUERY_CACHE("instrumentedQueryCache", State.DISABLED),
  PHANTOM_REFERENCE_CLEANUP("phantomReferenceCleanup", State.DISABLED),
  FTDC_EXECUTOR_METRICS_TO_PROMETHEUS("ftdcExecutorMetricsToPrometheus", State.DISABLED),
  MAX_STRING_FACET_CARDINALITY_METRIC("enableMaxStringFacetCardinalityMetric", State.DISABLED),
  INDEX_SIZE_QUANTIZATION_METRICS("enableIndexSizeQuantizationMetrics", State.DISABLED);

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
