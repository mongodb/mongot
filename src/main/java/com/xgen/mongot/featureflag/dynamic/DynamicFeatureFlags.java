package com.xgen.mongot.featureflag.dynamic;

/**
 * Enumeration of all dynamic feature flags supported by Mongot.
 *
 * <p>Each dynamic feature flag has a name (used for identification and evaluation) and a default
 * fallback state. Dynamic feature flags are evaluated at runtime through the {@link
 * DynamicFeatureFlagRegistry} and support gradual rollouts, allow/block lists, and scoped
 * evaluation.
 *
 * <p>Unlike static {@link com.xgen.mongot.featureflag.Feature} flags which are configured per
 * mongot instance, dynamic feature flags can be controlled remotely and evaluated based on entity
 * IDs (org, group, cluster, index, or query).
 *
 * <p>Dynamic Feature Flag config with same name must be created in MMS repo
 *
 * <p>Example usage:
 *
 * <pre>{@code
 * // Evaluate a cluster-invariant feature flag
 * DynamicFeatureFlags flag = DynamicFeatureFlags.DRILL_SIDEWAYS_CONCURRENCY;
 * boolean isEnabled = registry.evaluateClusterInvariant(flag);
 *
 * // Evaluate a feature flag for a specific entity (index or query)
 * boolean isEnabled = registry.evaluate(flag.getName(), entityId, flag.getFallback());
 * }</pre>
 *
 * @see DynamicFeatureFlagRegistry
 */
public enum DynamicFeatureFlags {
  DRILL_SIDEWAYS_CONCURRENCY("mongot.featureFlag.enableDrillSidewaysConcurrency", false),
  DRILL_SIDEWAYS_FACETING("mongot.featureFlag.enableDrillSidewaysFaceting", false),
  NUM_FIELDS_PER_DATATYPE_METRIC("mongot.featureFlag.enableNumFieldsPerDatatypeMetric", false),
  COLLECT_MULTI_PARTITION_EMPTY_SEARCH_PRODUCER(
      "mongot.featureFlag.collectMultiPartitionEmptySearchProducer", false),
  ENABLE_10K_BUCKET_LIMIT("mongot.featureFlag.enable10kBucketLimit", false),
  ENABLE_TOTAL_STRING_FACET_BUCKETS("mongot.featureFlag.enableTotalStringFacetBuckets", false),
  BLOOM_FILTER_FOR_ID_FIELD("mongot.featureFlag.enableBloomFilterNaturalOrderInitialSync", false),
  NUMERIC_V2_SEMANTICS("mongot.featureFlag.numericV2Semantics", false);

  private final String name;
  private final boolean fallback;

  DynamicFeatureFlags(String name, boolean fallback) {
    this.name = name;
    this.fallback = fallback;
  }

  public String getName() {
    return this.name;
  }

  public boolean getFallback() {
    return this.fallback;
  }
}
