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
  /**
   * When enabled, the token-facets ordinal-map cache records Caffeine hit/miss/eviction stats and
   * per-index gauges scrape heap, entries, and those counters from the live searcher. Off by
   * default; enable through the normal DFF rollout. Disable to drop the per-operation stats
   * bookkeeping on the next reader refresh and to skip the scrape acquire and heap walk.
   */
  TOKEN_FACETS_CACHE_METRICS("mongot.featureFlag.enableTokenFacetsCacheMetrics", false),
  COLLECT_MULTI_PARTITION_EMPTY_SEARCH_PRODUCER(
      "mongot.featureFlag.collectMultiPartitionEmptySearchProducer", false),
  ENABLE_10K_BUCKET_LIMIT("mongot.featureFlag.enable10kBucketLimit", false),
  /**
   * Gates embeddedDocument-scoped facets (CLOUDP-413034): a facet carrying an inline {@code
   * embeddedDocument: {path, operator}} block counts only the matching children of that operator,
   * deduplicated to the root document (each root at most once per bucket).
   */
  ENABLE_EMBEDDED_SCOPED_FACET("mongot.featureFlag.enableEmbeddedScopedFacet", false),
  ENABLE_TOTAL_FACET_BUCKETS("mongot.featureFlag.enableTotalFacetBuckets", false),
  /**
   * When enabled, a search aggregation query is rejected at plan time if the requested bucket space
   * — the product of each grouping level's {@code numBuckets} / boundary range count (plus a
   * number/date {@code default} bucket), summed across independent top-level trees — exceeds {@code
   * 100_000}. Distinct from the pre-GA search-aggregation bucket-limit flag, which is removed at
   * GA; this one persists. Fallback {@code true} so the cap is on unless explicitly disabled.
   */
  ENABLE_SEARCH_AGGREGATION_REQUESTED_BUCKET_LIMIT(
      "mongot.featureFlag.enableSearchAggregationRequestedBucketLimit", true),
  BLOOM_FILTER_FOR_ID_FIELD("mongot.featureFlag.enableBloomFilterNaturalOrderInitialSync", false),
  BLOOM_FILTER_IN_STEADY_STATE("mongot.featureFlag.enableBloomFilterInSteadyState", false),
  NUMERIC_V2_SEMANTICS("mongot.featureFlag.numericV2Semantics", false),
  /**
   * When enabled, wraps the Lucene child query of {@code embeddedDocument} operators so Lucene uses
   * default bulk scoring instead of specialized bulk scorers for that subtree (mitigation for
   * select Lucene 10 perf regressions on dense embedded child queries).
   */
  DISABLE_BULK_SCORER_QUERY_FOR_EMBEDDED_DOCUMENT_CHILD(
      "mongot.featureFlag.disableBulkScorerQueryForEmbeddedDocumentChild", true),
  /**
   * When enabled, a top-level search query that carries a (non-relevance) {@code sort} is wrapped
   * so Lucene uses default bulk scoring instead of {@code DenseConjunctionBulkScorer} for the whole
   * query subtree. {@code DenseConjunctionBulkScorer} (new in Lucene 10.4) streams every matching
   * doc through a per-window bitset, which defeats the {@code TermOrdValComparator} competitive
   * iterator that makes sort-without-sorted-index cheap, causing over-collection. Forcing the
   * default bulk scorer restores the Lucene 10.1 iteration path.
   */
  DISABLE_BULK_SCORER_QUERY_FOR_SORT("mongot.featureFlag.disableBulkScorerQueryForSort", true),
  /**
   * When enabled, a {@code compound} that translates to a single required clause plus one or more
   * {@code mustNot} clauses (a {@code +required -mustNot} / {@code ReqExclBulkScorer} shape) has
   * its required clause wrapped so Lucene uses default bulk scoring instead of {@code
   * DenseConjunctionBulkScorer} for it. For a single dense required clause the {@code
   * DenseConjunctionBulkScorer} (new in Lucene 10.4) {@code intoBitSet} + {@code
   * FixedBitSet.forEach} round trip is pure overhead with no AND-intersection benefit; forcing the
   * default bulk scorer restores the Lucene 10.1 direct postings iteration.
   */
  DISABLE_BULK_SCORER_QUERY_FOR_COMPOUND_REQ_EXCL(
      "mongot.featureFlag.disableBulkScorerQueryForCompoundReqExcl", true),
  /**
   * When enabled, {@code $vectorSearch} enforces the wall-clock deadline ({@code
   * deadlineTimestampMs}) supplied by mongod, failing the query with a timeout error once it
   * elapses. When disabled, the deadline is ignored and queries run to completion.
   */
  VECTOR_SEARCH_QUERY_TIMEOUT("mongot.featureFlag.enableVectorSearchQueryTimeout", false),
  /**
   * When enabled, parks (closes) the change-stream cursor of an index that has seen no change
   * events for the configured inactivity threshold, and reactivates it when activity resumes. Gates
   * parking behavior only; inactivity detection and logging are always on.
   */
  INACTIVE_CURSOR_PARKING("mongot.featureFlag.enableInactiveCursorParking", false),
  /**
   * When enabled, 7-bit scalar-quantized (Q7) HNSW vectors are scored with the native SIMD
   * implementation ({@code mongot_vecsim}) instead of Lucene's Java scorer. Falls back to the Java
   * scorer when disabled, on unaccelerated hardware, or for non-memory-mapped inputs. Toggling
   * requires no reindex; the on-disk format is unchanged.
   */
  NATIVE_SCALAR_VECTOR_SCORING("mongot.featureFlag.enableNativeScalarVectorScoring", false),
  /**
   * When enabled, runs the auto-embedding stale lease / materialized-view garbage collection sweep:
   * marks superseded MV leases eligible for cleanup, then drops the MV collection and deletes the
   * lease once the grace periods lapse. Evaluated per sweep tick so cleanup can be turned on or off
   * at runtime; the scanner thread itself is always scheduled. Off by default.
   */
  MV_GC_ENABLED("mongot.featureFlag.enableMvGc", false),
  /**
   * Gates only the destructive step of the auto-embedding GC sweep — dropping the superseded MV
   * collection and deleting its lease — separately from {@link #MV_GC_ENABLED}. With GC on but this
   * off, the sweep runs mark-only: it flags stale leases eligible for cleanup (for observability)
   * but deletes nothing. Both on means full GC. Off by default.
   */
  MV_GC_DELETION_ENABLED("mongot.featureFlag.enableMvGcDeletion", false),
  /**
   * When enabled, the materialized-view embedding indexing path uses the in-memory {@code
   * EmbeddingRetryCache} to reuse vectors across write retries. When disabled (default), it uses
   * the no-op cache, so behavior is unchanged.
   */
  EMBEDDING_RETRY_CACHE("mongot.featureFlag.enableEmbeddingRetryCache", false),
  /**
   * When enabled, Atlas auto-embedding materialized-view replication is disk-gated independently of
   * Lucene replication disk mode.
   */
  AUTO_EMBEDDING_MONGOD_DISK_MONITOR("mongot.featureFlag.autoEmbeddingMongodDiskMonitor", false),
  /**
   * When enabled, forces materialized-view auto-embedding update events to do the full re-embed +
   * replace.
   */
  AUTO_EMBEDDING_CHANGE_STREAM_FULL_UPDATE(
      "mongot.featureFlag.autoEmbeddingChangeStreamFullUpdate", false),
  ENABLE_OTEL_TRACING("mongot.featureFlag.enableOTELTracing", false),
  /**
   * When enabled, {@link com.xgen.mongot.server.executors.BulkheadCommandExecutor} deliberately
   * crashes the process if it observes {@code OutOfMemoryError: unable to create native thread}
   * while submitting a command, instead of silently continuing while unavailable.
   */
  CRASH_ON_THREAD_CREATION_OOM("mongot.featureFlag.crashOnThreadCreationOom", false),
  /**
   * When enabled for an index, an otherwise-unhandled Voyage HTTP 400 no longer blocks that index's
   * auto-embedding replication: the failing batch is skipped and logged instead, and the index can
   * still reach READY / STEADY with the skipped documents missing. Scoped per index ({@link
   * DynamicFeatureFlagConfig.Scope#MONGOT_INDEX}); off by default so a 400 blocks.
   */
  AUTO_EMBEDDING_400_SKIP("mongot.featureFlag.autoEmbedding400Skip", false),
  QUERY_OPTIMIZER("mongot.featureFlag.queryOptimizer", false),
  RANGE_BOUNDS_MERGE_OPTIMIZATION("mongot.featureFlag.rangeBoundsMergeOptimization", false),
  /**
   * When enabled, a search aggregation query is rejected at plan time if the upper bound of its
   * total bucket count — the product of each grouping level's field cardinality, summed across
   * levels — exceeds the configured limit ({@code aggregationMaxBuckets}, default {@code
   * 1_000_000}). The bound is a conservative estimate: it assumes every unique value of every
   * grouping field lands in its own bucket, so it over-rejects rather than risking an OOM from
   * materializing the buckets.
   */
  SEARCH_AGGREGATION_BUCKET_LIMIT("mongot.featureFlag.enableSearchAggregationBucketLimit", true),
  /**
   * When enabled, range queries on a field matching the index's primary sort field use Lucene's
   * {@code IndexSortSortedNumericDocValuesRangeQuery} to skip segments/blocks outside the query
   * bounds via binary search over the sort order, instead of a full scan. Off by default.
   */
  INDEX_SORT_RANGE_QUERY_OPTIMIZATION_ENABLED(
      "mongot.featureFlag.indexSortRangeQueryOptimizationEnabled", false);

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
