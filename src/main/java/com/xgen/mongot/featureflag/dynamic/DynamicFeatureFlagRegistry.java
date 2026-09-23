package com.xgen.mongot.featureflag.dynamic;

import static com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlagConfig.Phase;
import static com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlagConfig.Scope;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.flogger.FluentLogger;
import com.google.errorprone.annotations.RestrictedApi;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import com.xgen.mongot.index.DynamicFeatureFlagsMetricsRecorder;
import com.xgen.mongot.index.lucene.explain.explainers.DynamicFeatureFlagFeatureExplainer;
import com.xgen.mongot.index.lucene.explain.information.FeatureFlagEvaluationSpec;
import com.xgen.mongot.index.lucene.explain.tracing.Explain;
import com.xgen.mongot.index.query.Query;
import com.xgen.mongot.metrics.MetricsFactory;
import io.micrometer.core.instrument.Tags;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.CRC32;
import org.bson.types.ObjectId;

/**
 * Registry for managing dynamic feature flag configurations.
 *
 * <p>This class provides a centralized registry for dynamic feature flags used across the
 * application. It maintains a thread-safe collection of feature flag configurations that can be
 * retrieved by name. The registry supports updating configurations at runtime and clearing all
 * registered feature flags.
 */
public class DynamicFeatureFlagRegistry {
  private static final FluentLogger FLOGGER = FluentLogger.forEnclosingClass();

  /**
   * Process-wide {@link DynamicFeatureFlagRegistry} for non-DI code paths. Set exactly once by
   * {@link #initializeProcessInstance(DynamicFeatureFlagRegistry)} at bootstrap, read via {@link
   * #getStatic}; all access guarded by the {@code DynamicFeatureFlagRegistry.class} monitor.
   */
  @GuardedBy("DynamicFeatureFlagRegistry.class")
  private static Optional<DynamicFeatureFlagRegistry> globalDynamicFeatureFlagRegistry =
      Optional.empty();

  private volatile Map<String, DynamicFeatureFlagConfig> registeredDynamicFeatureFlagConfigs =
      new ConcurrentHashMap<>();
  private volatile Map<String, InternalDynamicFeatureFlagConfig> internalConfigs =
      new ConcurrentHashMap<>();

  private final Map<String, AtomicLong> clusterInvariantGauges = new HashMap<>();

  private final Optional<ObjectId> orgId;
  private final Optional<ObjectId> groupId;
  private final Optional<ObjectId> clusterId;
  private final Optional<MetricsFactory> metricsFactory;

  private final boolean isInitializedByInitialConfCall;

  /**
   * Internal, optimized "domain object" for a feature flag. This is pre-computed for fast
   * evaluation. For cluster-invariant scopes (ORG, GROUP, MONGOT_CLUSTER), the evaluation result is
   * eagerly cached in {@code cachedResult} so that subsequent lookups are a simple field read.
   */
  private record InternalDynamicFeatureFlagConfig(
      Phase phase,
      int seed,
      Set<ObjectId> entityIdAllowList,
      Set<ObjectId> entityIdBlockList,
      int rolloutPercentage,
      Scope scope,
      Optional<CachedEvaluation> cachedResult) {}

  /** Pre-computed evaluation result for a cluster-invariant flag. */
  private record CachedEvaluation(
      boolean result, FeatureFlagEvaluationSpec.DecisiveField decisiveField) {}

  /**
   * initialize {@link DynamicFeatureFlagRegistry} from dynamicFeatureFlagConfigs from initial conf
   * call When initial conf call returns ConfCallResponse with empty dynamic feature flags and there
   * are existing feature flags in configJournal, we shall mark {@link
   * this#isInitializedByInitialConfCall} to be true to avoid double initialization. orgId, groupId
   * and clusterId are passed in this registry to simplify evaluating cluster invariant DFFs.
   *
   * @param dynamicFeatureFlagConfigs dynamicFeatureFlagConfigs from initial conf call.
   * @param orgId orgId of the cluster.
   * @param groupId groupId of the cluster.
   * @param clusterId uniqueId of the cluster.
   */
  public DynamicFeatureFlagRegistry(
      Optional<List<DynamicFeatureFlagConfig>> dynamicFeatureFlagConfigs,
      Optional<ObjectId> orgId,
      Optional<ObjectId> groupId,
      Optional<ObjectId> clusterId) {
    this(dynamicFeatureFlagConfigs, orgId, groupId, clusterId, Optional.empty());
  }

  /**
   * initialize {@link DynamicFeatureFlagRegistry} with metrics support. When a {@link
   * MetricsFactory} is provided, the registry emits a gauge for each cluster-invariant dynamic
   * feature flag on every config update, allowing cross-cluster comparison of flag states.
   *
   * @param dynamicFeatureFlagConfigs dynamicFeatureFlagConfigs from initial conf call.
   * @param orgId orgId of the cluster.
   * @param groupId groupId of the cluster.
   * @param clusterId uniqueId of the cluster.
   * @param metricsFactory optional MetricsFactory for emitting flag status gauges.
   */
  public DynamicFeatureFlagRegistry(
      Optional<List<DynamicFeatureFlagConfig>> dynamicFeatureFlagConfigs,
      Optional<ObjectId> orgId,
      Optional<ObjectId> groupId,
      Optional<ObjectId> clusterId,
      Optional<MetricsFactory> metricsFactory) {
    this.isInitializedByInitialConfCall = dynamicFeatureFlagConfigs.isPresent();
    this.orgId = orgId;
    this.groupId = groupId;
    this.clusterId = clusterId;
    this.metricsFactory = metricsFactory;
    dynamicFeatureFlagConfigs.ifPresent(this::updateDynamicFeatureFlags);
  }

  /**
   * Synthetic cluster id for {@link #communityDefault()}. Supplying this id caches the {@code true}
   * result and ensure the evaluation returns true. The synthetic value are irrelevant (ENABLED
   * short-circuits the rollout hash); all-zero is a deterministic synthetic value.
   */
  static final ObjectId ONPREM_SENTINEL_CLUSTER_ID = new ObjectId(new byte[12]);

  /**
   * Registry with no flags configured; {@link #evaluateClusterInvariant(DynamicFeatureFlags)}
   * returns each flag's defined fallback.
   */
  public static DynamicFeatureFlagRegistry empty() {
    return new DynamicFeatureFlagRegistry(
        Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());
  }

  /**
   * Community has no DFF, so this factory seeds the flags that should default on for community /
   * on-prem — {@link DynamicFeatureFlags#BLOOM_FILTER_FOR_ID_FIELD}, {@link
   * DynamicFeatureFlags#MV_GC_ENABLED} and {@link DynamicFeatureFlags#MV_GC_DELETION_ENABLED} — as
   * {@code ENABLED}, while leaving every other flag unregistered (falling through to its declared
   * fallback, identical to {@link #empty()}).
   *
   * <p>A synthetic cluster id ({@link #ONPREM_SENTINEL_CLUSTER_ID}) is supplied so these flags'
   * evaluations are eagerly cached by {@code buildInternalConfig}. Without a cluster id a {@code
   * MONGOT_CLUSTER}-scoped flag would not be cached and {@code evaluateClusterInvariant} would fall
   * back to its declared {@code false} default. The scope here is only the synthetic caching key;
   * in Atlas these flags are scoped by the control plane (e.g. {@code MV_GC_ENABLED} rolls out per
   * group), independently of this default.
   */
  public static DynamicFeatureFlagRegistry communityDefault() {
    return new DynamicFeatureFlagRegistry(
        Optional.of(
            List.of(
                communityEnabled(DynamicFeatureFlags.BLOOM_FILTER_FOR_ID_FIELD),
                communityEnabled(DynamicFeatureFlags.MV_GC_ENABLED),
                communityEnabled(DynamicFeatureFlags.MV_GC_DELETION_ENABLED))),
        Optional.empty(),
        Optional.empty(),
        Optional.of(ONPREM_SENTINEL_CLUSTER_ID));
  }

  /**
   * Enterprise counterpart of {@link #communityDefault()}; seeds the same on-prem default flags.
   */
  public static DynamicFeatureFlagRegistry enterpriseDefault() {
    return new DynamicFeatureFlagRegistry(
        Optional.of(
            List.of(
                communityEnabled(DynamicFeatureFlags.BLOOM_FILTER_FOR_ID_FIELD),
                communityEnabled(DynamicFeatureFlags.MV_GC_ENABLED),
                communityEnabled(DynamicFeatureFlags.MV_GC_DELETION_ENABLED))),
        Optional.empty(),
        Optional.empty(),
        Optional.of(ONPREM_SENTINEL_CLUSTER_ID));
  }

  private static DynamicFeatureFlagConfig communityEnabled(DynamicFeatureFlags flag) {
    return new DynamicFeatureFlagConfig(
        flag.getName(), Phase.ENABLED, List.of(), List.of(), 100, Scope.MONGOT_CLUSTER);
  }

  /**
   * Evaluates a dynamic feature flag for a cluster-invariant scope (org, group, or mongot cluster),
   * using the flag's name and default fallback when the flag is absent.
   */
  public boolean evaluateClusterInvariant(DynamicFeatureFlags featureFlag) {
    return evaluateClusterInvariant(featureFlag.getName(), featureFlag.getFallback());
  }

  /**
   * Evaluates a dynamic feature flag for an cluster invariant entityId (eg. orgId). Introducing
   * this method to simplify evaluation of DFFs with ORG, MONGOT_CLUSTER and GROUP scopes.
   *
   * @param featureFlagName The unique name of the feature flag to evaluate.
   * @param fallback The default value to return if the feature flag is not registered.
   * @return {@code true} if the feature is enabled, {@code false} if disabled or not found.
   */
  public boolean evaluateClusterInvariant(String featureFlagName, boolean fallback) {
    InternalDynamicFeatureFlagConfig config = this.internalConfigs.get(featureFlagName);
    if (config == null) {
      FLOGGER.atWarning().atMostEvery(5, TimeUnit.MINUTES).log(
          "Feature flag not present in registry, falling back to provided value");
      return recordAndReturnFallback(featureFlagName, fallback);
    }

    if (config.cachedResult().isPresent()) {
      CachedEvaluation cached = config.cachedResult().get();
      recordEvaluation(featureFlagName, cached.result(), cached.decisiveField());
      return cached.result();
    }

    return switch (config.scope()) {
      case Scope.MONGOT_QUERY, Scope.MONGOT_INDEX, Scope.UNSPECIFIED -> {
        FLOGGER.atWarning().atMostEvery(5, TimeUnit.MINUTES).log(
            """
            Evaluating Cluster variant DFFs with evaluateClusterInvariant. \
            Check feature flag definitions.\
            """);
        yield recordAndReturnFallback(featureFlagName, fallback);
      }
      // Cluster-invariant scope but entity ID was Optional.empty(), so not cached
      default -> recordAndReturnFallback(featureFlagName, fallback);
    };
  }

  /**
   * Evaluates a dynamic feature flag for an entityId (eg. indexId).
   *
   * @param featureFlagName The unique name of the feature flag to evaluate.
   * @param entityId The ObjectId of the entity (e.g., indexId) to check.
   * @param fallback The default value to return if the feature flag is not registered.
   * @return {@code true} if the feature is enabled, {@code false} if disabled or not found.
   */
  public boolean evaluate(String featureFlagName, ObjectId entityId, boolean fallback) {
    InternalDynamicFeatureFlagConfig config = this.internalConfigs.get(featureFlagName);

    if (config == null) {
      FLOGGER.atWarning().atMostEvery(5, TimeUnit.MINUTES).log(
          "Feature flag not present in registry, falling back to provided value");
      return recordAndReturnFallback(featureFlagName, fallback);
    }

    CachedEvaluation outcome = computeEvaluation(config, entityId);
    recordEvaluation(featureFlagName, outcome.result(), outcome.decisiveField());
    return outcome.result();
  }

  /**
   * Evaluates a dynamic feature flag for a query.
   *
   * @param featureFlagName The unique name of the feature flag to evaluate.
   * @param query The Query object to check.
   * @param fallback The default value to return if the feature flag is not registered.
   * @return {@code true} if the feature is enabled, {@code false} if disabled or not found.
   */
  public boolean evaluate(String featureFlagName, Query query, boolean fallback) {
    InternalDynamicFeatureFlagConfig config = this.internalConfigs.get(featureFlagName);

    if (config == null) {
      FLOGGER.atWarning().atMostEvery(5, TimeUnit.MINUTES).log(
          "Feature flag not present in registry, falling back to provided value");
      return recordAndReturnFallback(featureFlagName, fallback);
    }

    // Check phase first - ENABLED/DISABLED take precedence over everything
    Optional<Boolean> phaseResult = evaluatePhase(featureFlagName, config);
    if (phaseResult.isPresent()) {
      return phaseResult.get();
    }

    // CONTROLLED phase: query-scoped flags skip allow/block list; check rollout percentage
    if (config.rolloutPercentage() == 0) {
      recordEvaluation(
          featureFlagName, false, FeatureFlagEvaluationSpec.DecisiveField.ROLLOUT_PERCENTAGE);
      return false;
    }
    if (config.rolloutPercentage() == 100) {
      recordEvaluation(
          featureFlagName, true, FeatureFlagEvaluationSpec.DecisiveField.ROLLOUT_PERCENTAGE);
      return true;
    }

    boolean result =
        isHashedIdWithinPercentage(
            config.seed(), System.identityHashCode(query), config.rolloutPercentage());

    recordEvaluation(
        featureFlagName, result, FeatureFlagEvaluationSpec.DecisiveField.ROLLOUT_PERCENTAGE);
    return result;
  }

  /**
   * Pure evaluation logic with no side effects. Returns the evaluation result and the decisive
   * field without recording metrics or explain output. Used for caching cluster-invariant results
   * and by {@link #evaluate(String, ObjectId, boolean)}.
   */
  private CachedEvaluation computeEvaluation(
      InternalDynamicFeatureFlagConfig config, ObjectId entityId) {
    return switch (config.phase()) {
      case ENABLED -> new CachedEvaluation(true, FeatureFlagEvaluationSpec.DecisiveField.PHASE);
      case DISABLED, UNSPECIFIED ->
          new CachedEvaluation(false, FeatureFlagEvaluationSpec.DecisiveField.PHASE);
      case CONTROLLED -> {
        if (config.entityIdBlockList().contains(entityId)) {
          yield new CachedEvaluation(false, FeatureFlagEvaluationSpec.DecisiveField.BLOCK_LIST);
        }
        if (config.entityIdAllowList().contains(entityId)) {
          yield new CachedEvaluation(true, FeatureFlagEvaluationSpec.DecisiveField.ALLOW_LIST);
        }
        if (config.rolloutPercentage() == 0) {
          yield new CachedEvaluation(
              false, FeatureFlagEvaluationSpec.DecisiveField.ROLLOUT_PERCENTAGE);
        }
        if (config.rolloutPercentage() == 100) {
          yield new CachedEvaluation(
              true, FeatureFlagEvaluationSpec.DecisiveField.ROLLOUT_PERCENTAGE);
        }
        boolean result =
            isHashedIdWithinPercentage(
                config.seed(), entityId.toByteArray(), config.rolloutPercentage());
        yield new CachedEvaluation(
            result, FeatureFlagEvaluationSpec.DecisiveField.ROLLOUT_PERCENTAGE);
      }
    };
  }

  /**
   * Evaluates the phase of a feature flag. Returns the result if phase determines the outcome
   * (ENABLED/DISABLED/UNSPECIFIED), or empty if evaluation should continue (CONTROLLED phase).
   */
  private Optional<Boolean> evaluatePhase(
      String featureFlagName, InternalDynamicFeatureFlagConfig config) {
    return switch (config.phase()) {
      case ENABLED -> {
        recordEvaluation(featureFlagName, true, FeatureFlagEvaluationSpec.DecisiveField.PHASE);
        yield Optional.of(true);
      }
      case DISABLED, UNSPECIFIED -> {
        recordEvaluation(featureFlagName, false, FeatureFlagEvaluationSpec.DecisiveField.PHASE);
        yield Optional.of(false);
      }
      case CONTROLLED -> Optional.empty();
    };
  }

  private boolean recordAndReturnFallback(String featureFlagName, boolean fallback) {
    recordEvaluation(featureFlagName, fallback, FeatureFlagEvaluationSpec.DecisiveField.FALLBACK);
    return fallback;
  }

  @VisibleForTesting
  public boolean isHashedIdWithinPercentage(
      int seed, byte[] entityByteArray, int rolloutPercentage) {
    CRC32 hasher = new CRC32();
    for (int shift = 24; shift >= 0; shift -= 8) {
      hasher.update(seed >>> shift);
    }
    hasher.update(entityByteArray);
    long hashedValue = hasher.getValue();
    return hashedValue % 100 < rolloutPercentage;
  }

  @VisibleForTesting
  public boolean isHashedIdWithinPercentage(int seed, int entityHash, int rolloutPercentage) {
    CRC32 hasher = new CRC32();
    for (int shift = 24; shift >= 0; shift -= 8) {
      hasher.update(seed >>> shift);
    }
    for (int shift = 24; shift >= 0; shift -= 8) {
      hasher.update(entityHash >>> shift);
    }
    long hashedValue = hasher.getValue();
    return hashedValue % 100 < rolloutPercentage;
  }

  /**
   * Retrieves a feature flag configuration by name.
   *
   * <p>If the requested feature flag is not registered, a log message is generated listing all
   * currently supported feature flags.
   *
   * @param featureFlagName the name of the feature flag to retrieve
   * @return an Optional containing the feature flag configuration if found, or an empty Optional if
   *     not registered
   */
  public Optional<DynamicFeatureFlagConfig> getFeatureFlag(String featureFlagName) {
    var dynamicFeatureFlagConfig =
        Optional.ofNullable(this.registeredDynamicFeatureFlagConfigs.get(featureFlagName));
    if (dynamicFeatureFlagConfig.isEmpty()) {
      FLOGGER.atInfo().atMostEvery(6, TimeUnit.HOURS).log(
          "FeatureFlag: %s not registered yet", featureFlagName);
    }
    return dynamicFeatureFlagConfig;
  }

  /**
   * Updates the registry with a new list of feature flag configurations.
   *
   * <p>This method converts each incoming DTO ({@link DynamicFeatureFlagConfig}) into an optimized
   * internal record via {@link #buildInternalConfig(DynamicFeatureFlagConfig)}, which also eagerly
   * caches evaluation results for cluster-invariant scopes (ORG, GROUP, MONGOT_CLUSTER). The new
   * maps are then published via a volatile write, and cluster-invariant gauges are emitted (or
   * zeroed out for removed flags).
   *
   * @param dynamicFeatureFlagConfigs The complete list of (DTO) feature flag configurations to
   *     register.
   */
  public void updateDynamicFeatureFlags(List<DynamicFeatureFlagConfig> dynamicFeatureFlagConfigs) {
    Map<String, DynamicFeatureFlagConfig> newFlagsMap = new ConcurrentHashMap<>();
    Map<String, InternalDynamicFeatureFlagConfig> newInternalConfigsMap = new ConcurrentHashMap<>();

    for (DynamicFeatureFlagConfig dto : dynamicFeatureFlagConfigs) {
      String featureName = dto.featureFlagName();
      newFlagsMap.put(featureName, dto);
      newInternalConfigsMap.put(featureName, buildInternalConfig(dto));
    }

    this.registeredDynamicFeatureFlagConfigs = newFlagsMap;
    this.internalConfigs = newInternalConfigsMap;
    emitClusterInvariantGauges();
  }

  /**
   * Converts a DTO into an optimized internal config with an eagerly cached evaluation result for
   * cluster-invariant scopes (ORG, GROUP, MONGOT_CLUSTER). Non-cluster-invariant scopes get {@code
   * Optional.empty()} for the cached result.
   */
  private InternalDynamicFeatureFlagConfig buildInternalConfig(DynamicFeatureFlagConfig dto) {
    InternalDynamicFeatureFlagConfig base = convertToInternalConfig(dto);

    Optional<ObjectId> entityId =
        switch (base.scope()) {
          case ORG -> this.orgId;
          case GROUP -> this.groupId;
          case MONGOT_CLUSTER -> this.clusterId;
          default -> Optional.empty();
        };

    Optional<CachedEvaluation> cached = entityId.map(id -> computeEvaluation(base, id));

    return new InternalDynamicFeatureFlagConfig(
        base.phase(),
        base.seed(),
        base.entityIdAllowList(),
        base.entityIdBlockList(),
        base.rolloutPercentage(),
        base.scope(),
        cached);
  }

  /**
   * Emits a gauge for each cluster-invariant dynamic feature flag, reporting whether it evaluates
   * to enabled (1) or disabled (0) on this cluster. Tags include the flag name and scope, enabling
   * cross-cluster comparison via Prometheus/Grafana joins.
   *
   * <p>Gauges for flags that have been removed since the last update are zeroed out to avoid
   * dashboards displaying stale values. The stale gauge meter remains registered in the
   * MeterRegistry (reporting 0) until the process restarts; if the flag reappears, {@link
   * MetricsFactory#numGauge} replaces the old meter automatically.
   */
  private void emitClusterInvariantGauges() {
    if (this.metricsFactory.isEmpty()) {
      return;
    }
    MetricsFactory factory = this.metricsFactory.get();

    Set<String> currentFlags = new HashSet<>();

    for (var entry : this.internalConfigs.entrySet()) {
      String flagName = entry.getKey();
      InternalDynamicFeatureFlagConfig config = entry.getValue();

      config
          .cachedResult()
          .ifPresent(
              cached -> {
                currentFlags.add(flagName);
                AtomicLong gauge =
                    factory.numGauge(
                        "clusterInvariantStatus",
                        Tags.of("featureFlagName", flagName, "scope", config.scope().name()));
                gauge.set(cached.result() ? 1 : 0);
                this.clusterInvariantGauges.put(flagName, gauge);
              });
    }

    var it = this.clusterInvariantGauges.entrySet().iterator();
    while (it.hasNext()) {
      var gaugeEntry = it.next();
      if (!currentFlags.contains(gaugeEntry.getKey())) {
        gaugeEntry.getValue().set(0);
        it.remove();
      }
    }
  }

  private InternalDynamicFeatureFlagConfig convertToInternalConfig(DynamicFeatureFlagConfig dto) {
    return switch (dto.phase()) {
      case ENABLED ->
          new InternalDynamicFeatureFlagConfig(
              Phase.ENABLED, 0, Set.of(), Set.of(), 100, dto.scope(), Optional.empty());
      case UNSPECIFIED, DISABLED ->
          new InternalDynamicFeatureFlagConfig(
              dto.phase(), 0, Set.of(), Set.of(), 0, dto.scope(), Optional.empty());
      case CONTROLLED -> {
        Set<ObjectId> allowList = new HashSet<>(dto.allowedList());
        Set<ObjectId> blockList = new HashSet<>(dto.blockedList());
        int seed = dto.featureFlagName().hashCode();

        yield new InternalDynamicFeatureFlagConfig(
            Phase.CONTROLLED,
            seed,
            allowList,
            blockList,
            dto.sanitizedRolloutPercentage(),
            dto.scope(),
            Optional.empty());
      }
    };
  }

  private void recordEvaluation(
      String featureFlagName,
      boolean evaluationResult,
      FeatureFlagEvaluationSpec.DecisiveField decisiveField) {
    DynamicFeatureFlagsMetricsRecorder.recordEvaluation(featureFlagName, evaluationResult);
    appendToExplainIfEnabled(
        new FeatureFlagEvaluationSpec(featureFlagName, evaluationResult, decisiveField));
  }

  private void appendToExplainIfEnabled(FeatureFlagEvaluationSpec spec) {
    if (Explain.isEnabled()) {
      Explain.getQueryInfo()
          .map(
              queryInfo ->
                  queryInfo.getFeatureExplainer(
                      DynamicFeatureFlagFeatureExplainer.class,
                      DynamicFeatureFlagFeatureExplainer::new))
          .ifPresent(explainer -> explainer.addFeatureFlagEvaluationSpec(spec));
    }
  }

  /**
   * Check if dynamic feature flag registry is already initialized. This is used when
   * initialConfCallResponse fails and cause {@link DynamicFeatureFlagRegistry} not being
   * initialized.
   *
   * <p>In this case, we shall initialize from {@link com.xgen.mongot.config.backup.ConfigJournalV1}
   * via {@link com.xgen.mongot.config.manager.DefaultConfigManager#initializeFromExistingJournal}
   *
   * @return true if {@link DynamicFeatureFlagRegistry#registeredDynamicFeatureFlagConfigs} is not
   *     empty.
   */
  public boolean isInitializedByInitialConfCall() {
    return this.isInitializedByInitialConfCall;
  }

  /**
   * Fetch a snapshot of existing dynamic feature flags for persistence of {@link
   * com.xgen.mongot.config.backup.ConfigJournalV1}
   *
   * @return list of sorted dynamic feature flags
   */
  public List<DynamicFeatureFlagConfig> snapshot() {
    return this.registeredDynamicFeatureFlagConfigs.values().stream()
        .sorted(DynamicFeatureFlagConfig.FEATURE_FLAG_SORTER)
        .toList();
  }

  /**
   * Installs the process-wide {@link DynamicFeatureFlagRegistry} for code that cannot receive it
   * via dependency injection (the Lucene SPI/codec layer).
   *
   * <p><b>Lifecycle contract:</b> mongot's bootstrapper calls this exactly once, early in process
   * startup, before any code path that calls {@link #getStatic()} runs. Once this method returns,
   * every subsequent {@link #getStatic()} call is guaranteed to return the populated instance
   * supplied here for the remainder of the process's lifetime. It must never be called more than
   * once per process.
   *
   * @param dynamicFeatureFlagRegistry the instance to publish as the process-wide static
   *     configuration
   * @throws IllegalStateException if the process instance has already been initialized; mongot
   *     processes must never initialize the global configuration twice
   * @see #getStatic()
   */
  public static synchronized void initializeProcessInstance(
      DynamicFeatureFlagRegistry dynamicFeatureFlagRegistry) {
    if (globalDynamicFeatureFlagRegistry.isPresent()) {
      throw new IllegalStateException("global DynamicFeatureFlagRegistry already initialized");
    }
    globalDynamicFeatureFlagRegistry = Optional.of(dynamicFeatureFlagRegistry);
  }

  /**
   * Returns the globally configured {@link DynamicFeatureFlagRegistry} installed by {@link
   * #initializeProcessInstance}.
   *
   * <p><b>Contract:</b> once the bootstrapper has called {@link #initializeProcessInstance} during
   * startup, this method is guaranteed to return a non-null, populated instance for the rest of the
   * process's lifetime. It throws only when that precondition is unmet — i.e. it is called before
   * initialization (ordering bug) or after {@link #resetForTest()} (which happens only in tests).
   *
   * <p>Use only from code that cannot receive {@link DynamicFeatureFlagRegistry} via dependency
   * injection (the Lucene SPI/codec layer); prefer injection everywhere else. Tests relying on the
   * global instance can be order-sensitive because its scope is process-wide. Injection avoids this
   * by scoping the configuration to the individual test.
   *
   * @return the globally configured {@link DynamicFeatureFlagRegistry} instance
   * @throws IllegalStateException if called before {@link #initializeProcessInstance} or after
   *     {@link #resetForTest()}
   * @see #initializeProcessInstance(DynamicFeatureFlagRegistry)
   */
  @RestrictedApi(
      explanation =
          "DynamicFeatureFlagRegistry.getStatic() should only be used in the Lucene"
              + " codec/quantization/vector layer or in DynamicFeatureFlagRegistryTest.java."
              + " If you're in the Lucene SPI/codec layer where"
              + " injection is impossible add the path to allowedOnPath"
              + " in the @RestrictedApi annotation of DynamicFeatureFlagRegistry.getStatic().",
      link = "https://jira.mongodb.org/browse/CLOUDP-412425",
      allowedOnPath =
          ".*/index/lucene/(codec|quantization|vector)/.*|.*DynamicFeatureFlagRegistryTest\\.java")
  public static synchronized DynamicFeatureFlagRegistry getStatic() {
    return globalDynamicFeatureFlagRegistry.orElseThrow(
        () ->
            new IllegalStateException(
                "global DynamicFeatureFlagRegistry not initialized or has been reset"));
  }

  @RestrictedApi(
      explanation =
          "DynamicFeatureFlagRegistry.resetForTest() must only be called from test code -- it"
              + " clears the process-global registry and could silently change feature-flag"
              + " evaluation mid-process if called from production code paths.",
      link = "https://jira.mongodb.org/browse/CLOUDP-415881",
      allowedOnPath =
          ".*/index/lucene/vector/.*Test\\.java|.*DynamicFeatureFlagRegistryTest\\.java")
  @VisibleForTesting
  public static synchronized void resetForTest() {
    globalDynamicFeatureFlagRegistry = Optional.empty();
  }
}
