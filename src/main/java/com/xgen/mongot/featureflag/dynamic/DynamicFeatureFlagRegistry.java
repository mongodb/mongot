package com.xgen.mongot.featureflag.dynamic;

import static com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlagConfig.Phase;
import static com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlagConfig.Scope;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.flogger.FluentLogger;
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
                Check feature flag definitions.""");
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
      case ENABLED ->
          new CachedEvaluation(true, FeatureFlagEvaluationSpec.DecisiveField.PHASE);
      case DISABLED, UNSPECIFIED ->
          new CachedEvaluation(false, FeatureFlagEvaluationSpec.DecisiveField.PHASE);
      case CONTROLLED -> {
        if (config.entityIdBlockList().contains(entityId)) {
          yield new CachedEvaluation(
              false, FeatureFlagEvaluationSpec.DecisiveField.BLOCK_LIST);
        }
        if (config.entityIdAllowList().contains(entityId)) {
          yield new CachedEvaluation(
              true, FeatureFlagEvaluationSpec.DecisiveField.ALLOW_LIST);
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
   * cluster-invariant scopes (ORG, GROUP, MONGOT_CLUSTER). Non-cluster-invariant scopes get
   * {@code Optional.empty()} for the cached result.
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

    Optional<CachedEvaluation> cached =
        entityId.map(id -> computeEvaluation(base, id));

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
}
