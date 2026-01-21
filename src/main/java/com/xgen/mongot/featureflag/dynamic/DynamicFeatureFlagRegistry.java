package com.xgen.mongot.featureflag.dynamic;

import static com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlagConfig.Scope;

import com.google.common.flogger.FluentLogger;
import com.xgen.mongot.index.DynamicFeatureFlagsMetricsRecorder;
import com.xgen.mongot.index.lucene.explain.explainers.DynamicFeatureFlagFeatureExplainer;
import com.xgen.mongot.index.lucene.explain.information.FeatureFlagEvaluationSpec;
import com.xgen.mongot.index.lucene.explain.tracing.Explain;
import com.xgen.mongot.index.query.Query;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
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

  private final Optional<ObjectId> orgId;
  private final Optional<ObjectId> groupId;
  private final Optional<ObjectId> clusterId;

  private final boolean isInitializedByInitialConfCall;

  /**
   * Internal, optimized "domain object" for a feature flag. This is pre-computed for fast
   * evaluation.
   */
  private record InternalDynamicFeatureFlagConfig(
      int seed,
      Set<ObjectId> entityIdAllowList,
      Set<ObjectId> entityIdBlockList,
      int rolloutPercentage,
      Scope scope) {}

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
    this.isInitializedByInitialConfCall = dynamicFeatureFlagConfigs.isPresent();
    this.orgId = orgId;
    this.groupId = groupId;
    this.clusterId = clusterId;
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
    var dynamicFeatureFlagConfig = this.getFeatureFlag(featureFlagName);
    return dynamicFeatureFlagConfig
        .map(
            featureFlagConfig ->
                switch (featureFlagConfig.scope()) {
                  case Scope.ORG ->
                      this.orgId
                          .map(id -> evaluate(featureFlagName, id, fallback))
                          .orElse(fallback);
                  case Scope.GROUP ->
                      this.groupId
                          .map(id -> evaluate(featureFlagName, id, fallback))
                          .orElse(fallback);
                  case Scope.MONGOT_CLUSTER ->
                      this.clusterId
                          .map(id -> evaluate(featureFlagName, id, fallback))
                          .orElse(fallback);
                  case Scope.MONGOT_QUERY, Scope.MONGOT_INDEX, Scope.UNSPECIFIED -> {
                    FLOGGER.atWarning().atMostEvery(5, TimeUnit.MINUTES).log(
                        """
                            Evaluating Cluster variant DFFs with evaluateClusterInvariant. \
                            Check feature flag definitions.""");
                    yield fallback;
                  }
                })
        .orElse(fallback);
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
    return evaluateHelper(
        featureFlagName, new EvaluateDynamicFeatureFlagContext(entityId), fallback);
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
    return evaluateHelper(featureFlagName, new EvaluateDynamicFeatureFlagContext(query), fallback);
  }

  private boolean evaluateHelper(
      String featureFlagName,
      EvaluateDynamicFeatureFlagContext evaluationContext,
      boolean fallback) {
    Optional<InternalDynamicFeatureFlagConfig> maybeConfig =
        Optional.ofNullable(this.internalConfigs.get(featureFlagName));

    if (maybeConfig.isPresent()) {
      InternalDynamicFeatureFlagConfig config = maybeConfig.get();
      return handleControlledPhase(featureFlagName, config, evaluationContext);
    }

    FLOGGER.atWarning().atMostEvery(5, TimeUnit.MINUTES).log(
        "Feature flag not present in registry, falling back to provided value");
    recordEvaluation(featureFlagName, fallback, FeatureFlagEvaluationSpec.DecisiveField.FALLBACK);
    return fallback;
  }

  private boolean handleControlledPhase(
      String featureFlagName,
      InternalDynamicFeatureFlagConfig config,
      EvaluateDynamicFeatureFlagContext evaluationContext) {
    // We skip allowlist/blocklist check for MONGOT_QUERY since we don't know query ids ahead of
    // time and cannot allow/block by query ids
    boolean skipAllowBlockList = config.scope() == Scope.MONGOT_QUERY;

    if (!skipAllowBlockList) {
      if (config.entityIdBlockList().contains(evaluationContext.entityId())) {
        recordEvaluation(
            featureFlagName, false, FeatureFlagEvaluationSpec.DecisiveField.BLOCK_LIST);
        return false;
      }
      if (config.entityIdAllowList().contains(evaluationContext.entityId())) {
        recordEvaluation(featureFlagName, true, FeatureFlagEvaluationSpec.DecisiveField.ALLOW_LIST);
        return true;
      }
    }

    // If the rollout percentage is 0 or 100, the flag's decisive field is considered PHASE
    // because 'ENABLED' and 'DISABLED/UNSPECIFIED' phases are converted to these percentages
    // in convertToInternalConfig
    if (config.rolloutPercentage() == 0) {
      recordEvaluation(featureFlagName, false, FeatureFlagEvaluationSpec.DecisiveField.PHASE);
      return false;
    }
    if (config.rolloutPercentage() == 100) {
      recordEvaluation(featureFlagName, true, FeatureFlagEvaluationSpec.DecisiveField.PHASE);
      return true;
    }

    boolean result =
        isHashedIdWithinPercentage(
            config.seed(), evaluationContext.entityByteArray(), config.rolloutPercentage());

    recordEvaluation(
        featureFlagName, result, FeatureFlagEvaluationSpec.DecisiveField.ROLLOUT_PERCENTAGE);
    return result;
  }

  private boolean isHashedIdWithinPercentage(
      int seed, byte[] entityByteArray, int rolloutPercentage) {
    CRC32 hasher = new CRC32();
    for (int shift = 24; shift >= 0; shift -= 8) {
      hasher.update(seed >>> shift);
    }
    hasher.update(entityByteArray);
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
   * <p>This method, intended to be invoked by a poller, processes the incoming list of DTOs ({@link
   * DynamicFeatureFlagConfig}) and converts them into optimized, internal "domain objects" ({@link
   * InternalDynamicFeatureFlagConfig}).
   *
   * <p>The update logic performs the following steps:
   *
   * <ol>
   *   <li>Partitions the incoming DTOs into temporary collections based on their phase:
   *       <ul>
   *         <li>{@code ENABLED} flags are added to a "always enabled" set.
   *         <li>{@code DISABLED/UNSPECIFIED} flags are added to an "always disabled" set.
   *         <li>{@code CONTROLLED} flags are converted into optimized internal records.
   *         <li>All raw DTOs are collected to a "raw" map for snapshotting.
   *       </ul>
   *   <li>Clears and repopulates the live, thread-safe registry collections
   * </ol>
   *
   * @param dynamicFeatureFlagConfigs The complete list of (DTO) feature flag configurations to
   *     register.
   */
  public void updateDynamicFeatureFlags(List<DynamicFeatureFlagConfig> dynamicFeatureFlagConfigs) {
    Map<String, DynamicFeatureFlagConfig> newFlagsMap = new ConcurrentHashMap<>();
    Map<String, InternalDynamicFeatureFlagConfig> newInternaConfigsMap = new ConcurrentHashMap<>();

    for (DynamicFeatureFlagConfig dto : dynamicFeatureFlagConfigs) {
      String featureName = dto.featureFlagName();
      newFlagsMap.put(featureName, dto);

      InternalDynamicFeatureFlagConfig internalConfig = convertToInternalConfig(dto);
      newInternaConfigsMap.put(featureName, internalConfig);
    }

    this.registeredDynamicFeatureFlagConfigs = newFlagsMap;
    this.internalConfigs = newInternaConfigsMap;
  }

  private InternalDynamicFeatureFlagConfig convertToInternalConfig(DynamicFeatureFlagConfig dto) {
    return switch (dto.phase()) {
      case ENABLED -> new InternalDynamicFeatureFlagConfig(0, Set.of(), Set.of(), 100, dto.scope());
      case UNSPECIFIED, DISABLED ->
          new InternalDynamicFeatureFlagConfig(0, Set.of(), Set.of(), 0, dto.scope());
      case CONTROLLED -> {
        Set<ObjectId> allowList = new HashSet<>(dto.allowedList());
        Set<ObjectId> blockList = new HashSet<>(dto.blockedList());

        int seed = dto.featureFlagName().hashCode();

        yield new InternalDynamicFeatureFlagConfig(
            seed, allowList, blockList, dto.sanitizedRolloutPercentage(), dto.scope());
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
