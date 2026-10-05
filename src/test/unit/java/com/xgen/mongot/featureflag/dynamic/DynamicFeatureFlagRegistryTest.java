package com.xgen.mongot.featureflag.dynamic;

import static com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlags.BLOOM_FILTER_FOR_ID_FIELD;
import static com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlags.BLOOM_FILTER_IN_STEADY_STATE;
import static com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlags.DISABLE_BULK_SCORER_QUERY_FOR_EMBEDDED_DOCUMENT_CHILD;
import static com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlags.MV_GC_DELETION_ENABLED;
import static com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlags.MV_GC_ENABLED;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.google.common.primitives.Ints;
import com.google.errorprone.annotations.Var;
import com.xgen.mongot.metrics.MetricsFactory;
import com.xgen.testing.mongot.index.query.OperatorQueryBuilder;
import com.xgen.testing.mongot.index.query.operators.OperatorBuilder;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.nio.ByteBuffer;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.zip.CRC32;
import org.bson.types.ObjectId;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses(
    value = {
      DynamicFeatureFlagRegistryTest.RegistryTest.class,
      DynamicFeatureFlagRegistryTest.StaticTest.class,
    })
public class DynamicFeatureFlagRegistryTest {

  public static class RegistryTest {

    @Test
    public void registerAndGetFeatureFlag_validConfig_retrievesCorrectly() {
      DynamicFeatureFlagConfig config =
          new DynamicFeatureFlagConfig(
              "test-feature",
              DynamicFeatureFlagConfig.Phase.ENABLED,
              List.of(),
              List.of(),
              0,
              DynamicFeatureFlagConfig.Scope.ORG);

      var dynamicFeatureFlagRegistry =
          new DynamicFeatureFlagRegistry(
              Optional.of(List.of(config)), Optional.empty(), Optional.empty(), Optional.empty());

      DynamicFeatureFlagConfig retrievedConfig =
          dynamicFeatureFlagRegistry.getFeatureFlag("test-feature").get();
      assertEquals("test-feature", retrievedConfig.featureFlagName());
      assertEquals(DynamicFeatureFlagConfig.Phase.ENABLED, retrievedConfig.phase());
    }

    @Test
    public void testGetMissingConfig() {
      var dynamicFeatureFlagRegistry =
          new DynamicFeatureFlagRegistry(
              Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());
      assertTrue(dynamicFeatureFlagRegistry.getFeatureFlag("non-existent-feature").isEmpty());
    }

    @Test
    public void updateConfig_withNewFeatureFlags_updatesRegistry() {
      DynamicFeatureFlagConfig initialConfig =
          new DynamicFeatureFlagConfig(
              "test-feature",
              DynamicFeatureFlagConfig.Phase.DISABLED,
              List.of(),
              List.of(),
              0,
              DynamicFeatureFlagConfig.Scope.ORG);

      var dynamicFeatureFlagRegistry =
          new DynamicFeatureFlagRegistry(
              Optional.of(List.of(initialConfig)),
              Optional.empty(),
              Optional.empty(),
              Optional.empty());
      dynamicFeatureFlagRegistry.updateDynamicFeatureFlags(List.of(initialConfig));

      DynamicFeatureFlagConfig updatedConfig =
          new DynamicFeatureFlagConfig(
              "test-feature",
              DynamicFeatureFlagConfig.Phase.ENABLED,
              List.of(),
              List.of(),
              0,
              DynamicFeatureFlagConfig.Scope.UNSPECIFIED);

      dynamicFeatureFlagRegistry.updateDynamicFeatureFlags(List.of(updatedConfig));

      DynamicFeatureFlagConfig retrievedConfig =
          dynamicFeatureFlagRegistry.getFeatureFlag("test-feature").get();
      assertEquals(DynamicFeatureFlagConfig.Phase.ENABLED, retrievedConfig.phase());
    }

    @Test
    public void updateDynamicFeatureFlags_removesNonExistentFlags() {
      // Arrange: Register two initial feature flags
      DynamicFeatureFlagConfig configToKeep =
          new DynamicFeatureFlagConfig(
              "feature-to-keep",
              DynamicFeatureFlagConfig.Phase.ENABLED,
              List.of(),
              List.of(),
              50,
              DynamicFeatureFlagConfig.Scope.ORG);
      DynamicFeatureFlagConfig configToRemove =
          new DynamicFeatureFlagConfig(
              "feature-to-remove",
              DynamicFeatureFlagConfig.Phase.DISABLED,
              List.of(),
              List.of(),
              0,
              DynamicFeatureFlagConfig.Scope.ORG);

      var dynamicFeatureFlagRegistry =
          new DynamicFeatureFlagRegistry(
              Optional.of(List.of(configToKeep, configToRemove)),
              Optional.empty(),
              Optional.empty(),
              Optional.empty());

      assertTrue(dynamicFeatureFlagRegistry.getFeatureFlag("feature-to-keep").isPresent());
      assertTrue(dynamicFeatureFlagRegistry.getFeatureFlag("feature-to-remove").isPresent());

      DynamicFeatureFlagConfig updatedConfigToKeep =
          new DynamicFeatureFlagConfig(
              "feature-to-keep",
              DynamicFeatureFlagConfig.Phase.CONTROLLED,
              List.of(),
              List.of(),
              75,
              DynamicFeatureFlagConfig.Scope.ORG);
      dynamicFeatureFlagRegistry.updateDynamicFeatureFlags(List.of(updatedConfigToKeep));

      assertTrue(dynamicFeatureFlagRegistry.getFeatureFlag("feature-to-remove").isEmpty());
      DynamicFeatureFlagConfig retrievedConfig =
          dynamicFeatureFlagRegistry.getFeatureFlag("feature-to-keep").get();
      assertEquals(DynamicFeatureFlagConfig.Phase.CONTROLLED, retrievedConfig.phase());
      assertEquals(75, (int) retrievedConfig.rolloutPercentage());
    }

    @Test
    public void updateDynamicFeatureFlags_CorrectlySetIsInitializedFromInitialConfCall() {
      // Arrange: Register two initial feature flags
      DynamicFeatureFlagConfig configToKeep =
          new DynamicFeatureFlagConfig(
              "feature-to-keep",
              DynamicFeatureFlagConfig.Phase.ENABLED,
              List.of(),
              List.of(),
              50,
              DynamicFeatureFlagConfig.Scope.ORG);

      var dynamicFeatureFlagRegistry =
          new DynamicFeatureFlagRegistry(
              Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());
      dynamicFeatureFlagRegistry.updateDynamicFeatureFlags(List.of(configToKeep));

      assertTrue(dynamicFeatureFlagRegistry.getFeatureFlag("feature-to-keep").isPresent());
      assertFalse(dynamicFeatureFlagRegistry.isInitializedByInitialConfCall());
    }

    @Test
    public void evaluate_enabledPhase_returnsTrue() {
      DynamicFeatureFlagConfig config =
          new DynamicFeatureFlagConfig(
              "test-feature",
              DynamicFeatureFlagConfig.Phase.ENABLED,
              List.of(),
              List.of(),
              0,
              DynamicFeatureFlagConfig.Scope.ORG);

      var featureFlagRegistry =
          new DynamicFeatureFlagRegistry(
              Optional.of(List.of(config)), Optional.empty(), Optional.empty(), Optional.empty());

      assertTrue(featureFlagRegistry.evaluate("test-feature", new ObjectId(), false));
    }

    @Test
    public void evaluate_disabledPhase_returnsFalse() {
      DynamicFeatureFlagConfig config =
          new DynamicFeatureFlagConfig(
              "test-feature",
              DynamicFeatureFlagConfig.Phase.DISABLED,
              List.of(),
              List.of(),
              100,
              DynamicFeatureFlagConfig.Scope.ORG);

      var featureFlagRegistry =
          new DynamicFeatureFlagRegistry(
              Optional.of(List.of(config)), Optional.empty(), Optional.empty(), Optional.empty());

      assertFalse(featureFlagRegistry.evaluate("test-feature", new ObjectId(), true));
    }

    @Test
    public void evaluate_phaseControlled_query_rolloutPercentage() {
      // Get expected hash for query
      String featureFlagName = "test-feature";
      var query =
          OperatorQueryBuilder.builder()
              .operator(OperatorBuilder.text().path("title").query("godfather").build())
              .returnStoredSource(false)
              .build();
      byte[] queryByteArr = ByteBuffer.allocate(4).putInt(System.identityHashCode(query)).array();
      int seed = featureFlagName.hashCode();
      CRC32 hasher = new CRC32();
      hasher.update(Ints.toByteArray(seed));
      hasher.update(queryByteArr);
      long expectedHashedValue = hasher.getValue();

      // calculate rollout percentage to pass evaluation
      int testPassRolloutPercentage = (int) ((expectedHashedValue % 100) + 1);

      DynamicFeatureFlagConfig config1 =
          new DynamicFeatureFlagConfig(
              featureFlagName,
              DynamicFeatureFlagConfig.Phase.CONTROLLED,
              List.of(),
              List.of(),
              testPassRolloutPercentage,
              DynamicFeatureFlagConfig.Scope.MONGOT_QUERY);

      var featureFlagRegistry =
          new DynamicFeatureFlagRegistry(
              Optional.of(List.of(config1)), Optional.empty(), Optional.empty(), Optional.empty());

      assertTrue(featureFlagRegistry.evaluate(featureFlagName, query, false));

      // calculate rollout percentage to fail evaluation
      int testFailRolloutPercentage = (int) ((expectedHashedValue % 100) - 1);
      DynamicFeatureFlagConfig config2 =
          new DynamicFeatureFlagConfig(
              featureFlagName,
              DynamicFeatureFlagConfig.Phase.CONTROLLED,
              List.of(),
              List.of(),
              testFailRolloutPercentage,
              DynamicFeatureFlagConfig.Scope.MONGOT_QUERY);
      featureFlagRegistry.updateDynamicFeatureFlags(List.of(config2));

      assertFalse(featureFlagRegistry.evaluate(featureFlagName, query, true));
    }

    @Test
    public void evaluate_phaseControlledWithAllowlist_returnsTrue() {
      ObjectId allowedId = new ObjectId();
      DynamicFeatureFlagConfig config =
          new DynamicFeatureFlagConfig(
              "test-feature",
              DynamicFeatureFlagConfig.Phase.CONTROLLED,
              List.of(allowedId),
              List.of(),
              0,
              DynamicFeatureFlagConfig.Scope.ORG);

      var featureFlagRegistry =
          new DynamicFeatureFlagRegistry(
              Optional.of(List.of(config)), Optional.empty(), Optional.empty(), Optional.empty());

      assertTrue(featureFlagRegistry.evaluate("test-feature", allowedId, false));
    }

    @Test
    public void evaluate_controlledPhaseWithBlocklist_returnsFalse() {
      ObjectId blockedId = new ObjectId();
      DynamicFeatureFlagConfig config =
          new DynamicFeatureFlagConfig(
              "test-feature",
              DynamicFeatureFlagConfig.Phase.CONTROLLED,
              List.of(),
              List.of(blockedId),
              0,
              DynamicFeatureFlagConfig.Scope.ORG);

      var featureFlagRegistry =
          new DynamicFeatureFlagRegistry(
              Optional.of(List.of(config)), Optional.empty(), Optional.empty(), Optional.empty());

      assertFalse(featureFlagRegistry.evaluate("test-feature", blockedId, true));
    }

    @Test
    public void evaluate_controlledPhaseWithRollout_respectsPercentage() {
      int rolloutPercentage = 20;
      int sampleSize = 1000;
      // Using the deterministic set generated below, we expect 196/1000 to pass
      int expectedPasses = 196;

      DynamicFeatureFlagConfig config =
          new DynamicFeatureFlagConfig(
              "test-feature",
              DynamicFeatureFlagConfig.Phase.CONTROLLED,
              List.of(),
              List.of(),
              rolloutPercentage,
              DynamicFeatureFlagConfig.Scope.ORG);

      var featureFlagRegistry =
          new DynamicFeatureFlagRegistry(
              Optional.of(List.of(config)), Optional.empty(), Optional.empty(), Optional.empty());

      @Var int actualPasses = 0;

      for (int i = 0; i < sampleSize; i++) {
        // Generate unique IDs for statistical independence
        ObjectId entityId = ObjectId.getSmallestWithDate(new Date(i * 1000));
        if (featureFlagRegistry.evaluate("test-feature", entityId, false)) {
          actualPasses++;
        }
      }

      assertEquals(expectedPasses, actualPasses);
    }

    @Test
    public void evaluate_fallback() {
      var featureFlagRegistry =
          new DynamicFeatureFlagRegistry(
              Optional.of(List.of()), Optional.empty(), Optional.empty(), Optional.empty());

      assertFalse(featureFlagRegistry.evaluate("non-existant-feature", new ObjectId(), false));
    }

    @Test
    public void evaluateClusterInvariant_orgScope_usesOrgId() {
      ObjectId org = new ObjectId();
      DynamicFeatureFlagConfig config =
          new DynamicFeatureFlagConfig(
              "org-feature",
              DynamicFeatureFlagConfig.Phase.CONTROLLED,
              List.of(org),
              List.of(),
              0,
              DynamicFeatureFlagConfig.Scope.ORG);

      var registry =
          new DynamicFeatureFlagRegistry(
              Optional.of(List.of(config)), Optional.of(org), Optional.empty(), Optional.empty());

      assertTrue(registry.evaluateClusterInvariant("org-feature", false));
    }

    @Test
    public void evaluateClusterInvariant_groupScope_usesGroupId() {
      ObjectId group = new ObjectId();
      DynamicFeatureFlagConfig config =
          new DynamicFeatureFlagConfig(
              "group-feature",
              DynamicFeatureFlagConfig.Phase.CONTROLLED,
              List.of(group),
              List.of(),
              0,
              DynamicFeatureFlagConfig.Scope.GROUP);

      var registry =
          new DynamicFeatureFlagRegistry(
              Optional.of(List.of(config)), Optional.empty(), Optional.of(group), Optional.empty());

      assertTrue(registry.evaluateClusterInvariant("group-feature", false));
    }

    @Test
    public void evaluateClusterInvariant_clusterScope_usesClusterId() {
      ObjectId cluster = new ObjectId();
      DynamicFeatureFlagConfig config =
          new DynamicFeatureFlagConfig(
              "cluster-feature",
              DynamicFeatureFlagConfig.Phase.CONTROLLED,
              List.of(cluster),
              List.of(),
              0,
              DynamicFeatureFlagConfig.Scope.MONGOT_CLUSTER);

      var registry =
          new DynamicFeatureFlagRegistry(
              Optional.of(List.of(config)),
              Optional.empty(),
              Optional.empty(),
              Optional.of(cluster));

      assertTrue(registry.evaluateClusterInvariant("cluster-feature", false));
    }

    @Test
    public void evaluateClusterInvariant_missingConfig_returnsFallback() {
      var registry =
          new DynamicFeatureFlagRegistry(
              Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());

      assertFalse(registry.evaluateClusterInvariant("missing-feature", false));
      assertTrue(registry.evaluateClusterInvariant("missing-feature", true));
    }

    @Test
    public void evaluateClusterInvariant_unsupportedScope_returnsFallback() {
      DynamicFeatureFlagConfig configIndexScope =
          new DynamicFeatureFlagConfig(
              "index-scope-feature",
              DynamicFeatureFlagConfig.Phase.ENABLED,
              List.of(),
              List.of(),
              0,
              DynamicFeatureFlagConfig.Scope.MONGOT_INDEX);

      var registry =
          new DynamicFeatureFlagRegistry(
              Optional.of(List.of(configIndexScope)),
              Optional.empty(),
              Optional.empty(),
              Optional.empty());

      assertTrue(registry.evaluateClusterInvariant("index-scope-feature", true));
      assertFalse(registry.evaluateClusterInvariant("index-scope-feature", false));
    }

    @Test
    public void isHashedIdWithinPercentage_byteArrayAndIntOverloads_equivalentForSameValue() {
      var registry =
          new DynamicFeatureFlagRegistry(
              Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());

      int seed = 42;
      int entityHash = 0x1234_5678;
      byte[] entityBytes = ByteBuffer.allocate(4).putInt(entityHash).array();

      for (int rolloutPercentage = 0; rolloutPercentage <= 100; rolloutPercentage++) {
        boolean resultByteArray =
            registry.isHashedIdWithinPercentage(seed, entityBytes, rolloutPercentage);
        boolean resultInt =
            registry.isHashedIdWithinPercentage(seed, entityHash, rolloutPercentage);
        assertEquals(
            "Seed=%d entityHash=%d rollout=%d".formatted(seed, entityHash, rolloutPercentage),
            resultByteArray,
            resultInt);
      }
    }

    @Test
    public void evaluateClusterInvariant_idMissing_returnsFallback() {
      DynamicFeatureFlagConfig configOrg =
          new DynamicFeatureFlagConfig(
              "org-feature-no-id",
              DynamicFeatureFlagConfig.Phase.ENABLED,
              List.of(),
              List.of(),
              0,
              DynamicFeatureFlagConfig.Scope.ORG);

      var registryNoIds =
          new DynamicFeatureFlagRegistry(
              Optional.of(List.of(configOrg)),
              Optional.empty(),
              Optional.empty(),
              Optional.empty());

      assertTrue(registryNoIds.evaluateClusterInvariant("org-feature-no-id", true));
      assertFalse(registryNoIds.evaluateClusterInvariant("org-feature-no-id", false));
    }

    @Test
    public void cache_and_gauge_invalidation_onConfigUpdate() {
      MeterRegistry meterRegistry = new SimpleMeterRegistry();
      MetricsFactory metricsFactory = new MetricsFactory("dynamicFeature", meterRegistry);

      ObjectId org = new ObjectId();
      DynamicFeatureFlagConfig enabledConfig =
          new DynamicFeatureFlagConfig(
              "flip-flag",
              DynamicFeatureFlagConfig.Phase.ENABLED,
              List.of(),
              List.of(),
              0,
              DynamicFeatureFlagConfig.Scope.ORG);

      var registry =
          new DynamicFeatureFlagRegistry(
              Optional.of(List.of(enabledConfig)),
              Optional.of(org),
              Optional.empty(),
              Optional.empty(),
              Optional.of(metricsFactory));

      assertTrue(registry.evaluateClusterInvariant("flip-flag", false));
      assertEquals(
          1.0,
          metricsFactory
              .get(
                  "clusterInvariantStatus", Tags.of("featureFlagName", "flip-flag", "scope", "ORG"))
              .gauge()
              .value(),
          0.0);

      DynamicFeatureFlagConfig disabledConfig =
          new DynamicFeatureFlagConfig(
              "flip-flag",
              DynamicFeatureFlagConfig.Phase.DISABLED,
              List.of(),
              List.of(),
              0,
              DynamicFeatureFlagConfig.Scope.ORG);
      registry.updateDynamicFeatureFlags(List.of(disabledConfig));

      assertFalse(registry.evaluateClusterInvariant("flip-flag", true));
      assertEquals(
          0.0,
          metricsFactory
              .get(
                  "clusterInvariantStatus", Tags.of("featureFlagName", "flip-flag", "scope", "ORG"))
              .gauge()
              .value(),
          0.0);
    }

    @Test
    public void cache_removedFlag_returnsFallback() {
      ObjectId org = new ObjectId();
      DynamicFeatureFlagConfig config =
          new DynamicFeatureFlagConfig(
              "transient-flag",
              DynamicFeatureFlagConfig.Phase.ENABLED,
              List.of(),
              List.of(),
              0,
              DynamicFeatureFlagConfig.Scope.ORG);

      var registry =
          new DynamicFeatureFlagRegistry(
              Optional.of(List.of(config)), Optional.of(org), Optional.empty(), Optional.empty());

      assertTrue(registry.evaluateClusterInvariant("transient-flag", false));

      registry.updateDynamicFeatureFlags(List.of());

      assertFalse(registry.evaluateClusterInvariant("transient-flag", false));
      assertTrue(registry.evaluateClusterInvariant("transient-flag", true));
    }

    @Test
    public void gaugeEmission_allClusterInvariantScopes_correctValuesAndTags() {
      MeterRegistry meterRegistry = new SimpleMeterRegistry();
      MetricsFactory metricsFactory = new MetricsFactory("dynamicFeature", meterRegistry);

      ObjectId org = new ObjectId();
      ObjectId group = new ObjectId();
      ObjectId cluster = new ObjectId();

      DynamicFeatureFlagConfig orgEnabled =
          new DynamicFeatureFlagConfig(
              "org-flag",
              DynamicFeatureFlagConfig.Phase.ENABLED,
              List.of(),
              List.of(),
              0,
              DynamicFeatureFlagConfig.Scope.ORG);

      DynamicFeatureFlagConfig groupDisabled =
          new DynamicFeatureFlagConfig(
              "group-flag",
              DynamicFeatureFlagConfig.Phase.DISABLED,
              List.of(),
              List.of(),
              0,
              DynamicFeatureFlagConfig.Scope.GROUP);

      DynamicFeatureFlagConfig clusterEnabled =
          new DynamicFeatureFlagConfig(
              "cluster-flag",
              DynamicFeatureFlagConfig.Phase.ENABLED,
              List.of(),
              List.of(),
              0,
              DynamicFeatureFlagConfig.Scope.MONGOT_CLUSTER);

      new DynamicFeatureFlagRegistry(
          Optional.of(List.of(orgEnabled, groupDisabled, clusterEnabled)),
          Optional.of(org),
          Optional.of(group),
          Optional.of(cluster),
          Optional.of(metricsFactory));

      assertEquals(
          1.0,
          metricsFactory
              .get("clusterInvariantStatus", Tags.of("featureFlagName", "org-flag", "scope", "ORG"))
              .gauge()
              .value(),
          0.0);

      assertEquals(
          0.0,
          metricsFactory
              .get(
                  "clusterInvariantStatus",
                  Tags.of("featureFlagName", "group-flag", "scope", "GROUP"))
              .gauge()
              .value(),
          0.0);

      assertEquals(
          1.0,
          metricsFactory
              .get(
                  "clusterInvariantStatus",
                  Tags.of("featureFlagName", "cluster-flag", "scope", "MONGOT_CLUSTER"))
              .gauge()
              .value(),
          0.0);
    }

    @Test
    public void noGaugeEmission_forNonClusterInvariantScopes() {
      MeterRegistry meterRegistry = new SimpleMeterRegistry();
      MetricsFactory metricsFactory = new MetricsFactory("dynamicFeature", meterRegistry);

      DynamicFeatureFlagConfig queryFlag =
          new DynamicFeatureFlagConfig(
              "query-flag",
              DynamicFeatureFlagConfig.Phase.ENABLED,
              List.of(),
              List.of(),
              0,
              DynamicFeatureFlagConfig.Scope.MONGOT_QUERY);

      DynamicFeatureFlagConfig indexFlag =
          new DynamicFeatureFlagConfig(
              "index-flag",
              DynamicFeatureFlagConfig.Phase.ENABLED,
              List.of(),
              List.of(),
              0,
              DynamicFeatureFlagConfig.Scope.MONGOT_INDEX);

      new DynamicFeatureFlagRegistry(
          Optional.of(List.of(queryFlag, indexFlag)),
          Optional.empty(),
          Optional.empty(),
          Optional.empty(),
          Optional.of(metricsFactory));

      assertNull(meterRegistry.find("dynamicFeature.clusterInvariantStatus").gauge());
    }

    @Test
    public void noGaugeEmission_whenMetricsFactoryAbsent() {
      ObjectId org = new ObjectId();
      DynamicFeatureFlagConfig config =
          new DynamicFeatureFlagConfig(
              "flag-no-metrics",
              DynamicFeatureFlagConfig.Phase.ENABLED,
              List.of(),
              List.of(),
              0,
              DynamicFeatureFlagConfig.Scope.ORG);

      var registry =
          new DynamicFeatureFlagRegistry(
              Optional.of(List.of(config)), Optional.of(org), Optional.empty(), Optional.empty());

      assertTrue(registry.evaluateClusterInvariant("flag-no-metrics", false));
    }

    @Test
    public void evaluate_entityId_andClusterInvariant_behavioralEquivalence() {
      ObjectId org = new ObjectId();

      DynamicFeatureFlagConfig allowConfig =
          new DynamicFeatureFlagConfig(
              "equiv-allow",
              DynamicFeatureFlagConfig.Phase.CONTROLLED,
              List.of(org),
              List.of(),
              0,
              DynamicFeatureFlagConfig.Scope.ORG);

      var registry =
          new DynamicFeatureFlagRegistry(
              Optional.of(List.of(allowConfig)),
              Optional.of(org),
              Optional.empty(),
              Optional.empty());

      boolean cachedResult = registry.evaluateClusterInvariant("equiv-allow", false);
      boolean directResult = registry.evaluate("equiv-allow", org, false);
      assertEquals(cachedResult, directResult);
    }

    @Test
    public void evaluate_entityId_andClusterInvariant_behavioralEquivalence_blockList() {
      ObjectId org = new ObjectId();

      DynamicFeatureFlagConfig blockConfig =
          new DynamicFeatureFlagConfig(
              "equiv-block",
              DynamicFeatureFlagConfig.Phase.CONTROLLED,
              List.of(),
              List.of(org),
              100,
              DynamicFeatureFlagConfig.Scope.ORG);

      var registry =
          new DynamicFeatureFlagRegistry(
              Optional.of(List.of(blockConfig)),
              Optional.of(org),
              Optional.empty(),
              Optional.empty());

      boolean cachedResult = registry.evaluateClusterInvariant("equiv-block", true);
      boolean directResult = registry.evaluate("equiv-block", org, true);
      assertEquals(cachedResult, directResult);
    }

    @Test
    public void gaugeEmission_controlledPhaseWithRollout_reflectsEvaluation() {
      MeterRegistry meterRegistry = new SimpleMeterRegistry();
      MetricsFactory metricsFactory = new MetricsFactory("dynamicFeature", meterRegistry);

      ObjectId cluster = new ObjectId();
      DynamicFeatureFlagConfig config =
          new DynamicFeatureFlagConfig(
              "rollout-flag",
              DynamicFeatureFlagConfig.Phase.CONTROLLED,
              List.of(),
              List.of(),
              100,
              DynamicFeatureFlagConfig.Scope.MONGOT_CLUSTER);

      new DynamicFeatureFlagRegistry(
          Optional.of(List.of(config)),
          Optional.empty(),
          Optional.empty(),
          Optional.of(cluster),
          Optional.of(metricsFactory));

      assertEquals(
          1.0,
          metricsFactory
              .get(
                  "clusterInvariantStatus",
                  Tags.of("featureFlagName", "rollout-flag", "scope", "MONGOT_CLUSTER"))
              .gauge()
              .value(),
          0.0);
    }

    @Test
    public void communityDefault_bloomAndMvGcEnabled_otherFlagsAtFallback() {
      var registry = DynamicFeatureFlagRegistry.communityDefault();

      assertTrue(
          "communityDefault should enable BLOOM_FILTER_FOR_ID_FIELD",
          registry.evaluateClusterInvariant(BLOOM_FILTER_FOR_ID_FIELD));

      assertTrue(
          "communityDefault should enable MV_GC_ENABLED so GC runs on community / on-prem",
          registry.evaluateClusterInvariant(MV_GC_ENABLED));

      assertTrue(
          "communityDefault should enable MV_GC_DELETION_ENABLED so GC actually cleans up"
              + " superseded MVs on community / on-prem",
          registry.evaluateClusterInvariant(MV_GC_DELETION_ENABLED));

      assertFalse(
          "communityDefault should leave BLOOM_FILTER_IN_STEADY_STATE at its false fallback",
          registry.evaluateClusterInvariant(BLOOM_FILTER_IN_STEADY_STATE));

      assertTrue(
          "communityDefault should preserve the true fallback of"
              + " DISABLE_BULK_SCORER_QUERY_FOR_EMBEDDED_DOCUMENT_CHILD",
          registry.evaluateClusterInvariant(DISABLE_BULK_SCORER_QUERY_FOR_EMBEDDED_DOCUMENT_CHILD));
    }

    @Test
    public void enterpriseDefault_bloomAndMvGcEnabled_otherFlagsAtFallback() {
      var registry = DynamicFeatureFlagRegistry.enterpriseDefault();

      assertTrue(
          "enterpriseDefault should enable BLOOM_FILTER_FOR_ID_FIELD",
          registry.evaluateClusterInvariant(BLOOM_FILTER_FOR_ID_FIELD));

      assertTrue(
          "enterpriseDefault should enable MV_GC_ENABLED so GC runs on enterprise / on-prem",
          registry.evaluateClusterInvariant(MV_GC_ENABLED));

      assertTrue(
          "enterpriseDefault should enable MV_GC_DELETION_ENABLED so GC actually cleans up"
              + " superseded MVs on enterprise / on-prem",
          registry.evaluateClusterInvariant(MV_GC_DELETION_ENABLED));

      assertFalse(
          "enterpriseDefault should leave BLOOM_FILTER_IN_STEADY_STATE at its false fallback",
          registry.evaluateClusterInvariant(BLOOM_FILTER_IN_STEADY_STATE));

      assertTrue(
          "enterpriseDefault should preserve the true fallback of"
              + " DISABLE_BULK_SCORER_QUERY_FOR_EMBEDDED_DOCUMENT_CHILD",
          registry.evaluateClusterInvariant(DISABLE_BULK_SCORER_QUERY_FOR_EMBEDDED_DOCUMENT_CHILD));
    }

    @Test
    public void gaugeEmission_removedFlag_zeroedOut() {
      MeterRegistry meterRegistry = new SimpleMeterRegistry();
      MetricsFactory metricsFactory = new MetricsFactory("dynamicFeature", meterRegistry);

      ObjectId org = new ObjectId();
      DynamicFeatureFlagConfig flagA =
          new DynamicFeatureFlagConfig(
              "flag-a",
              DynamicFeatureFlagConfig.Phase.ENABLED,
              List.of(),
              List.of(),
              0,
              DynamicFeatureFlagConfig.Scope.ORG);

      DynamicFeatureFlagConfig flagB =
          new DynamicFeatureFlagConfig(
              "flag-b",
              DynamicFeatureFlagConfig.Phase.ENABLED,
              List.of(),
              List.of(),
              0,
              DynamicFeatureFlagConfig.Scope.ORG);

      var registry =
          new DynamicFeatureFlagRegistry(
              Optional.of(List.of(flagA, flagB)),
              Optional.of(org),
              Optional.empty(),
              Optional.empty(),
              Optional.of(metricsFactory));

      assertEquals(
          1.0,
          metricsFactory
              .get("clusterInvariantStatus", Tags.of("featureFlagName", "flag-a", "scope", "ORG"))
              .gauge()
              .value(),
          0.0);
      assertEquals(
          1.0,
          metricsFactory
              .get("clusterInvariantStatus", Tags.of("featureFlagName", "flag-b", "scope", "ORG"))
              .gauge()
              .value(),
          0.0);

      registry.updateDynamicFeatureFlags(List.of(flagA));

      assertEquals(
          1.0,
          metricsFactory
              .get("clusterInvariantStatus", Tags.of("featureFlagName", "flag-a", "scope", "ORG"))
              .gauge()
              .value(),
          0.0);

      assertEquals(
          0.0,
          meterRegistry
              .find("dynamicFeature.clusterInvariantStatus")
              .tag("featureFlagName", "flag-b")
              .gauge()
              .value(),
          0.0);
    }
  }

  public static class StaticTest {
    private DynamicFeatureFlagRegistry registry;

    @Before
    public void setUp() {
      this.registry = DynamicFeatureFlagRegistry.empty();
    }

    @After
    public void tearDown() {
      DynamicFeatureFlagRegistry.resetForTest();
    }

    @Test
    public void initializeProcessInstance_called_instanceNotNull() {
      DynamicFeatureFlagRegistry.initializeProcessInstance(this.registry);
      assertNotNull(DynamicFeatureFlagRegistry.getStatic());
    }

    @Test
    public void getStatic_noDependencyInjection_enabledPhase_returnsTrue() {
      DynamicFeatureFlagConfig config =
          new DynamicFeatureFlagConfig(
              "test-feature",
              DynamicFeatureFlagConfig.Phase.ENABLED,
              List.of(),
              List.of(),
              0,
              DynamicFeatureFlagConfig.Scope.ORG);

      var featureFlagRegistry =
          new DynamicFeatureFlagRegistry(
              Optional.of(List.of(config)), Optional.empty(), Optional.empty(), Optional.empty());
      DynamicFeatureFlagRegistry.initializeProcessInstance(featureFlagRegistry);

      assertTrue(
          DynamicFeatureFlagRegistry.getStatic().evaluate("test-feature", new ObjectId(), false));
    }

    @Test
    public void getStatic_noDependencyInjection_disabledPhase_returnsFalse() {
      DynamicFeatureFlagConfig config =
          new DynamicFeatureFlagConfig(
              "test-feature",
              DynamicFeatureFlagConfig.Phase.DISABLED,
              List.of(),
              List.of(),
              100,
              DynamicFeatureFlagConfig.Scope.ORG);

      var featureFlagRegistry =
          new DynamicFeatureFlagRegistry(
              Optional.of(List.of(config)), Optional.empty(), Optional.empty(), Optional.empty());
      DynamicFeatureFlagRegistry.initializeProcessInstance(featureFlagRegistry);

      assertFalse(
          DynamicFeatureFlagRegistry.getStatic().evaluate("test-feature", new ObjectId(), true));
    }

    @Test
    public void getStatic_beforeInit_throwsIllegalStateException() {
      assertThrows(IllegalStateException.class, DynamicFeatureFlagRegistry::getStatic);
    }

    @Test
    public void initializeProcessInstance_calledTwice_throwsIllegalStateException() {
      DynamicFeatureFlagRegistry.initializeProcessInstance(this.registry);
      assertThrows(
          IllegalStateException.class,
          () -> DynamicFeatureFlagRegistry.initializeProcessInstance(this.registry));
    }

    @Test
    public void getStatic_afterReset_throwsIllegalStateException() {
      DynamicFeatureFlagRegistry.initializeProcessInstance(this.registry);
      DynamicFeatureFlagRegistry.resetForTest();
      assertThrows(IllegalStateException.class, DynamicFeatureFlagRegistry::getStatic);
    }

    @Test
    public void getStatic_succeeds_afterReset() {
      DynamicFeatureFlagRegistry.initializeProcessInstance(this.registry);
      DynamicFeatureFlagRegistry.resetForTest();
      DynamicFeatureFlagRegistry.initializeProcessInstance(this.registry);
      assertSame(this.registry, DynamicFeatureFlagRegistry.getStatic());
    }

    @Test
    public void autoEmbeddingChangeStreamFullUpdate_hasExpectedNameAndFallback() {
      assertEquals(
          "mongot.featureFlag.autoEmbeddingChangeStreamFullUpdate",
          DynamicFeatureFlags.AUTO_EMBEDDING_CHANGE_STREAM_FULL_UPDATE.getName());
      assertFalse(DynamicFeatureFlags.AUTO_EMBEDDING_CHANGE_STREAM_FULL_UPDATE.getFallback());
    }

    @Test
    public void leaseHeartbeatPartialUpdate_hasExpectedNameAndFallback() {
      // The name is the Atlas control-plane contract: it is what a per-group rollout targets, so
      // renaming it silently would strand the rollout at its fallback. Off by default because the
      // gated renewal is only safe once every acquirer runs the expiration-gated takeover filter.
      assertEquals(
          "mongot.featureFlag.enableLeaseHeartbeatPartialUpdate",
          DynamicFeatureFlags.LEASE_HEARTBEAT_PARTIAL_UPDATE.getName());
      assertFalse(DynamicFeatureFlags.LEASE_HEARTBEAT_PARTIAL_UPDATE.getFallback());
    }

    @Test
    public void updateDynamicFeatureFlags_updatesGlobalObject() {
      DynamicFeatureFlagConfig testFeature =
          new DynamicFeatureFlagConfig(
              "test-feature",
              DynamicFeatureFlagConfig.Phase.ENABLED,
              List.of(),
              List.of(),
              50,
              DynamicFeatureFlagConfig.Scope.ORG);

      DynamicFeatureFlagRegistry.initializeProcessInstance(this.registry);
      DynamicFeatureFlagRegistry.getStatic().updateDynamicFeatureFlags(List.of(testFeature));
      Optional<DynamicFeatureFlagConfig> result =
          DynamicFeatureFlagRegistry.getStatic().getFeatureFlag("test-feature");
      assertTrue(result.isPresent());
      assertEquals(testFeature, result.get());
    }
  }
}
