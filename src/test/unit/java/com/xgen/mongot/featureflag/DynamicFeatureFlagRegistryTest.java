package com.xgen.mongot.featureflag;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.common.primitives.Ints;
import com.google.errorprone.annotations.Var;
import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlagConfig;
import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlagRegistry;
import com.xgen.testing.mongot.index.query.OperatorQueryBuilder;
import com.xgen.testing.mongot.index.query.operators.OperatorBuilder;
import java.nio.ByteBuffer;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.zip.CRC32;
import org.bson.types.ObjectId;
import org.junit.Test;

public class DynamicFeatureFlagRegistryTest {
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
            Optional.of(List.of(config)), Optional.empty(), Optional.empty(), Optional.of(cluster));

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
            Optional.of(List.of(configOrg)), Optional.empty(), Optional.empty(), Optional.empty());

    assertTrue(registryNoIds.evaluateClusterInvariant("org-feature-no-id", true));
    assertFalse(registryNoIds.evaluateClusterInvariant("org-feature-no-id", false));
  }
}
