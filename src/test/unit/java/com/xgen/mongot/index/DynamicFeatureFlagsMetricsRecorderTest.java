package com.xgen.mongot.index;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlagConfig;
import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlagRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import java.util.List;
import java.util.Optional;
import org.bson.types.ObjectId;
import org.junit.Test;

/**
 * Tests for {@link DynamicFeatureFlagsMetricsRecorder} to verify that feature flags evaluated
 * during a request are correctly captured for metrics purposes.
 */
public class DynamicFeatureFlagsMetricsRecorderTest {

  @Test
  public void testRecordEvaluation_singleFlag_capturesCorrectly() {
    try (var scope = DynamicFeatureFlagsMetricsRecorder.setup()) {
      DynamicFeatureFlagsMetricsRecorder.recordEvaluation("test-flag", true);

      List<Tags> tagsPerFlag = DynamicFeatureFlagsMetricsRecorder.getTagsPerFlag();

      assertEquals(1, tagsPerFlag.size());
      Tags tags = tagsPerFlag.getFirst();
      List<Tag> tagList = tags.stream().toList();

      assertEquals(2, tagList.size());
      assertThat(tagList)
          .contains(Tag.of(DynamicFeatureFlagsMetricsRecorder.FEATURE_FLAG_NAME_TAG, "test-flag"));
      assertThat(tagList)
          .contains(Tag.of(DynamicFeatureFlagsMetricsRecorder.EVALUATION_RESULT_TAG, "true"));
    }
  }

  @Test
  public void testRecordEvaluation_multipleFlags_capturesAll() {
    try (var scope = DynamicFeatureFlagsMetricsRecorder.setup()) {
      DynamicFeatureFlagsMetricsRecorder.recordEvaluation("flag-1", true);
      DynamicFeatureFlagsMetricsRecorder.recordEvaluation("flag-2", false);
      DynamicFeatureFlagsMetricsRecorder.recordEvaluation("flag-3", true);

      List<Tags> tagsPerFlag = DynamicFeatureFlagsMetricsRecorder.getTagsPerFlag();

      assertEquals(3, tagsPerFlag.size());

      // Verify each flag is present with correct tags
      assertThat(tagsPerFlag)
          .contains(
              Tags.of(
                  DynamicFeatureFlagsMetricsRecorder.FEATURE_FLAG_NAME_TAG,
                  "flag-1",
                  DynamicFeatureFlagsMetricsRecorder.EVALUATION_RESULT_TAG,
                  "true"));
      assertThat(tagsPerFlag)
          .contains(
              Tags.of(
                  DynamicFeatureFlagsMetricsRecorder.FEATURE_FLAG_NAME_TAG,
                  "flag-2",
                  DynamicFeatureFlagsMetricsRecorder.EVALUATION_RESULT_TAG,
                  "false"));
      assertThat(tagsPerFlag)
          .contains(
              Tags.of(
                  DynamicFeatureFlagsMetricsRecorder.FEATURE_FLAG_NAME_TAG,
                  "flag-3",
                  DynamicFeatureFlagsMetricsRecorder.EVALUATION_RESULT_TAG,
                  "true"));
    }
  }

  @Test
  public void testScopeCleanup_automaticallyRemovesFlags() {
    // Create a scope and record some flags
    try (var scope = DynamicFeatureFlagsMetricsRecorder.setup()) {
      DynamicFeatureFlagsMetricsRecorder.recordEvaluation("flag-1", true);
      DynamicFeatureFlagsMetricsRecorder.recordEvaluation("flag-2", false);

      // Verify flags are recorded within scope
      List<Tags> tagsInScope = DynamicFeatureFlagsMetricsRecorder.getTagsPerFlag();
      assertEquals(2, tagsInScope.size());
    }

    // Outside the scope, getTagsPerFlag should return empty (no context)
    List<Tags> tagsOutsideScope = DynamicFeatureFlagsMetricsRecorder.getTagsPerFlag();
    assertEquals(0, tagsOutsideScope.size());
  }

  @Test
  public void testGetTagsPerFlag_emptyWhenNoScope() {
    // Without a scope, getTagsPerFlag should return empty
    List<Tags> tagsPerFlag = DynamicFeatureFlagsMetricsRecorder.getTagsPerFlag();

    assertEquals(0, tagsPerFlag.size());
  }

  @Test
  public void testGetTagsPerFlag_emptyWhenNoFlagsRecordedInScope() {
    try (var scope = DynamicFeatureFlagsMetricsRecorder.setup()) {
      List<Tags> tagsPerFlag = DynamicFeatureFlagsMetricsRecorder.getTagsPerFlag();

      assertEquals(0, tagsPerFlag.size());
    }
  }

  @Test
  public void testRecordEvaluation_noOpWithoutScope() {
    // Recording without a scope should be a no-op (not throw)
    DynamicFeatureFlagsMetricsRecorder.recordEvaluation("orphan-flag", true);

    // No tags should be recorded
    List<Tags> tagsPerFlag = DynamicFeatureFlagsMetricsRecorder.getTagsPerFlag();
    assertEquals(0, tagsPerFlag.size());
  }

  @Test
  public void testRegistryEvaluation_enabledPhase_recordsTrue() {
    DynamicFeatureFlagConfig config =
        new DynamicFeatureFlagConfig(
            "enabled-feature",
            DynamicFeatureFlagConfig.Phase.ENABLED,
            List.of(),
            List.of(),
            0,
            DynamicFeatureFlagConfig.Scope.ORG);

    DynamicFeatureFlagRegistry registry =
        new DynamicFeatureFlagRegistry(
            Optional.of(List.of(config)), Optional.empty(), Optional.empty(), Optional.empty());

    try (var scope = DynamicFeatureFlagsMetricsRecorder.setup()) {
      ObjectId entityId = new ObjectId();
      boolean result = registry.evaluate("enabled-feature", entityId, false);

      assertTrue(result);

      // Verify the evaluation was recorded in the metrics recorder
      List<Tags> tagsPerFlag = DynamicFeatureFlagsMetricsRecorder.getTagsPerFlag();

      assertEquals(1, tagsPerFlag.size());
      Tags tags = tagsPerFlag.getFirst();
      List<Tag> tagList = tags.stream().toList();

      assertEquals(2, tagList.size());
      assertThat(tagList)
          .contains(
              Tag.of(DynamicFeatureFlagsMetricsRecorder.FEATURE_FLAG_NAME_TAG, "enabled-feature"));
      assertThat(tagList)
          .contains(Tag.of(DynamicFeatureFlagsMetricsRecorder.EVALUATION_RESULT_TAG, "true"));
    }
  }

  @Test
  public void testRegistryEvaluation_multipleFlags_recordsAllEvaluations() {
    DynamicFeatureFlagConfig enabledConfig =
        new DynamicFeatureFlagConfig(
            "feature-enabled",
            DynamicFeatureFlagConfig.Phase.ENABLED,
            List.of(),
            List.of(),
            0,
            DynamicFeatureFlagConfig.Scope.ORG);

    DynamicFeatureFlagConfig disabledConfig =
        new DynamicFeatureFlagConfig(
            "feature-disabled",
            DynamicFeatureFlagConfig.Phase.DISABLED,
            List.of(),
            List.of(),
            0,
            DynamicFeatureFlagConfig.Scope.ORG);

    DynamicFeatureFlagRegistry registry =
        new DynamicFeatureFlagRegistry(
            Optional.of(List.of(enabledConfig, disabledConfig)),
            Optional.empty(),
            Optional.empty(),
            Optional.empty());

    try (var scope = DynamicFeatureFlagsMetricsRecorder.setup()) {
      ObjectId entityId = new ObjectId();
      boolean enabledResult = registry.evaluate("feature-enabled", entityId, false);
      boolean disabledResult = registry.evaluate("feature-disabled", entityId, true);

      assertTrue(enabledResult);
      assertFalse(disabledResult);

      // Verify both evaluations were recorded
      List<Tags> tagsPerFlag = DynamicFeatureFlagsMetricsRecorder.getTagsPerFlag();

      assertEquals(2, tagsPerFlag.size());
      assertThat(tagsPerFlag)
          .contains(
              Tags.of(
                  DynamicFeatureFlagsMetricsRecorder.FEATURE_FLAG_NAME_TAG,
                  "feature-enabled",
                  DynamicFeatureFlagsMetricsRecorder.EVALUATION_RESULT_TAG,
                  "true"));
      assertThat(tagsPerFlag)
          .contains(
              Tags.of(
                  DynamicFeatureFlagsMetricsRecorder.FEATURE_FLAG_NAME_TAG,
                  "feature-disabled",
                  DynamicFeatureFlagsMetricsRecorder.EVALUATION_RESULT_TAG,
                  "false"));
    }
  }
}
