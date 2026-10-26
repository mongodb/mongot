package com.xgen.mongot.index.definition;

import static com.google.common.truth.Truth.assertThat;

import com.xgen.mongot.index.version.IndexFormatVersion;
import org.junit.Test;

public class IndexCapabilitiesNormalizeCosineTest {

  @Test
  public void searchCapabilities_featureVersionBelowFive_doesNotNormalize() {
    assertThat(
            new SearchIndexCapabilities(IndexFormatVersion.CURRENT, 3)
                .shouldNormalizeCosineVectors())
        .isFalse();
    assertThat(
            new SearchIndexCapabilities(IndexFormatVersion.CURRENT, 4)
                .shouldNormalizeCosineVectors())
        .isFalse();
  }

  @Test
  public void searchCapabilities_featureVersionFiveOrAbove_normalizes() {
    assertThat(
            new SearchIndexCapabilities(IndexFormatVersion.CURRENT, 5)
                .shouldNormalizeCosineVectors())
        .isTrue();
    assertThat(
            new SearchIndexCapabilities(IndexFormatVersion.CURRENT, 6)
                .shouldNormalizeCosineVectors())
        .isTrue();
  }

  @Test
  public void vectorCapabilities_featureVersionBelowFive_doesNotNormalize() {
    assertThat(
            new VectorIndexCapabilities(IndexFormatVersion.CURRENT, 3)
                .shouldNormalizeCosineVectors())
        .isFalse();
    assertThat(
            new VectorIndexCapabilities(IndexFormatVersion.CURRENT, 4)
                .shouldNormalizeCosineVectors())
        .isFalse();
  }

  @Test
  public void vectorCapabilities_featureVersionFiveOrAbove_normalizes() {
    assertThat(
            new VectorIndexCapabilities(IndexFormatVersion.CURRENT, 5)
                .shouldNormalizeCosineVectors())
        .isTrue();
    assertThat(
            new VectorIndexCapabilities(IndexFormatVersion.CURRENT, 6)
                .shouldNormalizeCosineVectors())
        .isTrue();
  }

  @Test
  public void currentCapabilities_currentFeatureVersionIsBelowFive_doesNotNormalize() {
    // Guards that merging the gate does not activate it: normalization stays off for every index
    // until CURRENT_FEATURE_VERSION is bumped to 5.
    assertThat(SearchIndexCapabilities.CURRENT.shouldNormalizeCosineVectors()).isFalse();
    assertThat(VectorIndexCapabilities.CURRENT.shouldNormalizeCosineVectors()).isFalse();
  }
}
