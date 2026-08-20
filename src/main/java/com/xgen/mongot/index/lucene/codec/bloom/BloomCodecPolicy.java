package com.xgen.mongot.index.lucene.codec.bloom;

import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlagRegistry;
import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlags;
import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.status.IndexStatus;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;

/**
 * Determines when bloom postings should be used for indexing and when bloom bitsets should be
 * loaded while opening segments (via {@link MongotBloomReadPolicy}), driven by dynamic feature
 * flags and {@link IndexStatus}.
 *
 * <p>When {@link DynamicFeatureFlags#BLOOM_FILTER_FOR_ID_FIELD} is on and natural-order scan is
 * enabled, bloom is enabled while the index cannot yet service queries (typically {@link
 * IndexStatus.StatusCode#INITIAL_SYNC}). After {@link IndexStatus#canServiceQueries()} becomes
 * true, bloom stays off unless {@link DynamicFeatureFlags#BLOOM_FILTER_IN_STEADY_STATE} is enabled.
 */
public final class BloomCodecPolicy {

  private BloomCodecPolicy() {}

  /**
   * Supplier used both by {@link com.xgen.mongot.index.lucene.writer.SingleLuceneIndexWriter}
   * (whether new and merged segments use bloom-encoded {@code _id}) and by readers when paired with
   * {@link MongotBloomReadPolicy} (whether bloom payloads are loaded at all).
   */
  public static BooleanSupplier getBloomFilterEnabledForIdField(
      DynamicFeatureFlagRegistry dynamicFeatureFlagRegistry,
      boolean enableNaturalOrderScan,
      IndexDefinition indexDefinition,
      Supplier<IndexStatus> indexStatusSupplier) {

    return () -> {
      if (isNaturalScanUnsupportedForIndex(indexDefinition)) {
        return false;
      }
      if (!enableNaturalOrderScan) {
        return false;
      }
      IndexStatus indexStatus = indexStatusSupplier.get();
      if (indexStatus.getStatusCode() == IndexStatus.StatusCode.FAILED) {
        return false;
      }

      if (!dynamicFeatureFlagRegistry.evaluateClusterInvariant(
          DynamicFeatureFlags.BLOOM_FILTER_FOR_ID_FIELD)) {
        return false;
      }

      if (!indexStatus.canServiceQueries()) {
        return true;
      }

      return dynamicFeatureFlagRegistry.evaluateClusterInvariant(
          DynamicFeatureFlags.BLOOM_FILTER_IN_STEADY_STATE);
    };
  }

  public static boolean isNaturalScanUnsupportedForIndex(IndexDefinition indexDefinition) {
    return indexDefinition.isAutoEmbeddingIndex();
  }
}
