package com.xgen.mongot.index.lucene.codec.bloom;

import com.google.common.annotations.VisibleForTesting;
import com.xgen.mongot.featureflag.Feature;
import com.xgen.mongot.featureflag.FeatureFlags;
import org.jetbrains.annotations.Nullable;

/**
 * Resolves where bloom filter bitsets are read from, off the {@link Feature#BLOOM_FILTER_ON_HEAP}
 * static feature flag. The flag names the fallback rather than the default: disabled — the shipped
 * state — reads from the memory-mapped {@code .blm}, and enabling it reverts to copying each
 * segment's bitset onto the JVM heap at open.
 *
 * <p>The bloom codec is instantiated by Lucene through SPI, so it cannot receive {@link
 * FeatureFlags} by injection and reads the process-wide instance instead. The flag is
 * <em>static</em>: its value is fixed for the lifetime of the process, so the mode a segment
 * resolves at open time cannot change underneath it, and every segment in a process agrees.
 * Changing it takes a restart.
 *
 * <p>Only the read path is affected. Bitsets are always built on the heap during flush and merge,
 * and the serialized {@code .blm} is byte-identical in both modes.
 *
 * <p>When the process-wide {@link FeatureFlags} has not been installed — which happens only in unit
 * tests that exercise the codec directly, never in a real mongot process, where {@code
 * MmsMongotBootstrapper} installs it before any index is opened — this falls back to the flag's own
 * default state, so those tests exercise the same path production does.
 */
public final class BloomStoragePolicy {

  @Nullable private static volatile MongotFuzzySet.StorageMode overrideForTest = null;

  private BloomStoragePolicy() {}

  /**
   * Storage mode for bloom bitsets opened from now on. Resolved per call rather than cached, so
   * that tests using {@link #setStorageModeForTest} are not order-dependent; the read is an {@link
   * java.util.EnumMap} lookup and happens at segment open, not per probe.
   */
  public static MongotFuzzySet.StorageMode storageMode() {
    MongotFuzzySet.StorageMode override = overrideForTest;
    if (override != null) {
      return override;
    }
    return isOnHeapEnabled() ? MongotFuzzySet.StorageMode.HEAP : MongotFuzzySet.StorageMode.INPUT;
  }

  /** True when bloom bitsets should be read from the memory-mapped {@code .blm}. */
  public static boolean inputReadPathEnabled() {
    return storageMode() == MongotFuzzySet.StorageMode.INPUT;
  }

  private static boolean isOnHeapEnabled() {
    try {
      return FeatureFlags.getStatic().isEnabled(Feature.BLOOM_FILTER_ON_HEAP);
    } catch (IllegalStateException e) {
      // Process-wide flags not installed: codec-level unit test. Match the shipped default.
      return Feature.BLOOM_FILTER_ON_HEAP.getDefaultState() == Feature.State.ENABLED;
    }
  }

  /**
   * Forces the storage mode, ignoring the feature flag. Pass {@code null} to restore flag-driven
   * behavior; tests must do so in teardown, as this is process-wide state.
   */
  @VisibleForTesting
  public static void setStorageModeForTest(@Nullable MongotFuzzySet.StorageMode mode) {
    overrideForTest = mode;
  }
}
