package com.xgen.mongot.index.lucene;

import static com.google.common.truth.Truth.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.xgen.mongot.config.util.HysteresisConfig;
import com.xgen.mongot.featureflag.Feature;
import com.xgen.mongot.featureflag.FeatureFlags;
import com.xgen.mongot.index.lucene.config.LuceneConfig;
import com.xgen.mongot.index.lucene.merge.DiskUtilizationAwareMergePolicy;
import com.xgen.mongot.index.lucene.merge.MergePolicyFactory;
import com.xgen.mongot.index.lucene.merge.VectorMergePolicy;
import com.xgen.mongot.monitor.PeriodicDiskMonitor;
import com.xgen.mongot.monitor.ToggleGate;
import com.xgen.mongot.util.Bytes;
import com.xgen.testing.mongot.index.lucene.LuceneConfigBuilder;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.IOException;
import java.nio.file.FileStore;
import java.util.Optional;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.TieredMergePolicy;
import org.junit.Assert;
import org.junit.Test;

public class MergePolicyFactoryTest {

  /** Default values defined in Lucene's {@link TieredMergePolicy} */
  private static final int DEFAULT_DELETES_PCT_ALLOWED = 20;

  private static final int FORCE_MERGE_DELETES_PCT_ALLOWED = 10;

  @Test
  public void createMergePolicy_validConfig_nonDiskBased() {
    LuceneConfig config = createConfig(64, 50, 20);
    var meterRegistry = new SimpleMeterRegistry();

    MergePolicy mergePolicy =
        MergePolicyFactory.createMergePolicy(config, ToggleGate.opened(), meterRegistry);
    assertThat(mergePolicy).isInstanceOf(TieredMergePolicy.class);

    TieredMergePolicy tmp = (TieredMergePolicy) mergePolicy;
    Assert.assertEquals(64, tmp.getMaxMergedSegmentMB(), 0.1);
    Assert.assertEquals(50, tmp.getDeletesPctAllowed(), 0.1);
    Assert.assertEquals(20, tmp.getForceMergeDeletesPctAllowed(), 0.1);
  }

  @Test
  public void createMergePolicy_defaultOptionalConfig_nonDiskBased() {
    LuceneConfig config =
        LuceneConfigBuilder.builder().tempDataPath().maxMergedSegmentSize(Bytes.ofMebi(64)).build();
    var meterRegistry = new SimpleMeterRegistry();

    MergePolicy mergePolicy =
        MergePolicyFactory.createMergePolicy(config, ToggleGate.opened(), meterRegistry);
    assertThat(mergePolicy).isInstanceOf(TieredMergePolicy.class);

    TieredMergePolicy tmp = (TieredMergePolicy) mergePolicy;
    Assert.assertEquals(64, tmp.getMaxMergedSegmentMB(), 0.1);
    Assert.assertEquals(DEFAULT_DELETES_PCT_ALLOWED, tmp.getDeletesPctAllowed(), 0.1);
    Assert.assertEquals(FORCE_MERGE_DELETES_PCT_ALLOWED, tmp.getForceMergeDeletesPctAllowed(), 0.1);

    // Assert that lucene defaults from 10.1 are used.
    Assert.assertEquals(10, tmp.getSegmentsPerTier(), 0.1);
    Assert.assertEquals(2, tmp.getFloorSegmentMB(), 0.1);
  }

  @Test
  public void createMergePolicy_validConfig_diskBased() throws IOException {
    var gateConfig = new HysteresisConfig(0.7, 0.8);
    LuceneConfig config = createDiskBasedConfig(64, 50, 20, gateConfig);

    var meterRegistry = new SimpleMeterRegistry();

    // Create a mock FileStore with 50% disk utilization (below threshold)
    var mockFileStore = mock(FileStore.class);
    // Available disk space is 50% by default.
    when(mockFileStore.getTotalSpace()).thenReturn(1000_000_000L);
    when(mockFileStore.getUsableSpace()).thenReturn(500_000_000L);

    var diskMonitor = PeriodicDiskMonitor.create(mockFileStore, 0.95, meterRegistry);
    var gate = DiskUtilizationAwareMergePolicy.createMergeGate(config, diskMonitor);

    MergePolicy mergePolicy = MergePolicyFactory.createMergePolicy(config, gate, meterRegistry);
    assertThat(mergePolicy).isInstanceOf(DiskUtilizationAwareMergePolicy.class);

    MergePolicy parent = ((DiskUtilizationAwareMergePolicy) mergePolicy).unwrap();
    assertThat(parent).isInstanceOf(TieredMergePolicy.class);

    TieredMergePolicy tmp = (TieredMergePolicy) parent;
    Assert.assertEquals(64, tmp.getMaxMergedSegmentMB(), 0.1);
    Assert.assertEquals(50, tmp.getDeletesPctAllowed(), 0.1);
    Assert.assertEquals(20, tmp.getForceMergeDeletesPctAllowed(), 0.1);
  }

  @Test
  public void createVectorMergePolicy_validConfig_nonDiskBased() {
    LuceneConfig.VectorMergePolicyConfig vectorConfig =
        new LuceneConfig.VectorMergePolicyConfig(100, 1024, 2048, 1024, 2048);
    LuceneConfig config =
        LuceneConfigBuilder.builder()
            .tempDataPath()
            .floorSegmentMB(64)
            .maxMergedSegmentSize(Bytes.ofMebi(64))
            .deletesPctAllowed(50)
            .forceMergeDeletesPctAllowed(20)
            .vectorMergePolicyConfig(vectorConfig)
            .build();
    FeatureFlags featureFlags = createFeatureFlags(true);

    var meterRegistry = new SimpleMeterRegistry();

    Optional<MergePolicy> vectorMergePolicy =
        MergePolicyFactory.createVectorMergePolicy(
            config, featureFlags, ToggleGate.opened(), meterRegistry);
    assertThat(vectorMergePolicy).isPresent();
    assertThat(vectorMergePolicy.get()).isInstanceOf(VectorMergePolicy.class);

    MergePolicy parent = ((VectorMergePolicy) vectorMergePolicy.get()).unwrap();
    assertThat(parent).isInstanceOf(TieredMergePolicy.class);
    TieredMergePolicy tmp = (TieredMergePolicy) parent;
    Assert.assertEquals(64, tmp.getFloorSegmentMB(), 0.1);
    Assert.assertEquals(64, tmp.getMaxMergedSegmentMB(), 0.1);
    Assert.assertEquals(50, tmp.getDeletesPctAllowed(), 0.1);
    Assert.assertEquals(20, tmp.getForceMergeDeletesPctAllowed(), 0.1);
  }

  @Test
  public void createVectorMergePolicy_validConfig_nonDiskBased_featureFlagDisabled() {
    LuceneConfig.VectorMergePolicyConfig vectorConfig =
        new LuceneConfig.VectorMergePolicyConfig(100, 1024, 2048, 1024, 2048);
    LuceneConfig config =
        LuceneConfigBuilder.builder()
            .tempDataPath()
            .floorSegmentMB(64)
            .maxMergedSegmentSize(Bytes.ofMebi(64))
            .deletesPctAllowed(50)
            .forceMergeDeletesPctAllowed(20)
            .vectorMergePolicyConfig(vectorConfig)
            .build();
    FeatureFlags featureFlags = createFeatureFlags(false);

    var meterRegistry = new SimpleMeterRegistry();

    Optional<MergePolicy> vectorMergePolicy =
        MergePolicyFactory.createVectorMergePolicy(
            config, featureFlags, ToggleGate.opened(), meterRegistry);
    assertThat(vectorMergePolicy).isPresent();
    assertThat(vectorMergePolicy.get()).isInstanceOf(VectorMergePolicy.class);

    MergePolicy parent = ((VectorMergePolicy) vectorMergePolicy.get()).unwrap();
    assertThat(parent).isInstanceOf(TieredMergePolicy.class);
    TieredMergePolicy tmp = (TieredMergePolicy) parent;
    Assert.assertEquals(2, tmp.getFloorSegmentMB(), 0.1);
    Assert.assertEquals(64, tmp.getMaxMergedSegmentMB(), 0.1);
    Assert.assertEquals(50, tmp.getDeletesPctAllowed(), 0.1);
    Assert.assertEquals(20, tmp.getForceMergeDeletesPctAllowed(), 0.1);
  }

  @Test
  public void createVectorMergePolicy_defaultOptionalConfig_nonDiskBased() {
    LuceneConfig.VectorMergePolicyConfig vectorConfig =
        new LuceneConfig.VectorMergePolicyConfig(100, 1024, 2048, 1024, 2048);
    LuceneConfig config =
        LuceneConfigBuilder.builder().tempDataPath().vectorMergePolicyConfig(vectorConfig).build();
    var meterRegistry = new SimpleMeterRegistry();
    FeatureFlags featureFlags = createFeatureFlags(true);

    Optional<MergePolicy> vectorMergePolicy =
        MergePolicyFactory.createVectorMergePolicy(
            config, featureFlags, ToggleGate.opened(), meterRegistry);
    assertThat(vectorMergePolicy).isPresent();
    assertThat(vectorMergePolicy.get()).isInstanceOf(VectorMergePolicy.class);

    MergePolicy parent = ((VectorMergePolicy) vectorMergePolicy.get()).unwrap();
    assertThat(parent).isInstanceOf(TieredMergePolicy.class);
    TieredMergePolicy tmp = (TieredMergePolicy) parent;
    Assert.assertEquals(2, tmp.getFloorSegmentMB(), 0.1);
    Assert.assertEquals(config.maxMergedSegmentSize().toMebi(), tmp.getMaxMergedSegmentMB(), 0.1);
    Assert.assertEquals(20, tmp.getDeletesPctAllowed(), 0.1);
    Assert.assertEquals(10, tmp.getForceMergeDeletesPctAllowed(), 0.1);
  }

  @Test
  public void createVectorMergePolicy_validConfig_diskBased() throws IOException {
    var gateConfig = new HysteresisConfig(0.7, 0.8);
    LuceneConfig.VectorMergePolicyConfig vectorConfig =
        new LuceneConfig.VectorMergePolicyConfig(100, 1024, 2048, 1024, 2048);
    LuceneConfig config =
        LuceneConfigBuilder.builder()
            .tempDataPath()
            .maxMergedSegmentSize(Bytes.ofMebi(64))
            .deletesPctAllowed(50)
            .forceMergeDeletesPctAllowed(20)
            .vectorMergePolicyConfig(vectorConfig)
            .mergePolicyDiskUtilizationConfig(gateConfig)
            .build();
    FeatureFlags featureFlags = createFeatureFlags(true);

    var meterRegistry = new SimpleMeterRegistry();

    // Create a mock FileStore with 50% disk utilization (below threshold)
    var mockFileStore = mock(FileStore.class);
    // Available disk space is 50% by default.
    when(mockFileStore.getTotalSpace()).thenReturn(1000_000_000L);
    when(mockFileStore.getUsableSpace()).thenReturn(500_000_000L);

    var diskMonitor = PeriodicDiskMonitor.create(mockFileStore, 0.95, meterRegistry);
    var gate = DiskUtilizationAwareMergePolicy.createMergeGate(config, diskMonitor);

    Optional<MergePolicy> vectorMergePolicy =
        MergePolicyFactory.createVectorMergePolicy(config, featureFlags, gate, meterRegistry);
    assertThat(vectorMergePolicy).isPresent();
    assertThat(vectorMergePolicy.get()).isInstanceOf(VectorMergePolicy.class);

    MergePolicy parent = ((VectorMergePolicy) vectorMergePolicy.get()).unwrap();
    assertThat(parent).isInstanceOf(DiskUtilizationAwareMergePolicy.class);
    DiskUtilizationAwareMergePolicy diskAwarePolicy = (DiskUtilizationAwareMergePolicy) parent;
    MergePolicy diskAwareParent = diskAwarePolicy.unwrap();
    assertThat(diskAwareParent).isInstanceOf(TieredMergePolicy.class);

    TieredMergePolicy tmp = (TieredMergePolicy) diskAwareParent;
    Assert.assertEquals(64, tmp.getMaxMergedSegmentMB(), 0.1);
    Assert.assertEquals(50, tmp.getDeletesPctAllowed(), 0.1);
    Assert.assertEquals(20, tmp.getForceMergeDeletesPctAllowed(), 0.1);
  }

  @Test
  public void createVectorMergePolicy_noVectorConfig() {
    LuceneConfig config = createConfig(64, 50, 20);
    FeatureFlags featureFlags = createFeatureFlags(true);
    var meterRegistry = new SimpleMeterRegistry();

    Optional<MergePolicy> vectorMergePolicy =
        MergePolicyFactory.createVectorMergePolicy(
            config, featureFlags, ToggleGate.opened(), meterRegistry);
    assertThat(vectorMergePolicy).isEmpty();
  }

  private LuceneConfig createConfig(
      long maxMergedSegmentSize, double deletesPctAllowed, double forceMergeDeletesPctAllowed) {
    return LuceneConfigBuilder.builder()
        .tempDataPath()
        .maxMergedSegmentSize(Bytes.ofMebi(maxMergedSegmentSize))
        .deletesPctAllowed(deletesPctAllowed)
        .forceMergeDeletesPctAllowed(forceMergeDeletesPctAllowed)
        .build();
  }

  private LuceneConfig createDiskBasedConfig(
      long maxMergedSegmentSize,
      double deletesPctAllowed,
      double forceMergeDeletesPctAllowed,
      HysteresisConfig hysteresisConfig) {
    return LuceneConfigBuilder.builder()
        .tempDataPath()
        .maxMergedSegmentSize(Bytes.ofMebi(maxMergedSegmentSize))
        .deletesPctAllowed(deletesPctAllowed)
        .forceMergeDeletesPctAllowed(forceMergeDeletesPctAllowed)
        .mergePolicyDiskUtilizationConfig(hysteresisConfig)
        .build();
  }

  private FeatureFlags createFeatureFlags(boolean enableFloorSegmentMB) {
    var builder = FeatureFlags.withDefaults();
    if (enableFloorSegmentMB) {
      builder.enable(Feature.FLOOR_SEGMENT_MB);
    }
    return builder.build();
  }
}
