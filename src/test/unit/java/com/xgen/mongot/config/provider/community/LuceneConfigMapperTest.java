package com.xgen.mongot.config.provider.community;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.xgen.mongot.config.util.HysteresisConfig;
import com.xgen.mongot.index.lucene.config.LuceneConfig;
import com.xgen.mongot.index.lucene.config.LuceneConfig.VectorMergePolicyConfig;
import com.xgen.mongot.util.Bytes;
import com.xgen.mongot.util.DeploymentEnvironment;
import com.xgen.mongot.util.Runtime;
import com.xgen.testing.util.MockRuntimeBuilder;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Optional;
import org.junit.Test;

public class LuceneConfigMapperTest {

  private static final int NUM_CPUS = 8;
  private static final Bytes MAX_HEAP = Bytes.ofMebi(4096);
  private static final Bytes INSTANCE_MEMORY = Bytes.ofMebi(16384);
  private static final Runtime RUNTIME =
      new MockRuntimeBuilder()
          .withNumCpus(NUM_CPUS)
          .withMaxHeapSize(MAX_HEAP)
          .withTotalMemory(INSTANCE_MEMORY)
          .build();
  private static final Path DEFAULT_CONFIG =
      Path.of("src/test/unit/resources/config/provider/community/communityConfig.yaml");
  private static final Path TUNING_CONFIG =
      Path.of("src/test/unit/resources/config/provider/community/communityConfigTuning.yaml");

  @Test
  public void toLuceneConfig_appliesCommunityDefaultsWhenUnset() throws Exception {
    CommunityConfig config =
        CommunityConfig.readFromFile(DEFAULT_CONFIG, Optional.of(DeploymentEnvironment.COMMUNITY))
            .config();

    LuceneConfig lc =
        LuceneConfigMapper.toLuceneConfig(
            config, RUNTIME, Optional.of(new HysteresisConfig(0.9, 0.9)));

    assertEquals(config.storageConfig().dataPath(), lc.dataPath());
    assertEquals(Optional.empty(), lc.fieldLimit());
    assertEquals(Optional.of(LuceneConfig.MAX_DOCS_LIMIT), lc.docsLimit());
    assertEquals(NUM_CPUS, lc.numMaxMergeThreads());
    assertEquals(Optional.of(64.0), lc.vectorFloorSegmentMB());
    assertTrue(lc.enableConcurrentSearch());
    assertEquals(
        Optional.of(new HysteresisConfig(0.9, 0.9)), lc.mergePolicyDiskUtilizationConfig());

    VectorMergePolicyConfig vmp = lc.vectorMergePolicyConfig().get();
    assertEquals((int) Math.ceil(0.2 * 4096), vmp.segmentHeapBudgetMb());
    assertEquals((int) Math.ceil(0.8 * 4096), vmp.globalHeapBudgetMb());
  }

  @Test
  public void toLuceneConfig_honorsConfiguredValues() throws Exception {
    CommunityConfig config =
        CommunityConfig.readFromFile(TUNING_CONFIG, Optional.of(DeploymentEnvironment.COMMUNITY))
            .config();

    LuceneConfig lc = LuceneConfigMapper.toLuceneConfig(config, RUNTIME, Optional.empty());

    assertEquals(Duration.ofMillis(2000), lc.refreshInterval());
    assertEquals(6, lc.numMaxMergeThreads());
    assertEquals(Optional.of(500), lc.fieldLimit());
    assertEquals(Optional.of(2048), lc.maxClauseLimit());

    VectorMergePolicyConfig vmp = lc.vectorMergePolicyConfig().get();
    assertEquals(512, vmp.mergeBudgetMb());
    assertEquals((int) Math.ceil(0.5 * 512), vmp.maxVectorInputMb());
    assertEquals((int) Math.ceil(0.1 * Math.ceil(0.5 * 512)), vmp.maxCompoundDataMb());
    assertEquals((int) Math.ceil(0.2 * 4096), vmp.segmentHeapBudgetMb());
    assertEquals((int) Math.ceil(0.8 * 4096), vmp.globalHeapBudgetMb());

    assertEquals(Optional.of(LuceneConfig.MAX_DOCS_LIMIT), lc.docsLimit());
    assertEquals(Optional.of(128.0), lc.vectorFloorSegmentMB());
    assertTrue(lc.enableConcurrentSearch());
    assertEquals(Optional.empty(), lc.mergePolicyDiskUtilizationConfig());
  }

  @Test
  public void getVectorMergePolicy_derivesBudgetsFromInstanceMemoryAndHeap() {
    VectorMergePolicyConfig vmp =
        LuceneConfigMapper.getVectorMergePolicy(RUNTIME, Optional.empty());

    int expectedMergeBudgetMb = (int) (0.10 * 16384);
    int expectedMaxVectorInputMb = (int) Math.ceil(0.5 * expectedMergeBudgetMb);
    assertEquals(expectedMergeBudgetMb, vmp.mergeBudgetMb());
    assertEquals(expectedMaxVectorInputMb, vmp.maxVectorInputMb());
    assertEquals((int) Math.ceil(0.05 * expectedMergeBudgetMb), vmp.maxCompoundDataMb());
    assertEquals((int) Math.ceil(0.2 * 4096), vmp.segmentHeapBudgetMb());
    assertEquals((int) Math.ceil(0.8 * 4096), vmp.globalHeapBudgetMb());
  }
}
