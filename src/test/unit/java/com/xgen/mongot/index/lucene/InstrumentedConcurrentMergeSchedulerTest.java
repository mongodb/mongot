package com.xgen.mongot.index.lucene;

import static com.google.common.truth.Truth.assertThat;

import com.xgen.mongot.index.lucene.merge.InstrumentedConcurrentMergeScheduler;
import com.xgen.mongot.index.lucene.merge.InstrumentedConcurrentMergeScheduler.PerIndexPartitionMergeScheduler;
import com.xgen.mongot.index.version.GenerationId;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.lucene.store.Directory;
import org.junit.Test;

public class InstrumentedConcurrentMergeSchedulerTest {

  /**
   * Test-only wrapper that supplies a fixed {@code "test"} tag.
   */
  static PerIndexPartitionMergeScheduler createMergeScheduler(
      InstrumentedConcurrentMergeScheduler scheduler,
      GenerationId generationId,
      int indexPartitionId,
      int numIndexes,
      boolean cancelMergeEnabled,
      Directory directory) {
    return scheduler.createForIndexPartition(
        generationId,
        indexPartitionId,
        numIndexes,
        cancelMergeEnabled,
        () -> false,
        "test",
        directory);
  }

  @Test
  public void attributionDisabled_doesNotRegisterMergeAttributionMetrics() {
    var registry = new SimpleMeterRegistry();
    new InstrumentedConcurrentMergeScheduler(registry);

    assertThat(registry.find("mergeScheduler.merge.allocatedBytes").meters()).isEmpty();
    assertThat(registry.find("mergeScheduler.merge.cpuTimeNanos").meters()).isEmpty();
    assertThat(
            registry
                .find("executor.thread.allocatedBytes")
                .tag("subsystem", "merge")
                .meters())
        .isEmpty();
    assertThat(
            registry.find("executor.thread.cpuTime").tag("subsystem", "merge").meters())
        .isEmpty();
  }

  @Test
  public void attributionEnabled_registersAggregateLiveMergeCounters() {
    var registry = new SimpleMeterRegistry();
    new InstrumentedConcurrentMergeScheduler(
        registry,
        InstrumentedConcurrentMergeScheduler.DEFAULT_CANCEL_MERGE_PER_THREAD_TIMEOUT_MS,
        InstrumentedConcurrentMergeScheduler.DEFAULT_CANCEL_ALL_MERGES_PER_THREAD_TIMEOUT_MS,
        true);

    assertThat(
            registry
                .find("executor.thread.allocatedBytes")
                .tag("subsystem", "merge")
                .tag("name", "lucene-merge")
                .functionCounter())
        .isNotNull();
    assertThat(
            registry
                .find("executor.thread.cpuTime")
                .tag("subsystem", "merge")
                .tag("name", "lucene-merge")
                .functionCounter())
        .isNotNull();

    // Per-merge histograms are lazy-registered on first sample, not at construction time.
    assertThat(registry.find("mergeScheduler.merge.allocatedBytes").meters()).isEmpty();
    assertThat(registry.find("mergeScheduler.merge.cpuTimeNanos").meters()).isEmpty();
  }
}
