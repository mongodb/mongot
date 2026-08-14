package com.xgen.mongot.index.lucene;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import com.xgen.mongot.index.lucene.merge.InstrumentedConcurrentMergeScheduler;
import com.xgen.mongot.index.lucene.merge.InstrumentedConcurrentMergeScheduler.PerIndexPartitionMergeScheduler;
import com.xgen.mongot.index.version.GenerationId;
import com.xgen.mongot.metrics.ThreadPoolResourceMetrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.Optional;
import org.apache.lucene.index.ConcurrentMergeScheduler;
import org.apache.lucene.store.Directory;
import org.junit.Test;

public class InstrumentedConcurrentMergeSchedulerTest {

  /** Test-only wrapper that supplies a fixed {@code "test"} tag. */
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
  public void constructor_seedsMaxMergesAndThreads_notAutoDetect() {
    // Guards the Lucene 10.4 ConcurrentMergeScheduler.maybeStall hang: if maxMergeCount /
    // maxThreadCount are left at AUTO_DETECT_MERGES_AND_THREADS (-1), the `mergeThreadCount() >=
    // -1`
    // check is always true and maybeStall blocks forever. The constructor must seed real defaults
    // because PerIndexPartitionMergeScheduler bypasses ConcurrentMergeScheduler.initialize().
    var scheduler = new InstrumentedConcurrentMergeScheduler(new SimpleMeterRegistry());

    int coreCount =
        Runtime.getRuntime()
            .availableProcessors(); // (test can override via lucene.cms.override_core_count)
    int maxThreadCount = Math.max(1, coreCount / 2);
    int maxMergeCount = maxThreadCount + 5;

    assertEquals(maxThreadCount, scheduler.getMaxThreadCount());
    assertEquals(maxMergeCount, scheduler.getMaxMergeCount());
    assertNotEquals(
        ConcurrentMergeScheduler.AUTO_DETECT_MERGES_AND_THREADS, scheduler.getMaxThreadCount());
    assertNotEquals(
        ConcurrentMergeScheduler.AUTO_DETECT_MERGES_AND_THREADS, scheduler.getMaxMergeCount());
  }

  @Test
  public void attributionDisabled_doesNotRegisterMergeAttributionMetrics() {
    var registry = new SimpleMeterRegistry();
    new InstrumentedConcurrentMergeScheduler(registry);

    assertThat(registry.find("mergeScheduler.merge.allocatedBytes").meters()).isEmpty();
    assertThat(registry.find("mergeScheduler.merge.cpuTimeNanos").meters()).isEmpty();
    assertThat(registry.find("executor.thread.allocatedBytes").tag("subsystem", "merge").meters())
        .isEmpty();
    assertThat(registry.find("executor.thread.cpuTime").tag("subsystem", "merge").meters())
        .isEmpty();
  }

  @Test
  public void attributionEnabled_registersAggregateLiveMergeCounters() {
    var registry = new SimpleMeterRegistry();
    new InstrumentedConcurrentMergeScheduler(
        registry,
        InstrumentedConcurrentMergeScheduler.DEFAULT_CANCEL_MERGE_PER_THREAD_TIMEOUT_MS,
        InstrumentedConcurrentMergeScheduler.DEFAULT_CANCEL_ALL_MERGES_PER_THREAD_TIMEOUT_MS,
        Optional.of(new ThreadPoolResourceMetrics.Bindings()));

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
