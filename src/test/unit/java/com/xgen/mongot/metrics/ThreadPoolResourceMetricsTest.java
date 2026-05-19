package com.xgen.mongot.metrics;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;

import com.sun.management.ThreadMXBean;
import com.xgen.mongot.util.concurrent.Executors;
import com.xgen.mongot.util.concurrent.LiveThreadIdsRegistry;
import com.xgen.mongot.util.concurrent.NamedExecutorService;
import io.micrometer.core.instrument.FunctionCounter;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.lang.management.ManagementFactory;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.LongStream;
import org.junit.Test;

public class ThreadPoolResourceMetricsTest {

  private static final String SUBSYSTEM = "test";
  private static final String POOL_NAME = "tp-resource-test";
  private static final String ALLOCATED_BYTES = "executor.thread.allocatedBytes";
  private static final String CPU_TIME = "executor.thread.cpuTime";

  @Test
  public void register_addsBothCountersTaggedWithSubsystemAndPoolName() {
    var registry = new SimpleMeterRegistry();
    try (NamedExecutorService executor = Executors.fixedSizeThreadPool(POOL_NAME, 1, registry)) {
      ThreadPoolResourceMetrics.create(SUBSYSTEM).register(executor, registry);

      FunctionCounter allocated =
          registry
              .find(ALLOCATED_BYTES)
              .tag("subsystem", SUBSYSTEM)
              .tag("name", POOL_NAME)
              .functionCounter();
      FunctionCounter cpu =
          registry
              .find(CPU_TIME)
              .tag("subsystem", SUBSYSTEM)
              .tag("name", POOL_NAME)
              .functionCounter();

      assertThat(allocated).isNotNull();
      assertThat(cpu).isNotNull();
    }
  }

  @Test
  public void allocatedBytes_prunesDeadThreadIdsFromRegistry() throws Exception {
    var registry = new SimpleMeterRegistry();
    try (NamedExecutorService executor = Executors.fixedSizeThreadPool(POOL_NAME, 1, registry)) {
      // Make sure the real worker is alive so its id stays in the set.
      var ready = new CountDownLatch(1);
      executor.submit(ready::countDown);
      ready.await(1, TimeUnit.MINUTES);

      var liveIds = executor.getLiveThreadIdsRegistry().orElseThrow();
      // Inject a synthetic id the JVM cannot match, so getThreadAllocatedBytes returns -1 for it.
      long fakeDeadId = Long.MAX_VALUE - 1;
      liveIds.register(fakeDeadId);
      assertThat(liveIds.contains(fakeDeadId)).isTrue();

      ThreadPoolResourceMetrics.create(SUBSYSTEM).register(executor, registry);
      // Trigger a counter scrape (the path Prometheus follows) which sums + prunes.
      registry
          .find(ALLOCATED_BYTES)
          .tag("subsystem", SUBSYSTEM)
          .tag("name", POOL_NAME)
          .functionCounter()
          .count();

      assertThat(liveIds.contains(fakeDeadId)).isFalse();
    }
  }

  @Test
  public void cpuTimeNanos_prunesDeadThreadIdsFromRegistry() throws Exception {
    var registry = new SimpleMeterRegistry();
    try (NamedExecutorService executor = Executors.fixedSizeThreadPool(POOL_NAME, 1, registry)) {
      var ready = new CountDownLatch(1);
      executor.submit(ready::countDown);
      ready.await(1, TimeUnit.MINUTES);

      var liveIds = executor.getLiveThreadIdsRegistry().orElseThrow();
      long fakeDeadId = Long.MAX_VALUE - 1;
      liveIds.register(fakeDeadId);
      assertThat(liveIds.contains(fakeDeadId)).isTrue();

      ThreadPoolResourceMetrics.create(SUBSYSTEM).register(executor, registry);
      registry
          .find(CPU_TIME)
          .tag("subsystem", SUBSYSTEM)
          .tag("name", POOL_NAME)
          .functionCounter()
          .count();

      assertThat(liveIds.contains(fakeDeadId)).isFalse();
    }
  }

  @Test
  public void register_sameNameAcrossSubsystems_doesNotCollide() {
    var registry = new SimpleMeterRegistry();
    try (NamedExecutorService poolA = Executors.fixedSizeThreadPool(POOL_NAME, 1, registry);
        NamedExecutorService poolB = Executors.fixedSizeThreadPool(POOL_NAME + "-b", 1, registry)) {
      ThreadPoolResourceMetrics.create("query").register(poolA, registry);
      ThreadPoolResourceMetrics.create("replication").register(poolB, registry);

      assertThat(registry.find(ALLOCATED_BYTES).tag("subsystem", "query").functionCounter())
          .isNotNull();
      assertThat(registry.find(ALLOCATED_BYTES).tag("subsystem", "replication").functionCounter())
          .isNotNull();
    }
  }

  @Test
  public void allocatedBytes_increasesAfterPoolAllocates() throws Exception {
    var registry = new SimpleMeterRegistry();
    try (NamedExecutorService executor = Executors.fixedSizeThreadPool(POOL_NAME, 2, registry)) {
      ThreadPoolResourceMetrics.create(SUBSYSTEM).register(executor, registry);
      FunctionCounter allocated =
          registry
              .find(ALLOCATED_BYTES)
              .tag("subsystem", SUBSYSTEM)
              .tag("name", POOL_NAME)
              .functionCounter();

      // Touch threads first so the pool actually creates them and the counter has IDs to read.
      var ready = new CountDownLatch(2);
      executor.submit(ready::countDown);
      executor.submit(ready::countDown);
      ready.await(1, TimeUnit.MINUTES);

      double before = allocated.count();

      // Allocate ~1 MB on a pool thread, the precise byte count is JVM-internal but must increase.
      executor
          .submit(
              () -> {
                byte[] junk = new byte[1024 * 1024];
                junk[0] = 1;
              })
          .get(1, TimeUnit.MINUTES);

      double after = allocated.count();

      // On JVMs without thread allocation tracking the counter is constant 0, skip the
      // monotonicity assertion in that case rather than flaking.
      if (before != 0.0 || after != 0.0) {
        assertThat(after).isGreaterThan(before);
      }
    }
  }

  @Test
  public void register_withRegistry_emitsCountersTaggedWithSubsystemAndName() {
    var registry = new SimpleMeterRegistry();
    LiveThreadIdsRegistry liveIds = new LiveThreadIdsRegistry();
    ThreadPoolResourceMetrics.create("merge").register("lucene-merge", liveIds, registry);

    assertThat(
            registry
                .find(ALLOCATED_BYTES)
                .tag("subsystem", "merge")
                .tag("name", "lucene-merge")
                .functionCounter())
        .isNotNull();
    assertThat(
            registry
                .find(CPU_TIME)
                .tag("subsystem", "merge")
                .tag("name", "lucene-merge")
                .functionCounter())
        .isNotNull();
  }

  @Test
  public void register_withRegistry_summingZeroIdsReportsZero() {
    var registry = new SimpleMeterRegistry();
    LiveThreadIdsRegistry liveIds = new LiveThreadIdsRegistry();
    ThreadPoolResourceMetrics.create("merge").register("lucene-merge", liveIds, registry);

    FunctionCounter alloc =
        registry
            .find(ALLOCATED_BYTES)
            .tag("subsystem", "merge")
            .tag("name", "lucene-merge")
            .functionCounter();
    assertThat(alloc).isNotNull();
    // Empty live id registry -> counter reports 0 without throwing.
    assertThat(alloc.count()).isEqualTo(0.0);
  }

  @Test
  public void register_withRegistry_prunesDeadIds() {
    var registry = new SimpleMeterRegistry();
    LiveThreadIdsRegistry liveIds = new LiveThreadIdsRegistry();
    long fakeDeadId = Long.MAX_VALUE - 1;
    liveIds.register(fakeDeadId);

    ThreadPoolResourceMetrics.create("merge").register("lucene-merge", liveIds, registry);

    FunctionCounter alloc =
        registry
            .find(ALLOCATED_BYTES)
            .tag("subsystem", "merge")
            .tag("name", "lucene-merge")
            .functionCounter();
    assertThat(alloc).isNotNull();
    // Triggering the counter scrape prunes the synthetic dead id.
    alloc.count();
    assertThat(liveIds.contains(fakeDeadId)).isFalse();
  }

  @Test
  public void register_withRegistry_retainsContributionAfterThreadDeath() throws Exception {
    var registry = new SimpleMeterRegistry();
    LiveThreadIdsRegistry liveIds = new LiveThreadIdsRegistry();

    var threadStarted = new CountDownLatch(1);
    var allowExit = new CountDownLatch(1);

    Thread worker =
        new Thread(
            () -> {
              // Touch ~1 MB so the thread has a non-zero allocation footprint to be folded.
              byte[] randomAlloc = new byte[1024 * 1024];
              randomAlloc[0] = 1;
              threadStarted.countDown();
              try {
                allowExit.await(1, TimeUnit.MINUTES);
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
              }
            });
    worker.start();
    threadStarted.await(1, TimeUnit.MINUTES);

    long tid = worker.getId();
    liveIds.register(tid);

    ThreadPoolResourceMetrics.create(SUBSYSTEM).register(POOL_NAME, liveIds, registry);
    FunctionCounter alloc =
        registry
            .find(ALLOCATED_BYTES)
            .tag("subsystem", SUBSYSTEM)
            .tag("name", POOL_NAME)
            .functionCounter();
    assertThat(alloc).isNotNull();

    // Scrape while the thread is alive so lastSeenAllocByTid captures a positive value.
    double whileAlive = alloc.count();

    allowExit.countDown();
    worker.join(TimeUnit.MINUTES.toMillis(1));

    // Wait for the JVM to mark the thread dead in the MXBean view. Without this the next
    // scrape may still see a positive value for the now-joined thread.
    var bean = (ThreadMXBean) ManagementFactory.getThreadMXBean();
    long deadline = System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(1);
    while (bean.getThreadAllocatedBytes(tid) >= 0 && System.currentTimeMillis() < deadline) {
      Thread.sleep(50);
    }
    // Fail rather than assert on stale state if the JVM never reports the thread dead.
    assertWithMessage("JVM did not report thread death within deadline")
        .that(bean.getThreadAllocatedBytes(tid))
        .isLessThan(0L);

    double afterDeath = alloc.count();

    // The dying contribution must be folded into retiredAllocBytes so the counter does not
    // go backwards when the thread is pruned from the live set.
    assertThat(afterDeath).isAtLeast(whileAlive);
    assertThat(liveIds.contains(tid)).isFalse();
  }

  @Test
  public void cpuTime_increasesAfterPoolBurnsCpu() throws Exception {
    var registry = new SimpleMeterRegistry();
    try (NamedExecutorService executor = Executors.fixedSizeThreadPool(POOL_NAME, 1, registry)) {
      ThreadPoolResourceMetrics.create(SUBSYSTEM).register(executor, registry);
      FunctionCounter cpuTime =
          registry
              .find(CPU_TIME)
              .tag("subsystem", SUBSYSTEM)
              .tag("name", POOL_NAME)
              .functionCounter();

      var ready = new CountDownLatch(1);
      executor.submit(ready::countDown);
      ready.await(1, TimeUnit.MINUTES);

      double before = cpuTime.count();

      executor.submit(() -> LongStream.range(0, 5_000_000L).sum()).get(1, TimeUnit.MINUTES);

      double after = cpuTime.count();

      if (before != 0.0 || after != 0.0) {
        assertThat(after).isGreaterThan(before);
      }
    }
  }
}
