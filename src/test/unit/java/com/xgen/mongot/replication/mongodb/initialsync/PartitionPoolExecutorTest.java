package com.xgen.mongot.replication.mongodb.initialsync;

import com.xgen.mongot.replication.mongodb.common.InitialSyncException;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.Collectors;
import org.junit.Assert;
import org.junit.Test;

public class PartitionPoolExecutorTest {

  private static final String POOL_NAME = "parallel-scan-partition-pool";

  private static final Duration TIMEOUT = Duration.ofSeconds(10);

  private final MeterRegistry meterRegistry = new SimpleMeterRegistry();

  private PartitionPoolExecutor executor(int partitionCount) {
    return new PartitionPoolExecutor(POOL_NAME, partitionCount, this.meterRegistry);
  }

  private static ParallelCollectionScanner.Result result() {
    return new ParallelCollectionScanner.Result(
        ParallelCollectionScanner.StopReason.RANGE_CONSUMED, 1);
  }

  @Test
  public void submitRunsEveryPartitionAtOnce() throws Exception {
    PartitionPoolExecutor executor = executor(3);
    CountDownLatch allRunning = new CountDownLatch(3);

    List<CompletableFuture<ParallelCollectionScanner.Result>> scans =
        List.of(
            executor.submit(() -> arriveAndWaitForPeers(allRunning)),
            executor.submit(() -> arriveAndWaitForPeers(allRunning)),
            executor.submit(() -> arriveAndWaitForPeers(allRunning)));

    Assert.assertTrue(
        "a partition never got a thread of its own",
        allRunning.await(TIMEOUT.toSeconds(), TimeUnit.SECONDS));
    CompletableFuture.allOf(scans.toArray(CompletableFuture[]::new)).join();
    executor.shutdown();
  }

  /** A stack dump of a stuck sync has to name which run is holding the thread. */
  @Test
  public void submitRunsScansOnThreadsNamedForTheRun() {
    PartitionPoolExecutor executor = executor(2);
    Set<String> threadNames = ConcurrentHashMap.newKeySet();

    executor
        .submit(
            () -> {
              threadNames.add(Thread.currentThread().getName());
              return result();
            })
        .join();
    executor.shutdown();

    Assert.assertEquals(Set.of(POOL_NAME + "-0"), threadNames);
  }

  /** The scan's own failure is what the dispatcher reports, so it has to survive the hand-off. */
  @Test
  public void submitCompletesExceptionallyWithWhateverTheScanThrew() {
    PartitionPoolExecutor executor = executor(1);
    InitialSyncException failure = InitialSyncException.createRequiresResync("mocked failure");

    CompletableFuture<ParallelCollectionScanner.Result> scan =
        executor.submit(
            () -> {
              throw failure;
            });
    Throwable thrown = Assert.assertThrows(Exception.class, scan::join).getCause();
    executor.shutdown();

    Assert.assertSame(failure, thrown);
  }

  /** Nothing else reclaims these threads, so shutting the executor down has to end them. */
  @Test
  public void shutdownEndsTheThreads() {
    PartitionPoolExecutor executor = executor(2);
    executor.submit(PartitionPoolExecutorTest::result).join();

    executor.shutdown();

    Assert.assertEquals(Set.of(), survivingPoolThreads());
  }

  @Test
  public void shutdownRemovesTheRunsMeters() {
    PartitionPoolExecutor executor = executor(1);
    executor.submit(PartitionPoolExecutorTest::result).join();
    Assert.assertNotEquals(Set.of(), metersNamedForThePool());

    executor.shutdown();

    Assert.assertEquals(Set.of(), metersNamedForThePool());
  }

  /** A task submitted after shutdown must fail loudly rather than be silently dropped. */
  @Test
  public void submitAfterShutdownIsRejected() {
    PartitionPoolExecutor executor = executor(1);
    executor.shutdown();

    Assert.assertThrows(
        RejectedExecutionException.class, () -> executor.submit(PartitionPoolExecutorTest::result));
  }

  private Set<String> metersNamedForThePool() {
    return this.meterRegistry.getMeters().stream()
        .map(meter -> meter.getId().getName())
        .filter(name -> name.startsWith(POOL_NAME))
        .collect(Collectors.toSet());
  }

  /** Blocks like a partition scanning its window would, until every peer has a thread too. */
  private static ParallelCollectionScanner.Result arriveAndWaitForPeers(CountDownLatch allRunning) {
    allRunning.countDown();
    Instant limit = Instant.now().plus(TIMEOUT);
    while (allRunning.getCount() > 0 && Instant.now().isBefore(limit)) {
      LockSupport.parkNanos(Duration.ofMillis(1).toNanos());
    }
    return result();
  }

  /**
   * A worker signals pool termination on its way out, so it can still be alive the instant shutdown
   * returns. Give the threads a moment to actually die before reporting the survivors.
   */
  private static Set<String> survivingPoolThreads() {
    Instant limit = Instant.now().plus(TIMEOUT);
    while (Instant.now().isBefore(limit) && !threadsNamedForThePool().isEmpty()) {
      LockSupport.parkNanos(Duration.ofMillis(1).toNanos());
    }
    return threadsNamedForThePool();
  }

  private static Set<String> threadsNamedForThePool() {
    return Thread.getAllStackTraces().keySet().stream()
        .map(Thread::getName)
        .filter(name -> name.startsWith(POOL_NAME))
        .collect(Collectors.toSet());
  }
}
