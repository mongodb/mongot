package com.xgen.mongot.replication.mongodb.initialsync;

import com.xgen.mongot.replication.mongodb.common.InitialSyncException;
import com.xgen.mongot.util.FutureUtils;
import com.xgen.mongot.util.concurrent.Executors;
import com.xgen.mongot.util.concurrent.NamedExecutorService;
import com.xgen.mongot.util.functionalinterfaces.CheckedSupplier;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link CollectionScanTaskExecutor} backed by a pool dedicated to one sync, with a thread per
 * partition.
 */
final class PartitionPoolExecutor implements CollectionScanTaskExecutor {

  private static final Logger LOG = LoggerFactory.getLogger(PartitionPoolExecutor.class);

  /** Window for a stopping worker to return from its batch and record where it landed. */
  private static final Duration DRAIN_TIMEOUT = Duration.ofSeconds(30);

  private final NamedExecutorService pool;

  PartitionPoolExecutor(String poolName, int partitionCount, MeterRegistry meterRegistry) {
    this.pool = Executors.fixedSizeThreadPool(poolName, partitionCount, meterRegistry);
  }

  @Override
  public CompletableFuture<ParallelCollectionScanner.Result> submit(
      CheckedSupplier<ParallelCollectionScanner.Result, InitialSyncException> scan) {
    return FutureUtils.checkedSupplyAsync(scan, this.pool, InitialSyncException.class);
  }

  /**
   * Deliberately not {@code Executors.shutdownOrFail}, which never interrupts: it escalates a
   * missed deadline straight to halting the JVM, where this pool interrupts to release its threads.
   */
  @Override
  public void shutdown() {
    this.pool.shutdown();
    try {
      if (this.pool.awaitTermination(DRAIN_TIMEOUT.toNanos(), TimeUnit.NANOSECONDS)) {
        return;
      }
      LOG.atWarn()
          .addKeyValue("executorName", this.pool.getName())
          .log("Timed out draining the partition pool, interrupting it.");
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    this.pool.shutdownNow();
  }
}
