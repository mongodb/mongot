package com.xgen.mongot.replication.mongodb.initialsync;

import com.xgen.mongot.replication.mongodb.common.InitialSyncException;
import com.xgen.mongot.util.functionalinterfaces.CheckedSupplier;
import java.util.concurrent.CompletableFuture;

/**
 * Executor abstraction used by {@link ParallelCollectionScanDispatcher} to run one task per
 * partition.
 */
interface CollectionScanTaskExecutor {

  /**
   * Runs one partition's scan on a thread of its own. The returned future completes exceptionally
   * with whatever the scan threw.
   */
  CompletableFuture<ParallelCollectionScanner.Result> submit(
      CheckedSupplier<ParallelCollectionScanner.Result, InitialSyncException> scan);

  /**
   * Stops accepting work at once, then waits for scans already in flight to return before releasing
   * the threads.
   */
  void shutdown();
}
