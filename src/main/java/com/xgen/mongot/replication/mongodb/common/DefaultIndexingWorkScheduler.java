package com.xgen.mongot.replication.mongodb.common;

import com.xgen.mongot.index.FieldExceededLimitsException;
import com.xgen.mongot.replication.mongodb.common.IndexingWorkSchedulerFactory.IndexingStrategy;
import com.xgen.mongot.util.FutureUtils;
import com.xgen.mongot.util.concurrent.NamedExecutorService;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * The default indexing work scheduler accepts indexing work and schedules it to be completed on an
 * executor. Exclusively indexes batches, does not do any additional work.
 */
final class DefaultIndexingWorkScheduler extends IndexingWorkScheduler {

  DefaultIndexingWorkScheduler(NamedExecutorService executor) {
    super(executor, IndexingStrategy.DEFAULT);
  }

  /**
   * Creates and starts a new DefaultIndexingWorkScheduler.
   *
   * @return a DefaultIndexingWorkScheduler.
   */
  public static DefaultIndexingWorkScheduler create(NamedExecutorService executor) {
    DefaultIndexingWorkScheduler scheduler = new DefaultIndexingWorkScheduler(executor);
    scheduler.start();
    return scheduler;
  }

  @Override
  CompletableFuture<Void> getBatchTasksFuture(IndexingSchedulerBatch batch) {
    return FutureUtils.allOf(
        batch.events.stream()
            .map((doc) -> new IndexingTask(batch.indexer, doc))
            .map(
                (task) ->
                    FutureUtils.checkedRunAsync(
                        task, this.executor, FieldExceededLimitsException.class))
            .collect(Collectors.toList()));
  }

  @Override
  void handleBatchException(IndexingSchedulerBatch batch, Throwable throwable) {
    batch.future.completeExceptionally(throwable);
  }
}
