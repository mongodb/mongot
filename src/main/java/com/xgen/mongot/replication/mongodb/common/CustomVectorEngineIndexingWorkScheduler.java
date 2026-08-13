package com.xgen.mongot.replication.mongodb.common;

import com.google.errorprone.annotations.Var;
import com.xgen.mongot.index.DocumentEvent;
import com.xgen.mongot.index.FieldExceededLimitsException;
import com.xgen.mongot.replication.mongodb.common.IndexingWorkSchedulerFactory.IndexingStrategy;
import com.xgen.mongot.util.FutureUtils;
import com.xgen.mongot.util.concurrent.NamedExecutorService;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Indexing work scheduler for indexes that use a custom vector engine. Performs batch preparation
 * (e.g. custom vector ending ID resolution) before indexing individual documents in the batch.
 */
final class CustomVectorEngineIndexingWorkScheduler extends IndexingWorkScheduler {

  CustomVectorEngineIndexingWorkScheduler(NamedExecutorService executor) {
    super(executor, IndexingStrategy.CUSTOM_VECTOR_ENGINE);
  }

  /**
   * Creates and starts a new CustomVectorEngineIndexingWorkScheduler.
   *
   * @return a CustomVectorEngineIndexingWorkScheduler.
   */
  public static CustomVectorEngineIndexingWorkScheduler create(NamedExecutorService executor) {
    CustomVectorEngineIndexingWorkScheduler scheduler =
        new CustomVectorEngineIndexingWorkScheduler(executor);
    scheduler.start();
    return scheduler;
  }

  /**
   * This setting enables publishing resume userdata to Vectorlite when each replication batch
   * completes.
   */
  @Override
  protected boolean shouldCommitOnFinalize() {
    return true;
  }

  @Override
  CompletableFuture<Void> getBatchTasksFuture(IndexingSchedulerBatch batch) {
    return CompletableFuture.supplyAsync(
            () -> batch.indexer.prepareBatch(batch.events), this.executor)
        // Index events one-at-a-time so we do not flood Vectorlite's single-threaded inbound
        // queue. Each thenCompose waits for the prior IndexingTask before submitting the next,
        // which applies backpressure from the VL worker into this scheduler.
        .thenCompose(preparedEvents -> indexPreparedEventsSequentially(batch, preparedEvents));
  }

  private CompletableFuture<Void> indexPreparedEventsSequentially(
      IndexingSchedulerBatch batch, List<DocumentEvent> preparedEvents) {
    @Var CompletableFuture<Void> chain = CompletableFuture.completedFuture(null);
    for (DocumentEvent event : preparedEvents) {
      chain =
          chain.thenCompose(
              ignored ->
                  FutureUtils.checkedRunAsync(
                      new IndexingTask(batch.indexer, event),
                      this.executor,
                      FieldExceededLimitsException.class));
    }
    return chain;
  }

  @Override
  void handleBatchException(IndexingSchedulerBatch batch, Throwable throwable) {
    batch.future.completeExceptionally(throwable);
  }
}
