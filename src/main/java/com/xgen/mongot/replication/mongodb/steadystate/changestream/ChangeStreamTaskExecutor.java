package com.xgen.mongot.replication.mongodb.steadystate.changestream;

/**
 * Executor abstraction used by {@link SyncChangeStreamDispatcher} to enqueue per-index getMore
 * tasks. Implementations decide how a submission is scheduled (immediate, delayed, queued by
 * priority, etc.) using the metadata in the supplied {@link EnqueueContext}.
 */
interface ChangeStreamTaskExecutor {

  /**
   * Enqueues a task for execution and reports whether it was accepted. If the task cannot be
   * accepted (for example, after {@link #shutdown}), the implementation invokes
   * {@link EnqueueContext#onDropped()} rather than throwing, so the dispatcher does not need to
   * handle rejection on the enqueue path.
   *
   * @param task the work to run
   * @param context per-submission metadata the executor may use to decide how to schedule the task
   * @return {@code true} if the task was accepted for execution, {@code false} if it was dropped
   */
  boolean enqueue(Runnable task, EnqueueContext context);

  /**
   * Shuts the executor down: stops accepting new tasks, lets already-submitted tasks run to
   * completion, then releases the worker threads and any internal scheduling resources.
   */
  void shutdown();
}
