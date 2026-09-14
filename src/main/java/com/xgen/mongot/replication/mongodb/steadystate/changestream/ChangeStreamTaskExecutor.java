package com.xgen.mongot.replication.mongodb.steadystate.changestream;

import org.bson.types.ObjectId;

/**
 * Executor abstraction used by {@link SyncChangeStreamDispatcher} to enqueue per-index getMore
 * tasks. Implementations decide how a submission is scheduled (immediate, delayed, queued by
 * priority, etc.) using the metadata in the supplied {@link EnqueueContext}.
 */
interface ChangeStreamTaskExecutor {

  /**
   * Enqueues a task for execution and reports whether it was accepted. If the task cannot be
   * accepted (for example, after {@link #shutdown}), the implementation invokes {@link
   * EnqueueContext#onDropped()} rather than throwing, so the dispatcher does not need to handle
   * rejection on the enqueue path.
   *
   * @param task the work to run
   * @param context per-submission metadata the executor may use to decide how to schedule the task
   * @return {@code true} if the task was accepted for execution, {@code false} if it was dropped
   */
  boolean enqueue(Runnable task, EnqueueContext context);

  /**
   * Notifies the executor that an index is starting up, before any task for it is enqueued, so
   * per-index scheduling state can be established up front rather than inferred from the first
   * submission. Executors with no per-index state can ignore this; it is a no-op by default.
   *
   * @param indexId the id of the index that is starting up
   */
  default void addIndex(ObjectId indexId) {}

  /**
   * Notifies the executor that an index has been removed, so any per-index scheduling state can be
   * discarded. This is the only signal that an index is gone: nothing prunes such state on its own,
   * so an executor that holds it keeps it for the lifetime of the process if this is never called.
   * Executors with no per-index state can ignore this; it is a no-op by default.
   *
   * @param indexId the id of the index that was removed
   */
  default void removeIndex(ObjectId indexId) {}

  /**
   * Shuts the executor down: stops accepting new tasks, lets already-submitted tasks run to
   * completion, then releases the worker threads and any internal scheduling resources.
   */
  void shutdown();
}
