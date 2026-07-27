package com.xgen.mongot.replication.mongodb.steadystate.changestream;

import com.xgen.mongot.util.concurrent.DualQueueExecutor;
import com.xgen.mongot.util.concurrent.NamedExecutorService;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.Objects;

/**
 * {@link ChangeStreamTaskExecutor} for the text/vector sync change-stream path.
 *
 * <p>Contains the change-stream-specific scheduling policy: it applies a {@link
 * ChangeStreamPriorityStrategy} to the submission's {@link EnqueueContext} to decide priority, then
 * delegates the actual queueing to a generic {@link DualQueueExecutor}.
 */
final class ChangeStreamDualQueueExecutor implements ChangeStreamTaskExecutor {

  private final DualQueueExecutor delegate;
  private final ChangeStreamPriorityStrategy strategy;

  ChangeStreamDualQueueExecutor(
      String name,
      int numThreads,
      MeterRegistry meterRegistry,
      ChangeStreamPriorityStrategy strategy,
      int preferredTasksPerRegularTask) {
    Objects.requireNonNull(strategy, "strategy");
    this.delegate =
        new DualQueueExecutor(name, numThreads, meterRegistry, preferredTasksPerRegularTask);
    this.strategy = strategy;
  }

  @Override
  public boolean enqueue(Runnable task, EnqueueContext context) {
    // No previous batch = first submission for this index; default to high priority so it starts
    // draining promptly instead of waiting behind the regular queue.
    boolean isHighPriority =
        context.previousBatch().map(this.strategy::isHighPriority).orElse(true);
    return this.delegate.enqueue(task, isHighPriority, context.onDropped());
  }

  /**
   * Returns the executor wrapper for lifecycle/metrics registration ONLY. Do not submit tasks
   * through it: the returned wrapper wraps each task in a timed runnable, which would fail the
   * backing queue's {@code PrioritizedTask}-only contract (see TODO(CLOUDP-410283)) and throw at
   * enqueue time. All task submission must go through {@link #enqueue}.
   */
  NamedExecutorService getDelegate() {
    return this.delegate.getDelegate();
  }

  @Override
  public void shutdown() {
    this.delegate.shutdown();
  }
}
