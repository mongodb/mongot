package com.xgen.mongot.util.concurrent;

import io.micrometer.core.instrument.MeterRegistry;
import java.util.Objects;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Generic priority-queue executor with no caller-specific scheduling logic: callers decide {@code
 * isHighPriority} themselves.
 *
 * <p>Uses a {@link DualBlockingQueue} as the backing queue for the thread pool. Tasks are wrapped
 * in a {@link PrioritizedTask} and routed to the preferred or regular queue based on the
 * caller-supplied priority flag.
 *
 * <p>Thread-safe: all fields are final and set at construction; concurrency is delegated to the
 * {@link ThreadPoolExecutor} and {@link DualBlockingQueue}.
 */
public final class DualQueueExecutor {

  private final ThreadPoolExecutor rawExecutor;
  private final NamedExecutorService delegate;

  public DualQueueExecutor(
      String name, int numThreads, MeterRegistry meterRegistry, int preferredTasksPerRegularTask) {
    Objects.requireNonNull(name, "name");
    Objects.requireNonNull(meterRegistry, "meterRegistry");
    if (numThreads <= 0) {
      throw new IllegalArgumentException("numThreads must be positive, got: " + numThreads);
    }
    if (preferredTasksPerRegularTask <= 0) {
      throw new IllegalArgumentException(
          "preferredTasksPerRegularTask must be positive, got: " + preferredTasksPerRegularTask);
    }
    DualBlockingQueue queue =
        new DualBlockingQueue(preferredTasksPerRegularTask, name, meterRegistry);
    ThreadFactory threadFactory = Executors.countingNamedThreadFactory(name);
    this.rawExecutor =
        new ThreadPoolExecutor(numThreads, numThreads, 0L, TimeUnit.SECONDS, queue, threadFactory);
    this.delegate = Executors.namedExecutor(name, this.rawExecutor, meterRegistry);
  }

  /**
   * Enqueues a task for execution and reports whether it was accepted. If the task cannot be
   * accepted (for example, after {@link #shutdown}), {@code onDropped} is invoked rather than
   * throwing.
   *
   * @param task the work to run
   * @param isHighPriority {@code true} to route to the preferred queue, {@code false} for regular
   * @param onDropped invoked if the task cannot be accepted, to release resources
   * @return {@code true} if the task was accepted for execution, {@code false} if it was dropped
   */
  public boolean enqueue(Runnable task, boolean isHighPriority, Runnable onDropped) {
    Objects.requireNonNull(task, "task");
    Objects.requireNonNull(onDropped, "onDropped");
    try {
      this.rawExecutor.execute(new PrioritizedTask(task, isHighPriority));
      return true;
    } catch (RejectedExecutionException ex) {
      // The queue is unbounded, so rejection only happens after shutdown. Hand the task back to the
      // caller via onDropped so it can release resources; this is an expected part of shutdown,
      // not a failure, so it is intentionally not logged.
      onDropped.run();
      return false;
    }
  }

  /**
   * Returns the executor wrapper for lifecycle/metrics registration ONLY. Do not submit tasks
   * through it: the returned {@link NamedExecutorService} wraps each task in a timed runnable,
   * which would fail {@link DualBlockingQueue}'s {@code PrioritizedTask}-only contract (see
   * TODO(CLOUDP-410283)) and throw at enqueue time. All task submission must go through {@link
   * #enqueue}.
   */
  public NamedExecutorService getDelegate() {
    return this.delegate;
  }

  public void shutdown() {
    Executors.shutdownOrFail(this.delegate);
  }
}
