package com.xgen.mongot.replication.mongodb.steadystate.changestream;

import com.xgen.mongot.util.concurrent.DualBlockingQueue;
import com.xgen.mongot.util.concurrent.Executors;
import com.xgen.mongot.util.concurrent.NamedExecutorService;
import com.xgen.mongot.util.concurrent.PrioritizedTask;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.Objects;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * {@link ChangeStreamTaskExecutor} for the text/vector sync change-stream path.
 *
 * <p>Uses a {@link DualBlockingQueue} as the backing queue for the thread pool. Tasks are wrapped
 * in a {@link PrioritizedTask} and routed to the preferred or regular queue based on the
 * scheduler's priority signal.
 *
 * <p>Thread-safe: all fields are final and set at construction; concurrency is delegated to the
 * {@link ThreadPoolExecutor} and {@link DualBlockingQueue}.
 */
final class DualQueueExecutor implements ChangeStreamTaskExecutor {

  private static final int PREFERRED_TASKS_PER_REGULAR_TASK = 4;

  private final ThreadPoolExecutor rawExecutor;
  private final NamedExecutorService delegate;

  DualQueueExecutor(String name, int numThreads, MeterRegistry meterRegistry) {
    Objects.requireNonNull(name, "name");
    Objects.requireNonNull(meterRegistry, "meterRegistry");
    if (numThreads <= 0) {
      throw new IllegalArgumentException("numThreads must be positive, got: " + numThreads);
    }
    DualBlockingQueue queue =
        new DualBlockingQueue(PREFERRED_TASKS_PER_REGULAR_TASK, meterRegistry);
    ThreadFactory threadFactory = Executors.countingNamedThreadFactory(name);
    this.rawExecutor =
        new ThreadPoolExecutor(numThreads, numThreads, 0L, TimeUnit.SECONDS, queue, threadFactory);
    this.delegate = Executors.namedExecutor(name, this.rawExecutor, meterRegistry);
  }

  @Override
  public boolean enqueue(Runnable task, EnqueueContext context) {
    // No previous batch = first submission for this index; default to high priority so it starts
    // draining promptly instead of waiting behind the regular queue.
    boolean isHighPriority =
        context.previousBatch().map(PreviousBatchInfo::hadDocuments).orElse(true);
    try {
      this.rawExecutor.execute(new PrioritizedTask(task, isHighPriority));
      return true;
    } catch (RejectedExecutionException ex) {
      // The queue is unbounded, so rejection only happens after shutdown. Hand the task back to the
      // dispatcher via onDropped so it can release resources; this is an expected part of shutdown,
      // not a failure, so it is intentionally not logged.
      context.onDropped().run();
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
  NamedExecutorService getDelegate() {
    return this.delegate;
  }

  @Override
  public void shutdown() {
    Executors.shutdownOrFail(this.delegate);
  }
}
