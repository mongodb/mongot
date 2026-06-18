package com.xgen.mongot.util.concurrent;

import com.google.errorprone.annotations.Var;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.AbstractQueue;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * A {@link BlockingQueue} that replaces the default FIFO queue in the change-stream thread pool.
 *
 * <p>Tasks are routed to a preferred or regular queue based on {@link
 * PrioritizedTask#isHighPriority()}. Worker threads pull {@code preferredTasksPerRegularTask} tasks
 * from the preferred queue before pulling one from the regular queue. If the preferred queue is
 * empty, the regular queue is used as a fallback.
 *
 * <p>This class is thread-safe: all access to the internal queues and {@code
 * consecutiveHighPriorityCount} is guarded by a single {@link ReentrantLock}, and blocking
 * operations ({@link #take()}, {@link #poll(long, TimeUnit)}) wait on the {@code notEmpty}
 * condition. It is intended to back a {@link java.util.concurrent.ThreadPoolExecutor} shared by
 * multiple worker threads.
 *
 * <p>The {@code preferredTasksPerRegularTask} ratio is approximate: {@code
 * consecutiveHighPriorityCount} resets to 0 whenever the regular queue is chosen, including the
 * empty-queue fallback paths. When a queue intermittently empties, the actual ratio of served
 * tasks can favor preferred tasks more than configured; this is intentional.
 */
public class DualBlockingQueue extends AbstractQueue<Runnable> implements BlockingQueue<Runnable> {

  @GuardedBy("lock")
  private final ArrayDeque<Runnable> preferredQueue = new ArrayDeque<>();

  @GuardedBy("lock")
  private final ArrayDeque<Runnable> regularQueue = new ArrayDeque<>();

  private final ReentrantLock lock = new ReentrantLock();
  private final Condition notEmpty = this.lock.newCondition();

  // Number of preferred (high-priority) tasks to serve in a row before serving one regular
  // (low-priority) task.
  private final int preferredTasksPerRegularTask;

  // Counter to track how many high-priority tasks have been served in a row.
  @GuardedBy("lock")
  private int consecutiveHighPriorityCount = 0;

  private final Counter preferredQueueTasksServed;
  private final Counter regularQueueTasksServed;

  public DualBlockingQueue(int preferredTasksPerRegularTask, MeterRegistry meterRegistry) {
    Objects.requireNonNull(meterRegistry);
    if (preferredTasksPerRegularTask <= 0) {
      throw new IllegalArgumentException(
          "preferredTasksPerRegularTask must be positive, got: " + preferredTasksPerRegularTask);
    }
    this.preferredTasksPerRegularTask = preferredTasksPerRegularTask;
    this.preferredQueueTasksServed =
        meterRegistry.counter("indexing.steadyStateChangeStream.preferredQueueTasksServed");
    this.regularQueueTasksServed =
        meterRegistry.counter("indexing.steadyStateChangeStream.regularQueueTasksServed");
  }

  @Override
  public boolean offer(Runnable r) {
    Objects.requireNonNull(r, "runnable task must not be null");
    this.lock.lock();
    try {
      // TODO(CLOUDP-410283): This contract forces DualQueueExecutor to submit through
      // rawExecutor.execute(...) rather than the NamedExecutorService wrapper returned by
      // Executors.namedExecutor(...). Routing through the wrapper would have TimedExecutorService
      // wrap PrioritizedTask in a TimedRunnable, which would fail this instanceof check and also
      // collapse priority routing (every task would look non-prioritized). Trade-off: we lose
      // Micrometer's per-task timers (executor.duration, executor.idle) for DualQueueExecutor.
      // Hence, DualBlockingQueue only accepts PrioritizedTasks.
      if (!(r instanceof PrioritizedTask prioritized)) {
        throw new IllegalArgumentException(
            "DualBlockingQueue only accepts PrioritizedTask, got: " + r.getClass());
      }
      if (prioritized.isHighPriority()) {
        this.preferredQueue.add(r);
      } else {
        this.regularQueue.add(r);
      }
      this.notEmpty.signal();
      return true;
    } finally {
      this.lock.unlock();
    }
  }

  @Override
  public boolean offer(Runnable r, long timeout, TimeUnit unit) {
    Objects.requireNonNull(unit, "unit");
    return offer(r);
  }

  @Override
  public Runnable take() throws InterruptedException {
    this.lock.lockInterruptibly();
    try {
      // Wait until there is at least one task in either queue
      while (this.preferredQueue.isEmpty() && this.regularQueue.isEmpty()) {
        this.notEmpty.await();
      }
      return selectNext();
    } finally {
      this.lock.unlock();
    }
  }

  @Override
  public @Nullable Runnable poll() {
    this.lock.lock();
    try {
      if (this.preferredQueue.isEmpty() && this.regularQueue.isEmpty()) {
        return null;
      }
      return selectNext();
    } finally {
      this.lock.unlock();
    }
  }

  @Override
  public @Nullable Runnable poll(long timeout, TimeUnit unit) throws InterruptedException {
    Objects.requireNonNull(unit);
    @Var long remainingNanos = unit.toNanos(timeout);
    this.lock.lockInterruptibly();
    try {
      while (this.preferredQueue.isEmpty() && this.regularQueue.isEmpty()) {
        if (remainingNanos <= 0L) {
          return null;
        }
        // awaitNanos returns the remaining wait time
        remainingNanos = this.notEmpty.awaitNanos(remainingNanos);
      }
      return selectNext();
    } finally {
      this.lock.unlock();
    }
  }

  /**
   * Selects the next task to run.
   *
   * <p>While {@code consecutiveHighPriorityCount} is below {@code preferredTasksPerRegularTask},
   * tasks are pulled from the preferred queue and the counter is incremented. Once the ratio is
   * reached, one regular task is served and the counter resets to 0. If the preferred queue is
   * empty, falls back to the regular queue so a worker is never idle while tasks are waiting.
   */
  @GuardedBy("lock")
  private Runnable selectNext() {
    if (this.consecutiveHighPriorityCount < this.preferredTasksPerRegularTask) {
      Runnable task = this.preferredQueue.poll();
      if (task != null) {
        this.consecutiveHighPriorityCount++;
        this.preferredQueueTasksServed.increment();
        return task;
      }
    } else {
      Runnable task = this.regularQueue.poll();
      if (task != null) {
        this.consecutiveHighPriorityCount = 0;
        this.regularQueueTasksServed.increment();
        return task;
      }
      this.consecutiveHighPriorityCount = 0;
    }

    // Fallback: if the preferred queue is empty, serve from whichever queue has work and update
    // the counter symmetrically: a fallback regular resets it to 0, a fallback preferred increments
    // it.
    @Var Runnable task = this.regularQueue.poll();
    if (task != null) {
      this.consecutiveHighPriorityCount = 0;
      this.regularQueueTasksServed.increment();
      return task;
    }
    task = this.preferredQueue.poll();
    if (task != null) {
      this.consecutiveHighPriorityCount++;
      this.preferredQueueTasksServed.increment();
    }
    return task;
  }

  @Override
  public @Nullable Runnable peek() {
    this.lock.lock();
    try {
      if (this.consecutiveHighPriorityCount < this.preferredTasksPerRegularTask) {
        Runnable task = this.preferredQueue.peek();
        if (task != null) {
          return task;
        }
      } else {
        Runnable task = this.regularQueue.peek();
        if (task != null) {
          return task;
        }
      }
      Runnable lowTask = this.regularQueue.peek();
      return lowTask != null ? lowTask : this.preferredQueue.peek();
    } finally {
      this.lock.unlock();
    }
  }

  @Override
  public void put(Runnable r) {
    offer(r);
  }

  @Override
  public int remainingCapacity() {
    return Integer.MAX_VALUE;
  }

  @Override
  public int size() {
    this.lock.lock();
    try {
      return this.preferredQueue.size() + this.regularQueue.size();
    } finally {
      this.lock.unlock();
    }
  }

  /**
   * Returns an iterator over a copy of the queue contents at the time of this call. {@code
   * iterator.remove()} does not affect the underlying queues.
   */
  @Override
  public @NotNull Iterator<Runnable> iterator() {
    this.lock.lock();
    try {
      ArrayList<Runnable> snapshot =
          new ArrayList<>(this.preferredQueue.size() + this.regularQueue.size());
      snapshot.addAll(this.preferredQueue);
      snapshot.addAll(this.regularQueue);
      return snapshot.iterator();
    } finally {
      this.lock.unlock();
    }
  }

  @Override
  public boolean remove(Object o) {
    this.lock.lock();
    try {
      return this.preferredQueue.remove(o) || this.regularQueue.remove(o);
    } finally {
      this.lock.unlock();
    }
  }

  @Override
  public int drainTo(@NotNull Collection<? super Runnable> c) {
    return drainTo(c, Integer.MAX_VALUE);
  }

  @Override
  public int drainTo(@NotNull Collection<? super Runnable> c, int maxElements) {
    Objects.requireNonNull(c);
    if (c == this) {
      throw new IllegalArgumentException("Cannot drain to self");
    }
    if (maxElements <= 0) {
      return 0;
    }
    this.lock.lock();
    try {
      @Var int count = 0;
      while (count < maxElements
          && !(this.preferredQueue.isEmpty() && this.regularQueue.isEmpty())) {
        @Var Runnable task = this.preferredQueue.poll();
        if (task == null) {
          task = this.regularQueue.poll();
        }
        if (task != null) {
          c.add(task);
          count++;
        }
      }
      return count;
    } finally {
      this.lock.unlock();
    }
  }
}
