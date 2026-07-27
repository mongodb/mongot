package com.xgen.mongot.util.concurrent;

import com.google.errorprone.annotations.Var;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.AbstractQueue;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * A {@link BlockingQueue} that replaces the default FIFO queue in a thread pool, giving callers a
 * high/low priority scheme.
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
 * empty-queue fallback paths. When a queue intermittently empties, the actual ratio of served tasks
 * can favor preferred tasks more than configured; this is intentional.
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
  private final Counter tasksRoutedToPreferredQueue;
  private final Counter tasksRoutedToRegularQueue;

  public DualBlockingQueue(
      int preferredTasksPerRegularTask, String name, MeterRegistry meterRegistry) {
    Objects.requireNonNull(name, "name");
    Objects.requireNonNull(meterRegistry);
    if (preferredTasksPerRegularTask <= 0) {
      throw new IllegalArgumentException(
          "preferredTasksPerRegularTask must be positive, got: " + preferredTasksPerRegularTask);
    }
    this.preferredTasksPerRegularTask = preferredTasksPerRegularTask;
    this.preferredQueueTasksServed = meterRegistry.counter(name + ".preferredQueueTasksServed");
    this.regularQueueTasksServed = meterRegistry.counter(name + ".regularQueueTasksServed");
    this.tasksRoutedToPreferredQueue = meterRegistry.counter(name + ".tasksRoutedToPreferredQueue");
    this.tasksRoutedToRegularQueue = meterRegistry.counter(name + ".tasksRoutedToRegularQueue");
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
        this.tasksRoutedToPreferredQueue.increment();
      } else {
        this.regularQueue.add(r);
        this.tasksRoutedToRegularQueue.increment();
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
    // The queue is unbounded, so a timed offer never needs to wait for capacity; it behaves exactly
    // like offer(r) and returns without blocking, regardless of the timeout.
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
   * the preferred queue is served and the counter is incremented; once the ratio is reached, a
   * regular task is served and the counter resets to 0. When the queue that would normally be
   * served is empty, the other queue is served as a fallback so a worker is never idle.
   */
  @GuardedBy("lock")
  private Runnable selectNext() {
    if (this.consecutiveHighPriorityCount >= this.preferredTasksPerRegularTask) {
      // Ratio reached: serve a regular task, resetting the counter. Falls through to a preferred
      // task when no regular task is waiting.
      this.consecutiveHighPriorityCount = 0;
      if (!this.regularQueue.isEmpty()) {
        this.regularQueueTasksServed.increment();
        return this.regularQueue.poll();
      }
    } else if (this.preferredQueue.isEmpty()) {
      // Under the ratio but no preferred task waiting: serve a regular task.
      this.consecutiveHighPriorityCount = 0;
      this.regularQueueTasksServed.increment();
      return this.regularQueue.poll();
    }

    // Serve a preferred task: either we are under the ratio, or the regular queue was empty. At
    // least one queue is non-empty, so the preferred queue is guaranteed to have a task here.
    this.consecutiveHighPriorityCount++;
    this.preferredQueueTasksServed.increment();
    return this.preferredQueue.poll();
  }

  @Override
  public @Nullable Runnable peek() {
    this.lock.lock();
    try {
      if (this.consecutiveHighPriorityCount >= this.preferredTasksPerRegularTask) {
        Runnable regular = this.regularQueue.peek();
        return regular != null ? regular : this.preferredQueue.peek();
      }
      Runnable preferred = this.preferredQueue.peek();
      return preferred != null ? preferred : this.regularQueue.peek();
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
   * Returns an iterator over a snapshot of the queue contents at the time of this call. {@code
   * remove()} is unsupported (it would only mutate the snapshot, not the backing queues); use
   * {@link #remove(Object)} to remove an element. Nothing in the executor path relies on
   * iterator-based removal: {@link AbstractQueue#clear()} is implemented via {@code poll()}, {@code
   * shutdownNow()} drains via {@link #drainTo(Collection)}, and {@link
   * java.util.concurrent.ThreadPoolExecutor#purge()} only removes {@code Future}s — which never
   * enter this queue, since {@link #offer(Runnable)} accepts only {@link PrioritizedTask}.
   */
  @Override
  public @NotNull Iterator<Runnable> iterator() {
    this.lock.lock();
    try {
      ArrayList<Runnable> snapshot =
          new ArrayList<>(this.preferredQueue.size() + this.regularQueue.size());
      snapshot.addAll(this.preferredQueue);
      snapshot.addAll(this.regularQueue);
      // Unmodifiable so remove() throws UnsupportedOperationException rather than silently mutating
      // only the snapshot.
      return Collections.unmodifiableList(snapshot).iterator();
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
