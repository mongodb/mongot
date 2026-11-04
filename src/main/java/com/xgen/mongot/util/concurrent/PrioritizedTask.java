package com.xgen.mongot.util.concurrent;

import java.util.Objects;

/**
 * A {@link Runnable} that carries priority metadata for routing by {@link DualBlockingQueue}.
 *
 * @param runnable the work to execute
 * @param isHighPriority true if the scheduler has determined this task should be routed to the
 *     high-priority queue
 * @param createdNanos {@link System#nanoTime()} when the task was created, used to report how long
 *     it has been waiting in the queue
 */
public record PrioritizedTask(Runnable runnable, boolean isHighPriority, long createdNanos)
    implements Runnable {

  public PrioritizedTask {
    Objects.requireNonNull(runnable);
  }

  public PrioritizedTask(Runnable runnable, boolean isHighPriority) {
    this(runnable, isHighPriority, System.nanoTime());
  }

  @Override
  public void run() {
    this.runnable.run();
  }
}
