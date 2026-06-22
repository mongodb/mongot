package com.xgen.mongot.server.executors;

import java.util.Optional;
import java.util.OptionalInt;

/**
 * Configuration for the BulkheadCommandExecutor thread pool and queue.
 *
 * <p>Values are configured via CPU multipliers that scale with the number of available CPUs.
 * Defaults follow the design doc for query load shedding.
 *
 * <p>The {@code virtualQueueCapacity} flag controls queue behavior:
 *
 * <ul>
 *   <li>When {@code false} (default): queue capacity is enforced, tasks are rejected when full
 *   <li>When {@code true}: queue is unbounded, but wouldHaveRejected counter is recorded when
 *       virtual capacity is exceeded
 * </ul>
 */
public record RegularBlockingRequestSettings(
    double threadPoolSizeMultiplier, double queueCapacityMultiplier, boolean virtualQueueCapacity) {

  /** Returns default settings (Unbounded Caching mode - all values are 0/false). */
  public static RegularBlockingRequestSettings defaults() {
    return new RegularBlockingRequestSettings(0.0, 0.0, false);
  }

  public static RegularBlockingRequestSettings create(
      Optional<Double> threadPoolSizeMultiplier,
      Optional<Double> queueCapacityMultiplier,
      Optional<Boolean> virtualQueueCapacity) {
    double threadPoolParam = threadPoolSizeMultiplier.filter(param -> param > 0).orElse(0.0);
    double queueParam = queueCapacityMultiplier.filter(param -> param > 0).orElse(0.0);

    return new RegularBlockingRequestSettings(
        threadPoolParam, queueParam, virtualQueueCapacity.orElse(false));
  }

  public int resolvedPoolSize(int numCpus) {
    int cpuCount = Math.max(numCpus, 1);
    if (this.threadPoolSizeMultiplier > 0) {
      return Math.max((int) Math.ceil(this.threadPoolSizeMultiplier * cpuCount), 1);
    }
    return 1;
  }

  public int resolvedQueueCapacity(int numCpus) {
    int cpuCount = Math.max(numCpus, 1);
    if (this.queueCapacityMultiplier > 0) {
      return Math.max((int) Math.ceil(this.queueCapacityMultiplier * cpuCount), 1);
    }
    return 1;
  }

  public OptionalInt maybeResolvedQueueCapacity(int numCpus) {
    if (this.queueCapacityMultiplier <= 0) {
      return OptionalInt.empty();
    }
    return OptionalInt.of(resolvedQueueCapacity(numCpus));
  }

  /**
   * Determines the executor mode based on configuration.
   *
   * <p>Mode selection follows this logic:
   *
   * <ul>
   *   <li>UNBOUNDED_CACHING: no pool configuration (multiplier is 0)
   *   <li>FIXED_POOL_BOUNDED_QUEUE: pool + queue configured + virtualQueueCapacity=false
   *   <li>FIXED_POOL_UNBOUNDED_QUEUE: pool configured + (no queue OR virtualQueueCapacity=true)
   * </ul>
   */
  public Mode getMode() {
    boolean hasPoolConfig = this.threadPoolSizeMultiplier > 0;

    if (!hasPoolConfig) {
      return Mode.UNBOUNDED_CACHING;
    }

    boolean hasQueueConfig = this.queueCapacityMultiplier > 0;

    if (hasQueueConfig && !this.virtualQueueCapacity) {
      // Variant 2: Fixed Pool with Bounded Queue
      // Real queue limit that rejects tasks when full
      return Mode.FIXED_POOL_BOUNDED_QUEUE;
    }

    // Variant 1: Fixed Pool with Unbounded Queue
    // Either no queue config, or queue is virtual (just for wouldHaveRejected recording)
    return Mode.FIXED_POOL_UNBOUNDED_QUEUE;
  }

  /** Executor modes supported by BulkheadCommandExecutor. */
  public enum Mode {
    /** Unbounded caching thread pool (default behavior). */
    UNBOUNDED_CACHING,
    /** Fixed pool with unbounded queue, tracks virtual capacity for metrics. */
    FIXED_POOL_UNBOUNDED_QUEUE,
    /** Fixed pool with bounded queue, rejects tasks when full. */
    FIXED_POOL_BOUNDED_QUEUE
  }
}
