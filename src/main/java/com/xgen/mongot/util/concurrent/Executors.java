package com.xgen.mongot.util.concurrent;

import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.Crash;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.jvm.ExecutorServiceMetrics;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.lang3.Range;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Executors {

  private static final Logger LOG = LoggerFactory.getLogger(Executors.class);
  private static final Duration DEFAULT_EXECUTOR_SHUTDOWN_TIMEOUT = Duration.ofMinutes(1);

  /**
   * Creates a new NamedExecutorService with the supplied number of threads, operating off a shared
   * unbounded queue.
   *
   * @param name the name to use for the ExecutorService, used to prefix the name of Threads created
   *     by the ExecutorService
   * @param size the number of Threads to create for the ExecutorService
   * @param meterRegistry the MeterRegistry to register metrics with
   * @return the newly created thread pool
   */
  public static NamedExecutorService fixedSizeThreadPool(
      String name, int size, MeterRegistry meterRegistry) {
    Check.argIsPositive(size, "size");

    CountingNamedThreadFactory threadFactory = new CountingNamedThreadFactory(name);
    ExecutorService executor =
        java.util.concurrent.Executors.newFixedThreadPool(size, threadFactory);

    return new DefaultNamedExecutorService(
        ExecutorServiceMetrics.monitor(meterRegistry, executor, "executorMetrics", name),
        executor,
        name,
        meterRegistry,
        Optional.of(threadFactory));
  }

  /**
   * Creates a new NamedExecutorService with the supplied number of threads, queue capacity and the
   * rejected execution handler.
   *
   * @param name the name to use for the ExecutorService
   * @param poolSize the number of threads to create for the ExecutorService
   * @param queueSize the capacity of the thread pool queue, should be positive
   * @param handler the handler to use when execution is blocked due to queue capacity
   * @param meterRegistry the MeterRegistry to register metrics with
   * @return the newly created thread pool
   */
  public static NamedExecutorService fixedSizeThreadPool(
      String name,
      int poolSize,
      int queueSize,
      RejectedExecutionHandler handler,
      MeterRegistry meterRegistry) {
    Check.argIsPositive(poolSize, "poolSize");
    Check.argIsPositive(queueSize, "queueSize");
    CountingNamedThreadFactory threadFactory = new CountingNamedThreadFactory(name);
    ExecutorService executor =
        new ThreadPoolExecutor(
            poolSize,
            poolSize,
            0L,
            TimeUnit.SECONDS,
            new ArrayBlockingQueue<>(queueSize),
            threadFactory,
            handler);
    return new DefaultNamedExecutorService(
        ExecutorServiceMetrics.monitor(meterRegistry, executor, "executorMetrics", name),
        executor,
        name,
        meterRegistry,
        Optional.of(threadFactory));
  }

  /** Exposes a counting thread factory so other components can create named pools with counters. */
  public static ThreadFactory countingNamedThreadFactory(String name) {
    return new CountingNamedThreadFactory(name);
  }

  /**
   * Creates a new NamedExecutorService that supports an unbounded amount of parallelism.
   *
   * <p>When a task arrives, if there is an idle thread available in the pool it will be re-used,
   * otherwise a new thread will be created.
   *
   * <p>Idle threads are reaped after 60 seconds.
   *
   * @param name the name to use for the ExecutorService, used to prefix the name of Threads created
   *     by the ExecutorService
   * @param meterRegistry the MeterRegistry to register metrics with
   * @return the newly created thread pool
   */
  public static NamedExecutorService unboundedCachingThreadPool(
      String name, MeterRegistry meterRegistry) {
    CountingNamedThreadFactory threadFactory = new CountingNamedThreadFactory(name);
    ExecutorService executor =
        java.util.concurrent.Executors.newCachedThreadPool(threadFactory);

    return new DefaultNamedExecutorService(
        ExecutorServiceMetrics.monitor(meterRegistry, executor, "executorMetrics", name),
        executor,
        name,
        meterRegistry,
        Optional.of(threadFactory));
  }

  /**
   * Creates a new NamedScheduledExecutorService with a single thread.
   *
   * @param name the name to use for the ExecutorService, used as the name of the Thread created by
   *     the ExecutorService
   * @param size the number of Threads to create for the ExecutorService
   * @param meterRegistry the MeterRegistry try to register metrics with
   * @return the newly created scheduled executor
   */
  public static NamedScheduledExecutorService fixedSizeThreadScheduledExecutor(
      String name, int size, int priority, MeterRegistry meterRegistry) {
    CountingNamedThreadFactory threadFactory = new CountingNamedThreadFactory(name, priority);
    ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(size, threadFactory);
    executor.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);
    executor.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);

    return new DefaultNamedScheduledExecutorService(
        ExecutorServiceMetrics.monitor(meterRegistry, executor, "executorMetrics", name),
        name,
        meterRegistry,
        Optional.of(threadFactory));
  }

  public static NamedScheduledExecutorService fixedSizeThreadScheduledExecutor(
      String name, int size, MeterRegistry meterRegistry) {
    return fixedSizeThreadScheduledExecutor(name, size, Thread.NORM_PRIORITY, meterRegistry);
  }

  public static NamedScheduledExecutorService singleThreadScheduledExecutor(
      String name, int priority, MeterRegistry meterRegistry) {
    return fixedSizeThreadScheduledExecutor(name, 1, priority, meterRegistry);
  }

  public static NamedScheduledExecutorService singleThreadScheduledExecutor(
      String name, MeterRegistry meterRegistry) {
    return singleThreadScheduledExecutor(name, Thread.NORM_PRIORITY, meterRegistry);
  }

  public static NamedExecutorService namedExecutor(
      String name, ExecutorService executor, MeterRegistry meterRegistry) {
    // The supplied executor was built externally, so there is no thread factory to track its
    // threads.
    return new DefaultNamedExecutorService(
        ExecutorServiceMetrics.monitor(meterRegistry, executor, "executorMetrics", name),
        executor,
        name,
        meterRegistry,
        Optional.empty());
  }

  public static NamedScheduledExecutorService namedExecutor(
      String name, ScheduledExecutorService executor, MeterRegistry meterRegistry) {
    // The supplied executor was built externally, so there is no thread factory to track its
    // threads.
    return new DefaultNamedScheduledExecutorService(
        ExecutorServiceMetrics.monitor(meterRegistry, executor, "executorMetrics", name),
        name,
        meterRegistry,
        Optional.empty());
  }

  /**
   * Attempts to gracefully shut down the executor, failing if the executor has not shut down after
   * a minute.
   */
  public static void shutdownOrFail(NamedExecutorService executor) {
    shutdownOrFail(executor, DEFAULT_EXECUTOR_SHUTDOWN_TIMEOUT);
  }

  private static void shutdownOrFail(NamedExecutorService executor, Duration timeout) {
    LOG.atInfo().addKeyValue("executorName", executor.getName()).log("Shutting down executor.");

    executor.shutdown();
    boolean success =
        Crash.because(
                String.format("interrupted awaiting %s executor termination", executor.getName()))
            .withThreadDump()
            .ifThrows(() -> executor.awaitTermination(timeout.toNanos(), TimeUnit.NANOSECONDS));

    if (!success) {
      Crash.because(String.format("failed to shut down %s executor", executor.getName()))
          .withThreadDump()
          .now();
    }
  }

  private static class NamedThreadFactory implements ThreadFactory {

    private static final Range<Integer> PRIORITY_RANGE =
        Range.of(Thread.MIN_PRIORITY, Thread.MAX_PRIORITY);

    protected final String name;
    private final int priority;

    NamedThreadFactory(String name, int priority) {
      Check.checkState(
          PRIORITY_RANGE.contains(priority),
          "priority must be %s <= priority <= %s, is %s",
          Thread.MIN_PRIORITY,
          Thread.MAX_PRIORITY,
          priority);

      this.name = name;
      this.priority = priority;
    }

    NamedThreadFactory(String name) {
      this(name, Thread.NORM_PRIORITY);
    }

    protected String getName() {
      return this.name;
    }

    @Override
    public Thread newThread(Runnable runnable) {
      Thread thread = new Thread(runnable, getName());

      thread.setDaemon(false);
      thread.setPriority(this.priority);

      return thread;
    }
  }

  /**
   * Thread factory that names threads with an incrementing index and records the IDs of every
   * thread it creates in a {@link LiveThreadIdsRegistry}.
   */
  static class CountingNamedThreadFactory extends NamedThreadFactory {

    private final AtomicInteger threadCounter;
    private final LiveThreadIdsRegistry liveThreadIds = new LiveThreadIdsRegistry();

    CountingNamedThreadFactory(String name) {
      super(name);
      this.threadCounter = new AtomicInteger();
    }

    CountingNamedThreadFactory(String name, int priority) {
      super(name, priority);
      this.threadCounter = new AtomicInteger();
    }

    @Override
    protected String getName() {
      return String.format("%s-%d", this.name, this.threadCounter.getAndIncrement());
    }

    @Override
    public Thread newThread(Runnable runnable) {
      Thread thread = super.newThread(runnable);
      this.liveThreadIds.register(thread.threadId());
      return thread;
    }

    /** Returns the live thread id registry this factory writes to. */
    LiveThreadIdsRegistry getLiveThreadIdsRegistry() {
      return this.liveThreadIds;
    }
  }
}
