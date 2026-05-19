package com.xgen.mongot.metrics;

import com.google.common.flogger.FluentLogger;
import com.sun.management.ThreadMXBean;
import com.xgen.mongot.util.concurrent.LiveThreadIdsRegistry;
import com.xgen.mongot.util.concurrent.NamedExecutorService;
import io.micrometer.core.instrument.FunctionCounter;
import io.micrometer.core.instrument.MeterRegistry;
import java.lang.management.ManagementFactory;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Per-pool ThreadMXBean attribution metrics. Sums heap allocation and CPU time across a caller-
 * provided collection of thread ids, exposing the totals as Micrometer counters tagged with the
 * pool name.
 *
 * <p>Used to attribute heap allocation rate and CPU consumption to a specific workload
 * ({@code blocking-server-worker} vs {@code guaranteed-blocking-server-worker} vs
 * {@code concurrent-search} for query workloads, and similarly for replication-initialization,
 * Lucene merges, and other thread sources that do not live behind a {@link NamedExecutorService}).
 *
 * <p>Per-thread allocation tracking is a JVM-wide concept that does not include direct
 * {@link java.nio.ByteBuffer} or native (JNI) allocations. It only counts objects allocated into
 * the Java heap.
 */
public final class ThreadPoolResourceMetrics {

  private static final Logger LOG = LoggerFactory.getLogger(ThreadPoolResourceMetrics.class);
  private static final FluentLogger FLOGGER = FluentLogger.forEnclosingClass();

  private static final String ALLOCATED_BYTES_METRIC = "executor.thread.allocatedBytes";
  private static final String CPU_TIME_METRIC = "executor.thread.cpuTime";
  private static final String NAME_TAG = "name";
  private static final String SUBSYSTEM_TAG = "subsystem";

  private final Optional<ThreadMXBean> threadMxBean;

  private final String subsystem;

  private ThreadPoolResourceMetrics(Optional<ThreadMXBean> threadMxBean, String subsystem) {
    this.threadMxBean = threadMxBean;
    this.subsystem = subsystem;
  }

  /**
   * Creates an instance backed by the platform {@link java.lang.management.ThreadMXBean}, scoped
   * by {@code subsystem}. The subsystem is emitted as a meter tag, so two callers using distinct
   * subsystems can register pools that share a name without colliding in the meter registry.
   */
  public static ThreadPoolResourceMetrics create(String subsystem) {
    Optional<ThreadMXBean> mxBean =
        (ManagementFactory.getThreadMXBean() instanceof ThreadMXBean s)
            ? Optional.of(s)
            : Optional.empty();

    if (mxBean.isPresent()
        && mxBean.get().isThreadAllocatedMemorySupported()
        && mxBean.get().isThreadCpuTimeSupported()) {
      try {
        // Enable both tracking knobs explicitly (though HotSpot enables them by default).
        mxBean.get().setThreadAllocatedMemoryEnabled(true);
        mxBean.get().setThreadCpuTimeEnabled(true);
        return new ThreadPoolResourceMetrics(mxBean, subsystem);
      } catch (UnsupportedOperationException | SecurityException e) {
        LOG.warn("Per-thread allocation/CPU tracking unsupported on this JVM, counters will be 0.");
      }
    } else {
      LOG.warn(
          "Per-thread allocation or CPU tracking is unavailable on this JVM, counters will be 0.");
    }

    return new ThreadPoolResourceMetrics(Optional.empty(), subsystem);
  }

  /**
   * Registers per-pool allocation and CPU-time counters for {@code executor}. Convenience overload
   * for thread sources that live behind a {@link NamedExecutorService}.
   */
  public void register(NamedExecutorService executor, MeterRegistry meterRegistry) {
    Optional<LiveThreadIdsRegistry> liveThreadIds = executor.getLiveThreadIdsRegistry();
    if (liveThreadIds.isEmpty()) {
      logUnsupportedExecutor(executor);
      return;
    }
    register(executor.getName(), liveThreadIds.get(), meterRegistry);
  }

  /**
   * Registers per-pool allocation and CPU-time counters for an arbitrary thread source. The
   * registry can be mutated by {@link LiveThreadIdsRegistry#register(long)} on thread birth and
   * {@link LiveThreadIdsRegistry#remove(long)}. The consumer removes ids from the registry when
   * the JVM MXBean reports them dead, keeping the resulting counters monotonic.
   */
  public void register(
      String name, LiveThreadIdsRegistry liveThreadIds, MeterRegistry meterRegistry) {
    if (this.threadMxBean.isEmpty()) {
      return;
    }
    ThreadMXBean bean = this.threadMxBean.get();
    Registration bytesAllocated = new Registration(liveThreadIds, bean::getThreadAllocatedBytes);
    Registration cpu = new Registration(liveThreadIds, bean::getThreadCpuTime);

    // Capture the Registration in the lambda because Micrometer takes a WeakReference.
    FunctionCounter.builder(
            ALLOCATED_BYTES_METRIC, bytesAllocated, ignored -> bytesAllocated.value())
        .description(
            "Cumulative bytes allocated into the Java heap by all threads this source has ever"
                + " created. Excludes direct ByteBuffers and native allocations.")
        .baseUnit("bytes")
        .tag(SUBSYSTEM_TAG, this.subsystem)
        .tag(NAME_TAG, name)
        .register(meterRegistry);

    FunctionCounter.builder(CPU_TIME_METRIC, cpu, ignored -> cpu.value())
        .description(
            "Cumulative on-CPU nanoseconds across all threads this source has ever created.")
        .baseUnit("nanoseconds")
        .tag(SUBSYSTEM_TAG, this.subsystem)
        .tag(NAME_TAG, name)
        .register(meterRegistry);
  }

  private static void logUnsupportedExecutor(NamedExecutorService executor) {
    FLOGGER
        .atWarning()
        .atMostEvery(1, TimeUnit.HOURS)
        .log(
            "Per-thread attribution unavailable for executor '%s', counter will report 0.",
            executor.getName());
  }

  /**
   * Holds the sum of values for a single MXBean channel (allocation bytes or CPU nanoseconds).
   * Holds the last observed bean value per live thread id and a retired-total accumulator for
   * threads the JVM has reported as dead.
   */
  private static final class Registration {

    private final LiveThreadIdsRegistry liveThreadIds;
    // Function of [ThreadIds] -> [Bean values].
    private final Function<long[], long[]> beanReader;
    // Map of thread id -> last seen bean value.
    private final ConcurrentHashMap<Long, Long> lastSeen = new ConcurrentHashMap<>();
    private final AtomicLong retiredSum = new AtomicLong();

    Registration(LiveThreadIdsRegistry liveThreadIds, Function<long[], long[]> beanReader) {
      this.liveThreadIds = liveThreadIds;
      this.beanReader = beanReader;
    }

    double value() {
      long[] ids = this.liveThreadIds.stream().mapToLong(Long::longValue).toArray();
      if (ids.length > 0) {
        long[] beanValues;
        try {
          beanValues = this.beanReader.apply(ids);
        } catch (UnsupportedOperationException | SecurityException e) {
          return sumBeanValues();
        }
        for (int i = 0; i < ids.length; i++) {
          long tid = ids[i];
          long val = beanValues[i];
          if (val < 0) {
            // Thread died. The atomic remove guarantees only the scrape that wins folds the
            // prior value into retiredSum, even if value() runs concurrently.
            Long last = this.lastSeen.remove(tid);
            if (last != null) {
              this.retiredSum.addAndGet(last);
            }
            this.liveThreadIds.remove(tid);
          } else {
            this.lastSeen.put(tid, val);
          }
        }
      }
      return sumBeanValues();
    }

    private long sumBeanValues() {
      return this.retiredSum.get()
          + this.lastSeen.values().stream().mapToLong(Long::longValue).sum();
    }
  }
}
