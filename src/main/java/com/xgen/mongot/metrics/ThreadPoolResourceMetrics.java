package com.xgen.mongot.metrics;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.flogger.FluentLogger;
import com.sun.management.ThreadMXBean;
import com.xgen.mongot.util.concurrent.LiveThreadIdsRegistry;
import com.xgen.mongot.util.concurrent.NamedExecutorService;
import io.micrometer.core.instrument.FunctionCounter;
import io.micrometer.core.instrument.MeterRegistry;
import java.lang.management.ManagementFactory;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
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
 * <p>Used to attribute heap allocation rate and CPU consumption to a specific workload ({@code
 * blocking-server-worker} vs {@code guaranteed-blocking-server-worker} vs {@code concurrent-search}
 * for query workloads, and similarly for replication-initialization, Lucene merges, and other
 * thread sources that do not live behind a {@link NamedExecutorService}).
 *
 * <p>Per-thread allocation tracking is a JVM-wide concept that does not include direct {@link
 * java.nio.ByteBuffer} or native (JNI) allocations. It only counts objects allocated into the Java
 * heap.
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

  // One stable binding per pool, so that rebuilding a pool rebinds its existing counters instead
  // of registering duplicate meters.
  private final ConcurrentMap<String, PoolBinding> bindingsByPool = new ConcurrentHashMap<>();

  private final SubsystemResourceHistory resourceHistory;

  private ThreadPoolResourceMetrics(
      Optional<ThreadMXBean> threadMxBean,
      String subsystem,
      SubsystemResourceHistory resourceHistory) {
    this.threadMxBean = threadMxBean;
    this.subsystem = subsystem;
    this.resourceHistory = resourceHistory;
  }

  /**
   * Creates an instance backed by the platform {@link java.lang.management.ThreadMXBean}, scoped by
   * {@code subsystem}. Obtain one from {@link Bindings#forSubsystem}, which keeps it alive across
   * pool rebuilds.
   */
  static ThreadPoolResourceMetrics create(
      String subsystem, SubsystemResourceHistory resourceHistory) {
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
        return new ThreadPoolResourceMetrics(mxBean, subsystem, resourceHistory);
      } catch (UnsupportedOperationException | SecurityException e) {
        LOG.warn("Per-thread allocation/CPU tracking unsupported on this JVM, counters will be 0.");
      }
    } else {
      LOG.warn(
          "Per-thread allocation or CPU tracking is unavailable on this JVM, counters will be 0.");
    }

    return new ThreadPoolResourceMetrics(Optional.empty(), subsystem, resourceHistory);
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
   * {@link LiveThreadIdsRegistry#remove(long)}. The consumer removes ids from the registry when the
   * JVM MXBean reports them dead, keeping the resulting counters monotonic.
   */
  public void register(
      String name, LiveThreadIdsRegistry liveThreadIds, MeterRegistry meterRegistry) {
    if (this.threadMxBean.isEmpty()) {
      return;
    }
    ThreadMXBean bean = this.threadMxBean.get();

    // The map only hands out the binding. Registering meters and sweeping the outgoing pool's
    // threads happen under the binding.
    var recorder = this.resourceHistory.recorderFor(this.subsystem, name);
    var created = new PoolBinding(liveThreadIds, bean, recorder);
    Optional.ofNullable(this.bindingsByPool.putIfAbsent(name, created))
        .ifPresentOrElse(
            existing -> existing.rebind(liveThreadIds),
            () -> created.register(meterRegistry, this.subsystem, name));
  }

  private static void logUnsupportedExecutor(NamedExecutorService executor) {
    FLOGGER.atWarning().atMostEvery(1, TimeUnit.HOURS).log(
        "Per-thread attribution unavailable for executor '%s', counter will report 0.",
        executor.getName());
  }

  /**
   * Process-wide owner of the per-pool attribution state behind {@link ThreadPoolResourceMetrics}.
   *
   * <p>A pool's counters have to outlive the object that registered them: rebuilding a pool
   * re-registers the same meter which causes metrics to be suppressed. Holding one {@link
   * ThreadPoolResourceMetrics} per subsystem fixes the issue.
   *
   * <p>Also owns the {@link SubsystemResourceHistory} that stores the rolling history of every pool
   * registered here.
   */
  public static final class Bindings {

    private final ConcurrentMap<String, ThreadPoolResourceMetrics> bySubsystem =
        new ConcurrentHashMap<>();

    private final SubsystemResourceHistory resourceHistory;

    public Bindings() {
      this(new SubsystemResourceHistory());
    }

    @VisibleForTesting
    Bindings(SubsystemResourceHistory resourceHistory) {
      this.resourceHistory = resourceHistory;
    }

    /**
     * Metrics for {@code subsystem}, created on first use and returned unchanged thereafter. The
     * subsystem is emitted as a meter tag, to avoid name conflicts between different pools.
     */
    public ThreadPoolResourceMetrics forSubsystem(String subsystem) {
      return this.bySubsystem.computeIfAbsent(
          subsystem, s -> ThreadPoolResourceMetrics.create(s, this.resourceHistory));
    }

    /**
     * The rolling history fed by every pool registered through these bindings. Holds current values
     * as soon as counters are scraped.
     */
    public SubsystemResourceHistory resourceHistory() {
      return this.resourceHistory;
    }

    /** Refreshes every pool and appends one aligned history sample. */
    public void recordResourceHistorySample() {
      for (ThreadPoolResourceMetrics metrics : this.bySubsystem.values()) {
        for (PoolBinding binding : metrics.bindingsByPool.values()) {
          // Corner case for locking: Since rebind() can swap the threadid pool underneath, ensure
          // both reads are atomic.
          synchronized (binding) {
            binding.allocatedBytesValue();
            binding.cpuValue();
          }
        }
      }
      this.resourceHistory.appendCurrentSlots();
    }
  }

  /**
   * Stable state behind one pool's two FunctionCounters. Rebinding preserves the counters' process-
   * lifetime monotonic totals while moving their live reads to a replacement pool.
   */
  private static final class PoolBinding {
    private final Registration bytesAllocated;
    private final Registration cpu;
    private final SubsystemResourceHistory.Recorder recorder;
    private LiveThreadIdsRegistry liveThreadIds;

    PoolBinding(
        LiveThreadIdsRegistry liveThreadIds,
        ThreadMXBean bean,
        SubsystemResourceHistory.Recorder recorder) {
      this.liveThreadIds = liveThreadIds;
      this.recorder = recorder;
      this.bytesAllocated = new Registration(bean::getThreadAllocatedBytes);
      this.cpu = new Registration(bean::getThreadCpuTime);
    }

    synchronized void register(MeterRegistry meterRegistry, String subsystem, String name) {
      FunctionCounter.builder(ALLOCATED_BYTES_METRIC, this, PoolBinding::allocatedBytesValue)
          .description(
              "Cumulative bytes allocated into the Java heap by all threads this source has ever"
                  + " created. Excludes direct ByteBuffers and native allocations.")
          .baseUnit("bytes")
          .tag(SUBSYSTEM_TAG, subsystem)
          .tag(NAME_TAG, name)
          .register(meterRegistry);

      FunctionCounter.builder(CPU_TIME_METRIC, this, PoolBinding::cpuValue)
          .description(
              "Cumulative on-CPU nanoseconds across all threads this source has ever created.")
          .baseUnit("nanoseconds")
          .tag(SUBSYSTEM_TAG, subsystem)
          .tag(NAME_TAG, name)
          .register(meterRegistry);
    }

    synchronized void rebind(LiveThreadIdsRegistry replacement) {
      if (this.liveThreadIds == replacement) {
        return;
      }

      this.bytesAllocated.retireCurrentPool(this.liveThreadIds);
      this.cpu.retireCurrentPool(this.liveThreadIds);
      this.liveThreadIds = replacement;
    }

    synchronized double allocatedBytesValue() {
      long value = this.bytesAllocated.value(this.liveThreadIds);
      this.recorder.recordAllocBytes(value);
      return value;
    }

    synchronized double cpuValue() {
      long value = this.cpu.value(this.liveThreadIds);
      this.recorder.recordCpuNanos(value);
      return value;
    }
  }

  /**
   * Holds the sum of values for a single MXBean channel (allocation bytes or CPU nanoseconds).
   * Holds the last observed bean value per live thread id and a retired-total accumulator for
   * threads the JVM has reported as dead.
   */
  private static final class Registration {

    // Function of [ThreadIds] -> [Bean values].
    private final Function<long[], long[]> beanReader;
    // Map of thread id -> last seen bean value.
    private final ConcurrentHashMap<Long, Long> lastSeen = new ConcurrentHashMap<>();
    private final AtomicLong retiredSum = new AtomicLong();

    Registration(Function<long[], long[]> beanReader) {
      this.beanReader = beanReader;
    }

    long value(LiveThreadIdsRegistry liveThreadIds) {
      refresh(liveThreadIds);
      return sumBeanValues();
    }

    void retireCurrentPool(LiveThreadIdsRegistry liveThreadIds) {
      refresh(liveThreadIds);
      this.retiredSum.set(sumBeanValues());
      this.lastSeen.clear();
    }

    /** Refresh the bean values. */
    private void refresh(LiveThreadIdsRegistry liveThreadIds) {
      long[] ids = liveThreadIds.stream().mapToLong(Long::longValue).toArray();
      if (ids.length > 0) {
        long[] beanValues;
        try {
          beanValues = this.beanReader.apply(ids);
        } catch (UnsupportedOperationException | SecurityException e) {
          return;
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
            liveThreadIds.remove(tid);
          } else {
            this.lastSeen.put(tid, val);
          }
        }
      }
    }

    private long sumBeanValues() {
      return this.retiredSum.get()
          + this.lastSeen.values().stream().mapToLong(Long::longValue).sum();
    }
  }
}
