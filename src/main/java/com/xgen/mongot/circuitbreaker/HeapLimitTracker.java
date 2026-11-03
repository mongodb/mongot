package com.xgen.mongot.circuitbreaker;

import com.google.common.annotations.VisibleForTesting;
import com.sun.management.GarbageCollectionNotificationInfo;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryType;
import java.lang.management.MemoryUsage;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import javax.management.Notification;
import javax.management.NotificationEmitter;
import javax.management.openmbean.CompositeData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The max allowed heap for reservations is derived from live untracked heap. The reservations are
 * checked against it to determine if circuit breaker should be triggered.
 *
 * <p>Untracked heap is everything the breaker does not count per query: indexing, the facet caches,
 * Lucene internals, and query types it does not instrument yet. It cannot be configured, because it
 * differs per node and moves with the workload, so it is measured instead.
 *
 * <p>Each GC reports the heap it left behind. Paired with the reserved total read at that same
 * instant, it gives one untracked sample with both numbers from the same moment. The lowest sample
 * over a sliding window is the estimate: garbage only shrinks at a collection, so the low point is
 * closest to live data, and a young GC's high reading cannot raise it.
 *
 * <p><code> maxReservationAllowed = max heap - untracked - margin</code> It is never recomputed
 * later from an old heap reading and the current reserved bytes.
 *
 * <p>Between GCs, untracked growth is not seen. The margin covers that gap.
 */
public final class HeapLimitTracker {

  private static final Logger LOG = LoggerFactory.getLogger(HeapLimitTracker.class);

  static final long WINDOW_MS = 60_000;

  /**
   * Caps the window so a burst of GCs cannot grow it without bound. Dropping the oldest sample can
   * only raise the windowed minimum, which lowers the limit.
   */
  static final int MAX_WINDOW_SAMPLES = 1_000;

  /** The JVM's maximum heap size. */
  private final long maxHeapBytes;

  /** The share of max heap kept free for untracked growth between GCs. */
  private final long marginBytes;

  /** Bytes reserved by all running queries, shared with the registry that updates it. */
  private final AtomicLong totalReservedBytes;

  private final CircuitBreakerMetrics metrics;

  /**
   * The limit: the most all running queries together may reserve.
   *
   * <p>{@code limit = maxHeap - untracked - margin}. The breaker activates when {@code
   * totalReservedBytes} would pass it: a reservation that would cross it is refused, and once the
   * total is over it, the largest query cancels itself at its next checkpoint.
   */
  private final AtomicLong maxReservationAllowedBytes;

  /** One entry per GC inside the window: the untracked sample and when it was taken. */
  private final Deque<Sample> window = new ArrayDeque<>();

  /** How many GCs have been sampled. Used to allow at most one query cancellation per GC. */
  private final AtomicLong sampledGenerationCount = new AtomicLong();

  /** The latest generation for which a cancellation has already been done. */
  private final AtomicLong cancelClaimedGeneration = new AtomicLong(-1);

  /** The reserved total read at the previous GC sample. */
  private long reservedAtPreviousSample;

  /** Names of the memory pools that make up the heap, so non-heap pools are not counted. */
  private static final Set<String> HEAP_POOL_NAMES =
      ManagementFactory.getMemoryPoolMXBeans().stream()
          .filter(pool -> pool.getType() == MemoryType.HEAP)
          .map(MemoryPoolMXBean::getName)
          .collect(Collectors.toUnmodifiableSet());

  private record Sample(long untrackedBytes, long atMs) {}

  /**
   * Seeds untracked heap from the heap in use now, then listens for GCs. The listener is installed
   * after construction so a GC notification never sees a partly built tracker.
   */
  static HeapLimitTracker create(
      long maxHeapBytes,
      double marginFraction,
      AtomicLong totalReservedBytes,
      CircuitBreakerMetrics metrics) {
    HeapLimitTracker tracker =
        new HeapLimitTracker(
            maxHeapBytes,
            marginFraction,
            totalReservedBytes,
            metrics,
            ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed());
    tracker.listenForGcNotifications();
    return tracker;
  }

  /** Test seam: no JVM listener, and the seed untracked heap is supplied rather than read. */
  @VisibleForTesting
  HeapLimitTracker(
      long maxHeapBytes,
      double marginFraction,
      AtomicLong totalReservedBytes,
      CircuitBreakerMetrics metrics,
      long seedUntrackedBytes) {
    this.maxHeapBytes = maxHeapBytes;
    this.marginBytes = Math.round(marginFraction * maxHeapBytes);
    this.totalReservedBytes = totalReservedBytes;
    this.metrics = metrics;
    // Until the first GC, all heap in use counts as untracked: no query has reserved anything yet,
    // and the reading includes garbage, which overstates untracked heap and lowers the limit.
    // The first GC sample replaces this estimate.
    long seed = Math.max(0, seedUntrackedBytes);
    this.maxReservationAllowedBytes = new AtomicLong(computeMaxReservationAllowedBytes(seed));
    this.reservedAtPreviousSample = totalReservedBytes.get();
    // Right before the first GC, and when no collector emits GC notifications.
    metrics.recordUntrackedBytes(seed);
    metrics.recordEffectiveLimitBytes(this.maxReservationAllowedBytes.get());
  }

  /** Max allowed bytes that the reservations are checked against. */
  long getMaxReservationAllowedBytes() {
    return this.maxReservationAllowedBytes.get();
  }

  /**
   * Returns true for the first caller since the last GC, allowing it to cancel one query, and false
   * for every caller after that until the next GC.
   *
   * <p>Untracked heap growth can push running queries over the {@code
   * HeapLimitTracker.maxReservationAllowedBytes} without them allocating anything. Cancelling one
   * query per GC stops checkpoints from cancelling queries one after another until the next GC
   * recomputes the {@code HeapLimitTracker.maxReservationAllowedBytes}.
   */
  boolean tryClaimCancellationForCurrentGc() {
    long generation = this.sampledGenerationCount.get();
    long claimed = this.cancelClaimedGeneration.get();
    // >= so a caller holding a stale generation cannot move the claim backward.
    if (claimed >= generation) {
      return false;
    }
    return this.cancelClaimedGeneration.compareAndSet(claimed, generation);
  }

  private long computeMaxReservationAllowedBytes(long untrackedBytes) {
    return Math.max(0, this.maxHeapBytes - untrackedBytes - this.marginBytes);
  }

  private void listenForGcNotifications() {
    for (GarbageCollectorMXBean bean : ManagementFactory.getGarbageCollectorMXBeans()) {
      if (bean instanceof NotificationEmitter emitter) {
        try {
          emitter.addNotificationListener(this::updateLimitFromGcNotification, null, null);
        } catch (RuntimeException e) {
          LOG.atWarn()
              .addKeyValue("collector", bean.getName())
              .setCause(e)
              .log("Could not listen for GC notifications; the heap limit keeps its startup value");
        }
      }
    }
  }

  private void updateLimitFromGcNotification(Notification notification, Object handback) {
    if (!GarbageCollectionNotificationInfo.GARBAGE_COLLECTION_NOTIFICATION.equals(
        notification.getType())) {
      return;
    }
    if (!(notification.getUserData() instanceof CompositeData data)) {
      return;
    }
    GarbageCollectionNotificationInfo info = GarbageCollectionNotificationInfo.from(data);
    updateLimitFromGc(sumHeapPoolBytes(info.getGcInfo().getMemoryUsageAfterGc(), HEAP_POOL_NAMES));
  }

  /** The bytes in use across heap pools only, leaving out Metaspace and the code cache. */
  static long sumHeapPoolBytes(Map<String, MemoryUsage> usageByPool, Set<String> heapPoolNames) {
    return usageByPool.entrySet().stream()
        .filter(entry -> heapPoolNames.contains(entry.getKey()))
        .mapToLong(entry -> entry.getValue().getUsed())
        .sum();
  }

  /**
   * Runs the sampling for one GC that left {@code heapAfterGc} bytes in use. Bytes reserved after
   * the GC but before this runs are not in {@code heapAfterGc}, so the lower of the reserved total
   * now and at the previous sample is subtracted. That errs toward a lower limit, never a higher.
   */
  void updateLimitFromGc(long heapAfterGc) {
    long lowest;
    long limit;
    int samples;
    synchronized (this) {
      // Read inside the lock so samples enter the window in timestamp order; otherwise an older
      // sample appended after a newer one could never be popped from the front.
      long now = System.currentTimeMillis();
      long reservedNow = this.totalReservedBytes.get();
      long reserved = Math.min(reservedNow, this.reservedAtPreviousSample);
      this.reservedAtPreviousSample = reservedNow;
      long untracked = Math.max(0, heapAfterGc - reserved);
      this.window.addLast(new Sample(untracked, now));
      while (!this.window.isEmpty()
          && (now - this.window.peekFirst().atMs() > WINDOW_MS
              || this.window.size() > MAX_WINDOW_SAMPLES)) {
        this.window.removeFirst();
      }
      lowest = this.window.stream().mapToLong(Sample::untrackedBytes).min().orElse(untracked);
      limit = computeMaxReservationAllowedBytes(lowest);
      samples = this.window.size();
      this.maxReservationAllowedBytes.set(limit);
      // After the new limit is set, so a cancellation claimed for this GC sees that limit.
      this.sampledGenerationCount.incrementAndGet();
    }
    // Gauges are atomic, so they need no lock.
    this.metrics.recordUntrackedBytes(lowest);
    this.metrics.recordEffectiveLimitBytes(limit);
    this.metrics.recordUntrackedHeapSampleCount(samples);
  }
}
