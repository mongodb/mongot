package com.xgen.mongot.index.lucene.directory;

import com.google.errorprone.annotations.Var;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

/**
 * Tracks how much wall-clock time cache-warmer prewarming spent running, across every index
 * partition.
 *
 * <p>Prewarming runs concurrently on the bounded index-initialization thread pool, so adding up
 * each thread's elapsed time multiply-counts the windows that overlap and reports far more than the
 * real elapsed time. This class instead maintains the union of the recorded windows and reports the
 * total time covered by at least one prewarm: overlapping windows are counted once, and idle gaps
 * between windows (for example, before warming an index created after startup warming finished) are
 * not counted at all.
 *
 * <p>The cache warmer is only enabled for a bounded period after host startup, so windows are only
 * recorded during that period and the set of recorded windows stays small.
 *
 * <p>Instances are safe for concurrent use by multiple initialization threads.
 */
public final class CacheWarmerTimer {

  /**
   * The disjoint union of the recorded windows, keyed by window start with the window end as the
   * value. Because the windows are pairwise disjoint, ordering by start also orders by end.
   */
  @GuardedBy("this")
  private final NavigableMap<Long, Long> windowEndsByStart;

  // Written only while holding "this"; volatile so gauge reads need not take the lock.
  private volatile long warmingNanos;

  public CacheWarmerTimer() {
    this.windowEndsByStart = new TreeMap<>();
    this.warmingNanos = 0L;
  }

  /**
   * Records the wall-clock window of a single prewarm, expressed as {@link System#nanoTime()}
   * readings with {@code startNanos <= endNanos}. May be called concurrently from multiple threads.
   */
  public synchronized void recordPrewarmWindow(long startNanos, long endNanos) {
    @Var long mergedStartNanos = startNanos;
    @Var long mergedEndNanos = endNanos;
    @Var long absorbedNanos = 0L;

    // Among the windows starting at or before the new one, only the closest can overlap or touch
    // it, because the windows are disjoint.
    Map.Entry<Long, Long> before = this.windowEndsByStart.floorEntry(mergedStartNanos);
    if (before != null && before.getValue() >= mergedStartNanos) {
      mergedStartNanos = before.getKey();
      mergedEndNanos = Math.max(mergedEndNanos, before.getValue());
      absorbedNanos += before.getValue() - before.getKey();
      this.windowEndsByStart.remove(before.getKey());
    }

    // Absorb the windows overlapping or touching the merged window from the right. Each recorded
    // window is absorbed at most once over the lifetime of the timer, so the amortized cost of a
    // call is logarithmic in the number of recorded windows.
    Iterator<Map.Entry<Long, Long>> after =
        this.windowEndsByStart.tailMap(mergedStartNanos, false).entrySet().iterator();
    while (after.hasNext()) {
      Map.Entry<Long, Long> window = after.next();
      if (window.getKey() > mergedEndNanos) {
        break;
      }
      mergedEndNanos = Math.max(mergedEndNanos, window.getValue());
      absorbedNanos += window.getValue() - window.getKey();
      after.remove();
    }

    this.windowEndsByStart.put(mergedStartNanos, mergedEndNanos);
    // A single volatile store, so concurrent readers never observe a partially-merged total.
    this.warmingNanos += (mergedEndNanos - mergedStartNanos) - absorbedNanos;
  }

  /**
   * Returns the total wall-clock milliseconds during which at least one prewarm was running, or
   * {@code 0} if no prewarm has been recorded yet.
   */
  public long warmingMilliseconds() {
    return TimeUnit.NANOSECONDS.toMillis(this.warmingNanos);
  }
}
