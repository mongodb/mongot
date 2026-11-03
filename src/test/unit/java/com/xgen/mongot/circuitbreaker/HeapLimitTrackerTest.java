package com.xgen.mongot.circuitbreaker;

import static com.google.common.truth.Truth.assertThat;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.lang.management.MemoryUsage;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.Test;

public class HeapLimitTrackerTest {

  private static final long MIB = 1024L * 1024L;

  private final SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
  private final CircuitBreakerMetrics metrics = new CircuitBreakerMetrics(this.meterRegistry);
  private final AtomicLong total = this.metrics.totalReservedBytes();

  private HeapLimitTracker tracker(long seedUntrackedBytes) {
    return new HeapLimitTracker(100 * MIB, 0.0, this.total, this.metrics, seedUntrackedBytes);
  }

  private double gauge(String name) {
    return this.meterRegistry.get("circuitBreaker." + name).gauge().value();
  }

  @Test
  public void constructor_reportsStartupEstimateBeforeAnyGc() {
    tracker(30 * MIB);

    assertThat(gauge("untrackedBytes")).isEqualTo(30.0 * MIB);
    assertThat(gauge("effectiveLimitBytes")).isEqualTo(70.0 * MIB);
  }

  @Test
  public void sumHeapPoolBytes_countsOnlyHeapPools() {
    Map<String, MemoryUsage> usage =
        Map.of(
            "G1 Old Gen", new MemoryUsage(0, 40 * MIB, 40 * MIB, -1),
            "G1 Eden Space", new MemoryUsage(0, 10 * MIB, 10 * MIB, -1),
            "Metaspace", new MemoryUsage(0, 80 * MIB, 80 * MIB, -1));

    long bytes = HeapLimitTracker.sumHeapPoolBytes(usage, Set.of("G1 Old Gen", "G1 Eden Space"));

    assertThat(bytes).isEqualTo(50 * MIB);
  }

  @Test
  public void updateLimitFromGc_reservationsAfterTheGc_doNotShrinkUntrackedHeap() {
    HeapLimitTracker tracker = tracker(0);
    tracker.updateLimitFromGc(30 * MIB);

    // 20 MiB reserved after the next GC finished but before its notification ran.
    this.total.addAndGet(20 * MIB);
    tracker.updateLimitFromGc(30 * MIB);

    assertThat(gauge("untrackedBytes")).isEqualTo(30.0 * MIB);
  }

  @Test
  public void tryClaimCancellationForCurrentGc_oncePerGc() {
    HeapLimitTracker tracker = tracker(0);
    tracker.updateLimitFromGc(10 * MIB);

    assertThat(tracker.tryClaimCancellationForCurrentGc()).isTrue();
    assertThat(tracker.tryClaimCancellationForCurrentGc()).isFalse();

    tracker.updateLimitFromGc(10 * MIB);
    assertThat(tracker.tryClaimCancellationForCurrentGc()).isTrue();
  }

  @Test
  public void updateLimitFromGc_capsWindowAndDropsOldestSample() {
    HeapLimitTracker tracker = tracker(0);
    // The oldest sample holds the minimum until the cap pushes it out.
    tracker.updateLimitFromGc(10 * MIB);
    for (int i = 0; i < HeapLimitTracker.MAX_WINDOW_SAMPLES; i++) {
      tracker.updateLimitFromGc(30 * MIB);
    }

    assertThat(gauge("untrackedBytes")).isEqualTo(30.0 * MIB);
    assertThat(gauge("untrackedHeapSampleCount"))
        .isEqualTo((double) HeapLimitTracker.MAX_WINDOW_SAMPLES);
  }
}
