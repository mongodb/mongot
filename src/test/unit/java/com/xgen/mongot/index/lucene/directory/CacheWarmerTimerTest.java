package com.xgen.mongot.index.lucene.directory;

import static com.google.common.truth.Truth.assertThat;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.Test;

public class CacheWarmerTimerTest {

  private static long millisToNanos(long millis) {
    return TimeUnit.MILLISECONDS.toNanos(millis);
  }

  @Test
  public void warmingMilliseconds_withNoRecordedWindows_returnsZero() {
    CacheWarmerTimer timer = new CacheWarmerTimer();

    assertThat(timer.warmingMilliseconds()).isEqualTo(0L);
  }

  @Test
  public void warmingMilliseconds_withSingleWindow_returnsWindowDuration() {
    CacheWarmerTimer timer = new CacheWarmerTimer();

    timer.recordPrewarmWindow(millisToNanos(1_000), millisToNanos(1_100));

    assertThat(timer.warmingMilliseconds()).isEqualTo(100L);
  }

  @Test
  public void warmingMilliseconds_withOverlappingWindows_countsOverlapOnce() {
    CacheWarmerTimer timer = new CacheWarmerTimer();

    // Two prewarms overlapping in wall-clock time, as happens when indexes warm concurrently on the
    // initialization thread pool. The summed duration would be 200ms; the union is 150ms.
    timer.recordPrewarmWindow(millisToNanos(0), millisToNanos(100));
    timer.recordPrewarmWindow(millisToNanos(50), millisToNanos(150));

    assertThat(timer.warmingMilliseconds()).isEqualTo(150L);
  }

  @Test
  public void warmingMilliseconds_withDisjointWindows_excludesIdleGap() {
    CacheWarmerTimer timer = new CacheWarmerTimer();

    // An idle gap with no prewarm running, e.g. before warming an index created after startup
    // warming finished, contributes nothing to the metric.
    timer.recordPrewarmWindow(millisToNanos(0), millisToNanos(100));
    timer.recordPrewarmWindow(millisToNanos(200), millisToNanos(300));

    assertThat(timer.warmingMilliseconds()).isEqualTo(200L);
  }

  @Test
  public void warmingMilliseconds_withTouchingWindows_returnsCombinedDuration() {
    CacheWarmerTimer timer = new CacheWarmerTimer();

    timer.recordPrewarmWindow(millisToNanos(0), millisToNanos(100));
    timer.recordPrewarmWindow(millisToNanos(100), millisToNanos(200));

    assertThat(timer.warmingMilliseconds()).isEqualTo(200L);
  }

  @Test
  public void warmingMilliseconds_withContainedWindow_returnsOuterWindowDuration() {
    CacheWarmerTimer timer = new CacheWarmerTimer();

    timer.recordPrewarmWindow(millisToNanos(0), millisToNanos(300));
    timer.recordPrewarmWindow(millisToNanos(100), millisToNanos(200));

    assertThat(timer.warmingMilliseconds()).isEqualTo(300L);
  }

  @Test
  public void warmingMilliseconds_withDuplicateWindows_countsDurationOnce() {
    CacheWarmerTimer timer = new CacheWarmerTimer();

    timer.recordPrewarmWindow(millisToNanos(0), millisToNanos(100));
    timer.recordPrewarmWindow(millisToNanos(0), millisToNanos(100));

    assertThat(timer.warmingMilliseconds()).isEqualTo(100L);
  }

  @Test
  public void warmingMilliseconds_isIndependentOfRecordingOrder() {
    CacheWarmerTimer timer = new CacheWarmerTimer();

    // The latest-ending window is recorded first, then an earlier one.
    timer.recordPrewarmWindow(millisToNanos(200), millisToNanos(300));
    timer.recordPrewarmWindow(millisToNanos(0), millisToNanos(100));

    assertThat(timer.warmingMilliseconds()).isEqualTo(200L);
  }

  @Test
  public void warmingMilliseconds_withConcurrentlyRecordedWindows_returnsExactUnion() {
    CacheWarmerTimer timer = new CacheWarmerTimer();
    int threadCount = 4;
    int windowCount = 1_000;
    CountDownLatch startLatch = new CountDownLatch(1);

    // Window m covers [2m, 2m + 3) milliseconds, so consecutive windows overlap and the union is
    // [0, 2 * (windowCount - 1) + 3) no matter how the threads interleave. A lost or torn update
    // would leave a gap and produce a smaller total.
    try (ExecutorService threadPool = Executors.newFixedThreadPool(threadCount)) {
      for (int t = 0; t < threadCount; t++) {
        int thread = t;
        threadPool.submit(
            () -> {
              try {
                startLatch.await();
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
              }
              for (int m = thread; m < windowCount; m += threadCount) {
                timer.recordPrewarmWindow(millisToNanos(2L * m), millisToNanos(2L * m + 3));
              }
            });
      }
      startLatch.countDown();
    }

    assertThat(timer.warmingMilliseconds()).isEqualTo(2L * (windowCount - 1) + 3);
  }

  @Test
  public void warmingMilliseconds_withWindowBridgingDisjointWindows_mergesAllThree() {
    CacheWarmerTimer timer = new CacheWarmerTimer();

    // A late window overlaps both existing disjoint windows, merging everything into one window
    // that also covers the former gap.
    timer.recordPrewarmWindow(millisToNanos(0), millisToNanos(100));
    timer.recordPrewarmWindow(millisToNanos(200), millisToNanos(300));
    timer.recordPrewarmWindow(millisToNanos(50), millisToNanos(250));

    assertThat(timer.warmingMilliseconds()).isEqualTo(300L);
  }
}
