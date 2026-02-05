package com.xgen.mongot.util.timers;

import static org.junit.Assert.assertEquals;

import com.google.common.base.Ticker;
import com.google.common.testing.FakeTicker;
import com.google.common.truth.Truth;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.Test;

public class ThreadSafeInvocationCountingTimerTest {
  @Test
  public void testSingleThreadedInvocationCountAndElapsedTime() {
    FakeTicker fakeTicker = new FakeTicker();
    ThreadSafeInvocationCountingTimer timer = new ThreadSafeInvocationCountingTimer(fakeTicker);

    try (InvocationCountingTimer.SafeClosable split1 = timer.split()) {
      fakeTicker.advance(100, TimeUnit.MILLISECONDS);
    }

    try (InvocationCountingTimer.SafeClosable split2 = timer.split()) {
      fakeTicker.advance(200, TimeUnit.MILLISECONDS);
    }

    assertEquals(2, timer.getInvocationCount());
    assertEquals(300_000_000L, timer.getElapsedNanos()); // 300 ms in nanoseconds
  }

  @Test
  public void testConcurrentSplits() throws InterruptedException {
    ThreadSafeInvocationCountingTimer timer =
        new ThreadSafeInvocationCountingTimer(Ticker.systemTicker());

    int threadCount = 50;
    int threadSleepMs = 100;
    ExecutorService executor = Executors.newFixedThreadPool(threadCount);
    CountDownLatch latch = new CountDownLatch(threadCount);

    Runnable task =
        () -> {
          try (InvocationCountingTimer.SafeClosable split = timer.split()) {
            try {
              Thread.sleep(threadSleepMs);
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
            }
          }
          latch.countDown();
        };

    for (int i = 0; i < threadCount; i++) {
      executor.submit(task);
    }
    latch.await();
    executor.shutdown();

    Truth.assertWithMessage("Invocation count should match expected")
        .that(threadCount)
        .isEqualTo(timer.getInvocationCount());

    long expectedMinElapsedNanos = TimeUnit.MILLISECONDS.toNanos(threadSleepMs) * threadCount;
    long actualElapsedNanos = timer.getElapsedNanos();
    Truth.assertWithMessage("Elapsed time should be at least the expected time")
        .that(actualElapsedNanos)
        .isAtLeast(expectedMinElapsedNanos);
  }
}
