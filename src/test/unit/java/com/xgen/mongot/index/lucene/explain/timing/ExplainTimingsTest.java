package com.xgen.mongot.index.lucene.explain.timing;

import com.google.common.base.Ticker;
import com.google.common.testing.FakeTicker;
import com.google.common.truth.Truth;
import com.xgen.mongot.util.timers.InvocationCountingTimer;
import com.xgen.mongot.util.timers.TimingData;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.junit.Test;

public class ExplainTimingsTest {
  private static Ticker fakeTicker() {
    return new FakeTicker().setAutoIncrementStep(Duration.ofNanos(5));
  }

  @Test
  public void testExplainTimingsAllTimingDataIsEmpty() {
    ExplainTimings timings = ExplainTimings.builder().build();
    Truth.assertThat(timings.allTimingDataIsEmpty()).isTrue();
    Truth.assertThat(timings.extractTimingData()).hasSize(ExplainTimings.Type.values().length);
  }

  @Test
  public void testConcurrentExplainTimingsSingleType() throws Exception {
    ExplainTimings timings = ExplainTimings.builder().build();
    int threadCount = 50;
    int threadSleepMs = 100;
    ExecutorService executor = Executors.newFixedThreadPool(threadCount);
    CountDownLatch latch = new CountDownLatch(threadCount);

    Runnable task =
        () -> {
          try (InvocationCountingTimer.SafeClosable split =
              timings.split(ExplainTimings.Type.ADVANCE)) {
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
        .isEqualTo(timings.ofType(ExplainTimings.Type.ADVANCE).getInvocationCount());

    long expectedMinElapsedNanos = TimeUnit.MILLISECONDS.toNanos(threadSleepMs) * threadCount;
    long actualElapsedNanos = timings.ofType(ExplainTimings.Type.ADVANCE).getElapsedNanos();
    Truth.assertWithMessage("Elapsed time should be at least the expected time")
        .that(actualElapsedNanos)
        .isAtLeast(expectedMinElapsedNanos);
  }

  @Test
  public void testConcurrentExplainTimingsMultipleType() throws Exception {
    ExplainTimings timings = ExplainTimings.builder().build();
    int threadCount = 50;
    ExecutorService executor = Executors.newFixedThreadPool(threadCount);
    CountDownLatch latch = new CountDownLatch(threadCount);

    Runnable task =
        () -> {
          ThreadLocalRandom random = ThreadLocalRandom.current();
          int idx = random.nextInt(ExplainTimings.Type.values().length);
          ExplainTimings.Type type = ExplainTimings.Type.values()[idx];
          try (InvocationCountingTimer.SafeClosable split = timings.split(type)) {
            try {
              Thread.sleep(100);
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

    var totalInvocationCount =
        timings.stream().map(Map.Entry::getValue).mapToLong(TimingData::invocationCount).sum();

    long actualElapsedNanos =
        timings.stream().map(Map.Entry::getValue).mapToLong(TimingData::elapsedNanos).sum();

    Truth.assertWithMessage("Invocation count should match expected")
        .that(threadCount)
        .isEqualTo(totalInvocationCount);

    long expectedMinElapsedNanos = TimeUnit.MILLISECONDS.toNanos(100) * threadCount;
    Truth.assertWithMessage("Elapsed time should be at least the expected time")
        .that(actualElapsedNanos)
        .isAtLeast(expectedMinElapsedNanos);
  }

  @Test
  public void testMerge() {
    var first = ExplainTimings.builder().withTicker(fakeTicker()).build();
    var second = ExplainTimings.builder().withTicker(fakeTicker()).build();

    first.split(ExplainTimings.Type.COLLECT).close();
    second.split(ExplainTimings.Type.COLLECT).close();

    var result = ExplainTimings.merge(fakeTicker(), first, second);
    var collectTimer = result.ofType(ExplainTimings.Type.COLLECT);
    Truth.assertThat(collectTimer.getInvocationCount()).isEqualTo(2);
    Truth.assertThat(collectTimer.getElapsedNanos()).isEqualTo(10);

    for (var timingData :
        result.stream()
            .filter(entry -> entry.getKey() != ExplainTimings.Type.COLLECT)
            .map(Map.Entry::getValue)
            .collect(Collectors.toSet())) {
      Truth.assertThat(timingData.invocationCount()).isEqualTo(0);
      Truth.assertThat(timingData.elapsedNanos()).isEqualTo(0);
    }

    result.split(ExplainTimings.Type.COLLECT).close();
    var mergedCollectTimer = result.ofType(ExplainTimings.Type.COLLECT);
    Truth.assertThat(mergedCollectTimer.getInvocationCount()).isEqualTo(3);
    Truth.assertThat(mergedCollectTimer.getElapsedNanos()).isEqualTo(15);
  }
}
