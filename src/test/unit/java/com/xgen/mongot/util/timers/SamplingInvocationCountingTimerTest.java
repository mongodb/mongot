package com.xgen.mongot.util.timers;

import static org.junit.Assert.assertEquals;

import com.google.common.testing.FakeTicker;
import com.google.common.truth.Truth;
import java.util.concurrent.TimeUnit;
import org.junit.Test;

public class SamplingInvocationCountingTimerTest {

  @Test
  public void testFirstTenInvocationsAreMeasured() {
    FakeTicker fakeTicker = new FakeTicker();
    SamplingInvocationCountingTimer timer = new SamplingInvocationCountingTimer(fakeTicker);

    // First 10 invocations should all be measured
    for (int i = 0; i < 10; i++) {
      try (InvocationCountingTimer.SafeClosable split = timer.split()) {
        fakeTicker.advance(100, TimeUnit.NANOSECONDS);
      }
    }

    assertEquals(10, timer.getInvocationCount());
    // 10 measurements of 100ns each = 1000ns measured
    // Extrapolated: (1000 * 10) / 10 = 1000ns
    assertEquals(1000L, timer.getElapsedNanos());
  }

  @Test
  public void testSecondTierSampling() {
    FakeTicker fakeTicker = new FakeTicker();
    SamplingInvocationCountingTimer timer = new SamplingInvocationCountingTimer(fakeTicker);

    // 100 invocations total, each 100ns
    for (int i = 0; i < 100; i++) {
      try (InvocationCountingTimer.SafeClosable split = timer.split()) {
        fakeTicker.advance(100, TimeUnit.NANOSECONDS);
      }
    }

    assertEquals(100, timer.getInvocationCount());
    // Sampled: first 10 (always) + 10,20,30,40,50,60,70,80,90 (every 10th) = 19 measurements
    // Total measured: 19 * 100ns = 1900ns
    // Extrapolated: (1900 * 100) / 19 = 10000ns
    assertEquals(10000L, timer.getElapsedNanos());
  }

  @Test
  public void testThirdTierSampling() {
    FakeTicker fakeTicker = new FakeTicker();
    SamplingInvocationCountingTimer timer = new SamplingInvocationCountingTimer(fakeTicker);

    // 1000 invocations total, each 100ns
    for (int i = 0; i < 1000; i++) {
      try (InvocationCountingTimer.SafeClosable split = timer.split()) {
        fakeTicker.advance(100, TimeUnit.NANOSECONDS);
      }
    }

    assertEquals(1000, timer.getInvocationCount());
    // Sampled: 10 (always) + 9 (10-99 every 10th) + 9 (100-999 every 100th) = 28 measurements
    // Total measured: 28 * 100ns = 2800ns
    // Extrapolated: (2800 * 1000) / 28 = 100000ns
    assertEquals(100000L, timer.getElapsedNanos());
  }

  @Test
  public void testFourthTierSampling() {
    FakeTicker fakeTicker = new FakeTicker();
    SamplingInvocationCountingTimer timer = new SamplingInvocationCountingTimer(fakeTicker);

    // 3000 invocations total, each 100ns
    for (int i = 0; i < 3000; i++) {
      try (InvocationCountingTimer.SafeClosable split = timer.split()) {
        fakeTicker.advance(100, TimeUnit.NANOSECONDS);
      }
    }

    assertEquals(3000, timer.getInvocationCount());
    // Sampled: 10 + 9 + 9 + 2 (1000, 2000) = 30 measurements
    // Total measured: 30 * 100ns = 3000ns
    // Extrapolated: (3000 * 3000) / 30 = 300000ns
    assertEquals(300000L, timer.getElapsedNanos());
  }

  @Test
  public void testTimingDataCreator() {
    FakeTicker fakeTicker = new FakeTicker();
    SamplingInvocationCountingTimer timer =
        new SamplingInvocationCountingTimer(
            fakeTicker,
            (invocationCount, elapsedNanos) ->
                new TimingData.NoInvocationCountTimingData(elapsedNanos));

    for (int i = 0; i < 5; i++) {
      try (InvocationCountingTimer.SafeClosable split = timer.split()) {
        fakeTicker.advance(100, TimeUnit.NANOSECONDS);
      }
    }

    TimingData timingData = timer.getTimingData();
    Truth.assertThat(timingData).isInstanceOf(TimingData.NoInvocationCountTimingData.class);
    assertEquals(0, timingData.invocationCount());
    assertEquals(500L, timingData.elapsedNanos());
  }

  @Test
  public void testInvocationCountAlwaysAccurate() {
    FakeTicker fakeTicker = new FakeTicker();
    SamplingInvocationCountingTimer timer = new SamplingInvocationCountingTimer(fakeTicker);

    // Even when not sampling, invocation count should be accurate
    for (int i = 0; i < 500; i++) {
      try (InvocationCountingTimer.SafeClosable ignored = timer.split()) {
        fakeTicker.advance(10, TimeUnit.NANOSECONDS);
      }
    }

    assertEquals(500, timer.getInvocationCount());
  }

  @Test
  public void testZeroInvocations() {
    FakeTicker fakeTicker = new FakeTicker();
    SamplingInvocationCountingTimer timer = new SamplingInvocationCountingTimer(fakeTicker);

    assertEquals(0, timer.getInvocationCount());
    assertEquals(0L, timer.getElapsedNanos());
  }

  @Test
  public void testSingleInvocation() {
    FakeTicker fakeTicker = new FakeTicker();
    SamplingInvocationCountingTimer timer = new SamplingInvocationCountingTimer(fakeTicker);

    try (InvocationCountingTimer.SafeClosable split = timer.split()) {
      fakeTicker.advance(500, TimeUnit.NANOSECONDS);
    }

    assertEquals(1, timer.getInvocationCount());
    assertEquals(500L, timer.getElapsedNanos());
  }

  @Test
  public void testExtrapolationWithVaryingTimes() {
    FakeTicker fakeTicker = new FakeTicker();
    SamplingInvocationCountingTimer timer = new SamplingInvocationCountingTimer(fakeTicker);

    // Simulate varying times - first 10 are 200ns, rest are 100ns
    // This tests that the average is used correctly
    for (int i = 0; i < 100; i++) {
      try (InvocationCountingTimer.SafeClosable split = timer.split()) {
        if (i < 10) {
          fakeTicker.advance(200, TimeUnit.NANOSECONDS);
        } else {
          fakeTicker.advance(100, TimeUnit.NANOSECONDS);
        }
      }
    }

    assertEquals(100, timer.getInvocationCount());
    // Sampled at 0-9: 10 * 200ns = 2000ns
    // Sampled at 10,20,...,90: 9 * 100ns = 900ns
    // Total: 2900ns from 19 measurements
    // Extrapolated: (2900 * 100) / 19 = 15263ns (rounded down)
    assertEquals(15263L, timer.getElapsedNanos());
  }
}
