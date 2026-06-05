package com.xgen.mongot.metrics.cache;

import com.xgen.testing.util.ManuallyUpdatedClock;
import java.time.Clock;
import java.time.Duration;
import org.junit.Assert;
import org.junit.Test;

public class IntervalThrottleTest {

  private static final Duration INTERVAL = Duration.ofSeconds(30);

  @Test
  public void testFirstCallProceeds() {
    ManuallyUpdatedClock clock = new ManuallyUpdatedClock(Clock.systemUTC());
    IntervalThrottle throttle = new IntervalThrottle(clock, INTERVAL);
    Assert.assertFalse(throttle.shouldThrottle());
  }

  @Test
  public void testSecondCallWithinIntervalThrottled() {
    ManuallyUpdatedClock clock = new ManuallyUpdatedClock(Clock.systemUTC());
    IntervalThrottle throttle = new IntervalThrottle(clock, INTERVAL);
    Assert.assertFalse(throttle.shouldThrottle());
    Assert.assertTrue(throttle.shouldThrottle());
  }

  @Test
  public void testWindowRestartsFromLastProceed() {
    ManuallyUpdatedClock clock = new ManuallyUpdatedClock(Clock.systemUTC());
    IntervalThrottle throttle = new IntervalThrottle(clock, INTERVAL);
    Assert.assertFalse(throttle.shouldThrottle());
    clock.update(INTERVAL);
    Assert.assertFalse(throttle.shouldThrottle());
    clock.update(Duration.ofSeconds(1));
    Assert.assertTrue(throttle.shouldThrottle());
  }
}
