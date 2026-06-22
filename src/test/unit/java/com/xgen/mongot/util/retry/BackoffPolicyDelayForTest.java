package com.xgen.mongot.util.retry;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.time.Duration;
import org.junit.Test;

public class BackoffPolicyDelayForTest {

  @Test
  public void testFixedDelayForReturnsBaseWithoutJitter() {
    var policy = FixedBackoffPolicy.builder().delay(Duration.ofMillis(50)).build();
    assertEquals(Duration.ofMillis(50), policy.delayFor(1));
    assertEquals(Duration.ofMillis(50), policy.delayFor(7));
  }

  @Test
  public void testFixedDelayForWithJitterStaysInRange() {
    var policy = FixedBackoffPolicy.builder().delay(Duration.ofMillis(100)).jitter(0.2).build();
    for (int i = 0; i < 100; i++) {
      long ms = policy.delayFor(1).toMillis();
      assertTrue("delay " + ms + " out of jitter range", ms >= 80 && ms <= 120);
    }
  }

  @Test
  public void testFixedDelayForReturnsZeroWhenNoFailures() {
    var policy = FixedBackoffPolicy.builder().delay(Duration.ofMillis(50)).build();
    assertEquals(Duration.ZERO, policy.delayFor(0));
    assertEquals(Duration.ZERO, policy.delayFor(-1));
  }

  @Test
  public void testExponentialDelayForGrowsByFactor() {
    var policy =
        ExponentialBackoffPolicy.builder()
            .initialDelay(Duration.ofMillis(10))
            .backoffFactor(2)
            .maxDelay(Duration.ofMinutes(10))
            .build();
    assertEquals(Duration.ofMillis(10), policy.delayFor(1));
    assertEquals(Duration.ofMillis(20), policy.delayFor(2));
    assertEquals(Duration.ofMillis(40), policy.delayFor(3));
    assertEquals(Duration.ofMillis(80), policy.delayFor(4));
  }

  @Test
  public void testExponentialDelayForRespectsMaxDelay() {
    var policy =
        ExponentialBackoffPolicy.builder()
            .initialDelay(Duration.ofMillis(10))
            .backoffFactor(2)
            .maxDelay(Duration.ofMillis(50))
            .build();
    assertEquals(Duration.ofMillis(40), policy.delayFor(3));
    assertEquals(Duration.ofMillis(50), policy.delayFor(4));
    assertEquals(Duration.ofMillis(50), policy.delayFor(20));
    // Large failure counts must not overflow.
    assertEquals(Duration.ofMillis(50), policy.delayFor(1_000));
  }

  @Test
  public void testExponentialDelayForReturnsZeroWhenNoFailures() {
    var policy =
        ExponentialBackoffPolicy.builder()
            .initialDelay(Duration.ofMillis(10))
            .backoffFactor(2)
            .maxDelay(Duration.ofMillis(50))
            .build();
    assertEquals(Duration.ZERO, policy.delayFor(0));
    assertEquals(Duration.ZERO, policy.delayFor(-5));
  }

  @Test
  public void testExponentialDelayForWithJitterStaysInRange() {
    var policy =
        ExponentialBackoffPolicy.builder()
            .initialDelay(Duration.ofMillis(100))
            .backoffFactor(2)
            .maxDelay(Duration.ofMillis(1_000))
            .jitter(0.1)
            .build();
    for (int i = 0; i < 100; i++) {
      long ms = policy.delayFor(2).toMillis();
      // expected base: 200ms; jitter +/-10% -> [180, 220]
      assertTrue("delay " + ms + " out of jitter range", ms >= 180 && ms <= 220);
    }
  }
}
