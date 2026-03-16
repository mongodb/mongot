package com.xgen.mongot.embedding.providers.congestion;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class AimdCongestionControlTest {

  @Test
  public void defaultConstructor_initializesWithDefaultValues() {
    AimdCongestionControl aimd = new AimdCongestionControl();

    assertEquals(AimdCongestionControl.DEFAULT_INITIAL_CWND, aimd.getTotalPermits());
    assertEquals(1.0, aimd.getCwnd(), 1e-7);
    assertEquals(AimdCongestionControl.DEFAULT_SLOW_START_THRESHOLD, aimd.getSlowStartThreshold());
  }

  @Test
  public void builder_createsWithCustomValues() {
    AimdCongestionControl aimd =
        AimdCongestionControl.builder()
            .initialCwnd(5)
            .slowStartThreshold(20)
            .linearIncrease(2.0)
            .multiplicativeDecrease(0.5)
            .idleTimeoutMillis(10000)
            .build();

    assertEquals(5, aimd.getTotalPermits());
    assertEquals(5.0, aimd.getCwnd(), 1e-7);
    assertEquals(20, aimd.getSlowStartThreshold());
  }

  @Test
  public void onRelease_successInSlowStart_increasesWindowExponentially() {
    AimdCongestionControl aimd =
        AimdCongestionControl.builder()
            .initialCwnd(1)
            .slowStartThreshold(16)
            .linearIncrease(1.0)
            .build();

    // Initial cwnd = 1, in slow start phase (exponential increase)
    aimd.onRelease(true); // ACK
    assertEquals(2.0, aimd.getCwnd(), 1e-7);

    aimd.onRelease(true); // ACK
    assertEquals(3.0, aimd.getCwnd(), 1e-7);

    aimd.onRelease(true); // ACK
    assertEquals(4.0, aimd.getCwnd(), 1e-7);
  }

  @Test
  public void onRelease_successInCongestionAvoidance_increasesWindowLinearly() {
    AimdCongestionControl aimd =
        AimdCongestionControl.builder()
            .initialCwnd(1)
            .slowStartThreshold(16)
            .linearIncrease(1.0)
            .build();

    // cwnd = 1 = , so we're at the boundary (exponential i)
    double initialCwnd = aimd.getCwnd();
    aimd.onRelease(true); // ACK

    // cwnd += linearIncrease
    assertEquals(initialCwnd + 1.0, aimd.getCwnd(), 1e-7);
  }

  @Test
  public void onRelease_congestion_decreasesWindowMultiplicatively() {
    // Disable slow start so window doesn't grow much during setup
    AimdCongestionControl aimd =
        AimdCongestionControl.builder()
            .initialCwnd(1)
            .slowStartThreshold(0) // Disable slow start for predictable test
            .multiplicativeDecrease(0.75)
            .build();

    // AIMD limits decrease to once per cwnd to avoid over-reacting.
    // We need to send enough requests (>= cwnd) for the first decrease to trigger.
    // After 10 ACKs, cwnd will be 10 + 10*1.0 = 20, so we need at least 20 requests total
    for (int i = 0; i < 20; i++) {
      aimd.onRelease(true); // ACK - build up sequence number
    }

    double cwndBeforeCongestion = aimd.getCwnd();
    aimd.onRelease(false); // Congestion signal

    // Window should decrease by multiplicative factor
    assertEquals(cwndBeforeCongestion * 0.75, aimd.getCwnd(), 1e-7);
  }

  @Test
  public void onRelease_congestion_limitedToOncePerCwnd() {
    // Disable slow start so window doesn't grow much during setup
    AimdCongestionControl aimd =
        AimdCongestionControl.builder()
            .initialCwnd(1)
            .slowStartThreshold(0) // Disable slow start for predictable test
            .multiplicativeDecrease(0.75)
            .build();

    // Build up sequence number to enable first decrease
    // After 10 ACKs, cwnd will be 10 + 10*1.0 = 20, so we need at least 20 requests total
    for (int i = 0; i < 20; i++) {
      aimd.onRelease(true);
    }

    double cwndBeforeCongestion = aimd.getCwnd();

    // First congestion event triggers decrease
    aimd.onRelease(false);
    double afterFirstDecrease = aimd.getCwnd();
    assertEquals(cwndBeforeCongestion * 0.75, afterFirstDecrease, 1e-7);

    // Subsequent congestion events within the same cwnd period should not decrease further
    // because sequenceNumber - lastMultiplicativeDecreaseSequenceNumber < cwnd
    aimd.onRelease(false);
    assertEquals(afterFirstDecrease, aimd.getCwnd(), 1e-7);
  }

  @Test
  public void onRelease_congestion_neverDropsBelowMinCwnd() {
    AimdCongestionControl aimd =
        AimdCongestionControl.builder()
            .initialCwnd(1)
            .slowStartThreshold(16)
            .multiplicativeDecrease(0.5)
            .build();

    // Even with aggressive multiplicative decrease, cwnd should never go below MIN_CWND
    aimd.onRelease(false);
    assertTrue(aimd.getCwnd() >= AimdCongestionControl.MIN_CWND);
    assertEquals(1, aimd.getTotalPermits());
  }

  @Test
  public void onAcquire_resetsAfterIdleTimeout() {
    // Use a testable subclass with controllable time
    TestableAimdCongestionControl aimd = new TestableAimdCongestionControl(1, 16, 1.0, 0.75, 1000);

    // Build up window
    aimd.onAcquire();
    aimd.onRelease(true);
    aimd.onRelease(true);
    aimd.onRelease(true);
    double builtUpCwnd = aimd.getCwnd();
    assertTrue(builtUpCwnd > 1.0);

    // Simulate idle timeout
    aimd.setCurrentTime(System.currentTimeMillis() + 2000);
    aimd.onAcquire();

    // Window should reset to initial value
    assertEquals(1.0, aimd.getCwnd(), 1e-7);
    assertEquals(16, aimd.getSlowStartThreshold());
  }

  @Test
  public void getTotalPermits_returnsCeilingOfCwnd() {
    AimdCongestionControl aimd =
        AimdCongestionControl.builder()
            .initialCwnd(1)
            .slowStartThreshold(2)
            .linearIncrease(0.3)
            .build();

    // cwnd = 1.0, getTotalPermits should return 1
    assertEquals(1, aimd.getTotalPermits());

    // After one ACK in congestion avoidance: cwnd = 1.0 + 0.3 = 1.3
    aimd.onRelease(true);
    assertEquals(2, aimd.getTotalPermits()); // ceil(1.3) = 2

    // After another ACK: cwnd = 1.3 + 0.3/1.3 = 1.53
    aimd.onRelease(true);
    assertEquals(2, aimd.getTotalPermits()); // ceil(1.6) = 2

    // After another ACK: cwnd = 1.53 + 0.3/1.53 = 1.72
    aimd.onRelease(true);
    assertEquals(2, aimd.getTotalPermits()); // ceil(1.9) = 2
  }

  @Test
  public void slowStartTosCongestionAvoidance_transitionIsSmooth() {
    AimdCongestionControl aimd =
        AimdCongestionControl.builder()
            .initialCwnd(1)
            .slowStartThreshold(4)
            .linearIncrease(1.0)
            .build();

    // Slow start: cwnd increases by linearIncrease
    aimd.onRelease(true); // cwnd = 2
    aimd.onRelease(true); // cwnd = 3
    aimd.onRelease(true); // cwnd = 4, now at threshold

    assertEquals(4.0, aimd.getCwnd(), 1e-7);

    // Congestion avoidance
    aimd.onRelease(true);
    assertEquals(4.25, aimd.getCwnd(), 1e-7);
  }

  @Test
  public void aimd_convergesToFairShare() {
    // Simulate two flows competing for resources
    AimdCongestionControl flow1 =
        AimdCongestionControl.builder()
            .initialCwnd(1)
            .slowStartThreshold(32)
            .linearIncrease(1.0)
            .multiplicativeDecrease(0.75)
            .build();

    AimdCongestionControl flow2 =
        AimdCongestionControl.builder()
            .initialCwnd(20) // Start with higher initial value
            .slowStartThreshold(32)
            .linearIncrease(1.0)
            .multiplicativeDecrease(0.75)
            .build();

    // Simulate several rounds of AIMD
    // When total demand exceeds capacity, both flows get congestion signal
    int capacity = 24;

    // Run more iterations to allow convergence, and ensure each flow processes
    // enough requests to allow multiplicative decreases to take effect
    for (int i = 0; i < 500; i++) {
      System.out.println(
          "i = " + i + ", flow1 = " + flow1.getCwnd() + ", flow2 = " + flow2.getCwnd());
      int totalDemand = flow1.getTotalPermits() + flow2.getTotalPermits();
      boolean congested = totalDemand > capacity;
      System.out.println(congested);
      flow1.onAcquire();
      flow1.onRelease(!congested);
      flow2.onAcquire();
      flow2.onRelease(!congested);
    }

    // Both flows should converge to roughly equal share
    double diff = Math.abs(flow1.getCwnd() - flow2.getCwnd());
    assertTrue("Flows should converge to similar values, diff=" + diff, diff < 5.0);
  }

  /** Testable subclass that allows controlling the current time. */
  private static class TestableAimdCongestionControl extends AimdCongestionControl {
    private long currentTime = System.currentTimeMillis();

    TestableAimdCongestionControl(
        int initialCwnd,
        int slowStartThreshold,
        double linearIncrease,
        double multiplicativeDecrease,
        int idleTimeoutMillis) {
      super(
          initialCwnd,
          slowStartThreshold,
          linearIncrease,
          multiplicativeDecrease,
          idleTimeoutMillis);
    }

    void setCurrentTime(long time) {
      this.currentTime = time;
    }

    @Override
    long currentTimeMillis() {
      return this.currentTime;
    }
  }
}
