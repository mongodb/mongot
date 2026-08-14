package com.xgen.mongot.metrics;

import static com.google.common.truth.Truth.assertThat;

import com.xgen.mongot.metrics.SubsystemResourceHistory.Dimension;
import com.xgen.mongot.metrics.SubsystemResourceHistory.Recorder;
import java.time.Duration;
import java.util.function.LongSupplier;
import org.junit.Test;

public class SubsystemResourceHistoryTest {

  private static final String SUBSYSTEM = "replication";
  private static final String INDEXING = "indexing";
  private static final String DECODING = "decoding";

  /** Stand-in for the monotonic millisecond clock, so tests drive time deterministically. */
  private static final class TestClock implements LongSupplier {
    private volatile long millis;

    void setMillis(long millis) {
      this.millis = millis;
    }

    @Override
    public long getAsLong() {
      return this.millis;
    }
  }

  /** A subsystem nothing has registered under is unknown to every reader, rather than zero. */
  @Test
  public void unknownSubsystem_returnsEmpty() {
    var history = new SubsystemResourceHistory(12, new TestClock());

    assertThat(history.currentTotal("nope", Dimension.ALLOC_BYTES)).isEmpty();
    assertThat(history.increaseOver("nope", Dimension.ALLOC_BYTES, Duration.ofMinutes(1)))
        .isEmpty();
    assertThat(history.increaseOver("nope", INDEXING, Dimension.ALLOC_BYTES, Duration.ofMinutes(1)))
        .isEmpty();
    assertThat(history.pools("nope")).isEmpty();
    assertThat(history.sinceLastSample()).isEmpty();
  }

  /**
   * Registering the same pool twice reuses its recorder, so a pool rebuilt on a sync-source change
   * continues its existing series instead of starting a second one alongside it.
   */
  @Test
  public void recorderFor_isIdempotentPerPool() {
    var history = new SubsystemResourceHistory(12, new TestClock());

    Recorder first = history.recorderFor(SUBSYSTEM, INDEXING);
    Recorder again = history.recorderFor(SUBSYSTEM, INDEXING);
    history.recorderFor(SUBSYSTEM, DECODING);

    assertThat(again).isSameInstanceAs(first);
    assertThat(history.subsystems()).containsExactly(SUBSYSTEM);
    assertThat(history.pools(SUBSYSTEM)).containsExactly(INDEXING, DECODING);
  }

  /** A subsystem's total is the sum of its pools' latest pushes; an unpushed pool adds nothing. */
  @Test
  public void currentTotal_sumsPools() {
    var history = new SubsystemResourceHistory(12, new TestClock());
    history.recorderFor(SUBSYSTEM, INDEXING).recordAllocBytes(1000);
    history.recorderFor(SUBSYSTEM, DECODING).recordAllocBytes(500);
    history.recorderFor(SUBSYSTEM, "session-refresh");

    assertThat(history.currentTotal(SUBSYSTEM, Dimension.ALLOC_BYTES)).hasValue(1500L);
    assertThat(history.currentTotal(SUBSYSTEM, Dimension.CPU_NANOS)).hasValue(0L);
  }

  /**
   * With samples covering the whole horizon, the answer is the plain endpoint difference. Here 13
   * samples 5s apart span 60s, growing 100 per sample: a minute reads 1200, and the trailing 30s
   * reads the last six steps, 600.
   */
  @Test
  public void increaseOver_matchesHorizonWhenFullyCovered() {
    var clock = new TestClock();
    var history = new SubsystemResourceHistory(13, clock);
    Recorder indexing = history.recorderFor(SUBSYSTEM, INDEXING);

    for (int step = 0; step <= 12; step++) {
      clock.setMillis(step * 5000L);
      indexing.recordAllocBytes(1000 + step * 100L);
      history.appendCurrentSlots();
    }

    assertThat(history.increaseOver(SUBSYSTEM, Dimension.ALLOC_BYTES, Duration.ofMinutes(1)))
        .hasValue(1200L);
    assertThat(history.increaseOver(SUBSYSTEM, Dimension.ALLOC_BYTES, Duration.ofSeconds(30)))
        .hasValue(600L);
  }

  /**
   * A horizon reaching further back than the history goes is answered by scaling what is held, the
   * way PromQL increase() extrapolates to its range. Here 200 bytes were observed over 10s, so a
   * 20s horizon reads 400.
   */
  @Test
  public void increaseOver_normalizesShorterCoverageToHorizon() {
    var clock = new TestClock();
    var history = new SubsystemResourceHistory(12, clock);
    Recorder indexing = history.recorderFor(SUBSYSTEM, INDEXING);

    for (int step = 0; step <= 2; step++) {
      clock.setMillis(step * 5000L);
      indexing.recordAllocBytes(1000 + step * 100L);
      history.appendCurrentSlots();
    }

    assertThat(history.increaseOver(SUBSYSTEM, Dimension.ALLOC_BYTES, Duration.ofSeconds(20)))
        .hasValue(400L);
  }

  /**
   * Scaling stops once it would amplify too far: the same 10s of history answers a 20s horizon
   * above, but not a 30s one, because that would treble a third of a window's worth of samples.
   */
  @Test
  public void increaseOver_isEmptyBelowHalfCoverage() {
    var clock = new TestClock();
    var history = new SubsystemResourceHistory(12, clock);
    Recorder indexing = history.recorderFor(SUBSYSTEM, INDEXING);

    for (int step = 0; step <= 2; step++) {
      clock.setMillis(step * 5000L);
      indexing.recordAllocBytes(1000 + step * 100L);
      history.appendCurrentSlots();
    }

    assertThat(history.increaseOver(SUBSYSTEM, Dimension.ALLOC_BYTES, Duration.ofSeconds(30)))
        .isEmpty();
  }

  /** An increase needs two endpoints, so a single sample answers nothing rather than zero. */
  @Test
  public void increaseOver_isEmptyBelowTwoSamples() {
    var clock = new TestClock();
    var history = new SubsystemResourceHistory(12, clock);
    history.recorderFor(SUBSYSTEM, INDEXING).recordAllocBytes(1000);
    history.appendCurrentSlots();

    assertThat(history.increaseOver(SUBSYSTEM, Dimension.ALLOC_BYTES, Duration.ofSeconds(5)))
        .isEmpty();
  }

  /**
   * Pools are readable individually and as a subsystem, which is what lets a caller go from "which
   * subsystem" to "which of its pools". Indexing grows 300 and decoding 100 over the same 5s, so
   * the subsystem reads 400.
   */
  @Test
  public void increaseOver_reportsPerPoolAndSubsystemTotal() {
    var clock = new TestClock();
    var history = new SubsystemResourceHistory(12, clock);
    Recorder indexing = history.recorderFor(SUBSYSTEM, INDEXING);
    Recorder decoding = history.recorderFor(SUBSYSTEM, DECODING);

    clock.setMillis(0);
    indexing.recordAllocBytes(1000);
    decoding.recordAllocBytes(500);
    history.appendCurrentSlots();

    clock.setMillis(5000);
    indexing.recordAllocBytes(1300);
    decoding.recordAllocBytes(600);
    history.appendCurrentSlots();

    var horizon = Duration.ofSeconds(5);
    assertThat(history.increaseOver(SUBSYSTEM, INDEXING, Dimension.ALLOC_BYTES, horizon))
        .hasValue(300L);
    assertThat(history.increaseOver(SUBSYSTEM, DECODING, Dimension.ALLOC_BYTES, horizon))
        .hasValue(100L);
    assertThat(history.increaseOver(SUBSYSTEM, Dimension.ALLOC_BYTES, horizon)).hasValue(400L);
  }

  /**
   * A pool appearing part way through a horizon contributes only what it allocated after it
   * appeared. Decoding registers at 15s, so over a 20s horizon the subsystem reads indexing's 400
   * plus decoding's 200 rather than counting decoding's whole counter as growth.
   */
  @Test
  public void lateRegisteredPool_contributesFromRegistration() {
    var clock = new TestClock();
    var history = new SubsystemResourceHistory(12, clock);
    Recorder indexing = history.recorderFor(SUBSYSTEM, INDEXING);

    for (int step = 0; step <= 2; step++) {
      clock.setMillis(step * 5000L);
      indexing.recordAllocBytes(1000 + step * 100L);
      history.appendCurrentSlots();
    }

    // Pools are registered as they are built, so a newly created pool's threads start near zero.
    Recorder decoding = history.recorderFor(SUBSYSTEM, DECODING);
    clock.setMillis(15000);
    indexing.recordAllocBytes(1300);
    decoding.recordAllocBytes(0);
    history.appendCurrentSlots();

    clock.setMillis(20000);
    indexing.recordAllocBytes(1400);
    decoding.recordAllocBytes(200);
    history.appendCurrentSlots();

    assertThat(history.increaseOver(SUBSYSTEM, Dimension.ALLOC_BYTES, Duration.ofSeconds(20)))
        .hasValue(600L);
    assertThat(
            history.increaseOver(
                SUBSYSTEM, DECODING, Dimension.ALLOC_BYTES, Duration.ofSeconds(20)))
        .hasValue(200L);
  }

  /**
   * Once the buffer wraps the oldest sample is gone, so a horizon reaching past it starts at the
   * oldest still held. A depth of 3 fed 4 samples keeps t=5s..15s, so a 10s horizon starts at t=5s
   * and reads 200 rather than reaching back to t=0.
   */
  @Test
  public void samplesWrapAndDropOldest() {
    var clock = new TestClock();
    var history = new SubsystemResourceHistory(3, clock);
    Recorder indexing = history.recorderFor(SUBSYSTEM, INDEXING);

    for (int step = 0; step <= 3; step++) {
      clock.setMillis(step * 5000L);
      indexing.recordAllocBytes(100 + step * 100L);
      history.appendCurrentSlots();
    }

    assertThat(history.sampleCount()).isEqualTo(3);
    assertThat(history.increaseOver(SUBSYSTEM, Dimension.ALLOC_BYTES, Duration.ofSeconds(10)))
        .hasValue(200L);
  }

  /** Allocation and CPU are separate series on one recorder and do not bleed into each other. */
  @Test
  public void dimensionsAreIndependent() {
    var clock = new TestClock();
    var history = new SubsystemResourceHistory(12, clock);
    Recorder indexing = history.recorderFor(SUBSYSTEM, INDEXING);

    clock.setMillis(0);
    indexing.recordAllocBytes(1000);
    indexing.recordCpuNanos(70);
    history.appendCurrentSlots();

    clock.setMillis(5000);
    indexing.recordAllocBytes(1300);
    indexing.recordCpuNanos(90);
    history.appendCurrentSlots();

    var horizon = Duration.ofSeconds(5);
    assertThat(history.increaseOver(SUBSYSTEM, Dimension.ALLOC_BYTES, horizon)).hasValue(300L);
    assertThat(history.increaseOver(SUBSYSTEM, Dimension.CPU_NANOS, horizon)).hasValue(20L);
  }

  /** Staleness is the age of the newest sample, which is how a reader spots a stalled feed. */
  @Test
  public void sinceLastSample_reportsAgeOfLatestSample() {
    var clock = new TestClock();
    var history = new SubsystemResourceHistory(12, clock);
    history.recorderFor(SUBSYSTEM, INDEXING).recordAllocBytes(1000);

    clock.setMillis(1000);
    history.appendCurrentSlots();
    clock.setMillis(4000);

    assertThat(history.sinceLastSample()).hasValue(Duration.ofMillis(3000));
  }
}
