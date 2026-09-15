package com.xgen.mongot.replication.mongodb.steadystate.changestream;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.xgen.mongot.replication.mongodb.common.ChangeStreamBatch;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.apache.commons.codec.binary.Hex;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.BsonTimestamp;
import org.bson.types.ObjectId;
import org.junit.After;
import org.junit.Test;

/**
 * Covers {@link TopKWorstLagPriorityStrategy}'s own selection policy — K sizing from the drain
 * ratio, ranking by lag, and the healthy-lag floor. Shared registry/fallback/removal mechanics are
 * covered by {@link RecomputingLagPriorityStrategyTest}.
 */
public class TopKWorstLagPriorityStrategyTest {

  private static final int RECOMPUTE_INTERVAL_SECONDS_UNUSED_IN_TESTS = 1000;

  // The background scheduler backing each strategy uses non-daemon threads; without this, a
  // successfully constructed but un-shut-down strategy would leak a live thread past its test.
  private final List<TopKWorstLagPriorityStrategy> strategiesToShutDown = new ArrayList<>();

  @After
  public void shutDownAllStrategies() {
    this.strategiesToShutDown.forEach(TopKWorstLagPriorityStrategy::shutdown);
  }

  @Test
  public void isHighPriority_drainRatioOneToOne_promotesWorstHalf() {
    // drainRatio=1 -> targetPreferredFraction = 1/2 -> K = round(0.5 * 4) = 2.
    var strategy = newStrategy(/* drainRatio= */ 1);
    ObjectId worst = new ObjectId();
    ObjectId secondWorst = new ObjectId();
    ObjectId healthy1 = new ObjectId();
    ObjectId healthy2 = new ObjectId();
    // All four are registered, so K is sized off a population of 4.
    strategy.addIndex(worst);
    strategy.addIndex(secondWorst);
    strategy.addIndex(healthy1);
    strategy.addIndex(healthy2);
    assertThat(strategy.isHighPriority(worst, batchWithLag(300))).isFalse();
    assertThat(strategy.isHighPriority(secondWorst, batchWithLag(200))).isFalse();
    assertThat(strategy.isHighPriority(healthy1, batchWithLag(100))).isFalse();
    assertThat(strategy.isHighPriority(healthy2, batchWithLag(0))).isFalse();

    strategy.recomputeNowForTesting();

    assertThat(strategy.isHighPriority(worst, batchWithLag(300))).isTrue();
    assertThat(strategy.isHighPriority(secondWorst, batchWithLag(200))).isTrue();
    assertThat(strategy.isHighPriority(healthy1, batchWithLag(100))).isFalse();
    assertThat(strategy.isHighPriority(healthy2, batchWithLag(0))).isFalse();
  }

  @Test
  public void isHighPriority_registeredButUnreportedIndexes_countTowardsK() {
    var strategy = newStrategy(/* drainRatio= */ 1);
    ObjectId worst = new ObjectId();
    ObjectId secondWorst = new ObjectId();
    ObjectId registeredButSilent1 = new ObjectId();
    ObjectId registeredButSilent2 = new ObjectId();
    strategy.addIndex(worst);
    strategy.addIndex(secondWorst);
    strategy.addIndex(registeredButSilent1);
    strategy.addIndex(registeredButSilent2);
    assertThat(strategy.isHighPriority(worst, batchWithLag(300))).isFalse();
    assertThat(strategy.isHighPriority(secondWorst, batchWithLag(200))).isFalse();

    strategy.recomputeNowForTesting();

    assertThat(strategy.isHighPriority(worst, batchWithLag(300))).isTrue();
    assertThat(strategy.isHighPriority(secondWorst, batchWithLag(200))).isTrue();
    // A registered index sitting at zero lag enlarges K for others but can never take a slot
    // itself, since zero is below the healthy floor.
    assertThat(strategy.isHighPriority(registeredButSilent1, batchWithLag(0))).isFalse();
    assertThat(strategy.isHighPriority(registeredButSilent2, batchWithLag(0))).isFalse();
  }

  @Test
  public void isHighPriority_rankWithinKButBelowHealthyFloor_notPromoted() {
    // K = 1, but the single worst-ranked index hasn't cleared the floor, so nothing is promoted.
    var strategy =
        newStrategy(
            /* drainRatioPreferredTasksPerRegularTask= */ 1, /* healthyLagFloorSeconds= */ 50);
    ObjectId belowFloor = new ObjectId();
    strategy.addIndex(belowFloor);
    assertThat(strategy.isHighPriority(belowFloor, batchWithLag(10))).isFalse();

    strategy.recomputeNowForTesting();

    assertThat(strategy.isHighPriority(belowFloor, batchWithLag(10))).isFalse();
  }

  @Test
  public void isHighPriority_alreadyPreferredIndexFallsPastK_demotedSoSetStaysWithinK() {
    // K = 1 (drainRatio=1, population=2). "was" is the sole preferred index until a worse index
    // joins and pushes it to rank 2. It must be demoted, not kept: holding both would put two
    // indexes in a one-slot preferred set, leaving the regular queue empty on a two-index node
    // and the drain ratio with nothing to alternate between.
    var strategy = newStrategy(/* drainRatio= */ 1);
    ObjectId was = new ObjectId();
    strategy.addIndex(was);
    assertThat(strategy.isHighPriority(was, batchWithLag(100))).isFalse();
    strategy.recomputeNowForTesting();
    assertThat(strategy.isHighPriority(was, batchWithLag(100))).isTrue();

    ObjectId worse = new ObjectId();
    strategy.addIndex(worse);
    assertThat(strategy.isHighPriority(worse, batchWithLag(200))).isFalse();
    strategy.recomputeNowForTesting();

    assertThat(strategy.isHighPriority(worse, batchWithLag(200))).isTrue();
    assertThat(strategy.isHighPriority(was, batchWithLag(100))).isFalse();
  }

  @Test
  public void constructor_nonPositiveDrainRatio_throws() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new TopKWorstLagPriorityStrategy(
                /* drainRatioPreferredTasksPerRegularTask= */ 0,
                RECOMPUTE_INTERVAL_SECONDS_UNUSED_IN_TESTS,
                1,
                new SimpleMeterRegistry()));
  }

  // ---------------------------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------------------------

  private TopKWorstLagPriorityStrategy newStrategy(int drainRatio) {
    return newStrategy(drainRatio, /* healthyLagFloorSeconds= */ 1);
  }

  private TopKWorstLagPriorityStrategy newStrategy(int drainRatio, int healthyLagFloorSeconds) {
    var strategy =
        new TopKWorstLagPriorityStrategy(
            drainRatio,
            RECOMPUTE_INTERVAL_SECONDS_UNUSED_IN_TESTS,
            healthyLagFloorSeconds,
            new SimpleMeterRegistry());
    this.strategiesToShutDown.add(strategy);
    return strategy;
  }

  private static ChangeStreamBatch batchWithLag(int lagSeconds) {
    int now = (int) (System.currentTimeMillis() / 1000);
    BsonTimestamp resumeTokenTime = new BsonTimestamp(now - lagSeconds, 0);
    return new ChangeStreamBatch(
        Collections.emptyList(),
        resumeTokenForTime(resumeTokenTime),
        Optional.of(new BsonTimestamp(now, 0)));
  }

  /** Builds a PBRT whose decoded optime is {@code ts}. */
  private static BsonDocument resumeTokenForTime(BsonTimestamp ts) {
    ByteBuffer buf = ByteBuffer.allocate(9);
    buf.put((byte) 0x82);
    buf.putLong(ts.getValue());
    return new BsonDocument("_data", new BsonString(Hex.encodeHexString(buf.array())));
  }
}
