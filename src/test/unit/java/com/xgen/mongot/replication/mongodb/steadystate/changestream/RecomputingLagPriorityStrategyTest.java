package com.xgen.mongot.replication.mongodb.steadystate.changestream;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.xgen.mongot.replication.mongodb.common.ChangeStreamBatch;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.commons.codec.binary.Hex;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.BsonTimestamp;
import org.bson.types.ObjectId;
import org.junit.After;
import org.junit.Test;

/**
 * Covers the shared mechanics in {@link RecomputingLagPriorityStrategy} — the registry and its
 * add/remove lifecycle, the global healthy-lag-floor short-circuit, and shutdown — using a minimal
 * concrete subclass that promotes every candidate above the floor (so tests can isolate the base
 * class's own behavior from any particular selection policy).
 */
public class RecomputingLagPriorityStrategyTest {

  private static final int RECOMPUTE_INTERVAL_SECONDS_UNUSED_IN_TESTS = 1000;
  private static final String LAST_SUCCESSFUL_RECOMPUTE_METRIC =
      "replication.steadyState.priorityStrategy.lastSuccessfulRecomputeEpochSeconds";

  // The background scheduler backing each strategy uses non-daemon threads; without this, an
  // un-shut-down strategy would leak a live thread past the end of its test.
  private final List<PromoteAllAboveFloorStrategy> strategiesToShutDown = new ArrayList<>();

  @After
  public void shutDownAllStrategies() {
    this.strategiesToShutDown.forEach(PromoteAllAboveFloorStrategy::shutdown);
  }

  @Test
  public void isHighPriority_emptyRegistry_recomputeDoesNotCrash_andNothingIsPreferred() {
    var strategy = newStrategy(1);

    strategy.recomputeNowForTesting();

    assertThat(strategy.isHighPriority(new ObjectId(), batchWithLag(999))).isFalse();
  }

  @Test
  public void isHighPriority_allBelowHealthyFloor_nothingPromoted() {
    var strategy = newStrategy(/* healthyLagFloorSeconds= */ 10);
    ObjectId a = new ObjectId();
    ObjectId b = new ObjectId();
    strategy.addIndex(a);
    strategy.addIndex(b);
    // Promote both first, so the assertions below can only be explained by the second recompute
    // demoting them -- not by their never having been promoted in the first place.
    strategy.isHighPriority(a, batchWithLag(100));
    strategy.isHighPriority(b, batchWithLag(100));
    strategy.recomputeNowForTesting();
    // These calls report the now-healthy lag into the registry, and assert the pre-recompute state:
    // both are still preferred, carrying over from the cycle above.
    assertThat(strategy.isHighPriority(a, batchWithLag(5))).isTrue();
    assertThat(strategy.isHighPriority(b, batchWithLag(5))).isTrue();

    strategy.recomputeNowForTesting();

    assertThat(strategy.isHighPriority(a, batchWithLag(5))).isFalse();
    assertThat(strategy.isHighPriority(b, batchWithLag(5))).isFalse();
  }

  @Test
  public void isHighPriority_someAboveHealthyFloor_onlyThosePromoted() {
    var strategy = newStrategy(1);
    ObjectId active = new ObjectId();
    ObjectId idle = new ObjectId();
    strategy.addIndex(active);
    strategy.addIndex(idle);
    // These calls seed the registry, and assert the pre-recompute state: nothing is preferred until
    // a recompute has run, so the post-recompute assertions can only be the recompute's doing.
    assertThat(strategy.isHighPriority(active, batchWithLag(100))).isFalse();
    assertThat(strategy.isHighPriority(idle, batchWithLag(0))).isFalse();

    strategy.recomputeNowForTesting();

    assertThat(strategy.isHighPriority(active, batchWithLag(100))).isTrue();
    assertThat(strategy.isHighPriority(idle, batchWithLag(0))).isFalse();
  }

  @Test
  public void addIndex_beforeFirstBatch_entersPopulationWithZeroLag() {
    var strategy = newStrategy(1);
    ObjectId measured = new ObjectId();
    ObjectId justRegistered = new ObjectId();
    strategy.addIndex(measured);
    strategy.addIndex(justRegistered);
    // Only one of the two has reported a batch; the other has not been measured yet.
    strategy.isHighPriority(measured, batchWithLag(30));

    strategy.recomputeNowForTesting();

    // A registered but unmeasured index still counts towards the population subclasses size their
    // targets against, at a lag that cannot promote it.
    assertThat(strategy.lastSnapshot).containsExactly(measured, 30, justRegistered, 0);
    assertThat(strategy.isHighPriority(justRegistered, batchWithLag(0))).isFalse();
  }

  @Test
  public void isHighPriority_indexNeverRegistered_isNotTracked() {
    var strategy = newStrategy(1);
    ObjectId registered = new ObjectId();
    ObjectId neverRegistered = new ObjectId();
    strategy.addIndex(registered);
    strategy.isHighPriority(registered, batchWithLag(10));

    strategy.isHighPriority(neverRegistered, batchWithLag(9999));
    strategy.recomputeNowForTesting();

    // Reporting a batch does not itself enrol an index, so an unregistered one cannot distort the
    // population it is compared against, however bad its lag looks.
    assertThat(strategy.lastSnapshot.keySet()).containsExactly(registered);
    assertThat(strategy.isHighPriority(neverRegistered, batchWithLag(9999))).isFalse();
  }

  @Test
  public void isHighPriority_batchReportedAfterRemoveIndex_doesNotRestoreRegistryEntry() {
    var strategy = newStrategy(1);
    ObjectId stopped = new ObjectId();
    ObjectId stillRunning = new ObjectId();
    strategy.addIndex(stopped);
    strategy.addIndex(stillRunning);
    strategy.isHighPriority(stopped, batchWithLag(100));
    strategy.isHighPriority(stillRunning, batchWithLag(50));

    // The index is stopped while one of its getMores is still in flight, and that batch reports its
    // lag afterwards. The dispatcher checks for shutdown before fetching a batch, not before
    // reporting one, so this ordering is reachable in production.
    strategy.removeIndex(stopped);
    strategy.isHighPriority(stopped, batchWithLag(100));
    strategy.recomputeNowForTesting();

    // stillRunning keeps the registry non-empty, so this shows the stopped index was genuinely
    // excluded rather than the empty-registry short-circuit hiding the result.
    assertThat(strategy.lastSnapshot.keySet()).containsExactly(stillRunning);
    assertThat(strategy.isHighPriority(stopped, batchWithLag(100))).isFalse();
  }

  @Test
  public void removeIndex_droppedIndexNoLongerInfluencesRecompute() {
    var strategy = newStrategy(1);
    ObjectId toDrop = new ObjectId();
    strategy.addIndex(toDrop);
    strategy.isHighPriority(toDrop, batchWithLag(100));
    strategy.recomputeNowForTesting();
    // Preferred before the removal, so the assertion below shows that the removal is what took it
    // out of the registry.
    assertThat(strategy.isHighPriority(toDrop, batchWithLag(100))).isTrue();

    strategy.removeIndex(toDrop);
    strategy.recomputeNowForTesting();

    // Registry is now empty, so the dropped index is no longer promoted even though its last
    // known lag would have cleared the floor.
    assertThat(strategy.isHighPriority(toDrop, batchWithLag(0))).isFalse();
  }

  @Test
  public void recompute_promotesAndDemotes_incrementsPromotedAndDemotedCounters() {
    var registry = new SimpleMeterRegistry();
    var strategy = newStrategy(1, registry);
    ObjectId active = new ObjectId();
    strategy.addIndex(active);
    strategy.isHighPriority(active, batchWithLag(100));

    strategy.recomputeNowForTesting();

    assertThat(
            registry
                .get("replication.steadyState.priorityStrategy.promotedCount")
                .counter()
                .count())
        .isEqualTo(1.0);
    assertThat(
            registry
                .get("replication.steadyState.priorityStrategy.preferredSetSize")
                .gauge()
                .value())
        .isEqualTo(1.0);

    strategy.removeIndex(active);
    strategy.recomputeNowForTesting();

    assertThat(
            registry.get("replication.steadyState.priorityStrategy.demotedCount").counter().count())
        .isEqualTo(1.0);
    assertThat(
            registry
                .get("replication.steadyState.priorityStrategy.preferredSetSize")
                .gauge()
                .value())
        .isEqualTo(0.0);
  }

  @Test
  public void shutdown_stopsBackgroundRecompute_withoutThrowing() {
    var strategy = newStrategy(1);

    strategy.shutdown();
  }

  @Test
  public void shutdown_removesGauge_soStalePreferredSetSizeIsNotExportedAfterTeardown() {
    var registry = new SimpleMeterRegistry();
    var strategy = newStrategy(1, registry);
    ObjectId active = new ObjectId();
    strategy.addIndex(active);
    strategy.isHighPriority(active, batchWithLag(100));
    strategy.recomputeNowForTesting();
    assertThat(
            registry
                .get("replication.steadyState.priorityStrategy.preferredSetSize")
                .gauge()
                .value())
        .isEqualTo(1.0);

    strategy.shutdown();

    assertThat(registry.find("replication.steadyState.priorityStrategy.preferredSetSize").gauge())
        .isNull();
  }

  @Test
  public void recomputeNowForTesting_success_advancesLastSuccessfulRecomputeGauge() {
    var registry = new SimpleMeterRegistry();
    var strategy = newStrategy(1, registry);
    double initial = registry.get(LAST_SUCCESSFUL_RECOMPUTE_METRIC).gauge().value();

    strategy.recomputeNowForTesting();

    double afterRecompute = registry.get(LAST_SUCCESSFUL_RECOMPUTE_METRIC).gauge().value();
    assertThat(afterRecompute).isAtLeast(initial);
  }

  @Test
  public void recomputeNowForTesting_computePreferredSetThrows_doesNotAdvanceGauge() {
    var registry = new SimpleMeterRegistry();
    var strategy =
        new ThrowingComputePreferredSetStrategy(
            RECOMPUTE_INTERVAL_SECONDS_UNUSED_IN_TESTS, /* healthyLagFloorSeconds= */ 1, registry);
    try {
      // Clear the floor so recompute() actually reaches computePreferredSet instead of
      // short-circuiting on an empty/all-healthy registry.
      ObjectId registered = new ObjectId();
      strategy.addIndex(registered);
      strategy.isHighPriority(registered, batchWithLag(100));
      double initial = registry.get(LAST_SUCCESSFUL_RECOMPUTE_METRIC).gauge().value();

      assertThrows(RuntimeException.class, strategy::recomputeNowForTesting);

      // A subclass bug that makes computePreferredSet fail every cycle must not be masked as a
      // "successful" recompute -- the liveness alert needs to still be able to catch it.
      assertThat(registry.get(LAST_SUCCESSFUL_RECOMPUTE_METRIC).gauge().value()).isEqualTo(initial);
    } finally {
      strategy.shutdown();
    }
  }

  @Test
  public void isHighPriority_undecodableResumeTokenHex_treatedAsWorstCaseLag() {
    var strategy = newStrategy(1);
    ObjectId malformed = new ObjectId();
    strategy.addIndex(malformed);
    // Not valid hex at all, so decoding throws DecoderException.
    ChangeStreamBatch malformedBatch =
        batchWithResumeToken(new BsonDocument("_data", new BsonString("not-valid-hex")));
    // Seeds the registry with the malformed batch, and asserts the pre-recompute state.
    assertThat(strategy.isHighPriority(malformed, malformedBatch)).isFalse();

    strategy.recomputeNowForTesting();

    assertThat(strategy.isHighPriority(malformed, malformedBatch)).isTrue();
  }

  @Test
  public void isHighPriority_resumeTokenMissingDataField_treatedAsWorstCaseLag() {
    var strategy = newStrategy(1);
    ObjectId malformed = new ObjectId();
    strategy.addIndex(malformed);
    // No _data field, so the token never reaches hex decoding: IllegalStateException instead.
    ChangeStreamBatch malformedBatch = batchWithResumeToken(new BsonDocument());
    assertThat(strategy.isHighPriority(malformed, malformedBatch)).isFalse();

    strategy.recomputeNowForTesting();

    assertThat(strategy.isHighPriority(malformed, malformedBatch)).isTrue();
  }

  @Test
  public void isHighPriority_resumeTokenWithEmptyData_treatedAsWorstCaseLag() {
    var strategy = newStrategy(1);
    ObjectId malformed = new ObjectId();
    strategy.addIndex(malformed);
    // Decodes to zero bytes, and the leading byte is read before the length is checked, so this
    // fails with IndexOutOfBoundsException rather than IllegalStateException.
    ChangeStreamBatch malformedBatch =
        batchWithResumeToken(new BsonDocument("_data", new BsonString("")));
    assertThat(strategy.isHighPriority(malformed, malformedBatch)).isFalse();

    strategy.recomputeNowForTesting();

    assertThat(strategy.isHighPriority(malformed, malformedBatch)).isTrue();
  }

  @Test
  public void isHighPriority_resumeTokenTooShortForTimestamp_treatedAsWorstCaseLag() {
    var strategy = newStrategy(1);
    ObjectId malformed = new ObjectId();
    strategy.addIndex(malformed);
    // Leads with the timestamp identifier but carries only 3 of the 9 bytes a timestamp needs.
    ChangeStreamBatch malformedBatch =
        batchWithResumeToken(new BsonDocument("_data", new BsonString("820001")));
    assertThat(strategy.isHighPriority(malformed, malformedBatch)).isFalse();

    strategy.recomputeNowForTesting();

    assertThat(strategy.isHighPriority(malformed, malformedBatch)).isTrue();
  }

  @Test
  public void isHighPriority_resumeTokenNotLeadingWithTimestamp_treatedAsWorstCaseLag() {
    var strategy = newStrategy(1);
    ObjectId malformed = new ObjectId();
    strategy.addIndex(malformed);
    // Long enough, but its leading byte is not the timestamp identifier (0x82).
    ChangeStreamBatch malformedBatch =
        batchWithResumeToken(new BsonDocument("_data", new BsonString("000000000000000000")));
    assertThat(strategy.isHighPriority(malformed, malformedBatch)).isFalse();

    strategy.recomputeNowForTesting();

    assertThat(strategy.isHighPriority(malformed, malformedBatch)).isTrue();
  }

  // ---------------------------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------------------------

  /** A minimal concrete subclass that promotes every candidate clearing the healthy-lag floor. */
  private static final class PromoteAllAboveFloorStrategy extends RecomputingLagPriorityStrategy {

    private volatile Map<ObjectId, Integer> lastSnapshot = Map.of();

    PromoteAllAboveFloorStrategy(
        int recomputeIntervalSeconds, int healthyLagFloorSeconds, MeterRegistry meterRegistry) {
      super(
          "test-strategy-recompute",
          recomputeIntervalSeconds,
          healthyLagFloorSeconds,
          meterRegistry);
    }

    @Override
    Set<ObjectId> computePreferredSet(
        Map<ObjectId, Integer> lagSecondsSnapshot,
        Set<ObjectId> previousPreferredSet,
        int healthyLagFloorSeconds) {
      this.lastSnapshot = lagSecondsSnapshot;
      Set<ObjectId> preferred = new HashSet<>();
      for (Map.Entry<ObjectId, Integer> entry : lagSecondsSnapshot.entrySet()) {
        if (entry.getValue() >= healthyLagFloorSeconds) {
          preferred.add(entry.getKey());
        }
      }
      return preferred;
    }
  }

  /** A minimal concrete subclass whose {@code computePreferredSet} always throws. */
  private static final class ThrowingComputePreferredSetStrategy
      extends RecomputingLagPriorityStrategy {
    ThrowingComputePreferredSetStrategy(
        int recomputeIntervalSeconds, int healthyLagFloorSeconds, MeterRegistry meterRegistry) {
      super(
          "test-strategy-throwing",
          recomputeIntervalSeconds,
          healthyLagFloorSeconds,
          meterRegistry);
    }

    @Override
    Set<ObjectId> computePreferredSet(
        Map<ObjectId, Integer> lagSecondsSnapshot,
        Set<ObjectId> previousPreferredSet,
        int healthyLagFloorSeconds) {
      throw new IllegalStateException("always fails, for testing recompute-liveness tracking");
    }
  }

  private PromoteAllAboveFloorStrategy newStrategy(int healthyLagFloorSeconds) {
    return newStrategy(healthyLagFloorSeconds, new SimpleMeterRegistry());
  }

  private PromoteAllAboveFloorStrategy newStrategy(
      int healthyLagFloorSeconds, MeterRegistry meterRegistry) {
    var strategy =
        new PromoteAllAboveFloorStrategy(
            RECOMPUTE_INTERVAL_SECONDS_UNUSED_IN_TESTS, healthyLagFloorSeconds, meterRegistry);
    this.strategiesToShutDown.add(strategy);
    return strategy;
  }

  /** Builds a batch carrying {@code resumeToken}, whose command operation time is now. */
  private static ChangeStreamBatch batchWithResumeToken(BsonDocument resumeToken) {
    return new ChangeStreamBatch(
        Collections.emptyList(),
        resumeToken,
        Optional.of(new BsonTimestamp((int) (System.currentTimeMillis() / 1000), 0)));
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
