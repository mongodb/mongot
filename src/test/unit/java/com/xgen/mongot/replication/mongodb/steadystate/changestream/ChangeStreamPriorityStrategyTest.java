package com.xgen.mongot.replication.mongodb.steadystate.changestream;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.xgen.mongot.replication.mongodb.common.ChangeStreamBatch;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Optional;
import org.apache.commons.codec.binary.Hex;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.BsonTimestamp;
import org.bson.types.ObjectId;
import org.junit.Test;

public class ChangeStreamPriorityStrategyTest {

  // Index identity is irrelevant to these per-batch strategies; a fixed id keeps the test bodies
  // focused on the batch/threshold behavior being exercised.
  private static final ObjectId INDEX_ID = new ObjectId();

  // OptimeLagPriorityStrategy
  @Test
  public void optimeLag_lagAtThreshold_isHighPriority() {
    var strategy = new OptimeLagPriorityStrategy(10);
    assertThat(strategy.isHighPriority(INDEX_ID, batchWithLag(10))).isTrue();
  }

  @Test
  public void optimeLag_lagAboveThreshold_isHighPriority() {
    var strategy = new OptimeLagPriorityStrategy(10);
    assertThat(strategy.isHighPriority(INDEX_ID, batchWithLag(60))).isTrue();
  }

  @Test
  public void optimeLag_lagBelowThreshold_isLowPriority() {
    var strategy = new OptimeLagPriorityStrategy(10);
    assertThat(strategy.isHighPriority(INDEX_ID, batchWithLag(5))).isFalse();
  }

  @Test
  public void optimeLag_zeroLagCaughtUp_isLowPriority() {
    var strategy = new OptimeLagPriorityStrategy(10);
    assertThat(strategy.isHighPriority(INDEX_ID, batchWithLag(0))).isFalse();
  }

  @Test
  public void optimeLag_zeroThreshold_alwaysHighPriority() {
    var strategy = new OptimeLagPriorityStrategy(0);
    assertThat(strategy.isHighPriority(INDEX_ID, batchWithLag(0))).isTrue();
  }

  @Test
  public void optimeLag_negativeThreshold_throwsIllegalArgument() {
    IllegalArgumentException e =
        assertThrows(IllegalArgumentException.class, () -> new OptimeLagPriorityStrategy(-1));
    assertThat(e).hasMessageThat().contains("-1");
  }

  @Test
  public void optimeLag_negativeLag_isLowPriority() {
    var strategy = new OptimeLagPriorityStrategy(1);
    int now = (int) (System.currentTimeMillis() / 1000);
    // commandOperationTime behind resumeTokenTime — negative lag clamped to zero
    var batch =
        new ChangeStreamBatch(
            Collections.emptyList(),
            resumeTokenForTime(new BsonTimestamp(now, 0)),
            Optional.of(new BsonTimestamp(now - 1, 0)));
    assertThat(strategy.isHighPriority(INDEX_ID, batch)).isFalse();
  }

  @Test
  public void optimeLag_negativeLagWithZeroThreshold_isHighPriority() {
    var strategy = new OptimeLagPriorityStrategy(0);
    int now = (int) (System.currentTimeMillis() / 1000);
    var batch =
        new ChangeStreamBatch(
            Collections.emptyList(),
            resumeTokenForTime(new BsonTimestamp(now, 0)),
            Optional.of(new BsonTimestamp(now - 1, 0)));
    assertThat(strategy.isHighPriority(INDEX_ID, batch)).isTrue();
  }

  @Test
  public void optimeLag_malformedResumeToken_defaultsToHighPriority() {
    var strategy = new OptimeLagPriorityStrategy(10);
    int now = (int) (System.currentTimeMillis() / 1000);
    // "_data" value is not valid hex — ResumeTokenUtils will throw DecoderException
    var batch =
        new ChangeStreamBatch(
            Collections.emptyList(),
            new BsonDocument("_data", new BsonString("not-valid-hex")),
            Optional.of(new BsonTimestamp(now, 0)));
    assertThat(strategy.isHighPriority(INDEX_ID, batch)).isTrue();
  }

  @Test
  public void optimeLag_absentCommandOperationTime_defaultsToHighPriority() {
    var strategy = new OptimeLagPriorityStrategy(10);
    int now = (int) (System.currentTimeMillis() / 1000);
    // No commandOperationTime (e.g. session has not gossiped a $clusterTime yet) — cannot compute
    // lag, so default to high priority per the scheduling "prefer when uncertain" convention.
    var batch =
        new ChangeStreamBatch(
            Collections.emptyList(),
            resumeTokenForTime(new BsonTimestamp(now, 0)),
            Optional.empty());
    assertThat(strategy.isHighPriority(INDEX_ID, batch)).isTrue();
  }

  // Helpers

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
    // Resume token binary format: 0x82 (timestamp identifier) followed by the 8-byte long value.
    ByteBuffer buf = ByteBuffer.allocate(9);
    buf.put((byte) 0x82);
    buf.putLong(ts.getValue());
    return new BsonDocument("_data", new BsonString(Hex.encodeHexString(buf.array())));
  }
}
