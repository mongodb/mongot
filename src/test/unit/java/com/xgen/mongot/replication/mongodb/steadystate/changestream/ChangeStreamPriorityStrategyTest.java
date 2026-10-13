package com.xgen.mongot.replication.mongodb.steadystate.changestream;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.xgen.mongot.replication.mongodb.common.ChangeStreamBatch;
import com.xgen.mongot.replication.mongodb.common.ChangeStreamPartition;
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

  // OptimeLagPriorityStrategy with partitioned readers
  @Test
  public void optimeLag_partitioned_slowPartitionKeepsIndexHighPriority() {
    var strategy = new OptimeLagPriorityStrategy(10);
    strategy.addIndex(INDEX_ID);

    assertThat(strategy.isHighPriority(INDEX_ID, partitionBatch(0, 30))).isTrue();
    // A fast partition reporting afterwards must not hide the slow one.
    assertThat(strategy.isHighPriority(INDEX_ID, partitionBatch(1, 1))).isTrue();
    assertThat(strategy.isHighPriority(INDEX_ID, partitionBatch(2, 1))).isTrue();
  }

  @Test
  public void optimeLag_partitioned_latestReportPerPartitionWins() {
    var strategy = new OptimeLagPriorityStrategy(10);
    strategy.addIndex(INDEX_ID);

    assertThat(strategy.isHighPriority(INDEX_ID, partitionBatch(0, 30))).isTrue();
    // The slow partition catches up, so the index is no longer lagging.
    assertThat(strategy.isHighPriority(INDEX_ID, partitionBatch(0, 1))).isFalse();
  }

  @Test
  public void optimeLag_partitioned_allPartitionsBelowThreshold_isLowPriority() {
    var strategy = new OptimeLagPriorityStrategy(10);
    strategy.addIndex(INDEX_ID);

    assertThat(strategy.isHighPriority(INDEX_ID, partitionBatch(0, 2))).isFalse();
    assertThat(strategy.isHighPriority(INDEX_ID, partitionBatch(1, 3))).isFalse();
  }

  @Test
  public void optimeLag_partitioned_partitionNotYetReported_doesNotCountAsLagging() {
    var strategy = new OptimeLagPriorityStrategy(10);
    strategy.addIndex(INDEX_ID);

    // Partitions 1 and 2 haven't reported; the index is judged by what has.
    assertThat(strategy.isHighPriority(INDEX_ID, partitionBatch(0, 1))).isFalse();
  }

  @Test
  public void optimeLag_partitioned_unreadableTokenOnOnePartition_keepsIndexHighPriority() {
    var strategy = new OptimeLagPriorityStrategy(10);
    strategy.addIndex(INDEX_ID);
    var malformed =
        new ChangeStreamBatch(
            Collections.emptyList(),
            new BsonDocument("_data", new BsonString("not-hex")),
            Optional.of(new BsonTimestamp(1, 0)),
            Optional.of(new ChangeStreamPartition(1, 3)));

    assertThat(strategy.isHighPriority(INDEX_ID, malformed)).isTrue();
    assertThat(strategy.isHighPriority(INDEX_ID, partitionBatch(0, 1))).isTrue();
  }

  @Test
  public void optimeLag_unpartitioned_usesLatestBatchOnly() {
    var strategy = new OptimeLagPriorityStrategy(10);
    strategy.addIndex(INDEX_ID);

    assertThat(strategy.isHighPriority(INDEX_ID, batchWithLag(30))).isTrue();
    assertThat(strategy.isHighPriority(INDEX_ID, batchWithLag(1))).isFalse();
  }

  @Test
  public void optimeLag_unpartitioned_generationsDoNotOverwriteEachOther() {
    var strategy = new OptimeLagPriorityStrategy(10);
    // Two live generations of one index share an index id.
    strategy.addIndex(INDEX_ID);
    strategy.addIndex(INDEX_ID);

    // The serving generation is lagging, so its decision is high priority.
    assertThat(strategy.isHighPriority(INDEX_ID, batchWithLag(30))).isTrue();
    // A healthy generation reports in between.
    assertThat(strategy.isHighPriority(INDEX_ID, batchWithLag(1))).isFalse();
    // The serving generation is still lagging; the other generation's report must not hide it.
    assertThat(strategy.isHighPriority(INDEX_ID, batchWithLag(30))).isTrue();
  }

  @Test
  public void optimeLag_unpartitioned_leavesNoStateForPartitionedBatches() {
    var strategy = new OptimeLagPriorityStrategy(10);
    strategy.addIndex(INDEX_ID);

    // An unpartitioned lagging batch must not count as a slow partition 0.
    assertThat(strategy.isHighPriority(INDEX_ID, batchWithLag(30))).isTrue();
    assertThat(strategy.isHighPriority(INDEX_ID, partitionBatch(1, 1))).isFalse();
  }

  @Test
  public void optimeLag_indexNotAdded_judgesBatchAloneAndKeepsNoState() {
    var strategy = new OptimeLagPriorityStrategy(10);

    assertThat(strategy.isHighPriority(INDEX_ID, partitionBatch(0, 30))).isTrue();
    assertThat(strategy.isHighPriority(INDEX_ID, partitionBatch(1, 1))).isFalse();
  }

  @Test
  public void optimeLag_removedIndex_dropsItsPartitionState() {
    var strategy = new OptimeLagPriorityStrategy(10);
    strategy.addIndex(INDEX_ID);
    strategy.isHighPriority(INDEX_ID, partitionBatch(0, 30));
    strategy.removeIndex(INDEX_ID);

    // A late batch after removal is judged alone; the old slow partition is forgotten.
    assertThat(strategy.isHighPriority(INDEX_ID, partitionBatch(1, 1))).isFalse();
  }

  @Test
  public void optimeLag_generationsShareState_untilTheLastIsRemoved() {
    var strategy = new OptimeLagPriorityStrategy(10);
    strategy.addIndex(INDEX_ID);
    strategy.addIndex(INDEX_ID);
    strategy.isHighPriority(INDEX_ID, partitionBatch(0, 30));

    strategy.removeIndex(INDEX_ID);
    // One generation is still live, so the slow partition is still remembered.
    assertThat(strategy.isHighPriority(INDEX_ID, partitionBatch(1, 1))).isTrue();

    strategy.removeIndex(INDEX_ID);
    assertThat(strategy.isHighPriority(INDEX_ID, partitionBatch(1, 1))).isFalse();
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

  private static ChangeStreamBatch partitionBatch(int partitionId, int lagSeconds) {
    int now = (int) (System.currentTimeMillis() / 1000);
    return new ChangeStreamBatch(
        Collections.emptyList(),
        resumeTokenForTime(new BsonTimestamp(now - lagSeconds, 0)),
        Optional.of(new BsonTimestamp(now, 0)),
        Optional.of(new ChangeStreamPartition(partitionId, 3)));
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
