package com.xgen.mongot.replication.mongodb.steadystate.changestream;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.xgen.mongot.replication.mongodb.common.ChangeStreamBatch;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import org.apache.commons.codec.binary.Hex;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.BsonTimestamp;
import org.bson.RawBsonDocument;
import org.junit.Test;

public class ChangeStreamPriorityStrategyTest {

  // OptimeLagPriorityStrategy
  @Test
  public void optimeLag_lagAtThreshold_isHighPriority() {
    var strategy = new OptimeLagPriorityStrategy(10);
    assertThat(strategy.isHighPriority(batchWithLag(10))).isTrue();
  }

  @Test
  public void optimeLag_lagAboveThreshold_isHighPriority() {
    var strategy = new OptimeLagPriorityStrategy(10);
    assertThat(strategy.isHighPriority(batchWithLag(60))).isTrue();
  }

  @Test
  public void optimeLag_lagBelowThreshold_isLowPriority() {
    var strategy = new OptimeLagPriorityStrategy(10);
    assertThat(strategy.isHighPriority(batchWithLag(5))).isFalse();
  }

  @Test
  public void optimeLag_zeroLagCaughtUp_isLowPriority() {
    var strategy = new OptimeLagPriorityStrategy(10);
    assertThat(strategy.isHighPriority(batchWithLag(0))).isFalse();
  }

  @Test
  public void optimeLag_zeroThreshold_alwaysHighPriority() {
    var strategy = new OptimeLagPriorityStrategy(0);
    assertThat(strategy.isHighPriority(batchWithLag(0))).isTrue();
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
            new BsonTimestamp(now - 1, 0));
    assertThat(strategy.isHighPriority(batch)).isFalse();
  }

  @Test
  public void optimeLag_negativeLagWithZeroThreshold_isHighPriority() {
    var strategy = new OptimeLagPriorityStrategy(0);
    int now = (int) (System.currentTimeMillis() / 1000);
    var batch =
        new ChangeStreamBatch(
            Collections.emptyList(),
            resumeTokenForTime(new BsonTimestamp(now, 0)),
            new BsonTimestamp(now - 1, 0));
    assertThat(strategy.isHighPriority(batch)).isTrue();
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
            new BsonTimestamp(now, 0));
    assertThat(strategy.isHighPriority(batch)).isTrue();
  }

  // HadDocumentsPriorityStrategy
  @Test
  public void hadDocuments_true_isHighPriority() {
    var strategy = new HadDocumentsPriorityStrategy();
    assertThat(strategy.isHighPriority(batchWithEvents(1))).isTrue();
  }

  @Test
  public void hadDocuments_false_isLowPriority() {
    var strategy = new HadDocumentsPriorityStrategy();
    assertThat(strategy.isHighPriority(batchWithEvents(0))).isFalse();
  }

  // BatchDataSizePriorityStrategy
  @Test
  public void batchDataSize_sizeAtThreshold_isHighPriority() {
    var strategy = new BatchDataSizePriorityStrategy(1024);
    assertThat(strategy.isHighPriority(batchWithBytes(1024))).isTrue();
  }

  @Test
  public void batchDataSize_sizeAboveThreshold_isHighPriority() {
    var strategy = new BatchDataSizePriorityStrategy(1024);
    assertThat(strategy.isHighPriority(batchWithBytes(2048))).isTrue();
  }

  @Test
  public void batchDataSize_sizeBelowThreshold_isLowPriority() {
    var strategy = new BatchDataSizePriorityStrategy(1024);
    assertThat(strategy.isHighPriority(batchWithBytes(512))).isFalse();
  }

  @Test
  public void batchDataSize_emptyBatch_isLowPriority() {
    var strategy = new BatchDataSizePriorityStrategy(1024);
    assertThat(strategy.isHighPriority(batchWithBytes(0))).isFalse();
  }

  @Test
  public void batchDataSize_negativeThreshold_throwsIllegalArgument() {
    IllegalArgumentException e =
        assertThrows(IllegalArgumentException.class, () -> new BatchDataSizePriorityStrategy(-1));
    assertThat(e).hasMessageThat().contains("-1");
  }

  // Helpers

  private static ChangeStreamBatch batchWithLag(int lagSeconds) {
    int now = (int) (System.currentTimeMillis() / 1000);
    BsonTimestamp resumeTokenTime = new BsonTimestamp(now - lagSeconds, 0);
    return new ChangeStreamBatch(
        Collections.emptyList(), resumeTokenForTime(resumeTokenTime), new BsonTimestamp(now, 0));
  }

  private static ChangeStreamBatch batchWithEvents(int eventCount) {
    int now = (int) (System.currentTimeMillis() / 1000);
    BsonTimestamp ts = new BsonTimestamp(now, 0);
    List<RawBsonDocument> events =
        eventCount > 0
            ? Collections.nCopies(eventCount, new RawBsonDocument(new byte[] {5, 0, 0, 0, 0}))
            : Collections.emptyList();
    return new ChangeStreamBatch(events, resumeTokenForTime(ts), ts);
  }

  private static ChangeStreamBatch batchWithBytes(int totalBytes) {
    int now = (int) (System.currentTimeMillis() / 1000);
    BsonTimestamp ts = new BsonTimestamp(now, 0);
    List<RawBsonDocument> events =
        totalBytes > 0 ? List.of(rawDocOfSize(totalBytes)) : Collections.emptyList();
    return new ChangeStreamBatch(events, resumeTokenForTime(ts), ts);
  }

  /** Builds a PBRT whose decoded optime is {@code ts}. */
  private static BsonDocument resumeTokenForTime(BsonTimestamp ts) {
    // Resume token binary format: 0x82 (timestamp identifier) followed by the 8-byte long value.
    ByteBuffer buf = ByteBuffer.allocate(9);
    buf.put((byte) 0x82);
    buf.putLong(ts.getValue());
    return new BsonDocument("_data", new BsonString(Hex.encodeHexString(buf.array())));
  }

  /** Creates a RawBsonDocument whose byte buffer has exactly {@code size} remaining bytes. */
  private static RawBsonDocument rawDocOfSize(int size) {
    // A minimal valid BSON document is 5 bytes (int32 size + terminator). Pad with zeros.
    byte[] bytes = new byte[Math.max(5, size)];
    // BSON int32 little-endian length
    int len = bytes.length;
    bytes[0] = (byte) (len & 0xFF);
    bytes[1] = (byte) ((len >> 8) & 0xFF);
    bytes[2] = (byte) ((len >> 16) & 0xFF);
    bytes[3] = (byte) ((len >> 24) & 0xFF);
    return new RawBsonDocument(bytes);
  }
}
