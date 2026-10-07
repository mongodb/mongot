package com.xgen.mongot.replication.mongodb.common;

import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import java.util.Objects;
import java.util.Optional;
import java.util.StringJoiner;
import org.bson.BsonDocument;
import org.bson.BsonTimestamp;
import org.bson.BsonValue;

public final class BufferlessNaturalOrderInitialSyncResumeInfo extends InitialSyncResumeInfo {

  private static class Fields {
    private static final Field.Required<Long> HIGH_WATER_MARK =
        Field.builder("highWaterMark").longField().required();

    private static final Field.Required<BsonDocument> POST_BATCH_RESUME_TOKEN =
        Field.builder("postBatchResumeToken").documentField().required();

    private static final Field.Optional<String> SYNC_SOURCE_HOST =
        Field.builder("syncSourceHost").stringField().optional().noDefault();
  }

  /** The opTime of the last event seen during change stream application. */
  private final BsonTimestamp highWaterMark;

  /** The _id of the last document scanned during initial sync collection scan. */
  private final BsonDocument postBatchResumeToken;

  /** The syncSourceHost that initial sync collection scan is replicating from. */
  private final Optional<String> syncSourceHost;

  public static BufferlessNaturalOrderInitialSyncResumeInfo deserialize(DocumentParser parser)
      throws BsonParseException {
    return new BufferlessNaturalOrderInitialSyncResumeInfo(
        new BsonTimestamp(parser.getField(Fields.HIGH_WATER_MARK).unwrap()),
        parser.getField(Fields.POST_BATCH_RESUME_TOKEN).unwrap(),
        parser.getField(Fields.SYNC_SOURCE_HOST).unwrap());
  }

  /**
   * Creates an InitialSyncResumeInfo for the bufferless collection scan or change stream
   * application.
   */
  public BufferlessNaturalOrderInitialSyncResumeInfo(
      BsonTimestamp highWaterMark,
      BsonDocument postBatchResumeToken,
      Optional<String> syncSourceHost) {
    this.highWaterMark = highWaterMark;
    this.postBatchResumeToken = postBatchResumeToken;
    this.syncSourceHost = syncSourceHost;
  }

  @Override
  public BsonTimestamp getResumeOperationTime() {
    return this.highWaterMark;
  }

  @Override
  public BsonValue getResumeToken() {
    return this.postBatchResumeToken;
  }

  @Override
  public Optional<String> getSyncSourceHost() {
    return this.syncSourceHost;
  }

  @Override
  public final BsonDocument toBson() {
    return BsonDocumentBuilder.builder()
        .field(
            InitialSyncResumeInfo.Fields.BUFFERLESS_NATURAL_ORDER_INITIAL_SYNC_RESUME_INFO,
            Optional.of(this))
        .build();
  }

  @Override
  protected BsonValue resumeInfoToBson() {
    return BsonDocumentBuilder.builder()
        .field(Fields.HIGH_WATER_MARK, this.highWaterMark.getValue())
        .field(Fields.POST_BATCH_RESUME_TOKEN, this.postBatchResumeToken)
        .field(Fields.SYNC_SOURCE_HOST, this.syncSourceHost)
        .build();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    BufferlessNaturalOrderInitialSyncResumeInfo that =
        (BufferlessNaturalOrderInitialSyncResumeInfo) o;
    return this.highWaterMark.equals(that.highWaterMark)
        && this.postBatchResumeToken.equals(that.postBatchResumeToken)
        && this.syncSourceHost.equals(that.syncSourceHost);
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.highWaterMark, this.postBatchResumeToken, this.syncSourceHost);
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", "BufferlessNaturalOrderInitialSyncResumeInfo[", "]")
        .add("highWaterMark=" + this.highWaterMark)
        .add("postBatchResumeToken=" + this.postBatchResumeToken)
        .add("syncSourceHost=" + this.syncSourceHost)
        .toString();
  }
}
