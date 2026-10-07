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

public final class BufferlessIdOrderInitialSyncResumeInfo extends InitialSyncResumeInfo {

  private static class Fields {
    private static final Field.Required<Long> HIGH_WATER_MARK =
        Field.builder("highWaterMark").longField().required();

    private static final Field.Required<BsonValue> LAST_SCANNED_ID =
        Field.builder("lastScannedId").unparsedValueField().required();
  }

  /** The opTime of the last event seen during change stream application. */
  private final BsonTimestamp highWaterMark;

  /** The _id of the last document scanned during initial sync collection scan. */
  private final BsonValue lastScannedId;

  public static BufferlessIdOrderInitialSyncResumeInfo deserialize(DocumentParser parser)
      throws BsonParseException {
    return new BufferlessIdOrderInitialSyncResumeInfo(
        new BsonTimestamp(parser.getField(Fields.HIGH_WATER_MARK).unwrap()),
        parser.getField(Fields.LAST_SCANNED_ID).unwrap());
  }

  /**
   * Creates an InitialSyncResumeInfo for the bufferless collection scan or change stream
   * application.
   */
  public BufferlessIdOrderInitialSyncResumeInfo(
      BsonTimestamp highWaterMark, BsonValue lastScannedId) {
    this.highWaterMark = highWaterMark;
    this.lastScannedId = lastScannedId;
  }

  @Override
  public final BsonDocument toBson() {
    return BsonDocumentBuilder.builder()
        .field(InitialSyncResumeInfo.Fields.BUFFERLESS_RESUME_INFO, Optional.of(this))
        .build();
  }

  @Override
  public BsonTimestamp getResumeOperationTime() {
    return this.highWaterMark;
  }

  @Override
  public BsonValue getResumeToken() {
    return this.lastScannedId;
  }

  @Override
  protected BsonValue resumeInfoToBson() {
    return BsonDocumentBuilder.builder()
        .field(Fields.HIGH_WATER_MARK, this.highWaterMark.getValue())
        .field(Fields.LAST_SCANNED_ID, this.lastScannedId)
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
    BufferlessIdOrderInitialSyncResumeInfo that = (BufferlessIdOrderInitialSyncResumeInfo) o;
    return this.highWaterMark.equals(that.highWaterMark)
        && this.lastScannedId.equals(that.lastScannedId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.highWaterMark, this.lastScannedId);
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", "BufferlessIdOrderInitialSyncResumeInfo[", "]")
        .add("highWaterMark=" + this.highWaterMark)
        .add("lastScannedId=" + this.lastScannedId)
        .toString();
  }
}
