package com.xgen.mongot.replication.mongodb.common;

import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import java.util.Objects;
import java.util.Optional;
import java.util.StringJoiner;
import org.bson.BsonDocument;

/**
 * Per-partition resume entry inside a {@link BufferlessParallelNaturalOrderInitialSyncResumeInfo},
 * carrying what a resume needs to re-open this partition's cursor or skip it if it already
 * finished.
 *
 * <ul>
 *   <li>{@code partitionId}: the partition's index, 0-based in scan order.
 *   <li>{@code lowerRecordId}: exclusive lower bound; empty for the first partition.
 *   <li>{@code upperRecordId}: inclusive upper bound; empty for the last partition, which runs to
 *       end-of-cursor.
 *   <li>{@code resumeRecordId}: last committed recordId, empty before this partition's first
 *       commit. Combined with the parent's {@code initialSyncId} to build the cursor's {@code
 *       $_startAt}.
 *   <li>{@code isDone}: the partition consumed its entire range, so resume opens no cursor for it.
 * </ul>
 */
public class PartitionResumeInfo implements DocumentEncodable {

  static class Fields {
    static final Field.Required<Integer> PARTITION_ID =
        Field.builder("partitionId").intField().required();

    static final Field.Optional<Long> LOWER_RECORD_ID =
        Field.builder("lowerRecordId").longField().optional().noDefault();

    static final Field.Optional<Long> UPPER_RECORD_ID =
        Field.builder("upperRecordId").longField().optional().noDefault();

    static final Field.Optional<Long> RESUME_RECORD_ID =
        Field.builder("resumeRecordId").longField().optional().noDefault();

    static final Field.Required<Boolean> IS_DONE =
        Field.builder("isDone").booleanField().required();
  }

  private final int partitionId;
  private final Optional<Long> lowerRecordId;
  private final Optional<Long> upperRecordId;
  private final Optional<Long> resumeRecordId;
  private final boolean isDone;

  public PartitionResumeInfo(
      int partitionId,
      Optional<Long> lowerRecordId,
      Optional<Long> upperRecordId,
      Optional<Long> resumeRecordId,
      boolean isDone) {
    this.partitionId = partitionId;
    this.lowerRecordId = lowerRecordId;
    this.upperRecordId = upperRecordId;
    this.resumeRecordId = resumeRecordId;
    this.isDone = isDone;
  }

  public static PartitionResumeInfo deserialize(DocumentParser parser) throws BsonParseException {
    return new PartitionResumeInfo(
        parser.getField(Fields.PARTITION_ID).unwrap(),
        parser.getField(Fields.LOWER_RECORD_ID).unwrap(),
        parser.getField(Fields.UPPER_RECORD_ID).unwrap(),
        parser.getField(Fields.RESUME_RECORD_ID).unwrap(),
        parser.getField(Fields.IS_DONE).unwrap());
  }

  public int getPartitionId() {
    return this.partitionId;
  }

  public Optional<Long> getLowerRecordId() {
    return this.lowerRecordId;
  }

  public Optional<Long> getUpperRecordId() {
    return this.upperRecordId;
  }

  public Optional<Long> getResumeRecordId() {
    return this.resumeRecordId;
  }

  public boolean isDone() {
    return this.isDone;
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder()
        .field(Fields.PARTITION_ID, this.partitionId)
        .field(Fields.LOWER_RECORD_ID, this.lowerRecordId)
        .field(Fields.UPPER_RECORD_ID, this.upperRecordId)
        .field(Fields.RESUME_RECORD_ID, this.resumeRecordId)
        .field(Fields.IS_DONE, this.isDone)
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
    PartitionResumeInfo that = (PartitionResumeInfo) o;
    return this.partitionId == that.partitionId
        && this.isDone == that.isDone
        && this.lowerRecordId.equals(that.lowerRecordId)
        && this.upperRecordId.equals(that.upperRecordId)
        && this.resumeRecordId.equals(that.resumeRecordId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        this.partitionId, this.lowerRecordId, this.upperRecordId, this.resumeRecordId, this.isDone);
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", "PartitionResumeInfo[", "]")
        .add("partitionId=" + this.partitionId)
        .add("lowerRecordId=" + this.lowerRecordId)
        .add("upperRecordId=" + this.upperRecordId)
        .add("resumeRecordId=" + this.resumeRecordId)
        .add("isDone=" + this.isDone)
        .toString();
  }
}
