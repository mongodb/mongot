package com.xgen.mongot.replication.mongodb.common;

import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import com.xgen.mongot.util.bson.parser.Value;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.StringJoiner;
import java.util.UUID;
import org.bson.BsonDocument;
import org.bson.BsonTimestamp;
import org.bson.BsonValue;

/**
 * Resume info for a parallel natural-order collection scan, which splits the collection into K
 * partitions.
 *
 * <p>Persisted via {@link IndexCommitUserData} on every commit, which ties the durability of these
 * positions to that of the segments they describe: Lucene fsyncs both as one unit, so on crash the
 * persisted positions never run ahead of what is on disk.
 *
 * <p>Schema:
 *
 * <ul>
 *   <li>{@code highWaterMark}: The opTime of the last event seen during change stream application.
 *   <li>{@code initialSyncId}: Captured at planning time and persisted with the boundaries it
 *       grounds.
 *   <li>{@code partitions}: K self-contained entries, each carrying its own bounds.
 *   <li>{@code syncSourceHost}: the host this sync replicates from.
 * </ul>
 */
public class BufferlessParallelNaturalOrderInitialSyncResumeInfo extends InitialSyncResumeInfo {

  private static class Fields {
    private static final Field.Required<Long> HIGH_WATER_MARK =
        Field.builder("highWaterMark").longField().required();

    // Always a mongo::UUID (subtype 4), appended by CollectionScan::getPostBatchResumeToken:
    // https://github.com/mongodb/mongo/blob/f50352a5c2890fc6f5ed1d2ae50d8762117316a9/src/mongo/db/exec/classic/collection_scan.cpp#L428-L436
    private static final Field.Required<UUID> INITIAL_SYNC_ID =
        Field.builder("initialSyncId").uuidField().required();

    private static final Field.Required<List<PartitionResumeInfo>> PARTITIONS =
        Field.builder("partitions")
            .listOf(
                Value.builder()
                    .classValue(PartitionResumeInfo::deserialize)
                    .disallowUnknownFields()
                    .required())
            .mustNotBeEmpty()
            .required();

    private static final Field.Optional<String> SYNC_SOURCE_HOST =
        Field.builder("syncSourceHost").stringField().optional().noDefault();
  }

  private final BsonTimestamp highWaterMark;
  private final UUID initialSyncId;
  private final List<PartitionResumeInfo> partitions;
  private final Optional<String> syncSourceHost;

  public BufferlessParallelNaturalOrderInitialSyncResumeInfo(
      BsonTimestamp highWaterMark,
      UUID initialSyncId,
      List<PartitionResumeInfo> partitions,
      Optional<String> syncSourceHost) {
    this.highWaterMark = highWaterMark;
    this.initialSyncId = initialSyncId;
    this.partitions = List.copyOf(partitions);
    this.syncSourceHost = syncSourceHost;
  }

  public static BufferlessParallelNaturalOrderInitialSyncResumeInfo deserialize(
      DocumentParser parser) throws BsonParseException {
    return new BufferlessParallelNaturalOrderInitialSyncResumeInfo(
        new BsonTimestamp(parser.getField(Fields.HIGH_WATER_MARK).unwrap()),
        parser.getField(Fields.INITIAL_SYNC_ID).unwrap(),
        parser.getField(Fields.PARTITIONS).unwrap(),
        parser.getField(Fields.SYNC_SOURCE_HOST).unwrap());
  }

  @Override
  public BsonTimestamp getResumeOperationTime() {
    return this.highWaterMark;
  }

  /**
   * Always throws. Unlike the single-cursor variants, a parallel scan has no single resume token:
   * each partition carries its own position.
   */
  @Override
  public BsonValue getResumeToken() {
    throw new UnsupportedOperationException(
        "A parallel natural-order scan has no single resume token;"
            + " each partition carries its own.");
  }

  @Override
  public Optional<String> getSyncSourceHost() {
    return this.syncSourceHost;
  }

  public UUID getInitialSyncId() {
    return this.initialSyncId;
  }

  public List<PartitionResumeInfo> getPartitions() {
    return this.partitions;
  }

  @Override
  public final BsonDocument toBson() {
    return BsonDocumentBuilder.builder()
        .field(
            InitialSyncResumeInfo.Fields.BUFFERLESS_PARALLEL_NATURAL_ORDER_INITIAL_SYNC_RESUME_INFO,
            Optional.of(this))
        .build();
  }

  @Override
  protected BsonValue resumeInfoToBson() {
    return BsonDocumentBuilder.builder()
        .field(Fields.HIGH_WATER_MARK, this.highWaterMark.getValue())
        .field(Fields.INITIAL_SYNC_ID, this.initialSyncId)
        .field(Fields.PARTITIONS, this.partitions)
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
    BufferlessParallelNaturalOrderInitialSyncResumeInfo that =
        (BufferlessParallelNaturalOrderInitialSyncResumeInfo) o;
    return this.highWaterMark.equals(that.highWaterMark)
        && this.initialSyncId.equals(that.initialSyncId)
        && this.partitions.equals(that.partitions)
        && this.syncSourceHost.equals(that.syncSourceHost);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        this.highWaterMark, this.initialSyncId, this.partitions, this.syncSourceHost);
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", "BufferlessParallelNaturalOrderInitialSyncResumeInfo[", "]")
        .add("highWaterMark=" + this.highWaterMark)
        .add("initialSyncId=" + this.initialSyncId)
        .add("partitions=" + this.partitions)
        .add("syncSourceHost=" + this.syncSourceHost)
        .toString();
  }
}
