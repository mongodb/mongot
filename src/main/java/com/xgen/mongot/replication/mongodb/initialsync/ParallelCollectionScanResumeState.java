package com.xgen.mongot.replication.mongodb.initialsync;

import com.xgen.mongot.index.version.IndexFormatVersion;
import com.xgen.mongot.replication.mongodb.common.BufferlessParallelNaturalOrderInitialSyncResumeInfo;
import com.xgen.mongot.replication.mongodb.common.IndexCommitUserData;
import java.util.List;
import java.util.Optional;
import java.util.StringJoiner;
import java.util.UUID;
import org.bson.BsonTimestamp;

/**
 * In-memory, run-level aggregate for a parallel natural-order collection scan: the K per-partition
 * {@link ParallelCollectionScanPartitionState} elements, the shared {@code highWaterMark}, and the
 * run metadata stamped on every commit.
 *
 * <p>{@link #snapshot()} is the commit-data supplier. The periodic committer calls it to assemble a
 * fresh {@link IndexCommitUserData} from the live components as it fires, rather than from a blob
 * staged ahead of time. Each component has exactly one writer (a worker writes only its own
 * partition's {@code resumeRecordId}; only the single-threaded change-stream applier writes {@code
 * highWaterMark}), so no partition's commit can regress another's position.
 */
public class ParallelCollectionScanResumeState {

  private final IndexFormatVersion indexFormatVersion;
  private final UUID initialSyncId;
  private final Optional<String> syncSourceHost;
  private final List<ParallelCollectionScanPartitionState> partitions;
  private volatile BsonTimestamp highWaterMark;

  public ParallelCollectionScanResumeState(
      IndexFormatVersion indexFormatVersion,
      UUID initialSyncId,
      Optional<String> syncSourceHost,
      BsonTimestamp highWaterMark,
      List<ParallelCollectionScanPartitionState> partitions) {
    this.indexFormatVersion = indexFormatVersion;
    this.initialSyncId = initialSyncId;
    this.syncSourceHost = syncSourceHost;
    this.highWaterMark = highWaterMark;
    this.partitions = List.copyOf(partitions);
  }

  /**
   * Rebuilds the live run state from a persisted commit blob on resume. The format version is not
   * in the blob; it comes from the index generation being resumed.
   */
  public static ParallelCollectionScanResumeState fromPersisted(
      IndexFormatVersion indexFormatVersion,
      BufferlessParallelNaturalOrderInitialSyncResumeInfo persisted) {
    return new ParallelCollectionScanResumeState(
        indexFormatVersion,
        persisted.getInitialSyncId(),
        persisted.getSyncSourceHost(),
        persisted.getResumeOperationTime(),
        persisted.getPartitions().stream()
            .map(ParallelCollectionScanPartitionState::fromPersisted)
            .toList());
  }

  public UUID getInitialSyncId() {
    return this.initialSyncId;
  }

  public Optional<String> getSyncSourceHost() {
    return this.syncSourceHost;
  }

  public BsonTimestamp getHighWaterMark() {
    return this.highWaterMark;
  }

  public List<ParallelCollectionScanPartitionState> getPartitions() {
    return this.partitions;
  }

  /**
   * Advances the shared high-water mark. Called only by the change-stream applier between scan
   * windows; it becomes the {@code afterClusterTime} floor for the next scan window and the {@code
   * startAtOperationTime} for the next apply window.
   */
  public void advanceHighWaterMark(BsonTimestamp highWaterMark) {
    this.highWaterMark = highWaterMark;
  }

  /**
   * Assembles the current durable resume position into an {@link IndexCommitUserData}, reading the
   * live components at the instant of the call.
   */
  public IndexCommitUserData snapshot() {
    BufferlessParallelNaturalOrderInitialSyncResumeInfo resumeInfo =
        new BufferlessParallelNaturalOrderInitialSyncResumeInfo(
            this.highWaterMark,
            this.initialSyncId,
            this.partitions.stream()
                .map(ParallelCollectionScanPartitionState::toPersisted)
                .toList(),
            this.syncSourceHost);
    return IndexCommitUserData.createInitialSyncResume(this.indexFormatVersion, resumeInfo);
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", "ParallelCollectionScanResumeState[", "]")
        .add("highWaterMark=" + this.highWaterMark)
        .add("initialSyncId=" + this.initialSyncId)
        .add("syncSourceHost=" + this.syncSourceHost)
        .add("partitions=" + this.partitions)
        .toString();
  }
}
