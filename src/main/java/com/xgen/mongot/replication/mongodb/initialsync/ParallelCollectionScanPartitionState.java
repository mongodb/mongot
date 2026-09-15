package com.xgen.mongot.replication.mongodb.initialsync;

import com.xgen.mongot.replication.mongodb.common.PartitionResumeInfo;
import java.util.Optional;
import java.util.StringJoiner;

/**
 * In-memory, mutable per-partition session state for a parallel natural-order collection scan. One
 * instance per partition, held in a fixed-size list by {@link ParallelCollectionScanResumeState}.
 * It is the live counterpart of the persisted {@link PartitionResumeInfo}: a fresh scanner is built
 * per scan window, so a partition's progress lives here (surviving across windows), not on the
 * one-shot scanner.
 *
 * <p>Concurrency: {@code resumeRecordId} and {@code isDone} are {@code volatile}. {@code
 * resumeRecordId} has a single writer per cycle (that partition's current worker, which awaits each
 * batch's landing serially); {@code isDone} is written by the cycle on the manager thread via
 * {@link #onCycleEnd(boolean)}.
 */
public class ParallelCollectionScanPartitionState {

  private final int partitionId;
  private final Optional<Long> lowerRecordId;
  private final Optional<Long> upperRecordId;

  private volatile Optional<Long> resumeRecordId;
  private volatile boolean isDone;

  public ParallelCollectionScanPartitionState(
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

  /** Rebuilds the live session state from a persisted per-partition entry on resume. */
  public static ParallelCollectionScanPartitionState fromPersisted(PartitionResumeInfo persisted) {
    return new ParallelCollectionScanPartitionState(
        persisted.getPartitionId(),
        persisted.getLowerRecordId(),
        persisted.getUpperRecordId(),
        persisted.getResumeRecordId(),
        persisted.isDone());
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

  /**
   * Advances the durable resume position. Called by the partition's worker once a batch has landed
   * in Lucene's RAM buffer, which makes it a crash-safe lower bound on any commit snapshot.
   *
   * <p>The caller must advance monotonically and this deliberately does not check: one worker per
   * cycle awaits each batch before submitting the next, and a natural-order cursor yields
   * increasing recordIds.
   */
  public void advanceResumeRecordId(long recordId) {
    this.resumeRecordId = Optional.of(recordId);
  }

  /**
   * Records whether the partition consumed its entire range. Called on the manager thread once the
   * worker's future completes, success or failure.
   */
  public void onCycleEnd(boolean isDone) {
    this.isDone = isDone;
  }

  /** Produces the persisted, immutable mirror of this partition's current state. */
  public PartitionResumeInfo toPersisted() {
    return new PartitionResumeInfo(
        this.partitionId, this.lowerRecordId, this.upperRecordId, this.resumeRecordId, this.isDone);
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", "ParallelCollectionScanPartitionState[", "]")
        .add("partitionId=" + this.partitionId)
        .add("lowerRecordId=" + this.lowerRecordId)
        .add("upperRecordId=" + this.upperRecordId)
        .add("resumeRecordId=" + this.resumeRecordId)
        .add("isDone=" + this.isDone)
        .toString();
  }
}
