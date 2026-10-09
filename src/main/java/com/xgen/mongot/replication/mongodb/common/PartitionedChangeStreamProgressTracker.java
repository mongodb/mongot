package com.xgen.mongot.replication.mongodb.common;

import static com.xgen.mongot.util.Check.checkArg;

import com.google.errorprone.annotations.concurrent.GuardedBy;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Map;
import org.bson.BsonDocument;

/**
 * Tracks the contiguous indexed prefix of each partition for one replication attempt.
 *
 * <p>The replication owner registers batches in reader order and runs their acknowledgments only
 * after successful indexing. Registration, acknowledgment, and snapshots are thread-safe. Tokens
 * are opaque: registration order determines progress, including for empty batches.
 *
 * <p>Create a new tracker when restarting an attempt, so callbacks from the old attempt cannot
 * affect the new one. A failed batch remains a gap until that attempt is discarded.
 */
public final class PartitionedChangeStreamProgressTracker {
  @GuardedBy("this")
  private final Map<ChangeStreamPartition, PartitionProgress> partitions;

  /**
   * Seeds every partition with its starting position — in practice the shared restart token for
   * every partition. The map must contain one complete partition layout.
   */
  public PartitionedChangeStreamProgressTracker(
      Map<ChangeStreamPartition, BsonDocument> initialPositions) {
    checkArg(!initialPositions.isEmpty(), "initialPositions must contain every partition");
    this.partitions = new HashMap<>();
    initialPositions.forEach(
        (partition, token) -> {
          // IDs are unique map keys and bounded by partitionCount, so this also detects gaps.
          checkArg(
              partition.partitionCount() == initialPositions.size(),
              "initialPositions must contain one complete partition layout");
          this.partitions.put(partition, new PartitionProgress(token.clone()));
        });
  }

  /**
   * Registers a batch before scheduling it and returns its idempotent indexing acknowledgment. Call
   * in reader order, including for empty batches, and attach the result to the successful
   * completion of all indexing work for this batch (for example, with {@code thenRun}). Fetching,
   * decoding, failure, and cancellation must not acknowledge a batch.
   */
  public synchronized Runnable registerBatch(
      ChangeStreamPartition partition, BsonDocument postBatchResumeToken) {
    checkArg(this.partitions.containsKey(partition), "Unknown partition: %s", partition);
    PartitionProgress progress = this.partitions.get(partition);
    PendingBatch batch = new PendingBatch(postBatchResumeToken.clone());
    progress.pending.addLast(batch);
    return () -> acknowledge(progress, batch);
  }

  /**
   * Returns a detached snapshot containing every partition, including those with no indexed work.
   * These positions are eligible for a commit, not durable by themselves: the commit must include
   * all indexed changes through its saved positions. Snapshot before taking the writer's commit
   * snapshot, so later acknowledgments cannot move metadata ahead of the committed data.
   */
  public synchronized Map<ChangeStreamPartition, BsonDocument> snapshot() {
    Map<ChangeStreamPartition, BsonDocument> result = new HashMap<>();
    this.partitions.forEach(
        (partition, progress) -> result.put(partition, progress.confirmedToken.clone()));
    return Map.copyOf(result);
  }

  private synchronized void acknowledge(PartitionProgress progress, PendingBatch batch) {
    batch.indexed = true;
    while (!progress.pending.isEmpty() && progress.pending.getFirst().indexed) {
      progress.confirmedToken = progress.pending.removeFirst().token;
    }
  }

  // All progress and batch state is accessed under the tracker's monitor.
  private static final class PartitionProgress {
    private BsonDocument confirmedToken;
    private final ArrayDeque<PendingBatch> pending;

    private PartitionProgress(BsonDocument confirmedToken) {
      this.confirmedToken = confirmedToken;
      this.pending = new ArrayDeque<>();
    }
  }

  private static final class PendingBatch {
    private final BsonDocument token;
    private boolean indexed;

    private PendingBatch(BsonDocument token) {
      this.token = token;
      this.indexed = false;
    }
  }
}
