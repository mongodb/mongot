package com.xgen.mongot.replication.mongodb.steadystate.changestream;

import com.xgen.mongot.replication.mongodb.common.ChangeStreamBatch;
import com.xgen.mongot.replication.mongodb.common.ResumeTokenUtils;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.codec.DecoderException;
import org.bson.BsonTimestamp;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Treats an index as high priority if its change-stream cursor is at least a configured number of
 * seconds behind the server's current optime. With partitioned readers, an index is judged by its
 * slowest partition. An unpartitioned reader is judged by its own batch alone.
 */
final class OptimeLagPriorityStrategy implements ChangeStreamPriorityStrategy {

  private static final Logger LOG = LoggerFactory.getLogger(OptimeLagPriorityStrategy.class);

  private final int highPriorityLagThresholdSeconds;

  /** Latest lag per partition for each added index. Only partitioned batches are recorded. */
  private final ConcurrentHashMap<ObjectId, ConcurrentHashMap<Integer, Integer>>
      lagSecondsByIndexAndPartition = new ConcurrentHashMap<>();

  /** How many live index generations share each key of {@link #lagSecondsByIndexAndPartition}. */
  private final ConcurrentHashMap<ObjectId, Integer> activeGenerationsByIndex =
      new ConcurrentHashMap<>();

  OptimeLagPriorityStrategy(int highPriorityLagThresholdSeconds) {
    if (highPriorityLagThresholdSeconds < 0) {
      throw new IllegalArgumentException(
          "highPriorityLagThresholdSeconds must be non-negative, got: "
              + highPriorityLagThresholdSeconds);
    }
    this.highPriorityLagThresholdSeconds = highPriorityLagThresholdSeconds;
  }

  @Override
  public void addIndex(ObjectId indexId) {
    this.activeGenerationsByIndex.merge(indexId, 1, Integer::sum);
    this.lagSecondsByIndexAndPartition.putIfAbsent(indexId, new ConcurrentHashMap<>());
  }

  @Override
  public void removeIndex(ObjectId indexId) {
    // Drop the lag entries only once the last generation sharing them is gone.
    Integer remainingGenerations =
        this.activeGenerationsByIndex.computeIfPresent(
            indexId, (id, count) -> count == 1 ? null : count - 1);
    if (remainingGenerations == null) {
      this.lagSecondsByIndexAndPartition.remove(indexId);
    }
  }

  @Override
  public boolean isHighPriority(ObjectId indexId, ChangeStreamBatch batch) {
    int lagSeconds = computeLagSeconds(batch);
    if (batch.getPartition().isEmpty()) {
      return lagSeconds >= this.highPriorityLagThresholdSeconds;
    }
    var lagByPartition = this.lagSecondsByIndexAndPartition.get(indexId);
    if (lagByPartition == null) {
      // Not added, or already removed: judge this batch alone and keep no state for it.
      return lagSeconds >= this.highPriorityLagThresholdSeconds;
    }
    int partitionId = batch.getPartition().get().partitionId();
    lagByPartition.put(partitionId, lagSeconds);
    int worstLagSeconds =
        lagByPartition.values().stream().mapToInt(Integer::intValue).max().orElse(lagSeconds);
    return worstLagSeconds >= this.highPriorityLagThresholdSeconds;
  }

  /**
   * Seconds the batch is behind the server's optime. Returns {@link Integer#MAX_VALUE} when that
   * can't be computed, so the partition counts as lagging: preferring a healthy index costs less
   * than leaving a genuinely lagging one unpreferred.
   */
  private static int computeLagSeconds(ChangeStreamBatch batch) {
    // commandOperationTime is the server optime when the getMore ran, and the resume token can only
    // be at or behind it, so lag is normally >= 0. A negative value means the index is caught up,
    // and is treated as zero lag.
    BsonTimestamp resumeTokenTime;
    try {
      resumeTokenTime = ResumeTokenUtils.opTimeFromResumeToken(batch.getPostBatchResumeToken());
    } catch (DecoderException e) {
      LOG.warn(
          "Failed to decode resume token for optime lag check; defaulting to high priority", e);
      return Integer.MAX_VALUE;
    }
    Optional<BsonTimestamp> commandOperationTime = batch.getCommandOperationTime();
    if (commandOperationTime.isEmpty()) {
      // "No information yet" counts as lagging, matching the resume-token decode failure above.
      return Integer.MAX_VALUE;
    }
    return Math.max(0, commandOperationTime.get().getTime() - resumeTokenTime.getTime());
  }
}
