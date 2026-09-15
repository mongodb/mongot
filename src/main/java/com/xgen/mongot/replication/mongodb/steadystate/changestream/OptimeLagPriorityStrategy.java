package com.xgen.mongot.replication.mongodb.steadystate.changestream;

import com.xgen.mongot.replication.mongodb.common.ChangeStreamBatch;
import com.xgen.mongot.replication.mongodb.common.ResumeTokenUtils;
import java.util.Optional;
import org.apache.commons.codec.DecoderException;
import org.bson.BsonTimestamp;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Treats an index as high priority if its change-stream cursor is at least a configured number of
 * seconds behind the server's current optime.
 */
final class OptimeLagPriorityStrategy implements ChangeStreamPriorityStrategy {

  private static final Logger LOG = LoggerFactory.getLogger(OptimeLagPriorityStrategy.class);

  private final int highPriorityLagThresholdSeconds;

  OptimeLagPriorityStrategy(int highPriorityLagThresholdSeconds) {
    if (highPriorityLagThresholdSeconds < 0) {
      throw new IllegalArgumentException(
          "highPriorityLagThresholdSeconds must be non-negative, got: "
              + highPriorityLagThresholdSeconds);
    }
    this.highPriorityLagThresholdSeconds = highPriorityLagThresholdSeconds;
  }

  @Override
  public boolean isHighPriority(ObjectId indexId, ChangeStreamBatch batch) {
    // commandOperationTime is the server optime when the getMore ran, and the resume token can only
    // be at or behind it, so lag is normally >= 0. A negative value means the index is caught up,
    // and is treated as zero lag.
    BsonTimestamp resumeTokenTime;
    try {
      resumeTokenTime = ResumeTokenUtils.opTimeFromResumeToken(batch.getPostBatchResumeToken());
    } catch (DecoderException e) {
      LOG.warn(
          "Failed to decode resume token for optime lag check; defaulting to high priority", e);
      return true;
    }
    Optional<BsonTimestamp> commandOperationTime = batch.getCommandOperationTime();
    if (commandOperationTime.isEmpty()) {
      // Default "no information yet" to high priority, matching the resume-token decode failure
      // convention above: the cost of treating a healthy index as preferred in scheduling is lower
      // than the cost of a genuinely lagging index not being treated as preferred.
      return true;
    }
    int lagSeconds = Math.max(0, commandOperationTime.get().getTime() - resumeTokenTime.getTime());
    return lagSeconds >= this.highPriorityLagThresholdSeconds;
  }
}
