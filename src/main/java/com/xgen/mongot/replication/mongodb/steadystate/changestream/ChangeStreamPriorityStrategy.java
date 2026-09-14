package com.xgen.mongot.replication.mongodb.steadystate.changestream;

import com.xgen.mongot.replication.mongodb.common.ChangeStreamBatch;
import org.bson.types.ObjectId;

/**
 * Decides whether the next getMore for an index should be routed to the preferred or regular queue,
 * based on the previous batch.
 */
interface ChangeStreamPriorityStrategy {

  /**
   * Decides whether the index's next getMore should be treated as high priority.
   *
   * @param indexId the id of the index this batch belongs to
   * @param batch the index's just-processed batch
   * @return {@code true} if the index's next getMore should be routed to the preferred queue,
   *     {@code false} to route it to the regular queue
   */
  boolean isHighPriority(ObjectId indexId, ChangeStreamBatch batch);

  /**
   * Notifies the strategy that an index is starting up, before any of its batches are submitted.
   * Strategies that compare indexes against one another use this to establish which indexes are
   * active, so that a batch arriving late — after {@link #removeIndex} — cannot re-introduce a
   * stopped index.
   *
   * @param indexId the id of the index that is starting up
   */
  default void addIndex(ObjectId indexId) {}

  /**
   * Notifies the strategy that an index has been removed and any per-index state held for it should
   * be discarded. Paired with {@link #addIndex}; see there for the ordering guarantees.
   *
   * @param indexId the id of the index that was removed
   */
  default void removeIndex(ObjectId indexId) {}

  /** Releases any resources held by the strategy. Called when the owning executor shuts down. */
  default void shutdown() {}
}
