package com.xgen.mongot.replication.mongodb.steadystate.changestream;

import com.xgen.mongot.replication.mongodb.common.ChangeStreamBatch;

/**
 * Decides whether the next getMore for an index should be routed to the preferred or regular queue,
 * based on the previous batch.
 */
interface ChangeStreamPriorityStrategy {

  /**
   * Decides whether the index's next getMore should be treated as high priority.
   *
   * @param batch the index's just-processed batch
   * @return {@code true} if the index's next getMore should be routed to the preferred queue,
   *     {@code false} to route it to the regular queue
   */
  boolean isHighPriority(ChangeStreamBatch batch);
}
