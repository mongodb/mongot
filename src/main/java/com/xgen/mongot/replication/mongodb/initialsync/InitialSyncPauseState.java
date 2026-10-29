package com.xgen.mongot.replication.mongodb.initialsync;

import java.util.Collection;
import java.util.Set;
import org.bson.types.ObjectId;

/**
 * Holds the index IDs paused on this host, passed through conf calls. The state is shared with
 * replication managers so their queues can block new initial syncs for those indexes.
 */
public final class InitialSyncPauseState {
  private volatile Set<ObjectId> livePausedIds = Set.of();

  public void updatePausedIndexIds(Collection<ObjectId> ids) {
    this.livePausedIds = Set.copyOf(ids);
  }

  public boolean isPaused(ObjectId indexId) {
    return this.livePausedIds.contains(indexId);
  }
}
