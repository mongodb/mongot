package com.xgen.mongot.config.manager;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import org.bson.types.ObjectId;

/**
 * The indexes mongot cannot use because their config journal entry could not be parsed, held by
 * index id and split by journal state. This class is not thread-safe.
 *
 * <p>Staged and live are held separately because an index can have a quarantined generation in each
 * at once.
 *
 * <p>Deleted entries are not tracked: {@link ConfigInitializer} drops everything in the journal's
 * deleted array at startup, so none are left to report on.
 */
public class IncompatibleIndexes {

  private final ConcurrentHashMap<ObjectId, QuarantinedIndex> stagedIndexes =
      new ConcurrentHashMap<>();
  private final ConcurrentHashMap<ObjectId, QuarantinedIndex> liveIndexes =
      new ConcurrentHashMap<>();

  /** Adds a quarantined staged index, replacing any already tracked for the same index. */
  public void addStagedIndex(QuarantinedIndex quarantinedIndex) {
    this.stagedIndexes.put(quarantinedIndex.indexId(), quarantinedIndex);
  }

  /** Adds a quarantined live index, replacing any already tracked for the same index. */
  public void addLiveIndex(QuarantinedIndex quarantinedIndex) {
    this.liveIndexes.put(quarantinedIndex.indexId(), quarantinedIndex);
  }

  public Collection<QuarantinedIndex> getStagedIndexes() {
    return new ArrayList<>(this.stagedIndexes.values());
  }

  public Collection<QuarantinedIndex> getLiveIndexes() {
    return new ArrayList<>(this.liveIndexes.values());
  }
}
