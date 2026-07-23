package com.xgen.mongot.replication.mongodb.steadystate.changestream;

import com.google.errorprone.annotations.Var;
import com.mongodb.MongoNamespace;
import com.xgen.mongot.index.version.GenerationId;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.bson.BsonTimestamp;
import org.jetbrains.annotations.Nullable;

/**
 * Holds the per-collection roster and demux state for {@link ChangeStreamActivityObserver}: which
 * search indexes are managed on each collection, the namespace-to-collection demux index, and the
 * most recent write activity per collection. Also derives per-collection idle times from oplog
 * cluster times.
 *
 * <p>This is a pure state container: it performs no threading, stream, or scheduling work. Its maps
 * are concurrent so gauge scrapes can read them while {@code register}/{@code deregister} (called
 * under the {@link ChangeStreamManager} lock) and the read loop mutate them.
 */
class CollectionRoster {

  /** Roster: the managed indexes per collection, keyed by the collection's stable UUID. */
  private final Map<UUID, Set<GenerationId>> indexesByCollection;

  /** The cluster time of the most recent event seen per collection. */
  private final Map<UUID, BsonTimestamp> lastActivityByCollection;

  /** Demux index from an event's namespace to its collection UUID. */
  private final Map<MongoNamespace, UUID> collectionByNamespace;

  /** The current name per collection, used to keep the demux index correct across a rename. */
  private final Map<UUID, MongoNamespace> namespaceByCollection;

  /** Reverse index so deregister, which receives only a GenerationId, can find its collection. */
  private final Map<GenerationId, UUID> collectionByIndex;

  CollectionRoster() {
    this.indexesByCollection = new ConcurrentHashMap<>();
    this.lastActivityByCollection = new ConcurrentHashMap<>();
    this.collectionByNamespace = new ConcurrentHashMap<>();
    this.namespaceByCollection = new ConcurrentHashMap<>();
    this.collectionByIndex = new ConcurrentHashMap<>();
  }

  /**
   * Registers an index and its collection with the roster. If the collection is new to the roster
   * and {@code seedPosition} is non-null, its activity baseline is seeded to that position.
   */
  void register(
      GenerationId generationId,
      MongoNamespace namespace,
      UUID collectionUuid,
      @Nullable BsonTimestamp seedPosition) {
    this.collectionByIndex.put(generationId, collectionUuid);
    Set<GenerationId> indexes =
        this.indexesByCollection.computeIfAbsent(
            collectionUuid, uuid -> ConcurrentHashMap.newKeySet());
    if (indexes.isEmpty()) {
      // The collection is (re)entering the roster. Clear any activity a racing read-loop write
      // left behind after a past deregistration, before the demux mapping below makes the
      // collection reachable again, so the baseline seeds fresh instead of reporting stale idle.
      this.lastActivityByCollection.remove(collectionUuid);
    }
    indexes.add(generationId);

    // Update the demux index for the collection's current name. The new name is mapped before the
    // old one is removed so a concurrently demuxing batch never sees the collection unmapped.
    // This is what keeps demuxing correct across a collection rename.
    MongoNamespace previousNamespace = this.namespaceByCollection.put(collectionUuid, namespace);
    UUID previousOwner = this.collectionByNamespace.put(namespace, collectionUuid);
    if (previousOwner != null && !previousOwner.equals(collectionUuid)) {
      // Another tracked collection previously held this name (drop/recreate or rename cycles).
      // Clear its stale name entry so a later deregister cannot unmap the wrong collection.
      this.namespaceByCollection.remove(previousOwner, namespace);
    }
    if (previousNamespace != null && !previousNamespace.equals(namespace)) {
      this.collectionByNamespace.remove(previousNamespace, collectionUuid);
    }

    if (seedPosition != null) {
      this.lastActivityByCollection.putIfAbsent(collectionUuid, seedPosition);
    }
  }

  /**
   * Deregisters an index, removing its collection from the roster when it was the last index on it.
   *
   * @return true when the roster tracks no indexes afterwards, so the caller may close the observer
   */
  boolean deregister(GenerationId generationId) {
    UUID collectionUuid = this.collectionByIndex.remove(generationId);
    if (collectionUuid != null) {
      Set<GenerationId> indexes = this.indexesByCollection.get(collectionUuid);
      if (indexes != null) {
        indexes.remove(generationId);
        if (indexes.isEmpty()) {
          this.indexesByCollection.remove(collectionUuid);
          this.lastActivityByCollection.remove(collectionUuid);
          MongoNamespace namespace = this.namespaceByCollection.remove(collectionUuid);
          if (namespace != null) {
            this.collectionByNamespace.remove(namespace, collectionUuid);
          }
        }
      }
    }
    return this.collectionByIndex.isEmpty();
  }

  /**
   * Returns the collection UUID for the supplied namespace only if that collection is currently
   * tracked (has at least one managed index), otherwise empty.
   */
  Optional<UUID> trackedCollectionFor(MongoNamespace namespace) {
    UUID collectionUuid = this.collectionByNamespace.get(namespace);
    if (collectionUuid == null || !this.indexesByCollection.containsKey(collectionUuid)) {
      // The collection has no managed index (or was deregistered concurrently; a residual write
      // slipping through the remaining race window is cleared on re-registration).
      return Optional.empty();
    }
    return Optional.of(collectionUuid);
  }

  /** Records the cluster time of the most recent event seen for a collection. */
  void recordActivity(UUID collectionUuid, BsonTimestamp clusterTime) {
    this.lastActivityByCollection.put(collectionUuid, clusterTime);
  }

  /**
   * Seeds baselines for collections registered before the first batch (or after a baseline reset),
   * so a collection that is never written still ages from a defined point.
   */
  void seedBaselines(BsonTimestamp position) {
    for (UUID collectionUuid : this.indexesByCollection.keySet()) {
      this.lastActivityByCollection.putIfAbsent(collectionUuid, position);
    }
  }

  /** Clears all recorded activity so baselines re-seed from the next processed position. */
  void clearActivity() {
    this.lastActivityByCollection.clear();
  }

  /** The number of indexes tracked across all collections. */
  int trackedIndexCount() {
    return this.collectionByIndex.size();
  }

  /** The number of collections tracked. */
  int trackedCollectionCount() {
    return this.indexesByCollection.size();
  }

  /**
   * Counts the indexes on collections whose idle time at the supplied position is at least {@code
   * thresholdSeconds}.
   */
  long countInactiveIndexes(BsonTimestamp position, long thresholdSeconds) {
    @Var long inactive = 0;
    for (Map.Entry<UUID, Set<GenerationId>> entry : this.indexesByCollection.entrySet()) {
      if (this.idleSeconds(entry.getKey(), position) >= thresholdSeconds) {
        inactive += entry.getValue().size();
      }
    }
    return inactive;
  }

  /**
   * The idle seconds for a single index, looked up by its {@link GenerationId} via its collection.
   * All indexes on a collection share the value because they tail the same events. Returns zero
   * when the index is not tracked or {@code position} is null (no batch processed yet).
   */
  long idleSecondsForIndex(GenerationId generationId, @Nullable BsonTimestamp position) {
    if (position == null) {
      return 0;
    }
    UUID collectionUuid = this.collectionByIndex.get(generationId);
    if (collectionUuid == null) {
      return 0;
    }
    return this.idleSeconds(collectionUuid, position);
  }

  /**
   * A collection's idle seconds derived from optimes: the current stream position minus the cluster
   * time of the collection's most recent event. A collection whose baseline has not been seeded yet
   * reports zero.
   */
  private long idleSeconds(UUID collectionUuid, BsonTimestamp position) {
    BsonTimestamp lastActivity = this.lastActivityByCollection.get(collectionUuid);
    if (lastActivity == null) {
      return 0;
    }
    return Math.max(0, (long) position.getTime() - lastActivity.getTime());
  }
}
