package com.xgen.mongot.embedding.mongodb.leasing;

import com.mongodb.MongoNamespace;
import com.xgen.mongot.embedding.config.MaterializedViewCollectionMetadata;
import com.xgen.mongot.embedding.exceptions.MaterializedViewNonTransientException;
import com.xgen.mongot.embedding.exceptions.MaterializedViewTransientException;
import com.xgen.mongot.index.EncodedUserData;
import com.xgen.mongot.index.IndexGeneration;
import com.xgen.mongot.index.definition.MaterializedViewIndexDefinitionGeneration;
import com.xgen.mongot.index.status.IndexStatus;
import com.xgen.mongot.index.version.MaterializedViewGenerationId;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.bson.BsonTimestamp;
import org.jetbrains.annotations.TestOnly;

// TODO(CLOUDP-371278): Refactor interface to have stricter type that only allows MatView related
// IndexGeneration and GenerationId
/** Interface for managing leases for the materialized view leader. */
public interface LeaseManager {

  long DEFAULT_INDEX_DEFINITION_VERSION = 0;

  /**
   * Result of polling follower statuses from the lease manager.
   *
   * <p>Contains both the current status of each follower generation and the set of generation IDs
   * that are eligible for leadership acquisition. This includes:
   *
   * <ul>
   *   <li>Leases that have expired (previous leader failed or timed out)
   *   <li>Leases owned by this instance (re-acquiring after restart)
   *   <li>New indexes that don't have a lease yet (lease will be created during acquisition)
   * </ul>
   *
   * @param statuses Map of generation IDs to their current replication status
   * @param acquirableLeases Set of generation IDs eligible for leadership acquisition
   */
  record FollowerPollResult(
      Map<MaterializedViewGenerationId, IndexStatus> statuses,
      Set<MaterializedViewGenerationId> acquirableLeases) {
    /** An empty result with no statuses and no acquirable leases. */
    public static final FollowerPollResult EMPTY =
        new FollowerPollResult(Collections.emptyMap(), Collections.emptySet());
  }

  /**
   * Adds a lease for the given index generation. Used for first-time generator setup, where we
   * create a new lease and flag skipInitialSync isn’t used. The value does not make a difference,
   * but we used false here as it aligns with the behavior (initial sync).
   *
   * @param indexGeneration the index generation to add the lease for.
   */
  @TestOnly
  default void add(IndexGeneration indexGeneration) {
    add(indexGeneration, false);
  }

  /**
   * Adds a lease for the given index generation.
   *
   * @param indexGeneration the index generation to add the lease for.
   * @param skipInitialSync when true, preserve existing commitInfo so the new generator can resume
   *     replication from the last checkpoint; when false, the new version gets initial-sync-style
   *     commitInfo from high watermark to trigger initial sync.
   */
  void add(IndexGeneration indexGeneration, boolean skipInitialSync);

  /**
   * Drops the given index generation from the tracking list in LeaseManager.
   *
   * @param generationId the generation id to remove the lease for
   */
  void drop(MaterializedViewGenerationId generationId);

  /**
   * Deletes the lease entirely in both database and memory, only call this when corresponding
   * materialized view collection is GC'd or user deletes the whole index.
   *
   * @param leaseKey the string lease key for lease to be removed, should be same as collection
   *     name.
   * @return a future that completes when the lease has been dropped
   */
  CompletableFuture<Void> dropLease(String leaseKey);

  /**
   * Returns true if the current mongot is the leader for the given generation id.
   *
   * @param generationId the generation id to check
   * @return true if the caller is the leader
   */
  boolean isLeader(MaterializedViewGenerationId generationId);

  /**
   * Updates the commit info (replication checkpoint state) for the given generation id.
   *
   * @param generationId the generation id to update
   * @param encodedUserData the commit info to update
   */
  void updateCommitInfo(MaterializedViewGenerationId generationId, EncodedUserData encodedUserData)
      throws MaterializedViewTransientException, MaterializedViewNonTransientException;

  /**
   * Returns the commit info (replication checkpoint state) for the given generation id.
   *
   * @param generationId the generation id to get the commit info for
   * @return the commit info
   * @throws IOException if there are any issues talking to the lease store
   */
  EncodedUserData getCommitInfo(MaterializedViewGenerationId generationId) throws IOException;

  /**
   * Updates the status of the materialized view replication.
   *
   * @param generationId the generation id of the index generation
   * @param indexDefinitionVersion the index definition version to update
   * @param indexStatus the status to update to
   */
  void updateReplicationStatus(
      MaterializedViewGenerationId generationId,
      long indexDefinitionVersion,
      IndexStatus indexStatus)
      throws MaterializedViewTransientException, MaterializedViewNonTransientException;

  /**
   * Returns every lease in the internal databases known to this manager — the single scan both GC
   * passes work from. Reads the database (not the in-memory map) so orphaned leases are visible,
   * and registers each lease's database mapping for the mark/delete that may follow.
   *
   * <p>Best effort, never throws: corrupted leases are skipped (CLOUDP-384971) and a failed
   * per-database scan drops that database's leases. Partial results are safe — marking requires a
   * returned higher-version sibling, and databases scan all-or-nothing.
   */
  List<Lease> findGcCandidates();

  /**
   * Conditionally marks a lease {@link Lease.CleanupState#ELIGIBLE_FOR_CLEANUP} via an OCC update:
   * the write only commits if the lease is still {@code NOT_ELIGIBLE} (or a legacy lease with no
   * cleanupState field) <em>and</em> its {@code leaseExpiration} is older than {@code
   * expirationCutoff}. Encoding the staleness check in the write makes it self-guarding — a caller
   * acting on a stale in-memory view cannot mark a lease that is still being heartbeated.
   * Idempotent: a no-op returning {@code false} when already marked or not yet stale — {@code true}
   * means this call performed the transition, so concurrent leaderless scanners report each mark
   * exactly once.
   *
   * @param leaseKey the lease document {@code _id}
   * @param expirationCutoff a lease is eligible only if it expired before this instant
   * @return true if this call flipped the lease to {@code ELIGIBLE_FOR_CLEANUP}
   */
  boolean markEligibleForCleanup(String leaseKey, Instant expirationCutoff);

  /**
   * Re-reads {@code leaseKey} and returns its materialized view collection to drop, but only while
   * the lease is still eligible, stale past {@code expirationCutoff}, and at {@code
   * expectedLeaseVersion} — so a lease reused (and version-bumped) since the sweep is left alone.
   * Best effort, never throws.
   */
  Optional<MongoNamespace> resolveStaleMatViewNamespace(
      String leaseKey, long expectedLeaseVersion, Instant expirationCutoff);

  /**
   * Deletes the lease {@code leaseKey} — the final cleanup step, after its collection is dropped.
   * Commits only while the lease is still eligible, stale past {@code expirationCutoff}, and at
   * {@code expectedLeaseVersion}, so a reused lease is left in place. Best effort, never throws.
   */
  boolean deleteStaleLease(String leaseKey, long expectedLeaseVersion, Instant expirationCutoff);

  /**
   * Returns all generation IDs where this instance is the leader.
   *
   * @return a set of generation IDs where this instance is the leader
   */
  Set<MaterializedViewGenerationId> getLeaderGenerationIds();

  /**
   * Returns all generation IDs where this instance is a follower.
   *
   * @return a set of generation IDs where this instance is a follower
   */
  Set<MaterializedViewGenerationId> getFollowerGenerationIds();

  /**
   * Polls the status of all follower materialized views from the database. This method reads the
   * latest status from MongoDB for each follower generation ID managed by this lease manager.
   *
   * <p>For dynamic leader election, this also identifies leases that are eligible for leadership
   * acquisition (expired leases, leases we own, or new indexes without leases).
   *
   * @return a {@link FollowerPollResult} containing the status map and the set of acquirable
   *     leases. Returns {@link FollowerPollResult#EMPTY} in three cases: (1) this instance is the
   *     leader, (2) there are no follower generation IDs, or (3) a transient DB failure prevented
   *     the batch fetch. Callers MUST treat an empty result as "no new information" and preserve
   *     whatever status they last applied — never reset to UNKNOWN on an empty return.
   */
  FollowerPollResult pollFollowerStatuses();

  /**
   * Returns the oplog position at which the MV first became STEADY. Used by Lucene to determine if
   * it has caught up.
   *
   * @param generationId the generation id
   * @return the steady-as-of position, or empty if MV hasn't reached STEADY yet
   */
  Optional<BsonTimestamp> getSteadyAsOfOplogPosition(MaterializedViewGenerationId generationId);

  /**
   * Returns true if the given index definition version has ever been queryable (reached STEADY at
   * least once), as persisted in the lease. Used to initialize the composite index's queryable
   * ratchet after a restart.
   *
   * @param generationId the generation id
   * @param indexDefinitionVersion the definition version to check
   * @return true if that version has been queryable
   */
  boolean isCurrentVersionQueryable(
      MaterializedViewGenerationId generationId, long indexDefinitionVersion);

  /**
   * Returns the current lease version for the given generation ID. Used as a fencing token for MV
   * writes.
   *
   * @param generationId the generation ID
   * @return the current lease version, or Long.MAX_VALUE for static leaders
   */
  default long getLeaseVersion(MaterializedViewGenerationId generationId) {
    return Long.MAX_VALUE;
  }

  /**
   * Performs a heartbeat for all managed leases. For dynamic leader election, this renews the
   * leases for leaders to maintain leadership. For static leader, this is a no-op since leadership
   * is pre-assigned and constant.
   *
   * <p>This method is called periodically by the MaterializedViewManager.
   */
  default void heartbeat() {
    // Default implementation: no-op for static leader
  }

  /**
   * Attempts to acquire leadership for the given generation ID. This is called by the
   * MaterializedViewManager when it detects that a lease has expired and leadership can be
   * acquired.
   *
   * <p>For dynamic leader election, this attempts to claim the lease using optimistic concurrency
   * control. For static leader, this is a no-op since leadership is pre-assigned.
   *
   * @param generationId the generation ID to acquire leadership for
   * @return true if leadership was successfully acquired, false otherwise
   */
  default boolean tryAcquireLeadership(MaterializedViewGenerationId generationId) {
    // Default implementation: no-op for static leader
    return false;
  }

  /**
   * Releases leadership for the given generation, transitioning it to follower state. Used by
   * MaterializedViewManager to release zombie leases (generator dead but lease still held).
   *
   * @param generationId the generation ID to release leadership for
   */
  default void releaseLeadership(MaterializedViewGenerationId generationId) {}

  /**
   * Initializes an unowned Lease by trying to insert the proposed lease document into the database.
   * This operation should be synchronized across all mongots in the same replicaSet, so only one
   * Mongot wins in insertOne and the winner Lease with MaterializedViewCollectionMetadata should be
   * the same across all mongots.
   *
   * @param proposedMetadata the metadata to insert if it doesn't exist
   * @return the synchronized MaterializedViewCollectionMetadata
   */
  MaterializedViewCollectionMetadata initializeLease(
      MaterializedViewIndexDefinitionGeneration indexDefinitionGeneration,
      MaterializedViewCollectionMetadata proposedMetadata)
      throws Exception;

  /**
   * Invoked after {@link #initializeLease} has finished for the materialized view lease key (the
   * materialized view collection name, which is also the lease document {@code _id}).
   *
   * <p>Implementations may use this to apply bootstrap-time ops that are not run at lease-manager
   * construction because the lease store may not be ready yet (for example {@code opsGiveUpLease}
   * rebalance commands).
   */
  default void executeOpsCommandsAfterInitializeLease(String leaseKey) {}

  /**
   * Returns true if the lease manager is in a lease-acquisition blackout period (e.g. after
   * applying an ops give-up command). During blackout, this instance must not acquire new
   * leadership.
   *
   * @return true if in blackout, false otherwise
   */
  default boolean isInLeaseAcquisitionBlackout() {
    return false;
  }

  /**
   * Groups {@code leases} by {@link Lease#indexId()} and returns the key(s) at the highest version
   * per index — the set the GC mark pass must never mark (the current version is the live index).
   * Safe over a partial {@link #findGcCandidates()} result: exposing a lease as non-current
   * requires a present higher-version sibling; absence only protects.
   */
  static Set<String> currentVersionLeaseKeys(Collection<Lease> leases) {
    Map<String, List<Lease>> leasesByIndexId = new HashMap<>();
    for (Lease lease : leases) {
      leasesByIndexId.computeIfAbsent(lease.indexId(), key -> new ArrayList<>()).add(lease);
    }
    Set<String> currentVersionKeys = new HashSet<>();
    for (List<Lease> group : leasesByIndexId.values()) {
      boolean anyUnparseable =
          group.stream()
              .anyMatch(
                  lease -> parseDefinitionVersion(lease.latestIndexDefinitionVersion()).isEmpty());
      if (anyUnparseable) {
        // Cannot tell which lease is current; conservatively protect every lease of this index.
        group.forEach(lease -> currentVersionKeys.add(lease.id()));
        continue;
      }
      long highestVersion =
          group.stream()
              .mapToLong(
                  lease -> parseDefinitionVersion(lease.latestIndexDefinitionVersion()).getAsLong())
              .max()
              .orElseThrow();
      group.stream()
          .filter(
              lease ->
                  parseDefinitionVersion(lease.latestIndexDefinitionVersion()).getAsLong()
                      == highestVersion)
          .forEach(lease -> currentVersionKeys.add(lease.id()));
    }
    return currentVersionKeys;
  }

  private static OptionalLong parseDefinitionVersion(String version) {
    if (version == null) {
      // Treat a missing version as unparseable so the index is conservatively protected.
      return OptionalLong.empty();
    }
    try {
      return OptionalLong.of(Long.parseLong(version));
    } catch (NumberFormatException e) {
      return OptionalLong.empty();
    }
  }
}
