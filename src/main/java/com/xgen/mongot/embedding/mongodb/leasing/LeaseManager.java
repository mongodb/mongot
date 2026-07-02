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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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
   * Returns every lease whose {@code leaseExpiration} is older than {@code expirationCutoff} — the
   * stale-lease GC's candidate set. Scans the internal databases known to this manager (see the
   * implementations for how that set is built). Reads the database, not the in-memory map: the map
   * only holds leases for generations currently configured on this mongot, so an orphaned lease
   * (its generation dropped, possibly before a restart) is visible only through this query.
   *
   * <p>Leases already marked {@link Lease.CleanupState#ELIGIBLE_FOR_CLEANUP} are deliberately
   * included: the same candidate set serves both GC phases — the mark pass skips them, while the
   * cleaner pass (CLOUDP-384018) consumes exactly those.
   *
   * <p>Side effect: registers the lease→database mapping for each returned lease, so a subsequent
   * {@link #markEligibleForCleanup} resolves the database even when no configured generation ever
   * registered the lease on this mongot.
   *
   * <p>Best effort, never throws: unparseable (corrupted) lease documents are skipped — invisible
   * to GC (CLOUDP-384971 owns their cleanup) — and a failed per-database scan is logged and drops
   * only that database's candidates, so the result may be incomplete. Callers must not treat it as
   * a complete census of stale leases; the periodic sweep re-scans every tick, so a candidate
   * missed this round is picked up on a later one.
   *
   * @param expirationCutoff only leases that expired before this instant are returned
   * @return the stale leases found by the scan, possibly partial on query failure; never null
   */
  List<Lease> findGcCandidates(Instant expirationCutoff);

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
   * Resolves the materialized view collection to drop when cleaning up {@code leaseKey}'s stale
   * lease, re-reading the lease so a namespace is returned only while it is still {@link
   * Lease.CleanupState#ELIGIBLE_FOR_CLEANUP} and expired before {@code expirationCutoff}. A lease
   * reused in the meantime yields {@link Optional#empty()}.
   *
   * <p>The caller drops the returned collection and then removes the lease via {@link
   * #deleteStaleLease} — dropping the materialized view collection is the materialized-view
   * manager's responsibility, not the lease manager's. Best effort, never throws.
   *
   * @param leaseKey the lease document {@code _id}
   * @param expirationCutoff the delete-grace cutoff; the lease must have expired before this
   * @return the namespace of the materialized view collection to drop, or {@link Optional#empty()}
   *     if the lease is no longer eligible, already gone, its database is unknown, or the re-read
   *     failed
   */
  Optional<MongoNamespace> resolveStaleMatViewNamespace(String leaseKey, Instant expirationCutoff);

  /**
   * Removes the lease document {@code leaseKey} — the final step of cleaning up a stale lease, run
   * only after its materialized view collection has been dropped (see {@link
   * #resolveStaleMatViewNamespace}).
   *
   * <p>Deletes only while the lease is still {@link Lease.CleanupState#ELIGIBLE_FOR_CLEANUP} and
   * expired before {@code expirationCutoff}, so a lease reused after its collection was dropped is
   * left in place. Best effort, never throws.
   *
   * @param leaseKey the lease document {@code _id}
   * @param expirationCutoff the delete-grace cutoff; the lease must have expired before this
   * @return {@code true} if this call removed the lease; {@code false} if its database is unknown,
   *     it was already gone, is no longer eligible, or the delete did not complete
   */
  boolean deleteStaleLease(String leaseKey, Instant expirationCutoff);

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
}
