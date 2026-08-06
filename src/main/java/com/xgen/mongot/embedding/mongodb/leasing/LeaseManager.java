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
import org.bson.types.ObjectId;
import org.jetbrains.annotations.TestOnly;
import org.slf4j.LoggerFactory;

// TODO(CLOUDP-371278): Refactor interface to have stricter type that only allows MatView related
// IndexGeneration and GenerationId
/** Interface for managing leases for the materialized view leader. */
public interface LeaseManager {

  long DEFAULT_INDEX_DEFINITION_VERSION = 0;

  /**
   * Result of polling follower statuses from the lease manager.
   *
   * <p>Contains the current status and replication optime of each follower generation, plus the set
   * of generation IDs that are eligible for leadership acquisition. This includes:
   *
   * <ul>
   *   <li>Leases that have expired (previous leader failed or timed out)
   *   <li>Leases owned by this instance (re-acquiring after restart)
   *   <li>New indexes that don't have a lease yet (lease will be created during acquisition)
   * </ul>
   *
   * @param statuses Map of generation IDs to their current replication status
   * @param replicationOpTimes Map of generation IDs to replication optimes extracted from their
   *     lease checkpoints
   * @param acquirableLeases Set of generation IDs eligible for leadership acquisition
   */
  record FollowerPollResult(
      Map<MaterializedViewGenerationId, IndexStatus> statuses,
      Map<MaterializedViewGenerationId, BsonTimestamp> replicationOpTimes,
      Set<MaterializedViewGenerationId> acquirableLeases) {
    /** An empty result with no statuses, replication optimes, or acquirable leases. */
    public static final FollowerPollResult EMPTY =
        new FollowerPollResult(
            Collections.emptyMap(), Collections.emptyMap(), Collections.emptySet());

    public FollowerPollResult(
        Map<MaterializedViewGenerationId, IndexStatus> statuses,
        Set<MaterializedViewGenerationId> acquirableLeases) {
      this(statuses, Collections.emptyMap(), acquirableLeases);
    }
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
   * Drops the given index generation from the tracking list in LeaseManager. Local generation
   * tracking only: touches neither the lease document nor the leaseKey-keyed local state (see
   * {@link #evictCachedLease} and {@link #dropLease} for those).
   *
   * @param generationId the generation id to remove the lease for
   */
  void drop(MaterializedViewGenerationId generationId);

  /**
   * Deletes the lease entirely in both database and memory, only call this when corresponding
   * materialized view collection is GC'd or user deletes the whole index. For forgetting the local
   * state without deleting the document, see {@link #evictCachedLease}.
   *
   * @param leaseKey the string lease key for lease to be removed, should be same as collection
   *     name.
   * @return a future that completes when the lease has been dropped
   */
  CompletableFuture<Void> dropLease(String leaseKey);

  /**
   * Evicts this mongot's cached copy of the lease (in-memory lease entry and database mapping)
   * without touching the lease document in the database — the cache is rebuilt from the database
   * when needed. The database-deleting paths ({@link #dropLease}, {@link #deleteStaleLease}) call
   * this as their local-cleanup tail. The supersession path calls it directly: there the lease
   * document deliberately stays behind for the stale-lease GC, and the cached copy would otherwise
   * outlive it — a later re-initialization of the same definition would then read stale
   * materialized-view metadata (a recreated MV collection keeps its name but gets a new UUID). The
   * GC path is unaffected by the eviction: it re-registers the database mapping from the database
   * on every sweep.
   *
   * @param leaseKey the string lease key, same as the materialized view collection name
   */
  void evictCachedLease(String leaseKey);

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
   * A deleted index's lease paired with the materialized view namespace its document points at.
   * {@code matViewNamespace} is empty when the lease carries no usable collection metadata — then
   * there is nothing to deliberately drop and the lease is left for the stale-lease GC.
   */
  record DeletedIndexLease(Lease lease, Optional<MongoNamespace> matViewNamespace) {}

  /**
   * Returns every lease document belonging to {@code indexId}: the deleted index's own lease plus
   * any leftovers from earlier supersessions. All of an index's leases live in the database of
   * {@code knownLeaseKey}, keyed by index-id prefix, so this is a single prefix scan. Each lease is
   * paired with its MV namespace, and its database mapping is registered for the cleanup calls that
   * follow. Best effort, never throws; an unknown database or a failed read returns an empty list.
   */
  List<DeletedIndexLease> findLeasesForDeletedIndex(ObjectId indexId, String knownLeaseKey);

  /**
   * Deliberately removes the lease document {@code leaseKey} of a deleted index — regardless of
   * owner, staleness, or cleanup state, since the index is authoritatively gone from the desired
   * config and every lease under it is garbage. Run only after the lease's materialized view
   * collection has been dropped, preserving the drop-before-delete ordering the stale-lease GC
   * relies on. Also evicts this node's cached copy. Best effort, never throws.
   *
   * @param leaseKey the lease document {@code _id}
   * @return {@code true} if the document is gone (deleted now, or already absent — concurrent
   *     mongots race this idempotently); {@code false} if the database is unknown or the delete
   *     failed
   */
  boolean cleanupLeaseForDeletedIndex(String leaseKey);

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
   * @return a {@link FollowerPollResult} containing status and replication optime maps and the set
   *     of acquirable leases. Returns an empty result in three cases: (1) this instance is the
   *     leader, (2) there are no follower generation IDs, or (3) a transient DB failure prevented
   *     the batch fetch. Callers MUST treat an empty result as "no new information" and preserve
   *     whatever status and optime they last applied.
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

  /** Sentinel returned by {@link #getOwnedLeaseVersion} when no usable fencing token exists. */
  long INVALID_LEASE_VERSION = -1;

  /**
   * Returns the current lease version for the given generation ID <em>if this host owns the
   * lease</em>. Used as a fencing token for MV writes, so a host that does not own the lease must
   * never receive one: a token it does not own would be indistinguishable from the real leader's.
   *
   * @param generationId the generation ID
   * @return the owned lease version, {@link #INVALID_LEASE_VERSION} if there is no lease or it is
   *     owned by another host, or Long.MAX_VALUE for static leaders (fencing is skipped)
   */
  default long getOwnedLeaseVersion(MaterializedViewGenerationId generationId) {
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

  /**
   * The MV namespace {@code lease} points at, or empty (logged) when its metadata is missing or
   * invalid. Shared by the deleted-index scan and the GC's stale-namespace resolution.
   */
  static Optional<MongoNamespace> matViewNamespaceOf(String dbName, Lease lease) {
    try {
      return Optional.of(
          new MongoNamespace(dbName, lease.materializedViewCollectionMetadata().collectionName()));
    } catch (RuntimeException e) {
      LoggerFactory.getLogger(LeaseManager.class)
          .atWarn()
          .addKeyValue("leaseKey", lease.id())
          .setCause(e)
          .log("Lease carries no usable materialized view namespace");
      return Optional.empty();
    }
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
