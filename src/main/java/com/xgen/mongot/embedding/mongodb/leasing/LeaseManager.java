package com.xgen.mongot.embedding.mongodb.leasing;

import com.xgen.mongot.embedding.exceptions.MaterializedViewNonTransientException;
import com.xgen.mongot.embedding.exceptions.MaterializedViewTransientException;
import com.xgen.mongot.index.EncodedUserData;
import com.xgen.mongot.index.IndexGeneration;
import com.xgen.mongot.index.status.IndexStatus;
import com.xgen.mongot.index.version.GenerationId;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

// TODO(CLOUDP-371278): Refactor interface to have stricter type that only allows MatView related
// IndexGeneration and GenerationId
/** Interface for managing leases for the materialized view leader. */
public interface LeaseManager {

  /**
   * Result of polling follower statuses from the lease manager.
   *
   * <p>Contains both the current status of each follower generation and the set of generation IDs
   * that are eligible for leadership acquisition. This includes:
   * <ul>
   *   <li>Leases that have expired (previous leader failed or timed out)</li>
   *   <li>Leases owned by this instance (re-acquiring after restart)</li>
   *   <li>New indexes that don't have a lease yet (lease will be created during acquisition)</li>
   * </ul>
   *
   * @param statuses Map of generation IDs to their current replication status
   * @param acquirableLeases Set of generation IDs eligible for leadership acquisition
   */
  record FollowerPollResult(
      Map<GenerationId, IndexStatus> statuses, Set<GenerationId> acquirableLeases) {
    /** An empty result with no statuses and no acquirable leases. */
    public static final FollowerPollResult EMPTY =
        new FollowerPollResult(Collections.emptyMap(), Collections.emptySet());
  }

  /**
   * Adds a lease for the given index generation.
   *
   * @param indexGeneration the index generation to add the lease for.
   */
  void add(IndexGeneration indexGeneration);

  /**
   * Drops the given index generation from the lease. Deletes the lease entirely if this is the last
   * index generation in the lease.
   *
   * @param generationId the generation id to remove the lease for
   * @return a future that completes when the lease has been dropped
   */
  CompletableFuture<Void> drop(GenerationId generationId);

  /**
   * Returns true if the current mongot is the leader for the given generation id.
   *
   * @param generationId the generation id to check
   * @return true if the caller is the leader
   */
  boolean isLeader(GenerationId generationId);

  /**
   * Updates the commit info (replication checkpoint state) for the given generation id.
   *
   * @param generationId the generation id to update
   * @param encodedUserData the commit info to update
   */
  void updateCommitInfo(GenerationId generationId, EncodedUserData encodedUserData)
      throws MaterializedViewTransientException, MaterializedViewNonTransientException;

  /**
   * Returns the commit info (replication checkpoint state) for the given generation id.
   *
   * @param generationId the generation id to get the commit info for
   * @return the commit info
   * @throws IOException if there are any issues talking to the lease store
   */
  EncodedUserData getCommitInfo(GenerationId generationId) throws IOException;

  /**
   * Updates the status of the materialized view replication.
   *
   * @param generationId the generation id of the index generation
   * @param indexDefinitionVersion the index definition version to update
   * @param indexStatus the status to update to
   */
  void updateReplicationStatus(
      GenerationId generationId, long indexDefinitionVersion, IndexStatus indexStatus)
      throws MaterializedViewTransientException, MaterializedViewNonTransientException;

  /**
   * Returns all generation IDs where this instance is the leader.
   *
   * @return a set of generation IDs where this instance is the leader
   */
  Set<GenerationId> getLeaderGenerationIds();

  /**
   * Returns all generation IDs where this instance is a follower.
   *
   * @return a set of generation IDs where this instance is a follower
   */
  Set<GenerationId> getFollowerGenerationIds();

  /**
   * Polls the status of all follower materialized views from the database. This method reads the
   * latest status from MongoDB for each follower generation ID managed by this lease manager.
   *
   * <p>For dynamic leader election, this also identifies leases that are eligible for leadership
   * acquisition (expired leases, leases we own, or new indexes without leases).
   *
   * @return a {@link FollowerPollResult} containing the status map and the set of acquirable
   *     leases. Returns {@link FollowerPollResult#EMPTY} if this instance is the leader or if
   *     there are no follower generation IDs.
   */
  FollowerPollResult pollFollowerStatuses();

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
  default boolean tryAcquireLeadership(GenerationId generationId) {
    // Default implementation: no-op for static leader
    return false;
  }
}
