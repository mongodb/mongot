package com.xgen.mongot.embedding.mongodb.leasing;

import static com.xgen.mongot.embedding.mongodb.leasing.StatusResolutionUtils.getEffectiveMaterializedViewStatus;
import static com.xgen.mongot.util.FutureUtils.COMPLETED_FUTURE;

import com.google.common.annotations.VisibleForTesting;
import com.google.errorprone.annotations.Var;
import com.mongodb.ConnectionString;
import com.mongodb.ErrorCategory;
import com.mongodb.MongoWriteException;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.xgen.mongot.embedding.exceptions.MaterializedViewNonTransientException;
import com.xgen.mongot.embedding.exceptions.MaterializedViewTransientException;
import com.xgen.mongot.embedding.utils.MongoClientOperationExecutor;
import com.xgen.mongot.index.EncodedUserData;
import com.xgen.mongot.index.IndexGeneration;
import com.xgen.mongot.index.status.IndexStatus;
import com.xgen.mongot.index.version.GenerationId;
import com.xgen.mongot.metrics.MeterAndFtdcRegistry;
import com.xgen.mongot.metrics.MetricsFactory;
import com.xgen.mongot.util.mongodb.ConnectionStringUtil;
import com.xgen.mongot.util.mongodb.MongoClientBuilder;
import com.xgen.mongot.util.mongodb.SyncSourceConfig;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A lease manager that dynamically elects leaders at the index level. Leadership is determined
 * per-GenerationId through lease acquisition and renewal. A single mongot instance can be leader
 * for some materialized views and follower for others.
 *
 * <p>Leadership is acquired by attempting to claim an expired lease or create a new one. Leadership
 * is maintained by periodically renewing the lease via {@link #heartbeat()}. If a lease expires
 * (e.g., due to network partition or process failure), another instance can acquire leadership.
 *
 * <p>Expected to be used as a singleton.
 */
public class DynamicLeaderLeaseManager implements LeaseManager {

  private static final Logger LOG = LoggerFactory.getLogger(DynamicLeaderLeaseManager.class);

  private static final String AUTO_EMBEDDING_INTERNAL_DATABASE_NAME = "__mdb_internal_search";

  private static final String METRICS_NAMESPACE = "embedding.leasing.stats";

  private static final int MONGO_CLIENT_MAX_CONNECTIONS = 2;

  @VisibleForTesting static final String LEASE_COLLECTION_NAME = "auto_embedding_leases";

  public static final long DEFAULT_INDEX_DEFINITION_VERSION = 0;

  private final MongoClientOperationExecutor operationExecutor;
  private final String hostname;
  // Mapping of lease keys to leases.
  private final Map<String, Lease> leases;
  // Tracks GenerationIds where this instance is the leader.
  private final Set<GenerationId> leaderGenerationIds;
  // Tracks GenerationIds where this instance is a follower.
  private final Set<GenerationId> followerGenerationIds;
  // Maps GenerationId to its definition version (as String) for use in pollFollowerStatuses().
  private final Map<GenerationId, String> generationIdToDefinitionVersion;
  private final MongoCollection<Lease> collection;

  public DynamicLeaderLeaseManager(
      MongoClient mongoClient,
      MetricsFactory metricsFactory,
      String hostname,
      String databaseName) {
    this.operationExecutor =
        new MongoClientOperationExecutor(metricsFactory, "leaseTableCollection");
    this.hostname = hostname;
    this.leases = new ConcurrentHashMap<>();
    this.leaderGenerationIds = ConcurrentHashMap.newKeySet();
    this.followerGenerationIds = ConcurrentHashMap.newKeySet();
    this.generationIdToDefinitionVersion = new ConcurrentHashMap<>();
    this.collection =
        mongoClient
            .getDatabase(databaseName)
            .getCollection(LEASE_COLLECTION_NAME, Lease.class)
            .withReadConcern(ReadConcern.LINEARIZABLE)
            .withReadPreference(ReadPreference.primary());
    initLeases();
  }

  public static DynamicLeaderLeaseManager create(
      SyncSourceConfig syncSourceConfig,
      MeterAndFtdcRegistry meterAndFtdcRegistry,
      String hostname) {
    return new DynamicLeaderLeaseManager(
        getMongoClient(syncSourceConfig, meterAndFtdcRegistry),
        new MetricsFactory(METRICS_NAMESPACE, meterAndFtdcRegistry.meterRegistry()),
        hostname,
        AUTO_EMBEDDING_INTERNAL_DATABASE_NAME);
  }

  /**
   * Initializes the local lease state with the leases from the database.
   *
   * <p>Fetches all lease documents from the collection. Example lease document:
   *
   * <pre>{@code
   * {
   *   "_id": "6930985def257a5ef2f7f823",
   *   "leaseOwner": "mongot-host-1.example.com",
   *   "leaseExpiration": "2024-01-15T10:30:00Z",
   *   "leaseVersion": 5,
   *   "commitInfo": "checkpoint_data_here",
   *   "indexStatus": "READY"
   * }
   * }</pre>
   *
   * @throws RuntimeException if there is an error talking to the database.
   */
  private void initLeases() {
    try {
      List<Lease> leaseList =
          this.operationExecutor.execute(
              "getLeases", () -> this.collection.find().into(new ArrayList<>()));
      leaseList.forEach(lease -> this.leases.put(lease.id(), lease));
      LOG.atInfo()
          .addKeyValue("leaseCount", leaseList.size())
          .addKeyValue("hostname", this.hostname)
          .log("Initialized leases from database");
    } catch (Exception e) {
      throw new RuntimeException("Failed to initialize leases from database.", e);
    }
  }

  /**
   * Adds a new index generation to be managed by this lease manager.
   *
   * <p>If we already own the lease for this index (e.g., during index definition update), the
   * generation is added as a leader. Otherwise, it starts as a follower and leadership is acquired
   * via {@link #tryAcquireLeadership(GenerationId)}.
   *
   * <p>If no lease exists in memory, an in-memory lease with empty owner is created. This lease
   * will be persisted to the database when {@link #tryAcquireLeadership(GenerationId)} is called.
   */
  @Override
  public void add(IndexGeneration indexGeneration) {
    GenerationId generationId = indexGeneration.getGenerationId();
    String versionKey = getIndexDefinitionVersion(indexGeneration);
    this.generationIdToDefinitionVersion.put(generationId, versionKey);

    // TODO(CLOUDP-373389): add materializedViewMetadata that includes schemaFieldsMapping
    if (this.leases.containsKey(getLeaseKey(generationId))) {
      // Lease exists in memory - check if we own it.
      var lease = this.leases.get(getLeaseKey(generationId));
      boolean weOwnLease = this.hostname.equals(lease.leaseOwner());

      if (weOwnLease) {
        // We own the lease - this is likely an index definition update (e.g., filter field change).
        // Add to leaderGenerationIds so the generator can become leader immediately.
        this.leaderGenerationIds.add(generationId);
        LOG.atInfo()
            .addKeyValue("generationId", generationId)
            .addKeyValue("leaseOwner", lease.leaseOwner())
            .addKeyValue("leaseExpiration", lease.leaseExpiration())
            .addKeyValue("hostname", this.hostname)
            .log("Starting as leader - we own the existing lease");
      } else {
        // Another instance owns the lease - start as follower.
        this.followerGenerationIds.add(generationId);
        LOG.atInfo()
            .addKeyValue("generationId", generationId)
            .addKeyValue("leaseOwner", lease.leaseOwner())
            .addKeyValue("leaseExpiration", lease.leaseExpiration())
            .addKeyValue("hostname", this.hostname)
            .log("Starting as follower - existing lease owned by another instance");
      }

      // Update the lease with the new index definition version if needed.
      if (!lease.indexDefinitionVersionStatusMap().containsKey(versionKey)) {
        this.leases.put(
            getLeaseKey(generationId),
            lease.withNewIndexDefinitionVersion(
                versionKey, indexGeneration.getIndex().getStatus()));
      }
    } else {
      // No lease in memory - create an in-memory lease with an empty owner.
      // This will be persisted to the database when tryAcquireLeadership() is called.
      this.followerGenerationIds.add(generationId);
      LOG.atInfo()
          .addKeyValue("generationId", generationId)
          .addKeyValue("hostname", this.hostname)
          .log("Starting as follower - no existing lease, creating in-memory placeholder");
      Lease newLease =
          Lease.newLease(
              indexGeneration.getDefinition().getIndexId().toHexString(),
              indexGeneration.getDefinition().getCollectionUuid(),
              indexGeneration.getDefinition().getLastObservedCollectionName(),
              "", // Empty owner - no one owns this lease yet
              versionKey,
              indexGeneration.getIndex().getStatus());
      this.leases.put(getLeaseKey(generationId), newLease);
    }
  }

  @Override
  public CompletableFuture<Void> drop(GenerationId generationId) {
    boolean wasLeader = this.leaderGenerationIds.remove(generationId);
    boolean wasFollower = this.followerGenerationIds.remove(generationId);
    this.generationIdToDefinitionVersion.remove(generationId);

    // Only delete the lease from the database if we own the lease.
    // Enforce ownership check in the filter to handle stale in-memory state.
    Lease lease = this.leases.get(getLeaseKey(generationId));
    if (lease != null && this.hostname.equals(lease.leaseOwner())) {
      LOG.atInfo()
          .addKeyValue("generationId", generationId)
          .addKeyValue("hostname", this.hostname)
          .addKeyValue("wasLeader", wasLeader)
          .log("Dropping index - deleting lease from database (we own it)");
      // Remove the lease from memory first, then delete it from DB.
      // This ensures we stop considering ourselves the leader immediately.
      this.leases.remove(getLeaseKey(generationId));
      return CompletableFuture.runAsync(
          () -> {
            try {
              var filter =
                  Filters.and(
                      Filters.eq("_id", getLeaseKey(generationId)),
                      Filters.eq(Lease.Fields.LEASE_OWNER.getName(), this.hostname));
              var deleteResult = this.collection.deleteOne(filter);
              if (deleteResult.getDeletedCount() > 0) {
                LOG.atInfo()
                    .addKeyValue("generationId", generationId)
                    .log("Successfully deleted lease from database");
              } else {
                // This is expected if another instance took over before we deleted.
                LOG.atInfo()
                    .addKeyValue("generationId", generationId)
                    .log("Lease not deleted - ownership changed or lease already removed");
              }
            } catch (Exception e) {
              // Best effort cleanup. The lease will eventually expire if we can't delete it.
              LOG.warn(
                  "Failed to delete lease for {} from database. "
                      + "Lease will expire naturally if not deleted.",
                  getLeaseKey(generationId),
                  e);
            }
          });
    }

    // Follower path or no lease found - just clean up the in-memory state.
    if (lease != null) {
      LOG.atInfo()
          .addKeyValue("generationId", generationId)
          .addKeyValue("hostname", this.hostname)
          .addKeyValue("leaseOwner", lease.leaseOwner())
          .addKeyValue("wasFollower", wasFollower)
          .log("Dropping index - not deleting lease from database (we don't own it)");
    } else {
      LOG.atInfo()
          .addKeyValue("generationId", generationId)
          .addKeyValue("hostname", this.hostname)
          .addKeyValue("wasLeader", wasLeader)
          .addKeyValue("wasFollower", wasFollower)
          .log("Dropping index - no lease found in memory");
    }
    this.leases.remove(getLeaseKey(generationId));
    return COMPLETED_FUTURE;
  }

  @Override
  public boolean isLeader(GenerationId generationId) {
    return this.leaderGenerationIds.contains(generationId);
  }

  @Override
  public EncodedUserData getCommitInfo(GenerationId generationId) throws IOException {
    // Leader can read from the in-memory state.
    if (isLeader(generationId)) {
      ensureLeaseExists(generationId);
      return EncodedUserData.fromString(this.leases.get(getLeaseKey(generationId)).commitInfo());
    }
    // If follower, read from a database.
    try {
      Lease lease = getLeaseFromDatabase(generationId);
      if (lease == null) {
        return EncodedUserData.EMPTY;
      }
      return EncodedUserData.fromString(lease.commitInfo());
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public void updateCommitInfo(GenerationId generationId, EncodedUserData encodedUserData)
      throws MaterializedViewTransientException, MaterializedViewNonTransientException {
    // Only update the commit info in the database if leader.
    if (isLeader(generationId)) {
      ensureLeaseExists(generationId);
      Lease currentLease = this.leases.get(getLeaseKey(generationId));
      Lease updatedLease = currentLease.withUpdatedCheckpoint(encodedUserData);
      updateLeaseInDatabase(generationId, currentLease, updatedLease, encodedUserData);
    }
  }

  @Override
  public void updateReplicationStatus(
      GenerationId generationId, long indexDefinitionVersion, IndexStatus indexStatus)
      throws MaterializedViewTransientException, MaterializedViewNonTransientException {
    // Only update the status in the database of leader.
    if (isLeader(generationId)) {
      ensureLeaseExists(generationId);
      Lease currentLease = this.leases.get(getLeaseKey(generationId));
      Lease updatedLease = currentLease.withUpdatedStatus(indexStatus, indexDefinitionVersion);
      updateLeaseInDatabase(
          generationId,
          currentLease,
          updatedLease,
          EncodedUserData.fromString(currentLease.commitInfo()));
    }
  }

  @Override
  public Set<GenerationId> getLeaderGenerationIds() {
    return Collections.unmodifiableSet(this.leaderGenerationIds);
  }

  @Override
  public Set<GenerationId> getFollowerGenerationIds() {
    return Collections.unmodifiableSet(this.followerGenerationIds);
  }

  @Override
  public LeaseManager.FollowerPollResult pollFollowerStatuses() {
    Map<GenerationId, IndexStatus> statuses = new HashMap<>();
    Set<GenerationId> acquirableLeases = new HashSet<>();

    LOG.atDebug()
        .addKeyValue("followerCount", this.followerGenerationIds.size())
        .addKeyValue("hostname", this.hostname)
        .log("Polling follower statuses");

    if (this.followerGenerationIds.isEmpty()) {
      return new LeaseManager.FollowerPollResult(statuses, acquirableLeases);
    }

    // Build a mapping from lease key to generation ID for efficient lookup after batch fetch.
    Map<String, GenerationId> leaseKeyToGenerationId = new HashMap<>();
    List<String> leaseKeys = new ArrayList<>();
    for (GenerationId generationId : this.followerGenerationIds) {
      String leaseKey = getLeaseKey(generationId);
      leaseKeys.add(leaseKey);
      leaseKeyToGenerationId.put(leaseKey, generationId);
    }

    // Batch fetch all follower leases from the database.
    Map<String, Lease> fetchedLeases = new HashMap<>();
    try {
      List<Lease> leaseList =
          this.operationExecutor.execute(
              "getFollowerLeases",
              () -> this.collection.find(Filters.in("_id", leaseKeys)).into(new ArrayList<>()));
      for (Lease lease : leaseList) {
        fetchedLeases.put(lease.id(), lease);
      }
      LOG.atDebug()
          .addKeyValue("requestedCount", leaseKeys.size())
          .addKeyValue("fetchedCount", leaseList.size())
          .log("Batch fetched follower leases");
    } catch (Exception e) {
      LOG.warn("Failed to batch fetch follower leases, falling back to UNKNOWN status", e);
      // On failure, mark all followers as UNKNOWN.
      for (GenerationId generationId : this.followerGenerationIds) {
        statuses.put(generationId, new IndexStatus(IndexStatus.StatusCode.UNKNOWN));
      }
      return new LeaseManager.FollowerPollResult(statuses, acquirableLeases);
    }

    // Process each follower generation ID.
    Instant now = Instant.now();
    for (GenerationId generationId : this.followerGenerationIds) {
      String versionKey = this.generationIdToDefinitionVersion.get(generationId);
      if (versionKey == null) {
        LOG.warn("No definition version found for generation ID {}", generationId);
        continue;
      }

      String leaseKey = getLeaseKey(generationId);
      Lease lease = fetchedLeases.get(leaseKey);

      if (lease != null) {
        // Update in-memory lease with DB state.
        this.leases.put(leaseKey, lease);

        // Extract status from the lease.
        IndexStatus status = getStatusFromLease(lease, generationId, versionKey);
        statuses.put(generationId, status);

        // Check if the lease is expired or owned by us (eligible for leadership acquisition).
        boolean leaseExpired = now.isAfter(lease.leaseExpiration());
        boolean weOwnLease = this.hostname.equals(lease.leaseOwner());
        if (leaseExpired || weOwnLease) {
          LOG.atInfo()
              .addKeyValue("generationId", generationId)
              .addKeyValue("leaseOwner", lease.leaseOwner())
              .addKeyValue("leaseExpiration", lease.leaseExpiration())
              .addKeyValue("now", now)
              .addKeyValue("leaseExpired", leaseExpired)
              .addKeyValue("weOwnLease", weOwnLease)
              .log("Lease is acquirable");
          acquirableLeases.add(generationId);
        }
      } else {
        // No lease in DB - this is a new index. In-memory lease (with an empty owner) is kept.
        LOG.info("New index detected without lease for generation ID {}", generationId);
        statuses.put(generationId, new IndexStatus(IndexStatus.StatusCode.UNKNOWN));
        acquirableLeases.add(generationId);
      }
    }
    return new LeaseManager.FollowerPollResult(statuses, acquirableLeases);
  }

  /** Extracts the effective status from a lease for a given generation ID and version key. */
  private IndexStatus getStatusFromLease(
      Lease lease, GenerationId generationId, String versionKey) {
    @Var
    Lease.IndexDefinitionVersionStatus requestedStatus =
        new Lease.IndexDefinitionVersionStatus(false, IndexStatus.StatusCode.UNKNOWN);

    if (lease.indexDefinitionVersionStatusMap().containsKey(versionKey)) {
      requestedStatus = lease.indexDefinitionVersionStatusMap().get(versionKey);
    } else {
      LOG.warn(
          "Requested version key {} not found in lease for generation ID {}",
          versionKey,
          generationId);
    }
    Lease.IndexDefinitionVersionStatus latestStatus =
        lease.indexDefinitionVersionStatusMap().get(lease.latestIndexDefinitionVersion());

    return getEffectiveMaterializedViewStatus(requestedStatus, latestStatus);
  }

  @Override
  public void heartbeat() {
    // For leaders: renew the lease to maintain leadership.
    // For followers: do nothing - leadership acquisition is handled separately via
    // tryAcquireLeadership() called by MaterializedViewManager.
    int leaderCount = this.leaderGenerationIds.size();
    if (leaderCount > 0) {
      LOG.atDebug()
          .addKeyValue("leaderCount", leaderCount)
          .addKeyValue("hostname", this.hostname)
          .log("Heartbeat - renewing leases for leader generations");
    }
    for (GenerationId generationId : new ArrayList<>(this.leaderGenerationIds)) {
      renewLease(generationId);
    }
  }

  /**
   * Attempts to acquire leadership for the given generation ID. This method is called by
   * MaterializedViewManager when it detects that a lease is acquirable (expired, owned by us, or
   * new).
   *
   * <p>For new indexes (in-memory lease with empty owner), the lease is created via upsert. For
   * existing leases, leadership is acquired using optimistic concurrency control.
   *
   * @param generationId the generation ID to acquire leadership for
   * @return true if leadership was successfully acquired, false otherwise
   */
  @Override
  public boolean tryAcquireLeadership(GenerationId generationId) {
    try {
      Lease inMemoryLease = this.leases.get(getLeaseKey(generationId));
      if (inMemoryLease == null) {
        LOG.warn("No in-memory lease found for {}, cannot acquire leadership", generationId);
        return false;
      }

      // Check if this is a new index (empty owner) or an acquirable existing lease.
      Instant now = Instant.now();
      boolean isNewIndex = inMemoryLease.leaseOwner().isEmpty();
      boolean weOwnLease = this.hostname.equals(inMemoryLease.leaseOwner());
      boolean leaseExpired = now.isAfter(inMemoryLease.leaseExpiration());

      LOG.atInfo()
          .addKeyValue("generationId", generationId)
          .addKeyValue("leaseOwner", inMemoryLease.leaseOwner())
          .addKeyValue("leaseExpiration", inMemoryLease.leaseExpiration())
          .addKeyValue("now", now)
          .addKeyValue("hostname", this.hostname)
          .addKeyValue("isNewIndex", isNewIndex)
          .addKeyValue("weOwnLease", weOwnLease)
          .addKeyValue("leaseExpired", leaseExpired)
          .log("Attempting to acquire leadership");

      Lease newLease = inMemoryLease.withRenewedOwnership(this.hostname);

      if (isNewIndex) {
        return createLeaseForNewIndex(generationId, newLease);
      }
      if (weOwnLease || leaseExpired) {
        return acquireExistingLease(generationId, inMemoryLease, newLease);
      }
      LOG.atDebug()
          .addKeyValue("generationId", generationId)
          .log("Cannot acquire leadership - lease is owned by another instance and not expired");
      return false;
    } catch (Exception e) {
      LOG.warn("Error attempting to acquire leadership for {}", generationId, e);
      return false;
    }
  }

  /**
   * Creates a new lease in the database for a new index. Uses insertOne to atomically detect
   * conflicts - if another instance already created the lease, we get a duplicate key error.
   *
   * <p>Example new lease being created:
   *
   * <pre>{@code
   * {
   *   "_id": "6930985def257a5ef2f7f823",
   *   "leaseOwner": "mongot-host-1.example.com",
   *   "leaseExpiration": "2024-01-15T10:35:00Z",  // now + 5 minutes
   *   "leaseVersion": 1,
   *   "commitInfo": "",
   *   "indexStatus": "NOT_STARTED"
   * }
   * }</pre>
   */
  private boolean createLeaseForNewIndex(GenerationId generationId, Lease newLease)
      throws Exception {
    try {
      this.operationExecutor.execute("createLease", () -> this.collection.insertOne(newLease));
      // Insert succeeded - we created the lease and are the leader.
      this.leases.put(getLeaseKey(generationId), newLease);
      this.followerGenerationIds.remove(generationId);
      this.leaderGenerationIds.add(generationId);
      LOG.atInfo()
          .addKeyValue("generationId", generationId)
          .addKeyValue("hostname", this.hostname)
          .addKeyValue("leaseExpiration", newLease.leaseExpiration())
          .log("Created new lease and acquired leadership for new index");
      return true;
    } catch (MongoWriteException e) {
      if (e.getError().getCategory() == ErrorCategory.DUPLICATE_KEY) {
        // Another instance created the lease first - refresh from DB and become follower.
        refreshLeaseFromDatabase(generationId);
        Lease existingLease = this.leases.get(getLeaseKey(generationId));
        LOG.atInfo()
            .addKeyValue("generationId", generationId)
            .addKeyValue("hostname", this.hostname)
            .addKeyValue(
                "winningOwner", existingLease != null ? existingLease.leaseOwner() : "unknown")
            .log("Lost race to create lease - another instance won");
        return false;
      }
      throw e; // Re-throw non-duplicate-key errors
    }
  }

  /**
   * Acquires an existing lease using optimistic concurrency control with idempotent filter. The
   * filter matches either the expected version (normal case) or the new version with our hostname
   * (idempotent case - we already wrote but didn't get response).
   *
   * <p>Example: Acquiring an expired lease from another instance.
   *
   * <p>Current lease in DB (expired):
   *
   * <pre>{@code
   * {
   *   "_id": "6930985def257a5ef2f7f823",
   *   "leaseOwner": "mongot-host-2.example.com",
   *   "leaseExpiration": "2024-01-15T10:25:00Z",  // expired
   *   "leaseVersion": 3,
   *   "commitInfo": "checkpoint_v3",
   *   "indexStatus": "READY"
   * }
   * }</pre>
   *
   * <p>New lease being written:
   *
   * <pre>{@code
   * {
   *   "_id": "6930985def257a5ef2f7f823",
   *   "leaseOwner": "mongot-host-1.example.com",  // new owner
   *   "leaseExpiration": "2024-01-15T10:35:00Z",  // now + 5 minutes
   *   "leaseVersion": 4,                          // incremented
   *   "commitInfo": "checkpoint_v3",              // preserved
   *   "indexStatus": "READY"                      // preserved
   * }
   * }</pre>
   */
  private boolean acquireExistingLease(
      GenerationId generationId, Lease currentLease, Lease newLease) throws Exception {
    boolean isReclaimingOwnLease = this.hostname.equals(currentLease.leaseOwner());

    var filter =
        createAcquireLeaseFilter(
            generationId, currentLease.leaseVersion(), newLease.leaseVersion());

    var result =
        this.operationExecutor.execute(
            "acquireLease", () -> this.collection.replaceOne(filter, newLease));

    if (result.getMatchedCount() > 0) {
      this.leases.put(getLeaseKey(generationId), newLease);
      this.followerGenerationIds.remove(generationId);
      this.leaderGenerationIds.add(generationId);
      if (isReclaimingOwnLease) {
        LOG.atInfo()
            .addKeyValue("generationId", generationId)
            .addKeyValue("hostname", this.hostname)
            .addKeyValue("newExpiration", newLease.leaseExpiration())
            .log("Reclaimed own lease after restart");
      } else {
        LOG.atInfo()
            .addKeyValue("generationId", generationId)
            .addKeyValue("previousOwner", currentLease.leaseOwner())
            .addKeyValue("previousExpiration", currentLease.leaseExpiration())
            .addKeyValue("hostname", this.hostname)
            .log("Acquired leadership from expired lease");
      }
      return true;
    } else {
      refreshLeaseFromDatabase(generationId);
      LOG.atInfo()
          .addKeyValue("generationId", generationId)
          .addKeyValue("hostname", this.hostname)
          .log("Failed to acquire leadership - lease was updated by another instance");
      return false;
    }
  }

  /**
   * Creates a filter for acquiring an existing lease (expired or owned by us after restart). This
   * filter does NOT include ownership check because we're intentionally taking over from another
   * owner or reclaiming our own lease.
   *
   * <p>The filter matches either:
   *
   * <ul>
   *   <li>The expected version (normal case), OR
   *   <li>The new version with our hostname (idempotent case - we already wrote but didn't get
   *       response)
   * </ul>
   *
   * @param generationId the generation ID for the lease
   * @param currentLeaseVersion the expected current lease version
   * @param newLeaseVersion the new lease version after update
   */
  private Bson createAcquireLeaseFilter(
      GenerationId generationId, long currentLeaseVersion, long newLeaseVersion) {
    return Filters.and(
        Filters.eq("_id", getLeaseKey(generationId)),
        Filters.or(
            // Normal case: version matches expected (we're taking over)
            Filters.eq(Lease.Fields.LEASE_VERSION.getName(), currentLeaseVersion),
            // Idempotent case: we already wrote but didn't get response
            Filters.and(
                Filters.eq(Lease.Fields.LEASE_VERSION.getName(), newLeaseVersion),
                Filters.eq(Lease.Fields.LEASE_OWNER.getName(), this.hostname))));
  }

  /**
   * Creates a filter for updating an owned lease (renew or update commit info). This filter
   * includes an ownership check to ensure we still own the lease before modifying it.
   *
   * <p>The filter matches either:
   *
   * <ul>
   *   <li>The expected version AND our ownership (normal case), OR
   *   <li>The new version AND our ownership (idempotent case - we already wrote but didn't get a
   *       response)
   * </ul>
   *
   * <p>TODO(CLOUDP-382207): There is a potential race condition where MaterializedViewWriter writes
   * to the MV collection before updating commit info in the lease. If the lease expires between the
   * MV write and the lease update, another instance could acquire leadership and also write to the
   * MV collection, resulting in double writes. Consider checking lease expiration before
   * MaterializedViewWriter::commit, though this doesn't fully eliminate the race without using
   * transactions.
   *
   * @param generationId the generation ID for the lease
   * @param currentLeaseVersion the expected current lease version
   * @param newLeaseVersion the new lease version after update
   */
  private Bson createUpdateFilterForOwnedLease(
      GenerationId generationId, long currentLeaseVersion, long newLeaseVersion) {
    return Filters.and(
        Filters.eq("_id", getLeaseKey(generationId)),
        Filters.eq(Lease.Fields.LEASE_OWNER.getName(), this.hostname),
        Filters.or(
            // Normal case: version matches expected
            Filters.eq(Lease.Fields.LEASE_VERSION.getName(), currentLeaseVersion),
            // Idempotent case: we already wrote but didn't get a response
            Filters.eq(Lease.Fields.LEASE_VERSION.getName(), newLeaseVersion)));
  }

  /** Refreshes the local lease copy from the database. */
  private void refreshLeaseFromDatabase(GenerationId generationId) {
    try {
      Lease lease = getLeaseFromDatabase(generationId);
      if (lease != null) {
        this.leases.put(getLeaseKey(generationId), lease);
      }
    } catch (Exception e) {
      LOG.warn("Failed to refresh lease from database for {}", generationId, e);
    }
  }

  /**
   * Renews the lease for a generation ID where this instance is the leader. If renewal fails (e.g.,
   * another instance took over), transitions to follower.
   *
   * <p>Example: Renewing a lease during a heartbeat.
   *
   * <p>Current lease:
   *
   * <pre>{@code
   * {
   *   "_id": "6930985def257a5ef2f7f823",
   *   "leaseOwner": "mongot-host-1.example.com",
   *   "leaseExpiration": "2024-01-15T10:32:00Z",  // 3 minutes remaining
   *   "leaseVersion": 5,
   *   "commitInfo": "checkpoint_v5",
   *   "indexStatus": "READY"
   * }
   * }</pre>
   *
   * <p>Renewed lease:
   *
   * <pre>{@code
   * {
   *   "_id": "6930985def257a5ef2f7f823",
   *   "leaseOwner": "mongot-host-1.example.com",  // same owner
   *   "leaseExpiration": "2024-01-15T10:35:00Z",  // now + 5 minutes
   *   "leaseVersion": 6,                          // incremented
   *   "commitInfo": "checkpoint_v5",              // preserved
   *   "indexStatus": "READY"                      // preserved
   * }
   * }</pre>
   */
  private void renewLease(GenerationId generationId) {
    Lease currentLease = this.leases.get(getLeaseKey(generationId));
    if (currentLease == null) {
      LOG.warn(
          "No local lease found for leader generation {}, transitioning to follower", generationId);
      becomeFollower(generationId);
      return;
    }

    // If our in-memory lease has expired, we've lost the right to be leader.
    // Another instance may have already taken over. Give up leadership.
    Instant now = Instant.now();
    if (now.isAfter(currentLease.leaseExpiration())) {
      LOG.warn(
          "In-memory lease expired for {}, transitioning to follower. " + "Expiration: {}, Now: {}",
          generationId,
          currentLease.leaseExpiration(),
          now);
      refreshLeaseFromDatabase(generationId);
      becomeFollower(generationId);
      return;
    }

    Lease renewedLease = currentLease.withRenewedOwnership(this.hostname);
    var filter =
        createUpdateFilterForOwnedLease(
            generationId, currentLease.leaseVersion(), renewedLease.leaseVersion());

    try {
      var result =
          this.operationExecutor.execute(
              "renewLease", () -> this.collection.replaceOne(filter, renewedLease));

      if (result.getMatchedCount() > 0) {
        // Successfully renewed.
        this.leases.put(getLeaseKey(generationId), renewedLease);
        LOG.atInfo()
            .addKeyValue("generationId", generationId)
            .addKeyValue("newExpiration", renewedLease.leaseExpiration())
            .addKeyValue("leaseVersion", renewedLease.leaseVersion())
            .log("Renewed lease");
      } else {
        // Lease was taken by another instance - lost leadership.
        LOG.warn(
            "Lease renewal failed for {} - lease was updated by another instance", generationId);
        refreshLeaseFromDatabase(generationId);
        becomeFollower(generationId);
      }
    } catch (Exception e) {
      // Transient error (network, etc.) - don't give up leadership yet.
      // Let the next heartbeat cycle retry. If the lease expires, we'll give up then.
      LOG.warn("Failed to renew lease for {}, will retry on next heartbeat", generationId, e);
    }
  }

  /** Transitions a generation ID from leader to follower state. */
  private void becomeFollower(GenerationId generationId) {
    this.leaderGenerationIds.remove(generationId);
    // Only add to follower if we're still managing this generation.
    // If the lease was removed by drop(), we must not re-add to followerGenerationIds.
    // This prevents a race condition where drop() removes the generation from both sets,
    // but a concurrent renewLease() or updateLeaseInDatabase() call re-adds it to follower.
    if (this.leases.containsKey(getLeaseKey(generationId))) {
      this.followerGenerationIds.add(generationId);
      LOG.atInfo()
          .addKeyValue("generationId", generationId)
          .addKeyValue("hostname", this.hostname)
          .log("Transitioned from leader to follower");
    } else {
      LOG.atInfo()
          .addKeyValue("generationId", generationId)
          .addKeyValue("hostname", this.hostname)
          .log("Not transitioning to follower - lease was removed by drop()");
    }
  }

  private void ensureLeaseExists(GenerationId generationId) {
    if (!this.leases.containsKey(getLeaseKey(generationId))) {
      throw new IllegalStateException("Lease does not exist for " + getLeaseKey(generationId));
    }
  }

  @VisibleForTesting
  static String getLeaseKey(GenerationId generationId) {
    return generationId.indexId.toHexString();
  }

  /**
   * Fetches a single lease from the database by generationId.
   *
   * <p>Example returned lease:
   *
   * <pre>{@code
   * {
   *   "_id": "6930985def257a5ef2f7f823",
   *   "leaseOwner": "mongot-host-1.example.com",
   *   "leaseExpiration": "2024-01-15T10:35:00Z",
   *   "leaseVersion": 5,
   *   "commitInfo": "checkpoint_v5",
   *   "indexStatus": "READY"
   * }
   * }</pre>
   *
   * @return the lease document, or null if not found
   */
  private Lease getLeaseFromDatabase(GenerationId generationId) throws Exception {
    return this.operationExecutor.execute(
        "getLease",
        () -> this.collection.find(new Document("_id", getLeaseKey(generationId))).first());
  }

  /**
   * Updates the lease in the database with new commit info. Uses optimistic concurrency control.
   *
   * <p>Example: Updating commit info after writing to materialized view.
   *
   * <p>Current lease (before update):
   *
   * <pre>{@code
   * {
   *   "_id": "6930985def257a5ef2f7f823",
   *   "leaseOwner": "mongot-host-1.example.com",
   *   "leaseExpiration": "2024-01-15T10:35:00Z",
   *   "leaseVersion": 5,
   *   "commitInfo": "checkpoint_v5",
   *   "indexStatus": "READY"
   * }
   * }</pre>
   *
   * <p>Updated lease (after update):
   *
   * <pre>{@code
   * {
   *   "_id": "6930985def257a5ef2f7f823",
   *   "leaseOwner": "mongot-host-1.example.com",
   *   "leaseExpiration": "2024-01-15T10:35:00Z",  // unchanged
   *   "leaseVersion": 6,                          // incremented
   *   "commitInfo": "checkpoint_v6_new_data",     // updated
   *   "indexStatus": "READY"
   * }
   * }</pre>
   */
  private void updateLeaseInDatabase(
      GenerationId generationId,
      Lease currentLease,
      Lease updatedLease,
      EncodedUserData encodedUserData) {
    try {
      // Base filter checks ownership and version (normal or idempotent case).
      var baseFilter =
          createUpdateFilterForOwnedLease(
              generationId, currentLease.leaseVersion(), updatedLease.leaseVersion());
      // For the idempotent case, also verify commitInfo matches to confirm it was our write.
      var filter =
          Filters.and(
              baseFilter,
              Filters.or(
                  Filters.eq(Lease.Fields.LEASE_VERSION.getName(), currentLease.leaseVersion()),
                  Filters.eq(Lease.Fields.COMMIT_INFO.getName(), encodedUserData.asString())));
      var result =
          this.operationExecutor.execute(
              "updateLease", () -> this.collection.replaceOne(filter, updatedLease));
      if (result.getMatchedCount() == 0) {
        // OCC failure - we lost leadership (or lease was deleted during index drop).
        becomeFollower(generationId);
        LOG.warn(
            "Failed to update lease for {} - ownership/version mismatch or lease deleted.",
            getLeaseKey(generationId));
      } else {
        this.leases.put(getLeaseKey(generationId), updatedLease);
      }
    } catch (Exception e) {
      // Transient error (e.g., network issue) - throw so caller can retry on next cycle.
      throw new MaterializedViewTransientException(e);
    }
  }

  private static MongoClient getMongoClient(
      SyncSourceConfig syncSourceConfig, MeterAndFtdcRegistry meterAndFtdcRegistry) {
    // Use mongosUri if available (for sharded clusters), otherwise use mongodClusterUri (for
    // replica sets). We use mongodClusterUri instead of mongodUri because mongodUri is a direct
    // connection to a specific node (often a secondary), while mongodClusterUri contains all
    // replica set members and allows the driver to route to the primary. This is required for
    // LINEARIZABLE read concern and write operations.
    var originalConnectionString =
        syncSourceConfig.mongosUri.orElse(syncSourceConfig.mongodClusterUri);

    // Replace directConnection=true with directConnection=false to enable topology discovery.
    // MMS provides connection strings with directConnection=true which forces the driver to
    // connect only to the specified host . For lease operations, we need to route to the primary,
    // so we must enable topology discovery by setting directConnection=false.
    // TODO(CLOUDP-360542): have mms return connection strings with directConnection=false.
    var connectionString = disableDirectConnection(originalConnectionString);

    LOG.atInfo()
        .addKeyValue("hosts", connectionString.getHosts())
        .addKeyValue("directConnection", connectionString.isDirectConnection())
        .addKeyValue("replicaSet", connectionString.getRequiredReplicaSetName())
        .log("Creating MongoClient for DynamicLeaderLeaseManager");
    return MongoClientBuilder.buildNonReplicationWithDefaults(
        connectionString,
        "Dynamic Lease Manager mongo client",
        MONGO_CLIENT_MAX_CONNECTIONS,
        syncSourceConfig.sslContext,
        meterAndFtdcRegistry.meterRegistry());
  }

  /**
   * Disables direct connection in a connection string to enable topology discovery.
   *
   * <p>MMS provides connection strings with directConnection=true, which forces the MongoDB driver
   * to connect only to the specified host. This prevents the driver from discovering the primary
   * node in a replica set. For lease operations that require LINEARIZABLE read concern and writes,
   * we need to route to the primary, so we replace directConnection=true with
   * directConnection=false.
   *
   * <p>Note: We cannot simply remove directConnection because the MongoDB ConnectionString class is
   * immutable and has no builder pattern. Replacing the value in the URI string is the most
   * practical approach.
   *
   * <p>The MongoDB {@link ConnectionString} class is immutable and doesn't provide a builder
   * pattern for modification. Since we only need to change the directConnection option, string
   * replacement is the most practical approach.
   *
   * @param connectionString the original connection string
   * @return a new connection string with directConnection=false, or the original if it wasn't true
   */
  private static ConnectionString disableDirectConnection(ConnectionString connectionString) {
    if (!Boolean.TRUE.equals(connectionString.isDirectConnection())) {
      // directConnection is not set or is false, no need to modify
      return connectionString;
    }

    // Replace directConnection=true with directConnection=false
    String originalUri = connectionString.getConnectionString();
    // Replace directConnection=true with directConnection=false. This handles both cases:
    // - ?directConnection=true (first/only option)
    // - &directConnection=true (subsequent option)
    String modifiedUri = originalUri.replaceAll("directConnection=true", "directConnection=false");

    try {
      return ConnectionStringUtil.fromString(modifiedUri);
    } catch (ConnectionStringUtil.InvalidConnectionStringException e) {
      LOG.atError()
          .addKeyValue("originalUri", originalUri)
          .addKeyValue("modifiedUri", modifiedUri)
          .log("Failed to parse modified connection string, using original");
      return connectionString;
    }
  }

  private String getIndexDefinitionVersion(IndexGeneration indexGeneration) {
    return String.valueOf(
        indexGeneration
            .getDefinition()
            .getDefinitionVersion()
            .orElse(DEFAULT_INDEX_DEFINITION_VERSION));
  }
}
