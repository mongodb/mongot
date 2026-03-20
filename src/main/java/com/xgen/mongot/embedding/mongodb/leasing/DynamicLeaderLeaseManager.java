package com.xgen.mongot.embedding.mongodb.leasing;

import static com.xgen.mongot.embedding.mongodb.leasing.StatusResolutionUtils.getEffectiveMaterializedViewStatus;
import static com.xgen.mongot.util.FutureUtils.COMPLETED_FUTURE;
import static com.xgen.mongot.util.Uuids.NIL;
import static com.xgen.mongot.util.mongodb.MongoDbDatabase.getCollectionInfo;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.errorprone.annotations.Var;
import com.mongodb.ErrorCategory;
import com.mongodb.MongoWriteException;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.result.UpdateResult;
import com.mongodb.lang.Nullable;
import com.xgen.mongot.embedding.config.MaterializedViewCollectionMetadata;
import com.xgen.mongot.embedding.config.MaterializedViewCollectionMetadataCatalog;
import com.xgen.mongot.embedding.exceptions.MaterializedViewNonTransientException;
import com.xgen.mongot.embedding.exceptions.MaterializedViewTransientException;
import com.xgen.mongot.embedding.utils.MongoClientOperationExecutor;
import com.xgen.mongot.index.EncodedUserData;
import com.xgen.mongot.index.IndexGeneration;
import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.definition.IndexDefinitionGeneration;
import com.xgen.mongot.index.status.IndexStatus;
import com.xgen.mongot.index.version.GenerationId;
import com.xgen.mongot.metrics.MeterAndFtdcRegistry;
import com.xgen.mongot.metrics.MetricsFactory;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.mongodb.MongoClientBuilder;
import com.xgen.mongot.util.mongodb.SyncSourceConfig;
import com.xgen.mongot.util.mongodb.serialization.MongoDbCollectionInfo;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.bson.BsonDocument;
import org.bson.BsonTimestamp;
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

  // TODO(CLOUDP-356242): make this configurable
  private static final String AUTO_EMBEDDING_INTERNAL_DATABASE_NAME = "__mdb_internal_search";

  private static final String METRICS_NAMESPACE = "embedding.leasing.stats";

  private static final int MONGO_CLIENT_MAX_CONNECTIONS = 2;

  @VisibleForTesting static final String LEASE_COLLECTION_NAME = "auto_embedding_leases";

  public static final long DEFAULT_INDEX_DEFINITION_VERSION = 0;

  private static final long GIVE_UP_BLACKOUT_SECONDS = 60;

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
  private final MongoCollection<BsonDocument> collection;
  private final MaterializedViewCollectionMetadataCatalog mvMetadataCatalog;
  private final MongoClient mongoClient;
  private final String databaseName;
  // End of give-up blackout (epoch ms). While now < this, do not acquire leadership.
  private final AtomicLong giveUpBlackoutEndTimeMs = new AtomicLong(0);

  public DynamicLeaderLeaseManager(
      MongoClient mongoClient,
      MetricsFactory metricsFactory,
      String hostname,
      String databaseName,
      MaterializedViewCollectionMetadataCatalog mvMetadataCatalog) {
    this.operationExecutor =
        new MongoClientOperationExecutor(metricsFactory, "leaseTableCollection");
    this.hostname = hostname;
    this.mvMetadataCatalog = mvMetadataCatalog;
    this.leases = new ConcurrentHashMap<>();
    this.leaderGenerationIds = ConcurrentHashMap.newKeySet();
    this.followerGenerationIds = ConcurrentHashMap.newKeySet();
    this.generationIdToDefinitionVersion = new ConcurrentHashMap<>();
    this.databaseName = databaseName;
    this.collection =
        mongoClient
            .getDatabase(databaseName)
            .getCollection(LEASE_COLLECTION_NAME, BsonDocument.class)
            .withReadConcern(ReadConcern.LINEARIZABLE)
            .withReadPreference(ReadPreference.primary());
    this.mongoClient = mongoClient;
  }

  public static DynamicLeaderLeaseManager create(
      SyncSourceConfig syncSourceConfig,
      MeterAndFtdcRegistry meterAndFtdcRegistry,
      String hostname,
      MaterializedViewCollectionMetadataCatalog mvMetadataCatalog,
      LeaseManagerOpsCommands opsCommands) {
    DynamicLeaderLeaseManager manager =
        new DynamicLeaderLeaseManager(
            getMongoClient(syncSourceConfig, meterAndFtdcRegistry),
            new MetricsFactory(METRICS_NAMESPACE, meterAndFtdcRegistry.meterRegistry()),
            hostname,
            AUTO_EMBEDDING_INTERNAL_DATABASE_NAME,
            mvMetadataCatalog);
    manager.opsGiveUpLease(opsCommands.opsGiveUpLease());
    return manager;
  }

  /**
   * Applies the ops give-up lease command if applicable (instance match, not expired). When
   * leaseNames is non-empty, gives up those leases (best-effort) and sets a 60s blackout. When
   * leaseNames is empty, only sets blackout (instance signals overloaded, step away from taking new
   * leases).
   */
  void opsGiveUpLease(Optional<LeaseManagerOpsCommands.OpsGiveUpLeaseCommand> opsGiveUpLease) {
    if (opsGiveUpLease.isEmpty()) {
      return;
    }
    LeaseManagerOpsCommands.OpsGiveUpLeaseCommand cmd = opsGiveUpLease.get();
    if (!cmd.instance().equals(this.hostname)) {
      LOG.atDebug()
          .addKeyValue("commandInstance", cmd.instance())
          .addKeyValue("hostname", this.hostname)
          .log("Ignoring ops give-up lease - instance mismatch");
      return;
    }
    if (Instant.now().isAfter(cmd.expiresAt())) {
      LOG.atDebug()
          .addKeyValue("expiresAt", cmd.expiresAt())
          .log("Ignoring ops give-up lease - expired");
      return;
    }
    if (!cmd.leaseNames().isEmpty()) {
      applyGiveUpLease(cmd.leaseNames());
    }
    long blackoutEnd = System.currentTimeMillis() + GIVE_UP_BLACKOUT_SECONDS * 1000;
    this.giveUpBlackoutEndTimeMs.set(blackoutEnd);
    LOG.atInfo()
        .addKeyValue("leaseNames", cmd.leaseNames())
        .addKeyValue("blackoutSeconds", GIVE_UP_BLACKOUT_SECONDS)
        .log("Applied ops give-up lease and set blackout");
  }

  /**
   * Gives up ownership of the specified leases: expires them in the database and clears local
   * leader state. Only affects leases owned by this instance.
   */
  private void applyGiveUpLease(List<String> giveUpLeaseNames) {
    for (String leaseKeyToGiveUp : giveUpLeaseNames) {
      LOG.atInfo()
          .addKeyValue("leaseKeyToGiveUp", leaseKeyToGiveUp)
          .addKeyValue("hostname", this.hostname)
          .log("Attempting to give up lease");
      Lease lease = this.leases.get(leaseKeyToGiveUp);
      if (lease == null || !this.hostname.equals(lease.leaseOwner())) {
        LOG.atInfo()
            .addKeyValue("leaseKeyToGiveUp", leaseKeyToGiveUp)
            .addKeyValue("hostname", lease != null ? lease.leaseOwner() : "")
            .addKeyValue("realHostname", this.hostname)
            .log("Empty in-mem lease or we don't own it - not giving up");
        continue;
      }
      Lease released = lease.withReleasedOwnership();
      Bson filter =
          createUpdateFilterForOwnedLease(
              leaseKeyToGiveUp, lease.leaseVersion(), released.leaseVersion());
      try {
        UpdateResult result =
            this.operationExecutor.execute(
                "giveUpLease", () -> this.collection.replaceOne(filter, released.toBson()));
        if (result.getModifiedCount() == 1) {
          this.leases.put(leaseKeyToGiveUp, released);
          LOG.atInfo()
              .addKeyValue("leaseKeyToGiveUp", leaseKeyToGiveUp)
              .addKeyValue("hostname", this.hostname)
              .log("Gave up lease for rebalance");
        } else {
          LOG.atError()
              .addKeyValue("leaseKeyToGiveUp", leaseKeyToGiveUp)
              .addKeyValue("hostname", this.hostname)
              .log("No lease document updated, rebalance failed");
        }
      } catch (Exception e) {
        LOG.warn("Failed to give up lease for {}", leaseKeyToGiveUp, e);
      }
    }
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
   */
  public void syncLeasesFromMongod() {
    try {
      List<BsonDocument> rawLeases =
          this.operationExecutor.execute(
              "getLeases", () -> this.collection.find().into(new ArrayList<>()));
      for (BsonDocument rawLease : rawLeases) {
        Lease lease = normalizeLeaseIfNeeded(Lease.fromBson(rawLease));
        if (lease != null) {
          this.leases.put(lease.id(), lease);
        } else {
          // TODO(CLOUDP-384971): clean up corrupted leases
          LOG.atError()
              .addKeyValue("leaseId", rawLease.getString("_id"))
              .log("Corrupted lease found, skipping");
        }
      }
      LOG.atInfo()
          .addKeyValue("leaseCount", rawLeases.size())
          .addKeyValue("hostname", this.hostname)
          .log("Initialized leases from database");
    } catch (Exception e) {
      // initializeLease calls should populate in memory leases individually,
      LOG.atError()
          .setCause(e)
          .addKeyValue("hostname", this.hostname)
          .log("syncLeasesFromMongod fails, skipping syncLeases to avoid crash.");
    }
  }

  // Normalizes lease by populating missing mat view UUID field, this won't change lease
  // version since it's only meant for backward compatibility and lease state is unchanged.
  @Nullable
  private Lease normalizeLeaseIfNeeded(Lease lease) {
    if (!lease.materializedViewCollectionMetadata().collectionUuid().equals(NIL)) {
      // No need to normalize it by resolving mat view collection UUID.
      return lease;
    }
    try {
      return lease.withResolvedMatViewUuid(
          Check.instanceOf(
                  getCollectionInfo(
                      this.mongoClient,
                      this.databaseName,
                      lease.materializedViewCollectionMetadata().collectionName()),
                  MongoDbCollectionInfo.Collection.class)
              .info()
              .uuid());
    } catch (Exception e) {
      LOG.atWarn()
          .addKeyValue("leaseId", lease.id())
          .addKeyValue("leaseOwner", lease.leaseOwner())
          .addKeyValue(
              "matViewCollectionName", lease.materializedViewCollectionMetadata().collectionName())
          .setCause(e)
          .log(
              "Unable to normalize or validate lease, "
                  + "could be caused by dangling lease or corrupted lease");
      // TODO(CLOUDP-384971): We should have a way to clean up corrupted leases to avoid blocking
      // Lease creation.
      return null;
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
    String versionKey = getIndexDefinitionVersion(indexGeneration.getDefinition());
    this.generationIdToDefinitionVersion.put(generationId, versionKey);

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
      // mvMetadataCatalog should already have metadata for this generationId at this point after
      // CollectionResolver calls this.getMaterializedViewCollectionMetadata.
      MaterializedViewCollectionMetadata materializedViewCollectionMetadata =
          this.mvMetadataCatalog.getMetadata(generationId);
      Lease newLease =
          Lease.newLease(
              getLeaseKey(generationId),
              // This is source collection UUID
              indexGeneration.getDefinition().getCollectionUuid(),
              indexGeneration.getDefinition().getLastObservedCollectionName(),
              "", // Empty owner - no one owns this lease yet
              versionKey,
              indexGeneration.getIndex().getStatus(),
              materializedViewCollectionMetadata);
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

      BsonTimestamp oplogPosition = currentLease.extractHighWaterMark().orElse(null);
      Lease updatedLease =
          currentLease.withUpdatedStatus(indexStatus, indexDefinitionVersion, oplogPosition);
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
      List<BsonDocument> rawLeases =
          this.operationExecutor.execute(
              "getFollowerLeases",
              () -> this.collection.find(Filters.in("_id", leaseKeys)).into(new ArrayList<>()));
      for (BsonDocument rawLease : rawLeases) {
        Lease lease = Lease.fromBson(rawLease);
        fetchedLeases.put(lease.id(), lease);
      }
      LOG.atDebug()
          .addKeyValue("requestedCount", leaseKeys.size())
          .addKeyValue("fetchedCount", rawLeases.size())
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
   * Used only when executing the give up lease ops command. Returns whether this instance is in the
   * give-up lease blackout window. While true, this instance will not attempt to acquire leadership
   * (e.g. after an ops give-up lease command for rebalancing).
   *
   * @return true if in blackout, false otherwise
   */
  @Override
  public boolean isInLeaseAcquisitionBlackout() {
    return System.currentTimeMillis() < this.giveUpBlackoutEndTimeMs.get();
  }

  /**
   * Attempts to acquire leadership for the given generation ID. This method is called by
   * MaterializedViewManager when it detects that a lease is acquirable (expired, owned by us, or
   * new).
   *
   * <p>For new indexes (in-memory lease with empty owner by this::initializeLease) or other
   * existing leases, leadership is acquired using optimistic concurrency control.
   *
   * @return true if leadership was successfully acquired, false otherwise
   */
  @Override
  public boolean tryAcquireLeadership(GenerationId generationId) {
    if (isInLeaseAcquisitionBlackout()) {
      LOG.atDebug()
          .addKeyValue("generationId", generationId)
          .log("Skipping leadership acquisition - in give-up blackout");
      return false;
    }
    try {
      Lease inMemoryLease = this.leases.get(getLeaseKey(generationId));
      if (inMemoryLease == null) {
        LOG.warn("No in-memory lease found for {}, cannot acquire leadership", generationId);
        return false;
      }

      // Check if this is a new index (empty owner) or an acquirable existing lease.
      Instant now = Instant.now();
      boolean weOwnLease = this.hostname.equals(inMemoryLease.leaseOwner());
      boolean leaseExpired = now.isAfter(inMemoryLease.leaseExpiration());

      LOG.atInfo()
          .addKeyValue("generationId", generationId)
          .addKeyValue("leaseOwner", inMemoryLease.leaseOwner())
          .addKeyValue("leaseExpiration", inMemoryLease.leaseExpiration())
          .addKeyValue("now", now)
          .addKeyValue("hostname", this.hostname)
          .addKeyValue("weOwnLease", weOwnLease)
          .addKeyValue("leaseExpired", leaseExpired)
          .log("Attempting to acquire leadership");

      Lease newLease = inMemoryLease.withRenewedOwnership(this.hostname);
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

  @Override
  public Optional<BsonTimestamp> getSteadyAsOfOplogPosition(GenerationId generationId) {
    return Optional.ofNullable(this.leases.get(getLeaseKey(generationId)))
        .flatMap(Lease::getSteadyAsOfOplogPosition);
  }

  @Override
  public MaterializedViewCollectionMetadata initializeLease(
      IndexDefinitionGeneration indexDefinitionGeneration,
      MaterializedViewCollectionMetadata proposedMetadata)
      throws Exception {
    var existingLease = getLeaseFromDatabase(proposedMetadata.collectionName());
    if (existingLease != null) {
      // If another Mongot already created the initial lease before this mongot calls method
      // syncLeasesFromMongod in the constructor, just reuse, no need to make another network call.
      return existingLease.materializedViewCollectionMetadata();
    }
    Lease lease =
        Lease.initialLease(
            // Materialized View Collection Name from CollectionResolver
            proposedMetadata.collectionName(),
            // Source collection UUID
            indexDefinitionGeneration.getIndexDefinition().getCollectionUuid(),
            // Source collection name
            indexDefinitionGeneration.getIndexDefinition().getLastObservedCollectionName(),
            getIndexDefinitionVersion(indexDefinitionGeneration.getIndexDefinition()),
            IndexStatus.unknown(),
            proposedMetadata);
    boolean success = createLeaseForNewIndex(lease);
    if (success) {
      LOG.atInfo()
          .addKeyValue("generationId", indexDefinitionGeneration.getGenerationId())
          .addKeyValue("hostname", this.hostname)
          .addKeyValue("leaseExpiration", lease.leaseExpiration())
          .log("Created new lease entry for new index");
      return proposedMetadata;
    } else {
      Lease existing = this.leases.get(lease.id());
      LOG.atInfo()
          .addKeyValue("generationId", indexDefinitionGeneration.getGenerationId())
          .addKeyValue("hostname", this.hostname)
          .addKeyValue("winningOwner", existing.leaseOwner())
          .log("Lost race to create lease - another instance won");
      return existing.materializedViewCollectionMetadata();
    }
  }

  // TEST ONLY.
  @VisibleForTesting
  Map<String, Lease> getLeases() {
    return ImmutableMap.copyOf(this.leases);
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
  private boolean createLeaseForNewIndex(Lease newLease) throws Exception {
    try {
      this.operationExecutor.execute(
          "createLease", () -> this.collection.insertOne(newLease.toBson()));
      // Insert succeeded - we created the lease and synchronized in-memory lease state.
      this.leases.put(newLease.id(), newLease);
      // Don't add or remove generationId into this.followerGenerationIds or
      // this.leaderGenerationIds, which may break MaterializedViewManager when it's refreshing
      // status by looking up MaterializedViewCollectionMetadataCatalog.
      return true;
    } catch (MongoWriteException e) {
      if (e.getError().getCategory() == ErrorCategory.DUPLICATE_KEY) {
        // Another instance created the lease first - refresh from DB and synchronize it.
        Lease existingLease = getLeaseFromDatabase(newLease.id());
        if (existingLease == null) {
          // TODO(CLOUDP-384971): We should have a way to clean up corrupted leases to avoid
          // blocking Lease creation.
          throw new IllegalStateException(
              "Unable to create nor read existing lease in Lease table. "
                  + "This could be caused by dangling corrupted lease");
        }
        this.leases.put(existingLease.id(), existingLease);
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
            "acquireLease", () -> this.collection.replaceOne(filter, newLease.toBson()));

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
   * @param leaseKey the key and _id the lease
   * @param currentLeaseVersion the expected current lease version
   * @param newLeaseVersion the new lease version after update
   */
  private Bson createUpdateFilterForOwnedLease(
      String leaseKey, long currentLeaseVersion, long newLeaseVersion) {
    return Filters.and(
        Filters.eq("_id", leaseKey),
        Filters.eq(Lease.Fields.LEASE_OWNER.getName(), this.hostname),
        Filters.or(
            // Normal case: version matches expected
            Filters.eq(Lease.Fields.LEASE_VERSION.getName(), currentLeaseVersion),
            // Idempotent case: we already wrote but didn't get a response
            Filters.eq(Lease.Fields.LEASE_VERSION.getName(), newLeaseVersion)));
  }

  /**
   * Refreshes the local lease copy from the database.
   *
   * <p>Note: Can only be called after mvMetadataCatalog is populated.
   */
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
            getLeaseKey(generationId), currentLease.leaseVersion(), renewedLease.leaseVersion());

    try {
      var result =
          this.operationExecutor.execute(
              "renewLease", () -> this.collection.replaceOne(filter, renewedLease.toBson()));

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

  private String getLeaseKey(GenerationId generationId) {
    return this.mvMetadataCatalog.getMetadata(generationId).collectionName();
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
   * <p>Note: Can only be called after mvMetadataCatalog is populated.
   *
   * @return the lease document, or null if not found
   */
  @Nullable
  private Lease getLeaseFromDatabase(GenerationId generationId) throws Exception {
    return getLeaseFromDatabase(getLeaseKey(generationId));
  }

  @Nullable
  private Lease getLeaseFromDatabase(String collectionName) throws Exception {
    BsonDocument rawLease =
        this.operationExecutor.execute(
            "getLease", () -> this.collection.find(new Document("_id", collectionName)).first());
    if (rawLease == null) {
      return null;
    }
    return normalizeLeaseIfNeeded(Lease.fromBson(rawLease));
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
              getLeaseKey(generationId), currentLease.leaseVersion(), updatedLease.leaseVersion());
      // For the idempotent case, also verify commitInfo matches to confirm it was our write.
      var filter =
          Filters.and(
              baseFilter,
              Filters.or(
                  Filters.eq(Lease.Fields.LEASE_VERSION.getName(), currentLease.leaseVersion()),
                  Filters.eq(Lease.Fields.COMMIT_INFO.getName(), encodedUserData.asString())));
      var result =
          this.operationExecutor.execute(
              "updateLease", () -> this.collection.replaceOne(filter, updatedLease.toBson()));
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
    // Use mongosUri if available (for sharded clusters), otherwise use mongodClusterReadWriteUri
    // (for replica sets). We use mongodClusterReadWriteUri instead of mongodUri because mongodUri
    // is a direct connection to a specific node (often a secondary), while mongodClusterUri
    // contains all replica set members and allows the driver to route to the primary. This is
    // required for LINEARIZABLE read concern and write operations.
    var syncSource = syncSourceConfig.mongosUri.orElse(syncSourceConfig.mongodClusterReadWriteUri);
    LOG.atInfo()
        .addKeyValue("hosts", syncSource.uri().getHosts())
        .addKeyValue("directConnection", syncSource.uri().isDirectConnection())
        .addKeyValue("replicaSet", syncSource.uri().getRequiredReplicaSetName())
        .log("Creating MongoClient for DynamicLeaderLeaseManager");
    return MongoClientBuilder.buildNonReplicationWithDefaults(
        syncSource.uri(),
        "Dynamic Lease Manager mongo client",
        MONGO_CLIENT_MAX_CONNECTIONS,
        syncSource.sslContext(),
        meterAndFtdcRegistry.meterRegistry());
  }

  private String getIndexDefinitionVersion(IndexDefinition indexDefinition) {
    return String.valueOf(
        indexDefinition.getDefinitionVersion().orElse(DEFAULT_INDEX_DEFINITION_VERSION));
  }
}
