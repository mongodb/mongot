package com.xgen.mongot.embedding.mongodb.leasing;

import static com.xgen.mongot.embedding.mongodb.leasing.StatusResolutionUtils.getEffectiveMaterializedViewStatus;
import static com.xgen.mongot.util.FutureUtils.COMPLETED_FUTURE;
import static com.xgen.mongot.util.Uuids.NIL;
import static com.xgen.mongot.util.mongodb.MongoDbDatabase.getCollectionInfo;

import com.google.common.annotations.VisibleForTesting;
import com.google.errorprone.annotations.Var;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.lang.Nullable;
import com.xgen.mongot.embedding.config.MaterializedViewCollectionMetadata;
import com.xgen.mongot.embedding.config.MaterializedViewCollectionMetadataCatalog;
import com.xgen.mongot.embedding.exceptions.MaterializedViewNonTransientException;
import com.xgen.mongot.embedding.exceptions.MaterializedViewTransientException;
import com.xgen.mongot.embedding.utils.MongoClientOperationExecutor;
import com.xgen.mongot.index.EncodedUserData;
import com.xgen.mongot.index.IndexGeneration;
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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import org.bson.BsonDocument;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A lease manager that uses a static leader. Leadership status is pre-assigned and remains constant
 * for the lifetime of the process. Uses a MongoDB collection to store leases.
 *
 * <p>Expected to be used as a singleton.
 *
 * <p>TODO(CLOUDP-382207): Deprecate StaticLeaderLeaseManager
 *
 * @deprecated Use {@link DynamicLeaderLeaseManager} instead, which supports dynamic per-index
 *     leader election and handles leadership transitions gracefully.
 */
@Deprecated
public class StaticLeaderLeaseManager implements LeaseManager {

  private static final Logger LOG = LoggerFactory.getLogger(StaticLeaderLeaseManager.class);

  // TODO(CLOUDP-356242): make this configurable
  private static final String AUTO_EMBEDDING_INTERNAL_DATABASE_NAME = "__mdb_internal_search";

  private static final String METRICS_NAMESPACE = "embedding.leasing.stats";

  private static final ReplaceOptions REPLACE_OPTIONS = new ReplaceOptions().upsert(true);

  // placeholder value until we see the need to change this.
  private static final int MONGO_CLIENT_MAX_CONNECTIONS = 2;

  @VisibleForTesting static final String LEASE_COLLECTION_NAME = "auto_embedding_leases";

  public static final long DEFAULT_INDEX_DEFINITION_VERSION = 0;

  private final MongoClientOperationExecutor operationExecutor;
  private final String hostname;
  // Mapping of leases to index Ids.
  private final Map<String, Lease> leases;
  // Tracks all GenerationIds that have been added to this lease manager (both leader and follower).
  private final Set<GenerationId> managedGenerationIds;
  // Maps GenerationId to its definition version (as String) for use in pollFollowerStatuses().
  private final Map<GenerationId, String> generationIdToDefinitionVersion;
  private final MongoCollection<BsonDocument> collection;
  private final MongoClient mongoClient;
  private final boolean isLeader;
  private final MaterializedViewCollectionMetadataCatalog mvMetadataCatalog;
  private final String databaseName;

  /** Init static lease manager */
  public StaticLeaderLeaseManager(
      MongoClient mongoClient,
      MetricsFactory metricsFactory,
      String hostname,
      String databaseName,
      boolean isLeader,
      MaterializedViewCollectionMetadataCatalog mvMetadataCatalog) {
    this.operationExecutor =
        new MongoClientOperationExecutor(metricsFactory, "leaseTableCollection");
    this.hostname = hostname;
    this.mvMetadataCatalog = mvMetadataCatalog;
    this.leases = new ConcurrentHashMap<>();
    this.managedGenerationIds = ConcurrentHashMap.newKeySet();
    this.generationIdToDefinitionVersion = new ConcurrentHashMap<>();
    this.mongoClient = mongoClient;
    this.databaseName = databaseName;
    // Use LINEARIZABLE read concern for lease operations to ensure we always read the most
    // up-to-date lease state. This is critical for lease correctness.
    // LINEARIZABLE read concern requires ReadPreference.primary() to work correctly.
    this.collection =
        this.mongoClient
            .getDatabase(databaseName)
            .getCollection(LEASE_COLLECTION_NAME, BsonDocument.class)
            .withReadConcern(ReadConcern.LINEARIZABLE)
            .withReadPreference(ReadPreference.primary());
    this.isLeader = isLeader;
  }

  public static StaticLeaderLeaseManager create(
      SyncSourceConfig syncSourceConfig,
      MeterAndFtdcRegistry meterAndFtdcRegistry,
      String hostname,
      boolean isLeader,
      MaterializedViewCollectionMetadataCatalog mvMetadataCatalog) {
    return new StaticLeaderLeaseManager(
        getMongoClient(syncSourceConfig, meterAndFtdcRegistry),
        new MetricsFactory(METRICS_NAMESPACE, meterAndFtdcRegistry.meterRegistry()),
        hostname,
        AUTO_EMBEDDING_INTERNAL_DATABASE_NAME,
        isLeader,
        mvMetadataCatalog);
  }

  /** Initializes the local lease state with the leases from the database. */
  public void syncLeasesFromMongod() {
    try {
      List<BsonDocument> rawLeases =
          this.operationExecutor.execute(
              "getLeases", () -> this.collection.find().into(new ArrayList<>()));
      for (BsonDocument rawLease : rawLeases) {
        Lease lease = normalizedLeaseIfNeeded(Lease.fromBson(rawLease));
        if (lease != null) {
          this.leases.put(lease.id(), lease);
        } else {
          // TODO(CLOUDP-384971): clean up corrupted leases
          LOG.atError()
              .addKeyValue("leaseId", rawLease.getString("_id"))
              .log("Corrupted lease found, skipping");
        }
      }
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
  private Lease normalizedLeaseIfNeeded(Lease lease) {
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

  @Override
  public void add(IndexGeneration indexGeneration) {
    // Create the lease if it doesn't exist. The lease can already exist in the case of process
    // restarts.
    // If the lease already exists, then just add the index generation to the lease.
    // Note that we only add the lease in memory here, we write to the database only when we update
    // the commit info.
    GenerationId generationId = indexGeneration.getGenerationId();
    this.managedGenerationIds.add(generationId);
    this.generationIdToDefinitionVersion.put(
        generationId, getIndexDefinitionVersion(indexGeneration));
    if (this.leases.containsKey(getLeaseKey(generationId))) {
      var lease = this.leases.get(getLeaseKey(generationId));
      String versionKey =
          String.valueOf(
              indexGeneration
                  .getDefinition()
                  .getDefinitionVersion()
                  .orElse(DEFAULT_INDEX_DEFINITION_VERSION));
      if (!lease.indexDefinitionVersionStatusMap().containsKey(versionKey)) {
        this.leases.put(
            getLeaseKey(generationId),
            lease.withNewIndexDefinitionVersion(
                getIndexDefinitionVersion(indexGeneration),
                indexGeneration.getIndex().getStatus()));
      }
    } else {
      Lease lease =
          Lease.newLease(
              getLeaseKey(generationId),
              indexGeneration.getDefinition().getCollectionUuid(),
              indexGeneration.getDefinition().getLastObservedCollectionName(),
              this.hostname,
              getIndexDefinitionVersion(indexGeneration),
              indexGeneration.getIndex().getStatus(),
              Check.isPresent(
                  this.mvMetadataCatalog.getMetadataIfPresent(generationId),
                  "matViewSchemaMetadata"));
      this.leases.put(getLeaseKey(generationId), lease);
    }
  }

  @Override
  public CompletableFuture<Void> drop(GenerationId generationId) {
    // The current drop implementation only handles index/lease deletion and not index generation
    // deletion. This is because there might be followers that are still relying on the status of
    // this index generation. We could potentially put an upper bound on the number of index
    // generations we track to prevent the status map from growing unbounded.
    // Note that we're relying on MaterializedViewManager to do the right thing based on reference
    // counting and only invoke this method when the last index generation is being dropped.
    //
    // We remove from managedGenerationIds before the async delete completes. If deleteOne fails,
    // the lease remains in the database but we no longer track it locally. This is acceptable
    // because: (1) drop is a terminal operation - we don't need to manage this generation anymore,
    // (2) orphaned leases in the database don't affect correctness and can be cleaned up later.
    this.managedGenerationIds.remove(generationId);
    this.generationIdToDefinitionVersion.remove(generationId);
    if (this.isLeader) {
      return CompletableFuture.runAsync(
          () -> {
            this.collection.deleteOne(new Document("_id", getLeaseKey(generationId)));
            this.leases.remove(getLeaseKey(generationId));
          });
    } else {
      this.leases.remove(getLeaseKey(generationId));
      return COMPLETED_FUTURE;
    }
  }

  @Override
  public boolean isLeader(GenerationId generationId) {
    return this.isLeader;
  }

  @Override
  public void updateCommitInfo(GenerationId generationId, EncodedUserData encodedUserData) {
    ensureLeaseExists(generationId);
    ensureLeader();
    Lease currentLease = this.leases.get(getLeaseKey(generationId));
    Lease updatedLease = currentLease.withUpdatedCheckpoint(encodedUserData);
    updateLeaseInDatabase(generationId, currentLease, updatedLease, encodedUserData);
  }

  @Override
  public EncodedUserData getCommitInfo(GenerationId generationId) throws IOException {
    // Leader can read from in-memory state.
    if (this.isLeader) {
      ensureLeaseExists(generationId);
      return EncodedUserData.fromString(this.leases.get(getLeaseKey(generationId)).commitInfo());
    }
    // If follower, read from the database. Although technically, a follower should never call this
    // method as it's on the write path that a follower should never go into.
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
  public void updateReplicationStatus(
      GenerationId generationId, long indexDefinitionVersion, IndexStatus indexStatus)
      throws MaterializedViewTransientException, MaterializedViewNonTransientException {
    // only update status in database if leader. Followers may still call this method, but we treat
    // it as a no-op.
    if (this.isLeader) {
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

  /**
   * Reads the status of a materialized view from the database and applies status resolution logic.
   *
   * @param generationId the generation ID to get status for
   * @param versionKey the definition version key to look up in the status map
   * @return the effective status, or UNKNOWN if unable to determine
   */
  private IndexStatus getMaterializedViewReplicationStatus(
      GenerationId generationId, String versionKey) {
    @Var
    Lease.IndexDefinitionVersionStatus requestedStatus =
        new Lease.IndexDefinitionVersionStatus(false, IndexStatus.StatusCode.UNKNOWN);
    @Var
    Lease.IndexDefinitionVersionStatus latestStatus =
        new Lease.IndexDefinitionVersionStatus(false, IndexStatus.StatusCode.UNKNOWN);

    try {
      Lease lease = getLeaseFromDatabase(generationId);
      if (lease != null) {
        if (lease.indexDefinitionVersionStatusMap().containsKey(versionKey)) {
          requestedStatus = lease.indexDefinitionVersionStatusMap().get(versionKey);
        } else {
          LOG.warn(
              "Requested version key {} not found in lease for generation ID {}",
              versionKey,
              generationId);
        }
        latestStatus =
            lease.indexDefinitionVersionStatusMap().get(lease.latestIndexDefinitionVersion());
      } else {
        LOG.warn("No lease found in database for generation ID {}", generationId);
      }
    } catch (Exception e) {
      LOG.warn("Failed to poll status for generation ID {}", generationId, e);
    }
    return getEffectiveMaterializedViewStatus(requestedStatus, latestStatus);
  }

  @VisibleForTesting
  Map<String, Lease> getLeases() {
    return this.leases;
  }

  @Override
  public Set<GenerationId> getLeaderGenerationIds() {
    if (this.isLeader) {
      return Collections.unmodifiableSet(this.managedGenerationIds);
    }
    return Collections.emptySet();
  }

  @Override
  public Set<GenerationId> getFollowerGenerationIds() {
    if (!this.isLeader) {
      return Collections.unmodifiableSet(this.managedGenerationIds);
    }
    return Collections.emptySet();
  }

  @Override
  public LeaseManager.FollowerPollResult pollFollowerStatuses() {
    if (this.isLeader) {
      // Leaders don't poll follower statuses
      return LeaseManager.FollowerPollResult.EMPTY;
    }
    Map<GenerationId, IndexStatus> statuses = new HashMap<>();
    for (GenerationId generationId : this.managedGenerationIds) {
      String versionKey = this.generationIdToDefinitionVersion.get(generationId);
      if (versionKey == null) {
        LOG.warn("No definition version found for generation ID {}", generationId);
        continue;
      }
      IndexStatus status = getMaterializedViewReplicationStatus(generationId, versionKey);
      statuses.put(generationId, status);
    }
    // Static leader doesn't support dynamic leader election, so no expired leases
    return new LeaseManager.FollowerPollResult(statuses, Collections.emptySet());
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
    // When using static leader in Community version, we assume all mongots are using the same MV
    // collection schema version, no rolling upgrades.
    return proposedMetadata;
  }

  @Override
  public Optional<BsonTimestamp> getSteadyAsOfOplogPosition(GenerationId generationId) {
    return Optional.ofNullable(this.leases.get(getLeaseKey(generationId)))
        .flatMap(Lease::getSteadyAsOfOplogPosition);
  }

  private void ensureLeaseExists(GenerationId generationId) {
    if (!this.leases.containsKey(getLeaseKey(generationId))) {
      throw new IllegalStateException("Lease does not exist for " + getLeaseKey(generationId));
    }
  }

  private void ensureLeader() {
    if (!this.isLeader) {
      throw new IllegalStateException(
          "Attempting to update lease state while not being the leader");
    }
  }

  private String getLeaseKey(GenerationId generationId) {
    return this.mvMetadataCatalog.getMetadata(generationId).collectionName();
  }

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
    return normalizedLeaseIfNeeded(Lease.fromBson(rawLease));
  }

  private void updateLeaseInDatabase(
      GenerationId generationId,
      Lease currentLease,
      Lease updatedLease,
      EncodedUserData encodedUserData)
      throws MaterializedViewTransientException, MaterializedViewNonTransientException {
    try {
      if (currentLease.leaseVersion() == Lease.FIRST_LEASE_VERSION) {
        // Lease document doesn't exist yet, so we can use a simple upsert.
        var filter = Filters.eq("_id", getLeaseKey(generationId));
        this.operationExecutor.execute(
            "createLease",
            () -> this.collection.replaceOne(filter, updatedLease.toBson(), REPLACE_OPTIONS));
        this.leases.put(getLeaseKey(generationId), updatedLease);
      } else {
        // Update with optimistic concurrency control on the lease version to ensure the lease
        // doesn't get updated with a stale checkpoint.
        // The filter first finds the document with the correct ID. Then it ensures the document
        // is in one of two states:
        // 1. The document has the same lease version as the local lease. This means the document
        //    has not been updated and the update can proceed.
        // 2. The document has been updated by a previous call which was processed by the server but
        // not
        //    acknowledged by the client. In this case, we check both the version and the commit
        // info to
        //    ensure it's in the desired state already. An update here is thus a no-op and safe.
        var filter =
            Filters.and(
                Filters.eq("_id", getLeaseKey(generationId)),
                Filters.or(
                    Filters.eq(Lease.Fields.LEASE_VERSION.getName(), currentLease.leaseVersion()),
                    Filters.and(
                        Filters.eq(
                            Lease.Fields.LEASE_VERSION.getName(), updatedLease.leaseVersion()),
                        // we could potentially do a deeper check across more fields here for more
                        // safety.
                        Filters.eq(
                            Lease.Fields.COMMIT_INFO.getName(), encodedUserData.asString()))));
        var result =
            this.operationExecutor.execute(
                "updateLease", () -> this.collection.replaceOne(filter, updatedLease.toBson()));
        if (result.getMatchedCount() == 0) {
          // This means the document was not in one of the two desired states described above.
          // TODO(CLOUDP-364787): We should move the index to failed state as this is not a
          // recoverable error.
          LOG.warn(
              "Failed to update lease for {} due to version mismatch. Local lease is {}. ",
              getLeaseKey(generationId),
              updatedLease);
          throw new MaterializedViewNonTransientException(
              "Fails to update lease for "
                  + getLeaseKey(generationId)
                  + "please check Lease collection to clean up corrupted records");
        } else {
          this.leases.put(getLeaseKey(generationId), updatedLease);
        }
      }
    } catch (Exception e) {
      // Only this lease has problem, fails this index only but keeps Mongot alive.
      if (e instanceof MaterializedViewNonTransientException matViewNonTransientException) {
        throw matViewNonTransientException;
      }
      // TODO(CLOUDP-371153): we need to handle this appropriately in MatViewGenerator as updating
      // status can fail
      throw new MaterializedViewTransientException(e);
    }
  }

  private static MongoClient getMongoClient(
      SyncSourceConfig syncSourceConfig, MeterAndFtdcRegistry meterAndFtdcRegistry) {
    // Use mongosUri if available, otherwise fall back to mongodClusterReadWriteUri. This allows the
    // MongoDB driver to automatically discover replica set topology and route writes to the
    // primary, avoiding NotWritablePrimary errors after failovers.
    var syncSource = syncSourceConfig.mongosUri.orElse(syncSourceConfig.mongodClusterReadWriteUri);
    return MongoClientBuilder.buildNonReplicationWithDefaults(
        syncSource.uri(),
        "Lease Manager mongo client",
        MONGO_CLIENT_MAX_CONNECTIONS,
        syncSource.sslContext(),
        meterAndFtdcRegistry.meterRegistry());
  }

  private String getIndexDefinitionVersion(IndexGeneration indexGeneration) {
    return String.valueOf(
        indexGeneration
            .getDefinition()
            .getDefinitionVersion()
            .orElse(DEFAULT_INDEX_DEFINITION_VERSION));
  }
}
