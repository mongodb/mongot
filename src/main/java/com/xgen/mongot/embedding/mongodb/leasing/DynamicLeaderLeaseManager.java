package com.xgen.mongot.embedding.mongodb.leasing;

import static com.xgen.mongot.embedding.mongodb.leasing.StatusResolutionUtils.getEffectiveMaterializedViewStatus;
import static com.xgen.mongot.util.FutureUtils.COMPLETED_FUTURE;
import static com.xgen.mongot.util.Uuids.NIL;
import static com.xgen.mongot.util.mongodb.MongoDbDatabase.getCollectionInfo;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.errorprone.annotations.Var;
import com.mongodb.ErrorCategory;
import com.mongodb.MongoNamespace;
import com.mongodb.MongoSocketException;
import com.mongodb.MongoTimeoutException;
import com.mongodb.MongoWriteException;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import com.mongodb.client.result.UpdateResult;
import com.mongodb.lang.Nullable;
import com.xgen.mongot.embedding.config.MaterializedViewCollectionMetadata;
import com.xgen.mongot.embedding.config.MaterializedViewCollectionMetadataCatalog;
import com.xgen.mongot.embedding.exceptions.MaterializedViewNonTransientException;
import com.xgen.mongot.embedding.exceptions.MaterializedViewTransientException;
import com.xgen.mongot.embedding.mongodb.common.AutoEmbeddingMongoClient;
import com.xgen.mongot.embedding.mongodb.common.InternalDatabaseResolver;
import com.xgen.mongot.embedding.utils.MongoClientOperationExecutor;
import com.xgen.mongot.index.EncodedUserData;
import com.xgen.mongot.index.IndexGeneration;
import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.definition.MaterializedViewIndexDefinitionGeneration;
import com.xgen.mongot.index.status.IndexStatus;
import com.xgen.mongot.index.version.MaterializedViewGenerationId;
import com.xgen.mongot.metrics.MeterAndFtdcRegistry;
import com.xgen.mongot.metrics.MetricsFactory;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.mongodb.serialization.MongoDbCollectionInfo;
import io.micrometer.core.instrument.Counter;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.bson.BsonDateTime;
import org.bson.BsonDocument;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.jetbrains.annotations.TestOnly;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.spi.LoggingEventBuilder;

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

  private static final String METRICS_NAMESPACE = "embedding.leasing.stats";

  @VisibleForTesting static final String LEASE_COLLECTION_NAME = "auto_embedding_leases";

  private static final long GIVE_UP_BLACKOUT_SECONDS = 60;

  static final String OPS_GIVE_UP_LEASE_SUCCESS_COUNTER_NAME = "opsGiveUpLeaseSuccess";
  static final String OPS_GIVE_UP_LEASE_FAILURE_COUNTER_NAME = "opsGiveUpLeaseFailure";
  static final String POST_RESTORE_LEASE_REPAIR_SUCCESS_COUNTER_NAME =
      "postRestoreLeaseRepairSuccess";
  static final String POST_RESTORE_LEASE_REPAIR_FAILURE_COUNTER_NAME =
      "postRestoreLeaseRepairFailure";
  static final String CIRCUIT_BREAKER_OPENED_COUNTER_NAME = "circuitBreakerOpened";
  static final String ACQUISITION_SUPPRESSED_COUNTER_NAME = "acquisitionSuppressed";
  static final String CORRUPTED_LEASES_COUNTER_NAME = "corruptedLeases";
  static final String LEASE_STORE_STALLED_GAUGE_NAME = "leaseStoreStalled";
  static final String CHECKPOINT_REJECTED_COUNTER_NAME = "checkpointsRejectedNotOwner";
  static final String SKIPPED_CHECKPOINT_COUNTER_NAME = "checkpointsSkippedNotLeader";

  // Fixed like GIVE_UP_BLACKOUT_SECONDS and the client retry policy: an internal, fail-safe
  // resilience timing, not an ops knob. Cooldown base = refresh interval, cap = lease TTL.
  @VisibleForTesting static final int CIRCUIT_BREAKER_FAILURE_THRESHOLD = 3;
  private static final Duration CIRCUIT_BREAKER_BASE_COOLDOWN = Duration.ofSeconds(30);
  private static final Duration CIRCUIT_BREAKER_MAX_COOLDOWN = Duration.ofMinutes(5);

  private final MongoClientOperationExecutor operationExecutor;
  private final String hostname;
  // Mapping of lease keys to leases.
  private final Map<String, Lease> leases;
  // Tracks GenerationIds where this instance is the leader.
  private final Set<MaterializedViewGenerationId> leaderGenerationIds;
  // Tracks GenerationIds where this instance is a follower.
  private final Set<MaterializedViewGenerationId> followerGenerationIds;
  // Maps GenerationId to its definition version (as String) for use in pollFollowerStatuses().
  private final Map<MaterializedViewGenerationId, String> matViewGenerationIdToDefinitionVersion;
  private final MaterializedViewCollectionMetadataCatalog mvMetadataCatalog;
  private final InternalDatabaseResolver dbResolver;
  private final Map<String, String> leaseKeyToDatabase;
  private final AutoEmbeddingMongoClient autoEmbeddingMongoClient;
  // End of give-up blackout (epoch ms). While now < this, do not acquire leadership.
  private final AtomicLong giveUpBlackoutEndTimeMs = new AtomicLong(0);
  private final Counter postRestoreLeaseRepairSuccessCounter;
  private final Counter postRestoreLeaseRepairFailureCounter;
  private final boolean isDedicatedCluster;
  private final LeaseRepairUtil.CollectionUuidResolver uuidResolver;
  private final MetricsFactory metricsFactory;
  // Ops commands. Populated at construction but not executed until the source mongod is ready.
  private final LeaseManagerOpsCommands opsCommands;
  // Backs off acquisition writes during a customer commit-point stall; see
  // LeaseStoreCircuitBreaker.
  private final LeaseStoreCircuitBreaker leaseStoreCircuitBreaker;
  private final Counter acquisitionSuppressedCounter;
  private final Counter checkpointRejectedCounter;
  private final Counter skippedCheckpointCounter;

  @TestOnly
  public DynamicLeaderLeaseManager(
      AutoEmbeddingMongoClient autoEmbeddingMongoClient,
      MetricsFactory metricsFactory,
      String hostname,
      InternalDatabaseResolver dbResolver,
      MaterializedViewCollectionMetadataCatalog mvMetadataCatalog) {
    this(
        autoEmbeddingMongoClient,
        metricsFactory,
        hostname,
        dbResolver,
        mvMetadataCatalog,
        LeaseManagerOpsCommands.NONE);
  }

  private DynamicLeaderLeaseManager(
      AutoEmbeddingMongoClient autoEmbeddingMongoClient,
      MetricsFactory metricsFactory,
      String hostname,
      InternalDatabaseResolver dbResolver,
      MaterializedViewCollectionMetadataCatalog mvMetadataCatalog,
      LeaseManagerOpsCommands opsCommands) {
    this.metricsFactory = metricsFactory;
    this.operationExecutor =
        new MongoClientOperationExecutor(metricsFactory, "leaseTableCollection");
    this.postRestoreLeaseRepairSuccessCounter =
        metricsFactory.counter(POST_RESTORE_LEASE_REPAIR_SUCCESS_COUNTER_NAME);
    this.postRestoreLeaseRepairFailureCounter =
        metricsFactory.counter(POST_RESTORE_LEASE_REPAIR_FAILURE_COUNTER_NAME);
    this.hostname = hostname;
    this.mvMetadataCatalog = mvMetadataCatalog;
    this.leases = new ConcurrentHashMap<>();
    this.leaderGenerationIds = ConcurrentHashMap.newKeySet();
    this.followerGenerationIds = ConcurrentHashMap.newKeySet();
    this.matViewGenerationIdToDefinitionVersion = new ConcurrentHashMap<>();
    this.dbResolver = dbResolver;
    this.leaseKeyToDatabase = new ConcurrentHashMap<>();
    this.autoEmbeddingMongoClient = autoEmbeddingMongoClient;
    this.opsCommands = opsCommands;
    this.isDedicatedCluster = dbResolver.isDedicatedCluster();
    this.uuidResolver = buildUuidResolver();
    this.acquisitionSuppressedCounter =
        this.metricsFactory.counter(ACQUISITION_SUPPRESSED_COUNTER_NAME);
    this.checkpointRejectedCounter = this.metricsFactory.counter(CHECKPOINT_REJECTED_COUNTER_NAME);
    this.skippedCheckpointCounter = this.metricsFactory.counter(SKIPPED_CHECKPOINT_COUNTER_NAME);
    this.leaseStoreCircuitBreaker =
        new LeaseStoreCircuitBreaker(
            CIRCUIT_BREAKER_FAILURE_THRESHOLD,
            CIRCUIT_BREAKER_BASE_COOLDOWN,
            CIRCUIT_BREAKER_MAX_COOLDOWN,
            this.metricsFactory.counter(CIRCUIT_BREAKER_OPENED_COUNTER_NAME),
            this.metricsFactory.numGauge(LEASE_STORE_STALLED_GAUGE_NAME));
  }

  public static DynamicLeaderLeaseManager create(
      AutoEmbeddingMongoClient autoEmbeddingMongoClient,
      MeterAndFtdcRegistry meterAndFtdcRegistry,
      String hostname,
      InternalDatabaseResolver dbResolver,
      MaterializedViewCollectionMetadataCatalog mvMetadataCatalog,
      LeaseManagerOpsCommands opsCommands) {
    return new DynamicLeaderLeaseManager(
        autoEmbeddingMongoClient,
        new MetricsFactory(METRICS_NAMESPACE, meterAndFtdcRegistry.meterRegistry()),
        hostname,
        dbResolver,
        mvMetadataCatalog,
        opsCommands);
  }

  @Override
  public void executeOpsCommandsAfterInitializeLease(String leaseKey) {
    if (this.opsCommands.opsGiveUpLease().isEmpty()) {
      return;
    }
    LeaseManagerOpsCommands.OpsGiveUpLeaseCommand cmd = this.opsCommands.opsGiveUpLease().get();
    if (!isValidGiveUpLeaseCommand(cmd, leaseKey)) {
      return;
    }
    // ops command is valid. Enter execution phase.
    if (updateLeaseToGiveUpLeadership(leaseKey)) {
      long blackoutEnd = System.currentTimeMillis() + GIVE_UP_BLACKOUT_SECONDS * 1000L;
      this.giveUpBlackoutEndTimeMs.set(blackoutEnd);
      LOG.atInfo()
          .addKeyValue("leaseKey", leaseKey)
          .addKeyValue("blackoutSeconds", GIVE_UP_BLACKOUT_SECONDS)
          .addKeyValue("blackoutEnd", Instant.ofEpochMilli(blackoutEnd))
          .log("Applied lease give-up & blackout ops command");
    }
  }

  private boolean isValidGiveUpLeaseCommand(
      LeaseManagerOpsCommands.OpsGiveUpLeaseCommand cmd, String leaseKey) {
    if (!cmd.instance().equals(this.hostname)) {
      LOG.atDebug()
          .addKeyValue("leaseKey", leaseKey)
          .addKeyValue("commandInstance", cmd.instance())
          .addKeyValue("hostname", this.hostname)
          .log("Skipping give-up lease ops command - instance mismatch");
      return false;
    }
    if (Instant.now().isAfter(cmd.expiresAt())) {
      LOG.atDebug()
          .addKeyValue("leaseKey", leaseKey)
          .addKeyValue("expiresAt", cmd.expiresAt())
          .log("Skipping give-up lease ops command - command expired");
      return false;
    }
    if (cmd.leaseNames().isEmpty() || !cmd.leaseNames().contains(leaseKey)) {
      LOG.atDebug()
          .addKeyValue("leaseKey", leaseKey)
          .log("Skipping give-up lease ops command - lease not targeted");
      return false;
    }
    Lease lease = this.leases.get(leaseKey);
    if (lease == null) {
      LOG.atDebug()
          .addKeyValue("leaseKey", leaseKey)
          .log("Skipping give-up lease ops command - lease not found");
      return false;
    }
    if (!this.hostname.equals(lease.leaseOwner())) {
      LOG.atDebug()
          .addKeyValue("leaseKey", leaseKey)
          .addKeyValue("hostname", lease.leaseOwner())
          .addKeyValue("realHostname", this.hostname)
          .log("Skipping give-up lease ops command - lease not owned");
      return false;
    }
    return true;
  }

  /**
   * Update the lease document to officially give up ownership. This method expires the lease in the
   * database and clears local leader state.
   *
   * @return true if give-up is successful, false otherwise
   */
  private boolean updateLeaseToGiveUpLeadership(String leaseKeyToGiveUp) {
    Lease lease = this.leases.get(leaseKeyToGiveUp);
    if (lease == null) {
      LOG.atError()
          .addKeyValue("leaseKeyToGiveUp", leaseKeyToGiveUp)
          .log("Lease not found, skipping give-up. Please try later.");
      return false;
    }
    Lease released = lease.withReleasedOwnership();
    Bson filter =
        createUpdateFilterForOwnedLease(
            leaseKeyToGiveUp, lease.leaseVersion(), released.leaseVersion());
    UpdateResult result;
    try {
      result =
          this.operationExecutor.execute(
              "giveUpLease",
              () ->
                  this.getCollection(getDatabaseForLease(leaseKeyToGiveUp))
                      .replaceOne(filter, released.toBson()));
    } catch (Exception e) {
      this.metricsFactory.counter(OPS_GIVE_UP_LEASE_FAILURE_COUNTER_NAME).increment();
      LOG.atError()
          .addKeyValue("leaseKeyToGiveUp", leaseKeyToGiveUp)
          .addKeyValue("hostname", this.hostname)
          .setCause(e)
          .log("Failed to execute lease update command for give-up");
      return false;
    }
    if (result.getModifiedCount() != 1) {
      this.metricsFactory.counter(OPS_GIVE_UP_LEASE_FAILURE_COUNTER_NAME).increment();
      LOG.atInfo()
          .addKeyValue("leaseKeyToGiveUp", leaseKeyToGiveUp)
          .addKeyValue("hostname", this.hostname)
          .addKeyValue("matchedCount", result.getMatchedCount())
          .addKeyValue("modifiedCount", result.getModifiedCount())
          .log(
              "Lease give-up did not modify the document (filter/version mismatch or no longer"
                  + " owner); skipping local update and blackout");
      return false;
    }
    this.leases.put(leaseKeyToGiveUp, released);
    this.metricsFactory.counter(OPS_GIVE_UP_LEASE_SUCCESS_COUNTER_NAME).increment();
    LOG.atDebug()
        .addKeyValue("leaseKeyToGiveUp", leaseKeyToGiveUp)
        .addKeyValue("hostname", this.hostname)
        .log("Gave up lease successfully for rebalance");
    return true;
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
  @VisibleForTesting
  public void syncLeasesFromMongod() {
    try {
      String defaultDb = this.dbResolver.resolveDefault();
      List<BsonDocument> rawLeases =
          this.operationExecutor.execute(
              "getLeases", () -> this.getCollection(defaultDb).find().into(new ArrayList<>()));
      List<String> corruptedLeaseIds = new ArrayList<>();
      for (BsonDocument rawLease : rawLeases) {
        try {
          // Populate the database mapping before normalization so that getDatabaseForLease()
          // resolves correctly when normalizeLeaseIfNeeded() calls back into it for UUID
          // resolution.
          Lease parsed = parseLeaseOrThrow(rawLease);
          this.leaseKeyToDatabase.putIfAbsent(parsed.id(), defaultDb);
          Lease lease = normalizeLeaseIfNeeded(parsed);
          if (lease != null) {
            this.leases.put(lease.id(), lease);
          } else {
            // TODO(CLOUDP-384971): clean up corrupted leases
            this.metricsFactory.counter(CORRUPTED_LEASES_COUNTER_NAME).increment();
            corruptedLeaseIds.add(parsed.id());
          }
        } catch (BsonParseException e) {
          // parseLeaseOrThrow already incremented the corruptedLeases counter
          corruptedLeaseIds.add(extractLeaseId(rawLease));
        }
      }
      if (!corruptedLeaseIds.isEmpty()) {
        LOG.atError()
            .addKeyValue("corruptedLeaseIds", corruptedLeaseIds)
            .addKeyValue("count", corruptedLeaseIds.size())
            .log("Corrupted leases found during sync, skipping");
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

  /**
   * Wraps {@link Lease#fromBson} and increments the corrupted lease counter on parse failure.
   * Callers can catch the re-thrown exception to decide whether to skip or propagate.
   */
  private Lease parseLeaseOrThrow(BsonDocument rawLease) throws BsonParseException {
    try {
      return Lease.fromBson(rawLease);
    } catch (BsonParseException e) {
      this.metricsFactory.counter(CORRUPTED_LEASES_COUNTER_NAME).increment();
      throw e;
    }
  }

  private static String extractLeaseId(BsonDocument rawLease) {
    try {
      return rawLease.containsKey("_id") ? rawLease.get("_id").asString().getValue() : "unknown";
    } catch (Exception e) {
      return "unparseable";
    }
  }

  // Normalizes lease by populating missing mat view UUID field, this won't change lease
  // version since it's only meant for backward compatibility and lease state is unchanged.
  @Nullable
  private Lease normalizeLeaseIfNeeded(Lease lease) {
    @Var Lease normalized = lease;
    if (lease.materializedViewCollectionMetadata().collectionUuid().equals(NIL)) {
      try {
        normalized =
            lease.withResolvedMatViewUuid(
                Check.instanceOf(
                        getCollectionInfo(
                            Check.isPresent(
                                this.autoEmbeddingMongoClient.getLeaseManagerMongoClient(),
                                "leaseManagerMongoClient"),
                            getDatabaseForLease(lease.id()),
                            lease.materializedViewCollectionMetadata().collectionName()),
                        MongoDbCollectionInfo.Collection.class)
                    .info()
                    .uuid());
      } catch (Exception | AssertionError e) {
        LOG.atWarn()
            .addKeyValue("leaseId", lease.id())
            .addKeyValue("leaseOwner", lease.leaseOwner())
            .addKeyValue(
                "matViewCollectionName",
                lease.materializedViewCollectionMetadata().collectionName())
            .setCause(e)
            .log(
                "Unable to normalize or validate lease, "
                    + "could be caused by dangling lease or corrupted lease");
        // TODO(CLOUDP-384971): We should have a way to clean up corrupted leases to avoid blocking
        // Lease creation.
        return null;
      }
    }
    return postRestoreSanitizeIfNeeded(normalized);
  }

  // Sanitizes a stale flex→dedicated migration lease in-memory so downstream consumers see
  // cleared commitInfo + refreshed UUIDs before any replication starts. The persistent rewrite
  // happens naturally via the next leadership-acquisition replaceOne. On repair failure the lease
  // is dropped (null return) so downstream code doesn't start replication against a stale
  // namespace; repair is retried on the next lease load.
  @Nullable
  private Lease postRestoreSanitizeIfNeeded(Lease lease) {
    if (!LeaseRepairUtil.requiresPostRestoreRepair(lease, this.isDedicatedCluster)) {
      return lease;
    }
    try {
      Lease repaired =
          LeaseRepairUtil.repairLeaseForPostRestoreMigration(
              lease, this.uuidResolver, this.dbResolver.resolveDefault());
      if (repaired == lease) {
        // Customer database literally named with a 24-hex-char prefix — prefix regex matched
        // but UUIDs are already current, so there's nothing to repair.
        return lease;
      }
      this.postRestoreLeaseRepairSuccessCounter.increment();
      logRepair(lease)
          .addKeyValue("oldSourceUuid", lease.collectionUuid())
          .addKeyValue("newSourceUuid", repaired.collectionUuid())
          .addKeyValue("oldMvUuid", lease.materializedViewCollectionMetadata().collectionUuid())
          .addKeyValue("newMvUuid", repaired.materializedViewCollectionMetadata().collectionUuid())
          .log("Repaired post-restore stale lease (flex→dedicated migration artifact)");
      return repaired;
    } catch (LeaseRepairUtil.LeaseRepairException e) {
      this.postRestoreLeaseRepairFailureCounter.increment();
      logRepair(lease)
          .setCause(e)
          .log(
              "Post-restore lease repair failed at load; dropping lease to prevent replication on"
                  + " stale namespace");
      return null;
    }
  }

  private LoggingEventBuilder logRepair(Lease lease) {
    return LOG.atWarn()
        .addKeyValue("leaseId", lease.id())
        .addKeyValue("hostname", this.hostname)
        .addKeyValue("oldNamespace", LeaseRepairUtil.extractNamespace(lease).orElse(null));
  }

  // Rewrites the in-memory lease's MV UUID to the fresh one from proposedMetadata when the two
  // disagree — signals a flex→dedicated migration where the lease had no commitInfo namespace to
  // prefix-check (still in INITIAL_SYNC). No listCollections needed; the fresh UUID is supplied
  // by the caller.
  private Lease repairStaleMvUuidIfNeeded(Lease existing, UUID freshMvUuid) {
    if (!this.isDedicatedCluster) {
      return existing;
    }
    UUID stored = existing.materializedViewCollectionMetadata().collectionUuid();
    if (freshMvUuid.equals(stored)) {
      return existing;
    }
    Lease repaired = LeaseRepairUtil.repairMvUuidOnly(existing, freshMvUuid);
    this.leases.put(repaired.id(), repaired);
    this.postRestoreLeaseRepairSuccessCounter.increment();
    logRepair(existing)
        .addKeyValue("oldMvUuid", stored)
        .addKeyValue("newMvUuid", freshMvUuid)
        .log("Repaired post-restore stale MV UUID (empty-commitInfo migration case)");
    return repaired;
  }

  /**
   * Adds a new index generation to be managed by this lease manager.
   *
   * <p>If we already own an unexpired lease for this index (e.g., during index definition update),
   * the generation is added as a leader. Otherwise, it starts as a follower and leadership is
   * acquired via {@link #tryAcquireLeadership(MaterializedViewGenerationId)}.
   *
   * <p>If no lease exists in memory, an in-memory lease with empty owner is created. This lease
   * will be persisted to the database when {@link
   * #tryAcquireLeadership(MaterializedViewGenerationId)} is called.
   */
  @Override
  public void add(IndexGeneration indexGeneration, boolean skipInitialSync) {
    MaterializedViewGenerationId generationId =
        Check.instanceOf(indexGeneration.getGenerationId(), MaterializedViewGenerationId.class);
    String versionKey = getIndexDefinitionVersion(indexGeneration.getDefinition());
    this.matViewGenerationIdToDefinitionVersion.put(generationId, versionKey);
    String leaseKey = getLeaseKey(generationId);
    this.leaseKeyToDatabase.put(
        leaseKey, this.dbResolver.resolve(indexGeneration.getDefinition().getDatabase()));

    var lease = this.leases.get(leaseKey);
    if (lease != null) {
      // Lease exists in memory - only retain leadership while our ownership is still valid.
      boolean weOwnLease = this.hostname.equals(lease.leaseOwner());
      boolean leaseExpired = Instant.now().isAfter(lease.leaseExpiration());

      if (weOwnLease && !leaseExpired) {
        // We own the lease - this is likely an index definition update (e.g., filter field change).
        // Add to leaderGenerationIds so the generator can become leader immediately. Also clear any
        // followerGenerationIds membership: becomeFollower() can have added this generationId there
        // moments earlier in the same refreshStatus() tick (e.g. a terminal-generator demotion
        // immediately followed by retryFailedGeneratorCreations() re-adding it here), and leaving
        // it
        // in both sets makes the next pollFollowerStatuses() report this already-active lease as
        // acquirable, causing refreshStatus()'s acquirableLeases loop to redundantly reacquire
        // leadership on the generator this call just activated and trip the unsafe-reacquisition
        // detector on a same-tenure, non-corrupting reacquisition (CLOUDP-437181).
        this.followerGenerationIds.remove(generationId);
        this.leaderGenerationIds.add(generationId);
        LOG.atInfo()
            .addKeyValue("generationId", generationId)
            .addKeyValue("leaseOwner", lease.leaseOwner())
            .addKeyValue("leaseExpiration", lease.leaseExpiration())
            .addKeyValue("hostname", this.hostname)
            .log("Starting as leader - we own the existing lease");
      } else {
        // add() runs again after replication restart, while the previous leader tracking remains.
        // Clear it so expired ownership must be reacquired through OCC before the generator writes.
        this.leaderGenerationIds.remove(generationId);
        this.followerGenerationIds.add(generationId);
        LOG.atInfo()
            .addKeyValue("generationId", generationId)
            .addKeyValue("leaseOwner", lease.leaseOwner())
            .addKeyValue("leaseExpiration", lease.leaseExpiration())
            .addKeyValue("hostname", this.hostname)
            .addKeyValue("weOwnLease", weOwnLease)
            .addKeyValue("leaseExpired", leaseExpired)
            .log("Starting as follower - existing lease requires acquisition");
      }

      // Update the lease with the new index definition version if needed.
      if (!lease.indexDefinitionVersionStatusMap().containsKey(versionKey)) {
        this.leases.put(
            leaseKey,
            lease.withNewIndexDefinitionVersion(
                versionKey, indexGeneration.getIndex().getStatus(), skipInitialSync));
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
      this.leases.put(leaseKey, newLease);
    }
  }

  @Override
  public void drop(MaterializedViewGenerationId generationId) {
    this.leaderGenerationIds.remove(generationId);
    this.followerGenerationIds.remove(generationId);
    this.matViewGenerationIdToDefinitionVersion.remove(generationId);
  }

  @Override
  public CompletableFuture<Void> dropLease(String leaseKey) {
    String dbName;
    try {
      dbName = getDatabaseForLease(leaseKey);
    } catch (IllegalStateException e) {
      LOG.warn(
          "No database mapping for lease key {} during drop, skipping DB cleanup. "
              + "Lease document may remain in database.",
          leaseKey,
          e);
      evictCachedLease(leaseKey);
      return COMPLETED_FUTURE;
    }
    // Only delete the lease from the database if we own the lease.
    // Enforce ownership check in the filter to handle stale in-memory state.
    Lease lease = this.leases.get(leaseKey);
    if (lease != null && this.hostname.equals(lease.leaseOwner())) {
      LOG.atInfo()
          .addKeyValue("leaseKey", leaseKey)
          .addKeyValue("hostname", this.hostname)
          .log("Dropping index - deleting lease from database (we own it)");
      // Remove the lease from memory first, then delete it from DB.
      // This ensures we stop considering ourselves the leader immediately.
      evictCachedLease(leaseKey);
      return CompletableFuture.runAsync(
          () -> {
            var filter =
                Filters.and(
                    Filters.eq("_id", leaseKey),
                    Filters.eq(Lease.Fields.LEASE_OWNER.getName(), this.hostname));
            try {
              // Routed through the operationExecutor to stay consistent with every other
              // lease-table op; it retries transient failures and emits
              // successfulRequests/failedRequests tagged operation="dropLease".
              var deleteResult =
                  this.operationExecutor.execute(
                      "dropLease", () -> this.getCollection(dbName).deleteOne(filter));
              if (deleteResult.getDeletedCount() > 0) {
                LOG.atInfo()
                    .addKeyValue("leaseKey", leaseKey)
                    .log("Successfully deleted lease from database");
              } else {
                // This is expected if another instance took over before we deleted.
                LOG.atInfo()
                    .addKeyValue("leaseKey", leaseKey)
                    .log("Lease not deleted - ownership changed or lease already removed");
              }
            } catch (Exception e) {
              // Best effort cleanup. The lease will eventually expire if we can't delete it.
              LOG.warn(
                  "Failed to delete lease for {} from database. "
                      + "Lease will expire naturally if not deleted.",
                  leaseKey,
                  e);
            }
          });
    }

    // Follower path or no lease found - just clean up the in-memory state.
    if (lease != null) {
      LOG.atInfo()
          .addKeyValue("leaseKey", leaseKey)
          .addKeyValue("hostname", this.hostname)
          .addKeyValue("leaseOwner", lease.leaseOwner())
          .log("Dropping index - not deleting lease from database (we don't own it)");
    } else {
      LOG.atInfo()
          .addKeyValue("leaseKey", leaseKey)
          .addKeyValue("hostname", this.hostname)
          .log("Dropping index - no lease found in memory");
    }
    evictCachedLease(leaseKey);
    return COMPLETED_FUTURE;
  }

  @Override
  public void evictCachedLease(String leaseKey) {
    this.leases.remove(leaseKey);
    this.leaseKeyToDatabase.remove(leaseKey);
  }

  @Override
  public boolean isLeader(MaterializedViewGenerationId generationId) {
    return this.leaderGenerationIds.contains(generationId);
  }

  @Override
  public void releaseLeadership(MaterializedViewGenerationId generationId) {
    becomeFollower(generationId);
  }

  /**
   * The cache can hold a lease owned by another host — {@code refreshLeaseFromDatabase} installs
   * whatever the store returns on the leadership-loss paths, and {@code becomeFollower} leaves the
   * entry in place as its follower-transition signal — so ownership is checked here against the
   * same single read the version comes from (CLOUDP-429754).
   */
  @Override
  public long getOwnedLeaseVersion(MaterializedViewGenerationId generationId) {
    Lease lease = this.leases.get(getLeaseKey(generationId));
    if (lease == null) {
      return INVALID_LEASE_VERSION;
    }
    if (!this.hostname.equals(lease.leaseOwner())) {
      LOG.atWarn()
          .addKeyValue("generationId", generationId)
          .addKeyValue("leaseOwner", lease.leaseOwner())
          .addKeyValue("hostname", this.hostname)
          .addKeyValue("leaseVersion", lease.leaseVersion())
          .log("Refusing to hand out a fencing token for a lease owned by another host");
      return INVALID_LEASE_VERSION;
    }
    return lease.leaseVersion();
  }

  @Override
  public EncodedUserData getCommitInfo(MaterializedViewGenerationId generationId)
      throws IOException {
    // Leader can read from the in-memory state.
    if (isLeader(generationId)) {
      @Var String leaseKey = null;
      try {
        leaseKey = getLeaseKey(generationId);
        ensureLeaseExists(leaseKey);
        return EncodedUserData.fromString(this.leases.get(leaseKey).commitInfo());
      } catch (IllegalStateException e) {
        LOG.warn(
            "Metadata removed for generation {} during getCommitInfo, removing from leaders",
            generationId,
            e);
        cleanupAfterMetadataLoss(generationId, leaseKey);
        return EncodedUserData.EMPTY;
      }
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
  public void updateCommitInfo(
      MaterializedViewGenerationId generationId, EncodedUserData encodedUserData)
      throws MaterializedViewTransientException, MaterializedViewNonTransientException {
    // Only update the commit info in the database if leader.
    if (isLeader(generationId)) {
      @Var String leaseKey = null;
      try {
        leaseKey = getLeaseKey(generationId);
        ensureLeaseExists(leaseKey);
        Lease currentLease = this.leases.get(leaseKey);
        Lease updatedLease = currentLease.withUpdatedCheckpoint(encodedUserData);
        if (!updateLeaseInDatabase(
            leaseKey, generationId, currentLease, updatedLease, encodedUserData)) {
          // commit() has already run its bulkWrite, so MV documents may exist with no checkpoint.
          this.checkpointRejectedCounter.increment();
          LOG.atWarn()
              .addKeyValue("generationId", generationId)
              .addKeyValue("leaseKey", leaseKey)
              .addKeyValue("hostname", this.hostname)
              .addKeyValue("expectedLeaseVersion", currentLease.leaseVersion())
              .log("Checkpoint commit rejected, this host no longer owns the lease");
        }
      } catch (IllegalStateException e) {
        LOG.warn(
            "Metadata removed for generation {} during updateCommitInfo, removing from leaders",
            generationId,
            e);
        cleanupAfterMetadataLoss(generationId, leaseKey);
      }
    } else {
      // commit() has already run its bulkWrite, so MV documents may exist with a stale fencing
      // token and no checkpoint.
      this.skippedCheckpointCounter.increment();
      LOG.atWarn()
          .addKeyValue("generationId", generationId)
          .addKeyValue("hostname", this.hostname)
          .log("Skipped checkpoint commit, no longer leader for this generation");
    }
  }

  @Override
  public void updateReplicationStatus(
      MaterializedViewGenerationId generationId,
      long indexDefinitionVersion,
      IndexStatus indexStatus)
      throws MaterializedViewTransientException, MaterializedViewNonTransientException {
    // Only update the status in the database of leader.
    if (isLeader(generationId)) {
      @Var String leaseKey = null;
      try {
        leaseKey = getLeaseKey(generationId);
        ensureLeaseExists(leaseKey);
        Lease currentLease = this.leases.get(leaseKey);

        BsonTimestamp oplogPosition = currentLease.extractHighWaterMark().orElse(null);
        Lease updatedLease =
            currentLease.withUpdatedStatus(indexStatus, indexDefinitionVersion, oplogPosition);
        updateLeaseInDatabase(
            leaseKey,
            generationId,
            currentLease,
            updatedLease,
            EncodedUserData.fromString(currentLease.commitInfo()));
      } catch (IllegalStateException e) {
        LOG.warn(
            "Metadata removed for generation {} during updateReplicationStatus, "
                + "removing from leaders",
            generationId,
            e);
        cleanupAfterMetadataLoss(generationId, leaseKey);
      }
    }
  }

  /**
   * The scanned set is the default internal database plus the per-tenant internal databases learned
   * from configured generations (on multi-tenant clusters each source database resolves to its own
   * internal database). Tenant databases hosting no auto-embedding generation are never queried —
   * the set is learned, not enumerated from the cluster — so sweep cost scales with auto-embedding
   * adoption, not with the cluster's tenant count.
   */
  @Override
  public List<Lease> findGcCandidates() {
    Set<String> databases = new HashSet<>(this.leaseKeyToDatabase.values());
    databases.add(this.dbResolver.resolveDefault());

    List<Lease> leases = new ArrayList<>();
    for (String dbName : databases) {
      try {
        List<BsonDocument> rawLeases =
            this.operationExecutor.execute(
                "findGcCandidates",
                () -> this.getCollection(dbName).find().into(new ArrayList<>()));
        for (BsonDocument rawLease : rawLeases) {
          try {
            Lease lease = parseLeaseOrThrow(rawLease);
            // Register the mapping (orphans have no generation to do it) for the mark/delete.
            this.leaseKeyToDatabase.putIfAbsent(lease.id(), dbName);
            leases.add(lease);
          } catch (BsonParseException e) {
            // parseLeaseOrThrow counted it; corrupted leases are invisible to GC (CLOUDP-384971).
            LOG.atError()
                .addKeyValue("leaseId", extractLeaseId(rawLease))
                .setCause(e)
                .log("Failed to parse lease document during GC lease scan, skipping");
          }
        }
      } catch (Exception e) {
        // Drops this database's leases from the result — safe per the interface contract; the
        // periodic sweep picks them up on a later tick.
        LOG.atWarn()
            .addKeyValue("database", dbName)
            .setCause(e)
            .log("Failed to scan database for GC leases");
      }
    }
    return leases;
  }

  @Override
  public boolean markEligibleForCleanup(String leaseKey, Instant expirationCutoff) {
    String dbName;
    try {
      dbName = getDatabaseForLease(leaseKey);
    } catch (IllegalStateException e) {
      LOG.atWarn()
          .addKeyValue("leaseKey", leaseKey)
          .setCause(e)
          .log("No database mapping for lease during cleanup mark, skipping");
      return false;
    }
    // Self-guarding OCC: only mark if the lease is still NOT_ELIGIBLE (or a legacy lease with no
    // cleanupState field) AND its persisted expiration is past the cutoff. The expiration
    // predicate lives in the filter, so a heartbeat landing between our read and this write makes
    // it a no-op rather than a wrong mark.
    String stateField = Lease.Fields.CLEANUP_STATE.getName();
    var filter =
        Filters.and(
            Filters.eq("_id", leaseKey),
            Filters.or(
                Filters.eq(stateField, Lease.CleanupState.NOT_ELIGIBLE.name()),
                Filters.exists(stateField, false)),
            Filters.lt(
                Lease.Fields.LEASE_EXPIRATION.getName(),
                new BsonDateTime(expirationCutoff.toEpochMilli())));
    var update = Updates.set(stateField, Lease.CleanupState.ELIGIBLE_FOR_CLEANUP.name());
    try {
      var result =
          this.operationExecutor.execute(
              "markEligibleForCleanup", () -> this.getCollection(dbName).updateOne(filter, update));
      if (result.getModifiedCount() > 0) {
        this.leases.computeIfPresent(
            leaseKey,
            (k, lease) -> lease.withCleanupState(Lease.CleanupState.ELIGIBLE_FOR_CLEANUP));
        return true;
      }
      return false;
    } catch (Exception e) {
      // Best effort: a failed mark just means the scanner retries on a later tick.
      LOG.atWarn()
          .addKeyValue("leaseKey", leaseKey)
          .setCause(e)
          .log("Failed to mark lease eligible for cleanup");
      return false;
    }
  }

  @Override
  public Optional<MongoNamespace> resolveStaleMatViewNamespace(
      String leaseKey, long expectedLeaseVersion, Instant expirationCutoff) {
    String dbName;
    try {
      dbName = getDatabaseForLease(leaseKey);
    } catch (IllegalStateException e) {
      LOG.atWarn()
          .addKeyValue("leaseKey", leaseKey)
          .setCause(e)
          .log("No database mapping for lease during stale mat view resolve, skipping");
      return Optional.empty();
    }
    try {
      return readLeaseIfStillEligible(dbName, leaseKey, expectedLeaseVersion, expirationCutoff)
          .flatMap(lease -> LeaseManager.matViewNamespaceOf(dbName, lease));
    } catch (Exception e) {
      // Re-read failed; the next sweep re-evaluates.
      LOG.atWarn()
          .addKeyValue("leaseKey", leaseKey)
          .setCause(e)
          .log("Failed to resolve stale mat view namespace, skipping");
      return Optional.empty();
    }
  }

  /**
   * Re-reads {@code leaseKey} and returns it only if still eligible, stale past {@code
   * expirationCutoff}, and at {@code expectedLeaseVersion} — the fresh read narrows the race with a
   * lease reused after the sweep's snapshot, since a collection drop cannot be made conditional.
   */
  private Optional<Lease> readLeaseIfStillEligible(
      String dbName, String leaseKey, long expectedLeaseVersion, Instant expirationCutoff)
      throws Exception {
    BsonDocument rawLease =
        this.operationExecutor.execute(
            "readStaleLease",
            () -> this.getCollection(dbName).find(Filters.eq("_id", leaseKey)).first());
    if (rawLease == null) {
      LOG.atInfo()
          .addKeyValue("leaseKey", leaseKey)
          .log("Stale lease already deleted, skipping mat view drop");
      return Optional.empty();
    }
    Lease lease = parseLeaseOrThrow(rawLease);
    if (lease.cleanupState() != Lease.CleanupState.ELIGIBLE_FOR_CLEANUP
        || !lease.leaseExpiration().isBefore(expirationCutoff)
        || lease.leaseVersion() != expectedLeaseVersion) {
      LOG.atInfo()
          .addKeyValue("leaseKey", leaseKey)
          .addKeyValue("cleanupState", lease.cleanupState())
          .addKeyValue("leaseExpiration", lease.leaseExpiration())
          .addKeyValue("leaseVersion", lease.leaseVersion())
          .addKeyValue("expectedLeaseVersion", expectedLeaseVersion)
          .log("Lease no longer eligible for cleanup at drop time, skipping");
      return Optional.empty();
    }
    return Optional.of(lease);
  }

  @Override
  public boolean deleteStaleLease(
      String leaseKey, long expectedLeaseVersion, Instant expirationCutoff) {
    String dbName;
    try {
      dbName = getDatabaseForLease(leaseKey);
    } catch (IllegalStateException e) {
      LOG.atWarn()
          .addKeyValue("leaseKey", leaseKey)
          .setCause(e)
          .log("No database mapping for lease during stale lease delete, skipping");
      return false;
    }
    // OCC delete: commits only while the lease is still marked, still stale, AND still at the
    // version the sweep saw. leaseVersion is the fencing token — any reuse bumps it (and resets
    // cleanupState / extends expiration), so each predicate independently vetoes the delete.
    var filter =
        Filters.and(
            Filters.eq("_id", leaseKey),
            Filters.eq(
                Lease.Fields.CLEANUP_STATE.getName(),
                Lease.CleanupState.ELIGIBLE_FOR_CLEANUP.name()),
            Filters.eq(Lease.Fields.LEASE_VERSION.getName(), expectedLeaseVersion),
            Filters.lt(
                Lease.Fields.LEASE_EXPIRATION.getName(),
                new BsonDateTime(expirationCutoff.toEpochMilli())));
    try {
      var result =
          this.operationExecutor.execute(
              "deleteStaleLease", () -> this.getCollection(dbName).deleteOne(filter));
      if (result.getDeletedCount() > 0) {
        // A GC-eligible lease is orphaned (no local generation), so only the leaseKey-keyed state
        // needs clearing. The generation-keyed maps (leader/followerGenerationIds,
        // matViewGenerationIdToDefinitionVersion) track locally configured generations and are
        // cleared by drop(); a still-configured generation either renews its lease (never stale)
        // or is stopped by the cleaner's local-generation guard, so none leak here.
        evictCachedLease(leaseKey);
        return true;
      }
      return false;
    } catch (Exception e) {
      // Best effort: the next sweep re-evaluates this lease.
      LOG.atWarn()
          .addKeyValue("leaseKey", leaseKey)
          .setCause(e)
          .log("Failed to delete stale lease");
      return false;
    }
  }

  @Override
  public List<DeletedIndexLease> findLeasesForDeletedIndex(ObjectId indexId, String knownLeaseKey) {
    Optional<String> dbNameIfKnown =
        databaseForLease(knownLeaseKey, "listing a deleted index's leases");
    if (dbNameIfKnown.isEmpty()) {
      return List.of();
    }
    String dbName = dbNameIfKnown.get();
    // Index-bounded prefix prefilter only; Lease#indexId is the authority on lease-id layouts
    // and does the exact matching below.
    var filter = Filters.regex("_id", "^" + indexId.toHexString());
    try {
      List<BsonDocument> rawLeases =
          this.operationExecutor.execute(
              "findLeasesForDeletedIndex",
              () -> this.getCollection(dbName).find(filter).into(new ArrayList<>()));
      List<DeletedIndexLease> found = new ArrayList<>();
      for (BsonDocument rawLease : rawLeases) {
        Lease lease;
        try {
          lease = Lease.fromBson(rawLease);
        } catch (BsonParseException | RuntimeException e) {
          // Skip per document, as in the GC lease scan: one bad doc must not hide the rest.
          LOG.atWarn()
              .addKeyValue("indexId", indexId)
              .setCause(e)
              .log("Failed to parse a lease document while listing a deleted index's leases");
          continue;
        }
        if (!lease.indexId().equals(indexId.toHexString())) {
          // Prefix-matched but not this index's per Lease#indexId: corrupt data.
          LOG.atWarn()
              .addKeyValue("indexId", indexId)
              .addKeyValue("leaseKey", lease.id())
              .log("Skipping a prefix-matched lease that does not belong to the deleted index");
          continue;
        }
        this.leaseKeyToDatabase.putIfAbsent(lease.id(), dbName);
        found.add(new DeletedIndexLease(lease, LeaseManager.matViewNamespaceOf(dbName, lease)));
      }
      return found;
    } catch (Exception e) {
      LOG.atWarn()
          .addKeyValue("indexId", indexId)
          .setCause(e)
          .log("Failed to list a deleted index's leases");
      return List.of();
    }
  }

  /** The database mapped for {@code leaseKey}, or empty (logging {@code action}) if unknown. */
  private Optional<String> databaseForLease(String leaseKey, String action) {
    try {
      return Optional.of(getDatabaseForLease(leaseKey));
    } catch (IllegalStateException e) {
      LOG.atWarn()
          .addKeyValue("leaseKey", leaseKey)
          .addKeyValue("action", action)
          .setCause(e)
          .log("No database mapping for lease key, skipping");
      return Optional.empty();
    }
  }

  @Override
  public boolean cleanupLeaseForDeletedIndex(String leaseKey) {
    Optional<String> dbNameIfKnown = databaseForLease(leaseKey, "deleting a deleted index's lease");
    if (dbNameIfKnown.isEmpty()) {
      return false;
    }
    String dbName = dbNameIfKnown.get();
    // Evict the local copy first, as dropLease does: the cached entry must not outlive this
    // call even when the delete fails (the document is left for the GC either way).
    this.leases.remove(leaseKey);
    this.leaseKeyToDatabase.remove(leaseKey);
    // By id only: the index is gone from the desired config, so owner, staleness, and cleanup
    // state are all irrelevant — every lease under it is garbage. Concurrent mongots race this
    // idempotently; deletedCount 0 with no error means another one won.
    try {
      this.operationExecutor.execute(
          "cleanupLeaseForDeletedIndex",
          () -> this.getCollection(dbName).deleteOne(Filters.eq("_id", leaseKey)));
      return true;
    } catch (Exception e) {
      LOG.atWarn()
          .addKeyValue("leaseKey", leaseKey)
          .setCause(e)
          .log("Failed to delete a deleted index's lease");
      return false;
    }
  }

  @Override
  public Set<MaterializedViewGenerationId> getLeaderGenerationIds() {
    return Collections.unmodifiableSet(this.leaderGenerationIds);
  }

  @Override
  public Set<MaterializedViewGenerationId> getFollowerGenerationIds() {
    return Collections.unmodifiableSet(this.followerGenerationIds);
  }

  @Override
  public LeaseManager.FollowerPollResult pollFollowerStatuses() {
    Map<MaterializedViewGenerationId, IndexStatus> statuses = new HashMap<>();
    Map<MaterializedViewGenerationId, BsonTimestamp> replicationOpTimes = new HashMap<>();
    Set<MaterializedViewGenerationId> acquirableLeases = new HashSet<>();

    LOG.atDebug()
        .addKeyValue("followerCount", this.followerGenerationIds.size())
        .addKeyValue("hostname", this.hostname)
        .log("Polling follower statuses");

    if (this.followerGenerationIds.isEmpty()) {
      return new LeaseManager.FollowerPollResult(statuses, replicationOpTimes, acquirableLeases);
    }

    // Build a mapping from MaterializedViewGenerationID to LeaseKey for efficient lookup after
    // batch fetch.
    Map<MaterializedViewGenerationId, String> generationIdToLeaseKey = new HashMap<>();
    List<String> leaseKeys = new ArrayList<>();
    Set<MaterializedViewGenerationId> removedDuringPoll = new HashSet<>();
    for (MaterializedViewGenerationId generationId : new ArrayList<>(this.followerGenerationIds)) {
      try {
        String leaseKey = getLeaseKey(generationId);
        leaseKeys.add(leaseKey);
        generationIdToLeaseKey.put(generationId, leaseKey);
      } catch (IllegalStateException e) {
        LOG.warn(
            "Metadata removed for follower generation {} during poll, removing", generationId, e);
        cleanupAfterMetadataLoss(generationId, null);
        removedDuringPoll.add(generationId);
      }
    }

    if (leaseKeys.isEmpty()) {
      return new LeaseManager.FollowerPollResult(statuses, replicationOpTimes, acquirableLeases);
    }

    // Group lease keys by resolved database name so each batch query
    // targets a single database (one group for dedicated, potentially
    // many for MTM).
    Map<String, List<String>> dbToLeaseKeys = new HashMap<>();
    for (String leaseKey : leaseKeys) {
      try {
        dbToLeaseKeys
            .computeIfAbsent(getDatabaseForLease(leaseKey), k -> new ArrayList<>())
            .add(leaseKey);
      } catch (IllegalStateException e) {
        // Concurrent drop() removed the database mapping between the leaseKey resolution loop
        // above and this grouping step. Skip the key; it will be cleaned up by drop().
        LOG.warn(
            "No database mapping for lease key {} during follower poll, skipping.", leaseKey, e);
      }
    }

    // Batch fetch follower leases, one query per database.
    Map<String, Lease> fetchedLeases = new HashMap<>();
    try {
      for (var entry : dbToLeaseKeys.entrySet()) {
        String dbName = entry.getKey();
        List<String> keys = entry.getValue();
        List<BsonDocument> rawLeases =
            this.operationExecutor.execute(
                "getFollowerLeases",
                () ->
                    this.getCollectionForFollowerPoll(dbName)
                        .find(Filters.in("_id", keys))
                        .into(new ArrayList<>()));
        for (BsonDocument rawLease : rawLeases) {
          try {
            Lease parsed = parseLeaseOrThrow(rawLease);
            Lease normalized = normalizeLeaseIfNeeded(parsed);
            if (normalized != null) {
              fetchedLeases.put(normalized.id(), normalized);
            } else {
              this.metricsFactory.counter(CORRUPTED_LEASES_COUNTER_NAME).increment();
            }
          } catch (BsonParseException parseEx) {
            LOG.atError()
                .addKeyValue("leaseId", extractLeaseId(rawLease))
                .setCause(parseEx)
                .log("Failed to parse follower lease document, skipping");
          }
        }
      }
      LOG.atDebug()
          .addKeyValue("requestedCount", leaseKeys.size())
          .addKeyValue("fetchedCount", fetchedLeases.size())
          .log("Batch fetched follower leases");
    } catch (Exception e) {
      LOG.warn(
          "Failed to batch fetch follower leases, preserving existing statuses until next poll", e);
      // Return without updating statuses so callers preserve whatever status was last known
      // (e.g. STEADY). Setting UNKNOWN here would cause isTransient()=true in the confcall,
      // which filters the index before processReplicationStatus() can transition it to STALE
      // when replication stops (cluster pause / disk-full).
      return LeaseManager.FollowerPollResult.EMPTY;
    }

    // Process each follower generation ID.
    Instant now = Instant.now();
    for (MaterializedViewGenerationId generationId : new ArrayList<>(this.followerGenerationIds)) {
      if (removedDuringPoll.contains(generationId)) {
        continue;
      }
      String versionKey = this.matViewGenerationIdToDefinitionVersion.get(generationId);
      if (versionKey == null) {
        LOG.warn("No definition version found for generation ID {}", generationId);
        continue;
      }

      String leaseKey = generationIdToLeaseKey.get(generationId);
      Lease lease = fetchedLeases.get(leaseKey);

      if (lease != null) {
        // Update in-memory lease with DB state.
        this.leases.put(leaseKey, lease);

        // Extract status from the lease.
        IndexStatus status = getEffectiveMaterializedViewStatus(lease, versionKey, generationId);
        statuses.put(generationId, status);
        if (status.canServiceQueries()) {
          try {
            lease
                .extractHighWaterMark()
                .ifPresent(opTime -> replicationOpTimes.put(generationId, opTime));
          } catch (RuntimeException e) {
            this.metricsFactory.counter(CORRUPTED_LEASES_COUNTER_NAME).increment();
            LOG.atWarn()
                .addKeyValue("leaseId", lease.id())
                .addKeyValue("generationId", generationId)
                .setCause(e)
                .log("Failed to extract replication optime from follower lease, skipping optime");
          }
        }

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
    return new LeaseManager.FollowerPollResult(statuses, replicationOpTimes, acquirableLeases);
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
    List<MaterializedViewGenerationId> renewedGenerationIds = new ArrayList<>();
    List<MaterializedViewGenerationId> failedGenerationIds = new ArrayList<>();
    for (MaterializedViewGenerationId generationId : new ArrayList<>(this.leaderGenerationIds)) {
      if (renewLease(generationId)) {
        renewedGenerationIds.add(generationId);
      } else {
        failedGenerationIds.add(generationId);
      }
    }
    if (!renewedGenerationIds.isEmpty() || !failedGenerationIds.isEmpty()) {
      LOG.atInfo()
          .addKeyValue("renewedCount", renewedGenerationIds.size())
          .addKeyValue("renewedGenerationIds", renewedGenerationIds)
          .addKeyValue("failedCount", failedGenerationIds.size())
          .addKeyValue("failedGenerationIds", failedGenerationIds)
          .log("Heartbeat lease renewal summary");
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
  public boolean tryAcquireLeadership(MaterializedViewGenerationId generationId) {
    if (isInLeaseAcquisitionBlackout()) {
      LOG.atDebug()
          .addKeyValue("generationId", generationId)
          .log("Skipping leadership acquisition - in give-up blackout");
      return false;
    }
    if (!this.leaseStoreCircuitBreaker.allowAttempt()) {
      // Commit point stalled: skip the w:majority acquire that cannot commit; retried on half-open.
      this.acquisitionSuppressedCounter.increment();
      LOG.atDebug()
          .addKeyValue("generationId", generationId)
          .log("Skipping leadership acquisition - lease store circuit breaker open");
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

      // Record this generation's configured definition version on the lease being acquired. The
      // in-memory record from add() can be lost to a follower-poll overwrite before any write
      // persists it, and no other write path raises latestIndexDefinitionVersion — a reused lease
      // would otherwise resolve status and GC current-version protection against the old version
      // forever.
      String versionKey = this.matViewGenerationIdToDefinitionVersion.get(generationId);
      if (versionKey == null) {
        // Only possible when the generation is being dropped concurrently: add() registers the
        // version before a generation can become acquirable. Don't acquire leadership of a
        // generation that is being torn down.
        LOG.atDebug()
            .addKeyValue("generationId", generationId)
            .log("No definition version found during leadership acquisition");
        return false;
      }
      Lease newLease =
          inMemoryLease
              .withRenewedOwnership(this.hostname)
              .withRecordedIndexDefinitionVersion(versionKey);
      if (weOwnLease || leaseExpired) {
        boolean acquired = acquireExistingLease(generationId, inMemoryLease, newLease);
        // replaceOne completed (committed or OCC loss), so the store is reachable.
        this.leaseStoreCircuitBreaker.recordStoreReachable();
        return acquired;
      }
      LOG.atDebug()
          .addKeyValue("generationId", generationId)
          .log("Cannot acquire leadership - lease is owned by another instance and not expired");
      return false;
    } catch (Exception e) {
      if (isStallClassFailure(e)) {
        this.leaseStoreCircuitBreaker.recordStallFailure();
      }
      LOG.warn("Error attempting to acquire leadership for {}", generationId, e);
      return false;
    }
  }

  @Override
  public Optional<BsonTimestamp> getSteadyAsOfOplogPosition(
      MaterializedViewGenerationId generationId) {
    try {
      return Optional.ofNullable(this.leases.get(getLeaseKey(generationId)))
          .flatMap(Lease::getSteadyAsOfOplogPosition);
    } catch (IllegalStateException e) {
      // Index was deleted concurrently - metadata is gone.
      LOG.warn(
          "Metadata removed for generation {} during getSteadyAsOfOplogPosition", generationId, e);
      return Optional.empty();
    }
  }

  @Override
  public boolean isCurrentVersionQueryable(
      MaterializedViewGenerationId generationId, long indexDefinitionVersion) {
    try {
      Lease lease = this.leases.get(getLeaseKey(generationId));
      return lease != null && lease.isVersionQueryable(String.valueOf(indexDefinitionVersion));
    } catch (IllegalStateException e) {
      LOG.warn(
          "Failed to look up lease for generation {} during isCurrentVersionQueryable",
          generationId,
          e);
      return false;
    }
  }

  @Override
  public MaterializedViewCollectionMetadata initializeLease(
      MaterializedViewIndexDefinitionGeneration indexDefinitionGeneration,
      MaterializedViewCollectionMetadata proposedMetadata)
      throws Exception {
    this.leaseKeyToDatabase.put(
        proposedMetadata.collectionName(),
        this.dbResolver.resolve(indexDefinitionGeneration.getIndexDefinition().getDatabase()));
    // Register the definition version like add() does: leadership acquisition refuses generations
    // with no registered version, and acquisition can legitimately run between initializeLease and
    // add() (restart with an owned lease).
    this.matViewGenerationIdToDefinitionVersion.put(
        indexDefinitionGeneration.getGenerationId(),
        getIndexDefinitionVersion(indexDefinitionGeneration.getIndexDefinition()));
    @Var var existingLease = this.leases.get(proposedMetadata.collectionName());
    if (existingLease == null) {
      // Try to get lease from database and update in memory state.
      existingLease = refreshLeaseFromDatabase(proposedMetadata.collectionName());
    }
    if (existingLease != null) {
      // Catches the flex→dedicated migration case where the persisted lease is still in
      // INITIAL_SYNC (no commitInfo namespace to prefix-check) but its stored MV UUID no longer
      // matches the live restored MV. proposedMetadata.collectionUuid() comes from a fresh
      // getCollectionInfo() call in MaterializedViewCollectionResolver, so in ordinary
      // (non-migration) races both mongots resolve the same live MV collection to the same UUID
      // and this compare is a no-op. Only a stale snapshot-restored lease produces a mismatch.
      existingLease = repairStaleMvUuidIfNeeded(existingLease, proposedMetadata.collectionUuid());
      // If another Mongot already created the initial lease, just reuse, no extra network call.
      return existingLease.materializedViewCollectionMetadata();
    }
    // Fails to refresh from database, so we should be good to create a new entry in lease table.
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

  @VisibleForTesting
  void putLease(String leaseKey, Lease lease) {
    this.leases.put(leaseKey, lease);
  }

  @VisibleForTesting
  Map<String, String> getLeaseKeyToDatabase() {
    return ImmutableMap.copyOf(this.leaseKeyToDatabase);
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
          "createLease",
          () ->
              this.getCollection(getDatabaseForLease(newLease.id())).insertOne(newLease.toBson()));
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
      MaterializedViewGenerationId generationId, Lease currentLease, Lease newLease)
      throws Exception {
    boolean isReclaimingOwnLease = this.hostname.equals(currentLease.leaseOwner());

    var filter =
        createAcquireLeaseFilter(
            generationId, currentLease.leaseVersion(), newLease.leaseVersion());

    var result =
        this.operationExecutor.execute(
            "acquireLease",
            () ->
                this.getCollection(getDatabaseForLease(getLeaseKey(generationId)))
                    .replaceOne(filter, newLease.toBson()));

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
      MaterializedViewGenerationId generationId, long currentLeaseVersion, long newLeaseVersion) {
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
  private void refreshLeaseFromDatabase(MaterializedViewGenerationId generationId) {
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
   * Refreshes the local lease copy from the database.
   *
   * <p>Note: Can be called before mvMetadataCatalog is populated.
   */
  @Nullable
  private Lease refreshLeaseFromDatabase(String leaseKey) {
    try {
      Lease lease = getLeaseFromDatabase(leaseKey);
      if (lease != null) {
        this.leases.put(leaseKey, lease);
      }
      return lease;
    } catch (Exception e) {
      LOG.warn("Failed to refresh lease from database for key: {}", leaseKey, e);
      return null;
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
  private boolean renewLease(MaterializedViewGenerationId generationId) {
    @Var String leaseKey = null;
    try {
      leaseKey = getLeaseKey(generationId);
      Lease currentLease = this.leases.get(leaseKey);
      if (currentLease == null) {
        LOG.warn(
            "No local lease found for leader generation {}, transitioning to follower",
            generationId);
        becomeFollower(generationId);
        return false;
      }

      // If our in-memory lease has expired, we've lost the right to be leader.
      // Another instance may have already taken over. Give up leadership.
      Instant now = Instant.now();
      if (now.isAfter(currentLease.leaseExpiration())) {
        LOG.warn(
            "In-memory lease expired for {}, transitioning to follower. "
                + "Expiration: {}, Now: {}",
            generationId,
            currentLease.leaseExpiration(),
            now);
        refreshLeaseFromDatabase(leaseKey);
        becomeFollower(generationId);
        return false;
      }

      // Skip renewal if the lease still has more than half its TTL remaining.
      // This avoids a concurrent mutation race with updateCommitInfo/updateReplicationStatus,
      // which also extend the lease expiration as a side effect of their DB writes.
      // TODO(CLOUDP-397647): Consider plumbing the actual heartbeat interval here instead of
      // using a fixed TTL/2 threshold, to ensure sufficient buffer for non-default intervals.
      Instant renewThreshold = now.plusMillis(Lease.LEASE_EXPIRATION_MS / 2);
      if (currentLease.leaseExpiration().isAfter(renewThreshold)) {
        return true;
      }

      Lease renewedLease = currentLease.withRenewedOwnership(this.hostname);
      var filter =
          createUpdateFilterForOwnedLease(
              leaseKey, currentLease.leaseVersion(), renewedLease.leaseVersion());

      var result =
          this.operationExecutor.execute(
              "renewLease",
              () ->
                  this.getCollection(getDatabaseForLease(getLeaseKey(generationId)))
                      .replaceOne(filter, renewedLease.toBson()));

      // replaceOne reached the server (committed or OCC loss), so the store is reachable.
      this.leaseStoreCircuitBreaker.recordStoreReachable();
      if (result.getMatchedCount() > 0) {
        // Successfully renewed.
        this.leases.put(leaseKey, renewedLease);
        return true;
      } else {
        // Lease was taken by another instance - lost leadership.
        LOG.warn(
            "Lease renewal failed for {} - lease was updated by another instance", generationId);
        refreshLeaseFromDatabase(leaseKey);
        becomeFollower(generationId);
        return false;
      }
    } catch (IllegalStateException e) {
      LOG.warn(
          "Metadata removed for generation {} during lease renewal, removing from leaders",
          generationId,
          e);
      cleanupAfterMetadataLoss(generationId, leaseKey);
      return false;
    } catch (Exception e) {
      // Transient error (network, etc.) - don't give up leadership yet.
      // Let the next heartbeat cycle retry. If the lease expires, we'll give up then.
      if (isStallClassFailure(e)) {
        this.leaseStoreCircuitBreaker.recordStallFailure();
      }
      LOG.warn("Failed to renew lease for {}, will retry on next heartbeat", generationId, e);
      return false;
    }
  }

  /** Transitions a generation ID from leader to follower state. */
  private void becomeFollower(MaterializedViewGenerationId generationId) {
    this.leaderGenerationIds.remove(generationId);
    // Only add to follower if we're still managing this generation.
    // If the lease was removed by drop(), we must not re-add to followerGenerationIds.
    // This prevents a race condition where drop() removes the generation from both sets,
    // but a concurrent renewLease() or updateLeaseInDatabase() call re-adds it to follower.
    try {
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
    } catch (IllegalStateException e) {
      // Index was deleted concurrently - metadata is gone. Don't add to followers.
      LOG.warn(
          "Metadata removed for generation {} during becomeFollower, skipping follower transition",
          generationId,
          e);
      this.matViewGenerationIdToDefinitionVersion.remove(generationId);
    }
  }

  private void ensureLeaseExists(String leaseKey) {
    if (!this.leases.containsKey(leaseKey)) {
      throw new IllegalStateException("Lease does not exist for " + leaseKey);
    }
  }

  /**
   * Cleans up all per-generation state after metadata has been concurrently removed. Removes the
   * generation from leader/follower tracking sets, definition version mapping, and optionally the
   * in-memory lease cache.
   *
   * @param generationId the generation whose state should be purged
   * @param leaseKey the resolved lease key, or {@code null} if it was not resolved before the
   *     failure (e.g. {@code getLeaseKey} itself threw)
   */
  private void cleanupAfterMetadataLoss(
      MaterializedViewGenerationId generationId, @Nullable String leaseKey) {
    this.leaderGenerationIds.remove(generationId);
    this.followerGenerationIds.remove(generationId);
    this.matViewGenerationIdToDefinitionVersion.remove(generationId);
    if (leaseKey != null) {
      this.leases.remove(leaseKey);
    }
  }

  private String getLeaseKey(MaterializedViewGenerationId generationId) {
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
  private Lease getLeaseFromDatabase(MaterializedViewGenerationId generationId) throws Exception {
    return getLeaseFromDatabase(getLeaseKey(generationId));
  }

  @Nullable
  private Lease getLeaseFromDatabase(String collectionName) throws Exception {
    BsonDocument rawLease =
        this.operationExecutor.execute(
            "getLease",
            () ->
                this.getCollection(getDatabaseForLease(collectionName))
                    .find(new Document("_id", collectionName))
                    .first());
    if (rawLease == null) {
      return null;
    }
    Lease lease = normalizeLeaseIfNeeded(parseLeaseOrThrow(rawLease));
    if (lease == null) {
      this.metricsFactory.counter(CORRUPTED_LEASES_COUNTER_NAME).increment();
    }
    return lease;
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
  private boolean updateLeaseInDatabase(
      String leaseKey,
      MaterializedViewGenerationId generationId,
      Lease currentLease,
      Lease updatedLease,
      EncodedUserData encodedUserData) {
    try {
      // Base filter checks ownership and version (normal or idempotent case).
      var baseFilter =
          createUpdateFilterForOwnedLease(
              leaseKey, currentLease.leaseVersion(), updatedLease.leaseVersion());
      // For the idempotent case, also verify commitInfo matches to confirm it was our write.
      var filter =
          Filters.and(
              baseFilter,
              Filters.or(
                  Filters.eq(Lease.Fields.LEASE_VERSION.getName(), currentLease.leaseVersion()),
                  Filters.eq(Lease.Fields.COMMIT_INFO.getName(), encodedUserData.asString())));
      var result =
          this.operationExecutor.execute(
              "updateLease",
              () ->
                  this.getCollection(getDatabaseForLease(getLeaseKey(generationId)))
                      .replaceOne(filter, updatedLease.toBson()));
      if (result.getMatchedCount() == 0) {
        // OCC failure - we lost leadership (or lease was deleted during index drop).
        becomeFollower(generationId);
        LOG.warn(
            "Failed to update lease for {} - ownership/version mismatch or lease deleted.",
            leaseKey);
        return false;
      }
      this.leases.put(leaseKey, updatedLease);
      return true;
    } catch (Exception e) {
      // Transient error (e.g., network issue) - throw so caller can retry on next cycle.
      throw new MaterializedViewTransientException(
          String.valueOf(e.getMessage()),
          e,
          MaterializedViewTransientException.Reason.LEASE_OPERATION_FAILED);
    }
  }

  private String getIndexDefinitionVersion(IndexDefinition indexDefinition) {
    return String.valueOf(
        indexDefinition.getDefinitionVersion().orElse(DEFAULT_INDEX_DEFINITION_VERSION));
  }

  private String getDatabaseForLease(String leaseKey) {
    String db = this.leaseKeyToDatabase.get(leaseKey);
    if (db == null) {
      throw new IllegalStateException(
          "No database mapping found for lease key '"
              + leaseKey
              + "'. Ensure add() or initializeLease() has been called before operating on this"
              + " key.");
    }
    return db;
  }

  private LeaseRepairUtil.CollectionUuidResolver buildUuidResolver() {
    return (db, coll) -> {
      MongoDbCollectionInfo info;
      try {
        info =
            getCollectionInfo(
                Check.isPresent(
                    this.autoEmbeddingMongoClient.getLeaseManagerMongoClient(),
                    "leaseManagerMongoClient"),
                db,
                coll);
      } catch (Throwable t) {
        // Broad on purpose: repair must never crash leadership acquisition.
        throw new LeaseRepairUtil.LeaseRepairException(
            "failed to resolve UUID for " + db + "." + coll, t);
      }
      if (info instanceof MongoDbCollectionInfo.Collection collection) {
        return collection.info().uuid();
      }
      throw new LeaseRepairUtil.LeaseRepairException(
          "expected collection but found "
              + info.getClass().getSimpleName()
              + " at "
              + db
              + "."
              + coll);
    };
  }

  private MongoCollection<BsonDocument> getCollection(String databaseName)
      throws MaterializedViewTransientException {
    return getLeaseCollection(databaseName, ReadConcern.LINEARIZABLE);
  }

  /**
   * Lease handle for the follower poll only: {@code majority} read concern (not {@code
   * linearizable}) so the poll survives a stalled commit point. Safe because leadership is fenced
   * by the {@code w:majority} OCC acquire write, not this read; writes and GC reads keep {@link
   * #getCollection}'s linearizable concern.
   */
  private MongoCollection<BsonDocument> getCollectionForFollowerPoll(String databaseName)
      throws MaterializedViewTransientException {
    return getLeaseCollection(databaseName, ReadConcern.MAJORITY);
  }

  // Write concern is set unconditionally; it's ignored on the read-only follower poll.
  private MongoCollection<BsonDocument> getLeaseCollection(
      String databaseName, ReadConcern readConcern) throws MaterializedViewTransientException {
    try {
      return Check.isPresent(
              this.autoEmbeddingMongoClient.getLeaseManagerMongoClient(), "leaseManagerMongoClient")
          .getDatabase(databaseName)
          .getCollection(LEASE_COLLECTION_NAME, BsonDocument.class)
          .withReadConcern(readConcern)
          .withReadPreference(ReadPreference.primary())
          .withWriteConcern(WriteConcern.MAJORITY);
    } catch (AssertionError e) {
      // Empty getLeaseManagerMongoClient() when the sync source is missing.
      throw new MaterializedViewTransientException(
          String.valueOf(e.getMessage()),
          e,
          MaterializedViewTransientException.Reason.MONGO_CLIENT_NOT_AVAILABLE);
    }
  }

  /**
   * Whether a lease-op failure is stall-class (store unreachable / commit stalled) rather than an
   * OCC loss (does not throw) or a logic error — only these advance the circuit breaker.
   */
  private static boolean isStallClassFailure(Throwable t) {
    for (@Var Throwable cause = t; cause != null; cause = cause.getCause()) {
      if (cause instanceof MongoSocketException || cause instanceof MongoTimeoutException) {
        return true;
      }
    }
    return false;
  }
}
