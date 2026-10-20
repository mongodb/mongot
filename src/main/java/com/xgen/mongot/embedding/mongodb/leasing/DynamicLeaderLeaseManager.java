package com.xgen.mongot.embedding.mongodb.leasing;

import static com.xgen.mongot.embedding.mongodb.leasing.StatusResolutionUtils.getEffectiveMaterializedViewStatus;
import static com.xgen.mongot.util.FutureUtils.COMPLETED_FUTURE;
import static com.xgen.mongot.util.Uuids.NIL;
import static com.xgen.mongot.util.mongodb.MongoDbDatabase.getCollectionInfo;
import static com.xgen.mongot.util.mongodb.MongoDbDatabase.getCollectionInfos;

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
import io.micrometer.core.instrument.Tags;
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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;
import org.bson.BsonDateTime;
import org.bson.BsonDocument;
import org.bson.BsonTimestamp;
import org.bson.BsonValue;
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

  /** This instance's local role for a configured GenerationId. */
  private enum LocalLeaseRole {
    FOLLOWER,
    LEADER
  }

  /** Tracks this instance's local role for each configured GenerationId. */
  private static final class LocalLeaseRoleTracker {
    private final Map<MaterializedViewGenerationId, LocalLeaseRole> roleByGenerationId =
        new ConcurrentHashMap<>();

    boolean isLeader(MaterializedViewGenerationId generationId) {
      return this.roleByGenerationId.get(generationId) == LocalLeaseRole.LEADER;
    }

    void setLeader(MaterializedViewGenerationId generationId) {
      this.roleByGenerationId.put(generationId, LocalLeaseRole.LEADER);
    }

    void setFollower(MaterializedViewGenerationId generationId) {
      this.roleByGenerationId.put(generationId, LocalLeaseRole.FOLLOWER);
    }

    /** Unconditionally clears any role for {@code generationId}. */
    void forget(MaterializedViewGenerationId generationId) {
      this.roleByGenerationId.remove(generationId);
    }

    /**
     * Atomically remaps {@code generationId}'s role, same contract as {@link
     * java.util.concurrent.ConcurrentHashMap#compute}: {@code remappingFunction} is invoked with
     * the current role (or {@code null} if untracked) and its return value becomes the new role
     * ({@code null} to clear it). The whole read-decide-write happens under the map's per-key lock,
     * so a concurrent {@link #setLeader}/{@link #setFollower}/{@link #forget} can't land in the
     * middle of the decision and get silently clobbered by it.
     */
    void computeRole(
        MaterializedViewGenerationId generationId,
        BiFunction<MaterializedViewGenerationId, LocalLeaseRole, LocalLeaseRole>
            remappingFunction) {
      this.roleByGenerationId.compute(generationId, remappingFunction::apply);
    }

    /**
     * Whether any generation currently has {@code FOLLOWER}. A plain {@link
     * ConcurrentHashMap#containsValue} scan - measured at ~30us for 10,000 entries, negligible for
     * something called once per poll/heartbeat cycle - short-circuits on the first match and never
     * allocates, unlike {@link #followerIds}.
     */
    boolean hasAnyFollower() {
      return this.roleByGenerationId.containsValue(LocalLeaseRole.FOLLOWER);
    }

    Set<MaterializedViewGenerationId> leaderIds() {
      return idsWithRole(LocalLeaseRole.LEADER);
    }

    Set<MaterializedViewGenerationId> followerIds() {
      return idsWithRole(LocalLeaseRole.FOLLOWER);
    }

    private Set<MaterializedViewGenerationId> idsWithRole(LocalLeaseRole role) {
      return Collections.unmodifiableSet(
          this.roleByGenerationId.entrySet().stream()
              .filter(entry -> entry.getValue() == role)
              .map(Map.Entry::getKey)
              .collect(Collectors.toSet()));
    }
  }

  private static final Logger LOG = LoggerFactory.getLogger(DynamicLeaderLeaseManager.class);

  private static final String METRICS_NAMESPACE = "embedding.leasing.stats";

  @VisibleForTesting static final String LEASE_COLLECTION_NAME = "auto_embedding_leases";

  private static final long GIVE_UP_BLACKOUT_SECONDS = 60;

  static final String OPS_GIVE_UP_LEASE_SUCCESS_COUNTER_NAME = "opsGiveUpLeaseSuccess";
  static final String OPS_GIVE_UP_LEASE_FAILURE_COUNTER_NAME = "opsGiveUpLeaseFailure";
  // One counter tagged by outcome, as MaterializedViewManager does for collection drops. A
  // refusal is the guard working, not an error, so it must not share a counter with a real
  // failure that someone would page on.
  static final String OPS_REPAIR_LEASE_COUNTER_NAME = "opsRepairLease";
  static final String OPS_REPAIR_OUTCOME_TAG = "outcome";
  static final String REPAIR_OUTCOME_DELETED = "deleted";
  static final String REPAIR_OUTCOME_REFUSED_STILL_USABLE = "refusedStillUsable";
  static final String REPAIR_OUTCOME_ALREADY_GONE = "alreadyGone";
  static final String REPAIR_OUTCOME_LOST_RACE = "lostRace";
  // The load path could not be re-run to completion (a transient lookup failure), so the tool
  // cannot tell a corrupt lease from a healthy one and declines to delete. Distinct from
  // REPAIR_OUTCOME_ERROR, which is an unexpected failure someone would page on.
  static final String REPAIR_OUTCOME_INDETERMINATE = "indeterminate";
  // The lease has not expired yet, so another host may still hold it. Deleting now would let the
  // recreated lease restart at version 1 and defeat fencing; the guard working, not an error.
  static final String REPAIR_OUTCOME_REFUSED_NOT_EXPIRED = "refusedNotExpired";
  static final String REPAIR_OUTCOME_ERROR = "error";
  static final String POST_RESTORE_LEASE_REPAIR_SUCCESS_COUNTER_NAME =
      "postRestoreLeaseRepairSuccess";
  static final String POST_RESTORE_LEASE_REPAIR_FAILURE_COUNTER_NAME =
      "postRestoreLeaseRepairFailure";
  // Tenant-prefix shape matched but the lease was left alone. Tracks how often customer database
  // names trip the heuristic, fleet-wide.
  static final String POST_RESTORE_REPAIR_SUPPRESSED_COUNTER_NAME = "postRestoreRepairSuppressed";
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
  // Tracks this instance's local role (leader or follower) for each configured GenerationId. A
  // generation with no entry here is not currently tracked/registered.
  private final LocalLeaseRoleTracker localRoles;
  // Maps GenerationId to its definition version (as String) for use in pollFollowerStatuses().
  private final Map<MaterializedViewGenerationId, String> matViewGenerationIdToDefinitionVersion;
  private final MaterializedViewCollectionMetadataCatalog mvMetadataCatalog;
  private final InternalDatabaseResolver dbResolver;
  // Holds the internal database the lease document lives
  private final Map<String, String> leaseKeyToDatabase;
  // Lease key to the source database on the live index definition, unresolved as seen from MMS.
  private final Map<String, String> leaseKeyToSourceDatabase;
  private final AutoEmbeddingMongoClient autoEmbeddingMongoClient;
  // End of give-up blackout (epoch ms). While now < this, do not acquire leadership.
  private final AtomicLong giveUpBlackoutEndTimeMs = new AtomicLong(0);
  private final Counter postRestoreLeaseRepairSuccessCounter;
  private final Counter postRestoreLeaseRepairFailureCounter;
  private final Counter postRestoreRepairSuppressedCounter;
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
  private final CheckpointCommitRiskTracker checkpointCommitRisk;
  // Leases the load path rejected, keyed by lease id. A rejected lease never reaches this.leases,
  // so this map is the only in-process record that one exists. Entries clear as soon as the same
  // lease loads cleanly or the GC sweep stops seeing it.
  //
  // TODO(CLOUDP-437301): report these ids upward in the conf-call request. Today the only signal
  // an operator gets is the corruptedLeases counter, which carries no ids, so finding the lease to
  // repair means reading host logs. Sending the ids would make the repair tool self-service.
  private final Map<String, CorruptLease> corruptLeases;
  // The bootstrap repair command is applied once per process; see executeOpsRepairLeaseCommand.
  private final AtomicBoolean opsRepairLeaseApplied = new AtomicBoolean(false);
  // Evaluated per heartbeat tick (never cached): whether the lease renewal runs the field-scoped
  // $max updateOne instead of the whole-document replaceOne. Kept as a BooleanSupplier so this
  // class stays free of a DynamicFeatureFlagRegistry dependency and both branches are trivially
  // testable. Only safe when every acquirer in the cluster runs the expiration-gated takeover
  // filter; see the dynamic feature flag's Javadoc.
  private final BooleanSupplier leaseHeartbeatPartialUpdateEnabled;

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
        LeaseManagerOpsCommands.NONE,
        () -> false);
  }

  @TestOnly
  public DynamicLeaderLeaseManager(
      AutoEmbeddingMongoClient autoEmbeddingMongoClient,
      MetricsFactory metricsFactory,
      String hostname,
      InternalDatabaseResolver dbResolver,
      MaterializedViewCollectionMetadataCatalog mvMetadataCatalog,
      BooleanSupplier leaseHeartbeatPartialUpdateEnabled) {
    this(
        autoEmbeddingMongoClient,
        metricsFactory,
        hostname,
        dbResolver,
        mvMetadataCatalog,
        LeaseManagerOpsCommands.NONE,
        leaseHeartbeatPartialUpdateEnabled);
  }

  private DynamicLeaderLeaseManager(
      AutoEmbeddingMongoClient autoEmbeddingMongoClient,
      MetricsFactory metricsFactory,
      String hostname,
      InternalDatabaseResolver dbResolver,
      MaterializedViewCollectionMetadataCatalog mvMetadataCatalog,
      LeaseManagerOpsCommands opsCommands,
      BooleanSupplier leaseHeartbeatPartialUpdateEnabled) {
    this.metricsFactory = metricsFactory;
    this.operationExecutor =
        new MongoClientOperationExecutor(metricsFactory, "leaseTableCollection");
    this.postRestoreLeaseRepairSuccessCounter =
        metricsFactory.counter(POST_RESTORE_LEASE_REPAIR_SUCCESS_COUNTER_NAME);
    this.postRestoreLeaseRepairFailureCounter =
        metricsFactory.counter(POST_RESTORE_LEASE_REPAIR_FAILURE_COUNTER_NAME);
    this.postRestoreRepairSuppressedCounter =
        metricsFactory.counter(POST_RESTORE_REPAIR_SUPPRESSED_COUNTER_NAME);
    this.hostname = hostname;
    this.mvMetadataCatalog = mvMetadataCatalog;
    this.leases = new ConcurrentHashMap<>();
    this.localRoles = new LocalLeaseRoleTracker();
    this.matViewGenerationIdToDefinitionVersion = new ConcurrentHashMap<>();
    this.dbResolver = dbResolver;
    this.leaseKeyToDatabase = new ConcurrentHashMap<>();
    this.leaseKeyToSourceDatabase = new ConcurrentHashMap<>();
    this.corruptLeases = new ConcurrentHashMap<>();
    this.autoEmbeddingMongoClient = autoEmbeddingMongoClient;
    this.opsCommands = opsCommands;
    this.leaseHeartbeatPartialUpdateEnabled = leaseHeartbeatPartialUpdateEnabled;
    this.isDedicatedCluster = dbResolver.isDedicatedCluster();
    this.uuidResolver = buildUuidResolver();
    this.acquisitionSuppressedCounter =
        this.metricsFactory.counter(ACQUISITION_SUPPRESSED_COUNTER_NAME);
    this.checkpointRejectedCounter = this.metricsFactory.counter(CHECKPOINT_REJECTED_COUNTER_NAME);
    this.skippedCheckpointCounter = this.metricsFactory.counter(SKIPPED_CHECKPOINT_COUNTER_NAME);
    this.checkpointCommitRisk = new CheckpointCommitRiskTracker(this.metricsFactory, hostname);
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
    return create(
        autoEmbeddingMongoClient,
        meterAndFtdcRegistry,
        hostname,
        dbResolver,
        mvMetadataCatalog,
        opsCommands,
        () -> false);
  }

  public static DynamicLeaderLeaseManager create(
      AutoEmbeddingMongoClient autoEmbeddingMongoClient,
      MeterAndFtdcRegistry meterAndFtdcRegistry,
      String hostname,
      InternalDatabaseResolver dbResolver,
      MaterializedViewCollectionMetadataCatalog mvMetadataCatalog,
      LeaseManagerOpsCommands opsCommands,
      BooleanSupplier leaseHeartbeatPartialUpdateEnabled) {
    return new DynamicLeaderLeaseManager(
        autoEmbeddingMongoClient,
        new MetricsFactory(METRICS_NAMESPACE, meterAndFtdcRegistry.meterRegistry()),
        hostname,
        dbResolver,
        mvMetadataCatalog,
        opsCommands,
        leaseHeartbeatPartialUpdateEnabled);
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
            leaseKeyToGiveUp, lease.leaseVersion(), released.leaseVersion(), lease.epoch());
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
          Lease parsed = parseLeaseOrThrow(rawLease, defaultDb);
          this.leaseKeyToDatabase.putIfAbsent(parsed.id(), defaultDb);
          Lease lease = normalizeLeaseIfNeeded(parsed);
          if (lease != null) {
            this.leases.put(lease.id(), lease);
            // The lease loads cleanly now, so retire whatever an earlier sweep recorded against it.
            // An operator command must never delete a lease that has recovered.
            this.corruptLeases.remove(lease.id());
          } else {
            // normalizeLeaseIfNeeded already recorded which check rejected this lease.
            this.metricsFactory.counter(CORRUPTED_LEASES_COUNTER_NAME).increment();
            corruptedLeaseIds.add(parsed.id());
          }
        } catch (BsonParseException e) {
          // parseLeaseOrThrow counted and recorded it.
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
   * Wraps {@link Lease#fromBson}, increments the corrupted lease counter on parse failure, and
   * records the rejection against {@code dbName}. Callers can catch the re-thrown exception to
   * decide whether to skip or propagate.
   *
   * <p>Recording belongs here rather than in any one caller because every path that reads the lease
   * collection funnels through this method. A rejection recorded by whichever path happened to read
   * the document first is the only in-process trace of a lease that never reaches {@link #leases}.
   */
  private Lease parseLeaseOrThrow(BsonDocument rawLease, String dbName) throws BsonParseException {
    try {
      return Lease.fromBson(rawLease);
    } catch (BsonParseException e) {
      this.metricsFactory.counter(CORRUPTED_LEASES_COUNTER_NAME).increment();
      leaseIdOf(rawLease)
          .ifPresentOrElse(
              id -> recordCorruptLease(id, dbName, LeaseRepairUtil.CorruptionReason.UNPARSEABLE),
              // No readable string id, so there is no key to record the rejection under and the
              // lease is counted but absent from corruptLeases. The map is only a lookup hint, so
              // this is not a safety problem, but the runbook tells operators to read the lease id
              // out of the logs. Without this line the document is counted and then vanishes.
              () ->
                  LOG.atWarn()
                      .addKeyValue("database", dbName)
                      .addKeyValue("idType", idTypeOf(rawLease))
                      .log(
                          "Corrupt lease document has no readable string _id; counted but not"
                              + " recorded for repair. Read the raw document to recover its id"));
      throw e;
    }
  }

  private static String extractLeaseId(BsonDocument rawLease) {
    if (!rawLease.containsKey("_id")) {
      return "unknown";
    }
    return leaseIdOf(rawLease).orElse("unparseable");
  }

  /** The BSON type of the document's {@code _id}, for logs when it is not a readable string. */
  private static String idTypeOf(BsonDocument rawLease) {
    BsonValue id = rawLease.get("_id");
    return id == null ? "absent" : id.getBsonType().name();
  }

  /** The lease's {@code _id} as a string, or empty when the document has no readable string id. */
  private static Optional<String> leaseIdOf(BsonDocument rawLease) {
    try {
      return rawLease.containsKey("_id")
          ? Optional.of(rawLease.get("_id").asString().getValue())
          : Optional.empty();
    } catch (Exception e) {
      return Optional.empty();
    }
  }

  /**
   * Applies the bootstrap {@code opsRepairLease} command, deleting lease documents the load path
   * still rejects.
   *
   * <p>Applied once per process. The command arrives in bootstrap config, so it cannot be withdrawn
   * while the host runs, and re-attempting on every poll would keep incrementing the repair counter
   * long after the work was done. One-shot matches the delivery: an operator sets the config,
   * restarts the host, and the command fires on the first poll. If it does not take, the next
   * attempt is another restart, which the runbook says plainly.
   */
  private void executeOpsRepairLeaseCommand() {
    Optional<LeaseManagerOpsCommands.OpsRepairLeaseCommand> command =
        this.opsCommands.opsRepairLease();
    if (command.isEmpty() || this.opsRepairLeaseApplied.getAndSet(true)) {
      return;
    }
    if (!isValidRepairLeaseCommand(command.get())) {
      return;
    }
    @Var int deleted = 0;
    for (String leaseId : command.get().leaseIds()) {
      // Barrier per lease id. This runs at the top of pollFollowerStatuses, which
      // MaterializedViewManager schedules with scheduleWithFixedDelay, where a throw cancels every
      // future execution. An ops command must not be able to stop a host refreshing statuses for
      // all of its generations, so one bad lease id costs that id and nothing else. AssertionError
      // is caught too: Check.instanceOf and Check.isPresent throw it on this path.
      try {
        if (deleteCorruptLease(leaseId)) {
          deleted++;
        }
      } catch (Exception | AssertionError t) {
        recordRepairOutcome(REPAIR_OUTCOME_ERROR);
        LOG.atError()
            .addKeyValue("leaseId", leaseId)
            .addKeyValue("hostname", this.hostname)
            .setCause(t)
            .log("Repair of this lease id threw; continuing with the remaining ids");
      }
    }
    LOG.atWarn()
        .addKeyValue("deleted", deleted)
        .addKeyValue("requested", command.get().leaseIds().size())
        .addKeyValue("hostname", this.hostname)
        .log("Applied ops repair lease command");
  }

  private boolean isValidRepairLeaseCommand(LeaseManagerOpsCommands.OpsRepairLeaseCommand command) {
    if (!command.instance().equals(this.hostname)) {
      LOG.atInfo()
          .addKeyValue("commandInstance", command.instance())
          .addKeyValue("hostname", this.hostname)
          .log("Skipping repair lease ops command - instance mismatch");
      return false;
    }
    if (Instant.now().isAfter(command.expiresAt())) {
      // Logged at INFO, not DEBUG: an expired command is the most likely reason an operator sees
      // nothing happen, and the window has to outlast a reconfigure and restart.
      LOG.atInfo()
          .addKeyValue("expiresAt", command.expiresAt())
          .addKeyValue("hostname", this.hostname)
          .log("Skipping repair lease ops command - command expired");
      return false;
    }
    if (command.leaseIds().isEmpty()) {
      // WARN, not INFO: a misspelled leaseIds key is reported as an unknown field and ignored
      // rather than rejected, so the command still arrives here with the defaulted empty list.
      // This log is the only place that mistake becomes visible.
      LOG.atWarn()
          .addKeyValue("hostname", this.hostname)
          .log(
              "Skipping repair lease ops command - no leases targeted; check the leaseIds key "
                  + "for a typo");
      return false;
    }
    return true;
  }

  /**
   * Where a lease document lives, or why the search could not say.
   *
   * <p>{@code searchIncomplete} separates "every candidate database was checked and none held it"
   * from "at least one check did not complete". Only the first justifies concluding the document is
   * gone; the second is the same unanswered-lookup case the re-check declines on.
   */
  private record LeaseLocation(Optional<String> database, boolean searchIncomplete) {
    static LeaseLocation found(String database) {
      return new LeaseLocation(Optional.of(database), false);
    }

    static final LeaseLocation CONFIRMED_ABSENT = new LeaseLocation(Optional.empty(), false);
    static final LeaseLocation UNDETERMINED = new LeaseLocation(Optional.empty(), true);
  }

  /**
   * Locates the database holding {@code leaseId}. Prefers a recorded rejection, then the
   * generation's database mapping, then a search of every database this host knows about, mirroring
   * the set {@link #findGcCandidates} sweeps.
   *
   * <p>The search matters on a multi-tenant cluster, where leases live in per-tenant internal
   * databases and the caller names only a lease id.
   *
   * <p>A per-database failure is logged and the search continues, because another candidate may
   * still hold the document. If the search then finds nothing, the failures are what distinguish
   * {@link LeaseLocation#UNDETERMINED} from {@link LeaseLocation#CONFIRMED_ABSENT}: reporting a
   * lease gone when no database was successfully read would clear its rejection record on the
   * strength of an outage.
   */
  private LeaseLocation locateLease(String leaseId) {
    CorruptLease recorded = this.corruptLeases.get(leaseId);
    if (recorded != null) {
      return LeaseLocation.found(recorded.database());
    }
    Set<String> candidates = knownInternalDatabases();
    @Var boolean anyLookupFailed = false;
    for (String dbName : candidates) {
      try {
        BsonDocument found =
            this.operationExecutor.execute(
                "locateLease",
                () -> this.getCollection(dbName).find(Filters.eq("_id", leaseId)).first());
        if (found != null) {
          return LeaseLocation.found(dbName);
        }
      } catch (Exception e) {
        anyLookupFailed = true;
        LOG.atWarn()
            .addKeyValue("leaseId", leaseId)
            .addKeyValue("database", dbName)
            .setCause(e)
            .log("Failed to search database while locating a lease");
      }
    }
    if (anyLookupFailed) {
      return LeaseLocation.UNDETERMINED;
    }
    if (!this.isDedicatedCluster && this.leaseKeyToDatabase.isEmpty()) {
      // Multi-tenant cluster before any generation has registered, which happens when the one-shot
      // command fires on the first poll after a restart, ahead of conf-call index registration.
      // Only the default internal database was searched and the tenant databases are unreachable,
      // so absence is not established. Reporting alreadyGone here would burn the one-shot on a
      // false verdict; indeterminate tells the operator to reapply instead.
      LOG.atWarn()
          .addKeyValue("leaseId", leaseId)
          .log(
              "No tenant database mappings registered yet, cannot rule out a lease in a tenant"
                  + " database");
      return LeaseLocation.UNDETERMINED;
    }
    return LeaseLocation.CONFIRMED_ABSENT;
  }

  /**
   * Every internal database this host currently knows about: the default plus each database a
   * registered generation or sweep has mapped. Shared by {@link #locateLease} and {@link
   * #findGcCandidates} so the repair search and the GC sweep stay aligned by construction.
   */
  private Set<String> knownInternalDatabases() {
    Set<String> databases = new HashSet<>(this.leaseKeyToDatabase.values());
    databases.add(this.dbResolver.resolveDefault());
    return databases;
  }

  /** Records why the load path rejected {@code leaseId}, when its database is known. */
  private void recordCorruptLease(String leaseId, LeaseRepairUtil.CorruptionReason reason) {
    databaseForLease(leaseId, "recording a rejected lease")
        .ifPresent(dbName -> recordCorruptLease(leaseId, dbName, reason));
  }

  private void recordCorruptLease(
      String leaseId, String dbName, LeaseRepairUtil.CorruptionReason reason) {
    this.corruptLeases.put(leaseId, new CorruptLease(dbName, reason));
  }

  /** Increments the repair counter under {@code outcome}. */
  private void recordRepairOutcome(String outcome) {
    this.metricsFactory
        .counter(OPS_REPAIR_LEASE_COUNTER_NAME, Tags.of(OPS_REPAIR_OUTCOME_TAG, outcome))
        .increment();
  }

  /** Whether the load path rejects {@code rawLease}, refuses it as usable, or cannot tell. */
  private enum RepairClassification {
    /** The load path definitively rejects this document; mongot would skip it on every pass. */
    REJECTED_CORRUPT,
    /** The document loads cleanly, so deleting it would drop a live fencing token. */
    USABLE,
    /** A lookup failed, so corruption cannot be confirmed. Fail closed and decline the delete. */
    UNDETERMINED
  }

  /**
   * Re-runs the load path over {@code rawLease} and reports whether it is still rejected, usable
   * again, or indeterminate.
   *
   * <p>Not a cheap predicate. This issues the same listCollections call the load path issues, and
   * it refreshes the recorded rejection reason as a side effect. Re-running the whole load path is
   * deliberate: two of the three {@link LeaseRepairUtil.CorruptionReason} values parse cleanly, so
   * a parse check alone would call those leases healthy and refuse to delete exactly the documents
   * this tool exists to clear.
   *
   * <p>The three outcomes are kept distinct so a transient Mongo failure during the re-check is
   * never read as corruption. A failed lookup returns {@link RepairClassification#UNDETERMINED},
   * which the caller declines rather than deleting a possibly-valid lease.
   */
  private RepairClassification classifyLeaseForRepair(String dbName, BsonDocument rawLease) {
    Lease parsed;
    try {
      parsed = Lease.fromBson(rawLease);
    } catch (BsonParseException e) {
      // A required field is missing or malformed: the load path can never accept this document.
      return RepairClassification.REJECTED_CORRUPT;
    }
    @Var Lease normalized = parsed;
    if (parsed.materializedViewCollectionMetadata().collectionUuid().equals(NIL)) {
      Optional<UUID> resolved;
      try {
        resolved =
            this.uuidResolver.resolve(
                dbName, parsed.materializedViewCollectionMetadata().collectionName());
      } catch (LeaseRepairUtil.LeaseRepairException e) {
        // The lookup itself failed. A brief Mongo hiccup must not be read as licence to delete a
        // valid lease, so fail closed and let the operator reapply once the cluster is healthy.
        LOG.atWarn()
            .addKeyValue("leaseId", parsed.id())
            .setCause(e)
            .log(
                "Could not resolve materialized-view UUID during repair re-check; cannot confirm"
                    + " corruption");
        return RepairClassification.UNDETERMINED;
      }
      if (resolved.isEmpty()) {
        // The collection genuinely does not exist, so the nil MV UUID can never be resolved.
        // The three-arg overload, because dbName is in hand. The mapping-based overload consults
        // leaseKeyToDatabase, which is empty for a never-registered lease, and would silently skip
        // the record or point it at the wrong database.
        recordCorruptLease(
            parsed.id(), dbName, LeaseRepairUtil.CorruptionReason.UNRESOLVABLE_MV_UUID);
        return RepairClassification.REJECTED_CORRUPT;
      }
      normalized = parsed.withResolvedMatViewUuid(resolved.get());
    }
    try {
      postRestoreSanitizeIfNeeded(normalized);
      return RepairClassification.USABLE;
    } catch (LeaseRepairUtil.LeaseRepairException e) {
      // Evidence versus unanswered question. A DEFINITIVE failure, such as an MV collection the
      // resolver positively established is absent, is the load path re-rejecting the document, so
      // the delete may proceed under the expiry and OCC guards. A LOOKUP_FAILED one taught us
      // nothing, so fail closed and let the operator reapply once the cluster is healthy.
      recordPostRestoreFailure(normalized, dbName, e);
      return e.kind() == LeaseRepairUtil.LeaseRepairException.Kind.DEFINITIVE
          ? RepairClassification.REJECTED_CORRUPT
          : RepairClassification.UNDETERMINED;
    }
  }

  /** The document's {@code leaseVersion}, when it holds a readable integer value. */
  private static Optional<Long> readLeaseVersion(BsonDocument rawLease) {
    BsonValue value = rawLease.get(Lease.Fields.LEASE_VERSION.getName());
    if (value == null) {
      return Optional.empty();
    }
    if (value.isInt64()) {
      return Optional.of(value.asInt64().getValue());
    }
    if (value.isInt32()) {
      return Optional.of((long) value.asInt32().getValue());
    }
    return Optional.empty();
  }

  /** The document's {@code leaseExpiration}, when it holds a readable date value. */
  private static Optional<Instant> readLeaseExpiration(BsonDocument rawLease) {
    BsonValue value = rawLease.get(Lease.Fields.LEASE_EXPIRATION.getName());
    if (value == null || !value.isDateTime()) {
      return Optional.empty();
    }
    return Optional.of(Instant.ofEpochMilli(value.asDateTime().getValue()));
  }

  /**
   * The delete filter for {@code leaseId}, pinned to the observed {@code leaseVersion} so a
   * concurrent clean rewrite vetoes the delete, mirroring {@link #deleteStaleLease}. When the
   * document is too corrupt to carry a readable version, falls back to an id-only delete: such a
   * document cannot be a live lease any host is renewing, and the tool exists to clear it.
   */
  private static Bson corruptLeaseDeleteFilter(String leaseId, BsonDocument rawLease) {
    Optional<Long> version = readLeaseVersion(rawLease);
    if (version.isEmpty()) {
      LOG.atWarn()
          .addKeyValue("leaseId", leaseId)
          .log("Corrupt lease has no readable leaseVersion; deleting without OCC protection");
      return Filters.eq("_id", leaseId);
    }
    return Filters.and(
        Filters.eq("_id", leaseId),
        Filters.eq(Lease.Fields.LEASE_VERSION.getName(), version.get()));
  }

  /**
   * Deletes lease {@code leaseId} when the load path still rejects it, clearing a document that
   * mongot skips on every pass and that therefore blocks lease creation for its materialized view.
   * The next {@code initializeLease} recreates the lease from scratch.
   *
   * <p>The document is re-read and re-checked immediately before deleting, because deleting a lease
   * that has since been rewritten cleanly would drop the fencing token that stops two hosts writing
   * the same materialized view. That re-check is the safety property, not any prior record: callers
   * name an explicit lease id, so this must not depend on a background sweep having happened to see
   * the document first.
   *
   * <p>Three guards protect the delete. The re-check must positively re-reject the document: a
   * transient lookup failure is treated as indeterminate and declines the delete rather than
   * risking a valid lease. The lease must have expired, so no other host can still hold it. And the
   * delete is pinned to the observed {@code leaseVersion}, so a clean rewrite that slips into the
   * window between the re-check and the delete bumps the version and vetoes it, the same OCC {@code
   * deleteStaleLease} uses. Only a document too corrupt to carry a readable version or expiration
   * falls back past those last two guards; such a document cannot be a live lease any host renews.
   *
   * @return true when a document was deleted.
   */
  @VisibleForTesting
  boolean deleteCorruptLease(String leaseId) {
    // locateLease is inside the try on purpose. It reads every candidate database, so it can
    // throw just as readily as the delete itself, and anything escaping this method reaches
    // pollFollowerStatuses, whose scheduler stops running it again after a throw.
    try {
      LeaseLocation located = locateLease(leaseId);
      if (located.database().isEmpty()) {
        if (located.searchIncomplete()) {
          // At least one database could not be read, so "not found" proves nothing. Decline
          // and keep any recorded rejection, matching how an unanswered re-check behaves.
          recordRepairOutcome(REPAIR_OUTCOME_INDETERMINATE);
          LOG.atWarn()
              .addKeyValue("leaseId", leaseId)
              .log(
                  "Could not search every known database for the lease document, refusing to"
                      + " conclude it is gone");
          return false;
        }
        this.corruptLeases.remove(leaseId);
        recordRepairOutcome(REPAIR_OUTCOME_ALREADY_GONE);
        LOG.atInfo()
            .addKeyValue("leaseId", leaseId)
            .log("No lease document under this id in any known database, nothing to delete");
        return false;
      }
      String dbName = located.database().get();
      BsonDocument rawLease =
          this.operationExecutor.execute(
              "readCorruptLease",
              () -> this.getCollection(dbName).find(Filters.eq("_id", leaseId)).first());
      if (rawLease == null) {
        this.corruptLeases.remove(leaseId);
        recordRepairOutcome(REPAIR_OUTCOME_ALREADY_GONE);
        LOG.atInfo()
            .addKeyValue("leaseId", leaseId)
            .log("Corrupt lease already gone, nothing to delete");
        return false;
      }
      RepairClassification classification = classifyLeaseForRepair(dbName, rawLease);
      if (classification == RepairClassification.USABLE) {
        this.corruptLeases.remove(leaseId);
        recordRepairOutcome(REPAIR_OUTCOME_REFUSED_STILL_USABLE);
        LOG.atWarn()
            .addKeyValue("leaseId", leaseId)
            .addKeyValue("hostname", this.hostname)
            .log("Lease loads cleanly at delete time, refusing to delete");
        return false;
      }
      if (classification == RepairClassification.UNDETERMINED) {
        // A lookup failed, so corruption cannot be confirmed. Leave any recorded reason in place
        // and decline; the operator can reapply once the cluster is healthy.
        recordRepairOutcome(REPAIR_OUTCOME_INDETERMINATE);
        LOG.atWarn()
            .addKeyValue("leaseId", leaseId)
            .addKeyValue("hostname", this.hostname)
            .log("Could not confirm the lease is corrupt at delete time, refusing to delete");
        return false;
      }
      Optional<Instant> expiration = readLeaseExpiration(rawLease);
      if (expiration.isPresent() && Instant.now().isBefore(expiration.get())) {
        // The lease has not expired, so another host may still hold it. Deleting now would let the
        // next initializeLease recreate the lease at version 1 while the old writer's cached copy
        // survives fencing. The runbook says wait for expiry; enforce it here.
        recordRepairOutcome(REPAIR_OUTCOME_REFUSED_NOT_EXPIRED);
        LOG.atWarn()
            .addKeyValue("leaseId", leaseId)
            .addKeyValue("hostname", this.hostname)
            .addKeyValue("leaseExpiration", expiration.get())
            .log("Lease has not expired yet, refusing to delete");
        return false;
      }
      Bson deleteFilter = corruptLeaseDeleteFilter(leaseId, rawLease);
      var result =
          this.operationExecutor.execute(
              "deleteCorruptLease", () -> this.getCollection(dbName).deleteOne(deleteFilter));
      if (result.getDeletedCount() == 0) {
        this.corruptLeases.remove(leaseId);
        recordRepairOutcome(REPAIR_OUTCOME_LOST_RACE);
        LOG.atInfo()
            .addKeyValue("leaseId", leaseId)
            .log(
                "Corrupt lease not deleted; another host removed it or rewrote it cleanly, bumping"
                    + " the leaseVersion the delete was pinned to");
        return false;
      }
      this.corruptLeases.remove(leaseId);
      // Drop the cached lease, but deliberately not the database mappings. A rejected lease is not
      // removed from this.leases by the sweep, so an entry loaded cleanly before the document went
      // bad can still be sitting there, and it must not outlive the document.
      //
      // evictCachedLease is not used here. It also clears both mappings, which is right for the
      // index-drop paths that call it because those retire the generation too. Repair leaves the
      // generation registered, and without a mapping getDatabaseForLease throws, so the follower
      // poll skips that key on every pass: the materialized view ends up with no leader rather
      // than unblocked, which is the opposite of what this command is for.
      this.leases.remove(leaseId);
      demoteLeadershipForLease(leaseId);
      recordRepairOutcome(REPAIR_OUTCOME_DELETED);
      LOG.atWarn()
          .addKeyValue("leaseId", leaseId)
          .addKeyValue("hostname", this.hostname)
          .addKeyValue("database", dbName)
          .log(
              "Deleted corrupt lease; the materialized view will be recreated from scratch on the"
                  + " next lease initialization");
      return true;
    } catch (Exception | AssertionError e) {
      // Best effort, matching every other lease-table op: the operator can reapply the command.
      recordRepairOutcome(REPAIR_OUTCOME_ERROR);
      LOG.atError()
          .addKeyValue("leaseId", leaseId)
          .addKeyValue("hostname", this.hostname)
          .setCause(e)
          .log("Failed to delete corrupt lease");
      return false;
    }
  }

  // Normalizes lease by populating missing mat view UUID field, this won't change lease
  // version since it's only meant for backward compatibility and lease state is unchanged.
  //
  // Null return means the MV UUID could not be resolved or validated, or that a needed post-restore
  // repair could not be completed. Either way the lease is left unacquirable.
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
        recordCorruptLease(lease.id(), LeaseRepairUtil.CorruptionReason.UNRESOLVABLE_MV_UUID);
        return null;
      }
    }
    try {
      return postRestoreSanitizeIfNeeded(normalized);
    } catch (LeaseRepairUtil.LeaseRepairException e) {
      // The load path draws no distinction between the two failure kinds: either way it cannot
      // verify the namespace, so the lease stays unacquirable and repair is retried on the next
      // load. Only the repair re-check acts on the kind.
      recordPostRestoreFailure(normalized, e);
      return null;
    }
  }

  // Sanitizes a stale flex to dedicated migration lease in-memory so downstream consumers see
  // cleared
  // commitInfo + refreshed UUIDs before any replication starts; the persistent rewrite happens via
  // the next leadership-acquisition replaceOne. Every outcome we can positively determine, whether
  // repaired, nothing to repair, or not a migration, returns a usable lease.
  //
  // Throws rather than returning null when repair was needed but could not be completed. The two
  // callers need the LeaseRepairException.Kind: the load path treats either kind the same and
  // leaves the lease unacquirable, while the repair re-check may only delete on a DEFINITIVE
  // failure. Both must call recordPostRestoreFailure on the way out.
  private Lease postRestoreSanitizeIfNeeded(Lease lease)
      throws LeaseRepairUtil.LeaseRepairException {
    var repairNeed =
        LeaseRepairUtil.classifyRepairNeed(
            lease,
            this.isDedicatedCluster,
            Optional.ofNullable(this.leaseKeyToSourceDatabase.get(lease.id())));
    if (repairNeed == LeaseRepairUtil.RepairNeed.NOT_A_MIGRATION) {
      this.postRestoreRepairSuppressedCounter.increment();
      LOG.atDebug()
          .addKeyValue("leaseId", lease.id())
          .addKeyValue("namespace", LeaseRepairUtil.extractNamespace(lease).orElse(null))
          .log("Tenant-prefixed namespace is a customer-named database; skipping repair");
      return lease;
    }
    if (repairNeed == LeaseRepairUtil.RepairNeed.NONE) {
      return lease;
    }
    Lease repaired =
        LeaseRepairUtil.repairLeaseForPostRestoreMigration(
            lease, this.uuidResolver, this.dbResolver.resolveDefault());
    if (repaired == lease) {
      // Stripped namespace absent, or name and both UUIDs already current: nothing to rewrite.
      this.postRestoreRepairSuppressedCounter.increment();
      logRepair(lease).log("Post-restore repair found nothing to fix; leaving lease unchanged");
      return lease;
    }
    this.postRestoreLeaseRepairSuccessCounter.increment();
    logRepair(lease)
        .addKeyValue("oldSourceUuid", lease.collectionUuid())
        .addKeyValue("newSourceUuid", repaired.collectionUuid())
        .addKeyValue("oldMvUuid", lease.materializedViewCollectionMetadata().collectionUuid())
        .addKeyValue("newMvUuid", repaired.materializedViewCollectionMetadata().collectionUuid())
        .log("Repaired post-restore stale lease (flex to dedicated migration artifact)");
    return repaired;
  }

  /**
   * Records a failed post-restore repair: bumps the failure counter, records the rejection reason,
   * and logs. Shared by the load path and the repair re-check so both leave the same trace.
   */
  private void recordPostRestoreFailure(Lease lease, LeaseRepairUtil.LeaseRepairException e) {
    recordPostRestoreFailure(
        lease, databaseForLease(lease.id(), "recording a post-restore failure").orElse(null), e);
  }

  /** As above, but records against {@code dbName} when the caller knows which database it read. */
  private void recordPostRestoreFailure(
      Lease lease, @Nullable String dbName, LeaseRepairUtil.LeaseRepairException e) {
    this.postRestoreLeaseRepairFailureCounter.increment();
    if (dbName != null) {
      recordCorruptLease(
          lease.id(), dbName, LeaseRepairUtil.CorruptionReason.POST_RESTORE_REPAIR_FAILED);
    }
    logRepair(lease)
        .addKeyValue("failureKind", e.kind())
        .setCause(e)
        .log(
            "Post-restore lease repair failed; dropping lease to prevent replication on stale"
                + " namespace");
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
    this.leaseKeyToSourceDatabase.put(leaseKey, indexGeneration.getDefinition().getDatabase());

    var lease = this.leases.get(leaseKey);
    if (lease != null) {
      // Lease exists in memory - only retain leadership while our ownership is still valid.
      boolean weOwnLease = this.hostname.equals(lease.leaseOwner());
      boolean leaseExpired = Instant.now().isAfter(lease.leaseExpiration());

      if (weOwnLease && !leaseExpired) {
        // We own the lease - this is likely an index definition update (e.g., filter field change).
        // Mark this generation LEADER so the generator can become leader immediately. This also
        // overwrites any FOLLOWER role: becomeFollower() can have set this generationId to
        // FOLLOWER moments earlier in the same refreshStatus() tick (e.g. a terminal-generator
        // demotion immediately followed by retryFailedGeneratorCreations() re-adding it here), and
        // leaving it as FOLLOWER after this point would make the next pollFollowerStatuses()
        // report this already-active lease as acquirable, causing refreshStatus()'s
        // acquirableLeases loop to redundantly reacquire leadership on the generator this call
        // just activated and trip the unsafe-reacquisition detector on a same-tenure,
        // non-corrupting reacquisition (CLOUDP-437181).
        //
        // Becoming leader again does not clear checkpoint risk; only a checkpoint write the
        // database accepts does. See LeaseManager#clearCheckpointCommitRisk.
        this.localRoles.setLeader(generationId);
        LOG.atInfo()
            .addKeyValue("generationId", generationId)
            .addKeyValue("leaseOwner", lease.leaseOwner())
            .addKeyValue("leaseExpiration", lease.leaseExpiration())
            .addKeyValue("hostname", this.hostname)
            .log("Starting as leader - we own the existing lease");
      } else {
        // add() runs again after replication restart, while the previous leader tracking remains.
        // Clear it so expired ownership must be reacquired through OCC before the generator writes.
        this.localRoles.setFollower(generationId);
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
      this.localRoles.setFollower(generationId);
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
    this.localRoles.forget(generationId);
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
    this.leaseKeyToSourceDatabase.remove(leaseKey);
  }

  @Override
  public boolean isLeader(MaterializedViewGenerationId generationId) {
    return this.localRoles.isLeader(generationId);
  }

  @Override
  public void releaseLeadership(MaterializedViewGenerationId generationId) {
    becomeFollower(generationId);
  }

  @Override
  public boolean supportsFencingRecovery() {
    return true;
  }

  /**
   * The fencing token and the tenure identity from one lease read, so a tenure pins a matched pair
   * (CLOUDP-437680). The cache can hold a lease owned by another host or an expired lease that
   * still names this host before the next heartbeat demotes it, so ownership and expiration are
   * checked against the same single read the tenure comes from.
   */
  @Override
  public OwnedTenure getOwnedTenure(MaterializedViewGenerationId generationId) {
    String leaseKey;
    try {
      leaseKey = getLeaseKey(generationId);
    } catch (Exception e) {
      LOG.atInfo()
          .addKeyValue("generationId", generationId)
          .addKeyValue("hostname", this.hostname)
          .setCause(e)
          .log("Refusing to hand out a tenure because generation metadata is unavailable");
      return new OwnedTenure(INVALID_LEASE_VERSION, INVALID_LEASE_VERSION);
    }
    Lease lease = this.leases.get(leaseKey);
    if (lease == null) {
      return new OwnedTenure(INVALID_LEASE_VERSION, INVALID_LEASE_VERSION);
    }
    if (!this.hostname.equals(lease.leaseOwner())) {
      LOG.atWarn()
          .addKeyValue("generationId", generationId)
          .addKeyValue("leaseOwner", lease.leaseOwner())
          .addKeyValue("hostname", this.hostname)
          .addKeyValue("leaseVersion", lease.leaseVersion())
          .addKeyValue("epoch", lease.epoch())
          .log("Refusing to hand out a tenure for a lease owned by another host");
      return new OwnedTenure(INVALID_LEASE_VERSION, INVALID_LEASE_VERSION);
    }
    Instant now = Instant.now();
    if (now.isAfter(lease.leaseExpiration())) {
      LOG.atWarn()
          .addKeyValue("generationId", generationId)
          .addKeyValue("leaseOwner", lease.leaseOwner())
          .addKeyValue("hostname", this.hostname)
          .addKeyValue("leaseVersion", lease.leaseVersion())
          .addKeyValue("epoch", lease.epoch())
          .addKeyValue("leaseExpiration", lease.leaseExpiration())
          .addKeyValue("now", now)
          .log("Refusing to hand out a tenure for an expired lease");
      return new OwnedTenure(INVALID_LEASE_VERSION, INVALID_LEASE_VERSION);
    }
    return new OwnedTenure(lease.leaseVersion(), lease.epoch());
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
    updateCommitInfo(generationId, encodedUserData, false);
  }

  @Override
  public void updateCommitInfo(
      MaterializedViewGenerationId generationId,
      EncodedUserData encodedUserData,
      boolean materializedViewWriteCompleted)
      throws MaterializedViewTransientException, MaterializedViewNonTransientException {
    // Only update the commit info in the database if leader.
    if (isLeader(generationId)) {
      @Var String leaseKey = null;
      try {
        leaseKey = getLeaseKey(generationId);
        ensureLeaseExists(leaseKey);
        Lease currentLease = this.leases.get(leaseKey);
        Lease updatedLease = currentLease.withUpdatedCheckpoint(encodedUserData);
        if (updateLeaseInDatabase(
            leaseKey, generationId, currentLease, updatedLease, encodedUserData)) {
          // The database took this write, so commitInfo now covers any earlier flagged writes.
          this.checkpointCommitRisk.clear(generationId);
        } else {
          // commit() has already run its bulkWrite, so MV documents may exist with no checkpoint.
          this.checkpointRejectedCounter.increment();
          this.checkpointCommitRisk.record(generationId, materializedViewWriteCompleted);
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
      this.checkpointCommitRisk.record(generationId, materializedViewWriteCompleted);
      LOG.atWarn()
          .addKeyValue("generationId", generationId)
          .addKeyValue("hostname", this.hostname)
          .log("Skipped checkpoint commit, no longer leader for this generation");
    }
  }

  /** Visible so tests can drive the TTL sweep without waiting on wall-clock time. */
  @VisibleForTesting
  void expireStaleCheckpointCommitRisk(Instant now) {
    this.checkpointCommitRisk.expireStale(now);
  }

  @Override
  public void clearCheckpointCommitRisk(MaterializedViewGenerationId generationId) {
    this.checkpointCommitRisk.clear(generationId);
  }

  @Override
  public void clearCheckpointCommitRiskForIndex(ObjectId indexId) {
    this.checkpointCommitRisk.clearForIndex(indexId);
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
  public LeaseManager.GcScanResult findGcCandidates() {
    Set<String> databases = knownInternalDatabases();

    List<Lease> leases = new ArrayList<>();
    @Var boolean complete = true;
    @Var boolean sawDocuments = false;
    Set<String> sweptDatabases = new HashSet<>();
    Set<String> seenLeaseIds = new HashSet<>();
    for (String dbName : databases) {
      try {
        List<BsonDocument> rawLeases =
            this.operationExecutor.execute(
                "findGcCandidates",
                () -> this.getCollection(dbName).find().into(new ArrayList<>()));
        sawDocuments |= !rawLeases.isEmpty();
        sweptDatabases.add(dbName);
        rawLeases.forEach(raw -> leaseIdOf(raw).ifPresent(seenLeaseIds::add));
        for (BsonDocument rawLease : rawLeases) {
          try {
            Lease parsed = parseLeaseOrThrow(rawLease, dbName);
            // Register the mapping (orphans have no generation to do it) for the mark/delete.
            this.leaseKeyToDatabase.putIfAbsent(parsed.id(), dbName);
            // Parsing is not the same as loading cleanly: UNRESOLVABLE_MV_UUID and
            // POST_RESTORE_REPAIR_FAILED both parse and are still rejected by the load path.
            // Retire the rejection only when the whole load path accepts the lease, the same gate
            // syncLeasesFromMongod uses. Otherwise this sweep silently un-records leases an
            // operator still needs to repair.
            //
            // Run the load path only for leases that actually have a record to retire. It issues
            // listCollections calls and bumps the post-restore counters and logs, and doing that
            // for every lease on every sweep would turn a bookkeeping check into per-sweep
            // network traffic and telemetry noise.
            if (this.corruptLeases.containsKey(parsed.id())
                && normalizeLeaseIfNeeded(parsed) != null) {
              this.corruptLeases.remove(parsed.id());
            }
            // The parsed lease stays a GC candidate either way, so the candidate list is unchanged.
            leases.add(parsed);
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
        // periodic sweep picks them up on a later tick. Reported as an incomplete scan so an empty
        // result is not mistaken for an empty lease table.
        complete = false;
        LOG.atWarn()
            .addKeyValue("database", dbName)
            .setCause(e)
            .log("Failed to scan database for GC leases");
      }
    }
    // Retire records for leases this sweep no longer sees. Only databases actually swept are
    // considered, so a database whose scan threw above keeps its records rather than losing them
    // to a transient read failure. This is the only path with a complete view of every database.
    this.corruptLeases
        .entrySet()
        .removeIf(
            entry ->
                sweptDatabases.contains(entry.getValue().database())
                    && !seenLeaseIds.contains(entry.getKey()));
    return new LeaseManager.GcScanResult(leases, complete, sawDocuments);
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
    Lease lease = parseLeaseOrThrow(rawLease, dbName);
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
    // version the sweep saw. The predicates are independent vetoes. leaseVersion fences a new
    // tenure reusing the lease -- the only write that bumps it. A renewal does not bump the
    // version when partial-update renewal is enabled (the flag-off renewal still does); it is
    // fenced instead by cleanupState and expiration, which a renewal does reset and extend.
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
        // needs clearing. The generation-keyed maps (localRoles,
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
    this.leaseKeyToSourceDatabase.remove(leaseKey);
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
    return this.localRoles.leaderIds();
  }

  @Override
  public Set<MaterializedViewGenerationId> getFollowerGenerationIds() {
    return this.localRoles.followerIds();
  }

  @Override
  public LeaseManager.FollowerPollResult pollFollowerStatuses() {
    Map<MaterializedViewGenerationId, IndexStatus> statuses = new HashMap<>();
    Map<MaterializedViewGenerationId, BsonTimestamp> replicationOpTimes = new HashMap<>();
    Set<MaterializedViewGenerationId> acquirableLeases = new HashSet<>();

    // Applied at the top of the poll, before the early return below. A lease that blocks lease
    // creation can leave this host with no registered generation at all, and that early return
    // fires exactly in that state, so anything placed after it would never run for the case the
    // tool exists to fix. This is the earliest point on a periodic path that always executes.
    executeOpsRepairLeaseCommand();

    if (!this.localRoles.hasAnyFollower()) {
      LOG.atDebug()
          .addKeyValue("followerCount", 0)
          .addKeyValue("hostname", this.hostname)
          .log("Polling follower statuses");
      return new LeaseManager.FollowerPollResult(statuses, replicationOpTimes, acquirableLeases);
    }

    Set<MaterializedViewGenerationId> followerGenerationIds = this.localRoles.followerIds();
    LOG.atDebug()
        .addKeyValue("followerCount", followerGenerationIds.size())
        .addKeyValue("hostname", this.hostname)
        .log("Polling follower statuses");

    // Build a mapping from MaterializedViewGenerationID to LeaseKey for efficient lookup after
    // batch fetch.
    Map<MaterializedViewGenerationId, String> generationIdToLeaseKey = new HashMap<>();
    List<String> leaseKeys = new ArrayList<>();
    Set<MaterializedViewGenerationId> removedDuringPoll = new HashSet<>();
    for (MaterializedViewGenerationId generationId : new ArrayList<>(followerGenerationIds)) {
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
        // A key we asked for and did not get back no longer exists in this database, so retire a
        // rejection recorded against this database. The record's database must match the one just
        // read: a record naming another database says nothing was learned about it here, and
        // retiring it would forget a document that may still exist. A rejected lease is still
        // returned by the query (it exists, it just fails to load), so this cannot retire a record
        // that is still valid. findGcCandidates applies the same swept-database guard.
        Set<String> returnedIds = new HashSet<>();
        rawLeases.forEach(raw -> leaseIdOf(raw).ifPresent(returnedIds::add));
        keys.stream()
            .filter(key -> !returnedIds.contains(key))
            .forEach(
                key ->
                    this.corruptLeases.computeIfPresent(
                        key, (id, record) -> record.database().equals(dbName) ? null : record));
        for (BsonDocument rawLease : rawLeases) {
          try {
            Lease parsed = parseLeaseOrThrow(rawLease, dbName);
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

    // Process each follower generation ID. Deliberately rescans localRoles instead of reusing the
    // followerGenerationIds snapshot taken above: a generation promoted to leader during the batch
    // fetch above (e.g. via add() or acquireExistingLease()) must not be reprocessed here as a
    // follower, or it can be reported acquirable and trigger a redundant, CLOUDP-437181-style
    // reacquisition of a lease we already hold.
    Instant now = Instant.now();
    for (MaterializedViewGenerationId generationId :
        new ArrayList<>(this.localRoles.followerIds())) {
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
    // Followers too: a flag left behind by another host taking over sits on a follower.
    this.checkpointCommitRisk.expireStale(Instant.now());
    // For leaders: renew the lease to maintain leadership.
    // For followers: do nothing - leadership acquisition is handled separately via
    // tryAcquireLeadership() called by MaterializedViewManager.
    Set<MaterializedViewGenerationId> leaderGenerationIds = this.localRoles.leaderIds();
    int leaderCount = leaderGenerationIds.size();
    if (leaderCount > 0) {
      LOG.atDebug()
          .addKeyValue("leaderCount", leaderCount)
          .addKeyValue("hostname", this.hostname)
          .log("Heartbeat - renewing leases for leader generations");
    }
    List<MaterializedViewGenerationId> renewedGenerationIds = new ArrayList<>();
    List<MaterializedViewGenerationId> failedGenerationIds = new ArrayList<>();
    for (MaterializedViewGenerationId generationId : new ArrayList<>(leaderGenerationIds)) {
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
          inMemoryLease.withNewTenure(this.hostname).withRecordedIndexDefinitionVersion(versionKey);
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
    // Must precede refreshLeaseFromDatabase below, whose load consults this mapping.
    this.leaseKeyToSourceDatabase.put(
        proposedMetadata.collectionName(),
        indexDefinitionGeneration.getIndexDefinition().getDatabase());
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
  Map<String, CorruptLease> getCorruptLeases() {
    return ImmutableMap.copyOf(this.corruptLeases);
  }

  /**
   * A lease the load path rejected: which database holds it, and which check rejected it. The
   * database is carried because leases live in per-tenant internal databases on a multi-tenant
   * cluster, so an operator action cannot assume the default one.
   */
  record CorruptLease(String database, LeaseRepairUtil.CorruptionReason reason) {}

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
      // Don't add, remove, or otherwise touch generationId's role in this.localRoles, which may
      // break MaterializedViewManager when it's refreshing status by looking up
      // MaterializedViewCollectionMetadataCatalog.
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
            generationId,
            currentLease.leaseVersion(),
            newLease.leaseVersion(),
            !isReclaimingOwnLease);

    var result =
        this.operationExecutor.execute(
            "acquireLease",
            () ->
                this.getCollection(getDatabaseForLease(getLeaseKey(generationId)))
                    .replaceOne(filter, newLease.toBson()));

    if (result.getMatchedCount() > 0) {
      this.leases.put(getLeaseKey(generationId), newLease);
      this.localRoles.setLeader(generationId);
      if (isReclaimingOwnLease) {
        // Taking the lease back does not clear checkpoint risk: withNewTenure() copies commitInfo
        // over unchanged. The next commit clears it, even an empty one.
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
   * Creates a filter for acquiring an existing lease (expired, or owned by us after restart). This
   * filter does NOT include an ownership check because we're intentionally taking over from another
   * owner or reclaiming our own lease.
   *
   * <p>The filter matches either:
   *
   * <ul>
   *   <li>The expected version (normal case), OR
   *   <li>The new version with our hostname (idempotent case - we already wrote but didn't get the
   *       response)
   * </ul>
   *
   * <p>When {@code requireLeaseExpired} is true (a takeover of another host's lease), the normal
   * case also requires the lease to still be expired at write time. With partial-update renewal
   * enabled, the heartbeat no longer bumps {@code leaseVersion}, so fencing a takeover on version
   * alone would let an acquirer that read an expired lease at version {@code V} win after the
   * owner's heartbeat re-extended it (which leaves it at {@code V}) — stealing a lease its owner
   * just renewed. Requiring expiry re-closes that fence regardless of version. Own-lease reclaims
   * do not require expiry, because a host may reclaim its own still-valid lease after restart.
   *
   * @param generationId the generation ID for the lease
   * @param currentLeaseVersion the expected current lease version
   * @param newLeaseVersion the new lease version after update
   * @param requireLeaseExpired whether the normal branch must also match a still-expired lease
   */
  private Bson createAcquireLeaseFilter(
      MaterializedViewGenerationId generationId,
      long currentLeaseVersion,
      long newLeaseVersion,
      boolean requireLeaseExpired) {
    // Normal case: version matches expected (we're taking over).
    @Var Bson normalMatch = Filters.eq(Lease.Fields.LEASE_VERSION.getName(), currentLeaseVersion);
    if (requireLeaseExpired) {
      normalMatch =
          Filters.and(
              normalMatch,
              Filters.lt(
                  Lease.Fields.LEASE_EXPIRATION.getName(),
                  new BsonDateTime(Instant.now().toEpochMilli())));
    }
    return Filters.and(
        Filters.eq("_id", getLeaseKey(generationId)),
        Filters.or(
            normalMatch,
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
   * <p>The epoch term means an owned update never applies across an ownership change, including the
   * same-host reclaim that the version arms cannot tell apart: a reclaim bumps the version by one
   * and mints a new epoch, so a write whose snapshot predates the reclaim would otherwise still
   * match the new-version arm and put the old epoch back. The absent-field arm keeps leases last
   * written by a pre-epoch binary writable: {@link Lease#fromBson} defaults a missing epoch to that
   * document's lease version, and the arm is only ever ANDed with the version OCC above, never used
   * alone.
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
   * @param currentEpoch the tenure epoch of the snapshot this write was built from
   */
  private Bson createUpdateFilterForOwnedLease(
      String leaseKey, long currentLeaseVersion, long newLeaseVersion, long currentEpoch) {
    return Filters.and(
        Filters.eq("_id", leaseKey),
        Filters.eq(Lease.Fields.LEASE_OWNER.getName(), this.hostname),
        Filters.or(
            // Normal case: version matches expected
            Filters.eq(Lease.Fields.LEASE_VERSION.getName(), currentLeaseVersion),
            // Idempotent case: we already wrote but didn't get a response
            Filters.eq(Lease.Fields.LEASE_VERSION.getName(), newLeaseVersion)),
        Filters.or(
            Filters.eq(Lease.Fields.EPOCH.getName(), currentEpoch),
            Filters.exists(Lease.Fields.EPOCH.getName(), false)));
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
   *   "leaseVersion": 5,                          // unchanged
   *   "commitInfo": "checkpoint_v5",              // preserved
   *   "cleanupState": "NOT_ELIGIBLE",             // reset
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

      // Renew the lease. CLOUDP-449935 splits this into two shapes, chosen per heartbeat tick (via
      // the BooleanSupplier) so a flag flip needs no restart:
      //   - off (default): master's whole-document replaceOne with the owned-lease version fence.
      //   - on: field-scoped updateOne ($max on leaseExpiration, $set on cleanupState), no version
      //     predicate, no commitInfo write, and a cache merge instead of a whole-snapshot put.
      if (this.leaseHeartbeatPartialUpdateEnabled.getAsBoolean()) {
        // Renew by $max on leaseExpiration and $set on GC eligibility only. This must not be a
        // whole-document replace: replaceOne would carry this host's cached commitInfo and bump
        // leaseVersion, either of which can race a concurrent checkpoint on the same cached version
        // and cause the CLOUDP-397647 split-brain (a heartbeat write before the checkpoint's makes
        // the checkpoint's OCC filter miss and demote a live leader; one after it rewinds the
        // committed commitInfo). The heartbeat only extends the lease; it must not rewrite anything
        // the checkpoint owns. $max, not $set, so the heartbeat never moves expiration backwards
        // relative to a concurrent writer.
        Instant newExpiration = now.plusMillis(Lease.LEASE_EXPIRATION_MS);
        var update =
            Updates.combine(
                Updates.max(
                    Lease.Fields.LEASE_EXPIRATION.getName(),
                    new BsonDateTime(newExpiration.toEpochMilli())),
                Updates.set(
                    Lease.Fields.CLEANUP_STATE.getName(), Lease.CleanupState.NOT_ELIGIBLE.name()));
        // No leaseVersion predicate: adding one reintroduces the heartbeat-loses-first interleaving
        // (a checkpoint bumping the version in-between would make this miss and demote a live
        // leader). The _id + owner filter is sufficient — only a takeover by another host can make
        // a renewal fail, and the $max/$set are idempotent on retry, so the {N, N+1} clause is
        // unneeded.
        var filter =
            Filters.and(
                Filters.eq("_id", leaseKey),
                Filters.eq(Lease.Fields.LEASE_OWNER.getName(), this.hostname));

        var result =
            this.operationExecutor.execute(
                "renewLease",
                () ->
                    this.getCollection(getDatabaseForLease(getLeaseKey(generationId)))
                        .updateOne(filter, update));

        // updateOne reached the server (committed or OCC loss), so the store is reachable.
        this.leaseStoreCircuitBreaker.recordStoreReachable();
        if (result.getMatchedCount() > 0) {
          // Successfully renewed. Merge onto the cached lease rather than overwrite it: a
          // checkpoint may have advanced the cached version/commitInfo since this tick read, and
          // putting this snapshot back would roll the cache backward and OCC-fail that
          // checkpoint's next write.
          // Extend the cached expiration only when the cache is still the lease this tick read:
          // once a checkpoint's replace has landed, the database holds its earlier build-time
          // expiration, and trusting a later one would leave the cache ahead of the database -- the
          // renewal-window skip reads this cache, so the store could go unrenewed while the lease
          // is takeover-able.
          this.leases.computeIfPresent(
              leaseKey,
              (key, cachedLease) ->
                  cachedLease.leaseVersion() == currentLease.leaseVersion()
                      ? cachedLease.withRenewedExpiration(newExpiration)
                      : cachedLease);
          return true;
        } else {
          // Lease was taken by another instance - lost leadership.
          LOG.warn(
              "Lease renewal failed for {} - lease was updated by another instance", generationId);
          refreshLeaseFromDatabase(leaseKey);
          becomeFollower(generationId);
          return false;
        }
      } else {
        // Flag off: master's renewal, byte for byte. Whole-document replaceOne fenced on the owned
        // lease's version.
        Lease renewedLease = currentLease.withRenewedOwnership();
        var filter =
            createUpdateFilterForOwnedLease(
                leaseKey,
                currentLease.leaseVersion(),
                renewedLease.leaseVersion(),
                currentLease.epoch());

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

  /**
   * Demotes this host if it still holds leadership for {@code leaseId}, whose document has just
   * been deleted. Leaving a stale LEADER role would keep the generation on the leader path with no
   * lease behind it.
   *
   * <p>Scans the leader set rather than using a reverse index, since there is none and leaders are
   * few. A generation whose metadata has already gone has nothing to demote.
   */
  private void demoteLeadershipForLease(String leaseId) {
    for (MaterializedViewGenerationId generationId : this.localRoles.leaderIds()) {
      try {
        if (getLeaseKey(generationId).equals(leaseId)) {
          becomeFollower(generationId);
          return;
        }
      } catch (IllegalStateException e) {
        // Metadata already removed for this generation, so there is no lease key to match.
      }
    }
  }

  /** Transitions a generation ID from leader to follower state. */
  private void becomeFollower(MaterializedViewGenerationId generationId) {
    // The leader-check, lease-check, and role write all happen inside one computeRole() call so
    // the whole transition is atomic. Splitting it into a separate remove-then-put (as a plain
    // remove()/put() pair would) leaves a window where a concurrent add() or acquireExistingLease()
    // can reclaim leadership in between, only for this method's follow-up put() to silently
    // clobber that reclaimed LEADER role back to FOLLOWER.
    this.localRoles.computeRole(
        generationId,
        (id, currentRole) -> {
          if (currentRole != LocalLeaseRole.LEADER) {
            // Not currently leader (already follower, untracked, or a concurrent operation already
            // changed the role) - nothing to demote.
            return currentRole;
          }
          // Only set FOLLOWER if we're still managing this generation.
          // If the lease was removed by drop(), we must not re-add an entry for this generation.
          // This prevents a race condition where drop() removes the generation's entry entirely,
          // but a concurrent renewLease() or updateLeaseInDatabase() call re-adds it as follower.
          try {
            if (this.leases.containsKey(getLeaseKey(id))) {
              LOG.atInfo()
                  .addKeyValue("generationId", id)
                  .addKeyValue("hostname", this.hostname)
                  .log("Transitioned from leader to follower");
              return LocalLeaseRole.FOLLOWER;
            } else {
              LOG.atInfo()
                  .addKeyValue("generationId", id)
                  .addKeyValue("hostname", this.hostname)
                  .log("Not transitioning to follower - lease was removed by drop()");
              return null;
            }
          } catch (IllegalStateException e) {
            // Index was deleted concurrently - metadata is gone. Don't add to followers.
            LOG.warn(
                "Metadata removed for generation {} during becomeFollower, skipping follower"
                    + " transition",
                id,
                e);
            this.matViewGenerationIdToDefinitionVersion.remove(id);
            return null;
          }
        });
  }

  private void ensureLeaseExists(String leaseKey) {
    if (!this.leases.containsKey(leaseKey)) {
      throw new IllegalStateException("Lease does not exist for " + leaseKey);
    }
  }

  /**
   * Cleans up all per-generation state after metadata has been concurrently removed. Removes the
   * generation's local role entry, definition version mapping, and optionally the in-memory lease
   * cache.
   *
   * @param generationId the generation whose state should be purged
   * @param leaseKey the resolved lease key, or {@code null} if it was not resolved before the
   *     failure (e.g. {@code getLeaseKey} itself threw)
   */
  private void cleanupAfterMetadataLoss(
      MaterializedViewGenerationId generationId, @Nullable String leaseKey) {
    this.localRoles.forget(generationId);
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
    String dbName = getDatabaseForLease(collectionName);
    BsonDocument rawLease =
        this.operationExecutor.execute(
            "getLease",
            () -> this.getCollection(dbName).find(new Document("_id", collectionName)).first());
    if (rawLease == null) {
      return null;
    }
    Lease lease = normalizeLeaseIfNeeded(parseLeaseOrThrow(rawLease, dbName));
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
              leaseKey,
              currentLease.leaseVersion(),
              updatedLease.leaseVersion(),
              currentLease.epoch());
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
      // getCollectionInfos, not getCollectionInfo: the latter's orElseThrow collapses "namespace
      // absent" into the same exception as "lookup failed", and repair must tell those apart.
      Optional<MongoDbCollectionInfo> info;
      try {
        info =
            getCollectionInfos(
                    Check.isPresent(
                        this.autoEmbeddingMongoClient.getLeaseManagerMongoClient(),
                        "leaseManagerMongoClient"),
                    db)
                .getCollectionInfo(db, coll);
      } catch (Throwable t) {
        // Broad on purpose: repair must never crash leadership acquisition.
        throw new LeaseRepairUtil.LeaseRepairException(
            LeaseRepairUtil.LeaseRepairException.Kind.LOOKUP_FAILED,
            "failed to resolve UUID for " + db + "." + coll,
            t);
      }
      if (info.isEmpty()) {
        // Absent database or collection: listCollections on an unknown database is empty, not an
        // error, so both land here.
        return Optional.empty();
      }
      if (info.get() instanceof MongoDbCollectionInfo.Collection collection) {
        return Optional.of(collection.info().uuid());
      }
      // LOOKUP_FAILED rather than DEFINITIVE. The namespace resolving to something other than a
      // collection is deterministic, but it is an unexpected shape rather than a known corruption,
      // so the repair tool stays fail-closed on it and declines to delete.
      throw new LeaseRepairUtil.LeaseRepairException(
          LeaseRepairUtil.LeaseRepairException.Kind.LOOKUP_FAILED,
          "expected collection but found "
              + info.get().getClass().getSimpleName()
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
