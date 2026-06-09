package com.xgen.mongot.embedding.mongodb.leasing;

import static com.xgen.mongot.util.Uuids.NIL;
import static java.time.temporal.ChronoUnit.SECONDS;

import com.google.common.annotations.VisibleForTesting;
import com.google.errorprone.annotations.Var;
import com.mongodb.lang.NonNull;
import com.mongodb.lang.Nullable;
import com.xgen.mongot.embedding.config.MaterializedViewCollectionMetadata;
import com.xgen.mongot.index.EncodedUserData;
import com.xgen.mongot.index.status.IndexStatus;
import com.xgen.mongot.index.version.IndexFormatVersion;
import com.xgen.mongot.replication.mongodb.common.BufferlessIdOrderInitialSyncResumeInfo;
import com.xgen.mongot.replication.mongodb.common.IndexCommitUserData;
import com.xgen.mongot.replication.mongodb.common.InitialSyncResumeInfo;
import com.xgen.mongot.replication.mongodb.common.ResumeTokenUtils;
import com.xgen.mongot.util.BsonUtils;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonDocumentParser;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import com.xgen.mongot.util.bson.parser.Value;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import org.apache.commons.codec.DecoderException;
import org.bson.BsonDateTime;
import org.bson.BsonDocument;
import org.bson.BsonTimestamp;
import org.bson.codecs.pojo.annotations.BsonId;

/**
 * Represents a lease for the materialized view. A sample document looks like:
 *
 * <pre>
 * {
 *   "_id": "647343c92726405e854130e4",
 *   "schemaVersion": 1,
 *   "collectionUuid": "eb6c40ca-f25e-47e8-b48c-02a05b64a5aa",
 *   "collectionName": "myCollection",
 *   "leaseOwner": "host1",
 *   "leaseExpiration": "2025-12-06T00:37:15.661+00:00",
 *   "leaseVersion": 100,
 *   "commitInfo": "{"backendIndexVersion": {"$numberInt": "6"}, "initialSyncResumeInfo":
 *          {"bufferlessInitialSync": {"highWaterMark": {"$numberLong": "0"},
 *          "lastScannedId": {"$numberLong": "10000"}}}}",
 *   "latestIndexDefinitionVersion": "1",
 *   "indexDefinitionVersionStatusMap": {
 *     "0": {"isQueryable": true, "indexStatusCode": "STEADY"},
 *     "1": {"isQueryable": false, "indexStatusCode":
 *              "INITIAL_SYNC"}
 *     ...
 *    },
 *    "materializedViewCollectionMetadata": {
 *      "collectionUuid": "550e8400-e29b-41d4-a716-446655440000",
 *      "collectionName": "test-collection",
 *      "materializedViewMetadata": {
 *        "schemaFieldMapping": {"title": "_autoEmbed.title"},
 *        "mvMetadataSchemaVersion": 1
 *      }
 *     },
 *     "steadyAsOfOplogPosition": {"$timestamp": {"t": 1702857600, "i": 1}},
 *     "cleanupState": "NOT_ELIGIBLE"
 *   }
 * </pre>
 */
public record Lease(
    @BsonId String id,
    long schemaVersion,
    String collectionUuid,
    String collectionName,
    String leaseOwner,
    Instant leaseExpiration,
    long leaseVersion,
    String commitInfo,
    String latestIndexDefinitionVersion,
    Map<String, IndexDefinitionVersionStatus> indexDefinitionVersionStatusMap,
    @NonNull MaterializedViewCollectionMetadata materializedViewCollectionMetadata,
    @Nullable BsonTimestamp steadyAsOfOplogPosition,
    @NonNull CleanupState cleanupState)
    implements DocumentEncodable {

  /**
   * Compact constructor. Enforces non-null {@code cleanupState} so {@link #toBson()} always writes
   * a real value — the persistent contract is that every serialized lease document includes the
   * cleanupState field, which is what makes the "free backfill on write" property work for legacy
   * documents that pre-date the field. (Reads already default to {@code NOT_ELIGIBLE} via the
   * field's {@code withDefault}; this guards in-code construction.) A null would surface as a
   * runtime NPE deep inside the BSON writer; catch it at construction instead.
   *
   * <p>The {@link com.mongodb.lang.NonNull} annotation on the record component is advisory only;
   * Java records do not enforce it.
   */
  public Lease {
    Objects.requireNonNull(cleanupState, "cleanupState must not be null");
  }

  public static final long SCHEMA_VERSION = 1L;

  @VisibleForTesting static final long FIRST_LEASE_VERSION = 1L;

  /** Lease expiration time in milliseconds (5 minutes). */
  public static final long LEASE_EXPIRATION_MS = 300000L;

  /**
   * GC cleanup state of the lease (and its 1:1 materialized view collection). {@link
   * CleanupState#NOT_ELIGIBLE} is the steady state; {@link CleanupState#ELIGIBLE_FOR_CLEANUP} is a
   * one-way terminal state set by the stale-lease scanner when a superseded lease's heartbeat has
   * gone stale beyond the cleanup window. The scanner subsequently drops the materialized view
   * collection and the lease document. It is named for its narrow GC scope — it is not a general
   * lease lifecycle status.
   *
   * <p>This field was added after the lease document was already in use in production. For
   * backwards compatibility, documents missing this field are treated as {@code NOT_ELIGIBLE} on
   * read (see {@link #fromBson(DocumentParser)}). The schema is otherwise unchanged, so {@link
   * #SCHEMA_VERSION} is not bumped.
   */
  public enum CleanupState {
    NOT_ELIGIBLE,
    ELIGIBLE_FOR_CLEANUP
  }

  static class Fields {
    /**
     * The unique identifier for the lease. For now, we maintain one lease per mat view collection
     * and hence the id is the same as the mat view collection name (which in turn is the index ID).
     */
    public static final Field.Required<String> _ID = Field.builder("_id").stringField().required();

    /**
     * The schema version of the lease document. We currently only have one version, but this should
     * be incremented (and appropriate deserialization logic added) if we make any backwards
     * incompatible changes to the lease document.
     */
    public static final Field.Required<Long> SCHEMA_VERSION =
        Field.builder("schemaVersion").longField().required();

    /** The UUID of the collection that the index is on. */
    public static final Field.Required<String> COLLECTION_UUID =
        Field.builder("collectionUuid").stringField().required();

    /** The name of the collection that the index is on. */
    public static final Field.Required<String> COLLECTION_NAME =
        Field.builder("collectionName").stringField().required();

    /** The hostname of the mongot process that owns the lease. */
    public static final Field.Required<String> LEASE_OWNER =
        Field.builder("leaseOwner").stringField().required();

    /** The expiration time of the lease. */
    public static final Field.Required<BsonDateTime> LEASE_EXPIRATION =
        Field.builder("leaseExpiration").bsonDateTimeField().required();

    /** The version of the lease. Expected to be monotonically increasing. */
    public static final Field.Required<Long> LEASE_VERSION =
        Field.builder("leaseVersion").longField().required();

    /**
     * The commit info (replication checkpoint state) for the index. This is represented as a
     * string-ified version of @{link EncodedUserData} instead of a sub-document since some of the
     * replication code currently relies on this format. See {@link
     * com.xgen.mongot.replication.mongodb.common.IndexCommitUserData} to see what is stored in the
     * commit info.
     */
    public static final Field.Required<String> COMMIT_INFO =
        Field.builder("commitInfo").stringField().required();

    /**
     * The latest index definition version that is currently being replicated and managed by this
     * lease.
     */
    public static final Field.Required<String> LATEST_INDEX_DEFINITION_VERSION =
        Field.builder("latestIndexDefinitionVersion").stringField().required();

    /**
     * A map of index definition versions to their status. The key is the index definition version
     * and the value is an IndexDefinitionVersionStatus object corresponding to the status of that
     * index definition version.
     *
     * <p>Values use lenient parsing ({@code allowUnknownFields}) so an older binary can load
     * documents written by a newer binary during rollback or rolling updates. Strict parsing here
     * would throw {@link com.xgen.mongot.util.bson.parser.BsonParseException} on any unknown field
     * and break lease loading for the affected index. See CLOUDP-401373.
     */
    public static final Field.Required<Map<String, IndexDefinitionVersionStatus>>
        INDEX_DEFINITION_VERSION_STATUS_MAP =
            Field.builder("indexDefinitionVersionStatusMap")
                .mapOf(
                    Value.builder()
                        .classValue(IndexDefinitionVersionStatus::fromBson)
                        .allowUnknownFields()
                        .required())
                .required();

    public static final Field.Optional<MaterializedViewCollectionMetadata>
        MATERIALIZED_VIEW_COLLECTION_METADATA =
            Field.builder("materializedViewCollectionMetadata")
                .classField(MaterializedViewCollectionMetadata::fromBson)
                .allowUnknownFields()
                .optional()
                .noDefault();

    /**
     * The oplog position when the leader's MV transitioned to STEADY state. Used by followers to
     * detect MV replication lag. Null if MV has never reached STEADY or for legacy documents.
     */
    public static final Field.Optional<BsonTimestamp> STEADY_AS_OF_OPLOG_POSITION =
        Field.builder("steadyAsOfOplogPosition").bsonTimestampField().optional().noDefault();

    /**
     * GC cleanup state of the lease. Two layers of rollback/upgrade tolerance, both falling back to
     * {@link CleanupState#NOT_ELIGIBLE} (the benign state that never triggers MV deletion):
     *
     * <ul>
     *   <li>{@code withDefault} — the field is <em>absent</em> (a legacy lease written before this
     *       field existed).
     *   <li>{@code withFallback} — the field is <em>present but holds an unrecognized value</em> (a
     *       newer binary wrote a {@code CleanupState} this binary doesn't know, during a rollback
     *       or rolling upgrade). Without this, the parse would throw and break {@code
     *       syncLeasesFromMongod} for the index — the rollback-safety failure mode tracked in
     *       CLOUDP-401373.
     * </ul>
     *
     * <p>See {@link CleanupState} for the state machine.
     */
    public static final Field.WithDefault<CleanupState> CLEANUP_STATE =
        Field.builder("cleanupState")
            .enumField(CleanupState.class)
            .withFallback(CleanupState.NOT_ELIGIBLE)
            .asUpperUnderscore()
            .optional()
            .withDefault(CleanupState.NOT_ELIGIBLE);
  }

  public static Lease fromBson(DocumentParser parser) throws BsonParseException {
    return new Lease(
        parser.getField(Fields._ID).unwrap(),
        parser.getField(Fields.SCHEMA_VERSION).unwrap(),
        parser.getField(Fields.COLLECTION_UUID).unwrap(),
        parser.getField(Fields.COLLECTION_NAME).unwrap(),
        parser.getField(Fields.LEASE_OWNER).unwrap(),
        Instant.ofEpochMilli(parser.getField(Fields.LEASE_EXPIRATION).unwrap().getValue()),
        parser.getField(Fields.LEASE_VERSION).unwrap(),
        parser.getField(Fields.COMMIT_INFO).unwrap(),
        parser.getField(Fields.LATEST_INDEX_DEFINITION_VERSION).unwrap(),
        parser.getField(Fields.INDEX_DEFINITION_VERSION_STATUS_MAP).unwrap(),
        parser
            .getField(Fields.MATERIALIZED_VIEW_COLLECTION_METADATA)
            .unwrap()
            // Fallback metadata for community users.
            .orElse(
                new MaterializedViewCollectionMetadata(
                    MaterializedViewCollectionMetadata.MaterializedViewSchemaMetadata.VERSION_ZERO,
                    // Community users don't have this field, we need to have a UUID.NIL here.
                    NIL,
                    // _id should be same as MatView collection name.
                    parser.getField(Fields._ID).unwrap())),
        parser.getField(Fields.STEADY_AS_OF_OPLOG_POSITION).unwrap().orElse(null),
        // Backwards-compat: leases predating the cleanupState field read as NOT_ELIGIBLE via the
        // field's withDefault. Applied on read (not via normalize-on-startup) so every in-memory
        // Lease is complete; durable upgrade happens the next time the document is written.
        parser.getField(Fields.CLEANUP_STATE).unwrap());
  }

  public static Lease fromBson(BsonDocument bsonDocument) throws BsonParseException {
    try (var parser = BsonDocumentParser.fromRoot(bsonDocument).allowUnknownFields(true).build()) {
      return fromBson(parser);
    }
  }

  public record IndexDefinitionVersionStatus(
      boolean isQueryable, IndexStatus.StatusCode indexStatusCode) implements DocumentEncodable {
    public static IndexDefinitionVersionStatus fromBson(DocumentParser parser)
        throws BsonParseException {
      return new IndexDefinitionVersionStatus(
          parser.getField(Fields.IS_QUERYABLE).unwrap(),
          parser.getField(Fields.INDEX_STATUS).unwrap());
    }

    @Override
    public BsonDocument toBson() {
      return BsonDocumentBuilder.builder()
          .field(Fields.IS_QUERYABLE, this.isQueryable)
          .field(Fields.INDEX_STATUS, this.indexStatusCode)
          .build();
    }

    private static class Fields {
      /**
       * Whether the index definition version is queryable. This field durably tracks the
       * transition from building to queryable. Once it becomes queryable, it always remains so
       * (one-way ratchet). A new version may inherit queryable=true from the previous version
       * when {@code skipInitialSync=true} (Lucene-only or same-version change), since the MV
       * data is preserved and the index can continue serving queries.
       */
      public static final Field.Required<Boolean> IS_QUERYABLE =
          Field.builder("isQueryable").booleanField().required();

      /**
       * The status code of the index definition version.
       *
       * <p>Uses {@code UNKNOWN} as a fallback so an older binary can tolerate a newer {@code
       * StatusCode} value written by a newer binary during rollback or rolling updates. Without
       * the fallback, an unrecognized status string would throw {@link
       * com.xgen.mongot.util.bson.parser.BsonParseException} and break lease loading for the
       * affected index. See CLOUDP-401373.
       */
      public static final Field.Required<IndexStatus.StatusCode> INDEX_STATUS =
          Field.builder("indexStatusCode")
              .enumField(IndexStatus.StatusCode.class)
              .withFallback(IndexStatus.StatusCode.UNKNOWN)
              .asUpperUnderscore()
              .required();
    }
  }

  /**
   * Creates an initial Lease for synchronization purposes, this is acquirable by any other
   * instance.
   */
  public static Lease initialLease(
      String leaseId,
      UUID collectionUuid,
      String collectionName,
      String indexDefinitionVersion,
      IndexStatus initialIndexStatus,
      MaterializedViewCollectionMetadata materializedViewCollectionMetadata) {
    return new Lease(
        leaseId,
        SCHEMA_VERSION,
        collectionUuid.toString(),
        collectionName,
        "",
        Instant.now().minus(1, SECONDS),
        FIRST_LEASE_VERSION,
        EncodedUserData.EMPTY.asString(),
        indexDefinitionVersion,
        Map.of(
            indexDefinitionVersion,
            new IndexDefinitionVersionStatus(false, initialIndexStatus.getStatusCode())),
        materializedViewCollectionMetadata,
        null,
        CleanupState.NOT_ELIGIBLE);
  }

  public static Lease newLease(
      String leaseId,
      UUID collectionUuid,
      String collectionName,
      String leaseOwner,
      String indexDefinitionVersion,
      IndexStatus initialIndexStatus,
      MaterializedViewCollectionMetadata materializedViewCollectionMetadata) {
    return new Lease(
        leaseId,
        SCHEMA_VERSION,
        collectionUuid.toString(),
        collectionName,
        leaseOwner,
        Instant.now().plusMillis(LEASE_EXPIRATION_MS),
        FIRST_LEASE_VERSION,
        EncodedUserData.EMPTY.asString(),
        indexDefinitionVersion,
        Map.of(
            indexDefinitionVersion,
            new IndexDefinitionVersionStatus(false, initialIndexStatus.getStatusCode())),
        materializedViewCollectionMetadata,
        null,
        CleanupState.NOT_ELIGIBLE);
  }

  public Lease withUpdatedCheckpoint(EncodedUserData commitInfo) {
    return new Lease(
        this.id,
        SCHEMA_VERSION,
        this.collectionUuid,
        this.collectionName,
        this.leaseOwner,
        Instant.now().plusMillis(LEASE_EXPIRATION_MS),
        this.leaseVersion + 1,
        commitInfo.asString(),
        this.latestIndexDefinitionVersion,
        this.indexDefinitionVersionStatusMap,
        this.materializedViewCollectionMetadata,
        this.steadyAsOfOplogPosition,
        this.cleanupState);
  }

  /**
   * Creates a new lease with a new index definition version.
   *
   * <p>{@code skipInitialSync == true} (Lucene-only change, e.g. hnswOptions): Keep current
   * commitInfo and steadyAsOfOplogPosition. The new generator will RESUME_STEADY_STATE from the
   * previous checkpoint.
   *
   * <p>{@code skipInitialSync == false} (real definition change, e.g. filter): Replace commit info
   * with initial-sync-style resume from the current high watermark (latest oplog position we had
   * reached). The new generator will RESUME_INITIAL_SYNC from that point; the original resume token
   * is not kept.
   */
  public Lease withNewIndexDefinitionVersion(
      String indexDefinitionVersion, IndexStatus initialIndexStatus, boolean skipInitialSync) {
    // 1. Add the new index definition version to the map.
    // 2. Set latestIndexDefinitionVersion to the new version.
    // 3. Either use high water mark (reset to initial-sync from that point) or preserve commitInfo.
    Map<String, IndexDefinitionVersionStatus> newVersionStatus =
        new HashMap<>(this.indexDefinitionVersionStatusMap);
    // When skipInitialSync=true (Lucene-only or same-version change), preserve isQueryable
    // from the latest version — the generator resumes steady state so the index remains
    // queryable. When skipInitialSync=false (schema change), reset to false since initial
    // sync must complete before the index is queryable again.
    boolean isQueryable =
        skipInitialSync
            && this.latestIndexDefinitionVersion != null
            && Optional.ofNullable(
                    this.indexDefinitionVersionStatusMap.get(this.latestIndexDefinitionVersion))
                .map(IndexDefinitionVersionStatus::isQueryable)
                .orElse(false);
    newVersionStatus.put(
        indexDefinitionVersion,
        new IndexDefinitionVersionStatus(isQueryable, initialIndexStatus.getStatusCode()));
    @Var String newCommitInfo = this.commitInfo;
    if (!skipInitialSync) {
      newCommitInfo =
          extractHighWaterMark()
              .map(
                  highWaterMark -> {
                    InitialSyncResumeInfo resumeInfo =
                        new BufferlessIdOrderInitialSyncResumeInfo(
                            highWaterMark, BsonUtils.MIN_KEY);
                    return IndexCommitUserData.createInitialSyncResume(
                            IndexFormatVersion.CURRENT, resumeInfo)
                        .toEncodedData()
                        .asString();
                  })
              .orElse(EncodedUserData.EMPTY.asString());
    }
    return new Lease(
        this.id,
        SCHEMA_VERSION,
        this.collectionUuid,
        this.collectionName,
        this.leaseOwner,
        Instant.now().plusMillis(LEASE_EXPIRATION_MS),
        this.leaseVersion,
        newCommitInfo,
        indexDefinitionVersion,
        newVersionStatus,
        this.materializedViewCollectionMetadata,
        skipInitialSync ? this.steadyAsOfOplogPosition : null,
        this.cleanupState);
  }

  /**
   * Updates status and potentially sets steadyAsOfOplogPosition when transitioned to STEADY
   *
   * @param indexStatus the new status
   * @param indexDefinitionVersion version being updated
   */
  public Lease withUpdatedStatus(
      IndexStatus indexStatus, long indexDefinitionVersion, @Nullable BsonTimestamp oplogPosition) {
    var isSteady = indexStatus.getStatusCode() == IndexStatus.StatusCode.STEADY;
    Map<String, IndexDefinitionVersionStatus> newVersionStatus =
        new HashMap<>(this.indexDefinitionVersionStatusMap);
    var versionKey = String.valueOf(indexDefinitionVersion);
    var wasQueryable =
        Optional.ofNullable(newVersionStatus.get(versionKey))
            .map(IndexDefinitionVersionStatus::isQueryable)
            .orElse(false);
    newVersionStatus.put(
        versionKey,
        new IndexDefinitionVersionStatus(isSteady || wasQueryable, indexStatus.getStatusCode()));

    // Set steadyAsOfOplogPosition only on first STEADY transition
    @Var BsonTimestamp newSteadyPosition = this.steadyAsOfOplogPosition;
    if (isSteady && this.steadyAsOfOplogPosition == null) {
      newSteadyPosition = oplogPosition;
    }

    return new Lease(
        this.id,
        SCHEMA_VERSION,
        this.collectionUuid,
        this.collectionName,
        this.leaseOwner,
        Instant.now().plusMillis(LEASE_EXPIRATION_MS),
        this.leaseVersion + 1,
        this.commitInfo,
        this.latestIndexDefinitionVersion,
        newVersionStatus,
        this.materializedViewCollectionMetadata,
        newSteadyPosition,
        this.cleanupState);
  }

  /**
   * Creates a renewed lease with a new owner, updated expiration, and incremented version. Used for
   * lease acquisition and renewal (heartbeat).
   *
   * @param newOwner the hostname of the new lease owner
   * @return a new Lease with updated ownership, expiration, and version
   */
  public Lease withRenewedOwnership(String newOwner) {
    return new Lease(
        this.id,
        SCHEMA_VERSION,
        this.collectionUuid,
        this.collectionName,
        newOwner,
        Instant.now().plusMillis(LEASE_EXPIRATION_MS),
        this.leaseVersion + 1,
        this.commitInfo,
        this.latestIndexDefinitionVersion,
        this.indexDefinitionVersionStatusMap,
        this.materializedViewCollectionMetadata,
        this.steadyAsOfOplogPosition,
        this.cleanupState);
  }

  public Lease withResolvedMatViewUuid(UUID matViewCollectionUuid) {
    var newMetadata =
        new MaterializedViewCollectionMetadata(
            this.materializedViewCollectionMetadata().schemaMetadata(),
            matViewCollectionUuid,
            this.materializedViewCollectionMetadata().collectionName());
    // Keeps everything same but new schema version and mat view collection UUID.
    return new Lease(
        this.id(),
        SCHEMA_VERSION,
        this.collectionUuid(),
        this.collectionName(),
        this.leaseOwner(),
        this.leaseExpiration(),
        this.leaseVersion(),
        this.commitInfo(),
        this.latestIndexDefinitionVersion(),
        this.indexDefinitionVersionStatusMap(),
        newMetadata,
        this.steadyAsOfOplogPosition,
        this.cleanupState);
  }

  /**
   * Returns a copy of this lease with ownership released: expiration set to the past and the owner
   * set to empty string so other instances can acquire via replaceOne. Used when giving up leases.
   */
  public Lease withReleasedOwnership() {
    return new Lease(
        this.id,
        SCHEMA_VERSION,
        this.collectionUuid,
        this.collectionName,
        "",
        Instant.now().minusSeconds(1),
        this.leaseVersion + 1,
        this.commitInfo,
        this.latestIndexDefinitionVersion,
        this.indexDefinitionVersionStatusMap,
        this.materializedViewCollectionMetadata,
        this.steadyAsOfOplogPosition,
        this.cleanupState);
  }

  /**
   * Returns a copy of this lease with the GC cleanup state changed. Used by the stale-lease scanner
   * to transition {@link CleanupState#NOT_ELIGIBLE} to {@link CleanupState#ELIGIBLE_FOR_CLEANUP} (a
   * one-way move by design).
   *
   * <p>Does not bump {@code leaseVersion}: this is a cleanupState-only change and must not
   * invalidate concurrent OCC updates on other fields. Race coordination between the resolver
   * (which checks {@code cleanupState != ELIGIBLE_FOR_CLEANUP} before reusing) and the scanner is
   * via the conditional filter on the update, not via OCC on {@code leaseVersion}.
   *
   * <p>The one-way invariant is not enforced here; the scanner is the sole writer and is
   * responsible for never moving back to NOT_ELIGIBLE.
   */
  public Lease withCleanupState(CleanupState newCleanupState) {
    return new Lease(
        this.id,
        SCHEMA_VERSION,
        this.collectionUuid,
        this.collectionName,
        this.leaseOwner,
        this.leaseExpiration,
        this.leaseVersion,
        this.commitInfo,
        this.latestIndexDefinitionVersion,
        this.indexDefinitionVersionStatusMap,
        this.materializedViewCollectionMetadata,
        this.steadyAsOfOplogPosition,
        newCleanupState);
  }

  /**
   * Extracts the highWaterMark from the commit info. - If V1 is in steady state: return opTime from
   * resumeToken - Otherwise (initial sync or not started): return empty
   */
  public Optional<BsonTimestamp> extractHighWaterMark() {
    EncodedUserData encodedUserData = EncodedUserData.fromString(this.commitInfo);
    IndexCommitUserData userData =
        IndexCommitUserData.fromEncodedData(encodedUserData, Optional.empty());
    if (userData.getResumeInfo().isPresent()) {
      try {
        BsonDocument resumeToken = userData.getResumeInfo().get().getResumeToken();
        return Optional.of(ResumeTokenUtils.opTimeFromResumeToken(resumeToken));
      } catch (DecoderException e) {
        return Optional.empty();
      }
    } else {
      return Optional.empty();
    }
  }

  /** Fetch the last steady oplog position from the lease */
  public Optional<BsonTimestamp> getSteadyAsOfOplogPosition() {
    return Optional.ofNullable(this.steadyAsOfOplogPosition);
  }

  /**
   * Returns true if the given index definition version is queryable. A version becomes queryable
   * when it reaches STEADY at least once, or when it inherits queryable status from the previous
   * version via {@code skipInitialSync=true} (Lucene-only or same-version change where MV data is
   * preserved). Once queryable, a version always remains so (one-way ratchet).
   */
  public boolean isVersionQueryable(String version) {
    IndexDefinitionVersionStatus status = this.indexDefinitionVersionStatusMap.get(version);
    return status != null && status.isQueryable();
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder()
        .field(Fields._ID, this.id)
        .field(Fields.SCHEMA_VERSION, this.schemaVersion)
        .field(Fields.COLLECTION_UUID, this.collectionUuid)
        .field(Fields.COLLECTION_NAME, this.collectionName)
        .field(Fields.LEASE_OWNER, this.leaseOwner)
        .field(Fields.LEASE_EXPIRATION, new BsonDateTime(this.leaseExpiration.toEpochMilli()))
        .field(Fields.LEASE_VERSION, this.leaseVersion)
        .field(Fields.COMMIT_INFO, this.commitInfo)
        .field(Fields.LATEST_INDEX_DEFINITION_VERSION, this.latestIndexDefinitionVersion)
        .field(Fields.INDEX_DEFINITION_VERSION_STATUS_MAP, this.indexDefinitionVersionStatusMap)
        .field(
            Fields.MATERIALIZED_VIEW_COLLECTION_METADATA,
            Optional.of(this.materializedViewCollectionMetadata))
        .field(
            Fields.STEADY_AS_OF_OPLOG_POSITION, Optional.ofNullable(this.steadyAsOfOplogPosition))
        .field(Fields.CLEANUP_STATE, this.cleanupState)
        .build();
  }
}
