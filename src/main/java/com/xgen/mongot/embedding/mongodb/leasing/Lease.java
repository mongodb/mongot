package com.xgen.mongot.embedding.mongodb.leasing;

import com.google.common.annotations.VisibleForTesting;
import com.xgen.mongot.index.EncodedUserData;
import com.xgen.mongot.index.status.IndexStatus;
import com.xgen.mongot.index.version.IndexFormatVersion;
import com.xgen.mongot.replication.mongodb.common.BufferlessIdOrderInitialSyncResumeInfo;
import com.xgen.mongot.replication.mongodb.common.IndexCommitUserData;
import com.xgen.mongot.replication.mongodb.common.InitialSyncResumeInfo;
import com.xgen.mongot.replication.mongodb.common.ResumeTokenUtils;
import com.xgen.mongot.util.BsonUtils;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import com.xgen.mongot.util.bson.parser.Value;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
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
 *     }
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
    Map<String, IndexDefinitionVersionStatus> indexDefinitionVersionStatusMap)
    implements DocumentEncodable {

  private static final long SCHEMA_VERSION = 1L;

  @VisibleForTesting static final long FIRST_LEASE_VERSION = 1L;
  /** Lease expiration time in milliseconds (5 minutes). */
  public static final long LEASE_EXPIRATION_MS = 300000L;

  static class Fields {
    /**
     * The unique identifier for the lease. For now, we maintain one lease per mat view collection
     * and hence the id is the same as the mat view collection name (which in turn is the index ID).
     */
    public static final Field.Required<String> _ID = Field.builder("id").stringField().required();

    /**
     * The schema version of the lease document. We currently only have one version, but this should
     * be incremented (and appropriate deserialization logic added) if we make any backwards
     * incompatible changes to the lease document.
     */
    public static final Field.Required<Long> SCHEMA_VERSION =
        Field.builder("schemaVersion").longField().required();

    /** The UUID of the collection that the index is on. */
    public static final Field.Required<String> COLLECTION_UUID =
        Field.builder("collectionUUID").stringField().required();

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
     */
    public static final Field.Required<Map<String, IndexDefinitionVersionStatus>>
        INDEX_DEFINITION_VERSION_STATUS_MAP =
            Field.builder("indexDefinitionVersionStatusMap")
                .mapOf(
                    Value.builder()
                        .classValue(IndexDefinitionVersionStatus::fromBson)
                        .disallowUnknownFields()
                        .required())
                .required();
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
       * Whether the index definition version is queryable. This field is to durably track the
       * transition of this index definition version from building to queryable. Once it becomes
       * queryable, it always remains so.
       */
      public static final Field.Required<Boolean> IS_QUERYABLE =
          Field.builder("isQueryable").booleanField().required();

      /** The status code of the index definition version. */
      public static final Field.Required<IndexStatus.StatusCode> INDEX_STATUS =
          Field.builder("indexStatusCode")
              .enumField(IndexStatus.StatusCode.class)
              .asCamelCase()
              .required();
    }
  }

  public static Lease newLease(
      String leaseId,
      UUID collectionUuid,
      String collectionName,
      String leaseOwner,
      String indexDefinitionVersion,
      IndexStatus initialIndexStatus) {
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
            new IndexDefinitionVersionStatus(false, initialIndexStatus.getStatusCode())));
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
        this.indexDefinitionVersionStatusMap);
  }

  /**
   * Creates a new lease with a new index definition version, preserving the highWaterMark.
   */
  public Lease withNewIndexDefinitionVersion(
      String indexDefinitionVersion, IndexStatus initialIndexStatus) {
    // this method creates a new lease with the following changes
    // 1. it adds the new index definition version to the indexDefinitionVersionStatusMap
    // 2. it sets the latestIndexDefinitionVersion to the new index definition version
    // 3. it preserves the highWaterMark from the previous commitInfo but resets scan position.
    Map<String, IndexDefinitionVersionStatus> newVersionStatus =
        new HashMap<>(this.indexDefinitionVersionStatusMap);
    newVersionStatus.put(
        indexDefinitionVersion,
        new IndexDefinitionVersionStatus(false, initialIndexStatus.getStatusCode()));
    String newCommitInfo = extractHighWaterMark().map(highWaterMark -> {
      InitialSyncResumeInfo resumeInfo =
          new BufferlessIdOrderInitialSyncResumeInfo(highWaterMark, BsonUtils.MIN_KEY);
      // MaterializedViewGeneration always uses CURRENT, so this will match the expected version.
      return IndexCommitUserData.createInitialSyncResume(IndexFormatVersion.CURRENT, resumeInfo)
          .toEncodedData().asString();
    }).orElse(EncodedUserData.EMPTY.asString());
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
        newVersionStatus);
  }

  public Lease withUpdatedStatus(IndexStatus indexStatus, long indexDefinitionVersion) {
    var isSteady = indexStatus.getStatusCode() == IndexStatus.StatusCode.STEADY;
    Map<String, IndexDefinitionVersionStatus> newVersionStatus =
        new HashMap<>(this.indexDefinitionVersionStatusMap);
    newVersionStatus.put(
        String.valueOf(indexDefinitionVersion),
        new IndexDefinitionVersionStatus(isSteady, indexStatus.getStatusCode()));
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
        newVersionStatus);
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
        this.indexDefinitionVersionStatusMap);
  }

  /**
   * Extracts the highWaterMark from the commit info.
   * - If V1 is in steady state: return opTime from resumeToken
   * - Otherwise (initial sync or not started): return empty
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
        .build();
  }
}
