package com.xgen.mongot.replication.mongodb.common;

import com.google.common.annotations.VisibleForTesting;
import com.xgen.mongot.index.EncodedUserData;
import com.xgen.mongot.index.version.GenerationId;
import com.xgen.mongot.util.bson.JsonCodec;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonDocumentParser;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import java.util.Objects;
import java.util.Optional;
import org.bson.BsonDocument;
import org.bson.json.JsonMode;
import org.bson.json.JsonWriterSettings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IndexCommitUserData implements DocumentEncodable {
  private static final Logger LOG = LoggerFactory.getLogger(IndexCommitUserData.class);

  public static final IndexCommitUserData EMPTY =
      new IndexCommitUserData(
          Optional.empty(),
          Optional.empty(),
          Optional.empty(),
          Optional.empty(),
          Optional.empty(),
          Optional.empty(),
          Optional.empty(),
          Optional.empty());

  private final Optional<ChangeStreamResumeInfo> resumeInfo;
  private final Optional<Integer> backendIndexVersion;
  private final Optional<String> exceededLimitsReason;
  private final Optional<InitialSyncResumeInfo> initialSyncResumeInfo;
  private final Optional<StaleStateInfo> staleStateInfo;
  private final Optional<IndexStateInfo> indexStateInfo;
  private final Optional<Long> customVectorEngineLsn;
  private final Optional<AutoEmbeddingCommitMetadata> autoEmbeddingMetadata;

  private static class Fields {
    private static final Field.Optional<ChangeStreamResumeInfo> CHANGE_STREAM_RESUME_INFO =
        Field.builder("changeStreamResumeInfo")
            .classField(ChangeStreamResumeInfo::fromBson)
            .disallowUnknownFields()
            .optional()
            .noDefault();
    private static final Field.Optional<Integer> BACKEND_INDEX_VERSION =
        Field.builder("backendIndexVersion").intField().optional().noDefault();

    private static final Field.Optional<String> EXCEEDED_LIMITS_REASON =
        Field.builder("exceededLimitsReason").stringField().optional().noDefault();

    private static final Field.Optional<InitialSyncResumeInfo> INITIAL_SYNC_RESUME_INFO =
        Field.builder("initialSyncResumeInfo")
            .classField(
                parser ->
                    InitialSyncResumeInfo.fromBson(parser)
                        .orElseThrow(
                            () ->
                                parser
                                    .getContext()
                                    .deserializationException("get empty InitialSyncResumeInfo")))
            .disallowUnknownFields()
            .optional()
            .noDefault();

    // Note: allowUnknownFields() is used here for forwards compatibility.
    // If mongot is rolled back to an older version after a stale index was persisted
    // with a newer field (e.g., changeStreamResumeInfo), the old mongot should still
    // be able to parse the StaleStateInfo and recover the index (though without the
    // new field's data).
    // This covers keys added directly to staleStateInfo and does not extend to the sub-documents
    // it parses strictly: changeStreamResumeInfo is one, so a field added inside that still empties
    // the record on rollback. See docs/internal/development/commit-metadata-rollback-policy.md.
    // TODO(CLOUDP-339757): Change back to disallowUnknownFields() in v1.66+
    private static final Field.Optional<StaleStateInfo> STALE_STATE_INFO =
        Field.builder("staleStateInfo")
            .classField(StaleStateInfo::fromBson)
            .allowUnknownFields()
            .optional()
            .noDefault();
    private static final Field.Optional<IndexStateInfo> INDEX_STATE_INFO =
        Field.builder("indexStateInfo")
            .classField(IndexStateInfo::fromBson)
            .disallowUnknownFields()
            .optional()
            .noDefault();
    private static final Field.Optional<Long> CUSTOM_VECTOR_ENGINE_LSN =
        Field.builder("customVectorEngineLsn").longField().optional().noDefault();

    // Persisted only on the derived Lucene commit of an auto-embedding composite index. Holds the
    // auto-embedding state needed to report the index on a restart while mongod is unreachable.
    // Absent for non-auto-embedding indexes.
    //
    // Rollback behavior: binaries that predate this top-level field reject it as unknown, catch the
    // parse error, and return IndexCommitUserData.EMPTY. That avoids crashing but can force the
    // existing re-sync fallback after rollback. The allowUnknownFields() below applies to future
    // fields added inside autoEmbeddingMetadata once this top-level field is known.
    private static final Field.Optional<AutoEmbeddingCommitMetadata> AUTO_EMBEDDING_METADATA =
        Field.builder("autoEmbeddingMetadata")
            .classField(AutoEmbeddingCommitMetadata::fromBson)
            .allowUnknownFields()
            .optional()
            .noDefault();
  }

  /**
   * Package-private so every record written outside this package comes from a named factory or
   * {@code with} method. Those are the entry points IndexCommitUserDataFieldInventory enumerates to
   * work out which fields mongot can persist, and a caller assembling the record field by field
   * would be invisible to it.
   */
  @VisibleForTesting
  IndexCommitUserData(
      Optional<ChangeStreamResumeInfo> resumeInfo,
      Optional<Integer> backendIndexVersion,
      Optional<String> exceededLimitsReason,
      Optional<InitialSyncResumeInfo> initialSyncResumeInfo,
      Optional<StaleStateInfo> staleStateInfo,
      Optional<IndexStateInfo> indexStateInfo,
      Optional<Long> customVectorEngineLsn,
      Optional<AutoEmbeddingCommitMetadata> autoEmbeddingMetadata) {
    this.resumeInfo = resumeInfo;
    this.backendIndexVersion = backendIndexVersion;
    this.exceededLimitsReason = exceededLimitsReason;
    this.initialSyncResumeInfo = initialSyncResumeInfo;
    this.staleStateInfo = staleStateInfo;
    this.indexStateInfo = indexStateInfo;
    this.customVectorEngineLsn = customVectorEngineLsn;
    this.autoEmbeddingMetadata = autoEmbeddingMetadata;
  }

  public static IndexCommitUserData createExceeded(String reason) {
    return new IndexCommitUserData(
        Optional.empty(),
        Optional.empty(),
        Optional.of(reason),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty());
  }

  public static IndexCommitUserData createStale(StaleStateInfo staleStateInfo) {
    return new IndexCommitUserData(
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.of(staleStateInfo),
        Optional.empty(),
        Optional.empty(),
        Optional.empty());
  }

  public static IndexCommitUserData createInitialSyncResume(
      int formatVersion, InitialSyncResumeInfo resumeInfo) {
    return new IndexCommitUserData(
        Optional.empty(),
        Optional.of(formatVersion),
        Optional.empty(),
        Optional.of(resumeInfo),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty());
  }

  public static IndexCommitUserData createChangeStreamResume(
      ChangeStreamResumeInfo resumeInfo, int formatVersion) {
    return new IndexCommitUserData(
        Optional.of(resumeInfo),
        Optional.of(formatVersion),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty());
  }

  /**
   * Note: IndexStateInfo is intended to eventually include information about other index states but
   * for now should only be used for the DOES_NOT_EXIST status, because if we persist a
   * DOES_NOT_EXIST status then have to rollback from a version including this field to one not
   * including this field, it will cause a reindex. This is acceptable behavior for DOES_NOT_EXIST
   * status but not for other statuses such as STALE
   */
  public static IndexCommitUserData createFromIndexStateInfo(IndexStateInfo indexStateInfo) {
    return new IndexCommitUserData(
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.of(indexStateInfo),
        Optional.empty(),
        Optional.empty());
  }

  /**
   * Constructs a new IndexCommitUserData from the supplied {@link EncodedUserData}, logging
   * deserialization errors to the given logger.
   */
  public static IndexCommitUserData fromEncodedData(
      EncodedUserData encodedUserData, Optional<GenerationId> generationId) {
    return fromEncodedString(encodedUserData.asString(), generationId);
  }

  public static IndexCommitUserData fromBson(DocumentParser parser) throws BsonParseException {
    return new IndexCommitUserData(
        parser.getField(Fields.CHANGE_STREAM_RESUME_INFO).unwrap(),
        parser.getField(Fields.BACKEND_INDEX_VERSION).unwrap(),
        parser.getField(Fields.EXCEEDED_LIMITS_REASON).unwrap(),
        parser.getField(Fields.INITIAL_SYNC_RESUME_INFO).unwrap(),
        parser.getField(Fields.STALE_STATE_INFO).unwrap(),
        parser.getField(Fields.INDEX_STATE_INFO).unwrap(),
        parser.getField(Fields.CUSTOM_VECTOR_ENGINE_LSN).unwrap(),
        parser.getField(Fields.AUTO_EMBEDDING_METADATA).unwrap());
  }

  private static IndexCommitUserData fromEncodedString(
      String encodedString, Optional<GenerationId> generationId) {
    try {
      BsonDocument document = JsonCodec.fromJson(encodedString);
      try (var parser = BsonDocumentParser.fromRoot(document).build()) {
        return fromBson(parser);
      }
    } catch (BsonParseException e) {
      LOG.atInfo()
          .addKeyValue("indexId", generationId.map(genId -> genId.indexId))
          .addKeyValue("generationId", generationId)
          .setCause(e)
          .log("Failed to decode IndexCommitUserData");
      // Returns empty data to trigger re-sync.
      return IndexCommitUserData.EMPTY;
    }
  }

  public boolean isEmpty() {
    return this.resumeInfo.isEmpty() && this.backendIndexVersion.isEmpty();
  }

  public Optional<ChangeStreamResumeInfo> getResumeInfo() {
    return this.resumeInfo;
  }

  public Optional<Integer> getBackendIndexVersion() {
    return this.backendIndexVersion;
  }

  public Optional<String> getExceededLimitsReason() {
    return this.exceededLimitsReason;
  }

  public Optional<StaleStateInfo> getStaleStateInfo() {
    return this.staleStateInfo;
  }

  public Optional<InitialSyncResumeInfo> getInitialSyncResumeInfo() {
    return this.initialSyncResumeInfo;
  }

  public Optional<IndexStateInfo> getIndexStateInfo() {
    return this.indexStateInfo;
  }

  public Optional<Long> getCustomVectorEngineLsn() {
    return this.customVectorEngineLsn;
  }

  public Optional<AutoEmbeddingCommitMetadata> getAutoEmbeddingMetadata() {
    return this.autoEmbeddingMetadata;
  }

  public IndexCommitUserData withCustomVectorEngineLsn(long lsn) {
    return new IndexCommitUserData(
        this.resumeInfo,
        this.backendIndexVersion,
        this.exceededLimitsReason,
        this.initialSyncResumeInfo,
        this.staleStateInfo,
        this.indexStateInfo,
        Optional.of(lsn),
        this.autoEmbeddingMetadata);
  }

  public IndexCommitUserData withAutoEmbeddingMetadata(AutoEmbeddingCommitMetadata metadata) {
    return new IndexCommitUserData(
        this.resumeInfo,
        this.backendIndexVersion,
        this.exceededLimitsReason,
        this.initialSyncResumeInfo,
        this.staleStateInfo,
        this.indexStateInfo,
        this.customVectorEngineLsn,
        Optional.of(metadata));
  }

  /**
   * Converts the {@link IndexCommitUserData} into {@link EncodedUserData} matching the format that
   * can later be used to reconstruct {@link IndexCommitUserData} via {@link
   * IndexCommitUserData#fromEncodedData}.
   */
  public EncodedUserData toEncodedData() {
    return EncodedUserData.fromString(toEncodedString());
  }

  @VisibleForTesting
  String toEncodedString() {
    return toBson()
        .asDocument()
        .toJson(JsonWriterSettings.builder().outputMode(JsonMode.EXTENDED).build());
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder()
        .field(Fields.CHANGE_STREAM_RESUME_INFO, this.resumeInfo)
        .field(Fields.BACKEND_INDEX_VERSION, this.backendIndexVersion)
        .field(Fields.EXCEEDED_LIMITS_REASON, this.exceededLimitsReason)
        .field(Fields.INITIAL_SYNC_RESUME_INFO, this.initialSyncResumeInfo)
        .field(Fields.STALE_STATE_INFO, this.staleStateInfo)
        .field(Fields.INDEX_STATE_INFO, this.indexStateInfo)
        .field(Fields.CUSTOM_VECTOR_ENGINE_LSN, this.customVectorEngineLsn)
        .field(Fields.AUTO_EMBEDDING_METADATA, this.autoEmbeddingMetadata)
        .build();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    IndexCommitUserData userData = (IndexCommitUserData) o;
    return this.resumeInfo.equals(userData.resumeInfo)
        && this.backendIndexVersion.equals(userData.backendIndexVersion)
        && this.exceededLimitsReason.equals(userData.exceededLimitsReason)
        && this.staleStateInfo.equals(userData.staleStateInfo)
        && this.initialSyncResumeInfo.equals(userData.initialSyncResumeInfo)
        && this.indexStateInfo.equals(userData.indexStateInfo)
        && this.customVectorEngineLsn.equals(userData.customVectorEngineLsn)
        && this.autoEmbeddingMetadata.equals(userData.autoEmbeddingMetadata);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        this.resumeInfo,
        this.backendIndexVersion,
        this.exceededLimitsReason,
        this.staleStateInfo,
        this.initialSyncResumeInfo,
        this.indexStateInfo,
        this.customVectorEngineLsn,
        this.autoEmbeddingMetadata);
  }
}
