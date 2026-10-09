package com.xgen.mongot.replication.mongodb.common;

import com.xgen.mongot.index.status.StaleStatusReason;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonDocumentParser;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import java.util.Objects;
import java.util.Optional;
import org.bson.BsonDocument;
import org.bson.BsonTimestamp;
import org.bson.json.JsonMode;
import org.bson.json.JsonWriterSettings;

/** Contains information about transition to the STALE status. */
public class StaleStateInfo implements DocumentEncodable {

  private static class Fields {

    private static final Field.Required<BsonTimestamp> LAST_OPTIME =
        Field.builder("lastOptime").bsonTimestampField().required();

    private static final Field.Required<StaleStatusReason> REASON =
        Field.builder("reason").enumField(StaleStatusReason.class).asUpperUnderscore().required();

    private static final Field.WithDefault<String> MESSAGE =
        Field.builder("message").stringField().optional().withDefault("");

    private static final Field.Optional<ChangeStreamResumeInfo> CHANGE_STREAM_RESUME_INFO =
        Field.builder("changeStreamResumeInfo")
            .classField(ChangeStreamResumeInfo::fromBson)
            .disallowUnknownFields()
            .optional()
            .noDefault();

    private static final Field.Optional<Integer> BACKEND_INDEX_VERSION =
        Field.builder("backendIndexVersion").intField().optional().noDefault();
  }

  /** last optime that have been seen before transitioning to STALE state */
  private final BsonTimestamp lastOptime;

  /** the reason of transitioning to STALE state */
  private final StaleStatusReason reason;

  private final String message;

  /**
   * The change stream resume info that was active when the index went stale. This allows the index
   * to be resumed from the exact point where it stopped, including the namespace and resume token.
   * Optional for backward compatibility with existing persisted data.
   */
  private final Optional<ChangeStreamResumeInfo> changeStreamResumeInfo;

  /**
   * The backend index format version that was active when the index went stale. Optional for
   * backward compatibility with existing persisted data.
   */
  private final Optional<Integer> backendIndexVersion;

  private StaleStateInfo(
      BsonTimestamp lastOptime,
      StaleStatusReason reason,
      String message,
      Optional<ChangeStreamResumeInfo> changeStreamResumeInfo,
      Optional<Integer> backendIndexVersion) {
    this.lastOptime = lastOptime;
    this.reason = reason;
    this.message = message;
    this.changeStreamResumeInfo = changeStreamResumeInfo;
    this.backendIndexVersion = backendIndexVersion;
  }

  /**
   * Maintained for testing backward compatibility with existing persisted data.
   *
   * @return StaleStateInfo with empty changeStreamResumeInfo and backendIndexVersion
   */
  @Deprecated
  public static StaleStateInfo create(BsonTimestamp lastOptime, StaleStatusReason reason) {
    return new StaleStateInfo(
        lastOptime, reason, reason.formatMessage(), Optional.empty(), Optional.empty());
  }

  /**
   * Creates a StaleStateInfo from an {@link IndexCommitUserData}, capturing both the change stream
   * resume info and the backend index format version at the time the index went stale.
   *
   * @throws IllegalStateException if the IndexCommitUserData does not contain a change stream
   *     resume info
   */
  public static StaleStateInfo create(
      StaleStatusReason reason, String message, IndexCommitUserData indexCommitUserData) {
    ChangeStreamResumeInfo changeStreamResumeInfo =
        indexCommitUserData
            .resumeInfo()
            .orElseThrow(
                () ->
                    new IllegalStateException(
                        "IndexCommitUserData must have changeStreamResumeInfo to create"
                            + " StaleStateInfo"));
    return new StaleStateInfo(
        getLastOptimeFromResumeInfo(changeStreamResumeInfo),
        reason,
        message,
        Optional.of(changeStreamResumeInfo),
        indexCommitUserData.backendIndexVersion());
  }

  /**
   * Extracts the lastOptime from the ChangeStreamResumeInfo's resume token.
   *
   * @throws IllegalArgumentException if the resume token cannot be decoded
   */
  private static BsonTimestamp getLastOptimeFromResumeInfo(
      ChangeStreamResumeInfo changeStreamResumeInfo) {
    try {
      return ResumeTokenUtils.opTimeFromResumeToken(changeStreamResumeInfo.getResumeToken());
    } catch (org.apache.commons.codec.DecoderException e) {
      throw new IllegalArgumentException("Failed to decode resume token to extract lastOptime", e);
    }
  }

  public static StaleStateInfo fromBson(DocumentParser parser) throws BsonParseException {
    return new StaleStateInfo(
        parser.getField(Fields.LAST_OPTIME).unwrap(),
        parser.getField(Fields.REASON).unwrap(),
        parser.getField(Fields.MESSAGE).unwrap(),
        parser.getField(Fields.CHANGE_STREAM_RESUME_INFO).unwrap(),
        parser.getField(Fields.BACKEND_INDEX_VERSION).unwrap());
  }

  public static StaleStateInfo fromBson(BsonDocument document) throws BsonParseException {
    try (var parser = BsonDocumentParser.fromRoot(document).build()) {
      return fromBson(parser);
    }
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder()
        .field(Fields.LAST_OPTIME, this.lastOptime)
        .field(Fields.REASON, this.reason)
        .field(Fields.MESSAGE, this.message)
        .field(Fields.CHANGE_STREAM_RESUME_INFO, this.changeStreamResumeInfo)
        .field(Fields.BACKEND_INDEX_VERSION, this.backendIndexVersion)
        .build();
  }

  public String toJson() {
    return toBson().toJson(JsonWriterSettings.builder().outputMode(JsonMode.EXTENDED).build());
  }

  public BsonTimestamp getLastOptime() {
    return this.lastOptime;
  }

  public StaleStatusReason getReason() {
    return this.reason;
  }

  public String getMessage() {
    return this.message.isEmpty() ? this.reason.formatMessage() : this.message;
  }

  public Optional<ChangeStreamResumeInfo> getChangeStreamResumeInfo() {
    return this.changeStreamResumeInfo;
  }

  public Optional<Integer> getBackendIndexVersion() {
    return this.backendIndexVersion;
  }

  @Override
  public boolean equals(Object object) {
    if (this == object) {
      return true;
    }
    if (!(object instanceof StaleStateInfo)) {
      return false;
    }
    StaleStateInfo that = (StaleStateInfo) object;
    return Objects.equals(this.lastOptime, that.lastOptime)
        && this.reason == that.reason
        && Objects.equals(this.message, that.message)
        && Objects.equals(this.changeStreamResumeInfo, that.changeStreamResumeInfo)
        && Objects.equals(this.backendIndexVersion, that.backendIndexVersion);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        this.lastOptime,
        this.reason,
        this.message,
        this.changeStreamResumeInfo,
        this.backendIndexVersion);
  }
}
