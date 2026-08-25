package com.xgen.mongot.catalogservice.schema;

import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import java.util.Objects;
import java.util.Optional;
import org.bson.BsonDocument;
import org.bson.BsonTimestamp;

/**
 * Persisted representation of the internal {@code IndexStatus} model. This is the schema-owned
 * record written to and read back from the index-stats metadata collection (embedded in {@link
 * IndexStatsEntry.DetailedIndexStats}). It is a plain data record: it carries the persisted fields
 * but none of {@code IndexStatus}'s in-memory behavior, factory methods, or reason/statusCode
 * validation. Use {@link com.xgen.mongot.catalogservice.IndexStatusMapper} to convert to/from the
 * internal {@code IndexStatus} model.
 *
 * <p>{@link StatusCode} and {@link Reason} mirror the internal enums so the persisted schema owns
 * its own value set independently of the in-memory model. The mirrors are kept 1:1 with the
 * internal enums; the mapper converts between them exactly.
 *
 * <p>All four fields are persisted (including {@code optime} and {@code reason}) so a customer
 * inspecting the metadata collection directly sees the full status, even though the read path
 * currently only consumes {@code statusCode} and {@code message}.
 */
public record IndexStatusP(
    StatusCode statusCode,
    Optional<String> message,
    Optional<BsonTimestamp> optime,
    Optional<Reason> reason)
    implements DocumentEncodable {

  public IndexStatusP {
    Objects.requireNonNull(statusCode);
    Objects.requireNonNull(message);
    Objects.requireNonNull(optime);
    Objects.requireNonNull(reason);
  }

  /** Persisted mirror of the internal {@code IndexStatus.StatusCode} enum. */
  public enum StatusCode {
    UNKNOWN,
    DOES_NOT_EXIST,
    NOT_STARTED,
    INITIAL_SYNC,
    STALE,
    RECOVERING_TRANSIENT,
    RECOVERING_NON_TRANSIENT,
    STEADY,
    FAILED
  }

  /** Persisted mirror of the internal {@code IndexStatus.Reason} enum. */
  public enum Reason {
    INITIALIZATION_FAILED,
    INITIAL_SYNC_REPLICATION_FAILED,
    STEADY_STATE_REPLICATION_FAILED,
    USER_ERROR,
    EXCEED_MAX_LIMIT,
    COLLECTION_NOT_FOUND,
    INDEX_DROPPED,
    AUTO_EMBEDDING_RESOLUTION_FAILED,
    AUTO_EMBEDDING_RESOLUTION_RETRY,
    INITIAL_SYNC_REPLICATION_FAILED_RETRY
  }

  static class Fields {
    static final Field.Required<StatusCode> STATUS_CODE =
        Field.builder("statusCode").enumField(StatusCode.class).asUpperUnderscore().required();

    static final Field.Optional<String> MESSAGE =
        Field.builder("message").stringField().optional().noDefault();

    static final Field.Optional<BsonTimestamp> OP_TIME =
        Field.builder("optime").bsonTimestampField().optional().noDefault();

    static final Field.Optional<Reason> REASON =
        Field.builder("reason").enumField(Reason.class).asUpperUnderscore().optional().noDefault();
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder()
        .field(Fields.STATUS_CODE, this.statusCode)
        .field(Fields.MESSAGE, this.message)
        .field(Fields.OP_TIME, this.optime)
        .field(Fields.REASON, this.reason)
        .build();
  }

  public static IndexStatusP fromBson(DocumentParser parser) throws BsonParseException {
    return new IndexStatusP(
        parser.getField(Fields.STATUS_CODE).unwrap(),
        parser.getField(Fields.MESSAGE).unwrap(),
        parser.getField(Fields.OP_TIME).unwrap(),
        parser.getField(Fields.REASON).unwrap());
  }
}
