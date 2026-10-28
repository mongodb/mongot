package com.xgen.mongot.server.command.search.definition.request;

import static com.xgen.mongot.util.Check.checkState;

import com.xgen.mongot.index.query.VectorSearchQuery;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import java.util.Optional;
import java.util.UUID;
import org.bson.BsonDocument;

public record VectorSearchCommandDefinition(
    com.xgen.mongot.server.command.search.definition.request.VectorSearchCommandDefinition
            .VectorSearchQueryOrUserError
        vectorSearchQueryOrUserError,
    String db,
    String collectionName,
    UUID collectionUuid,
    Optional<String> viewName,
    Optional<ExplainDefinition> explain,
    Optional<Long> deadlineTimestampMs,
    Optional<CursorOptionsDefinition> cursorOptions,
    Optional<String> expectedModel)
    implements DocumentEncodable {

  static class Fields {
    static final Field.Required<String> VECTOR_SEARCH =
        Field.builder("vectorSearch").stringField().required();

    static final Field.Required<String> DB = Field.builder("$db").stringField().required();

    static final Field.Required<UUID> COLLECTION_UUID =
        Field.builder("collectionUUID").uuidField().required();

    static final Field.Optional<String> VIEW_NAME =
        Field.builder("viewName").stringField().optional().noDefault();

    static final Field.Optional<ExplainDefinition> EXPLAIN =
        Field.builder("explain")
            .classField(ExplainDefinition::fromBson)
            .disallowUnknownFields()
            .optional()
            .noDefault();

    static final Field.Optional<Long> DEADLINE_TIMESTAMP_MS =
        Field.builder("deadlineTimestampMs").longField().optional().noDefault();

    // Stamped by the router's materializeVectorSearch; the shard-side check runs only when the
    // request also carried the x-mongodb-materialized mark, and mongots that predate the field
    // ignore it (top-level parsing allows unknown fields).
    static final Field.Optional<String> EXPECTED_MODEL =
        Field.builder("expectedModel").stringField().optional().noDefault();

    static final Field.Optional<CursorOptionsDefinition> CURSOR_OPTIONS =
        Field.builder("cursorOptions")
            .classField(CursorOptionsDefinition::fromBson)
            .allowUnknownFields()
            .optional()
            .noDefault();
  }

  public static final String NAME = "vectorSearch";

  /**
   * When deserializing the BSON command in {@link
   * com.xgen.mongot.server.command.search.VectorSearchCommand.Factory#create}, mongot should throw
   * different exceptions if a deserialization error is due to an error in the caller logic that
   * mis-uses some part of the protocol (e.g. mongod does not include the UUID for the collection
   * being queried), or if the issue is user error.
   *
   * <p>This class lets mongot deserialize both the command and the user-supplied query at the same
   * time, and throw an exception from a user syntax error at a different time than one thrown for a
   * malformed command that does not result from user error.
   */
  public sealed interface VectorSearchQueryOrUserError
      permits VectorSearchQueryOrUserError.ValidVectorQuery,
          VectorSearchQueryOrUserError.InvalidVectorQuery {

    /** Contains a {@link VectorSearchQuery} that was able to be successfully deserialized. */
    record ValidVectorQuery(VectorSearchQuery vectorSearchQuery)
        implements VectorSearchQueryOrUserError {}

    /**
     * Contains a deserialization exception thrown by a {@link VectorSearchQuery} during
     * deserialization.
     */
    record InvalidVectorQuery(BsonParseException userBsonParseException)
        implements VectorSearchQueryOrUserError {

      @Override
      public VectorSearchQuery vectorSearchQuery() throws BsonParseException {
        throw this.userBsonParseException;
      }
    }

    static VectorSearchQueryOrUserError create(VectorSearchQuery vectorSearchQuery) {
      return new ValidVectorQuery(vectorSearchQuery);
    }

    static VectorSearchQueryOrUserError create(BsonParseException userBsonParseException) {
      return new InvalidVectorQuery(userBsonParseException);
    }

    VectorSearchQuery vectorSearchQuery() throws BsonParseException;

    static VectorSearchQueryOrUserError fromBson(
        DocumentParser parser, boolean vectorStoredSourceEnabled) {
      try {
        return create(VectorSearchQuery.fromBson(parser, vectorStoredSourceEnabled));
      } catch (BsonParseException e) {
        return create(e);
      }
    }
  }

  public VectorSearchQuery getQuery() throws BsonParseException {
    return this.vectorSearchQueryOrUserError.vectorSearchQuery();
  }

  public static VectorSearchCommandDefinition fromBson(
      DocumentParser parser, boolean vectorStoredSourceEnabled) throws BsonParseException {
    return new VectorSearchCommandDefinition(
        VectorSearchQueryOrUserError.fromBson(parser, vectorStoredSourceEnabled),
        parser.getField(Fields.DB).unwrap(),
        parser.getField(Fields.VECTOR_SEARCH).unwrap(),
        parser.getField(Fields.COLLECTION_UUID).unwrap(),
        parser.getField(Fields.VIEW_NAME).unwrap(),
        parser.getField(Fields.EXPLAIN).unwrap(),
        parser.getField(Fields.DEADLINE_TIMESTAMP_MS).unwrap(),
        parser.getField(Fields.CURSOR_OPTIONS).unwrap(),
        parser.getField(Fields.EXPECTED_MODEL).unwrap());
  }

  @Override
  public BsonDocument toBson() {
    checkState(
        this.vectorSearchQueryOrUserError instanceof VectorSearchQueryOrUserError.ValidVectorQuery,
        "Cannot serialize invalid vector query");

    BsonDocument withoutVectorQuery =
        BsonDocumentBuilder.builder()
            // Fields.VECTOR_SEARCH must be first field in BsonDocument
            .field(Fields.VECTOR_SEARCH, this.collectionName)
            .field(Fields.DB, this.db)
            .field(Fields.COLLECTION_UUID, this.collectionUuid)
            .field(Fields.EXPLAIN, this.explain)
            .field(Fields.DEADLINE_TIMESTAMP_MS, this.deadlineTimestampMs)
            .field(Fields.CURSOR_OPTIONS, this.cursorOptions)
            .field(Fields.EXPECTED_MODEL, this.expectedModel)
            .build();

    withoutVectorQuery.putAll(
        ((VectorSearchQueryOrUserError.ValidVectorQuery) this.vectorSearchQueryOrUserError)
            .vectorSearchQuery.toBson());

    return withoutVectorQuery;
  }
}
