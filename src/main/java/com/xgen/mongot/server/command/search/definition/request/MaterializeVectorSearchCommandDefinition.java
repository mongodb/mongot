package com.xgen.mongot.server.command.search.definition.request;

import com.xgen.mongot.index.query.VectorSearchQuery;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import java.util.Optional;

/**
 * Request for the {@code materializeVectorSearch} command.
 *
 * <p>This is the {@code vectorSearch} wire command under a different command name, addressed by
 * collection <em>name</em> instead of collection UUID. It is issued once by the router (mongos)
 * before a sharded query is scattered, and the router has no per-shard collection UUID, so mongot
 * resolves the index by {@code (db, collectionName, indexName)}.
 *
 * <p>Fields that only make sense when a search actually runs on a shard ({@code collectionUUID},
 * {@code explain}, {@code cursorOptions}) are not part of this request.
 */
public record MaterializeVectorSearchCommandDefinition(
    VectorSearchCommandDefinition.VectorSearchQueryOrUserError vectorSearchQueryOrUserError,
    String db,
    String collectionName,
    Optional<String> viewName,
    Optional<Long> deadlineTimestampMs) {

  public static final String NAME = "materializeVectorSearch";

  static class Fields {
    static final Field.Required<String> AUTO_EMBEDDING_MATERIALIZE_VECTOR_SEARCH =
        Field.builder(NAME).stringField().required();

    static final Field.Required<String> DB = Field.builder("$db").stringField().required();

    static final Field.Optional<String> VIEW_NAME =
        Field.builder("viewName").stringField().optional().noDefault();

    static final Field.Optional<Long> DEADLINE_TIMESTAMP_MS =
        Field.builder("deadlineTimestampMs").longField().optional().noDefault();
  }

  /** The user's query, or the user-facing parse error captured while deserializing it. */
  public VectorSearchQuery getQuery() throws BsonParseException {
    return this.vectorSearchQueryOrUserError.vectorSearchQuery();
  }

  public static MaterializeVectorSearchCommandDefinition create(
      VectorSearchQuery query,
      String db,
      String collectionName,
      Optional<String> viewName,
      Optional<Long> deadlineTimestampMs) {
    return new MaterializeVectorSearchCommandDefinition(
        VectorSearchCommandDefinition.VectorSearchQueryOrUserError.create(query),
        db,
        collectionName,
        viewName,
        deadlineTimestampMs);
  }

  public static MaterializeVectorSearchCommandDefinition fromBson(
      DocumentParser parser, boolean vectorStoredSourceEnabled) throws BsonParseException {
    return new MaterializeVectorSearchCommandDefinition(
        VectorSearchCommandDefinition.VectorSearchQueryOrUserError.fromBson(
            parser, vectorStoredSourceEnabled),
        parser.getField(Fields.DB).unwrap(),
        parser.getField(Fields.AUTO_EMBEDDING_MATERIALIZE_VECTOR_SEARCH).unwrap(),
        parser.getField(Fields.VIEW_NAME).unwrap(),
        parser.getField(Fields.DEADLINE_TIMESTAMP_MS).unwrap());
  }
}
