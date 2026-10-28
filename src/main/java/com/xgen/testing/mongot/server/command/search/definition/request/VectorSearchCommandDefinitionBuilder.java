package com.xgen.testing.mongot.server.command.search.definition.request;

import com.xgen.mongot.index.query.VectorSearchQuery;
import com.xgen.mongot.server.command.search.definition.request.CursorOptionsDefinition;
import com.xgen.mongot.server.command.search.definition.request.ExplainDefinition;
import com.xgen.mongot.server.command.search.definition.request.VectorSearchCommandDefinition;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import java.util.Optional;
import java.util.UUID;

public class VectorSearchCommandDefinitionBuilder {
  private Optional<String> db = Optional.empty();
  private Optional<String> collectionName = Optional.empty();
  private Optional<UUID> collectionUuid = Optional.empty();
  private Optional<String> viewName = Optional.empty();
  private Optional<ExplainDefinition> explain = Optional.empty();
  private Optional<Long> deadlineTimestampMs = Optional.empty();
  private Optional<VectorSearchCommandDefinition.VectorSearchQueryOrUserError> vectorSearchQuery =
      Optional.empty();
  private Optional<CursorOptionsDefinition> cursorOptions = Optional.empty();
  private Optional<String> expectedModel = Optional.empty();

  public static VectorSearchCommandDefinitionBuilder builder() {
    return new VectorSearchCommandDefinitionBuilder();
  }

  public VectorSearchCommandDefinitionBuilder vectorSearchQuery(VectorSearchQuery query) {
    this.vectorSearchQuery =
        Optional.of(VectorSearchCommandDefinition.VectorSearchQueryOrUserError.create(query));
    return this;
  }

  public VectorSearchCommandDefinitionBuilder vectorSearchQuery(
      BsonParseException userBsonException) {
    this.vectorSearchQuery =
        Optional.of(
            VectorSearchCommandDefinition.VectorSearchQueryOrUserError.create(userBsonException));
    return this;
  }

  public VectorSearchCommandDefinitionBuilder db(String db) {
    this.db = Optional.of(db);
    return this;
  }

  public VectorSearchCommandDefinitionBuilder collectionName(String collectionName) {
    this.collectionName = Optional.of(collectionName);
    return this;
  }

  public VectorSearchCommandDefinitionBuilder collectionUuid(UUID collectionUuid) {
    this.collectionUuid = Optional.of(collectionUuid);
    return this;
  }

  public VectorSearchCommandDefinitionBuilder viewName(String viewName) {
    this.viewName = Optional.of(viewName);
    return this;
  }

  public VectorSearchCommandDefinitionBuilder explain(ExplainDefinition explain) {
    this.explain = Optional.of(explain);
    return this;
  }

  public VectorSearchCommandDefinitionBuilder deadlineTimestampMs(long deadlineTimestampMs) {
    this.deadlineTimestampMs = Optional.of(deadlineTimestampMs);
    return this;
  }

  public VectorSearchCommandDefinitionBuilder cursorOptions(CursorOptionsDefinition cursorOptions) {
    this.cursorOptions = Optional.of(cursorOptions);
    return this;
  }

  public VectorSearchCommandDefinitionBuilder expectedModel(String expectedModel) {
    this.expectedModel = Optional.ofNullable(expectedModel);
    return this;
  }

  /** Builds the VectorSearchCommandDefinition. */
  public VectorSearchCommandDefinition build() {
    Check.isPresent(this.db, "db");
    Check.isPresent(this.collectionName, "collectionName");
    Check.isPresent(this.collectionUuid, "collectionUuid");

    return new VectorSearchCommandDefinition(
        this.vectorSearchQuery.get(),
        this.db.get(),
        this.collectionName.get(),
        this.collectionUuid.get(),
        this.viewName,
        this.explain,
        this.deadlineTimestampMs,
        this.cursorOptions,
        this.expectedModel);
  }
}
