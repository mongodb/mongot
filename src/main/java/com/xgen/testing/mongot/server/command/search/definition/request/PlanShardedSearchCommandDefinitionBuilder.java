package com.xgen.testing.mongot.server.command.search.definition.request;

import com.xgen.mongot.server.command.search.definition.request.PlanShardedSearchCommandDefinition;
import com.xgen.mongot.util.Check;
import java.util.Optional;
import java.util.UUID;
import org.bson.BsonDocument;

public class PlanShardedSearchCommandDefinitionBuilder {

  private Optional<String> db = Optional.empty();
  private Optional<String> collectionName = Optional.empty();
  private Optional<UUID> collectionUuid = Optional.empty();
  private Optional<String> viewName = Optional.empty();
  private Optional<BsonDocument> query = Optional.empty();

  private Optional<PlanShardedSearchCommandDefinition.SearchFeatures> searchFeatures =
      Optional.empty();

  public static PlanShardedSearchCommandDefinitionBuilder builder() {
    return new PlanShardedSearchCommandDefinitionBuilder();
  }

  public PlanShardedSearchCommandDefinitionBuilder db(String db) {
    this.db = Optional.of(db);
    return this;
  }

  public PlanShardedSearchCommandDefinitionBuilder collectionName(String collectionName) {
    this.collectionName = Optional.of(collectionName);
    return this;
  }

  public PlanShardedSearchCommandDefinitionBuilder collectionUuid(UUID collectionUuid) {
    this.collectionUuid = Optional.of(collectionUuid);
    return this;
  }

  public PlanShardedSearchCommandDefinitionBuilder viewName(String viewName) {
    this.viewName = Optional.of(viewName);
    return this;
  }

  public PlanShardedSearchCommandDefinitionBuilder query(BsonDocument query) {
    this.query = Optional.of(query);
    return this;
  }

  public PlanShardedSearchCommandDefinitionBuilder searchFeatures(
      PlanShardedSearchCommandDefinition.SearchFeatures searchFeatures) {
    this.searchFeatures = Optional.of(searchFeatures);
    return this;
  }

  /** Builds the PlanShardedSearchCommandDefinition. */
  public PlanShardedSearchCommandDefinition build() {
    Check.isPresent(this.db, "db");
    Check.isPresent(this.collectionName, "collectionName");
    Check.isPresent(this.query, "query");

    return new PlanShardedSearchCommandDefinition(
        this.db.get(),
        this.collectionName.get(),
        this.collectionUuid,
        this.viewName,
        this.query.get(),
        this.searchFeatures.orElse(
            new PlanShardedSearchCommandDefinition.SearchFeatures(
                PlanShardedSearchCommandDefinition.Fields.SEARCH_FEATURES
                    .getDefaultValue()
                    .shardedSort())));
  }
}
