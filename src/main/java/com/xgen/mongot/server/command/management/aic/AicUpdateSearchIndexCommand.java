package com.xgen.mongot.server.command.management.aic;

import static com.xgen.mongot.server.command.management.definition.common.CommonDefinitions.OK_RESPONSE;

import com.xgen.mongot.catalogservice.AuthoritativeIndexCatalog;
import com.xgen.mongot.catalogservice.AuthoritativeIndexKey;
import com.xgen.mongot.catalogservice.MetadataServiceException;
import com.xgen.mongot.config.provider.community.embedding.AutoEmbeddingIndexValidator;
import com.xgen.mongot.config.util.Invariants;
import com.xgen.mongot.config.util.Invariants.AnalyzerInvariants;
import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.definition.SearchIndexDefinition;
import com.xgen.mongot.index.definition.VectorIndexDefinition;
import com.xgen.mongot.server.command.Command;
import com.xgen.mongot.server.command.management.definition.UpdateSearchIndexCommandDefinition;
import com.xgen.mongot.server.command.management.definition.common.UserViewDefinition;
import com.xgen.mongot.server.command.management.util.IndexMapper;
import com.xgen.mongot.server.message.MessageUtils;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.mongodb.Errors;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.bson.BsonDocument;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AicUpdateSearchIndexCommand implements Command {

  private static final Logger LOG = LoggerFactory.getLogger(AicUpdateSearchIndexCommand.class);

  private final AuthoritativeIndexCatalog authoritativeIndexCatalog;

  private final String db;

  private final String collectionName;

  private final UUID collectionUuid;

  private final Optional<UserViewDefinition> view;

  private final UpdateSearchIndexCommandDefinition definition;

  AicUpdateSearchIndexCommand(
      AuthoritativeIndexCatalog authoritativeIndexCatalog,
      String db,
      UUID collectionUuid,
      String collectionName,
      Optional<UserViewDefinition> view,
      UpdateSearchIndexCommandDefinition definition) {
    this.authoritativeIndexCatalog = authoritativeIndexCatalog;
    this.db = db;
    this.collectionUuid = collectionUuid;
    this.collectionName = collectionName;
    this.view = view;
    this.definition = definition;
  }

  @Override
  public String name() {
    return UpdateSearchIndexCommandDefinition.NAME;
  }

  @Override
  public boolean maybeLoadShed() {
    return false;
  }

  @Override
  public BsonDocument run() {
    // This should be validated by command deserialization, but do a sanity check here.
    Check.exactlyOneOf(
        "Expected one of index name or ID", this.definition.name(), this.definition.id());

    // Only one of indexName and indexId will be added to the log attributes. Keys with empty
    // optionals as values will be omitted.
    LOG.atInfo()
        .addKeyValue("command", UpdateSearchIndexCommandDefinition.NAME)
        .addKeyValue("indexName", this.definition.name())
        .addKeyValue("indexId", this.definition.id())
        .log("Received command");

    try {
      Optional<String> viewName = this.view.map(UserViewDefinition::name);
      Optional<ObjectId> indexId = this.definition.id();
      Optional<String> indexName = this.definition.name();
      Optional<IndexDefinition> matchingIndex =
          this.authoritativeIndexCatalog.listIndexDefinitions(this.collectionUuid).stream()
              .filter(
                  index ->
                      indexName.map(index.getName()::equals).orElse(true)
                          && indexId.map(index.getIndexId()::equals).orElse(true))
              .findAny();

      if (matchingIndex.isEmpty()) {
        String idType = this.definition.name().isPresent() ? "name" : "id";
        Object idValue =
            this.definition.name().map(Object.class::cast).or(this.definition::id).orElseThrow();
        return MessageUtils.createError(
            Errors.INDEX_NOT_FOUND,
            String.format(
                "No index with %s %s exists in namespace %s.%s",
                idType, idValue, this.db, viewName.orElse(this.collectionName)));
      }

      IndexDefinition oldIndex = matchingIndex.get();
      IndexDefinition newIndex =
          IndexMapper.toInternal(
              oldIndex.getName(),
              Optional.of(oldIndex.getIndexId()),
              this.definition.indexDefinition(oldIndex.getType()),
              oldIndex.getCollectionUuid(),
              this.db,
              this.collectionName,
              oldIndex.getView(),
              oldIndex.getDefinitionVersion().map(old -> old + 1).orElse(1L),
              Instant.now());

      if (oldIndex.getType() != newIndex.getType()) {
        // Can't switch between search/vector indexes via an update.
        return MessageUtils.createError(
            Errors.INVALID_INDEX_SPECIFICATION_OPTION, "An index's type cannot be changed");
      }

      List<IndexDefinition> allIndexes = this.authoritativeIndexCatalog.listIndexDefinitions();
      var searchList = new ArrayList<SearchIndexDefinition>();
      var vectorList = new ArrayList<VectorIndexDefinition>();
      this.authoritativeIndexCatalog.listIndexDefinitions().stream()
          .filter(definition -> !definition.getIndexId().equals(oldIndex.getIndexId()))
          .forEach(
              definition -> {
                switch (definition) {
                  case SearchIndexDefinition search -> searchList.add(search);
                  case VectorIndexDefinition vector -> vectorList.add(vector);
                }
              });
      switch (newIndex) {
        case SearchIndexDefinition search -> searchList.add(search);
        case VectorIndexDefinition vector -> vectorList.add(vector);
      }
      Invariants.validateInvariants(List.of(), searchList, vectorList, allIndexes);

      if (newIndex instanceof SearchIndexDefinition searchIndex) {
        AnalyzerInvariants.validate(searchList, List.of());
        AnalyzerInvariants.validateFieldAnalyzerReferences(searchIndex, Set.of(), Set.of());
      }
      if (newIndex instanceof VectorIndexDefinition vectorIndex) {
        Invariants.validateVectorNestedRootReferences(List.of(vectorIndex));
      }

      // Validate auto-embedding index update restrictions
      if (oldIndex instanceof VectorIndexDefinition oldVector
          && newIndex instanceof VectorIndexDefinition newVector) {
        AutoEmbeddingIndexValidator.validateNoAutoEmbeddingTypeConversion(oldVector, newVector);
        if (oldVector.isAutoEmbeddingIndex() && newVector.isAutoEmbeddingIndex()) {
          AutoEmbeddingIndexValidator.validateNoAutoEmbeddingFieldChanges(oldVector, newVector);
        }
      } else if (oldIndex instanceof SearchIndexDefinition oldSearch
          && newIndex instanceof SearchIndexDefinition newSearch) {
        if (oldSearch.isAutoEmbeddingIndex() && newSearch.isAutoEmbeddingIndex()) {
          AutoEmbeddingIndexValidator.validateNoAutoEmbeddingFieldChanges(oldSearch, newSearch);
        }
      } else {
        throw new IllegalStateException(
            "Cannot compare index definitions of different types: "
                + oldIndex.getClass().getSimpleName()
                + " vs "
                + newIndex.getClass().getSimpleName());
      }

      this.authoritativeIndexCatalog.updateIndex(
          new AuthoritativeIndexKey(this.collectionUuid, newIndex.getName()),
          newIndex,
          this.definition.definitionBson());
    } catch (MetadataServiceException e) {
      return MessageUtils.createError(Errors.COMMAND_FAILED, e.getMessage());
    } catch (Exception e) {
      String message = Objects.requireNonNullElse(e.getMessage(), "unknown error");
      return MessageUtils.createError(Errors.COMMAND_FAILED, message);
    }

    return OK_RESPONSE;
  }

  @Override
  public ExecutionPolicy getExecutionPolicy() {
    return ExecutionPolicy.ASYNC;
  }
}
