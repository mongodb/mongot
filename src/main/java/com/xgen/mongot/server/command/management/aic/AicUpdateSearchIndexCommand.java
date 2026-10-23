package com.xgen.mongot.server.command.management.aic;

import static com.xgen.mongot.server.command.management.definition.common.CommonDefinitions.OK_RESPONSE;

import com.xgen.mongot.catalogservice.AuthoritativeIndexCatalog;
import com.xgen.mongot.catalogservice.CatalogAccessGuard;
import com.xgen.mongot.catalogservice.MetadataServiceException;
import com.xgen.mongot.catalogservice.TopologyMismatchException;
import com.xgen.mongot.catalogservice.schema.AuthoritativeIndexKey;
import com.xgen.mongot.catalogservice.schema.IndexEntry;
import com.xgen.mongot.config.provider.community.embedding.AutoEmbeddingIndexValidator;
import com.xgen.mongot.config.util.Invariants;
import com.xgen.mongot.config.util.Invariants.AnalyzerInvariants;
import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.definition.InvalidIndexDefinitionException;
import com.xgen.mongot.index.definition.SearchIndexDefinition;
import com.xgen.mongot.index.definition.VectorIndexDefinition;
import com.xgen.mongot.index.definition.config.IndexDefinitionConfig;
import com.xgen.mongot.server.command.Command;
import com.xgen.mongot.server.command.management.definition.UpdateSearchIndexCommandDefinition;
import com.xgen.mongot.server.command.management.definition.common.UserIndexDefinition;
import com.xgen.mongot.server.command.management.definition.common.UserViewDefinition;
import com.xgen.mongot.server.command.management.util.IndexMapper;
import com.xgen.mongot.server.message.MessageUtils;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.bson.parser.BsonDocumentParser;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.mongodb.CheckedMongoException;
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
  private final CatalogAccessGuard catalogAccessGuard;

  private final String db;

  private final String collectionName;

  private final UUID collectionUuid;

  private final Optional<UserViewDefinition> view;

  private final UpdateSearchIndexCommandDefinition definition;

  private final IndexDefinitionConfig indexDefinitionConfig;

  AicUpdateSearchIndexCommand(
      AuthoritativeIndexCatalog authoritativeIndexCatalog,
      CatalogAccessGuard catalogAccessGuard,
      String db,
      UUID collectionUuid,
      String collectionName,
      Optional<UserViewDefinition> view,
      UpdateSearchIndexCommandDefinition definition,
      IndexDefinitionConfig indexDefinitionConfig) {
    this.authoritativeIndexCatalog = authoritativeIndexCatalog;
    this.catalogAccessGuard = catalogAccessGuard;
    this.db = db;
    this.collectionUuid = collectionUuid;
    this.collectionName = collectionName;
    this.view = view;
    this.definition = definition;
    this.indexDefinitionConfig = indexDefinitionConfig;
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
      this.catalogAccessGuard.requireTopologyMatch();
    } catch (TopologyMismatchException | CheckedMongoException e) {
      LOG.atError().setCause(e).log("Rejecting updateSearchIndex; topology check failed");
      return MessageUtils.createError(
          Errors.COMMAND_FAILED, Objects.requireNonNullElse(e.getMessage(), "unknown error"));
    }

    try {
      Optional<String> viewName = this.view.map(UserViewDefinition::name);
      Optional<ObjectId> indexId = this.definition.id();
      Optional<String> indexName = this.definition.name();
      Optional<IndexEntry> matchingEntry =
          this.authoritativeIndexCatalog.listIndexes(this.collectionUuid).stream()
              .filter(
                  entry ->
                      indexName.map(entry.indexKey().indexName()::equals).orElse(true)
                          && indexId.map(entry.indexId()::equals).orElse(true))
              .findAny();

      if (matchingEntry.isEmpty()) {
        String idType = this.definition.name().isPresent() ? "name" : "id";
        Object idValue =
            this.definition.name().map(Object.class::cast).or(this.definition::id).orElseThrow();
        return MessageUtils.createError(
            Errors.INDEX_NOT_FOUND,
            String.format(
                "No index with %s %s exists in namespace %s.%s",
                idType, idValue, this.db, viewName.orElse(this.collectionName)));
      }

      IndexDefinition oldIndex;
      try (var parser = BsonDocumentParser.fromRoot(matchingEntry.get().definitionBson()).build()) {
        oldIndex = IndexDefinition.fromBson(parser);
      } catch (BsonParseException e) {
        LOG.atWarn()
            .addKeyValue("indexEntry", matchingEntry.get())
            .setCause(e)
            .log("Index definition is stored in an unrecognized format and cannot be updated");
        return MessageUtils.createError(
            Errors.COMMAND_FAILED,
            "Index definition is stored in an unrecognized format and cannot be updated.");
      }
      UserIndexDefinition newUserDefinition;
      try {
        newUserDefinition = this.definition.indexDefinition(oldIndex.getType());
      } catch (BsonParseException e) {
        // The requested definition is client input, so a definition that cannot be parsed is a
        // client error rather than an internal failure; report it like the other definition-level
        // rejections below.
        return MessageUtils.createError(Errors.BAD_VALUE, e.getMessage());
      }

      IndexDefinition newIndex =
          IndexMapper.toInternal(
              oldIndex.getName(),
              Optional.of(oldIndex.getIndexId()),
              newUserDefinition,
              oldIndex.getCollectionUuid(),
              this.db,
              this.collectionName,
              oldIndex.getView(),
              oldIndex.getDefinitionVersion().map(old -> old + 1).orElse(1L),
              Instant.now(),
              oldIndex.getMaterializedViewNameFormatVersion());

      if (oldIndex.getType() != newIndex.getType()) {
        // Can't switch between search/vector indexes via an update.
        return MessageUtils.createError(
            Errors.INVALID_INDEX_SPECIFICATION_OPTION, "An index's type cannot be changed");
      }

      List<IndexDefinition> allIndexes = this.authoritativeIndexCatalog.listIndexDefinitions();
      var searchList = new ArrayList<SearchIndexDefinition>();
      var vectorList = new ArrayList<VectorIndexDefinition>();
      allIndexes.stream()
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

      if (newIndex instanceof SearchIndexDefinition searchIndex) {
        try {
          searchIndex.validateEmbeddedDocumentsNestingLevel(
              this.indexDefinitionConfig.getMaxEmbeddedDocumentsNestingLevel());
          searchIndex.validateSortDefinition();
        } catch (InvalidIndexDefinitionException e) {
          return MessageUtils.createError(Errors.BAD_VALUE, e.getMessage());
        }
        AnalyzerInvariants.validate(searchList, List.of());
        AnalyzerInvariants.validateFieldAnalyzerReferences(searchIndex, Set.of(), Set.of());
        AnalyzerInvariants.validateSynonymAnalyzersAreReferenceable(searchIndex);
      }
      if (newIndex instanceof VectorIndexDefinition vectorIndex) {
        Invariants.validateVectorNestedRootReferences(List.of(vectorIndex));
      }

      // Validate auto-embedding index updates. Converting a type:autoEmbed index to type:vector
      // is rejected; all other updates (including changes to auto-embedding field values such as
      // path, model, or quantization) are allowed. The new definition is re-validated like create
      // (registered models, no mixed vector types) so an update cannot persist a definition that
      // create would reject; the superseded MV is cleaned up by GC. Failures of these
      // auto-embedding checks are client errors (BAD_VALUE), matching the same checks in the
      // create workflow; the catalog/analyzer invariants below return COMMAND_FAILED on create
      // and update alike.
      try {
        if (oldIndex instanceof VectorIndexDefinition oldVector
            && newIndex instanceof VectorIndexDefinition newVector) {
          AutoEmbeddingIndexValidator.validateNoAutoEmbeddingTypeConversion(oldVector, newVector);
          if (newVector.isAutoEmbeddingIndex()) {
            AutoEmbeddingIndexValidator.validate(newVector);
          }
        } else if (oldIndex instanceof SearchIndexDefinition
            && newIndex instanceof SearchIndexDefinition newSearch) {
          if (newSearch.isAutoEmbeddingIndex()) {
            AutoEmbeddingIndexValidator.validate(newSearch);
          }
        } else {
          throw new IllegalStateException(
              "Cannot compare index definitions of different types: "
                  + oldIndex.getClass().getSimpleName()
                  + " vs "
                  + newIndex.getClass().getSimpleName());
        }
      } catch (InvalidIndexDefinitionException e) {
        return MessageUtils.createError(Errors.BAD_VALUE, e.getMessage());
      }

      // Run the catalog invariants after the definition-level checks above so a definition that
      // also violates an invariant reports the same BAD_VALUE error on update as on create.
      Invariants.validateInvariants(List.of(), searchList, vectorList, allIndexes);

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
