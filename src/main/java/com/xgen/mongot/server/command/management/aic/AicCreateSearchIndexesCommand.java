package com.xgen.mongot.server.command.management.aic;

import com.mongodb.ErrorCategory;
import com.mongodb.MongoWriteException;
import com.xgen.mongot.catalogservice.AuthoritativeIndexCatalog;
import com.xgen.mongot.catalogservice.AuthoritativeIndexKey;
import com.xgen.mongot.catalogservice.MetadataServiceException;
import com.xgen.mongot.config.provider.community.embedding.AutoEmbeddingIndexValidator;
import com.xgen.mongot.config.util.Invariants;
import com.xgen.mongot.config.util.Invariants.AnalyzerInvariants;
import com.xgen.mongot.config.util.Invariants.InvariantException;
import com.xgen.mongot.config.util.ViewValidator;
import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.definition.InvalidIndexDefinitionException;
import com.xgen.mongot.index.definition.InvalidViewDefinitionException;
import com.xgen.mongot.index.definition.SearchIndexDefinition;
import com.xgen.mongot.index.definition.VectorIndexDefinition;
import com.xgen.mongot.index.definition.ViewDefinition;
import com.xgen.mongot.server.command.Command;
import com.xgen.mongot.server.command.management.definition.CreateSearchIndexesCommandDefinition;
import com.xgen.mongot.server.command.management.definition.CreateSearchIndexesCommandDefinition.CreateSearchIndexesResponse;
import com.xgen.mongot.server.command.management.definition.common.NamedSearchIndex;
import com.xgen.mongot.server.command.management.definition.common.NamedSearchIndexId;
import com.xgen.mongot.server.command.management.definition.common.UserViewDefinition;
import com.xgen.mongot.server.command.management.util.IndexMapper;
import com.xgen.mongot.server.message.MessageUtils;
import com.xgen.mongot.util.CollectionUtils;
import com.xgen.mongot.util.mongodb.Errors;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.bson.BsonDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AicCreateSearchIndexesCommand implements Command {

  private static final Logger LOG = LoggerFactory.getLogger(AicCreateSearchIndexesCommand.class);

  private final AuthoritativeIndexCatalog authoritativeIndexCatalog;

  private final String db;

  private final String collectionName;

  private final UUID collectionUuid;

  private final Optional<UserViewDefinition> view;

  private final CreateSearchIndexesCommandDefinition definition;

  AicCreateSearchIndexesCommand(
      AuthoritativeIndexCatalog authoritativeIndexCatalog,
      String db,
      UUID collectionUuid,
      String collectionName,
      Optional<UserViewDefinition> view,
      CreateSearchIndexesCommandDefinition definition) {
    this.authoritativeIndexCatalog = authoritativeIndexCatalog;
    this.db = db;
    this.collectionUuid = collectionUuid;
    this.collectionName = collectionName;
    this.view = view;
    this.definition = definition;
  }

  @Override
  public String name() {
    return CreateSearchIndexesCommandDefinition.NAME;
  }

  @Override
  public boolean maybeLoadShed() {
    return false;
  }

  @Override
  public BsonDocument run() {
    LOG.atInfo()
        .addKeyValue("command", CreateSearchIndexesCommandDefinition.NAME)
        .addKeyValue("db", this.db)
        .addKeyValue("collectionName", this.collectionName)
        .log("Received command");
    try {
      Optional<ViewDefinition> view =
          this.view.map(
              definition ->
                  ViewDefinition.existing(
                      definition.name(), definition.effectivePipeline().orElseThrow()));

      if (view.isPresent()) {
        try {
          ViewValidator.validate(view.get());
        } catch (InvalidViewDefinitionException e) {
          return MessageUtils.createError(Errors.BAD_VALUE, e.getMessage());
        }
      }

      List<InternalAndExternalDefinition> newIndexes =
          this.definition.indexes().stream().map(i -> toInternalAndExternalDef(i, view)).toList();

      // Validate auto-embedding indexes
      for (InternalAndExternalDefinition newIndex : newIndexes) {
        if (newIndex.internal.isAutoEmbeddingIndex()) {
          try {
            AutoEmbeddingIndexValidator.validate(
                newIndex.internal, Optional.of(newIndex.external));
          } catch (InvalidIndexDefinitionException e) {
            return MessageUtils.createError(Errors.BAD_VALUE, e.getMessage());
          }
        }
      }

      // If a requested index conflicts with an existing index (same name but different definition)
      // we return an error and don't create any of the indexes.
      List<InternalAndExternalDefinition> deduplicatedIndexes = new ArrayList<>();
      Map<String, IndexDefinition> existingIndexDefinitionsByName =
          this.authoritativeIndexCatalog.listIndexDefinitions(this.collectionUuid).stream()
              .collect(CollectionUtils.toMapUnsafe(IndexDefinition::getName, Function.identity()));
      for (InternalAndExternalDefinition newInternalAndExternal : newIndexes) {
        IndexDefinition newIndex = newInternalAndExternal.internal;
        Optional<IndexDefinition> existingIndex =
            Optional.ofNullable(existingIndexDefinitionsByName.get(newIndex.getName()));
        if (existingIndex.isPresent()
            && !IndexMapper.areEquivalent(existingIndex.get(), newIndex)) {
          return indexExistsWithDifferentDefinitionError(newIndex);
        }

        // Per spec, do not fail indexes with same name and same definition.
        // However, do not attempt to create an index if it already exists.
        // See http://go/search-index-mgmt-create-index for more detail.
        if (existingIndex.isEmpty()) {
          deduplicatedIndexes.add(newInternalAndExternal);
        }
      }

      // Indexes are free of known conflicts, so proceed with adding them
      var searchList = new ArrayList<SearchIndexDefinition>();
      var vectorList = new ArrayList<VectorIndexDefinition>();
      var existingIndexesFromCatalog = this.authoritativeIndexCatalog.listIndexDefinitions();
      Stream.concat(
              existingIndexesFromCatalog.stream(),
              deduplicatedIndexes.stream().map(i -> i.internal))
          .forEach(
              definition -> {
                switch (definition) {
                  case SearchIndexDefinition search -> searchList.add(search);
                  case VectorIndexDefinition vector -> vectorList.add(vector);
                }
              });

      // Validate catalog invariants over the new final state of the authoritative catalog.
      // These invariants will also be validated when each mongot fetches the authoritative
      // catalog and applies the changes to its local index catalog, so it is OK to not check
      // intermediate states here.
      Invariants.validateInvariants(List.of(), searchList, vectorList, existingIndexesFromCatalog);
      AnalyzerInvariants.validate(searchList, List.of());
      for (SearchIndexDefinition searchIndex : searchList) {
        AnalyzerInvariants.validateFieldAnalyzerReferences(searchIndex, Set.of(), Set.of());
      }
      Invariants.validateVectorNestedRootReferences(vectorList);

      for (InternalAndExternalDefinition indexToCreate : deduplicatedIndexes) {
        try {
          this.authoritativeIndexCatalog.createIndex(
              new AuthoritativeIndexKey(this.collectionUuid, indexToCreate.internal.getName()),
              indexToCreate.internal,
              indexToCreate.external);
        } catch (Exception e) {
          if (e instanceof MetadataServiceException mse
              && mse.getCause() instanceof MongoWriteException mwe
              && mwe.getError().getCategory() == ErrorCategory.DUPLICATE_KEY) {
            // If the write failed due to duplicate key, meaning the index didn't exist when we
            // initially checked and was added to deduplicatedIndexes, but now does exist,
            // revalidate the index definition for equivalency and throw an error iff they are not
            // equivalent.
            Optional<IndexDefinition> existingIndex =
                this.authoritativeIndexCatalog.listIndexDefinitions(this.collectionUuid).stream()
                    .filter(idx -> idx.getName().equals(indexToCreate.internal.getName()))
                    .findFirst();
            if (existingIndex.isPresent()
                && !IndexMapper.areEquivalent(existingIndex.get(), indexToCreate.internal)) {
              return indexExistsWithDifferentDefinitionError(indexToCreate.internal);
            }
            // If index exists with equivalent definition, continue to next index
            if (existingIndex.isPresent()) {
              continue;
            }
            // If index not found, fall through to raise the error
          }
          String message =
              String.format(
                  "Error creating index '%s': %s",
                  Objects.requireNonNull(indexToCreate).internal.getName(),
                  Objects.requireNonNullElse(e.getMessage(), "unknown error"));
          return MessageUtils.createError(Errors.COMMAND_FAILED, message);
        }
      }

      List<NamedSearchIndexId> responseData =
          newIndexes.stream()
              .map(
                  id ->
                      new NamedSearchIndexId(
                          id.internal.getName(), id.internal.getIndexId().toString()))
              .collect(Collectors.toList());

      return new CreateSearchIndexesResponse(responseData).toBson();
    } catch (InvariantException | InvalidIndexDefinitionException | MetadataServiceException e) {
      String message = Objects.requireNonNullElse(e.getMessage(), "unknown error");
      return MessageUtils.createError(Errors.COMMAND_FAILED, message);
    }
  }

  private static BsonDocument indexExistsWithDifferentDefinitionError(IndexDefinition newIndex) {
    return MessageUtils.createError(
        Errors.INDEX_ALREADY_EXISTS,
        String.format(
            "Index %s already exists with a different definition. Drop it first if needed.",
            newIndex.getName()));
  }

  private InternalAndExternalDefinition toInternalAndExternalDef(
      NamedSearchIndex external, Optional<ViewDefinition> view) {

    return new InternalAndExternalDefinition(
        IndexMapper.toInternal(
            external.name(),
            Optional.empty(),
            external.definition(),
            this.collectionUuid,
            this.db,
            this.collectionName,
            view,
            0L,
            Instant.now()),
        external.definitionBson());
  }

  record InternalAndExternalDefinition(IndexDefinition internal, BsonDocument external) {}

  @Override
  public ExecutionPolicy getExecutionPolicy() {
    return ExecutionPolicy.ASYNC;
  }
}
