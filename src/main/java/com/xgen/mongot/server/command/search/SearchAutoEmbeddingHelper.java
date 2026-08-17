package com.xgen.mongot.server.command.search;

import static com.xgen.mongot.embedding.utils.AutoEmbedFieldMappingCreator.getMatViewFieldPath;
import static com.xgen.mongot.index.definition.IndexDefinitionGeneration.Type.AUTO_EMBEDDING;

import com.google.common.base.Supplier;
import com.xgen.mongot.catalog.IndexCatalog;
import com.xgen.mongot.embedding.EmbeddingRequestContext;
import com.xgen.mongot.embedding.VectorOrError;
import com.xgen.mongot.embedding.providers.EmbeddingServiceManager;
import com.xgen.mongot.embedding.providers.configs.EmbeddingModelCatalog;
import com.xgen.mongot.embedding.providers.configs.EmbeddingServiceConfig;
import com.xgen.mongot.index.IndexGeneration;
import com.xgen.mongot.index.autoembedding.AutoEmbeddingIndexGeneration;
import com.xgen.mongot.index.autoembedding.UnresolvedAutoEmbeddingIndex;
import com.xgen.mongot.index.definition.FieldDefinition;
import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.definition.SearchAutoEmbedFieldDefinition;
import com.xgen.mongot.index.definition.SearchIndexDefinition;
import com.xgen.mongot.index.query.InvalidQueryException;
import com.xgen.mongot.index.query.operators.VectorSearchCriteria;
import com.xgen.mongot.util.FieldPath;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Embeds text queries against an auto-embedding index: resolves the auto-embedding definition,
 * validates the embedding model for a queried field, calls the embedding service, and returns
 * criteria with the resulting query vector. Shared by the $search and $vectorSearch command paths.
 */
final class SearchAutoEmbeddingHelper {

  private static final Logger LOG = LoggerFactory.getLogger(SearchAutoEmbeddingHelper.class);

  private SearchAutoEmbeddingHelper() {}

  /**
   * Resolves the auto-embedding definition backing a query. When {@code initializedDefinition} is
   * the derived (non-auto-embedding) index exposed for a materialized view, the raw auto-embedding
   * definition carrying the model configuration is looked up from the catalog.
   *
   * @return the auto-embedding definition, or empty when none backs the query
   */
  static Optional<IndexDefinition> resolveAutoEmbeddingDefinition(
      IndexCatalog indexCatalog,
      String databaseName,
      UUID collectionUuid,
      Optional<String> viewName,
      String indexName,
      IndexDefinition initializedDefinition) {
    if (initializedDefinition.isAutoEmbeddingIndex()) {
      return Optional.of(initializedDefinition);
    }
    return indexCatalog
        .getIndex(databaseName, collectionUuid, viewName, indexName)
        .map(IndexGeneration::getDefinition)
        .filter(IndexDefinition::isAutoEmbeddingIndex);
  }

  /**
   * Resolves the source-path to materialized-view-path mapping for the auto-embedding index backing
   * a query: from the live materialized view when the index is resolved, otherwise (offline, view
   * unresolved) from the mapping persisted with the reconstructed derived index. Empty when no
   * auto-embedding index backs the query.
   */
  static Map<FieldPath, FieldPath> resolveAutoEmbeddingFieldsMapping(
      IndexCatalog indexCatalog,
      String databaseName,
      UUID collectionUuid,
      Optional<String> viewName,
      String indexName) {
    Optional<IndexGeneration> maybeGeneration =
        indexCatalog.getIndex(databaseName, collectionUuid, viewName, indexName);
    if (maybeGeneration.isEmpty()) {
      return Map.of();
    }
    IndexGeneration generation = maybeGeneration.get();
    if (generation.getType() == AUTO_EMBEDDING) {
      return ((AutoEmbeddingIndexGeneration) generation)
          .getMaterializedViewIndexGeneration()
          .getIndex()
          .getSchemaMetadata()
          .autoEmbeddingFieldsMapping();
    }
    if (generation.getIndex() instanceof UnresolvedAutoEmbeddingIndex unresolved) {
      return unresolved.getAutoEmbeddingFieldsMapping();
    }
    return Map.of();
  }

  /**
   * Picks the embedding model for a query: the index's own model when no query model is supplied,
   * otherwise the query model after checking that it is allowed and compatible with the index
   * model.
   *
   * @throws InvalidQueryException if the query model is not allowed or not compatible with the
   *     index model
   */
  static String resolveQueryModel(String indexModelName, Optional<String> queryModel)
      throws InvalidQueryException {
    if (queryModel.isEmpty()) {
      LOG.atDebug()
          .addKeyValue("indexModel", indexModelName)
          .log("Using indexing model for embedding query (no query model specified)");
      return indexModelName;
    }
    if (!EmbeddingModelCatalog.isQueryModelAllowed(queryModel.get())) {
      throw new InvalidQueryException(
          String.format(
              "model '%s' is not allowed. Allowed models are: %s",
              queryModel.get(), EmbeddingModelCatalog.getAllowedQueryModels()));
    }
    // Catalog model keys are stored lowercased: getCompatibleIndexModels and isQueryModelAllowed
    // lowercase queryModel internally, so only indexModelName needs normalizing for the lookup.
    if (!EmbeddingModelCatalog.getCompatibleIndexModels(queryModel.get())
        .contains(indexModelName.toLowerCase())) {
      throw new InvalidQueryException(
          String.format(
              "query model '%s' is not compatible with the index model '%s'",
              queryModel.get(), indexModelName));
    }
    LOG.atDebug()
        .addKeyValue("queryModel", queryModel.get())
        .addKeyValue("indexModel", indexModelName)
        .log("Using user-specified model for embedding (compatible with indexing model)");
    return queryModel.get();
  }

  /**
   * Embeds {@code textToEmbed} against {@code searchDef} and returns a copy of {@code criteria}
   * with the resulting vector replacing the text input and the path retargeted, via {@code
   * autoEmbeddingFieldsMapping}, to the materialized-view field path the derived search index
   * indexes the embedding under. The model is the queried field's configured model, or a compatible
   * user-supplied {@code queryModel}; dimensions and quantization come from the field's
   * specification.
   *
   * @return a new {@link VectorSearchCriteria} with the embedded vector and materialized-view path
   */
  static VectorSearchCriteria embedAndReplaceCriteria(
      VectorSearchCriteria criteria,
      String textToEmbed,
      Optional<String> queryModel,
      SearchIndexDefinition searchDef,
      Map<FieldPath, FieldPath> autoEmbeddingFieldsMapping,
      Supplier<EmbeddingServiceManager> embeddingServiceManagerSupplier)
      throws InvalidQueryException {

    FieldPath path = criteria.path();
    // Resolve the auto-embed field by destination path. getRootFields() is keyed by full dotted
    // FieldPath, so nested paths (e.g. a.b) resolve too, unlike the top-level-only fields() map.
    SearchAutoEmbedFieldDefinition autoEmbed =
        Optional.ofNullable(
                searchDef.getMappings().fieldHierarchyContext().getRootFields().get(path))
            .flatMap(FieldDefinition::searchAutoEmbedFieldDefinition)
            .orElseThrow(
                () ->
                    new InvalidQueryException(
                        String.format(
                            "No auto-embedding model configured for field path '%s'", path)));
    String modelName = resolveQueryModel(autoEmbed.modelName(), queryModel);
    var autoEmbedSpec = autoEmbed.specification();

    EmbeddingServiceManager embeddingServiceManager = embeddingServiceManagerSupplier.get();
    if (embeddingServiceManager == null) {
      throw new InvalidQueryException(
          "Embedding service not initialized for search auto-embedding");
    }

    EmbeddingRequestContext context =
        new EmbeddingRequestContext(
            searchDef.getDatabase(),
            searchDef.getName(),
            searchDef.getLastObservedCollectionName(),
            autoEmbedSpec.numDimensions(),
            autoEmbedSpec.autoEmbedQuantization());

    List<VectorOrError> results =
        embeddingServiceManager.embed(
            List.of(textToEmbed),
            EmbeddingModelCatalog.getModelConfig(modelName),
            EmbeddingServiceConfig.ServiceTier.QUERY,
            context);
    if (results.size() != 1) {
      throw new InvalidQueryException(
          "Expected exactly one embedding result, got " + results.size());
    }
    VectorOrError result = results.get(0);
    if (result.vector.isEmpty()) {
      throw new InvalidQueryException(
          "Failed to embed query text: " + result.errorMessage.orElse("unknown error"));
    }

    // Retarget the query at the materialized-view field path that holds the embedded vector; the
    // derived search index keys the auto-embed field by that path, not the user-facing path.
    FieldPath matViewPath =
        getMatViewFieldPath(autoEmbed.sourceField(), autoEmbeddingFieldsMapping);
    return criteria.withQueryVector(result.vector.get()).withPath(matViewPath);
  }
}
