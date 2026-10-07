package com.xgen.mongot.server.command.search;

import static com.xgen.mongot.index.definition.IndexDefinition.Type.VECTOR_SEARCH;
import static com.xgen.mongot.index.definition.IndexDefinitionGeneration.Type.AUTO_EMBEDDING;

import com.google.common.base.Supplier;
import com.google.errorprone.annotations.Var;
import com.xgen.mongot.catalog.IndexCatalog;
import com.xgen.mongot.embedding.EmbeddingRequestContext;
import com.xgen.mongot.embedding.VectorOrError;
import com.xgen.mongot.embedding.exceptions.EmbeddingProviderNonTransientException;
import com.xgen.mongot.embedding.exceptions.EmbeddingProviderTransientException;
import com.xgen.mongot.embedding.providers.EmbeddingServiceManager;
import com.xgen.mongot.embedding.providers.configs.EmbeddingModelCatalog;
import com.xgen.mongot.embedding.providers.configs.EmbeddingServiceConfig;
import com.xgen.mongot.index.IndexGeneration;
import com.xgen.mongot.index.autoembedding.AutoEmbeddingIndexGeneration;
import com.xgen.mongot.index.autoembedding.UnresolvedAutoEmbeddingIndex;
import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.definition.VectorAutoEmbedFieldDefinition;
import com.xgen.mongot.index.definition.VectorAutoEmbedFieldSpecification;
import com.xgen.mongot.index.definition.VectorFieldSpecification;
import com.xgen.mongot.index.definition.VectorIndexDefinition;
import com.xgen.mongot.index.definition.VectorIndexFieldDefinition;
import com.xgen.mongot.index.definition.quantization.VectorAutoEmbedQuantization;
import com.xgen.mongot.index.query.AutoEmbedQueryVectorReconciler;
import com.xgen.mongot.index.query.InvalidQueryException;
import com.xgen.mongot.index.query.MaterializedVectorSearchQuery;
import com.xgen.mongot.index.query.VectorSearchQuery;
import com.xgen.mongot.index.query.operators.VectorSearchCriteria;
import com.xgen.mongot.index.query.operators.VectorSearchQueryInput;
import com.xgen.mongot.trace.Tracing;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.bson.Vector;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Turns a {@code $vectorSearch} query into the vector it will search with: a user-supplied {@code
 * queryVector} is reconciled against the index's auto-embed encoding, and query text is embedded
 * through the auto-embedding provider using the model the index was built with.
 *
 * <p>This is the only place query text is embedded for vector search, so every caller (the
 * per-shard search, and the router-side embed-once step for sharded clusters) produces the same
 * vector for the same text and index. It reads index definitions and the embedding service only; it
 * never touches a Lucene reader, so it does not require the index to be queryable locally.
 *
 * <p>One instance serves one namespace: the {@code (database, collectionUuid, viewName)} the query
 * is addressed to, which is what the catalog lookups are keyed by.
 */
final class VectorQueryEmbedder {

  private static final Logger LOG = LoggerFactory.getLogger(VectorQueryEmbedder.class);

  private final IndexCatalog indexCatalog;
  private final Supplier<EmbeddingServiceManager> embeddingServiceManagerSupplier;
  private final String database;
  private final UUID collectionUuid;
  private final Optional<String> viewName;

  VectorQueryEmbedder(
      IndexCatalog indexCatalog,
      Supplier<EmbeddingServiceManager> embeddingServiceManagerSupplier,
      String database,
      UUID collectionUuid,
      Optional<String> viewName) {
    this.indexCatalog = indexCatalog;
    this.embeddingServiceManagerSupplier = embeddingServiceManagerSupplier;
    this.database = database;
    this.collectionUuid = collectionUuid;
    this.viewName = viewName;
  }

  /**
   * Bundles the resolved embedding model name together with the raw (source) auto-embedding index
   * definition. The source definition carries the original user-facing database name which is
   * needed by {@link EmbeddingRequestContext} for multi-tenant credential lookup.
   */
  record EmbedRequestInfo(Optional<String> modelName, IndexDefinition sourceDefinition) {}

  // EmbedRequestInfo for an auto-embedded path: with a model for text queries, model-less for
  // queryVector (whose source definition is still needed to reconcile the vector dtype).
  Optional<EmbedRequestInfo> findEmbedRequestInfo(
      IndexDefinition regularIndexDefinition, VectorSearchQuery vectorSearchQuery)
      throws InvalidQueryException {
    try (var guard = Tracing.simpleSpanGuard("VectorQueryEmbedder.findEmbedRequestInfo")) {
      boolean isTextQuery = vectorSearchQuery.criteria().query().isPresent();
      // TODO(CLOUDP-353553): Handle search index with autoEmbed field.
      if (regularIndexDefinition.getType() != VECTOR_SEARCH) {
        return Optional.empty();
      }
      @Var var vectorIndexDef = regularIndexDefinition.asVectorDefinition();
      // For the EmbedRequestInfo we need the source definition that carries the original
      // user-facing database name. For direct auto-embedding indexes this is regularIndexDefinition
      // itself; for mat-view derived indexes (whose queried definition is a plain vector spec) it
      // is the raw auto-embedding definition resolved from the catalog below, used for both the
      // model (text queries) and the quantization (queryVector reconciliation).
      if (!regularIndexDefinition.isAutoEmbeddingIndex()) {
        Optional<IndexDefinition> rawAutoEmbeddingDefinition =
            this.indexCatalog
                .getIndex(
                    this.database, this.collectionUuid, this.viewName, vectorSearchQuery.index())
                .map(IndexGeneration::getDefinition)
                .filter(IndexDefinition::isAutoEmbeddingIndex);
        if (rawAutoEmbeddingDefinition.isPresent()) {
          vectorIndexDef = rawAutoEmbeddingDefinition.get().asVectorDefinition();
        } else {
          return Optional.empty();
        }
      }
      var fieldPath = vectorSearchQuery.criteria().path();
      boolean isAutoEmbeddedPath = vectorIndexDef.getModelNamePerPath().containsKey(fieldPath);

      // queryVector needs the source definition only to reconcile against an auto-embedded path;
      // on any other path it carries no embedding request (regular vector field semantics).
      if (!isTextQuery) {
        return isAutoEmbeddedPath
            ? Optional.of(new EmbedRequestInfo(Optional.empty(), vectorIndexDef))
            : Optional.empty();
      }

      // Text query path: the path must be auto-embedded.
      if (!isAutoEmbeddedPath) {
        throw new InvalidQueryException(
            "Path '%s' is not defined in Automated Embedding index '%s'"
                .formatted(fieldPath, vectorSearchQuery.index()));
      }
      String indexModelName = vectorIndexDef.getModelNamePerPath().get(fieldPath);
      Optional<String> queryModel =
          vectorSearchQuery.criteria().query().flatMap(VectorSearchQueryInput::getModel);
      // Shared with the $search path: resolves to the index model when no query model is supplied,
      // otherwise the query model after validating it is allowed and compatible with the index
      // model.
      String resolvedModel =
          SearchAutoEmbeddingHelper.resolveQueryModel(indexModelName, queryModel);
      return Optional.of(new EmbedRequestInfo(Optional.of(resolvedModel), vectorIndexDef));
    }
  }

  // Produces the vector to search with. Exactly one of query/queryVector is present (enforced by
  // VectorSearchCriteria): a user-supplied queryVector is reconciled against the index encoding; a
  // text query is embedded into a vector via the provider.
  MaterializedVectorSearchQuery materializeQuery(
      VectorSearchQuery vectorSearchQuery, Optional<EmbedRequestInfo> embedRequestInfo)
      throws InvalidQueryException,
          EmbeddingProviderTransientException,
          EmbeddingProviderNonTransientException {
    try (var guard = Tracing.simpleSpanGuard("VectorQueryEmbedder.materializeQuery")) {
      if (vectorSearchQuery.criteria().queryVector().isPresent()) {
        return materializeUserQueryVector(vectorSearchQuery, embedRequestInfo);
      }
      return embedQueryText(vectorSearchQuery, embedRequestInfo);
    }
  }

  /**
   * The vector the query would search with: a user-supplied {@code queryVector} reconciled against
   * the index encoding, or the embedded query text. Unlike {@link #materializeQuery} this does not
   * resolve the index's internal field mapping, so it costs no catalog lookups; it is for callers
   * that only need the vector, such as {@code materializeVectorSearch}.
   */
  Vector resolveQueryVector(
      VectorSearchQuery vectorSearchQuery, Optional<EmbedRequestInfo> embedRequestInfo)
      throws InvalidQueryException,
          EmbeddingProviderTransientException,
          EmbeddingProviderNonTransientException {
    if (vectorSearchQuery.criteria().queryVector().isPresent()) {
      return reconcileUserQueryVector(vectorSearchQuery, embedRequestInfo);
    }
    return embedText(vectorSearchQuery, embedRequestInfo);
  }

  // Builds the query from a user-supplied queryVector, reconciling it against the index encoding.
  private MaterializedVectorSearchQuery materializeUserQueryVector(
      VectorSearchQuery vectorSearchQuery, Optional<EmbedRequestInfo> embedRequestInfo)
      throws InvalidQueryException {
    return new MaterializedVectorSearchQuery(
        vectorSearchQuery,
        reconcileUserQueryVector(vectorSearchQuery, embedRequestInfo),
        findAutoEmbeddingFieldsMapping(vectorSearchQuery),
        findAutoEmbedVectorSourcePaths(embedRequestInfo));
  }

  // Reconciles a user-supplied queryVector against the index's autoEmbed encoding: returns it
  // unchanged for regular type:vector (BYOV) and matching autoEmbed dtypes (dtype/dimension
  // validation happens later in VectorSearchQueryFactory); throws on a mismatch.
  private Vector reconcileUserQueryVector(
      VectorSearchQuery vectorSearchQuery, Optional<EmbedRequestInfo> embedRequestInfo)
      throws InvalidQueryException {
    try (var guard = Tracing.simpleSpanGuard("VectorQueryEmbedder.reconcileUserQueryVector")) {
      VectorSearchCriteria criteria = vectorSearchQuery.criteria();
      Vector queryVector = criteria.queryVector().get();
      Optional<VectorAutoEmbedQuantization> autoEmbedQuantization =
          embedRequestInfo
              .filter(info -> info.sourceDefinition().isAutoEmbeddingIndex())
              .flatMap(
                  info ->
                      findAutoEmbedQuantization(
                          info.sourceDefinition().asVectorDefinition(), criteria.path()));
      if (autoEmbedQuantization.isEmpty()) {
        return queryVector;
      }
      return AutoEmbedQueryVectorReconciler.reconcile(
          queryVector, autoEmbedQuantization.get(), criteria.path());
    }
  }

  // Embeds a text query into a vector via the auto-embedding provider. Only reached when there is
  // no queryVector, so a non-auto-embedding (BYOV) index here means the user omitted the required
  // queryVector.
  private MaterializedVectorSearchQuery embedQueryText(
      VectorSearchQuery vectorSearchQuery, Optional<EmbedRequestInfo> embedRequestInfo)
      throws InvalidQueryException,
          EmbeddingProviderTransientException,
          EmbeddingProviderNonTransientException {
    return new MaterializedVectorSearchQuery(
        vectorSearchQuery,
        embedText(vectorSearchQuery, embedRequestInfo),
        findAutoEmbeddingFieldsMapping(vectorSearchQuery),
        findAutoEmbedVectorSourcePaths(embedRequestInfo));
  }

  // Embeds the query text with the resolved model and returns the vector.
  private Vector embedText(
      VectorSearchQuery vectorSearchQuery, Optional<EmbedRequestInfo> embedRequestInfo)
      throws InvalidQueryException,
          EmbeddingProviderTransientException,
          EmbeddingProviderNonTransientException {
    try (var guard = Tracing.simpleSpanGuard("VectorQueryEmbedder.embedQueryText")) {
      VectorSearchCriteria criteria = vectorSearchQuery.criteria();
      Optional<String> textToEmbed = criteria.query().flatMap(VectorSearchQueryInput::getText);

      if (embedRequestInfo
          .filter(info -> info.sourceDefinition().isAutoEmbeddingIndex())
          .isEmpty()) {
        throw new InvalidQueryException("queryVector must be present");
      }

      // Text query path: requires a valid text query.
      if (criteria.query().isPresent() && textToEmbed.isEmpty()) {
        throw new InvalidQueryException(
            "'query' field cannot be empty when querying Automated Embedding index");
      }

      // Text query path: requires an auto-embedding-capable index and a resolvable model.
      Optional<String> canonicalModel = embedRequestInfo.flatMap(EmbedRequestInfo::modelName);
      if (canonicalModel.isEmpty()) {
        throw new InvalidQueryException(
            "Automated Embedding index query is invalid due to missing model");
      }
      if (this.embeddingServiceManagerSupplier.get() == null) {
        throw new EmbeddingProviderTransientException(
            "Embedding service not initialized.",
            EmbeddingProviderTransientException.Reason.SERVICE_NOT_INITIALIZED);
      }

      // Use the raw (source) auto-embedding index definition for the embedding context.
      // This carries the original user-facing database name needed for multi-tenant credential
      // lookup, as opposed to the derived definition whose database is __mdb_internal_search.
      IndexDefinition sourceDefinition = embedRequestInfo.get().sourceDefinition();
      EmbeddingRequestContext context =
          // TODO(CLOUDP-353553): Handle search index with autoEmbed field.
          prepareEmbeddingRequestContext(vectorSearchQuery, sourceDefinition.asVectorDefinition());
      List<VectorOrError> results;
      try {
        results =
            this.embeddingServiceManagerSupplier
                .get()
                .embed(
                    List.of(textToEmbed.get()),
                    EmbeddingModelCatalog.getModelConfig(canonicalModel.get()),
                    EmbeddingServiceConfig.ServiceTier.QUERY,
                    context);
      } catch (Exception e) {
        LOG.atError()
            .addKeyValue("model", canonicalModel.get())
            .addKeyValue("errorType", e.getClass().getSimpleName())
            .addKeyValue("errorMessage", e.getMessage())
            .log("Failed to embed query text");
        throw e;
      }
      var result = Check.hasSingleElement(results, "vectorized query");
      if (result.errorMessage.isPresent()) {
        LOG.atError()
            .addKeyValue("model", canonicalModel.get())
            .addKeyValue("errorMessage", result.errorMessage.get())
            .log("Embedding service returned error for query");
      }
      Check.checkState(
          result.errorMessage.isEmpty(), "Got error when embedding query: %s", result.errorMessage);
      return Check.isPresent(result.vector, "vector");
    }
  }

  /** AutoEmbed quantization for the path; empty for non-autoEmbed fields (no reconciliation). */
  private static Optional<VectorAutoEmbedQuantization> findAutoEmbedQuantization(
      VectorIndexDefinition sourceIndexDefinition, FieldPath path) {
    return sourceIndexDefinition
        .getMappings()
        .getFieldDefinition(path)
        .filter(fieldDef -> fieldDef.getType() == VectorIndexFieldDefinition.Type.AUTO_EMBED)
        .map(VectorIndexFieldDefinition::asVectorAutoEmbedField)
        .map(fieldDef -> fieldDef.specification().autoEmbedQuantization());
  }

  private EmbeddingRequestContext prepareEmbeddingRequestContext(
      VectorSearchQuery vectorSearchQuery, VectorIndexDefinition sourceIndexDefinition)
      throws InvalidQueryException {
    // Get autoEmbed field definition to resolve numDimensions & providerOutputDataType
    FieldPath sourcePath = vectorSearchQuery.criteria().path();
    Optional<VectorAutoEmbedFieldDefinition> autoEmbedField =
        sourceIndexDefinition
            .getMappings()
            .getFieldDefinition(sourcePath)
            .filter(fieldDef -> fieldDef.getType() == VectorIndexFieldDefinition.Type.AUTO_EMBED)
            .map(VectorIndexFieldDefinition::asVectorAutoEmbedField);
    if (autoEmbedField.isPresent()) {
      // MV-based auto-embedding index: use provider quantization from auto-embed spec.
      VectorAutoEmbedFieldSpecification spec = autoEmbedField.get().specification();
      return new EmbeddingRequestContext(
          sourceIndexDefinition.getDatabase(),
          sourceIndexDefinition.getName(),
          sourceIndexDefinition.getLastObservedCollectionName(),
          spec.numDimensions(),
          spec.autoEmbedQuantization());
    }
    // Type-text index: provider dtype is always float.
    VectorFieldSpecification spec =
        sourceIndexDefinition
            .getMappings()
            .getFieldDefinition(sourcePath)
            .filter(VectorIndexFieldDefinition::isVectorField)
            .map(def -> def.asVectorField().specification())
            .orElseThrow(
                () ->
                    new InvalidQueryException(
                        ("Error when preparing embedding request context: path '%s' is not defined "
                                + "in Automated Embedding index")
                            .formatted(sourcePath)));

    return new EmbeddingRequestContext(
        sourceIndexDefinition.getDatabase(),
        sourceIndexDefinition.getName(),
        sourceIndexDefinition.getLastObservedCollectionName(),
        spec.numDimensions(),
        VectorAutoEmbedQuantization.FLOAT // type:TEXT → always float32 provider output
        );
  }

  // All auto-embed (vector) source paths declared by the index, used to keep filter clauses from
  // being remapped into a vector field's MV path. The schema mapping conflates AUTO_EMBED and
  // FILTER remap entries, so MaterializedVectorSearchQuery needs the embed source paths to tell
  // them apart when an index declares more than one auto-embed field (CLOUDP-402522).
  private Set<FieldPath> findAutoEmbedVectorSourcePaths(
      Optional<EmbedRequestInfo> embedRequestInfo) {
    return embedRequestInfo
        .filter(info -> info.sourceDefinition().isAutoEmbeddingIndex())
        .map(info -> info.sourceDefinition().asVectorDefinition())
        .map(
            definition ->
                definition.getFields().stream()
                    .filter(VectorIndexFieldDefinition::isVectorField)
                    .map(VectorIndexFieldDefinition::getPath)
                    .collect(Collectors.toUnmodifiableSet()))
        .orElse(Set.of());
  }

  // TODO(CLOUDP-423143): this catalog lookup is independent of the one that picks the reader to
  // serve from, so a generation swap between them can pair a mapping with a reader from different
  // generations. Resolve the catalog entry once per query instead.
  private Map<FieldPath, FieldPath> findAutoEmbeddingFieldsMapping(
      VectorSearchQuery vectorSearchQuery) {
    Optional<IndexGeneration> maybeGeneration =
        this.indexCatalog.getIndex(
            this.database, this.collectionUuid, this.viewName, vectorSearchQuery.index());
    if (maybeGeneration.isEmpty()) {
      return Map.of();
    }
    IndexGeneration generation = maybeGeneration.get();
    // Resolved index: the mapping comes from the materialized view.
    if (generation.getType() == AUTO_EMBEDDING) {
      return ((AutoEmbeddingIndexGeneration) generation)
          .getMaterializedViewIndexGeneration()
          .getIndex()
          .getSchemaMetadata()
          .autoEmbeddingFieldsMapping();
    }
    // Offline (view unresolved): fall back to the mapping persisted with the reconstructed index,
    // without which query paths would not be remapped to the derived Lucene path.
    if (generation.getIndex() instanceof UnresolvedAutoEmbeddingIndex unresolved) {
      return unresolved.getAutoEmbeddingFieldsMapping();
    }
    return Map.of();
  }
}
