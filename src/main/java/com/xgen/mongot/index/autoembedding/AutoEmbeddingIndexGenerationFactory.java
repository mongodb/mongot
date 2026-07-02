package com.xgen.mongot.index.autoembedding;

import static com.xgen.mongot.embedding.utils.AutoEmbeddingIndexDefinitionUtils.getDerivedSearchIndexDefinition;
import static com.xgen.mongot.embedding.utils.AutoEmbeddingIndexDefinitionUtils.getDerivedVectorIndexDefinition;
import static com.xgen.mongot.index.definition.MaterializedViewIndexDefinitionGeneration.MIN_VERSION_FOR_MATERIALIZED_VIEW_EMBEDDING;

import com.xgen.mongot.catalog.InitializedIndexCatalog;
import com.xgen.mongot.embedding.exceptions.MaterializedViewTransientException;
import com.xgen.mongot.index.Index;
import com.xgen.mongot.index.IndexFactory;
import com.xgen.mongot.index.IndexGeneration;
import com.xgen.mongot.index.InitializedIndex;
import com.xgen.mongot.index.analyzer.InvalidAnalyzerDefinitionException;
import com.xgen.mongot.index.definition.AnalyzerBoundSearchIndexDefinition;
import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.definition.IndexDefinitionGeneration;
import com.xgen.mongot.index.definition.MaterializedViewIndexDefinitionGeneration;
import com.xgen.mongot.index.definition.SearchIndexDefinition;
import com.xgen.mongot.index.definition.SearchIndexDefinitionGeneration;
import com.xgen.mongot.index.definition.VectorIndexDefinition;
import com.xgen.mongot.index.definition.VectorIndexDefinitionGeneration;
import com.xgen.mongot.index.version.MaterializedViewGeneration;
import com.xgen.mongot.util.Check;
import java.io.IOException;
import java.util.Optional;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Factory that generates composite AutoEmbedding index generation, used by ConfigManager to create
 * new IndexGeneration for any index lifecycle events (staged/live/recovered). Composite
 * IndexGeneration is used for easier index state management and status consolidation between
 * Materialized View and Lucene. For example, steady state Lucene on generationID:123_f1_u1_a1 could
 * be un-queryable if corresponding Materialized View status is in initial sync. Supports both
 * vector and search index types.
 */
public class AutoEmbeddingIndexGenerationFactory {
  private static final Logger LOG =
      LoggerFactory.getLogger(AutoEmbeddingIndexGenerationFactory.class);

  /**
   * Returns a new AutoEmbeddingIndexGeneration or an UnresolvedAutoEmbeddingIndexGeneration if
   * syncSource is not available.
   */
  public static IndexGeneration getAutoEmbeddingIndexGeneration(
      IndexFactory indexFactory,
      MaterializedViewIndexFactory matViewIndexFactory,
      IndexDefinitionGeneration rawDefinitionGeneration,
      InitializedIndexCatalog initializedIndexCatalog,
      BooleanSupplier isReplicationStoppedSupplier)
      throws IOException, InvalidAnalyzerDefinitionException {
    IndexDefinition indexDef = rawDefinitionGeneration.getIndexDefinition();
    Check.checkArg(
        indexDef.isAutoEmbeddingIndex()
            && indexDef.getParsedAutoEmbeddingFeatureVersion()
                >= MIN_VERSION_FOR_MATERIALIZED_VIEW_EMBEDDING,
        "Input definition is not materialized view based auto-embedding index");

    // Source collection not on this node's mongod (e.g. unsharded collection on a non-owner
    // shard): skip the MV collection, lease, and leaderStatus gauge, and report
    // COLLECTION_NOT_FOUND so the recovery loop re-stages the index when the collection appears
    // on this shard.
    if (matViewIndexFactory.isSourceCollectionAbsentOnMongod(indexDef)) {
      // Debug, not info: this fires whenever the index is (re)added/staged (e.g. on every restart)
      // for non-owner shards that don't host the collection — the steady-state case this PR
      // targets — so keep it quiet. The COLLECTION_NOT_FOUND index status is the durable signal;
      // the index add itself is already logged by the caller (IndexActions).
      LOG.atDebug()
          .addKeyValue("generationId", rawDefinitionGeneration.getGenerationId())
          .addKeyValue("database", indexDef.getDatabase())
          .addKeyValue("collectionUuid", indexDef.getCollectionUuid())
          .log(
              "Source collection not found on this shard; skipping materialized view setup and "
                  + "reporting COLLECTION_NOT_FOUND");
      return UnresolvedAutoEmbeddingIndexGeneration.createCollectionNotFound(
          rawDefinitionGeneration);
    }

    InitializedMaterializedViewIndex matViewIndex;
    try {
      matViewIndex =
          matViewIndexFactory.getIndex(
              createMaterializedViewIndexDefinitionGeneration(rawDefinitionGeneration));
    } catch (MaterializedViewTransientException e) {
      LOG.atError()
          .setCause(e)
          .addKeyValue("generationId", rawDefinitionGeneration.getGenerationId())
          .log(
              "Failed to create materialized view index, "
                  + "creates unresolved index instead, will retry it when sync source is updated.");
      return UnresolvedAutoEmbeddingIndexGeneration.createFailed(
          rawDefinitionGeneration, e.getReason());
    }

    IndexDefinitionGeneration derivedDefGen =
        derivedIndexDefinitionGeneration(rawDefinitionGeneration, matViewIndex);
    Index derivedIndex = indexFactory.getIndex(derivedDefGen);
    // Supplier that looks up initialized index from catalog
    Supplier<Optional<InitializedIndex>> initializedIndexSupplier =
        () -> initializedIndexCatalog.getIndex(derivedDefGen.getGenerationId());
    LOG.atInfo()
        .addKeyValue("generationId", rawDefinitionGeneration.getGenerationId())
        .log("Created auto-embedding composite index generation");
    return new AutoEmbeddingIndexGeneration(
        new AutoEmbeddingCompositeIndex(
            matViewIndex, derivedIndex, initializedIndexSupplier, isReplicationStoppedSupplier),
        rawDefinitionGeneration,
        derivedDefGen);
  }

  /**
   * Helper function to check whether IndexGeneration resolution fails when generating derived
   * definition for auto embedding index, only returns true when auto-embedding index is unresolved.
   */
  public static boolean isAutoEmbeddingResolutionFailed(IndexGeneration indexGeneration) {
    return indexGeneration instanceof UnresolvedAutoEmbeddingIndexGeneration
        || indexGeneration.getIndex() instanceof UnresolvedAutoEmbeddingIndex;
  }

  static MaterializedViewIndexDefinitionGeneration createMaterializedViewIndexDefinitionGeneration(
      IndexDefinitionGeneration rawDefinitionGeneration) {
    return new MaterializedViewIndexDefinitionGeneration(
        rawDefinitionGeneration.getIndexDefinition(),
        new MaterializedViewGeneration(rawDefinitionGeneration.generation()));
  }

  static IndexDefinitionGeneration derivedIndexDefinitionGeneration(
      IndexDefinitionGeneration rawDefinitionGeneration,
      InitializedMaterializedViewIndex matViewIndex) {
    IndexDefinition def = rawDefinitionGeneration.getIndexDefinition();
    return switch (def) {
      case VectorIndexDefinition vectorDef ->
          new VectorIndexDefinitionGeneration(
              getDerivedVectorIndexDefinition(
                  vectorDef,
                  matViewIndex.getMaterializedViewDatabaseName(),
                  matViewIndex.getMaterializedViewCollectionUuid(),
                  matViewIndex.getSchemaMetadata()),
              rawDefinitionGeneration.generation());
      case SearchIndexDefinition searchDef -> {
        SearchIndexDefinition derivedSearchDef =
            getDerivedSearchIndexDefinition(
                searchDef,
                matViewIndex.getMaterializedViewDatabaseName(),
                matViewIndex.getMaterializedViewCollectionUuid(),
                matViewIndex.getSchemaMetadata());
        // A search-typed IndexDefinition reaches the factory only via
        // SearchIndexDefinitionGeneration.
        // MaterializedViewIndexDefinitionGeneration is constructed by this factory, not passed in.
        SearchIndexDefinitionGeneration rawSearchGen =
            Check.instanceOf(rawDefinitionGeneration, SearchIndexDefinitionGeneration.class);
        AnalyzerBoundSearchIndexDefinition analyzerBound =
            AnalyzerBoundSearchIndexDefinition.withRelevantOverriddenAnalyzers(
                derivedSearchDef, rawSearchGen.definition().analyzerDefinitions());
        yield new SearchIndexDefinitionGeneration(
            analyzerBound, rawDefinitionGeneration.generation());
      }
    };
  }
}
