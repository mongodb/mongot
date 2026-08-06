package com.xgen.mongot.index.autoembedding;

import static com.xgen.mongot.embedding.utils.AutoEmbeddingIndexDefinitionUtils.getDerivedSearchIndexDefinition;
import static com.xgen.mongot.embedding.utils.AutoEmbeddingIndexDefinitionUtils.getDerivedVectorIndexDefinition;
import static com.xgen.mongot.index.definition.MaterializedViewIndexDefinitionGeneration.MIN_VERSION_FOR_MATERIALIZED_VIEW_EMBEDDING;

import com.google.common.annotations.VisibleForTesting;
import com.xgen.mongot.catalog.InitializedIndexCatalog;
import com.xgen.mongot.embedding.config.MaterializedViewCollectionMetadata;
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
import com.xgen.mongot.index.status.IndexStatus;
import com.xgen.mongot.index.version.MaterializedViewGeneration;
import com.xgen.mongot.replication.mongodb.common.AutoEmbeddingCommitMetadata;
import com.xgen.mongot.replication.mongodb.common.IndexCommitUserData;
import com.xgen.mongot.replication.mongodb.common.ResumeTokenUtils;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.FieldPath;
import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.commons.codec.DecoderException;
import org.bson.BsonTimestamp;
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

    // Source collection absent on this node's mongod (e.g. unsharded collection on a non-owner
    // shard): skip the MV, lease, and leaderStatus gauge and report COLLECTION_NOT_FOUND so the
    // recovery loop re-stages once the collection appears on this shard.
    if (matViewIndexFactory.isSourceCollectionAbsentOnMongod(indexDef)) {
      // Debug, not info: fires on every (re)add/stage for non-owner shards, so keep it quiet. The
      // COLLECTION_NOT_FOUND status is the durable signal.
      LOG.atDebug()
          .addKeyValue("generationId", rawDefinitionGeneration.getGenerationId())
          .addKeyValue("database", indexDef.getDatabase())
          .addKeyValue("collectionUuid", indexDef.getCollectionUuid())
          .log(
              "Source collection not found on this shard; skipping materialized view setup and "
                  + "reporting COLLECTION_NOT_FOUND");
      // A persisted commit distinguishes a former owner with derived data to reclaim from a shard
      // that never owned the source collection.
      Optional<Index> derivedIndexForCleanup =
          indexFactory.readPersistedCommitUserData(rawDefinitionGeneration).isPresent()
              ? createCleanupIndex(indexFactory, rawDefinitionGeneration)
              : Optional.empty();
      return createWithGenerationMetrics(
          matViewIndexFactory,
          rawDefinitionGeneration,
          generationMetrics -> {
            generationMetrics.recordCollectionNotFound();
            return UnresolvedAutoEmbeddingIndexGeneration.createCollectionNotFound(
                rawDefinitionGeneration, derivedIndexForCleanup, generationMetrics);
          });
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
      return createWithGenerationMetrics(
          matViewIndexFactory,
          rawDefinitionGeneration,
          generationMetrics -> {
            generationMetrics.recordUnresolved(e.getReason());
            return offlineUnresolvedGeneration(
                indexFactory, rawDefinitionGeneration, e.getReason(), generationMetrics);
          });
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
    return createWithGenerationMetrics(
        matViewIndexFactory,
        rawDefinitionGeneration,
        generationMetrics -> {
          generationMetrics.recordResolved(
              matViewIndex.getGenerationId(),
              matViewIndex.getMaterializedViewCollectionMetadata().collectionName());
          return new AutoEmbeddingIndexGeneration(
              new AutoEmbeddingCompositeIndex(
                  matViewIndex,
                  derivedIndex,
                  initializedIndexSupplier,
                  isReplicationStoppedSupplier,
                  generationMetrics),
              rawDefinitionGeneration,
              derivedDefGen);
        });
  }

  private static IndexGeneration createWithGenerationMetrics(
      MaterializedViewIndexFactory matViewIndexFactory,
      IndexDefinitionGeneration rawDefinitionGeneration,
      Function<AutoEmbeddingGenerationMetrics, IndexGeneration> createGeneration) {
    AutoEmbeddingGenerationMetrics generationMetrics =
        matViewIndexFactory.createAutoEmbeddingGenerationMetrics(rawDefinitionGeneration);
    try {
      return createGeneration.apply(generationMetrics);
    } catch (RuntimeException | Error e) {
      generationMetrics.close();
      throw e;
    }
  }

  /**
   * Helper function to check whether IndexGeneration resolution fails when generating derived
   * definition for auto embedding index, only returns true when auto-embedding index is unresolved.
   */
  public static boolean isAutoEmbeddingResolutionFailed(IndexGeneration indexGeneration) {
    return indexGeneration instanceof UnresolvedAutoEmbeddingIndexGeneration
        || indexGeneration.getIndex() instanceof UnresolvedAutoEmbeddingIndex;
  }

  /** TEST ONLY: do not use this helper in production recovery. */
  @VisibleForTesting
  public static Optional<InitializedIndex> reconstructOfflineDerivedVectorIndex(
      IndexFactory indexFactory, IndexDefinitionGeneration rawDefinitionGeneration) {
    Optional<AutoEmbeddingCommitMetadata> metadata =
        readPersistedCommit(indexFactory, rawDefinitionGeneration)
            .flatMap(IndexCommitUserData::getAutoEmbeddingMetadata);
    return reconstructOfflineDerivedIndex(indexFactory, rawDefinitionGeneration, metadata)
        .flatMap(derived -> initializeDerivedIndexForOfflineServing(indexFactory, derived));
  }

  /** The derived Lucene index rebuilt offline from the commit, paired with its definition. */
  private record ReconstructedDerived(
      Index index, IndexDefinitionGeneration definitionGeneration) {}

  /** Reconstructs from already-read commit metadata without opening a reader. */
  private static Optional<ReconstructedDerived> reconstructOfflineDerivedIndex(
      IndexFactory indexFactory,
      IndexDefinitionGeneration rawDefinitionGeneration,
      Optional<AutoEmbeddingCommitMetadata> metadata) {
    if (metadata.isEmpty()) {
      return Optional.empty();
    }
    Optional<String> databaseName = metadata.get().materializedViewDatabaseName();
    Optional<MaterializedViewCollectionMetadata> collectionMetadata =
        metadata.get().materializedViewCollectionMetadata();
    if (databaseName.isEmpty() || collectionMetadata.isEmpty()) {
      return Optional.empty();
    }
    // TODO(CLOUDP-353553): support SearchIndexDefinition here.
    if (!(rawDefinitionGeneration.getIndexDefinition()
        instanceof VectorIndexDefinition vectorDef)) {
      // Proof scope: only vector auto-embedding indexes are reconstructed offline for now.
      return Optional.empty();
    }
    try {
      // Rebuilding from persisted schema metadata can throw unchecked (malformed/incompatible); the
      // catch degrades to empty. Returned as the base InitializedIndex (always Lucene-backed).
      VectorIndexDefinition derivedDefinition =
          getDerivedVectorIndexDefinition(
              vectorDef,
              databaseName.get(),
              collectionMetadata.get().collectionUuid(),
              collectionMetadata.get().schemaMetadata());
      IndexDefinitionGeneration derivedDefGen =
          new VectorIndexDefinitionGeneration(
              derivedDefinition, rawDefinitionGeneration.generation());
      Index derivedIndex = indexFactory.getIndex(derivedDefGen);
      return Optional.of(new ReconstructedDerived(derivedIndex, derivedDefGen));
    } catch (IOException | InvalidAnalyzerDefinitionException | RuntimeException e) {
      LOG.atWarn()
          .setCause(e)
          .addKeyValue("generationId", rawDefinitionGeneration.getGenerationId())
          .log("Failed to reconstruct derived Lucene index offline from persisted commit metadata");
      return Optional.empty();
    }
  }

  /**
   * Initializes a reconstructed derived Lucene index for offline querying. The unresolved index
   * owns and closes the returned index. Empty when initialization fails.
   */
  private static Optional<InitializedIndex> initializeDerivedIndexForOfflineServing(
      IndexFactory indexFactory, ReconstructedDerived derived) {
    try {
      return Optional.of(
          indexFactory.getInitializedIndex(derived.index(), derived.definitionGeneration()));
    } catch (IOException | RuntimeException e) {
      LOG.atWarn()
          .setCause(e)
          .addKeyValue("generationId", derived.definitionGeneration().getGenerationId())
          .log("Failed to initialize reconstructed derived Lucene index for offline serving");
      return Optional.empty();
    }
  }

  /**
   * Builds an unresolved generation from the last on-disk commit. A ready commit with a recoverable
   * optime is opened and served STALE. Other transient-failure cases with a persisted commit retain
   * an unopened index handle for cleanup so phasing out the generation deletes its derived Lucene
   * directory.
   */
  private static UnresolvedAutoEmbeddingIndexGeneration offlineUnresolvedGeneration(
      IndexFactory indexFactory,
      IndexDefinitionGeneration rawDefinitionGeneration,
      MaterializedViewTransientException.Reason reason,
      AutoEmbeddingGenerationMetrics generationMetrics) {
    Optional<IndexCommitUserData> commit =
        readPersistedCommit(indexFactory, rawDefinitionGeneration);
    Optional<AutoEmbeddingCommitMetadata> metadata =
        commit.flatMap(IndexCommitUserData::getAutoEmbeddingMetadata);
    Optional<ReconstructedDerived> reconstructed =
        reconstructOfflineDerivedIndex(indexFactory, rawDefinitionGeneration, metadata);

    if (reconstructed.isPresent()
        && metadata.map(AutoEmbeddingCommitMetadata::wasMatViewReady).orElse(false)) {
      Optional<BsonTimestamp> optime =
          commit.flatMap(AutoEmbeddingIndexGenerationFactory::lastCommittedOptime);
      if (optime.isPresent()) {
        // Unresolved generations skip IndexLifecycleManager, so this is the only initialization.
        Optional<InitializedIndex> served =
            initializeDerivedIndexForOfflineServing(indexFactory, reconstructed.get());
        if (served.isPresent()) {
          served
              .get()
              .setStatus(
                  IndexStatus.stale(
                      "Serving Automated Embedding index from last committed data while its source "
                          + "is unavailable",
                      optime.get()));
          return UnresolvedAutoEmbeddingIndexGeneration.createStale(
              rawDefinitionGeneration,
              served.get(),
              persistedAutoEmbeddingFieldsMapping(metadata),
              generationMetrics);
        }
      }
    }

    Optional<Index> derivedIndexForCleanup =
        reconstructed
            .map(ReconstructedDerived::index)
            .or(
                () ->
                    commit.isPresent()
                        ? createCleanupIndex(indexFactory, rawDefinitionGeneration)
                        : Optional.empty());
    return UnresolvedAutoEmbeddingIndexGeneration.createFailed(
        rawDefinitionGeneration, reason, derivedIndexForCleanup, generationMetrics);
  }

  /**
   * Creates an unopened index handle whose backing paths are derived from the raw generation. The
   * derived Lucene uses the same index id and generation, so this handle targets the same data and
   * metadata directories even when old commit metadata is insufficient to rebuild its definition.
   */
  private static Optional<Index> createCleanupIndex(
      IndexFactory indexFactory, IndexDefinitionGeneration rawDefinitionGeneration) {
    try {
      return Optional.of(indexFactory.getIndex(rawDefinitionGeneration));
    } catch (IOException | InvalidAnalyzerDefinitionException | RuntimeException e) {
      LOG.atWarn()
          .setCause(e)
          .addKeyValue("generationId", rawDefinitionGeneration.getGenerationId())
          .log("Failed to create derived Lucene cleanup handle for unresolved index");
      return Optional.empty();
    }
  }

  /**
   * The last committed optime for the STALE status: the explicit stale-state optime, else decoded
   * from the change-stream resume token. Empty when the commit carries neither.
   */
  private static Optional<BsonTimestamp> lastCommittedOptime(IndexCommitUserData commit) {
    if (commit.getStaleStateInfo().isPresent()) {
      return Optional.of(commit.getStaleStateInfo().get().getLastOptime());
    }
    if (commit.getResumeInfo().isEmpty()) {
      return Optional.empty();
    }
    try {
      return Optional.of(
          ResumeTokenUtils.opTimeFromResumeToken(commit.getResumeInfo().get().getResumeToken()));
    } catch (DecoderException | RuntimeException e) {
      // A malformed resume token must not abort resolution; without an optime the caller falls back
      // to FAILED/retry.
      LOG.atWarn()
          .setCause(e)
          .log("Failed to decode last committed optime from persisted resume token");
      return Optional.empty();
    }
  }

  /** Reads the derived index's last on-disk commit without contacting mongod. */
  private static Optional<IndexCommitUserData> readPersistedCommit(
      IndexFactory indexFactory, IndexDefinitionGeneration rawDefinitionGeneration) {
    return indexFactory
        .readPersistedCommitUserData(rawDefinitionGeneration)
        .map(
            encoded ->
                IndexCommitUserData.fromEncodedData(
                    encoded, Optional.of(rawDefinitionGeneration.getGenerationId())));
  }

  /**
   * The persisted user-to-internal field mapping (e.g. {@code "title" -> "_autoEmbed.title"}) used
   * to remap query paths while serving offline, since the unresolved view cannot supply it. Empty
   * when the commit carries no collection metadata.
   */
  private static Map<FieldPath, FieldPath> persistedAutoEmbeddingFieldsMapping(
      Optional<AutoEmbeddingCommitMetadata> commitMetadata) {
    return commitMetadata
        .flatMap(AutoEmbeddingCommitMetadata::materializedViewCollectionMetadata)
        .map(metadata -> metadata.schemaMetadata().autoEmbeddingFieldsMapping())
        .orElse(Map.of());
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
        // A search-typed definition reaches the factory only via SearchIndexDefinitionGeneration;
        // MaterializedViewIndexDefinitionGeneration is built here, not passed in.
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
