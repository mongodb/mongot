package com.xgen.mongot.index.autoembedding;

import static com.xgen.mongot.embedding.utils.AutoEmbeddingIndexDefinitionUtils.getDerivedSearchIndexDefinition;
import static com.xgen.mongot.embedding.utils.AutoEmbeddingIndexDefinitionUtils.getDerivedVectorIndexDefinition;
import static com.xgen.mongot.index.definition.MaterializedViewIndexDefinitionGeneration.MIN_VERSION_FOR_MATERIALIZED_VIEW_EMBEDDING;

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
      return staleOfflineServingGeneration(indexFactory, rawDefinitionGeneration)
          .orElseGet(
              () ->
                  UnresolvedAutoEmbeddingIndexGeneration.createFailed(
                      rawDefinitionGeneration, e.getReason()));
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

  /**
   * Reconstructs the derived Lucene vector index for offline querying using only the {@link
   * AutoEmbeddingCommitMetadata} in the last on-disk commit (no materialized view or mongod): the
   * raw definition supplies the field shape and the commit supplies the resolved database name,
   * collection UUID, and schema mapping. The derived generation reuses the raw generation, so it
   * maps to the same directory the derived Lucene last committed to.
   *
   * <p>Best-effort: returns empty when the commit lacks reconstruction metadata, the raw definition
   * is not a vector index, or rebuilding/opening fails (any exception degrades to empty). Not gated
   * on {@code wasMatViewReady} -- the caller decides from that signal whether to serve.
   */
  public static Optional<InitializedIndex> reconstructOfflineDerivedVectorIndex(
      IndexFactory indexFactory, IndexDefinitionGeneration rawDefinitionGeneration) {
    return reconstructOfflineDerivedVectorIndex(
        indexFactory,
        rawDefinitionGeneration,
        readPersistedCommit(indexFactory, rawDefinitionGeneration)
            .flatMap(IndexCommitUserData::getAutoEmbeddingMetadata));
  }

  /** Reconstructs from already-read commit metadata, reusing the caller's snapshot. */
  private static Optional<InitializedIndex> reconstructOfflineDerivedVectorIndex(
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
      return Optional.of(indexFactory.getInitializedIndex(derivedIndex, derivedDefGen));
    } catch (IOException | InvalidAnalyzerDefinitionException | RuntimeException e) {
      LOG.atWarn()
          .setCause(e)
          .addKeyValue("generationId", rawDefinitionGeneration.getGenerationId())
          .log("Failed to reconstruct derived Lucene index offline from persisted commit metadata");
      return Optional.empty();
    }
  }

  /**
   * Brings up the on-disk derived Lucene as a degraded, query-only STALE generation when the last
   * commit says the view was ready and an optime can be recovered. Query-only because no
   * replication runs: mongod is unreachable, so there is no composite writer and the reconstructed
   * index only serves reads. The reconstructed index is set STALE as of that optime and serves
   * queries until mongod returns. Empty (caller falls back to FAILED/retry) when the view was not
   * ready, no optime is recoverable, or reconstruction fails.
   */
  private static Optional<UnresolvedAutoEmbeddingIndexGeneration> staleOfflineServingGeneration(
      IndexFactory indexFactory, IndexDefinitionGeneration rawDefinitionGeneration) {
    Optional<IndexCommitUserData> commit =
        readPersistedCommit(indexFactory, rawDefinitionGeneration);
    Optional<AutoEmbeddingCommitMetadata> metadata =
        commit.flatMap(IndexCommitUserData::getAutoEmbeddingMetadata);
    if (!metadata.map(AutoEmbeddingCommitMetadata::wasMatViewReady).orElse(false)) {
      return Optional.empty();
    }
    // Validate the optime before reconstructing, so a rejected commit skips the work and never
    // leaves a reader open.
    Optional<BsonTimestamp> optime =
        commit.flatMap(AutoEmbeddingIndexGenerationFactory::lastCommittedOptime);
    if (optime.isEmpty()) {
      return Optional.empty();
    }
    Optional<InitializedIndex> reconstructed =
        reconstructOfflineDerivedVectorIndex(indexFactory, rawDefinitionGeneration, metadata);
    if (reconstructed.isEmpty()) {
      return Optional.empty();
    }
    reconstructed
        .get()
        .setStatus(
            IndexStatus.stale(
                "Serving Automated Embedding index from last committed data while its source is "
                    + "unavailable",
                optime.get()));
    return Optional.of(
        UnresolvedAutoEmbeddingIndexGeneration.createStale(
            rawDefinitionGeneration,
            reconstructed.get(),
            persistedAutoEmbeddingFieldsMapping(metadata)));
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
