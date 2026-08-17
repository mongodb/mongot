package com.xgen.mongot.config.manager;

import static com.xgen.mongot.index.autoembedding.AutoEmbeddingIndexGenerationFactory.isAutoEmbeddingResolutionFailed;
import static com.xgen.mongot.index.definition.MaterializedViewIndexDefinitionGeneration.isMaterializedViewBasedIndex;
import static com.xgen.mongot.util.Check.checkState;

import com.xgen.mongot.config.util.IndexDefinitions;
import com.xgen.mongot.index.Index;
import com.xgen.mongot.index.IndexGeneration;
import com.xgen.mongot.index.IndexGenerationFactory;
import com.xgen.mongot.index.InitializedIndex;
import com.xgen.mongot.index.analyzer.InvalidAnalyzerDefinitionException;
import com.xgen.mongot.index.autoembedding.AutoEmbeddingIndexGenerationFactory;
import com.xgen.mongot.index.autoembedding.UnresolvedAutoEmbeddingIndex;
import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.definition.IndexDefinitionGeneration;
import com.xgen.mongot.index.definition.VectorIndexDefinition;
import com.xgen.mongot.index.version.GenerationId;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.Crash;
import com.xgen.mongot.util.FutureUtils;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Only adds and removes indexes from ConfigState, never modifies the journal.
 *
 * <p>May throw runtime for invariant violations. Specifically: enforces that staged indexes have
 * corresponding indexes in the catalog. These shouldn't happen as we would fail writing-ahead an
 * invalid journal before calling IndexActions.
 *
 * <p>Failure atomicity isn't guaranteed here. For instance: say creating the 3rd index throws
 * IOException, we still have 2 previously created indexes present. Since this type of error isn't
 * recoverable, we don't attempt to revert any actions before the exception occurred.
 */
public class IndexActions {
  private static final Logger LOG = LoggerFactory.getLogger(IndexActions.class);

  private final boolean withReplication;
  private final ConfigState configState;

  private IndexActions(boolean withReplication, ConfigState configState) {
    this.withReplication = withReplication;
    this.configState = configState;
  }

  public static IndexActions withReplication(ConfigState configState) {
    return new IndexActions(true, configState);
  }

  static IndexActions withoutReplication(ConfigState configState) {
    return new IndexActions(false, configState);
  }

  public void addNewIndexes(List<IndexDefinitionGeneration> definitions)
      throws IOException, InvalidAnalyzerDefinitionException {
    for (IndexDefinitionGeneration definition : definitions) {
      addIndex(definition);
    }
  }

  private void addIndex(IndexDefinitionGeneration definitionGeneration)
      throws IOException, InvalidAnalyzerDefinitionException {
    GenerationId generationId = definitionGeneration.getGenerationId();
    LOG.atInfo()
        .addKeyValue("indexId", generationId.indexId)
        .addKeyValue("generationId", generationId)
        .log("adding live index");
    checkState(
        this.configState
            .indexCatalog
            .getIndexById(definitionGeneration.getIndexDefinition().getIndexId())
            .isEmpty(),
        "trying to insert an index to the catalog clobbering existing indexId: %s",
        definitionGeneration.getGenerationId());
    IndexGeneration indexGeneration = getIndexGenerationByDefinitionType(definitionGeneration);
    // When it returns UnresolvedAutoEmbeddingIndexGeneration, we still add it to indexCatalog to
    // let config manager decide whether to retry on each update cycle.
    this.configState.indexCatalog.addIndex(indexGeneration);
    if (isAutoEmbeddingResolutionFailed(indexGeneration)) {
      // Skips lifecycle for this unresolved indexGeneration. Registering a generation id the
      // initialized-index catalog already holds would throw and take the process down with it; the
      // checkState above rejects a duplicate indexId, so the generation id here is always fresh.
      registerOfflineServingIndex(indexGeneration);
      if (indexGeneration.getIndex().getStatus().isCollectionNotFound()) {
        // Expected, quiet steady state on a shard that does not host the source collection (e.g.
        // an unsharded collection on a non-owner shard) -- not a failure, so don't WARN.
        LOG.atDebug()
            .addKeyValue("indexId", generationId.indexId)
            .addKeyValue("generationId", generationId)
            .log("Source collection not found on this shard; skipping replication.");
      } else {
        LOG.atWarn()
            .addKeyValue("indexId", generationId.indexId)
            .addKeyValue("generationId", generationId)
            .log(
                "Unable to create auto-embedding index, "
                    + "Skips replication for this definition at this time.");
      }
      return;
    }

    if (deferVectorliteInitialization(indexGeneration)) {
      // The custom vector engine (vectorlite) initialization path is driven by the SLS conf call
      // and is not yet implemented; leave the index uninitialized in the catalog for now.
      LOG.atInfo()
          .addKeyValue("indexId", generationId.indexId)
          .addKeyValue("generationId", generationId)
          .log("Leaving custom vector engine index uninitialized; SLS-driven init not yet wired.");
      return;
    }

    if (this.withReplication) {
      this.configState.getLifecycleManager().add(indexGeneration);
    }
  }

  /**
   * Registers what an unresolved auto-embedding generation reconstructed from its last commit, so
   * queries resolve it like any other index. Such a generation skips the lifecycle that would
   * otherwise register it, and registering cannot start replication because that is gated on the
   * generation reporting itself closed. Only a generation serving offline carries a reconstructed
   * index; failed ones hold an unopened handle and stay unregistered.
   *
   * <p>Each add path already rejects clobbering an existing index id and recovery stages a new
   * attempt under a distinct generation id, so the registration is always fresh. Registering a
   * generation id the catalog already holds throws, which fails the whole update cycle, so this
   * skips instead of letting that happen -- but loudly, since reaching it means one of those
   * guarantees no longer holds. Only the config manager registers, single-threaded, so nothing can
   * take the id between the check and the add.
   */
  private void registerOfflineServingIndex(IndexGeneration indexGeneration) {
    if (!(indexGeneration.getIndex() instanceof UnresolvedAutoEmbeddingIndex unresolved)) {
      return;
    }
    Optional<InitializedIndex> reconstructed = unresolved.getReconstructedDerivedIndex();
    if (reconstructed.isEmpty()) {
      return;
    }
    // The key the catalog would store under, rather than the generation's own id, so the check
    // matches the add even if the derived generation id ever stops mirroring the raw one.
    GenerationId generationId = reconstructed.get().getGenerationId();
    if (this.configState.initializedIndexCatalog.getIndex(generationId).isPresent()) {
      LOG.atWarn()
          .addKeyValue("indexId", generationId.indexId)
          .addKeyValue("generationId", generationId)
          .log(
              "Initialized-index catalog already holds this generation id; keeping the registered"
                  + " index and skipping the offline-serving registration.");
      return;
    }
    this.configState.initializedIndexCatalog.addIndex(reconstructed.get());
  }

  /**
   * Whether initialization of {@code indexGeneration} should be deferred because it is a custom
   * vector engine (vectorlite) index whose SLS-driven initialization path is not yet implemented.
   *
   * <p>When {@code true}, callers must skip {@link LifecycleManager#add} and leave the index in the
   * catalog (reported as {@code NOT_STARTED}). The index is still visible to the SLS conf call so
   * it can be assigned a log; a future change will initialize it once a log id has been assigned.
   * Whether vectorlite indexes exist at all is gated on the control-plane side (mms), so no mongot
   * feature flag is consulted here.
   */
  static boolean deferVectorliteInitialization(IndexGeneration indexGeneration) {
    return indexGeneration.getDefinition() instanceof VectorIndexDefinition vectorDefinition
        && vectorDefinition.isCustomVectorEngineIndex();
  }

  void addStagedIndexes(List<IndexDefinitionGeneration> definitions)
      throws IOException, InvalidAnalyzerDefinitionException {
    for (IndexDefinitionGeneration definition : definitions) {
      addStagedIndex(definition);
    }
  }

  /** Add a new staged index. */
  public void addStagedIndex(IndexDefinitionGeneration definitionGeneration)
      throws IOException, InvalidAnalyzerDefinitionException {
    GenerationId generationId = definitionGeneration.getGenerationId();
    LOG.atInfo()
        .addKeyValue("indexId", generationId.indexId)
        .addKeyValue("generationId", generationId)
        .log("adding staged index");
    checkState(
        this.configState
            .indexCatalog
            .getIndexById(definitionGeneration.getIndexDefinition().getIndexId())
            .isPresent(),
        "can not stage an index without corresponding index in catalog: %s",
        definitionGeneration.getGenerationId());
    // staged won't let us clobber an index with the same indexId, we don't need to validate for it
    IndexGeneration indexGeneration = getIndexGenerationByDefinitionType(definitionGeneration);
    this.configState.staged.addIndex(indexGeneration);
    if (isAutoEmbeddingResolutionFailed(indexGeneration)) {
      // Skips lifecycle for this unresolved indexGeneration.
      registerOfflineServingIndex(indexGeneration);
      if (indexGeneration.getIndex().getStatus().isCollectionNotFound()) {
        // Expected, quiet steady state on a shard that does not host the source collection (e.g.
        // an unsharded collection on a non-owner shard) -- not a failure, so don't WARN.
        LOG.atDebug()
            .addKeyValue("indexId", generationId.indexId)
            .addKeyValue("generationId", generationId)
            .log("Source collection not found on this shard; skipping staged replication.");
      } else {
        LOG.atWarn()
            .addKeyValue("indexId", generationId.indexId)
            .addKeyValue("generationId", generationId)
            .log(
                "Unable to create auto-embedding stage index, "
                    + "Skips replication for this definition at this time.");
      }
      return;
    }
    if (deferVectorliteInitialization(indexGeneration)) {
      // See addIndex: custom vector engine (vectorlite) staged indexes are left uninitialized until
      // the SLS-driven initialization path exists.
      LOG.atInfo()
          .addKeyValue("indexId", generationId.indexId)
          .addKeyValue("generationId", generationId)
          .log(
              "Leaving custom vector engine staged index uninitialized; SLS-driven init not yet"
                  + " wired.");
      return;
    }
    if (this.withReplication) {
      this.configState.getLifecycleManager().add(indexGeneration);
    }
  }

  void dropFromCatalog(List<IndexGeneration> indexes) {
    if (indexes.isEmpty()) {
      return;
    }

    logIds(indexes, "dropping index from catalog");

    validateAllInCatalogAndNoPendingSwaps(indexes);

    // First remove the indexes from the IndexCatalog. This stops new cursors from being opened.
    indexes.stream()
        .map(IndexGeneration::getDefinition)
        .map(IndexDefinition::getIndexId)
        .forEach(this.configState.indexCatalog::removeIndex);

    dropAllOrFail(indexes);
  }

  void dropFromStaged(List<IndexGeneration> indexes) {
    if (indexes.isEmpty()) {
      return;
    }

    logIds(indexes, "dropping index from staged");

    validateAllInStaged(indexes);
    indexes.forEach(this.configState.staged::removeIndex);

    dropAllOrFail(indexes);
  }

  void dropFromPhasingOut(List<IndexGeneration> indexes) {
    if (indexes.isEmpty()) {
      return;
    }

    logIds(indexes, "dropping index from phasingOut");

    validateAllInPhasingOut(indexes);
    indexes.forEach(this.configState.phasingOut::removeIndex);

    dropAllOrFail(indexes);
  }

  private void dropAllOrFail(List<IndexGeneration> indexes) {
    List<CompletableFuture<?>> dropFutures =
        indexes.stream().map(this::dropIndex).collect(Collectors.toList());

    CompletableFuture<Void> allDrops = FutureUtils.allOf(dropFutures);

    Crash.because("failed to drop indexes").ifThrows(() -> allDrops.get());
  }

  private CompletableFuture<Void> dropIndex(IndexGeneration indexGeneration) {
    GenerationId generationId = indexGeneration.getGenerationId();
    LOG.atInfo()
        .addKeyValue("indexId", generationId.indexId)
        .addKeyValue("generationId", generationId)
        .log("dropping index");

    Index index = indexGeneration.getIndex();
    CompletableFuture<Void> replicationDropFuture;

    if (isAutoEmbeddingResolutionFailed(indexGeneration)) {
      // An unresolved auto-embedding generation never entered the lifecycle, so a lifecycle drop
      // has nothing to do and would only log a spurious "not added" warning. Kill its cursors here
      // so they do not outlive the reconstructed reader closed below.
      this.configState.cursorManager.killIndexCursors(generationId);
      replicationDropFuture = CompletableFuture.completedFuture(null);
    } else if (this.withReplication) {
      // This will look up the index in InitializedIndexCatalog to kill cursors
      replicationDropFuture = this.configState.getLifecycleManager().dropIndex(generationId);
    } else {
      replicationDropFuture = CompletableFuture.completedFuture(null);
    }

    return replicationDropFuture.handle(
        (result, exception) -> {
          if (exception != null) {
            LOG.atError()
                .addKeyValue("indexId", generationId.indexId)
                .addKeyValue("generationId", generationId)
                .setCause(exception)
                .log("Replication drop failed, proceeding with physical drop");
          }

          // Remove the index from the catalog.
          var initializedIndex = this.configState.initializedIndexCatalog.removeIndex(generationId);

          Crash.because("failed to close and drop index")
              .ifThrows(
                  () -> {
                    try {
                      if (initializedIndex.isPresent()) {
                        initializedIndex.get().close();
                      }
                    } catch (Exception e) {
                      LOG.atError()
                          .addKeyValue("indexId", generationId.indexId)
                          .addKeyValue("generationId", generationId)
                          .addKeyValue("exceptionMessage", e.getMessage())
                          .log("Exception while closing index");
                    }
                    // Drop and close the index on any exception.
                    try {
                      index.close();
                    } catch (Exception e) {
                      LOG.atDebug().log(
                          "Ignored exception during redundant close: " + e.getMessage());
                    }
                    index.drop();
                  });
          return null;
        });
  }

  private void validateAllInCatalogAndNoPendingSwaps(List<IndexGeneration> indexes) {
    for (IndexGeneration indexGeneration : indexes) {
      // verify index is in the catalog
      ObjectId indexId = indexGeneration.getDefinition().getIndexId();
      Optional<IndexGeneration> optionalIndex = this.configState.indexCatalog.getIndexById(indexId);
      checkState(
          optionalIndex.isPresent() && optionalIndex.get() == indexGeneration,
          "attempting to remove index not present in catalog: id=%s, dropping=%s, existing=%s",
          indexGeneration.getGenerationId(),
          indexGeneration,
          optionalIndex);

      // if we drop an index from the catalog that has a staged index, we "orphan" the staged one.
      Optional<IndexGeneration> existingStaged = this.configState.staged.getIndex(indexId);
      checkState(
          existingStaged.isEmpty(),
          "attempting to remove index from catalog with a present staged index: id=%s, staged=%s",
          indexId,
          existingStaged
              .map(IndexGeneration::getDefinitionGeneration)
              .map(IndexDefinitionGeneration::getGenerationId)
              .map(GenerationId::uniqueString));
    }
  }

  private void validateAllInStaged(List<IndexGeneration> indexes) {
    for (IndexGeneration index : indexes) {
      ObjectId indexId = index.getDefinition().getIndexId();
      Optional<IndexGeneration> optionalIndex = this.configState.staged.getIndex(indexId);
      checkState(
          optionalIndex.isPresent() && optionalIndex.get() == index,
          "attempting to remove index not present in staged: id=%s, dropping=%s, existing=%s",
          index.getGenerationId(),
          index,
          optionalIndex);
    }
  }

  private void validateAllInPhasingOut(List<IndexGeneration> indexes) {
    List<IndexGeneration> existingIndexes = this.configState.phasingOut.getIndexes();
    for (IndexGeneration index : indexes) {
      checkState(
          existingIndexes.contains(index),
          "attempting to remove index not present in phasingOut: id=%s, phasingOut=%s",
          index.getGenerationId(),
          getGenerationIds(existingIndexes));
    }
  }

  private static List<String> getGenerationIds(List<IndexGeneration> indexes) {
    return IndexDefinitions.indexesGenerationIds(indexes).stream()
        .map(GenerationId::uniqueString)
        .collect(Collectors.toList());
  }

  private static void logIds(List<IndexGeneration> indexes, String message) {
    IndexDefinitions.indexesGenerationIds(indexes)
        .forEach(
            e ->
                LOG.atInfo()
                    .addKeyValue("indexId", e.indexId)
                    .addKeyValue("generationId", e)
                    .log(message));
  }

  private IndexGeneration getIndexGenerationByDefinitionType(
      IndexDefinitionGeneration definitionGeneration)
      throws IOException, InvalidAnalyzerDefinitionException {
    if (isMaterializedViewBasedIndex(definitionGeneration.getIndexDefinition())) {
      return AutoEmbeddingIndexGenerationFactory.getAutoEmbeddingIndexGeneration(
          this.configState.indexFactory,
          Check.isPresent(
              this.configState.materializedViewIndexFactory, "materializedViewIndexFactory"),
          definitionGeneration,
          this.configState.initializedIndexCatalog,
          () -> !this.configState.getLifecycleManager().isReplicationSupported());
    } else {
      return IndexGenerationFactory.getIndexGeneration(
          this.configState.indexFactory, definitionGeneration);
    }
  }
}
