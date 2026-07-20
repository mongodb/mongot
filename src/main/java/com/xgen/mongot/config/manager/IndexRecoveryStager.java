package com.xgen.mongot.config.manager;

import com.xgen.mongot.config.backup.ConfigJournalV1;
import com.xgen.mongot.config.backup.JournalEditor;
import com.xgen.mongot.config.util.IndexDefinitions;
import com.xgen.mongot.config.util.Invariants;
import com.xgen.mongot.featureflag.Feature;
import com.xgen.mongot.featureflag.FeatureFlags;
import com.xgen.mongot.index.IndexGeneration;
import com.xgen.mongot.index.analyzer.InvalidAnalyzerDefinitionException;
import com.xgen.mongot.index.autoembedding.AutoEmbeddingIndexGenerationFactory;
import com.xgen.mongot.index.definition.IndexDefinitionGeneration;
import com.xgen.mongot.index.status.IndexStatus;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class IndexRecoveryStager {
  private static final Logger LOG = LoggerFactory.getLogger(IndexRecoveryStager.class);

  private final ConfigState configState;
  private final IndexActions indexActions;
  private final Set<UUID> directMongodCollectionSet;
  private final FeatureFlags featureFlags;

  private IndexRecoveryStager(
      ConfigState configState, Set<UUID> directMongodCollectionSet, FeatureFlags featureFlags) {
    this.configState = configState;
    this.indexActions = IndexActions.withReplication(this.configState);
    this.directMongodCollectionSet = directMongodCollectionSet;
    this.featureFlags = featureFlags;
  }

  static void stageRecoveryAttempts(
      ConfigState configState, Set<UUID> directMongodCollectionSet, FeatureFlags featureFlags)
      throws IOException, Invariants.InvariantException, InvalidAnalyzerDefinitionException {
    IndexRecoveryStager indexRecoveryStager =
        new IndexRecoveryStager(configState, directMongodCollectionSet, featureFlags);
    indexRecoveryStager.retryFailedStageIndexes();
    indexRecoveryStager.stageIndexes();
  }

  private boolean isRecoveringIndex(IndexGeneration indexGeneration) {
    IndexStatus indexStatus = indexGeneration.getIndex().getStatus();
    boolean isOnline =
        !AutoEmbeddingIndexGenerationFactory.isAutoEmbeddingResolutionFailed(indexGeneration);
    boolean hasRecoverableStatus =
        indexStatus.getStatusCode() == IndexStatus.StatusCode.RECOVERING_NON_TRANSIENT
            || indexStatus.canBeRecovered();
    boolean isOnlineCandidate = isOnline && hasRecoverableStatus;
    boolean isOfflineCandidate =
        !isOnline
            && (hasRecoverableStatus || indexStatus.getStatusCode() == IndexStatus.StatusCode.STALE)
            && canStageAutoEmbedIndexRecovery();
    return isOnlineCandidate || isOfflineCandidate;
  }

  private boolean canStageAutoEmbedIndexRecovery() {
    return this.configState
        .materializedViewIndexFactory
        .map(factory -> factory.isMongodReachableFromCachedTopology())
        .orElse(false);
  }

  private static void logIndexIds(List<IndexGeneration> indexes, String logMessage) {
    indexes.forEach(
        e ->
            LOG.atInfo()
                .addKeyValue("indexId", e.getGenerationId().indexId)
                .addKeyValue("generationId", e.getGenerationId())
                .log(logMessage));
  }

  private static void logRecoveringIndexIds(List<IndexGeneration> recoveringIndexes) {
    logIndexIds(recoveringIndexes, "Going to stage new attempt for recovering index");
  }

  private static void logCollectionNotFoundIndexIds(
      List<IndexGeneration> collectionNotFoundIndexes) {
    logIndexIds(
        collectionNotFoundIndexes,
        "Going to stage new attempt for previously collection not found index");
  }

  private static void logFailedStageIndexIds(List<IndexGeneration> failedIndexes) {
    failedIndexes.forEach(
        e ->
            LOG.atError()
                .addKeyValue("indexId", e.getGenerationId().indexId)
                .addKeyValue("generationId", e.getGenerationId())
                .addKeyValue("reason", e.getIndex().getStatus().getReason().map(Enum::name))
                .log("Stage index failed"));
  }

  private boolean hasNoExistingStagedIndex(IndexGeneration indexGeneration) {
    // Used to filter out indexes which already have a new generation staged.
    return this.configState.staged.getIndex(indexGeneration.getDefinition().getIndexId()).isEmpty();
  }

  private List<IndexGeneration> getRecoveringIndexesToStage() {
    return this.configState.indexCatalog.getIndexes().stream()
        .filter(this::isRecoveringIndex)
        .filter(this::hasNoExistingStagedIndex)
        .collect(Collectors.toList());
  }

  private List<IndexGeneration> getCollectionNotFoundIndexes() {
    return this.configState.indexCatalog.getIndexes().stream()
        .filter(indexGeneration -> indexGeneration.getIndex().getStatus().isCollectionNotFound())
        .filter(this::hasNoExistingStagedIndex)
        .toList();
  }

  private List<IndexGeneration> getCollectionNotFoundIndexesToStage() {
    List<IndexGeneration> collectionNotFoundIndexes = getCollectionNotFoundIndexes();
    return collectionNotFoundIndexes.stream()
        .filter(
            index ->
                this.directMongodCollectionSet.contains(index.getDefinition().getCollectionUuid()))
        .collect(Collectors.toList());
  }

  private void stageIndexes()
      throws IOException, Invariants.InvariantException, InvalidAnalyzerDefinitionException {
    List<IndexGeneration> indexesNeedToBeStaged = new ArrayList<>();

    // Retrieve indexes marked for recovery that need to be staged, such as those that became stale
    // due to non invalidating errors or failed due to initialization errors or failed to resolve
    // materialized view collection metadata for auto-embedding index.
    List<IndexGeneration> recoveringIndexes = getRecoveringIndexesToStage();
    logRecoveringIndexIds(recoveringIndexes);
    indexesNeedToBeStaged.addAll(recoveringIndexes);

    if (this.featureFlags.isEnabled(Feature.SHUT_DOWN_REPLICATION_WHEN_COLLECTION_NOT_FOUND)) {
      // Retrieve indexes whose collections were previously not found but are now present, allowing
      // staging attempts to be added for these indexes.
      List<IndexGeneration> collectionNotFoundIndexes = getCollectionNotFoundIndexesToStage();
      logCollectionNotFoundIndexIds(collectionNotFoundIndexes);
      indexesNeedToBeStaged.addAll(collectionNotFoundIndexes);
    }

    Set<ObjectId> seenIndexIds = new HashSet<>();
    for (IndexGeneration indexGeneration : indexesNeedToBeStaged) {
      // Deduplicate indexes based on their index ID
      if (seenIndexIds.add(indexGeneration.getDefinition().getIndexId())) {
        stageNewAttempt(indexGeneration);
      }
    }
  }

  private void retryFailedStageIndexes()
      throws Invariants.InvariantException, IOException, InvalidAnalyzerDefinitionException {
    List<IndexGeneration> failedStagedIndexes = getRetryableStagedIndexes();

    if (failedStagedIndexes.isEmpty()) {
      return;
    }

    removeFailedStageIndexes(failedStagedIndexes);

    for (IndexGeneration indexGeneration : failedStagedIndexes) {
      stageNewAttempt(indexGeneration);
    }
  }

  private void stageNewAttempt(IndexGeneration indexGeneration)
      throws IOException, Invariants.InvariantException, InvalidAnalyzerDefinitionException {
    // Increment the attempt counter.
    IndexDefinitionGeneration nextAttempt =
        indexGeneration.getDefinitionGeneration().incrementAttempt();

    // Write our intent to add a new index generation to the config journal.
    ConfigJournalV1 preAdd =
        JournalEditor.on(this.configState.currentJournal()).addStaged(nextAttempt).journal();
    this.configState.persist(preAdd);

    this.indexActions.addStagedIndex(nextAttempt);
  }

  private List<IndexGeneration> getRetryableStagedIndexes() {
    // we only want to re-stage attempt for failed stage index when it failed during initialization
    // or resolving materialized view collection metadata for auto-embedding index
    return this.configState.staged.getIndexes().stream()
        .filter(
            indexGeneration -> {
              IndexStatus status = indexGeneration.getIndex().getStatus();
              return status.getStatusCode() == IndexStatus.StatusCode.FAILED
                  && status
                      .getReason()
                      .map(
                          reason ->
                              reason == IndexStatus.Reason.INITIALIZATION_FAILED
                                  || reason == IndexStatus.Reason.AUTO_EMBEDDING_RESOLUTION_RETRY)
                      .orElse(false);
            })
        .filter(
            indexGeneration ->
                !AutoEmbeddingIndexGenerationFactory.isAutoEmbeddingResolutionFailed(
                        indexGeneration)
                    || canStageAutoEmbedIndexRecovery())
        .collect(Collectors.toList());
  }

  private void removeFailedStageIndexes(List<IndexGeneration> failedStagedIndexes)
      throws IOException, Invariants.InvariantException {
    logFailedStageIndexIds(failedStagedIndexes);

    // write our intent to drop the failed staged indexes to the config journal
    ConfigJournalV1 preDrop =
        JournalEditor.on(this.configState.currentJournal())
            .fromStagedToDropped(IndexDefinitions.indexesGenerationIds(failedStagedIndexes))
            .journal();
    this.configState.persist(preDrop);

    // add the failed indexes to be phased out and remove from staged
    // PhasingOutIndexesDropper will drop the failed index data from disk
    failedStagedIndexes.forEach(this.configState.phasingOut::addIndex);
    failedStagedIndexes.forEach(this.configState.staged::removeIndex);
  }
}
