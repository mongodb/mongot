package com.xgen.mongot.config.manager;

import com.google.common.flogger.FluentLogger;
import com.xgen.mongot.config.backup.JournalEditor;
import com.xgen.mongot.config.util.IndexDefinitions;
import com.xgen.mongot.config.util.Invariants;
import com.xgen.mongot.index.IndexGeneration;
import com.xgen.mongot.index.analyzer.InvalidAnalyzerDefinitionException;
import com.xgen.mongot.index.definition.IndexDefinitionGeneration;
import com.xgen.mongot.index.definition.IndexDefinitionGeneration.Type;
import com.xgen.mongot.index.status.IndexStatus;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.bson.types.ObjectId;

/**
 * Responsible for retrying failed initial sync indexes.
 * An index is retryable when:
 * - It has {@code StatusCode.FAILED} with reason {@code INITIAL_SYNC_REPLICATION_FAILED_RETRY}.
 * - It has no staged replacement OR it's staged replacement also has a retryable failure.
 *
 */
class IndexInitialSyncRecovery {
  private static final FluentLogger FLOGGER = FluentLogger.forEnclosingClass();

  private IndexInitialSyncRecovery() {
  }

  /**
   * Fetches indexes whose failed initial sync can be retried.
   *
   * @param configState the current config state.
   * @return retryable indexes from the live catalog.
   */
  static List<IndexGeneration> getRetryableInitialSyncIndexes(ConfigState configState) {
    List<IndexGeneration> retryable = new ArrayList<>();
    for (IndexGeneration index : configState.indexCatalog.getIndexes()) {
      if (!isRetryableInitialSyncFailure(index)) {
        continue;
      }
      ObjectId indexId = index.getDefinition().getIndexId();
      Optional<IndexGeneration> staged = configState.staged.getIndex(indexId);
      if (staged.isPresent() && !isRetryableInitialSyncFailure(staged.get())) {
        FLOGGER.atInfo().atMostEvery(5, TimeUnit.MINUTES).log(
            "Skipping retry of failed initial sync index because staged replacement"
                + " is not a retryable failure: indexId=%s, generationId=%s",
            indexId, index.getGenerationId());
        continue;
      }
      retryable.add(index);
    }
    return retryable;
  }

  /**
   * Retries builds for indexes that failed during initial sync, but can be retried.
   *
   * @param configState:  The current config state.
   * @param indexActions: Used to create the new index generations.
   * @throws Invariants.InvariantException:      If the resulting journal is semantically invalid.
   * @throws IOException:                        There is an error writing to the config journal.
   * @throws InvalidAnalyzerDefinitionException: The new index has an invalid analyzer definition.
   */
  static void retryFailedInitialSyncIndexes(
      ConfigState configState, IndexActions indexActions)
      throws
      Invariants.InvariantException,
      IOException,
      InvalidAnalyzerDefinitionException {

    // Drop any failed staged index that has a healthy, live version.
    dropFailedStagedWithHealthyLive(configState, indexActions);

    List<IndexGeneration> failedInitialSyncIndexes = getRetryableInitialSyncIndexes(configState);
    if (failedInitialSyncIndexes.isEmpty()) {
      return;
    }
    logFailedInitialSyncIndexIds(failedInitialSyncIndexes);

    // The live FAILED index stays in the catalog. The retry is always added as staged.
    // If a failed staged index already exists, drop it and add a new staged index.
    List<IndexGeneration> failedStagedIndexes = new ArrayList<>();
    List<IndexDefinitionGeneration> nextStagedAttempts = new ArrayList<>();
    for (IndexGeneration index : failedInitialSyncIndexes) {
      ObjectId indexId = index.getDefinition().getIndexId();
      Optional<IndexGeneration> staged = configState.staged.getIndex(indexId);
      IndexDefinitionGeneration base;
      if (staged.isPresent()) {
        failedStagedIndexes.add(staged.get());
        base = staged.get().getDefinitionGeneration();
      } else {
        base = index.getDefinitionGeneration();
      }
      IndexDefinitionGeneration nextAttempt = base.incrementAttempt();
      nextStagedAttempts.add(nextAttempt);
      logRetryGenerations(index, staged, nextAttempt);
    }

    JournalEditor editor = JournalEditor.on(configState.currentJournal());
    if (!failedStagedIndexes.isEmpty()) {
      editor.fromStagedToDropped(IndexDefinitions.indexesGenerationIds(failedStagedIndexes));
    }
    nextStagedAttempts.forEach(editor::addStaged);
    configState.persist(editor.journal());

    // Apply in-memory changes: drop old staged indexes (if any), add new staged attempts.
    if (!failedStagedIndexes.isEmpty()) {
      indexActions.dropFromStaged(failedStagedIndexes);
    }
    indexActions.addStagedIndexes(nextStagedAttempts);
  }

  /**
   * Drops retryable, failed staged indexes that have a healthy, live version.
   *
   * @param configState  The current config state.
   * @param indexActions Used to drop the failed staged indexes.
   * @throws Invariants.InvariantException: If the resulting journal is semantically invalid.
   * @throws IOException:                   There is an error writing to the config journal.
   */
  private static void dropFailedStagedWithHealthyLive(
      ConfigState configState, IndexActions indexActions)
      throws
      IOException,
      Invariants.InvariantException {
    List<IndexGeneration> toDrop = new ArrayList<>();
    for (IndexGeneration staged : configState.staged.getIndexes()) {
      if (!isRetryableInitialSyncFailure(staged)) {
        continue;
      }
      ObjectId indexId = staged.getDefinition().getIndexId();
      Optional<IndexGeneration> live = configState.indexCatalog.getIndexById(indexId);
      if (live.isPresent() && live.get().getIndex().getStatus().canServiceQueries()) {
        FLOGGER.atInfo().atMostEvery(5, TimeUnit.MINUTES).log(
            "Dropping failed staged index because live index can service queries:"
                + " indexId=%s, stagedGenerationId=%s, liveGenerationId=%s",
            indexId, staged.getGenerationId(), live.get().getGenerationId());
        toDrop.add(staged);
      }
    }

    if (toDrop.isEmpty()) {
      return;
    }

    JournalEditor editor = JournalEditor.on(configState.currentJournal());
    editor.fromStagedToDropped(IndexDefinitions.indexesGenerationIds(toDrop));
    configState.persist(editor.journal());

    indexActions.dropFromStaged(toDrop);
  }

  private static void logFailedInitialSyncIndexIds(List<IndexGeneration> failedIndexes) {
    failedIndexes.forEach(e -> FLOGGER.atSevere().atMostEvery(5, TimeUnit.MINUTES).log(
        "Initial sync failed: indexId=%s, generationId=%s, reason=%s",
        e.getGenerationId().indexId, e.getGenerationId(),
        e.getIndex().getStatus().getReason().map(Enum::name).orElse("none")));
  }

  /**
   * Logs all generations involved in a retry attempt for a single index.
   * This log is intended to help developers determine which generations
   * potentially exist on disk, so they can be dropped if needed.
   *
   * @param liveIndex     the failed live index being retried.
   * @param droppedStaged the old failed staged index that was dropped,
   *                      or empty if there was no prior staged attempt.
   * @param newAttempt    the new staged attempt being created.
   */
  private static void logRetryGenerations(
      IndexGeneration liveIndex, Optional<IndexGeneration> droppedStaged,
      IndexDefinitionGeneration newAttempt
  ) {
    FLOGGER.atInfo().atMostEvery(5, TimeUnit.MINUTES).log(
        "Retrying failed initial sync index; these generations may exist on disk:"
            + " indexId=%s, liveGeneration=%s, droppedStagedGeneration=%s,"
            + " newStagedGeneration=%s",
        liveIndex.getGenerationId().indexId, liveIndex.getGenerationId(),
        droppedStaged.map(s -> s.getGenerationId().toString()).orElse("none"),
        newAttempt.getGenerationId());
  }

  private static boolean isRetryableInitialSyncFailure(IndexGeneration indexGeneration) {
    // Materialized view indexes cannot be retried because
    // MaterializedViewGeneration.nextAttempt() is a no-op, which would produce a duplicate
    // GenerationId and fail Invariants.validateUniqueGenerationIds during persist().
    if (indexGeneration.getType().equals(Type.AUTO_EMBEDDING)) {
      return false;
    }
    IndexStatus status = indexGeneration.getIndex().getStatus();
    return status.getStatusCode() == IndexStatus.StatusCode.FAILED
        && status.getReason()
        .map(reason -> reason == IndexStatus.Reason.INITIAL_SYNC_REPLICATION_FAILED_RETRY)
        .orElse(false);
  }
}
