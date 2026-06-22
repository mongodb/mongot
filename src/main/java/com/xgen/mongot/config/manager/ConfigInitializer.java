package com.xgen.mongot.config.manager;

import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.Var;
import com.xgen.mongot.config.backup.ConfigJournalV1;
import com.xgen.mongot.config.backup.JournalEditor;
import com.xgen.mongot.config.util.Invariants;
import com.xgen.mongot.index.Index;
import com.xgen.mongot.index.analyzer.InvalidAnalyzerDefinitionException;
import com.xgen.mongot.index.definition.IndexDefinitionGeneration;
import com.xgen.mongot.index.version.GenerationId;
import com.xgen.mongot.metrics.MetricsFactory;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ConfigInitializer {
  private static final Logger LOG = LoggerFactory.getLogger(ConfigInitializer.class);

  /**
   * Initializes ConfigState with all the indexes recorded in the config Journal.
   *
   * @param configState - Maintain the state of indexes and persist the current state to disk when
   *     required.
   * @param configJournal - Describing indexes in different generations and states.
   * @param desiredIndexIds - An optional set of index IDs extracted from the initial conf call
   *     response. If provided, any index not included in this set will be removed from the config
   *     journal. This ensures that only indexes expected to exist are initialized.
   * @param meterRegistry - Meter registry to bind.
   * @throws Invariants.InvariantException - If journal invariants are violated.
   * @throws IOException - If an I/O error occurs during initialization.
   * @throws InvalidAnalyzerDefinitionException - If an analyzer definition is invalid during setup.
   */
  public static void initialize(
      ConfigState configState,
      @Var ConfigJournalV1 configJournal,
      Optional<Set<ObjectId>> desiredIndexIds,
      MeterRegistry meterRegistry)
      throws Invariants.InvariantException, IOException, InvalidAnalyzerDefinitionException {
    LOG.atInfo()
        .addKeyValue("numExistingIndexes", configJournal.getLiveIndexes().size())
        .addKeyValue("numDeletedIndexes", configJournal.getDeletedIndexes().size())
        .addKeyValue("numStagedIndexes", configJournal.getStagedIndexes().size())
        .log(
            "Found config journal. "
                + "Initializing existing indexes, cleaning up deleted indexes, staging indexes");

    validateJournalInvariants(configJournal);

    // remove indexes that are not present in the initial confcall response,
    // but are still present in the config journal.
    if (desiredIndexIds.isPresent()) {
      configJournal =
          removeDeletedIndexesFromConfigJournal(
              configState, desiredIndexIds.get(), configJournal, meterRegistry);
    }

    // Drop indexes that were ready for deletion before mongot stopped running.
    dropDeletedIndexes(configState, configJournal.getDeletedIndexes());

    // Add the indexes that should exist.
    addLiveIndexes(configState, configJournal.getLiveIndexes());

    // Add the staged indexes that should exist (must be after adding live indexes so
    // we don't have "orphan" staged indexes).
    addStagedIndexes(configState, configJournal.getStagedIndexes());

    // Rewrite the config journal for our current state, without the deleted
    // indexes so we don't have to drop them again if we restart.
    configState.persist(configState.currentJournal());
  }

  private static void validateJournalInvariants(ConfigJournalV1 config)
      throws Invariants.InvariantException {
    // Check that invariants about indexes that would produce undefined
    // behavior if violated hold prior to doing any more work.
    Invariants.validateGenerationalInvariants(
        config.getStagedIndexes(), config.getLiveIndexes(), config.getDeletedIndexes());
  }

  private static List<GenerationId> getRemovedIndexGenerations(
      List<IndexDefinitionGeneration> indexDefinitions, Set<ObjectId> desiredIndexIds) {

    return indexDefinitions.stream()
        .filter(indexDefGen -> !desiredIndexIds.contains(indexDefGen.getIndexId()))
        .map(IndexDefinitionGeneration::getGenerationId)
        .toList();
  }

  private static ConfigJournalV1 removeDeletedIndexesFromConfigJournal(
      ConfigState configState,
      Set<ObjectId> desiredIndexIds,
      ConfigJournalV1 configJournal,
      MeterRegistry meterRegistry)
      throws Invariants.InvariantException, IOException {
    LOG.atInfo()
        .addKeyValue("numDesiredIndexes", desiredIndexIds.size())
        .log("Desired indexes from initial conf call");
    // Retrieve generation IDs for indexes that no longer exist in the desired index list
    List<GenerationId> removedStagedIndexes =
        getRemovedIndexGenerations(configJournal.getStagedIndexes(), desiredIndexIds);
    List<GenerationId> removedLiveIndexes =
        getRemovedIndexGenerations(configJournal.getLiveIndexes(), desiredIndexIds);

    MetricsFactory metricsFactory = new MetricsFactory("ConfigInitializer", meterRegistry);

    if (!removedStagedIndexes.isEmpty()) {
      metricsFactory
          .counter("removedIndexesAtStartup", Tags.of("type", "stage"))
          .increment(removedStagedIndexes.size());
      removedStagedIndexes.forEach(
          e ->
              LOG.atInfo()
                  .addKeyValue("indexId", e.indexId)
                  .addKeyValue("generationId", e)
                  .log("Removed staged index not present in initial conf call"));
    }
    if (!removedLiveIndexes.isEmpty()) {
      metricsFactory
          .counter("removedIndexesAtStartup", Tags.of("type", "live"))
          .increment(removedLiveIndexes.size());
      removedLiveIndexes.forEach(
          e ->
              LOG.atInfo()
                  .addKeyValue("indexId", e.indexId)
                  .addKeyValue("generationId", e)
                  .log("Removed live index not present in initial conf call"));
    }

    // transition identified indexes to the dropped state.
    // we'll drop indexes from both live and stage
    ConfigJournalV1 preDrop =
        JournalEditor.on(configJournal)
            .fromStagedToDropped(removedStagedIndexes)
            .fromLiveToDropped(removedLiveIndexes)
            .journal();
    configState.persist(preDrop);
    return preDrop;
  }

  private static void dropDeletedIndexes(
      ConfigState configState, ImmutableList<IndexDefinitionGeneration> deletedIndexes)
      throws IOException, InvalidAnalyzerDefinitionException {
    // The deleted indexes were in the process of deleting or phasing out when mongot stopped
    // running. In order to clean them up, we'll have to actually create new Indexes for them, so
    // that we can drop them.
    for (IndexDefinitionGeneration definition : deletedIndexes) {
      LOG.atInfo()
          .addKeyValue("indexId", definition.getIndexId())
          .addKeyValue("generationId", definition.getGenerationId())
          .log("Dropping deleted index");

      Index index = configState.indexFactory.getIndex(definition);
      // Avoid initializing the index before closing it,
      // to prevent potential oom issues with large index data
      index.close();
      index.drop();
    }
  }

  private static void addLiveIndexes(
      ConfigState configState, List<IndexDefinitionGeneration> definitions)
      throws IOException, InvalidAnalyzerDefinitionException {
    IndexActions.withoutReplication(configState).addNewIndexes(definitions);
  }

  private static void addStagedIndexes(
      ConfigState configState, List<IndexDefinitionGeneration> definitions)
      throws IOException, InvalidAnalyzerDefinitionException {
    IndexActions.withoutReplication(configState).addStagedIndexes(definitions);
  }
}
