package com.xgen.mongot.config.manager;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.errorprone.annotations.Var;
import com.xgen.mongot.config.backup.ConfigJournalV1;
import com.xgen.mongot.config.backup.JournalEditor;
import com.xgen.mongot.config.backup.JournalEntry;
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
import java.util.stream.Stream;
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
    int numIncompatibleIndexes = configJournal.getQuarantinedEntries().size();

    LOG.atInfo()
        .addKeyValue("numExistingIndexes", configJournal.getLiveIndexes().size())
        .addKeyValue("numDeletedIndexes", configJournal.getDeletedIndexes().size())
        .addKeyValue("numStagedIndexes", configJournal.getStagedIndexes().size())
        .addKeyValue("numIncompatibleIndexes", numIncompatibleIndexes)
        .log(
            "Found config journal. "
                + "Initializing existing indexes, cleaning up deleted indexes, staging indexes");

    // Must precede the invariant check: a parseable staged index whose only live counterpart is
    // quarantined looks orphaned to checks that see parsed definitions only.
    if (numIncompatibleIndexes > 0) {
      configJournal = moveSupersededQuarantinedIndexesToDropped(configState, configJournal);
    }

    validateJournalInvariants(configJournal);

    // remove indexes that are not present in the initial confcall response,
    // but are still present in the config journal.
    if (desiredIndexIds.isPresent()) {
      configJournal =
          removeDeletedIndexesFromConfigJournal(
              configState, desiredIndexIds.get(), configJournal, meterRegistry);
    }

    // Drop indexes that were ready for deletion before mongot stopped running.
    dropDeletedIndexes(configState, configJournal.getDeletedEntries());

    // Add the indexes that should exist.
    addLiveIndexes(configState, configJournal.getLiveIndexes());

    // Add the staged indexes that should exist (must be after adding live indexes so
    // we don't have "orphan" staged indexes).
    addStagedIndexes(configState, configJournal.getStagedIndexes());

    // Record the entries whose definitions could not be parsed, so they can be reported on. They
    // have no index to create, but must be tracked before we persist so they are written back out.
    if (numIncompatibleIndexes > 0) {
      addIncompatibleIndexes(configState, configJournal);
    }

    // Counted from what is still quarantined after moveSupersededQuarantinedIndexesToDropped ran,
    // not from configJournal.getQuarantinedEntries(): a quarantined live entry that a parseable
    // staged successor auto-resolved at boot is not a degraded index, and counting it would fire an
    // alert for a condition that healed itself.
    MetricsFactory metricsFactory = new MetricsFactory("ConfigInitializer", meterRegistry);
    int numQuarantinedLiveIndexes = configState.incompatibleIndexes.getLiveIndexes().size();
    int numQuarantinedStagedIndexes = configState.incompatibleIndexes.getStagedIndexes().size();
    if (numQuarantinedLiveIndexes > 0) {
      metricsFactory
          .counter("quarantinedIndexesAtStartup", Tags.of("type", "live"))
          .increment(numQuarantinedLiveIndexes);
    }
    if (numQuarantinedStagedIndexes > 0) {
      metricsFactory
          .counter("quarantinedIndexesAtStartup", Tags.of("type", "staged"))
          .increment(numQuarantinedStagedIndexes);
    }

    // Rewrite the config journal for our current state, without the deleted
    // indexes so we don't have to drop them again if we restart.
    configState.persist(configState.currentJournal());
  }

  /**
   * Drops quarantined live entries that a parseable staged index can replace, promoting that staged
   * index into their place. Persisting the move before {@link #dropDeletedIndexes} deletes anything
   * is what lets a restart part way through resume.
   *
   * <p>Quarantined entries with no parseable successor stay: such an entry is the only copy of that
   * index, and a binary that merely cannot read a definition should not delete it.
   */
  private static ConfigJournalV1 moveSupersededQuarantinedIndexesToDropped(
      ConfigState configState, ConfigJournalV1 configJournal)
      throws Invariants.InvariantException, IOException {
    ImmutableSet<ObjectId> stagedIndexIds =
        configJournal.getStagedIndexes().stream()
            .map(IndexDefinitionGeneration::getIndexId)
            .collect(ImmutableSet.toImmutableSet());

    List<QuarantinedIndex> superseded =
        quarantined(configJournal.getLiveEntries())
            .filter(index -> stagedIndexIds.contains(index.indexId()))
            .toList();

    if (superseded.isEmpty()) {
      return configJournal;
    }

    ImmutableSet<ObjectId> supersededIndexIds =
        superseded.stream().map(QuarantinedIndex::indexId).collect(ImmutableSet.toImmutableSet());
    List<GenerationId> successors =
        configJournal.getStagedIndexes().stream()
            .filter(definition -> supersededIndexIds.contains(definition.getIndexId()))
            .map(IndexDefinitionGeneration::getGenerationId)
            .toList();

    superseded.forEach(
        index ->
            LOG.atInfo()
                .addKeyValue("indexId", index.indexId())
                .addKeyValue("generationId", index.generationId())
                .log("Dropping quarantined live index superseded by a parseable staged index"));

    ConfigJournalV1 preDrop =
        JournalEditor.on(configJournal)
            .fromLiveToDropped(superseded.stream().map(QuarantinedIndex::generationId).toList())
            .fromStagedToLive(successors)
            .journal();
    configState.persist(preDrop);
    return preDrop;
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
      ConfigState configState,
      ImmutableList<JournalEntry<? extends IndexDefinitionGeneration>> deletedEntries)
      throws IOException, InvalidAnalyzerDefinitionException {
    // The deleted indexes were in the process of deleting or phasing out when mongot stopped
    // running. In order to clean them up, we'll have to actually create new Indexes for them, so
    // that we can drop them.
    for (JournalEntry<? extends IndexDefinitionGeneration> entry : deletedEntries) {
      LOG.atInfo()
          .addKeyValue("indexId", entry.getIdentity().indexId())
          .addKeyValue("generationId", entry.getGenerationId())
          .addKeyValue("quarantined", entry.isQuarantined())
          .log("Dropping deleted index");

      Optional<? extends IndexDefinitionGeneration> definition = entry.getDefinition();
      if (definition.isEmpty()) {
        // No definition to build an Index from, so the data goes by generation id. Safe here: the
        // journal is only read at startup, so this entry was never in the catalog and has no
        // cursors to wait on.
        configState.indexFactory.deleteGenerationData(entry.getGenerationId());
        continue;
      }

      Index index = configState.indexFactory.getIndex(definition.get());
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

  /**
   * Tracks the staged and live entries that could not be parsed. Deleted entries are not tracked:
   * everything in the journal's deleted arrays is dropped at startup.
   *
   * <p>The per-entry log is at ERROR because a quarantined index is a silently degraded one: a node
   * that used to crash-loop loudly now starts quietly, so this log is the per-index record an
   * operator needs.
   */
  private static void addIncompatibleIndexes(
      ConfigState configState, ConfigJournalV1 configJournal) {
    quarantined(configJournal.getLiveEntries())
        .forEach(
            quarantinedIndex -> {
              LOG.atError()
                  .addKeyValue("indexId", quarantinedIndex.indexId())
                  .addKeyValue("name", quarantinedIndex.name())
                  .addKeyValue("database", quarantinedIndex.database())
                  .addKeyValue("generationId", quarantinedIndex.generationId())
                  .addKeyValue("journalState", "live")
                  .addKeyValue("parseError", quarantinedIndex.parseError())
                  .log("adding incompatible live index");
              configState.incompatibleIndexes.addLiveIndex(quarantinedIndex);
            });

    quarantined(configJournal.getStagedEntries())
        .forEach(
            quarantinedIndex -> {
              LOG.atError()
                  .addKeyValue("indexId", quarantinedIndex.indexId())
                  .addKeyValue("name", quarantinedIndex.name())
                  .addKeyValue("database", quarantinedIndex.database())
                  .addKeyValue("generationId", quarantinedIndex.generationId())
                  .addKeyValue("journalState", "staged")
                  .addKeyValue("parseError", quarantinedIndex.parseError())
                  .log("adding incompatible staged index");
              configState.incompatibleIndexes.addStagedIndex(quarantinedIndex);
            });
  }

  private static Stream<QuarantinedIndex> quarantined(
      List<JournalEntry<? extends IndexDefinitionGeneration>> entries) {
    return entries.stream()
        .filter(entry -> entry instanceof JournalEntry.Quarantined)
        .map(entry -> QuarantinedIndex.from((JournalEntry.Quarantined<?>) entry));
  }
}
