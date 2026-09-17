package com.xgen.mongot.config.manager;

import com.google.common.collect.Iterables;
import com.xgen.mongot.config.backup.ConfigJournalV1;
import com.xgen.mongot.config.backup.JournalEditor;
import com.xgen.mongot.config.util.IndexDefinitions;
import com.xgen.mongot.config.util.Invariants;
import com.xgen.mongot.index.IndexGeneration;
import com.xgen.mongot.index.analyzer.InvalidAnalyzerDefinitionException;
import com.xgen.mongot.index.analyzer.definition.OverriddenBaseAnalyzerDefinition;
import com.xgen.mongot.index.definition.IndexDefinitionGeneration;
import com.xgen.mongot.index.definition.IndexDefinitionGenerationProducer;
import com.xgen.mongot.index.definition.SearchIndexDefinition;
import com.xgen.mongot.index.definition.VectorIndexDefinition;
import com.xgen.mongot.index.version.GenerationId;
import com.xgen.mongot.index.version.UserIndexVersion;
import com.xgen.mongot.metrics.MetricsFactory;
import com.xgen.mongot.util.bson.JsonCodec;
import io.micrometer.core.instrument.Tags;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Updates ConfigState to reflect a desired set of index definitions.
 *
 * <p>DesiredConfigStateUpdater guarantees that changes to the config state are always durable and
 * written ahead of the actual changes. Failures during modifications to the config state aren't
 * going to leak indexes to disk.
 *
 * <p>Note that DesiredConfigStateUpdater can not persist a journal that violates invariants as
 * these are validated by ConfigState::persist.
 */
class DesiredConfigStateUpdater {
  private static final Logger LOG = LoggerFactory.getLogger(DesiredConfigStateUpdater.class);

  private final ConfigState configState;
  private final IndexActions indexActions;
  private final MetricsFactory metricsFactory;

  private DesiredConfigStateUpdater(ConfigState configState, MetricsFactory metricsFactory) {
    this.configState = configState;
    this.indexActions = IndexActions.withReplication(this.configState);
    this.metricsFactory = metricsFactory;
  }

  /**
   * Update the ConfigState in accordance with a set of desired index definitions. In case of
   * modifications, stage indexes for swaps.
   */
  static void update(
      ConfigState configState,
      List<VectorIndexDefinition> vectorDefinitions,
      List<SearchIndexDefinition> searchDefinitions,
      List<OverriddenBaseAnalyzerDefinition> analyzerDefinitions,
      List<IndexDefinitionGenerationProducer> producers,
      Set<ObjectId> unreadableDesiredIds,
      MetricsFactory metricsFactory)
      throws IOException, InvalidAnalyzerDefinitionException, Invariants.InvariantException {
    new DesiredConfigStateUpdater(configState, metricsFactory)
        .updateConfigState(
            vectorDefinitions,
            searchDefinitions,
            analyzerDefinitions,
            producers,
            unreadableDesiredIds);
  }

  private void updateConfigState(
      List<VectorIndexDefinition> vectorDefinitions,
      List<SearchIndexDefinition> searchDefinitions,
      List<OverriddenBaseAnalyzerDefinition> analyzerDefinitions,
      List<IndexDefinitionGenerationProducer> producers,
      Set<ObjectId> unreadableDesiredIds)
      throws IOException, InvalidAnalyzerDefinitionException, Invariants.InvariantException {

    try {
      // We validate the new index definitions against the current indexCatalog.
      Invariants.validateInvariants(
          analyzerDefinitions, searchDefinitions, vectorDefinitions, this.configState.indexCatalog);
    } catch (Invariants.InvariantException e) {
      LOG.atError()
          .addKeyValue("exceptionMessage", e.getMessage())
          .log("Supplied configuration violated invariant, not updating");
      return;
    }

    // Before planning: a superseded quarantined index the planner can still see is one it never
    // builds a replacement for.
    dropQuarantinedIndexes(unreadableDesiredIds);

    ConfigManagerChangePlan plan =
        ConfigManagerChangePlanner.plan(this.configState, producers, unreadableDesiredIds);

    logPlanInfo(plan);
    applyPlan(plan);
  }

  /**
   * Drops from the journal and from disk every quarantined index whose id the authoritative source
   * no longer reports as unreadable, which is how a quarantine ends: the index was deleted, or it
   * was updated to a definition we can parse and the replacement is built fresh.
   */
  private void dropQuarantinedIndexes(Set<ObjectId> unreadableDesiredIds)
      throws IOException, Invariants.InvariantException {
    IncompatibleIndexes incompatibleIndexes = this.configState.incompatibleIndexes;
    if (incompatibleIndexes.isEmpty()) {
      return;
    }

    List<QuarantinedIndex> droppedStaged =
        withoutKeptIds(incompatibleIndexes.getStagedIndexes(), unreadableDesiredIds);
    List<QuarantinedIndex> droppedLive =
        withoutKeptIds(incompatibleIndexes.getLiveIndexes(), unreadableDesiredIds);
    if (droppedStaged.isEmpty() && droppedLive.isEmpty()) {
      return;
    }

    logQuarantinedDrops(droppedStaged, "staged");
    logQuarantinedDrops(droppedLive, "live");

    // Write our intent to drop, as any other drop does, so a crash before the deletion is recorded.
    ConfigJournalV1 preDrop =
        JournalEditor.on(this.configState.currentJournal())
            .fromStagedToDropped(generationIds(droppedStaged))
            .fromLiveToDropped(generationIds(droppedLive))
            .journal();
    this.configState.persist(preDrop);

    // Stop tracking them, so the journal rewritten below no longer carries them at all.
    incompatibleIndexes.removeAllExcept(unreadableDesiredIds);

    this.indexActions.dropQuarantined(
        Stream.concat(droppedStaged.stream(), droppedLive.stream())
            .map(QuarantinedIndex::generationId)
            .toList());

    persistCurrentState();
  }

  private static List<QuarantinedIndex> withoutKeptIds(
      Collection<QuarantinedIndex> quarantinedIndexes, Set<ObjectId> keepIds) {
    return quarantinedIndexes.stream()
        .filter(quarantinedIndex -> !keepIds.contains(quarantinedIndex.indexId()))
        .toList();
  }

  private static List<GenerationId> generationIds(List<QuarantinedIndex> quarantinedIndexes) {
    return quarantinedIndexes.stream().map(QuarantinedIndex::generationId).toList();
  }

  private static void logQuarantinedDrops(List<QuarantinedIndex> dropped, String journalState) {
    for (QuarantinedIndex quarantinedIndex : dropped) {
      LOG.atWarn()
          .addKeyValue("indexId", quarantinedIndex.indexId())
          .addKeyValue("indexName", quarantinedIndex.name())
          .addKeyValue("database", quarantinedIndex.database())
          .addKeyValue("generationId", quarantinedIndex.generationId())
          .addKeyValue("journalState", journalState)
          .addKeyValue("parseError", quarantinedIndex.parseError())
          .log("Dropping quarantined index, it is no longer desired in an unreadable form");
    }
  }

  private void maybeRecordReindexing(ConfigManagerChangePlan plan) {
    Set<IndexChangeReason> allUniqueReasons =
        plan.modifiedIndexes().stream()
            .flatMap(info -> info.getReasons().stream())
            .map(IndexChangeReason::findByDescription)
            .collect(Collectors.toUnmodifiableSet());

    String reindexReason;
    if (allUniqueReasons.isEmpty()) {
      return;
    } else if (allUniqueReasons.size() == 1) {
      reindexReason = Iterables.getOnlyElement(allUniqueReasons).name();
    } else {
      reindexReason = "MULTIPLE";
    }
    this.metricsFactory.counter("reindex", Tags.of("reason", reindexReason)).increment();
  }

  private void applyPlan(ConfigManagerChangePlan plan)
      throws IOException, InvalidAnalyzerDefinitionException, Invariants.InvariantException {
    // start applying the plan on the config state in a durable manner, each of the following
    // methods writes ahead the appropriate config journal, then delegates the  addition/removal of
    // indexes to this.indexActions  The modification of the config state and journal are
    // incremental, so the order we call these methods doesn't matter.
    dropDeletedIndexes(plan.droppedIndexes());

    stageSwapsForModifiedIndexes(plan.modifiedIndexes());

    addNewIndexes(plan.addedIndexDefinitions());
  }

  private void logPlanInfo(ConfigManagerChangePlan plan) {

    if (plan.hasChanges()) {
      LOG.atInfo()
          .addKeyValue("configManagerChangePlan", JsonCodec.toJson(plan))
          .log("Created plan");
    }

    for (var definition : plan.addedIndexDefinitions()) {
      var indexDefinition = definition.getIndexDefinition();
      LOG.atInfo()
          .addKeyValue("type", indexDefinition.getType())
          .addKeyValue("indexId", indexDefinition.getIndexId())
          .addKeyValue("indexName", indexDefinition.getName())
          .addKeyValue("view", indexDefinition.getView())
          .addKeyValue("collection", indexDefinition.getLastObservedCollectionName())
          .addKeyValue("collectionUuid", indexDefinition.getCollectionUuid())
          .addKeyValue("database", indexDefinition.getDatabase())
          .log("Adding index definition");
    }

    for (var index : plan.droppedIndexes()) {
      LOG.atInfo().addKeyValue("indexId", index).log("Dropping index");
    }

    for (var modifiedIndexInfo : plan.modifiedIndexes()) {
      LOG.atInfo()
          .addKeyValue("indexId", modifiedIndexInfo.getIndexId())
          .addKeyValue("modificationReasons", modifiedIndexInfo.getReasons())
          .addKeyValue(
              "desiredDefinition", modifiedIndexInfo.getDesiredDefinition().getIndexDefinition())
          .log("Index modification: index differs with '{}'.", modifiedIndexInfo.getType());
    }

    maybeRecordReindexing(plan);
  }

  /** Drops indexes with provided ids from staged, index catalog and phasingOut. */
  private void dropDeletedIndexes(List<ObjectId> droppedIds)
      throws IOException, Invariants.InvariantException {
    // We drop these indexes from anywhere they could exist in
    List<IndexGeneration> droppedStagedIndexes = getFromStaged(droppedIds);
    List<IndexGeneration> droppedLiveIndexes = getFromCatalog(droppedIds);
    List<IndexGeneration> droppedPhasingOutIndexes = getFromPhasingOut(droppedIds);

    // Write our intent to drop these indexes (the phasing out indexes are already journaled as
    // dropped).
    ConfigJournalV1 preDrop =
        JournalEditor.on(this.configState.currentJournal())
            .fromStagedToDropped(IndexDefinitions.indexesGenerationIds(droppedStagedIndexes))
            .fromLiveToDropped(IndexDefinitions.indexesGenerationIds(droppedLiveIndexes))
            .journal();
    this.configState.persist(preDrop);

    // we drop the staged ones first to avoid having staged indexes without corresponding indexes in
    // the catalog.
    this.indexActions.dropFromStaged(droppedStagedIndexes);
    this.indexActions.dropFromCatalog(droppedLiveIndexes);
    this.indexActions.dropFromPhasingOut(droppedPhasingOutIndexes);

    // Rewrite the config journal without the dropped indexes now that we know they were
    // deleted.
    persistCurrentState();
  }

  private void addNewIndexes(List<IndexDefinitionGenerationProducer> addedDefinitions)
      throws IOException, InvalidAnalyzerDefinitionException, Invariants.InvariantException {
    // Assign a first generation for newly added indexes.
    List<IndexDefinitionGeneration> addedIndexes =
        addedDefinitions.stream().map(this::newUserVersion).collect(Collectors.toList());

    // Write our intent to have these indexes exist in addition to the existing ones.
    ConfigJournalV1 preAdd =
        JournalEditor.on(this.configState.currentJournal()).addLive(addedIndexes).journal();
    this.configState.persist(preAdd);

    // Actually add the indexes.
    this.indexActions.addNewIndexes(addedIndexes);
  }

  private void stageSwapsForModifiedIndexes(List<ModifiedIndexInformation> modifications)
      throws IOException, InvalidAnalyzerDefinitionException, Invariants.InvariantException {
    for (ModifiedIndexInformation modifiedIndex : modifications) {
      if (modifiedIndex.getLiveIndex().getDefinitionGeneration().hasCurrentFormatVersion()) {
        stageSwapForModifiedIndex(modifiedIndex);
      } else {
        stageSwapForModifiedOutdatedFormatLiveIndex(modifiedIndex);
      }
    }
  }

  private void stageSwapForModifiedIndex(ModifiedIndexInformation modification)
      throws IOException, Invariants.InvariantException, InvalidAnalyzerDefinitionException {

    switch (modification.getType()) {
      case DIFFERENT_FROM_LIVE_NO_STAGED -> {
        // There hasn't been a previous swap for this index, we only need to stage one.
        LOG.atInfo()
            .addKeyValue("indexId", modification.getIndexId())
            .log(
                "Index modification: no existing staged index,"
                    + " staging new index with desired definition.");
        stageSwap(modification.getDesiredDefinition());
      }
      case SAME_AS_LIVE_DIFFERENT_FROM_STAGED -> {
        // The index definition was changed back to the one we happen to have in the catalog
        // already. all we need is to drop the staged one:
        LOG.atInfo()
            .addKeyValue("indexId", modification.getIndexId())
            .log("Index modification: desired definition matches live, dropping staged.");
        dropStaged(modification.asSameAsLiveDifferentFromStaged().getStagedIndex());
      }
      case DIFFERENT_FROM_BOTH -> {
        // Definition differs from a swap we currently have running, we re-create the staged index.
        LOG.atInfo()
            .addKeyValue("indexId", modification.getIndexId())
            .log("Index modification: re-creating staged index with desired definition.");
        dropStaged(modification.asDifferentFromBoth().getStagedIndex());
        stageSwap(modification.getDesiredDefinition());
      }
    }
  }

  private void stageSwapForModifiedOutdatedFormatLiveIndex(ModifiedIndexInformation modification)
      throws Invariants.InvariantException, IOException, InvalidAnalyzerDefinitionException {
    switch (modification.getType()) {
      case DIFFERENT_FROM_LIVE_NO_STAGED -> {
        // If the live index is outdated, we should have staged an index with upgraded format
        // version at startup via IndexFormatVersionUpgrader. If there's no staged index, this means
        // we somehow dropped the staged index or never staged one. Either way, this is a bug.
        LOG.atError()
            .addKeyValue("indexId", modification.getIndexId())
            .log(
                "Index modification:"
                    + " live index has outdated format version but no staged index exists!"
                    + " Staging new current index.");
        stageSwap(modification.getDesiredDefinition());
      }
      case SAME_AS_LIVE_DIFFERENT_FROM_STAGED -> {
        // Since the live index is an outdated format version, we still need to stage a new index.
        LOG.atInfo()
            .addKeyValue("indexId", modification.getIndexId())
            .log(
                "Index modification:"
                    + " desired definition matches live, but live has outdated format version."
                    + " Staging new index with desired definition.");
        dropStaged(modification.asSameAsLiveDifferentFromStaged().getStagedIndex());
        stageSwap(modification.getDesiredDefinition());
      }
      case DIFFERENT_FROM_BOTH -> {
        // Definition differs from a swap we currently have running, we re-create the staged index.
        LOG.atInfo()
            .addKeyValue("indexId", modification.getIndexId())
            .log("Index modification: re-creating staged index with desired definition.");
        dropStaged(modification.asDifferentFromBoth().getStagedIndex());
        stageSwap(modification.getDesiredDefinition());
      }
    }
  }

  private void stageSwap(IndexDefinitionGenerationProducer desiredDefinition)
      throws IOException, InvalidAnalyzerDefinitionException, Invariants.InvariantException {
    // use the new definition with incremented user version value
    IndexDefinitionGeneration addedStaged = newUserVersion(desiredDefinition);

    ConfigJournalV1 preAdd =
        JournalEditor.on(this.configState.currentJournal()).addStaged(addedStaged).journal();
    this.configState.persist(preAdd);

    this.indexActions.addStagedIndex(addedStaged);
  }

  private void dropStaged(IndexGeneration staged)
      throws IOException, Invariants.InvariantException {
    ConfigJournalV1 preDrop =
        JournalEditor.on(this.configState.currentJournal())
            .fromStagedToDropped(List.of(staged.getGenerationId()))
            .journal();
    this.configState.persist(preDrop);

    this.indexActions.dropFromStaged(List.of(staged));

    // persist without the deletion
    persistCurrentState();
  }

  private List<IndexGeneration> getFromStaged(List<ObjectId> droppedIds) {
    return droppedIds.stream()
        .map(this.configState.staged::getIndex)
        .flatMap(Optional::stream)
        .collect(Collectors.toList());
  }

  private List<IndexGeneration> getFromCatalog(List<ObjectId> droppedIds) {
    return droppedIds.stream()
        .map(this.configState.indexCatalog::getIndexById)
        .flatMap(Optional::stream)
        .collect(Collectors.toList());
  }

  private List<IndexGeneration> getFromPhasingOut(List<ObjectId> droppedIds) {
    return droppedIds.stream()
        .map(this.configState.phasingOut::getIndexesById)
        .flatMap(Collection::stream)
        .collect(Collectors.toList());
  }

  /**
   * Increments a user index version - to be higher than all existing indexes with the same indexId,
   * so that GenerationIds are unique.
   */
  private IndexDefinitionGeneration newUserVersion(
      IndexDefinitionGenerationProducer generationProducer) {
    UserIndexVersion userIndexVersion =
        this.configState.getNewUserIndexVersion(
            generationProducer.getIndexDefinition().getIndexId());
    return generationProducer.createIndexDefinitionGeneration(
        generationProducer.getIndexDefinition().newGeneration(userIndexVersion));
  }

  private void persistCurrentState() throws IOException, Invariants.InvariantException {
    this.configState.persist(this.configState.currentJournal());
  }
}
