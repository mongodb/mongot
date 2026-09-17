package com.xgen.mongot.config.manager;

import com.xgen.mongot.config.backup.ConfigJournalV1;
import com.xgen.mongot.config.backup.JournalEditor;
import com.xgen.mongot.config.util.Invariants;
import com.xgen.mongot.index.IndexGeneration;
import com.xgen.mongot.index.analyzer.InvalidAnalyzerDefinitionException;
import com.xgen.mongot.index.definition.IndexDefinitionGeneration;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.CollectionUtils;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IndexFormatVersionUpgrader {

  private static final Logger LOGGER = LoggerFactory.getLogger(IndexFormatVersionUpgrader.class);

  private final ConfigState configState;
  private final IndexActions indexActions;

  private IndexFormatVersionUpgrader(ConfigState configState) {
    this.configState = configState;
    this.indexActions = IndexActions.withoutReplication(this.configState);
  }

  /**
   * Stages upgraded versions of live indexes not matching the current index format version, and
   * drops any indexes whose format version is unsupported.
   */
  public static void upgradeAndDrop(ConfigState configState)
      throws Invariants.InvariantException, IOException, InvalidAnalyzerDefinitionException {
    new IndexFormatVersionUpgrader(configState).updateIndexFormatVersions();
  }

  private void updateIndexFormatVersions()
      throws Invariants.InvariantException, IOException, InvalidAnalyzerDefinitionException {
    // The overall strategy by which we stage indexes, is to first determine the "canonical" index
    // definition generation for any given index id. The logic for this is somewhat straightforward;
    // we prefer to upgrade based off the index definition of the staged index, if one exists. Since
    // staged indexes will always eventually be swapped to live, it is a reasonable assumption that
    // the staged index has the "freshest" definition, even in theoretical situations where the
    // staged index's format version is older than the live index.
    //
    // Regardless of what we do here, there is always the possibility that a new conf call cycle
    // will change definitions, but by doing this, we create the most accurate reflection of the
    // most recent conf call that the loaded index journal represents.
    Map<ObjectId, IndexDefinitionGeneration> canonicalDefinitionGenerationMap =
        this.configState.indexCatalog.getIndexes().stream()
            .map(
                index ->
                    this.configState
                        .staged
                        .getIndex(index.getDefinition().getIndexId())
                        .orElse(index))
            .collect(
                CollectionUtils.toMapUnsafe(
                    indexGeneration -> indexGeneration.getDefinition().getIndexId(),
                    IndexGeneration::getDefinitionGeneration));

    // First, we drop all staged indexes that are no longer current. However, unlike for the live
    // indexes, we won't immediately attempt to stage a new, current version of these indexes.
    //
    // This is because, at time of writing, there are a wide variety of reasons an index may have
    // had a new copy staged in the first place. The live copy may have fallen off the oplog, the
    // index definition may have changed, or the index may have been pending a format version
    // upgrade initiated by an older or newer version of mongot. Rather than try to guess the
    // "correct" action to take to replace the staged index here based on the inferred reason the
    // index was staged in the first place, we'll rely on the remainder of the config manager
    // machinery to incrementally correct the changes we make here on startup.
    List<IndexGeneration> nonCurrentStagedIndexGenerations =
        this.configState.staged.getIndexes().stream()
            .filter(Predicate.not(IndexFormatVersionUpgrader::isCurrentFormatVersion))
            .collect(Collectors.toList());
    dropNonCurrentStagedIndexes(nonCurrentStagedIndexGenerations);

    // Next, we'll find all live indexes that are not the current index format version. This may
    // include unsupported indexes, which will be handled differently than live indexes which are
    // still supported. Supported indexes will have an upgraded copy staged, while unsupported
    // indexes will be dropped and an upgraded copy added in its place using the same index
    // definition as the dropped live index.
    List<IndexGeneration> nonCurrentLiveIndexGenerations =
        this.configState.indexCatalog.getIndexes().stream()
            .filter(Predicate.not(IndexFormatVersionUpgrader::isCurrentFormatVersion))
            .collect(Collectors.toList());

    // If all live indexes are current, there's nothing to do.
    if (nonCurrentLiveIndexGenerations.isEmpty()) {
      return;
    }

    nonCurrentLiveIndexGenerations.forEach(
        e ->
            LOGGER
                .atInfo()
                .addKeyValue("indexId", e.getGenerationId().indexId)
                .addKeyValue("generationId", e.getGenerationId())
                .log("Going to upgrade index format version for non-current index"));

    for (IndexGeneration liveIndex : nonCurrentLiveIndexGenerations) {
      boolean liveIndexIsSupported = isSupportedFormatVersion(liveIndex);
      Optional<IndexGeneration> stagedIndexOptional =
          this.configState.staged.getIndex(liveIndex.getDefinition().getIndexId());
      IndexDefinitionGeneration canonicalDefinitionGeneration =
          canonicalDefinitionGenerationMap.get(liveIndex.getDefinition().getIndexId());
      Check.isNotNull(canonicalDefinitionGeneration, "canonicalDefinitionGeneration");

      if (liveIndexIsSupported) {
        upgradeSupportedLiveIndex(liveIndex, canonicalDefinitionGeneration, stagedIndexOptional);
      } else {
        upgradeUnsupportedLiveIndex(liveIndex, canonicalDefinitionGeneration, stagedIndexOptional);
      }
    }

    // We drop phasing-out indexes here, otherwise unsupported live indexes won't be cleaned up
    // until this happens. Normally phasing-out indexes won't get dropped until the first config
    // cycle, but since any phasing-out indexes present at startup are dropped anyway, dropping
    // them a bit early shouldn't have any ill effects.
    PhasingOutIndexesDropper.dropUnused(this.configState, this.indexActions);
  }

  private void dropNonCurrentStagedIndexes(List<IndexGeneration> nonCurrentStagedIndexGenerations)
      throws Invariants.InvariantException, IOException {
    // If there aren't any non-current staged indexes, there's nothing to do.
    if (nonCurrentStagedIndexGenerations.isEmpty()) {
      return;
    }

    nonCurrentStagedIndexGenerations.forEach(
        e ->
            LOGGER
                .atInfo()
                .addKeyValue("indexId", e.getGenerationId().indexId)
                .addKeyValue("generationId", e.getGenerationId())
                .log("Going to drop non-current staged index"));

    dropStagedIndexes(nonCurrentStagedIndexGenerations);
  }

  private void upgradeSupportedLiveIndex(
      IndexGeneration liveIndex,
      IndexDefinitionGeneration canonicalIndexDefinition,
      Optional<IndexGeneration> stagedIndexOptional)
      throws Invariants.InvariantException, IOException, InvalidAnalyzerDefinitionException {
    boolean stagedIndexIsCurrent =
        stagedIndexOptional.map(IndexFormatVersionUpgrader::isCurrentFormatVersion).orElse(false);
    Check.checkState(
        stagedIndexOptional.isEmpty() || stagedIndexIsCurrent,
        "Non-current staged index should have been dropped, but still exists.");

    // Don't do anything if the currently staged index is current. Wait for it to swap in.
    if (stagedIndexIsCurrent) {
      LOGGER
          .atInfo()
          .addKeyValue("liveIndexId", liveIndex.getGenerationId().indexId)
          .addKeyValue("liveIndexGeneration", liveIndex.getGenerationId().generation)
          .addKeyValue("stagedIndexId", stagedIndexOptional.get().getGenerationId().indexId)
          .addKeyValue(
              "stagedIndexGeneration", stagedIndexOptional.get().getGenerationId().generation)
          .log("Not staging upgrade for live index with current staged index");
      return;
    }

    LOGGER
        .atInfo()
        .addKeyValue("liveIndexId", liveIndex.getGenerationId().indexId)
        .addKeyValue("liveIndexGeneration", liveIndex.getGenerationId().generation)
        .addKeyValue("baseIndexId", canonicalIndexDefinition.getGenerationId().indexId)
        .addKeyValue("baseIndexGeneration", canonicalIndexDefinition.getGenerationId().generation)
        .log("Staging upgrade for live index using base index.");

    stageUpgradedIndex(canonicalIndexDefinition);
  }

  private void stageUpgradedIndex(IndexDefinitionGeneration indexDefinitionGeneration)
      throws Invariants.InvariantException, IOException, InvalidAnalyzerDefinitionException {
    // Create a DefinitionGeneration with the current format version.
    IndexDefinitionGeneration currentDefinitionGeneration =
        indexDefinitionGeneration.upgradeToCurrentFormatVersion();

    // Write our intent to add a new index generation to the config journal.
    ConfigJournalV1 preAdd =
        JournalEditor.on(this.configState.currentJournal())
            .addStaged(currentDefinitionGeneration)
            .journal();
    this.configState.persist(preAdd);

    this.indexActions.addStagedIndex(currentDefinitionGeneration);
  }

  private void dropStagedIndexes(List<IndexGeneration> staged)
      throws IOException, Invariants.InvariantException {
    // Journal our intent to drop the indexes.
    ConfigJournalV1 preDrop =
        JournalEditor.on(this.configState.currentJournal())
            .fromStagedToDropped(
                staged.stream()
                    .map(IndexGeneration::getDefinitionGeneration)
                    .map(IndexDefinitionGeneration::getGenerationId)
                    .collect(Collectors.toList()))
            .journal();
    this.configState.persist(preDrop);

    // Actually drop the indexes from configState.
    this.indexActions.dropFromStaged(staged);

    // Persist the journal without the deletion.
    this.configState.persist(this.configState.currentJournal());
  }

  private void upgradeUnsupportedLiveIndex(
      IndexGeneration liveIndex,
      IndexDefinitionGeneration canonicalIndexDefinition,
      Optional<IndexGeneration> stagedIndexOptional)
      throws Invariants.InvariantException, IOException, InvalidAnalyzerDefinitionException {
    boolean stagedIndexIsCurrent =
        stagedIndexOptional.map(IndexFormatVersionUpgrader::isCurrentFormatVersion).orElse(false);
    Check.checkState(
        stagedIndexOptional.isEmpty() || stagedIndexIsCurrent,
        "Non-current staged index should have been dropped, but still exists.");

    // If for some reason the staged index is the current format version but the live index is
    // unsupported, we'll promote the staged index to live, and not do anything else here.
    //
    // This should be an exceedingly rare case, but it is theoretically possible for this to happen
    // if we support more than 2 index format versions at a time.
    if (stagedIndexIsCurrent) {
      IndexGeneration stagedIndex = stagedIndexOptional.get();

      LOGGER
          .atInfo()
          .addKeyValue("liveIndexId", liveIndex.getGenerationId().indexId)
          .addKeyValue("liveIndexGeneration", liveIndex.getGenerationId().generation)
          .addKeyValue("stagedIndexId", stagedIndex.getGenerationId().indexId)
          .addKeyValue("stagedIndexGeneration", stagedIndex.getGenerationId().generation)
          .log("Swapping unsupported live index with current format version staged index");
      StagedIndexesSwapper.swap(this.configState, List.of(stagedIndex), List.of(liveIndex));
      return;
    }

    // At this point, we have an unsupported live index, and if a staged index existed, it was
    // non-current and was dropped. We'll replace the existing live index with a new, current one.
    LOGGER
        .atInfo()
        .addKeyValue("liveIndexId", liveIndex.getGenerationId().indexId)
        .addKeyValue("liveIndexGeneration", liveIndex.getGenerationId().generation)
        .addKeyValue("baseIndexId", canonicalIndexDefinition.getGenerationId().indexId)
        .addKeyValue("baseIndexGeneration", canonicalIndexDefinition.getGenerationId().generation)
        .log("Replacing and upgrading live index using base index");

    // Journal our intent to drop the index.
    ConfigJournalV1 preDrop =
        JournalEditor.on(this.configState.currentJournal())
            .fromLiveToDropped(List.of(liveIndex.getGenerationId()))
            .journal();
    this.configState.persist(preDrop);

    // Actually drop the index.
    this.indexActions.dropFromCatalog(List.of(liveIndex));

    // Create a DefinitionGeneration with the current format version.
    IndexDefinitionGeneration currentDefinitionGeneration =
        canonicalIndexDefinition.upgradeToCurrentFormatVersion();

    // Write our intent to add a new index generation to the config journal.
    ConfigJournalV1 preAdd =
        JournalEditor.on(this.configState.currentJournal())
            .addLive(List.of(currentDefinitionGeneration))
            .journal();
    this.configState.persist(preAdd);

    this.indexActions.addNewIndexes(List.of(currentDefinitionGeneration));
  }

  private static boolean isCurrentFormatVersion(IndexGeneration indexGeneration) {
    return indexGeneration.getDefinitionGeneration().hasCurrentFormatVersion();
  }

  private static boolean isSupportedFormatVersion(IndexGeneration indexGeneration) {
    return indexGeneration.getDefinitionGeneration().hasSupportedFormatVersion();
  }
}
