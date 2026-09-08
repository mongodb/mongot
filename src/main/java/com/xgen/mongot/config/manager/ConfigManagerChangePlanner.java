package com.xgen.mongot.config.manager;

import static com.xgen.mongot.util.Check.checkState;

import com.google.common.base.Functions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.xgen.mongot.catalog.IndexCatalog;
import com.xgen.mongot.config.util.IndexDefinitions;
import com.xgen.mongot.index.IndexGeneration;
import com.xgen.mongot.index.analyzer.AnalyzerChangePlan;
import com.xgen.mongot.index.analyzer.definition.OverriddenBaseAnalyzerDefinition;
import com.xgen.mongot.index.definition.AnalyzerBoundSearchIndexDefinition;
import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.definition.IndexDefinitionGeneration;
import com.xgen.mongot.index.definition.IndexDefinitionGenerationProducer;
import com.xgen.mongot.index.definition.SearchIndexDefinition;
import com.xgen.mongot.index.definition.VectorIndexDefinition;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.CollectionUtils;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.bson.types.ObjectId;

/**
 * ConfigManagerChangePlanner is capable of producing ConfigManagerChangePlans for a ConfigManager.
 */
class ConfigManagerChangePlanner {

  private final IndexCatalog indexCatalog;
  private final StagedIndexes staged;

  private ConfigManagerChangePlanner(ConfigState configState) {
    this.indexCatalog = configState.indexCatalog;
    this.staged = configState.staged;
  }

  /**
   * Contains information about which index definitions that exist in both the current and desired
   * state have been modified in ways that require re-indexing.
   */
  private record OverlappingIndexInfo(
      List<ModifiedIndexInformation> modified, List<ObjectId> unmodified) {}

  /**
   * Creates a plan for how to change a ConfigManager given the desired producers and set if
   * incompatible indexes that should be dropped immediately.
   *
   * <p>Note that this assumes that the IndexCatalog is immutable for the duration of the call to
   * plan, and the plan is only valid so long as it is immutable.
   *
   * <p>It is assumed that the definitions provided are valid and that the references to all the
   * analyzers are satisfied.
   *
   * @param unreadableDesiredIds ids that are still desired but whose authoritative definition could
   *     not be used. Pass an empty set when every definition was readable. Only pass ids from a
   *     listing that <em>succeeded</em>: a failed read must not reach the planner at all, or every
   *     index would look deleted.
   */
  public static ConfigManagerChangePlan plan(
      ConfigState configState,
      List<IndexDefinitionGenerationProducer> producers,
      Set<ObjectId> unreadableDesiredIds) {
    return new ConfigManagerChangePlanner(configState).plan(producers, unreadableDesiredIds);
  }

  private ConfigManagerChangePlan plan(
      List<IndexDefinitionGenerationProducer> desiredIndexes, Set<ObjectId> unreadableDesiredIds) {
    // Get the ids that exist and are desired.
    Map<ObjectId, IndexDefinitionGenerationProducer> desiredIndexesById =
        desiredIndexes.stream()
            .collect(
                CollectionUtils.toMapUnsafe(
                    index -> index.getIndexDefinition().getIndexId(), Function.identity()));

    Map<ObjectId, ExistingIndex> existingIndexesById = existingIndexesById();
    Set<ObjectId> desiredIds = desiredIndexesById.keySet();
    Set<ObjectId> existingIds = existingIndexesById.keySet();

    // Group the indexes by those that didn't exist before, no longer exist, and exist in
    // both the desired and current state. An index whose authoritative definition could not be
    // used is absent from desiredIds, but it is not deleted -- it is unreadable -- so it is left
    // out of the removed group. It is likewise never staged or modified, since staging only
    // considers ids present in both existingIds and desiredIds.
    List<ObjectId> removedIndexes =
        getRemovedIndexes(existingIds, desiredIds, unreadableDesiredIds);
    List<IndexDefinitionGenerationProducer> addedIndexes =
        getAddedIndexDefinitions(desiredIndexesById, existingIds, desiredIds);

    ImmutableSet<ObjectId> overlappingIndexIds = getOverlappingIndexIds(existingIds, desiredIds);

    // For the index definitions that exist in both the desired and current state, find which have
    // changed in ways that require them to be re-indexed.
    OverlappingIndexInfo overlappingIndexInfo =
        getModifiedIndexes(overlappingIndexIds, existingIndexesById, desiredIndexesById);

    return new ConfigManagerChangePlan(
        addedIndexes,
        removedIndexes,
        overlappingIndexInfo.modified(),
        overlappingIndexInfo.unmodified());
  }

  private Map<ObjectId, ExistingIndex> existingIndexesById() {
    Map<ObjectId, IndexDefinitionGeneration> catalogIndexesById =
        IndexDefinitions.versionedById(this.indexCatalog.getIndexes());
    Map<ObjectId, IndexDefinitionGeneration> stagedIndexesById =
        IndexDefinitions.versionedById(this.staged.getIndexes());

    Set<ObjectId> stagedWithoutLive =
        Sets.difference(stagedIndexesById.keySet(), catalogIndexesById.keySet());
    checkState(
        stagedWithoutLive.isEmpty(),
        "staged indexes present without corresponding index in catalog: %s",
        stagedWithoutLive);

    return this.indexCatalog.getIndexes().stream()
        .map(
            index -> {
              ObjectId id = index.getDefinition().getIndexId();
              Optional<IndexGeneration> staged = this.staged.getIndex(id);
              return new ExistingIndex(index, staged);
            })
        .collect(CollectionUtils.toMapUnsafe(ExistingIndex::getIndexId, Functions.identity()));
  }

  /**
   * Returns a indexes that have ids that are not in the current set, but are in the desired set of
   * indexes.
   */
  private List<IndexDefinitionGenerationProducer> getAddedIndexDefinitions(
      Map<ObjectId, IndexDefinitionGenerationProducer> indexesById,
      Set<ObjectId> existingIds,
      Set<ObjectId> desiredIds) {
    Sets.SetView<ObjectId> addedIds = Sets.difference(desiredIds, existingIds);
    return addedIds.stream().map(indexesById::get).collect(Collectors.toList());
  }

  /**
   * Returns a list of Index ids that are in the current set, but are not in the desired set of
   * indexes. An id whose definition could not be used is left out: it is missing from the desired
   * set for a reason that is not "the user deleted it".
   */
  private static List<ObjectId> getRemovedIndexes(
      Set<ObjectId> existingIds, Set<ObjectId> desiredIds, Set<ObjectId> unreadableDesiredIds) {
    Sets.SetView<ObjectId> removedIds = Sets.difference(existingIds, desiredIds);

    if (unreadableDesiredIds.isEmpty()) {
      return new ArrayList<>(removedIds);
    }

    return new ArrayList<>(Sets.difference(removedIds, unreadableDesiredIds));
  }

  /** Returns a list of IndexIds that exist both in the desired set and the current set. */
  private ImmutableSet<ObjectId> getOverlappingIndexIds(
      Set<ObjectId> existingIds, Set<ObjectId> desiredIds) {
    Sets.SetView<ObjectId> overlappingIds = Sets.intersection(desiredIds, existingIds);
    return overlappingIds.immutableCopy();
  }

  /**
   * Returns OverlappingIndexInfo for each index that has an id that exists in both the current set
   * and the desired set.
   */
  private OverlappingIndexInfo getModifiedIndexes(
      Set<ObjectId> overlappingIndexIds,
      Map<ObjectId, ExistingIndex> existingIndexesById,
      Map<ObjectId, IndexDefinitionGenerationProducer> desiredIndexesById) {

    List<ModifiedIndexInformation> modifiedIndexes = new ArrayList<>();
    List<ObjectId> unmodifiedIndexes = new ArrayList<>();

    // Check to see for each index definition if it has been modified.
    for (ObjectId indexId : overlappingIndexIds) {
      ExistingIndex existing = Objects.requireNonNull(existingIndexesById.get(indexId));
      IndexDefinitionGenerationProducer desired =
          Objects.requireNonNull(desiredIndexesById.get(indexId));

      Optional<ModifiedIndexInformation> modification = maybeModified(desired, existing);

      if (modification.isPresent()) {
        modifiedIndexes.add(modification.get());
      } else {
        unmodifiedIndexes.add(indexId);
      }
    }
    return new OverlappingIndexInfo(modifiedIndexes, unmodifiedIndexes);
  }

  /** returns an empty optional if there was no modification. */
  private Optional<ModifiedIndexInformation> maybeModified(
      IndexDefinitionGenerationProducer desired, ExistingIndex existing) {
    // we follow this table to produce the correct modification by comparing desired with both the
    // index in the catalog (the "live" one) and the staged index.
    // +-------------------+-------------------------------+---------------------------+
    // |                   | == live                       | != live                   |
    // +-------------------+-------------------------------+---------------------------+
    // | no staged present | no modification               | DifferentFromLiveNoStaged |
    // +-------------------+-------------------------------+---------------------------+
    // | == staged         | no modification               |                           |
    // +-------------------+-------------------------------+---------------------------+
    // | != staged         | SameAsLiveDifferentFromStaged | DifferentFromBoth         |
    // +-------------------+-------------------------------+---------------------------+
    // The nested boolean logic is hard to follow, but this way the compiler ensures that we have
    // covered all the possible cases.

    IndexModification currentModification = modification(existing.live, desired);

    if (existing.staged.isEmpty()) {
      return modificationWithoutStagedIndex(currentModification, desired);

    } else {
      // we have a staged index
      IndexGeneration stagedIndex = existing.staged.get();
      IndexModification stagedModification = modification(stagedIndex, desired);
      return modificationWithStagedIndex(stagedModification, currentModification, desired);
    }
  }

  private Optional<ModifiedIndexInformation> modificationWithoutStagedIndex(
      IndexModification live, IndexDefinitionGenerationProducer desired) {
    // when a staged index isn't present we only need to compare to our one live index.

    if (live.sameAsDesired()) {
      // desired index is the same as the live index, nothing for us to do
      return Optional.empty();
    } else {
      // desired index differs from our live one
      return Optional.of(
          new ModifiedIndexInformation.DifferentFromLiveNoStaged(
              desired, live.modificationReasons, live.index));
    }
  }

  private Optional<ModifiedIndexInformation> modificationWithStagedIndex(
      IndexModification staged, IndexModification live, IndexDefinitionGenerationProducer desired) {

    if (staged.sameAsDesired()) {
      // staged index equivalent to the desired definition.
      // we don't need to compare the live one.
      return Optional.empty();

    } else {
      // desired differs from staged, now we compare to the live index.

      if (live.sameAsDesired()) {
        // desired is different only from staged.
        return Optional.of(
            new ModifiedIndexInformation.SameAsLiveDifferentFromStaged(
                desired,
                live.modificationReasons,
                live.index,
                staged.modificationReasons,
                staged.index));

      } else {
        // desired differs from both
        return Optional.of(
            new ModifiedIndexInformation.DifferentFromBoth(
                desired,
                live.modificationReasons,
                live.index,
                staged.modificationReasons,
                staged.index));
      }
    }
  }

  /**
   * Whether the existing index is equivalent to the desired index definition. And a description of
   * the differences.
   */
  private IndexModification modification(
      IndexGeneration existingIndex, IndexDefinitionGenerationProducer index) {
    return switch (index.getIndexDefinition().getType()) {
      case SEARCH -> {
        Check.expectedType(IndexDefinition.Type.SEARCH, existingIndex.getDefinition().getType());
        yield modification(existingIndex, index.asSearch().getAnalyzerBoundDefinition());
      }
      case VECTOR_SEARCH -> {
        Check.expectedType(
            IndexDefinition.Type.VECTOR_SEARCH, existingIndex.getDefinition().getType());
        yield modification(existingIndex, index.asVector().getIndexDefinition());
      }
    };
  }

  /**
   * Whether the existing index is equivalent to the desired index definition. And a description of
   * the differences.
   */
  private IndexModification modification(
      IndexGeneration existingIndex, VectorIndexDefinition desired) {
    VectorIndexDefinition existing = existingIndex.getDefinition().asVectorDefinition();
    List<String> reasons = new ArrayList<>();

    if (!existing.getFields().equals(desired.getFields())) {
      reasons.add(IndexChangeReason.FIELDS.getDescription());
    }

    if (existing.getNumPartitions() != desired.getNumPartitions()) {
      reasons.add(IndexChangeReason.NUM_PARTITIONS.getDescription());
    }

    if (!Objects.equals(
        existing.getDefinitionVersion().orElse(0L), desired.getDefinitionVersion().orElse(0L))) {
      reasons.add(IndexChangeReason.DEFINITION_VERSION.getDescription());
    }

    if (!Objects.equals(
        existing.getAutoEmbeddingDefinitionVersion().orElse(0L),
        desired.getAutoEmbeddingDefinitionVersion().orElse(0L))) {
      reasons.add(IndexChangeReason.AUTO_EMBEDDING_DEFINITION_VERSION.getDescription());
    }

    if (!existing.getView().equals(desired.getView())) {
      reasons.add(IndexChangeReason.VIEW.getDescription());
    }

    return new IndexModification(existingIndex, reasons);
  }

  /**
   * Whether the existing search index and corresponding analyzers are equivalent to the desired
   * index definition. And a description of the differences.
   */
  private IndexModification modification(
      IndexGeneration existingIndex, AnalyzerBoundSearchIndexDefinition desiredBound) {
    SearchIndexDefinition existing = existingIndex.getDefinition().asSearchDefinition();
    SearchIndexDefinition desired = desiredBound.indexDefinition();
    List<String> reasons = new ArrayList<>();
    if (existing.getParsedIndexFeatureVersion() != desired.getParsedIndexFeatureVersion()) {
      reasons.add(IndexChangeReason.INDEX_FEATURE_VERSION.getDescription());
    }

    if (!existing.getAnalyzerName().equals(desired.getAnalyzerName())) {
      reasons.add(IndexChangeReason.ANALYZER.getDescription());
    }

    if (!existing.getSearchAnalyzerName().equals(desired.getSearchAnalyzerName())) {
      reasons.add(IndexChangeReason.SEARCH_ANALYZER.getDescription());
    }

    if (!existing.getMappings().equals(desired.getMappings())) {
      reasons.add(IndexChangeReason.MAPPINGS.getDescription());
    }

    if (!existing.getAnalyzerMap().equals(desired.getAnalyzerMap())) {
      reasons.add(IndexChangeReason.ANALYZERS.getDescription());
    }

    if (!existing.getSynonymMap().equals(desired.getSynonymMap())) {
      reasons.add(IndexChangeReason.SYNONYMS.getDescription());
    }

    if (!existing.getStoredSource().equals(desired.getStoredSource())) {
      reasons.add(IndexChangeReason.STORED_SOURCE.getDescription());
    }

    if (existing.getNumPartitions() != desired.getNumPartitions()) {
      reasons.add(IndexChangeReason.NUM_PARTITIONS.getDescription());
    }

    if (!Objects.equals(
        existing.getDefinitionVersion().orElse(0L), desired.getDefinitionVersion().orElse(0L))) {
      reasons.add(IndexChangeReason.DEFINITION_VERSION.getDescription());
    }

    if (!Objects.equals(
        existing.getAutoEmbeddingDefinitionVersion().orElse(0L),
        desired.getAutoEmbeddingDefinitionVersion().orElse(0L))) {
      reasons.add(IndexChangeReason.AUTO_EMBEDDING_DEFINITION_VERSION.getDescription());
    }

    if (!existing.getView().equals(desired.getView())) {
      reasons.add(IndexChangeReason.VIEW.getDescription());
    }

    Map<String, OverriddenBaseAnalyzerDefinition> modifiedAnalyzers =
        AnalyzerChangePlan.modifiedOverridenAnalyzers(
            existingIndex.getDefinitionGeneration().asSearch().definition().analyzerDefinitions(),
            desiredBound.analyzerDefinitions());

    // It is not needed to check for added or removed analyzers, it is covered by the tests above.
    if (!modifiedAnalyzers.isEmpty()) {
      reasons.add(
          String.format(
              "references modified analyzers: %s", String.join(", ", modifiedAnalyzers.keySet())));
    }

    return new IndexModification(existingIndex, reasons);
  }

  /*Represents an index in the catalog that may have a swap staged for it.*/
  private record ExistingIndex(IndexGeneration live, Optional<IndexGeneration> staged) {

    private ObjectId getIndexId() {
      return this.live.getDefinition().getIndexId();
    }
  }

  private record IndexModification(IndexGeneration index, List<String> modificationReasons) {

    private boolean sameAsDesired() {
      return this.modificationReasons.isEmpty();
    }
  }
}
