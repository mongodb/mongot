package com.xgen.mongot.config.manager;

import static com.xgen.mongot.util.Check.checkState;

import com.google.common.collect.Streams;
import com.xgen.mongot.config.manager.AllIndexInformations.IndexInformationAndGenerationStateMetrics;
import com.xgen.mongot.config.manager.metrics.IndexConfigState;
import com.xgen.mongot.config.manager.metrics.IndexGenerationStateMetrics;
import com.xgen.mongot.index.AggregatedIndexMetrics;
import com.xgen.mongot.index.IndexDetailedStatus;
import com.xgen.mongot.index.IndexGeneration;
import com.xgen.mongot.index.IndexGenerationMetrics;
import com.xgen.mongot.index.IndexInformation;
import com.xgen.mongot.index.IndexMetrics;
import com.xgen.mongot.index.InitializedIndex;
import com.xgen.mongot.index.SearchIndex;
import com.xgen.mongot.index.autoembedding.AutoEmbeddingCompositeIndex;
import com.xgen.mongot.index.autoembedding.AutoEmbeddingIndexGenerationFactory;
import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.definition.SearchIndexDefinition;
import com.xgen.mongot.index.definition.VectorIndexDefinition;
import com.xgen.mongot.index.status.IndexStatus;
import com.xgen.mongot.index.status.SynonymStatus;
import com.xgen.mongot.index.synonym.SynonymRegistry;
import com.xgen.mongot.index.version.GenerationId;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.CollectionUtils;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.bson.types.ObjectId;
import org.jetbrains.annotations.NotNull;
import org.slf4j.LoggerFactory;

class AllPresentIndexes {
  private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(AllPresentIndexes.class);

  /**
   * For all present indexes, return IndexInformation-s representing the state of each group of
   * indexes with the same indexId.
   *
   * <p>Most information is taken out of the staged index (if exists), then the live one, or any of
   * the phasing out ones in this order of precedence.
   *
   * <p>Data size and cold storage bytes are summed over all the indexes.
   */
  static AllIndexInformations allIndexInfos(ConfigState configState) {
    return new AllIndexInformations(allIndexInfosAndMetrics(configState));
  }

  private static List<IndexInformationAndGenerationStateMetrics> allIndexInfosAndMetrics(
      ConfigState configState) {
    return Streams.concat(
            configState.staged.getIndexes().stream(),
            configState.indexCatalog.getIndexes().stream(),
            configState.phasingOut.getIndexes().stream())
        .map(IndexGeneration::getDefinition)
        .map(IndexDefinition::getIndexId)
        .distinct()
        // at this point we have unique indexIds for all existing indexes
        .map(id -> AllPresentIndexes.getIndexInfoAndMetrics(configState, id))
        .collect(Collectors.toList());
  }

  private static IndexInformationAndGenerationStateMetrics getIndexInfoAndMetrics(
      ConfigState configState, ObjectId indexId) {
    Optional<@NotNull IndexGeneration> staged = configState.staged.getIndex(indexId);
    Optional<@NotNull IndexGeneration> live = configState.indexCatalog.getIndexById(indexId);
    List<@NotNull IndexGeneration> phaseOuts = configState.phasingOut.getIndexesById(indexId);
    Map<GenerationId, InitializedIndex> initializedIndexMap =
        Streams.concat(staged.stream(), live.stream(), phaseOuts.stream())
            .map(IndexGeneration::getGenerationId)
            .distinct()
            .map(
                generationId -> {
                  var initializedIndex = configState.initializedIndexCatalog.getIndex(generationId);
                  if (initializedIndex.isEmpty()) {
                    LOG.atWarn()
                        .addKeyValue("indexId", generationId.indexId)
                        .addKeyValue("generationId", generationId)
                        .log("Skipping index info for index, not yet initialized");
                  }
                  return initializedIndex;
                })
            .flatMap(Optional::stream)
            .collect(
                CollectionUtils.toMapUniqueKeys(
                    InitializedIndex::getGenerationId, Function.identity()));
    Map<IndexGeneration, IndexStatus> indexStatuses =
        Streams.concat(staged.stream(), live.stream(), phaseOuts.stream())
            .distinct()
            .collect(
                CollectionUtils.toMapUniqueKeys(
                    Function.identity(),
                    indexGeneration -> indexGeneration.getIndex().getStatus()));
    checkState(
        staged.isPresent() || live.isPresent() || !phaseOuts.isEmpty(),
        "could not find any indexes in config state even though id %s was looked up in it",
        indexId);

    IndexGroup indexes =
        new IndexGroup(staged, live, phaseOuts, initializedIndexMap, indexStatuses);

    IndexDefinition definition = indexes.getDefinition().orElseThrow();
    IndexStatus status = indexes.getStatus().orElseThrow();
    List<IndexGenerationStateMetrics> indexGenerationStateMetrics =
        indexes.getDescendingOrderOfPrecedenceIndexGenerationStateMetrics();
    List<IndexGenerationMetrics> indexGenerationMetrics =
        indexGenerationStateMetrics.stream()
            .map(IndexGenerationStateMetrics::indexGenerationMetrics)
            .collect(Collectors.toList());
    AggregatedIndexMetrics aggregatedMetrics =
        indexes.getAggregatedMetrics(indexGenerationStateMetrics);
    Optional<IndexDetailedStatus> mainIndex = indexes.getMainIndex();
    Optional<IndexDetailedStatus> stagedIndex = indexes.getStagedIndex();

    return new IndexInformationAndGenerationStateMetrics(
        switch (definition) {
          case SearchIndexDefinition searchIndexDefinition -> {
            var synonymStatuses = indexes.getSynonymStatus().orElseGet(Collections::emptyMap);
            yield new IndexInformation.Search(
                searchIndexDefinition,
                status,
                indexGenerationMetrics,
                aggregatedMetrics,
                mainIndex,
                stagedIndex,
                synonymStatuses);
          }
          case VectorIndexDefinition vectorIndexDefinition ->
              new IndexInformation.Vector(
                  vectorIndexDefinition,
                  status,
                  indexGenerationMetrics,
                  aggregatedMetrics,
                  mainIndex,
                  stagedIndex);
        },
        indexGenerationStateMetrics);
  }

  private record IndexGroup(
      Optional<IndexGeneration> staged,
      Optional<IndexGeneration> live,
      List<IndexGeneration> phaseOuts,
      Map<GenerationId, InitializedIndex> initializedIndexes,
      Map<IndexGeneration, IndexStatus> indexStatuses) {

    private Optional<IndexDefinition> getDefinition() {
      return descendingOrderOfPrecedence().map(IndexGeneration::getDefinition).findFirst();
    }

    private Optional<IndexStatus> getStatus() {
      if (this.staged.isPresent()) {
        IndexStatus stagedStatus = this.indexStatuses.get(this.staged.get());
        // it is possible that a staged index has reached steady state, and we just didn't get to
        // swap it in yet. If this is the case, report that it "building", as it is not yet
        // queryable.
        if (stagedStatus.getStatusCode() == IndexStatus.StatusCode.STEADY) {
          return Optional.of(IndexStatus.initialSync());
        }
      }

      return descendingOrderOfPrecedence().map(this.indexStatuses::get).findFirst();
    }

    private Optional<Map<String, SynonymStatus>> getSynonymStatus() {
      return descendingOrderOfPrecedence()
          .map(IndexGeneration::getIndex)
          .map(
              index -> {
                if (index instanceof SearchIndex si) {
                  return Optional.of(si.getSynonymRegistry());
                }
                if (index instanceof AutoEmbeddingCompositeIndex composite
                    && composite.derivedIndex instanceof SearchIndex si) {
                  return Optional.of(si.getSynonymRegistry());
                }
                return Optional.<SynonymRegistry>empty();
              })
          .filter(Optional::isPresent)
          .map(Optional::get)
          .findFirst()
          .map(SynonymRegistry::getStatuses);
    }

    private List<IndexGenerationStateMetrics>
        getDescendingOrderOfPrecedenceIndexGenerationStateMetrics() {
      return descendingOrderOfPrecedenceConfigStates()
          .map(
              indexGenerationState ->
                  Optional.ofNullable(
                          this.initializedIndexes.get(
                              indexGenerationState.indexGeneration().getGenerationId()))
                      .map(
                          initializedIndex ->
                              new IndexGenerationMetrics(
                                  initializedIndex.getGenerationId(),
                                  initializedIndex.getMetrics()))
                      .map(
                          metrics ->
                              new IndexGenerationStateMetrics(
                                  metrics, indexGenerationState.configState())))
          .filter(Optional::isPresent)
          .map(Optional::get)
          .toList();
    }

    private AggregatedIndexMetrics getAggregatedMetrics(
        List<IndexGenerationStateMetrics> descendingOrderOfPrecedenceIndexGenerationStateMetrics) {
      List<IndexMetrics> metricsByDescendingPrecedence =
          descendingOrderOfPrecedenceIndexGenerationStateMetrics.stream()
              .map(IndexGenerationStateMetrics::indexGenerationMetrics)
              .map(IndexGenerationMetrics::indexMetrics)
              .collect(Collectors.toList());
      return AggregatedIndexMetrics.createFromOrderedMetrics(metricsByDescendingPrecedence);
    }

    private Stream<IndexGenerationState> descendingOrderOfPrecedenceConfigStates() {
      return Streams.concat(
          this.staged.stream().map(IndexGenerationState::staged),
          this.live.stream().map(IndexGenerationState::live),
          this.phaseOuts.stream().map(IndexGenerationState::phasingOut));
    }

    private Stream<IndexGeneration> descendingOrderOfPrecedence() {
      return descendingOrderOfPrecedenceConfigStates().map(IndexGenerationState::indexGeneration);
    }

    private Optional<IndexDetailedStatus> getStagedIndex() {
      return this.staged.map(this::getIndexDetailedStatus);
    }

    private Optional<IndexDetailedStatus> getMainIndex() {
      return this.live.map(this::getIndexDetailedStatus);
    }

    @SuppressWarnings("resource")
    private IndexDetailedStatus getIndexDetailedStatus(IndexGeneration indexGeneration) {
      Optional<AggregatedIndexMetrics> indexMetrics =
          Optional.ofNullable(this.initializedIndexes.get(indexGeneration.getGenerationId()))
              .map(InitializedIndex::getMetrics)
              .map(metrics -> AggregatedIndexMetrics.createFromOrderedMetrics(List.of(metrics)));

      // An unresolved auto-embedding generation reports the raw definition's type (SEARCH or
      // VECTOR), but its index is a stub with neither a derived search index nor a synonym
      // registry, so the detailed status is built from the definition alone.
      if (AutoEmbeddingIndexGenerationFactory.isAutoEmbeddingResolutionFailed(indexGeneration)) {
        return switch (indexGeneration.getDefinition()) {
          case SearchIndexDefinition searchDef ->
              new IndexDetailedStatus.Search(
                  Map.of(),
                  searchDef,
                  indexGeneration.getOverriddenAnalyzers(),
                  this.indexStatuses.get(indexGeneration),
                  indexGeneration.getGenerationId(),
                  indexMetrics);
          case VectorIndexDefinition vectorDef ->
              new IndexDetailedStatus.Vector(
                  vectorDef,
                  this.indexStatuses.get(indexGeneration),
                  indexGeneration.getGenerationId(),
                  indexMetrics);
        };
      }

      return switch (indexGeneration.getType()) {
        case SEARCH ->
            new IndexDetailedStatus.Search(
                indexGeneration
                    .getIndex()
                    .asSearchIndex()
                    .getSynonymRegistry()
                    .getDetailedStatuses(),
                indexGeneration.getDefinition().asSearchDefinition(),
                indexGeneration.getOverriddenAnalyzers(),
                this.indexStatuses.get(indexGeneration),
                indexGeneration.getGenerationId(),
                indexMetrics);
        case VECTOR ->
            new IndexDetailedStatus.Vector(
                indexGeneration.getIndex().getDefinition().asVectorDefinition(),
                this.indexStatuses.get(indexGeneration),
                indexGeneration.getGenerationId(),
                indexMetrics);
        case AUTO_EMBEDDING -> {
          IndexDefinition def = indexGeneration.getDefinition();
          yield switch (def) {
            case SearchIndexDefinition searchDef -> {
              AutoEmbeddingCompositeIndex composite =
                  Check.instanceOf(indexGeneration.getIndex(), AutoEmbeddingCompositeIndex.class);
              yield new IndexDetailedStatus.Search(
                  composite.derivedIndex.asSearchIndex().getSynonymRegistry().getDetailedStatuses(),
                  searchDef,
                  indexGeneration.getOverriddenAnalyzers(),
                  this.indexStatuses.get(indexGeneration),
                  indexGeneration.getGenerationId(),
                  indexMetrics);
            }
            case VectorIndexDefinition vectorDef ->
                new IndexDetailedStatus.Vector(
                    vectorDef,
                    this.indexStatuses.get(indexGeneration),
                    indexGeneration.getGenerationId(),
                    indexMetrics);
          };
        }
      };
    }

    private record IndexGenerationState(
        IndexGeneration indexGeneration, IndexConfigState configState) {
      private static IndexGenerationState staged(IndexGeneration indexGeneration) {
        return new IndexGenerationState(indexGeneration, IndexConfigState.STAGED);
      }

      private static IndexGenerationState live(IndexGeneration indexGeneration) {
        return new IndexGenerationState(indexGeneration, IndexConfigState.LIVE);
      }

      private static IndexGenerationState phasingOut(IndexGeneration indexGeneration) {
        return new IndexGenerationState(indexGeneration, IndexConfigState.PHASING_OUT);
      }
    }
  }
}
