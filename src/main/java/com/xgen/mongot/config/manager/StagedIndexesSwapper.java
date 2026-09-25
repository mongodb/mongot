package com.xgen.mongot.config.manager;

import com.xgen.mongot.config.backup.ConfigJournalV1;
import com.xgen.mongot.config.backup.JournalEditor;
import com.xgen.mongot.config.util.IndexDefinitions;
import com.xgen.mongot.config.util.Invariants;
import com.xgen.mongot.featureflag.Feature;
import com.xgen.mongot.featureflag.FeatureFlags;
import com.xgen.mongot.index.IndexGeneration;
import com.xgen.mongot.index.autoembedding.AutoEmbeddingIndexGenerationFactory;
import com.xgen.mongot.index.status.IndexStatus;
import com.xgen.mongot.index.version.GenerationId;
import com.xgen.mongot.metrics.MetricsFactory;
import com.xgen.mongot.util.Check;
import io.micrometer.core.instrument.Tags;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class StagedIndexesSwapper {
  private static final Logger LOG = LoggerFactory.getLogger(StagedIndexesSwapper.class);

  static void swapReady(
      ConfigState configState, FeatureFlags featureFlags, MetricsFactory metricsFactory)
      throws IOException, Invariants.InvariantException {
    List<IndexGeneration> swapIn =
        configState.staged.getIndexes().stream()
            .filter(
                staged -> {
                  // select serviceable staged indexes
                  if (staged.getIndex().getStatus().canServiceQueries()) {
                    return true;
                  }

                  Optional<IndexGeneration> mainIndexOpt =
                      configState.indexCatalog.getIndexById(staged.getDefinition().getIndexId());

                  if (mainIndexOpt.isEmpty()) {
                    // This indicates that there is no corresponding main index, which theoretically
                    // should not occur. If it does, the current behavior is to swap in these staged
                    // indexes.
                    LOG.atError()
                        .addKeyValue("indexId", staged.getGenerationId().indexId)
                        .addKeyValue("generationId", staged.getGenerationId())
                        .log("Found staged index when main index is missing during swap.");
                    return true;
                  }

                  IndexGeneration mainIndex = mainIndexOpt.get();
                  IndexStatus mainIndexStatus = mainIndex.getIndex().getStatus();

                  // A recovered auto-embedding attempt can determine that this shard does not
                  // host the source collection. Replace its stale transient-failure predecessor
                  // so the index reports the quiet DOES_NOT_EXIST state instead of remaining
                  // permanently FAILED.
                  if (staged.getIndex().getStatus().isCollectionNotFound()
                      && AutoEmbeddingIndexGenerationFactory.isAutoEmbeddingResolutionFailed(
                          mainIndex)) {
                    LOG.atInfo()
                        .addKeyValue("indexId", staged.getGenerationId().indexId)
                        .addKeyValue("generationId", staged.getGenerationId())
                        .log(
                            "Converging unresolved auto-embedding index to DOES_NOT_EXIST after "
                                + "recovered collection-not-found resolution.");
                    return true;
                  }

                  // Do the swap when both the main index and staged index are in the
                  // DOES_NOT_EXIST state due to the collection is not found on the mongod. This
                  // might occur due to an Index Definition or Format Update, or other
                  // circumstances where the staged index fails to resolve the collection during
                  // the initial sync attempt. Prevent other types of swaps to ensure that the
                  // main index is not replaced with unready staged indexes.
                  if (featureFlags.isEnabled(
                      Feature.SHUT_DOWN_REPLICATION_WHEN_COLLECTION_NOT_FOUND)) {
                    if (mainIndexStatus.isCollectionNotFound()) {
                      return staged.getIndex().getStatus().isCollectionNotFound();
                    }
                  }

                  // or non-ready staged indexes which have a corresponding live index in any
                  // non-queryable status (case when live index has failed, got corrupted or in
                  // INITIAL_SYNC and we need to prematurely swap it with the staged one to avoid
                  // wasteful simultaneous syncs for both generations).
                  // However, do not swap prematurely if the index retains recovery potential,
                  // such as failure during initialization.
                  // Allow premature swap only if the main index
                  // (1) cannot serve queries
                  // (2) cannot be recovered (due to initialization error)
                  // (3) is not transient (UNKNOWN) - the corresponding index may transition to a
                  //     queryable status after resolving commit token.
                  // (4) is not DOES_NOT_EXIST due to collection not found - this check is added
                  // to prevent premature swap when feature flag
                  // shutDownReplicationWhenCollectionNotFound is disabled
                  return !(mainIndexStatus.canServiceQueries()
                      || mainIndexStatus.canBeRecovered()
                      || mainIndexStatus.getStatusCode().equals(IndexStatus.StatusCode.UNKNOWN)
                      || mainIndexStatus.isCollectionNotFound());
                })
            .collect(Collectors.toList());

    if (swapIn.isEmpty()) {
      return;
    }

    // Live indexes from the catalog we intend to swap out
    List<IndexGeneration> swapOut = correspondingIndexesInCatalog(configState, swapIn);

    swap(configState, swapIn, swapOut);
    updateCounters(swapIn, swapOut, metricsFactory);
  }

  static void swap(
      ConfigState configState, List<IndexGeneration> swapIn, List<IndexGeneration> swapOut)
      throws IOException, Invariants.InvariantException {
    List<GenerationId> swapOutIds = IndexDefinitions.indexesGenerationIds(swapOut);
    List<GenerationId> swapInIds = IndexDefinitions.indexesGenerationIds(swapIn);
    logIds(swapInIds, swapOutIds);

    // write our intent to swap in the new indexes and phase out the old ones.
    ConfigJournalV1 writeAhead =
        JournalEditor.on(configState.currentJournal())
            .fromLiveToDropped(swapOutIds)
            .fromStagedToLive(swapInIds)
            .journal();
    configState.persist(writeAhead);

    // we shift indexes phasingOut<-catalog<-staged

    // add the old ones to be phased out
    swapOut.forEach(configState.phasingOut::addIndex);

    // move the new ones into the catalog
    // we wish to add the new ones atomically to the index catalog because it may be queried at any
    // point. So instead of removing the old ones we simply over-write them with the new ones
    // (as they have the same namespace and indexId).
    swapIn.forEach(configState.indexCatalog::addIndex);

    // remove the new ones from staged
    swapIn.forEach(configState.staged::removeIndex);

    // configState is now consistent with the journal written above.
  }

  private static List<IndexGeneration> correspondingIndexesInCatalog(
      ConfigState configState, List<IndexGeneration> swapIn) {
    // look up all the index ids in the index catalog
    List<ObjectId> indexIds = IndexDefinitions.indexIds(swapIn);

    // shouldn't happen. Staged indexes are unique by indexId.
    Check.elementAttributesAreUnique(indexIds, id -> id, "indexId", "swapIn");

    return indexIds.stream()
        .map(configState.indexCatalog::getIndexById)
        /* Ignores ready staged indexes that have no corresponding index in the catalog.*/
        .flatMap(Optional::stream)
        .collect(Collectors.toList());
  }

  private static void logIds(List<GenerationId> swapIn, List<GenerationId> swapOut) {
    swapIn.forEach(
        e ->
            LOG.atInfo()
                .addKeyValue("indexId", e.indexId)
                .addKeyValue("generationId", e.generation)
                .log("Going to swap in index"));
    swapOut.forEach(
        e ->
            LOG.atInfo()
                .addKeyValue("indexId", e.indexId)
                .addKeyValue("generationId", e.generation)
                .log("Going to phase out index"));
  }

  private static void updateCounters(
      List<IndexGeneration> swapIn, List<IndexGeneration> swapOut, MetricsFactory metricsFactory) {
    for (IndexGeneration swapInGeneration : swapIn) {
      IndexStatus status = swapInGeneration.getIndex().getStatus();
      Tags statusTags = Tags.of("indexStatus", status.getStatusCode().name());
      metricsFactory.counter("swappedInIndexes", statusTags).increment();
    }

    for (IndexGeneration swapOutGeneration : swapOut) {
      IndexStatus status = swapOutGeneration.getIndex().getStatus();
      Tags statusTags = Tags.of("indexStatus", status.getStatusCode().name());
      metricsFactory.counter("swappedOutIndexes", statusTags).increment();
    }
  }
}
