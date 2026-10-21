package com.xgen.mongot.replication.mongodb.common;

import com.google.common.annotations.VisibleForTesting;
import com.mongodb.MongoNamespace;
import com.xgen.mongot.catalog.IndexCatalog;
import com.xgen.mongot.catalog.InitializedIndexCatalog;
import com.xgen.mongot.featureflag.Feature;
import com.xgen.mongot.featureflag.FeatureFlags;
import com.xgen.mongot.index.IndexMetricsUpdater.IndexingMetricsUpdater.SourceCollectionDocCounts;
import com.xgen.mongot.index.InitializedIndex;
import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.version.GenerationId;
import com.xgen.mongot.util.VerboseRunnable;
import com.xgen.mongot.util.concurrent.Executors;
import com.xgen.mongot.util.concurrent.NamedScheduledExecutorService;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Periodically sets each index's {@code sourceCollectionDocCount} gauge to its source collection's
 * document count, and its {@code sourceCollectionCheckIndexedDocCount} gauge to the index's
 * source-document count read right after. Comparing the pair finds indexes that are missing
 * documents.
 *
 * <p>The collection count is the fast count from {@code collStats}: it reads metadata, but can
 * drift after an unclean mongod shutdown. On a sharded cluster it is the count on this mongot's
 * shard, including orphans. Both gauges are cleared for indexes whose document count is not
 * comparable with their collection's (see {@link #isCountComparable}), and when a read fails.
 */
public class SourceCollectionCountUpdater implements AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(SourceCollectionCountUpdater.class);

  public static final Duration DEFAULT_UPDATE_INTERVAL = Duration.ofHours(1);

  /** Delay before the first update, so that indexes have time to initialize after startup. */
  private static final Duration INITIAL_DELAY = Duration.ofMinutes(5);

  @VisibleForTesting
  static final String ERROR_COUNTER_METRIC_NAME = "sourceCollectionCountUpdaterError";

  private final IndexCatalog indexCatalog;
  private final InitializedIndexCatalog initializedIndexCatalog;
  private final CollectionStatsMongoClient statsClient;
  private final NamedScheduledExecutorService executor;
  private final Counter errorCounter;

  @VisibleForTesting
  SourceCollectionCountUpdater(
      IndexCatalog indexCatalog,
      InitializedIndexCatalog initializedIndexCatalog,
      CollectionStatsMongoClient statsClient,
      NamedScheduledExecutorService executor,
      MeterRegistry meterRegistry) {
    this.indexCatalog = indexCatalog;
    this.initializedIndexCatalog = initializedIndexCatalog;
    this.statsClient = statsClient;
    this.executor = executor;
    this.errorCounter = meterRegistry.counter(ERROR_COUNTER_METRIC_NAME);
  }

  /**
   * Creates an updater that runs every {@code interval}, starting a few minutes from now, unless
   * {@link Feature#FORCE_DISABLE_SOURCE_COLLECTION_COUNT_UPDATER} is enabled.
   */
  public static SourceCollectionCountUpdater create(
      IndexCatalog indexCatalog,
      InitializedIndexCatalog initializedIndexCatalog,
      CollectionStatsMongoClient statsClient,
      Duration interval,
      FeatureFlags featureFlags,
      MeterRegistry meterRegistry) {
    return create(
        indexCatalog,
        initializedIndexCatalog,
        statsClient,
        Executors.singleThreadScheduledExecutor("sourceCollectionCountUpdater", meterRegistry),
        interval,
        featureFlags.isEnabled(Feature.FORCE_DISABLE_SOURCE_COLLECTION_COUNT_UPDATER),
        meterRegistry);
  }

  @VisibleForTesting
  static SourceCollectionCountUpdater create(
      IndexCatalog indexCatalog,
      InitializedIndexCatalog initializedIndexCatalog,
      CollectionStatsMongoClient statsClient,
      NamedScheduledExecutorService executor,
      Duration interval,
      boolean disabled,
      MeterRegistry meterRegistry) {
    var updater =
        new SourceCollectionCountUpdater(
            indexCatalog, initializedIndexCatalog, statsClient, executor, meterRegistry);
    if (disabled) {
      LOG.info(
          "Not starting SourceCollectionCountUpdater because "
              + "forceDisableSourceCollectionCountUpdater is enabled.");
      return updater;
    }
    executor.scheduleWithFixedDelay(
        new VerboseRunnable() {
          @Override
          public void verboseRun() {
            updater.update();
          }

          @Override
          public Logger getLogger() {
            return LOG;
          }
        },
        INITIAL_DELAY.toMillis(),
        interval.toMillis(),
        TimeUnit.MILLISECONDS);
    return updater;
  }

  /**
   * Updates every initialized index. Errors are counted and logged rather than thrown, because a
   * thrown exception would cancel the schedule.
   */
  @VisibleForTesting
  void update() {
    // Generations of the same index share a collection, so fetch each namespace's count once.
    Map<MongoNamespace, Optional<Long>> counts = new HashMap<>();
    try {
      this.indexCatalog.getIndexes().stream()
          .map(generation -> this.initializedIndexCatalog.getIndex(generation.getGenerationId()))
          .flatMap(Optional::stream)
          .filter(index -> !index.isClosed())
          .forEach(index -> updateIndex(index, counts));
    } catch (RuntimeException e) {
      this.errorCounter.increment();
      LOG.atWarn().setCause(e).log("Failed to update source collection document counts");
    }
  }

  private void updateIndex(InitializedIndex index, Map<MongoNamespace, Optional<Long>> counts) {
    try {
      IndexDefinition definition = index.getDefinition();
      Optional<Long> count =
          isCountComparable(definition)
              ? counts.computeIfAbsent(
                  definition.getLastObservedNamespace(),
                  namespace -> fetchCount(namespace, index.getGenerationId()))
              : Optional.empty();
      // Read the index's count right after the collection's, so that the two gauges are a pair.
      Optional<SourceCollectionDocCounts> pair =
          count.map(
              collectionDocs ->
                  new SourceCollectionDocCounts(
                      collectionDocs, index.getMetricsUpdater().getDocCounts().numMongoDbDocs));
      index.getMetricsUpdater().getIndexingMetricsUpdater().setSourceCollectionDocCounts(pair);
    } catch (RuntimeException e) {
      if (index.isClosed()) {
        // The index was closed after it was filtered; its metrics are already deregistered.
        return;
      }
      clearCounts(index);
      this.errorCounter.increment();
      LOG.atWarn()
          .addKeyValue("indexId", index.getGenerationId().indexId)
          .addKeyValue("generationId", index.getGenerationId())
          .setCause(e)
          .log("Failed to update source collection document count");
    }
  }

  private static void clearCounts(InitializedIndex index) {
    try {
      index
          .getMetricsUpdater()
          .getIndexingMetricsUpdater()
          .setSourceCollectionDocCounts(Optional.empty());
    } catch (RuntimeException ignored) {
      // Keep reporting the original failure.
    }
  }

  private Optional<Long> fetchCount(MongoNamespace namespace, GenerationId generationId) {
    try {
      return Optional.of(this.statsClient.getStats(namespace).getCount());
    } catch (RuntimeException e) {
      this.errorCounter.increment();
      LOG.atWarn()
          .addKeyValue("indexId", generationId.indexId)
          .addKeyValue("generationId", generationId)
          .addKeyValue("namespace", namespace)
          .setCause(e)
          .log("Failed to fetch source collection document count");
      return Optional.empty();
    }
  }

  /**
   * Returns false when the index is not expected to hold one document per collection document:
   * auto-embedding indexes, views whose {@code $match} filters out collection documents, and views
   * that no longer exist.
   */
  @VisibleForTesting
  static boolean isCountComparable(IndexDefinition definition) {
    return !definition.isAutoEmbeddingIndex()
        && definition
            .getView()
            .filter(view -> !view.exists() || ViewPipeline.hasMatchStage(view))
            .isEmpty();
  }

  @Override
  public void close() {
    LOG.info("Shutting down.");
    Executors.shutdownOrFail(this.executor);
  }
}
