package com.xgen.mongot.replication.mongodb.common;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.mongodb.MongoNamespace;
import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.version.GenerationId;
import com.xgen.mongot.metrics.MetricsFactory;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.VerboseRunnable;
import com.xgen.mongot.util.concurrent.Executors;
import com.xgen.mongot.util.concurrent.NamedScheduledExecutorService;
import com.xgen.mongot.util.mongodb.CollectionStatsResponse;
import io.micrometer.core.instrument.Counter;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.bson.RawBsonDocument;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Heuristic-based change stream mode selector. Asynchronously selects a mode for the supplied
 * generations or returns {@link ChangeStreamMode#getDefault()} while the selection is not available
 * yet. Ensures that mode is selected only once per generation.
 */
public class HeuristicChangeStreamModeSelector implements ChangeStreamModeSelector {

  private static final int DEFAULT_REFRESH_PERIOD_MIN = 1;
  private static final Logger LOG =
      LoggerFactory.getLogger(HeuristicChangeStreamModeSelector.class);

  private final Map<GenerationId, ModeSelectionContext> registry;
  private final NamedScheduledExecutorService scheduler;
  private final Counter failedSamplingAttemptsCounter;
  private final Optional<ChangeStreamMode> override;

  private volatile boolean shutdown;

  public HeuristicChangeStreamModeSelector(
      CollectionStatsMongoClient statsClient,
      CollectionSamplingMongoClient samplingClient,
      NamedScheduledExecutorService scheduler,
      Optional<ChangeStreamMode> override,
      MetricsFactory metricsFactory) {
    this(
        statsClient,
        samplingClient,
        scheduler,
        override,
        metricsFactory,
        DEFAULT_REFRESH_PERIOD_MIN,
        TimeUnit.MINUTES);
  }

  @VisibleForTesting
  HeuristicChangeStreamModeSelector(
      CollectionStatsMongoClient statsClient,
      CollectionSamplingMongoClient samplingClient,
      NamedScheduledExecutorService scheduler,
      Optional<ChangeStreamMode> override,
      MetricsFactory metricsFactory,
      int refreshPeriod,
      TimeUnit unit) {
    this.shutdown = false;
    this.registry = new ConcurrentHashMap<>();
    this.scheduler = scheduler;
    this.override = override;
    this.failedSamplingAttemptsCounter = metricsFactory.counter("failedSamplingAttemptsCounter");
    this.scheduler.scheduleWithFixedDelay(
        new Refresher(statsClient, samplingClient), 0, refreshPeriod, unit);
  }

  @Override
  public ChangeStreamMode getMode(GenerationId generationId) {
    return Optional.ofNullable(this.registry.get(generationId))
        .flatMap(ModeSelectionContext::getMode)
        .orElse(ChangeStreamMode.getDefault());
  }

  @Override
  public void register(
      GenerationId generationId, IndexDefinition definition, MongoNamespace namespace) {
    var previousValue =
        this.registry.putIfAbsent(
            generationId,
            new ModeSelectionContext(definition, namespace, generationId, this.override));
    Check.isNull(previousValue, "previousValue");
  }

  @Override
  public void remove(GenerationId generationId) {
    var previousValue = this.registry.remove(generationId);
    Check.isNotNull(previousValue, "previousValue");
  }

  @Override
  public void shutdown() {
    LOG.info("Shutting down.");
    this.shutdown = true;
    Executors.shutdownOrFail(this.scheduler);
  }

  private class Refresher implements VerboseRunnable {

    /**
     * The threshold which is used to increase accuracy of the heuristic approach. We keep
     * monitoring the collection size until it exceeds this number, and only after that execute the
     * sampling query for heuristic analysis.
     */
    private static final int COLLECTION_SIZE_THRESHOLD = 10_000;

    /** A maximum number of documents to sample for average size after $project. */
    private static final int COLLECTION_SAMPLING_LIMIT = 100;

    /**
     * Projection has an overhead in case of use with change streams, so it can be enabled only when
     * performance benefit outweighs this overhead. In general, it is a function of the ratio
     * between a full document size and size of the projected data, MongoDB query performance,
     * network latency, decryption latency etc. Assuming that everything except the ratio is more or
     * less stable on the environment, we determine the {@link ChangeStreamMode} solely based on the
     * heuristic ratio: https://tinyurl.com/3anxzunt. This number is a subject for future
     * corrections.
     */
    private static final int HEURISTIC_RATIO_BETWEEN_FULL_AND_PROJECTED_PAYLOAD = 8;

    private final CollectionStatsMongoClient statsClient;
    private final CollectionSamplingMongoClient samplingClient;

    Refresher(
        CollectionStatsMongoClient statsClient, CollectionSamplingMongoClient samplingClient) {
      this.statsClient = statsClient;
      this.samplingClient = samplingClient;
    }

    /**
     * Periodically triggered to select a mode for the registered generations which do not have a
     * defined mode yet.
     */
    @Override
    public void verboseRun() {
      HeuristicChangeStreamModeSelector.this.registry.entrySet().stream()
          .filter(entry -> entry.getValue().getMode().isEmpty())
          .forEach(
              entry -> {
                GenerationId generationId = entry.getKey();
                if (HeuristicChangeStreamModeSelector.this.shutdown) {
                  // Since it may take some time to select a mode per index (especially if a
                  // connection to mongod is not available), we exit early to make shutdown more
                  // responsive.
                  LOG.atInfo()
                      .addKeyValue("indexId", generationId.indexId)
                      .addKeyValue("generationId", generationId)
                      .log("Received a shutdown signal, skipping mode selection for index");
                  return;
                }

                try {
                  var mode = selectMode(generationId, entry.getValue());
                  mode.ifPresent(m -> entry.getValue().setMode(m));
                } catch (RuntimeException e) {
                  HeuristicChangeStreamModeSelector.this.failedSamplingAttemptsCounter.increment();
                  LOG.atError()
                      .addKeyValue("indexId", generationId.indexId)
                      .addKeyValue("generationId", generationId)
                      .setCause(e)
                      .log("Unable to select a mode, skipping");
                }
              });
    }

    Optional<ChangeStreamMode> selectMode(GenerationId generationId, ModeSelectionContext context) {
      var projection = Projection.forRegularQuery(context.definition);

      if (projection.isEmpty()) {
        LOG.atInfo()
            .addKeyValue("indexId", generationId.indexId)
            .addKeyValue("generationId", generationId)
            .addKeyValue("mode", ChangeStreamMode.ALL_FIELDS)
            .log("Change stream mode is selected due to empty projection");
        return Optional.of(ChangeStreamMode.ALL_FIELDS);
      }

      // A $match stage before $sample forces a full collection scan because MongoDB cannot use
      // the fast pseudo-random sampling path. Skip sampling and fall back to ALL_FIELDS.
      if (context
          .definition
          .getView()
          .filter(view -> view.exists() && ViewPipeline.hasMatchStage(view))
          .isPresent()) {
        LOG.atInfo()
            .addKeyValue("indexId", generationId.indexId)
            .addKeyValue("generationId", generationId)
            .addKeyValue("mode", ChangeStreamMode.ALL_FIELDS)
            .log("Change stream mode is selected, sampling skipped for view with $match");
        return Optional.of(ChangeStreamMode.ALL_FIELDS);
      }

      var stats = this.statsClient.getStats(context.namespace);

      if (!isCollectionReadyForSampling(stats)) {
        return Optional.empty();
      }

      // view-defined stages might filter and transform the documents in the collection,
      // so we apply them to estimate the document size more accurately
      Optional<ImmutableList<Bson>> viewDefinedStages =
          context.definition.getView().map(ViewPipeline::forRegularQuery);

      var samplingBatch =
          this.samplingClient.getSamples(
              context.namespace, COLLECTION_SAMPLING_LIMIT, viewDefinedStages, projection);

      var avgObjSizeAfterProject = getAvgBsonSize(samplingBatch);

      var mode =
          isProjectionRequired(stats.getAvgObjSize(), avgObjSizeAfterProject)
              ? ChangeStreamMode.INDEXED_FIELDS
              : ChangeStreamMode.ALL_FIELDS;

      LOG.atInfo()
          .addKeyValue("indexId", generationId.indexId)
          .addKeyValue("generationId", generationId)
          .addKeyValue("mode", mode)
          .addKeyValue("documentCount", stats.getCount())
          .addKeyValue("avgSize", stats.getAvgObjSize())
          .addKeyValue("avgSizeAfterProject", avgObjSizeAfterProject)
          .log("Change stream mode is selected");

      return Optional.of(mode);
    }

    /** Returns true if the collection is large enough to run sampling. */
    boolean isCollectionReadyForSampling(CollectionStatsResponse stats) {
      return stats.getCount() >= COLLECTION_SIZE_THRESHOLD;
    }

    /** Returns true if the projection is required according to the heuristic. */
    boolean isProjectionRequired(double avgObjSize, double avgSizeAfterProject) {
      return avgSizeAfterProject > 0
          && avgObjSize / avgSizeAfterProject > HEURISTIC_RATIO_BETWEEN_FULL_AND_PROJECTED_PAYLOAD;
    }

    /** Returns avg size of the provided BSON documents or -1 if the list is empty. */
    double getAvgBsonSize(List<RawBsonDocument> documents) {
      return documents.stream()
          .mapToInt(document -> document.getByteBuffer().remaining())
          .average()
          .orElse(-1);
    }

    @Override
    public Logger getLogger() {
      return LOG;
    }
  }

  private static class ModeSelectionContext {

    private final IndexDefinition definition;
    private final MongoNamespace namespace;
    private final AtomicReference<ChangeStreamMode> mode;

    ModeSelectionContext(
        IndexDefinition definition,
        MongoNamespace namespace,
        GenerationId generationId,
        Optional<ChangeStreamMode> override) {
      this.definition = definition;
      this.namespace = namespace;
      this.mode = new AtomicReference<>();

      /*
       * Note that the override is set here in the constructor rather in #selectMode,
       * because we want to apply it immediately after the index is registered,
       * not asynchronously when the scheduled process picks it up.
       */
      if (override.isPresent()) {
        try {
          // apply override if projection is applicable for the definition
          if (Projection.forRegularQuery(this.definition).isPresent()) {
            this.mode.set(override.get());
            LOG.atInfo()
                .addKeyValue("indexId", generationId.indexId)
                .addKeyValue("generationId", generationId)
                .addKeyValue("mode", override.get())
                .log("Change stream mode is set due to an override");
          }
        } catch (RuntimeException e) {
          LOG.error("Failed to calculate projection", e);
        }
      }
    }

    public Optional<ChangeStreamMode> getMode() {
      return Optional.ofNullable(this.mode.get());
    }

    public void setMode(ChangeStreamMode mode) {
      this.mode.set(mode);
    }
  }
}
