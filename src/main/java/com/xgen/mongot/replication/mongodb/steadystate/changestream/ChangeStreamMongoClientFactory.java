package com.xgen.mongot.replication.mongodb.steadystate.changestream;


import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.version.GenerationId;
import com.xgen.mongot.metrics.MeterAndFtdcRegistry;
import com.xgen.mongot.metrics.MetricsFactory;
import com.xgen.mongot.replication.mongodb.common.ChangeStreamAggregateOperationBuilder.AggregateOperationTemplate;
import com.xgen.mongot.replication.mongodb.common.ChangeStreamAggregateOperationFactory;
import com.xgen.mongot.replication.mongodb.common.ChangeStreamModeSelector;
import com.xgen.mongot.replication.mongodb.common.ChangeStreamModeSelector.ChangeStreamMode;
import com.xgen.mongot.replication.mongodb.common.ChangeStreamMongoClient;
import com.xgen.mongot.replication.mongodb.common.ChangeStreamMongoCursorClient;
import com.xgen.mongot.replication.mongodb.common.ChangeStreamResumeInfo;
import com.xgen.mongot.replication.mongodb.common.SessionRefresher;
import com.xgen.mongot.replication.mongodb.common.SplitEventChangeStreamClient;
import com.xgen.mongot.replication.mongodb.common.SteadyStateException;
import com.xgen.mongot.replication.mongodb.common.TimeableChangeStreamMongoClient;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.DurationUtils;
import com.xgen.mongot.util.mongodb.BatchMongoClient;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Optional;

class ChangeStreamMongoClientFactory {
  private final BatchMongoClient mongoClient;
  private final SessionRefresher sessionRefresher;
  private final ChangeStreamModeSelector modeSelector;
  private final Duration changeStreamCursorMaxTime;
  private final int changeStreamQueryMaxTimeMs;
  private final Optional<Integer> embeddingGetMoreBatchSize;
  private final List<String> excludedChangestreamFields;
  private final boolean matchCollectionUuidForUpdateLookup;
  private final MeterRegistry ftdcRegistry;
  private final MetricsFactory meterMetricsFactory;
  private final boolean enableSplitLargeChangeStreamEvents;

  private ChangeStreamMongoClientFactory(
      BatchMongoClient mongoClient,
      SessionRefresher sessionRefresher,
      ChangeStreamModeSelector modeSelector,
      MeterAndFtdcRegistry meterAndFtdcRegistry,
      SteadyStateReplicationConfig replicationConfig) {
    this.mongoClient = mongoClient;
    this.sessionRefresher = sessionRefresher;
    this.modeSelector = modeSelector;
    this.changeStreamQueryMaxTimeMs = replicationConfig.getChangeStreamQueryMaxTimeMs();
    this.changeStreamCursorMaxTime =
        Duration.of(replicationConfig.getChangeStreamCursorMaxTimeSec(), ChronoUnit.SECONDS);
    this.embeddingGetMoreBatchSize = replicationConfig.getEmbeddingGetMoreBatchSize();
    this.excludedChangestreamFields = replicationConfig.getExcludedChangestreamFields();
    this.matchCollectionUuidForUpdateLookup =
        replicationConfig.getMatchCollectionUuidForUpdateLookup();
    this.ftdcRegistry = meterAndFtdcRegistry.ftdcRegistry();
    this.meterMetricsFactory =
        new MetricsFactory("changestream", meterAndFtdcRegistry.meterRegistry());
    this.enableSplitLargeChangeStreamEvents =
        replicationConfig.getEnableSplitLargeChangeStreamEvents();
  }

  static ChangeStreamMongoClientFactory create(
      SessionRefresher sessionRefresher,
      BatchMongoClient mongoClient,
      ChangeStreamModeSelector modeSelector,
      MeterAndFtdcRegistry meterAndFtdcRegistry,
      SteadyStateReplicationConfig replicationConfig) {
    Check.argIsPositive(
        replicationConfig.getChangeStreamQueryMaxTimeMs(), "changeStreamQueryMaxTimeMs");
    Check.argIsPositive(
        replicationConfig.getChangeStreamCursorMaxTimeSec(), "changeStreamCursorMaxTimeSec");
    return new ChangeStreamMongoClientFactory(
        mongoClient, sessionRefresher, modeSelector, meterAndFtdcRegistry, replicationConfig);
  }

  /**
   * Starts a change stream on the supplied collection using the supplied {@link
   * AggregateOperationTemplate}. Uses the default batch size.
   */
  ChangeStreamMongoClient<SteadyStateException> resumeDefaultChangeStream(
      GenerationId generationId, AggregateOperationTemplate aggregateTemplate) {

    ChangeStreamMongoClient<SteadyStateException> client =
        new ChangeStreamMongoCursorClient<>(
            generationId,
            this.mongoClient,
            this.sessionRefresher,
            aggregateTemplate,
            this.ftdcRegistry,
            SteadyStateException::wrapIfThrows,
            Optional.empty());

    if (this.enableSplitLargeChangeStreamEvents) {
      return new SplitEventChangeStreamClient<>(
          client, SteadyStateException::wrapIfThrows,
              this.meterMetricsFactory, generationId);
    } else {
      return client;
    }
  }

  /**
   * Starts a change stream on the supplied collection using the supplied {@link
   * AggregateOperationTemplate}. Sets the batch size to the value specified in replication configs.
   */
  ChangeStreamMongoClient<SteadyStateException> resumeBatchSizeConfiguredChangeStream(
      GenerationId generationId, AggregateOperationTemplate aggregateTemplate) {
    ChangeStreamMongoClient<SteadyStateException> client =
        new ChangeStreamMongoCursorClient<>(
            generationId,
            this.mongoClient,
            this.sessionRefresher,
            aggregateTemplate,
            this.ftdcRegistry,
            SteadyStateException::wrapIfThrows,
            this.embeddingGetMoreBatchSize);

    if (this.enableSplitLargeChangeStreamEvents) {
      return new SplitEventChangeStreamClient<>(
          client, SteadyStateException::wrapIfThrows,
              this.meterMetricsFactory, generationId);
    } else {
      return client;
    }
  }

  /**
   * Starts a change stream which might be restarted when {@link ChangeStreamMode} needs to be
   * switched.
   */
  private ModeAwareChangeStreamClient resumeModeAwareChangeStream(
      GenerationId generationId,
      ChangeStreamResumeInfo resumeInfo,
      IndexDefinition indexDefinition,
      boolean removeMatchCollectionUuid) {

    // removeMatchCollectionUuid will be true if change stream commands are failing because
    // matchCollectionUuidForUpdateLookup is unrecognized.
    var operationFactory =
        new ChangeStreamAggregateOperationFactory(
            indexDefinition,
            this.changeStreamQueryMaxTimeMs,
            this.excludedChangestreamFields,
            !removeMatchCollectionUuid && this.matchCollectionUuidForUpdateLookup,
            this.enableSplitLargeChangeStreamEvents);

    return new ModeAwareChangeStreamClient(
        this.modeSelector,
        operationFactory,
        indexDefinition.isAutoEmbeddingIndex()
            ? this::resumeBatchSizeConfiguredChangeStream
            : this::resumeDefaultChangeStream,
        resumeInfo,
        generationId);
  }

  /**
   * Starts a change stream which might be restarted when {@link ChangeStreamMode} needs to be
   * switched or passed TTL.
   */
  ChangeStreamMongoClient<SteadyStateException> resumeTimedModeAwareChangeStream(
      GenerationId generationId,
      ChangeStreamResumeInfo resumeInfo,
      IndexDefinition indexDefinition,
      boolean removeMatchCollectionUuid) {

    TimedChangeStreamClientFactory clientFactory =
        (GenerationId id, ChangeStreamResumeInfo info) ->
            this.resumeModeAwareChangeStream(id, info, indexDefinition, removeMatchCollectionUuid);

    // randomly distribute given ttl by 10% to avoid restarting all cursors in a short period
    Duration timeToLive =
        DurationUtils.getRandomlyDistributionDuration(this.changeStreamCursorMaxTime, 0.1);

    return new TimedChangeStreamClient(clientFactory, resumeInfo, generationId, timeToLive);
  }

  @FunctionalInterface
  interface ChangeStreamClientFactory {

    ChangeStreamMongoClient<SteadyStateException> resumeChangeStream(
        GenerationId generationId, AggregateOperationTemplate aggregateTemplate);
  }

  @FunctionalInterface
  interface TimedChangeStreamClientFactory {

    TimeableChangeStreamMongoClient<SteadyStateException> resumeChangeStream(
        GenerationId generationId, ChangeStreamResumeInfo resumeInfo);
  }
}
