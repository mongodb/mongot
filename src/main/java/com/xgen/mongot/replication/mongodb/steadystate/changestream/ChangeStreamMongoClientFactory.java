package com.xgen.mongot.replication.mongodb.steadystate.changestream;

import com.google.common.annotations.VisibleForTesting;
import com.mongodb.MongoNamespace;
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
import com.xgen.mongot.replication.mongodb.common.ChangeStreamPartition;
import com.xgen.mongot.replication.mongodb.common.ChangeStreamResumeInfo;
import com.xgen.mongot.replication.mongodb.common.PartitionedChangeStreamClient;
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
import java.util.concurrent.atomic.AtomicBoolean;
import org.bson.BsonDocument;
import org.bson.BsonTimestamp;

class ChangeStreamMongoClientFactory {
  private final BatchMongoClient mongoClient;
  private final SessionRefresher sessionRefresher;
  private final ChangeStreamModeSelector modeSelector;
  private final int changeStreamQueryMaxTimeMs;
  private final Duration changeStreamCursorMaxTime;
  private final Optional<Integer> embeddingGetMoreBatchSize;
  private final List<String> excludedChangestreamFields;
  private final boolean matchCollectionUuidForUpdateLookup;
  private final MeterRegistry ftdcRegistry;
  private final MetricsFactory meterMetricsFactory;
  private final boolean enableSplitLargeChangeStreamEvents;
  private final boolean enableParallelChangeStreamReaders;
  private final int numParallelChangeStreamReaders;

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
    this.enableParallelChangeStreamReaders =
        replicationConfig.getEnableParallelChangeStreamReaders();
    this.numParallelChangeStreamReaders = replicationConfig.getNumParallelChangeStreamReaders();
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
            Optional.empty(),
            Optional.of(this.changeStreamQueryMaxTimeMs));

    if (this.enableSplitLargeChangeStreamEvents) {
      return new SplitEventChangeStreamClient<>(
          client, SteadyStateException::wrapIfThrows, this.meterMetricsFactory, generationId);
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
            this.embeddingGetMoreBatchSize,
            Optional.of(this.changeStreamQueryMaxTimeMs));

    if (this.enableSplitLargeChangeStreamEvents) {
      return new SplitEventChangeStreamClient<>(
          client, SteadyStateException::wrapIfThrows, this.meterMetricsFactory, generationId);
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

    var operationFactory = operationFactoryFor(indexDefinition, removeMatchCollectionUuid);

    return new ModeAwareChangeStreamClient(
        this.modeSelector,
        operationFactory,
        cursorFactoryFor(indexDefinition),
        resumeInfo,
        generationId);
  }

  private ChangeStreamAggregateOperationFactory operationFactoryFor(
      IndexDefinition indexDefinition, boolean removeMatchCollectionUuid) {
    // removeMatchCollectionUuid will be true if change stream commands are failing because
    // matchCollectionUuidForUpdateLookup is unrecognized.
    return new ChangeStreamAggregateOperationFactory(
        indexDefinition,
        this.excludedChangestreamFields,
        !removeMatchCollectionUuid && this.matchCollectionUuidForUpdateLookup,
        this.enableSplitLargeChangeStreamEvents);
  }

  private ChangeStreamClientFactory cursorFactoryFor(IndexDefinition indexDefinition) {
    return indexDefinition.isAutoEmbeddingIndex()
        ? this::resumeBatchSizeConfiguredChangeStream
        : this::resumeDefaultChangeStream;
  }

  /**
   * Starts a change stream which might be restarted when {@link ChangeStreamMode} needs to be
   * switched or passed TTL. When {@link Feature#PARALLEL_CHANGE_STREAM_READERS} is enabled, one
   * reader per partition is started instead; otherwise behavior is unchanged.
   */
  ChangeStreamMongoClient<SteadyStateException> resumeTimedModeAwareChangeStream(
      GenerationId generationId,
      ChangeStreamResumeInfo resumeInfo,
      IndexDefinition indexDefinition,
      boolean removeMatchCollectionUuid)
      throws SteadyStateException {
    if (this.enableParallelChangeStreamReaders) {
      return resumePartitionedChangeStream(
          generationId, resumeInfo, indexDefinition, removeMatchCollectionUuid);
    }

    TimedChangeStreamClientFactory clientFactory =
        (GenerationId id, ChangeStreamResumeInfo info) ->
            this.resumeModeAwareChangeStream(id, info, indexDefinition, removeMatchCollectionUuid);

    // randomly distribute given ttl by 10% to avoid restarting all cursors in a short period
    Duration timeToLive =
        DurationUtils.getRandomlyDistributionDuration(this.changeStreamCursorMaxTime, 0.1);

    return new TimedChangeStreamClient(clientFactory, resumeInfo, generationId, timeToLive);
  }

  /** Starts a {@link PartitionedChangeStreamClient} with one reader per partition. */
  private ChangeStreamMongoClient<SteadyStateException> resumePartitionedChangeStream(
      GenerationId generationId,
      ChangeStreamResumeInfo resumeInfo,
      IndexDefinition indexDefinition,
      boolean removeMatchCollectionUuid)
      throws SteadyStateException {

    var operationFactory = operationFactoryFor(indexDefinition, removeMatchCollectionUuid);
    ChangeStreamClientFactory cursorFactory = cursorFactoryFor(indexDefinition);

    // cursorFactory closes over this.mongoClient, so the client argument is unused here.
    PartitionedChangeStreamClient.ReaderFactory<SteadyStateException> readerFactory =
        (unusedMongoClient, partition, start) ->
            newPartitionReader(
                generationId,
                resumeInfo.getNamespace(),
                operationFactory,
                cursorFactory,
                partition,
                start);

    return PartitionedChangeStreamClient.resume(
        this.numParallelChangeStreamReaders,
        resumeInfo.getResumeToken(),
        this.mongoClient,
        readerFactory,
        SteadyStateException::wrapIfThrows);
  }

  /**
   * Builds one partition's reader. The first open uses {@code start} because a shared resume token
   * is not valid with {@code startAfter} on a filtered per-partition stream; restarts use the
   * partition's own post-batch resume token (see {@link #partitionClientFactory}).
   */
  private TimeableChangeStreamMongoClient<SteadyStateException> newPartitionReader(
      GenerationId generationId,
      MongoNamespace namespace,
      ChangeStreamAggregateOperationFactory operationFactory,
      ChangeStreamClientFactory cursorFactory,
      ChangeStreamPartition partition,
      BsonTimestamp start)
      throws SteadyStateException {

    // Jitter the TTL so N partitions of one index don't all restart at once.
    Duration timeToLive =
        DurationUtils.getRandomlyDistributionDuration(this.changeStreamCursorMaxTime, 0.1);
    TimedChangeStreamClientFactory clientFactory =
        partitionClientFactory(operationFactory, cursorFactory, namespace, start, partition);

    // TimedChangeStreamClient requires a token, but the first open ignores it (opens by operation
    // time) and later restarts replace it with the partition's own.
    ChangeStreamResumeInfo placeholderResumeInfo =
        ChangeStreamResumeInfo.create(namespace, new BsonDocument());

    return new TimedChangeStreamClient(
        clientFactory, placeholderResumeInfo, generationId, timeToLive);
  }

  /**
   * Restart factory for one partition: the first invocation opens by operation time with the
   * partition filter; later ones resume from the partition's own resume token in {@code info}.
   */
  @VisibleForTesting
  TimedChangeStreamClientFactory partitionClientFactory(
      ChangeStreamAggregateOperationFactory operationFactory,
      ChangeStreamClientFactory cursorFactory,
      MongoNamespace namespace,
      BsonTimestamp start,
      ChangeStreamPartition partition) {
    AtomicBoolean isFirstOpen = new AtomicBoolean(true);
    return (id, info) -> {
      if (isFirstOpen.getAndSet(false)) {
        return new ModeAwareChangeStreamClient(
            this.modeSelector, operationFactory, cursorFactory, namespace, start, partition, id);
      }
      return new ModeAwareChangeStreamClient(
          this.modeSelector, operationFactory, cursorFactory, info, partition, id);
    };
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
