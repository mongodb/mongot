package com.xgen.mongot.replication.mongodb.steadystate.changestream;

import static com.xgen.mongot.replication.mongodb.common.ChangeStreamModeSelector.ChangeStreamMode;

import com.mongodb.MongoNamespace;
import com.xgen.mongot.index.version.GenerationId;
import com.xgen.mongot.replication.mongodb.common.ChangeStreamAggregateOperationFactory;
import com.xgen.mongot.replication.mongodb.common.ChangeStreamBatch;
import com.xgen.mongot.replication.mongodb.common.ChangeStreamModeSelector;
import com.xgen.mongot.replication.mongodb.common.ChangeStreamMongoClient;
import com.xgen.mongot.replication.mongodb.common.ChangeStreamPartition;
import com.xgen.mongot.replication.mongodb.common.ChangeStreamResumeInfo;
import com.xgen.mongot.replication.mongodb.common.SteadyStateException;
import com.xgen.mongot.replication.mongodb.common.TimeableChangeStreamMongoClient;
import com.xgen.mongot.replication.mongodb.steadystate.changestream.ChangeStreamMongoClientFactory.ChangeStreamClientFactory;
import com.xgen.mongot.util.Check;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import org.bson.BsonDocument;
import org.bson.BsonTimestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ModeAwareChangeStreamClient implements TimeableChangeStreamMongoClient<SteadyStateException> {
  private static final Logger LOG = LoggerFactory.getLogger(ModeAwareChangeStreamClient.class);

  private final ChangeStreamModeSelector modeSelector;
  private final ChangeStreamAggregateOperationFactory operationFactory;
  private final ChangeStreamClientFactory clientFactory;
  private final MongoNamespace namespace;
  private final GenerationId generationId;
  private final Instant createdAt;
  private final Optional<ChangeStreamPartition> partition;

  private ChangeStreamMode currentMode;
  private ChangeStreamMongoClient<SteadyStateException> wrapped;

  public ModeAwareChangeStreamClient(
      ChangeStreamModeSelector modeSelector,
      ChangeStreamAggregateOperationFactory operationFactory,
      ChangeStreamClientFactory clientFactory,
      ChangeStreamResumeInfo resumeInfo,
      GenerationId generationId) {
    this.modeSelector = modeSelector;
    this.currentMode = modeSelector.getMode(generationId);
    this.operationFactory = operationFactory;
    this.clientFactory = clientFactory;
    this.namespace = resumeInfo.getNamespace();
    this.generationId = generationId;
    this.createdAt = Instant.now();
    this.partition = Optional.empty();
    this.wrapped = createChangeStreamClient(this.currentMode, resumeInfo.getResumeToken());
  }

  /**
   * Creates a mode-aware client for one partition of a parallel change stream. The first cursor
   * starts at {@code startAtOperationTime} with a partition {@code $match}; later restarts (TTL or
   * mode switch) resume the same partition after its own post-batch resume token.
   */
  public ModeAwareChangeStreamClient(
      ChangeStreamModeSelector modeSelector,
      ChangeStreamAggregateOperationFactory operationFactory,
      ChangeStreamClientFactory clientFactory,
      MongoNamespace namespace,
      BsonTimestamp startAtOperationTime,
      ChangeStreamPartition partition,
      GenerationId generationId) {
    Check.argNotNull(namespace, "namespace");
    Check.argNotNull(startAtOperationTime, "startAtOperationTime");
    Check.argNotNull(partition, "partition");

    this.modeSelector = modeSelector;
    this.currentMode = modeSelector.getMode(generationId);
    this.operationFactory = operationFactory;
    this.clientFactory = clientFactory;
    this.namespace = namespace;
    this.generationId = generationId;
    this.createdAt = Instant.now();
    this.partition = Optional.of(partition);
    this.wrapped =
        this.clientFactory.resumeChangeStream(
            this.generationId,
            this.operationFactory.fromOperationTime(
                this.namespace, startAtOperationTime, this.currentMode, partition));
  }

  /**
   * Creates a mode-aware client for one partition of a parallel change stream, resuming from a
   * saved per-partition resume token (for example, after a {@link TimedChangeStreamClient} TTL
   * restart). Later restarts (TTL or mode switch) continue to resume the same partition after its
   * own post-batch resume token.
   */
  public ModeAwareChangeStreamClient(
      ChangeStreamModeSelector modeSelector,
      ChangeStreamAggregateOperationFactory operationFactory,
      ChangeStreamClientFactory clientFactory,
      ChangeStreamResumeInfo resumeInfo,
      ChangeStreamPartition partition,
      GenerationId generationId) {
    Check.argNotNull(partition, "partition");

    this.modeSelector = modeSelector;
    this.currentMode = modeSelector.getMode(generationId);
    this.operationFactory = operationFactory;
    this.clientFactory = clientFactory;
    this.namespace = resumeInfo.getNamespace();
    this.generationId = generationId;
    this.createdAt = Instant.now();
    this.partition = Optional.of(partition);
    this.wrapped = createChangeStreamClient(this.currentMode, resumeInfo.getResumeToken());
  }

  /**
   * Proxies {@link ChangeStreamMongoClient#getNext} call to the wrapped client and restarts it with
   * a different command when the change stream mode needs to be switched based on {@link
   * ChangeStreamModeSelector} decision.
   */
  @Override
  public ChangeStreamBatch getNext() throws SteadyStateException {
    ChangeStreamBatch batch = this.wrapped.getNext();
    ChangeStreamMode selectedMode = this.modeSelector.getMode(this.generationId);

    if (selectedMode != this.currentMode) {
      restartClient(batch.getPostBatchResumeToken(), selectedMode);
      this.currentMode = selectedMode;
    }

    return batch;
  }

  @Override
  public void close() {
    this.wrapped.close();
  }

  @Override
  public Duration getUptime() {
    return Duration.between(this.createdAt, Instant.now());
  }

  private void restartClient(BsonDocument resumeToken, ChangeStreamMode mode) {
    LOG.atInfo()
        .addKeyValue("indexId", this.generationId.indexId)
        .addKeyValue("generationId", this.generationId)
        .addKeyValue("mode", mode)
        .log("Restarting change-stream client!");

    this.wrapped.close();
    this.wrapped = createChangeStreamClient(mode, resumeToken);
  }

  private ChangeStreamMongoClient<SteadyStateException> createChangeStreamClient(
      ChangeStreamMode mode, BsonDocument resumeToken) {

    ChangeStreamResumeInfo resumeInfo = ChangeStreamResumeInfo.create(this.namespace, resumeToken);
    if (this.partition.isPresent()) {
      return this.clientFactory.resumeChangeStream(
          this.generationId,
          this.operationFactory.fromResumeInfo(resumeInfo, mode, this.partition.get()));
    }
    return this.clientFactory.resumeChangeStream(
        this.generationId, this.operationFactory.fromResumeInfo(resumeInfo, mode));
  }
}
