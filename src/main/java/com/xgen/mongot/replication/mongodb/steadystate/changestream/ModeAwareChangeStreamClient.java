package com.xgen.mongot.replication.mongodb.steadystate.changestream;

import static com.xgen.mongot.replication.mongodb.common.ChangeStreamModeSelector.ChangeStreamMode;

import com.mongodb.MongoNamespace;
import com.xgen.mongot.index.version.GenerationId;
import com.xgen.mongot.replication.mongodb.common.ChangeStreamAggregateOperationFactory;
import com.xgen.mongot.replication.mongodb.common.ChangeStreamBatch;
import com.xgen.mongot.replication.mongodb.common.ChangeStreamModeSelector;
import com.xgen.mongot.replication.mongodb.common.ChangeStreamMongoClient;
import com.xgen.mongot.replication.mongodb.common.ChangeStreamResumeInfo;
import com.xgen.mongot.replication.mongodb.common.SteadyStateException;
import com.xgen.mongot.replication.mongodb.common.TimeableChangeStreamMongoClient;
import com.xgen.mongot.replication.mongodb.steadystate.changestream.ChangeStreamMongoClientFactory.ChangeStreamClientFactory;
import java.time.Duration;
import java.time.Instant;
import org.bson.BsonDocument;
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

    return this.clientFactory.resumeChangeStream(
        this.generationId,
        this.operationFactory.fromResumeInfo(
            ChangeStreamResumeInfo.create(this.namespace, resumeToken), mode));
  }
}
