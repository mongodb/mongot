package com.xgen.mongot.replication.mongodb.steadystate.changestream;

import com.xgen.mongot.index.version.GenerationId;
import com.xgen.mongot.logging.DefaultKeyValueLogger;
import com.xgen.mongot.replication.mongodb.common.ChangeStreamBatch;
import com.xgen.mongot.replication.mongodb.common.SteadyStateException;
import com.xgen.mongot.replication.mongodb.common.TimeableChangeStreamMongoClient;
import com.xgen.mongot.replication.mongodb.steadystate.changestream.ChangeStreamMongoClientFactory.TimedChangeStreamClientFactory;
import java.time.Duration;
import java.util.HashMap;
import org.bson.BsonDocument;

/**
 * Replaces its wrapped client after a batch read if the client's uptime exceeds its TTL. Passes
 * that batch's post-batch resume token to the factory when creating the replacement.
 */
public class TimedChangeStreamClient
    implements TimeableChangeStreamMongoClient<SteadyStateException> {

  private final DefaultKeyValueLogger logger;
  private final TimedChangeStreamClientFactory clientFactory;
  private final GenerationId generationId;
  private final Duration timeToLive;
  private volatile TimeableChangeStreamMongoClient<SteadyStateException> wrapped;
  private boolean closed = false;

  /** Takes ownership of the initial client and uses the factory only for TTL restarts. */
  public TimedChangeStreamClient(
      TimeableChangeStreamMongoClient<SteadyStateException> initialClient,
      TimedChangeStreamClientFactory clientFactory,
      GenerationId generationId,
      Duration timeToLive) {
    HashMap<String, Object> defaultKeyValues = new HashMap<>();
    defaultKeyValues.put("indexId", generationId.indexId);
    defaultKeyValues.put("generationId", generationId);
    defaultKeyValues.put("TTL", timeToLive.toSeconds());
    this.logger = DefaultKeyValueLogger.getLogger(TimedChangeStreamClient.class, defaultKeyValues);
    this.logger.info("Starting timed change stream client");

    this.clientFactory = clientFactory;
    this.generationId = generationId;
    this.timeToLive = timeToLive;
    this.wrapped = initialClient;
  }

  @Override
  public ChangeStreamBatch getNext() throws SteadyStateException {
    ChangeStreamBatch batch = this.wrapped.getNext();

    if (shouldRestartClient()) {
      restartChangeStreamClient(batch.getPostBatchResumeToken());
    }

    return batch;
  }

  @Override
  public void close() {
    TimeableChangeStreamMongoClient<SteadyStateException> current;
    synchronized (this) {
      this.closed = true;
      current = this.wrapped;
    }
    current.close();
  }

  @Override
  public Duration getUptime() {
    return this.wrapped.getUptime();
  }

  private boolean shouldRestartClient() {
    return this.timeToLive.minus(this.wrapped.getUptime()).isNegative();
  }

  private void restartChangeStreamClient(BsonDocument resumeToken) {
    this.logger.info("Restarting change-stream client!");
    this.wrapped.close();
    TimeableChangeStreamMongoClient<SteadyStateException> restarted =
        this.clientFactory.resumeChangeStream(this.generationId, resumeToken);
    synchronized (this) {
      if (!this.closed) {
        this.wrapped = restarted;
        return;
      }
    }
    // close() ran while the new client was being created and closed only the old one.
    restarted.close();
  }
}
