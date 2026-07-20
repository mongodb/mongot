package com.xgen.mongot.replication.mongodb.steadystate.changestream;

import com.google.errorprone.annotations.Var;
import com.mongodb.MongoNamespace;
import com.mongodb.client.MongoClient;
import com.xgen.mongot.index.version.GenerationId;
import com.xgen.mongot.metrics.MetricsFactory;
import com.xgen.mongot.replication.mongodb.common.ChangeStreamBatch;
import com.xgen.mongot.replication.mongodb.common.ChangeStreamMongoClient;
import com.xgen.mongot.replication.mongodb.common.CommonReplicationConfig;
import com.xgen.mongot.replication.mongodb.common.DefaultChangeStreamMongoClient;
import com.xgen.mongot.replication.mongodb.common.ResumeTokenUtils;
import com.xgen.mongot.replication.mongodb.common.SessionRefresher;
import com.xgen.mongot.replication.mongodb.common.SteadyStateException;
import com.xgen.mongot.util.concurrent.Executors;
import com.xgen.mongot.util.concurrent.NamedScheduledExecutorService;
import com.xgen.mongot.util.mongodb.ChangeStreamAggregateCommand;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.codec.DecoderException;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonTimestamp;
import org.bson.BsonValue;
import org.bson.RawBsonDocument;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Observes a single database's change stream to centrally track per-collection write activity for
 * the search indexes managed on that database.
 *
 * <p>The observer opens one collection-less (database-level) change stream, demultiplexes each
 * event to a collection by namespace, and derives per-collection idle times from oplog cluster
 * times: a collection's idle time is equal to the difference between the optime of the latest
 * post-batch resume token processed (which advances even on an idle database because the replica
 * set periodically writes no-op oplog entries) and the oplog cluster time of the most recent event
 * seen for that collection. A collection with no managed index is ignored after the demux lookup.
 *
 * <p>The observer is purely observational: it emits gauges from this central view but does not
 * close cursors, park indexes, or change replication behavior in any way (yet). It holds no durable
 * state; after a mongot restart, per-collection baselines are re-seeded from the stream's current
 * position, so idle times age from the restart.
 *
 * <p>Threading: {@link #register} and {@link #deregister} are called under the {@link
 * ChangeStreamManager} lock. The read loop runs on the supplied single-threaded executor; the
 * stream client and resume token are confined to that thread until {@link #close()} has terminated
 * the executor. Gauges are computed at scrape time from concurrent maps.
 */
class ChangeStreamActivityObserver implements AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(ChangeStreamActivityObserver.class);

  /** Delay before retrying after a stream failure, so a down mongod is not hot-looped. */
  private static final Duration STREAM_RETRY_DELAY = Duration.ofSeconds(5);

  private static final Duration CLOSE_TIMEOUT = Duration.ofSeconds(5);

  /**
   * Events-only projection: the observer needs just enough of each event to demux it by collection.
   * Keeping {@code _id} is mandatory for change-stream cursors. Full documents are never fetched
   * (no {@code fullDocument} option is set).
   */
  private static final BsonDocument EVENTS_ONLY_PROJECTION =
      new BsonDocument(
          "$project",
          new BsonDocument()
              .append("_id", new BsonInt32(1))
              .append("operationType", new BsonInt32(1))
              .append("ns", new BsonInt32(1))
              .append("documentKey", new BsonInt32(1))
              .append("clusterTime", new BsonInt32(1)));

  /** Opens the database-level change stream, optionally resuming after a previous token. */
  @FunctionalInterface
  interface StreamFactory {
    ChangeStreamMongoClient<SteadyStateException> open(Optional<BsonDocument> startAfter)
        throws SteadyStateException;
  }

  private final String databaseName;
  private final StreamFactory streamFactory;
  private final NamedScheduledExecutorService executor;
  private final Duration inactivityThreshold;
  private final MetricsFactory metricsFactory;
  private final Duration streamRetryDelay;
  private final Duration closeTimeout;

  /** The per-collection roster and demux state. */
  private final CollectionRoster roster;

  /** The optime of the latest post-batch resume token processed; null until the first batch. */
  private final AtomicReference<BsonTimestamp> currentPosition;

  private final Counter eventsObserved;
  private final Counter streamReopens;
  private final Counter baselineResets;

  /**
   * The latest post-batch resume token, kept so a transient failure can reopen from it. Confined to
   * the executor thread (read by the closing thread only after executor termination).
   */
  @Nullable private BsonDocument lastResumeToken;

  /** The open stream, or null before first open and after a failure. Confined like the token. */
  @Nullable private ChangeStreamMongoClient<SteadyStateException> stream;

  /** Whether the previous read-loop iteration failed; two consecutive failures reset baselines. */
  private boolean previousIterationFailed;

  private volatile boolean closed;

  /**
   * Creates an observer for the supplied database with a real database-level change stream, running
   * its read loop on a dedicated single-threaded executor.
   */
  static ChangeStreamActivityObserver create(
      String databaseName,
      MongoClient mongoClient,
      SessionRefresher sessionRefresher,
      MeterRegistry meterRegistry,
      Duration inactivityThreshold,
      CommonReplicationConfig.Type replicationType) {
    MetricsFactory metricsFactory =
        new MetricsFactory(
            "indexing.steadyStateChangeStream.activityObserver",
            meterRegistry,
            Tags.of("database", databaseName, "replicationType", replicationType.name()));
    NamedScheduledExecutorService executor =
        Executors.singleThreadScheduledExecutor(
            replicationType.metricsNamespacePrefix
                + "change-stream-activity-observer-"
                + databaseName,
            meterRegistry);
    StreamFactory streamFactory =
        startAfter -> {
          ChangeStreamAggregateCommand.Builder commandBuilder =
              new ChangeStreamAggregateCommand.Builder()
                  .indexedFieldsProjectionStage(EVENTS_ONLY_PROJECTION);
          startAfter.ifPresent(commandBuilder::startAfter);
          return DefaultChangeStreamMongoClient.createDatabaseLevel(
              commandBuilder.build(), mongoClient, sessionRefresher, databaseName, metricsFactory);
        };
    return new ChangeStreamActivityObserver(
        databaseName,
        streamFactory,
        executor,
        inactivityThreshold,
        metricsFactory,
        STREAM_RETRY_DELAY,
        CLOSE_TIMEOUT);
  }

  ChangeStreamActivityObserver(
      String databaseName,
      StreamFactory streamFactory,
      NamedScheduledExecutorService executor,
      Duration inactivityThreshold,
      MetricsFactory metricsFactory,
      Duration streamRetryDelay,
      Duration closeTimeout) {
    this.databaseName = databaseName;
    this.streamFactory = streamFactory;
    this.executor = executor;
    this.inactivityThreshold = inactivityThreshold;
    this.metricsFactory = metricsFactory;
    this.streamRetryDelay = streamRetryDelay;
    this.closeTimeout = closeTimeout;

    this.roster = new CollectionRoster();
    this.currentPosition = new AtomicReference<>();
    this.lastResumeToken = null;
    this.stream = null;
    this.previousIterationFailed = false;
    this.closed = false;

    this.eventsObserved = metricsFactory.counter("eventsObserved");
    this.streamReopens = metricsFactory.counter("streamReopens");
    this.baselineResets = metricsFactory.counter("baselineResets");
    metricsFactory.objectValueGauge(
        "trackedIndexes", this, observer -> observer.roster.trackedIndexCount());
    metricsFactory.objectValueGauge(
        "trackedCollections", this, observer -> observer.roster.trackedCollectionCount());
    metricsFactory.objectValueGauge(
        "inactiveIndexes", this, ChangeStreamActivityObserver::computeInactiveIndexes);

    this.executor.execute(this::runOnce);
  }

  /**
   * Registers an index and its collection with the observer. Called under the ChangeStreamManager
   * lock. If the collection is new to the roster, its activity baseline is seeded to the stream's
   * current position; if no batch has been read yet, the seed is deferred to the read loop.
   */
  void register(GenerationId generationId, MongoNamespace namespace, UUID collectionUuid) {
    this.roster.register(generationId, namespace, collectionUuid, this.currentPosition.get());
  }

  /**
   * Deregisters an index, removing its collection from the roster when it was the last index on it.
   * Called under the ChangeStreamManager lock.
   *
   * @return true when the observer tracks no indexes afterwards, so the caller may close it
   */
  boolean deregister(GenerationId generationId) {
    return this.roster.deregister(generationId);
  }

  /**
   * Closes the observer: terminates the read loop, closes the stream, and deregisters the gauges.
   * Blocks up to a short timeout for the in-flight read-loop iteration to finish.
   */
  @Override
  public synchronized void close() {
    if (this.closed) {
      return;
    }
    this.closed = true;

    this.executor.shutdown();
    @Var boolean terminated = false;
    try {
      terminated =
          this.executor.awaitTermination(this.closeTimeout.toMillis(), TimeUnit.MILLISECONDS);
      if (!terminated) {
        this.executor.shutdownNow();
        terminated =
            this.executor.awaitTermination(this.closeTimeout.toMillis(), TimeUnit.MILLISECONDS);
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }

    if (terminated) {
      // The executor has terminated, so the stream is exclusively owned by this thread now and
      // the termination provides the happens-before edge for reading the stream field.
      this.closeStreamQuietly();
    } else {
      // The read loop is still blocked in getNext. Touching the stream from this thread would
      // race the in-flight call, so leave cleanup to the read loop's own closed-check (it closes
      // the stream when getNext returns); mongod reaps the server-side cursor on timeout even if
      // the loop never returns.
      LOG.atWarn()
          .addKeyValue("database", this.databaseName)
          .log(
              "Activity observer read loop did not stop within the close timeout;"
                  + " leaving stream cleanup to the read loop");
    }
    this.metricsFactory.close();
    LOG.atInfo().addKeyValue("database", this.databaseName).log("Closed activity observer");
  }

  private void runOnce() {
    if (this.closed) {
      this.closeStreamQuietly();
      return;
    }

    @Var boolean failed = false;
    try {
      ChangeStreamBatch batch = this.ensureStream().getNext();
      this.processBatch(batch);
    } catch (SteadyStateException | DecoderException | RuntimeException e) {
      // Any getNext failure (including a SteadyStateException such as SHUT_DOWN) is treated as a
      // transient stream failure: the loop reopens and retries after streamRetryDelay. Shutdown
      // is not signaled this way; it is driven by close() setting `closed` (checked above and
      // below), which stops the loop without reopening. So SHUT_DOWN cannot outlive close(), and
      // the retry delay bounds any retry spam.
      failed = true;
      if (!this.closed) {
        this.handleStreamFailure(e);
      }
    }

    if (this.closed) {
      this.closeStreamQuietly();
      return;
    }
    this.resubmit(failed ? this.streamRetryDelay : Duration.ZERO);
  }

  private ChangeStreamMongoClient<SteadyStateException> ensureStream() throws SteadyStateException {
    ChangeStreamMongoClient<SteadyStateException> currentStream = this.stream;
    if (currentStream != null) {
      return currentStream;
    }
    ChangeStreamMongoClient<SteadyStateException> newStream =
        this.streamFactory.open(Optional.ofNullable(this.lastResumeToken));
    this.stream = newStream;
    return newStream;
  }

  private void processBatch(ChangeStreamBatch batch) throws DecoderException {
    for (RawBsonDocument event : batch.getRawEvents()) {
      this.recordEvent(event);
    }

    // Advance the position only after demuxing the batch's events so idle times are never
    // overstated within an iteration.
    this.lastResumeToken = batch.getPostBatchResumeToken();
    BsonTimestamp position =
        ResumeTokenUtils.opTimeFromResumeToken(batch.getPostBatchResumeToken());
    this.currentPosition.set(position);
    this.previousIterationFailed = false;

    this.roster.seedBaselines(position);
  }

  private void recordEvent(RawBsonDocument event) {
    // Activity-only: record the event's cluster time for demux, then discard the event. This
    // stream is a wake signal (not delivery), so it may drop events and can skip a window on
    // reset-at-tip. Parking stays lossless because a woken index resumes from its own committed
    // token, not the observer's position. Do not repurpose to update indexes without adding
    // delivery guarantees.
    this.eventsObserved.increment();

    BsonValue nsValue = event.get("ns");
    if (nsValue == null || !nsValue.isDocument()) {
      // Events like invalidate carry no namespace; there is nothing to demux.
      return;
    }
    BsonDocument ns = nsValue.asDocument();
    BsonValue dbValue = ns.get("db");
    BsonValue collValue = ns.get("coll");
    if (dbValue == null || !dbValue.isString() || collValue == null || !collValue.isString()) {
      // Database-scoped events like dropDatabase carry no collection.
      return;
    }

    Optional<UUID> collectionUuid =
        this.roster.trackedCollectionFor(
            new MongoNamespace(dbValue.asString().getValue(), collValue.asString().getValue()));
    if (collectionUuid.isEmpty()) {
      return;
    }

    BsonValue clusterTime = event.get("clusterTime");
    if (clusterTime == null || !clusterTime.isTimestamp()) {
      return;
    }
    this.roster.recordActivity(collectionUuid.get(), clusterTime.asTimestamp());
  }

  private void handleStreamFailure(Exception e) {
    this.closeStreamQuietly();
    this.streamReopens.increment();

    if (this.previousIterationFailed) {
      if (this.lastResumeToken != null) {
        // Resuming from the saved token did not recover the stream (for example the token aged
        // out of the oplog, or the database was dropped and the stream invalidated). Reopen at
        // the current tip and reset all activity: baselines re-seed and idle times restart from
        // the reopened position, so the stale pre-outage position can never seed a registration.
        this.lastResumeToken = null;
        this.currentPosition.set(null);
        this.roster.clearActivity();
        this.baselineResets.increment();
        LOG.atWarn()
            .addKeyValue("database", this.databaseName)
            .setCause(e)
            .log(
                "Activity observer could not resume; reopening at the tip and resetting"
                    + " baselines");
      } else {
        // Already reset; the retry itself keeps failing (sync source still unavailable).
        LOG.atInfo()
            .addKeyValue("database", this.databaseName)
            .setCause(e)
            .log("Activity observer retry failed; will retry from the tip");
      }
    } else {
      LOG.atInfo()
          .addKeyValue("database", this.databaseName)
          .setCause(e)
          .log("Activity observer stream failed; will reopen from the last resume token");
    }
    this.previousIterationFailed = true;
  }

  private void resubmit(Duration delay) {
    try {
      if (delay.isZero()) {
        this.executor.execute(this::runOnce);
      } else {
        this.executor.schedule(this::runOnce, delay.toMillis(), TimeUnit.MILLISECONDS);
      }
    } catch (RejectedExecutionException e) {
      // The observer is closing; close() terminates the executor and closes the stream.
    }
  }

  private void closeStreamQuietly() {
    ChangeStreamMongoClient<SteadyStateException> currentStream = this.stream;
    if (currentStream == null) {
      return;
    }
    this.stream = null;
    try {
      currentStream.close();
    } catch (RuntimeException e) {
      LOG.atWarn()
          .addKeyValue("database", this.databaseName)
          .setCause(e)
          .log("Failed closing activity observer stream");
    }
  }

  private double computeInactiveIndexes() {
    BsonTimestamp position = this.currentPosition.get();
    if (position == null) {
      return 0;
    }
    return this.roster.countInactiveIndexes(position, this.inactivityThreshold.toSeconds());
  }
}
