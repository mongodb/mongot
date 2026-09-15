package com.xgen.mongot.replication.mongodb.common;

import static com.xgen.mongot.util.Check.checkState;

import com.google.errorprone.annotations.concurrent.LazyInit;
import com.mongodb.ClientSessionOptions;
import com.mongodb.ServerAddress;
import com.mongodb.client.ClientSession;
import com.mongodb.internal.operation.AggregateOperation;
import com.mongodb.internal.operation.AggregateResponseBatchCursor;
import com.mongodb.internal.operation.BatchCursor;
import com.xgen.mongot.index.version.GenerationId;
import com.xgen.mongot.logging.DefaultKeyValueLogger;
import com.xgen.mongot.metrics.MetricsFactory;
import com.xgen.mongot.replication.mongodb.common.ChangeStreamAggregateOperationBuilder.AggregateOperationTemplate;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.mongodb.BatchMongoClient;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import org.bson.BsonDocument;
import org.bson.BsonTimestamp;
import org.bson.BsonValue;
import org.bson.RawBsonDocument;
import org.jetbrains.annotations.Nullable;

/**
 * A dedicated client to tail change-stream changes on a collection or database. This client
 * provides an abstraction over required logic to maintain a MongoDB cursor.
 *
 * <p>This client will open a {@link ClientSession} and {@link
 * com.mongodb.internal.operation.BatchCursor} on first call to getNext().
 *
 * <p>This client is intended to be used by a single thread at a given time and is not thread safe!.
 */
public class ChangeStreamMongoCursorClient<E extends Exception>
    implements ChangeStreamMongoClient<E> {

  private final DefaultKeyValueLogger logger;
  private final BatchMongoClient mongoClient;
  private final SessionRefresher sessionRefresher;
  private final AggregateOperationTemplate aggregateTemplate;
  private final WrapIfThrows<E> exceptionWrapper;
  private final AtomicBoolean closed;
  private final Optional<Integer> batchSize;
  private final Optional<Integer> maxTimeMs;
  private final MetricsFactory ftdcMetricsFactory;

  private State state;
  private @LazyInit RefreshingClientSession<ClientSession> refreshingSession;
  private @LazyInit AggregateResponseBatchCursor<RawBsonDocument> batchCursor;
  private Optional<ServerAddress> serverAddress = Optional.empty();

  /**
   * Creates a new {@link ChangeStreamMongoCursorClient} instance.
   *
   * @param generationId The given index unique identifier.
   * @param mongoClient MongoDB client instance. Assumed to be shared and not owned by this client.
   * @param sessionRefresher A session refresher instance.
   * @param aggregateTemplate An aggregate operation for cursor creation
   * @param exceptionWrapper An exception transform function into {@link SteadyStateException}
   */
  public ChangeStreamMongoCursorClient(
      GenerationId generationId,
      BatchMongoClient mongoClient,
      SessionRefresher sessionRefresher,
      AggregateOperationTemplate aggregateTemplate,
      MeterRegistry ftdcRegistry,
      WrapIfThrows<E> exceptionWrapper,
      Optional<Integer> batchSize) {
    this(
        generationId,
        mongoClient,
        sessionRefresher,
        aggregateTemplate,
        ftdcRegistry,
        exceptionWrapper,
        batchSize,
        Optional.empty());
  }

  /**
   * Creates a new {@link ChangeStreamMongoCursorClient} instance.
   *
   * @param generationId The given index unique identifier.
   * @param mongoClient MongoDB client instance. Assumed to be shared and not owned by this client.
   * @param sessionRefresher A session refresher instance.
   * @param aggregateTemplate An aggregate operation for cursor creation
   * @param exceptionWrapper An exception transform function into {@link SteadyStateException}
   * @param batchSize optional change-stream getMore batch size
   * @param maxTimeMs optional maxTimeMS to apply to the change-stream aggregate command (driver 5.x
   *     applies it via the operation's {@code TimeoutContext}; see {@code BatchMongoClient})
   */
  public ChangeStreamMongoCursorClient(
      GenerationId generationId,
      BatchMongoClient mongoClient,
      SessionRefresher sessionRefresher,
      AggregateOperationTemplate aggregateTemplate,
      MeterRegistry ftdcRegistry,
      WrapIfThrows<E> exceptionWrapper,
      Optional<Integer> batchSize,
      Optional<Integer> maxTimeMs) {
    Check.isNotNull(mongoClient, "mongoClient");
    Check.isNotNull(sessionRefresher, "sessionRefresher");

    HashMap<String, Object> defaultKeyValues = new HashMap<>();
    defaultKeyValues.put("indexId", generationId.indexId);
    defaultKeyValues.put("generationId", generationId);
    this.logger =
        DefaultKeyValueLogger.getLogger(ChangeStreamMongoCursorClient.class, defaultKeyValues);
    this.mongoClient = mongoClient;
    this.sessionRefresher = sessionRefresher;
    this.aggregateTemplate = aggregateTemplate;
    this.exceptionWrapper = exceptionWrapper;
    this.closed = new AtomicBoolean(false);
    this.state = State.OPEN_CURSOR;
    this.batchSize = batchSize;
    this.maxTimeMs = maxTimeMs;
    this.ftdcMetricsFactory =
        new MetricsFactory(
            "indexing.changeStreamMongoCursorClient",
            ftdcRegistry,
            Tags.of("generationId", generationId.toString()));
  }

  public Optional<ServerAddress> getServerAddress() {
    return Optional.ofNullable(this.batchCursor).map(BatchCursor::getServerAddress);
  }

  @Override
  public ChangeStreamBatch getNext() throws E {
    switch (this.state) {
      case OPEN_CURSOR:
        this.openCursor();
      // fall through

      case GET_MORE:
        return this.getMore();

      case CLOSED:
        throw new IllegalStateException("getNext() called in CLOSED state");
    }
    this.logger.error("Unhandled state: {}", this.state);
    return Check.unreachable("Unhandled state");
  }

  private void openCursor() throws E {
    this.ensureState(State.OPEN_CURSOR);

    this.refreshingSession =
        this.exceptionWrapper.wrapIfThrows(
            () ->
                this.sessionRefresher.register(
                    this.mongoClient.openSession(ClientSessionOptions.builder().build())));

    AggregateOperation<RawBsonDocument> aggregateOperation =
        this.aggregateTemplate.create(
            this.mongoClient.getSettings().getCodecRegistry().get(RawBsonDocument.class));

    this.batchCursor =
        this.exceptionWrapper.wrapIfThrows(
            () -> {
              var cursor =
                  (AggregateResponseBatchCursor<RawBsonDocument>)
                      this.mongoClient.openCursor(
                          aggregateOperation, this.refreshingSession.getSession(), this.maxTimeMs);

              this.batchSize.ifPresent(cursor::setBatchSize);
              this.state = State.GET_MORE;
              return cursor;
            });

    this.serverAddress = getServerAddress();
    this.serverAddress.ifPresent(
        server -> {
          this.logger.atInfo().addKeyValue("server", server).log("New sync source for index");
          this.ftdcMetricsFactory
              .numGauge("syncSource", Tags.of("server", server.toString()))
              .incrementAndGet();
        });
  }

  private ChangeStreamBatch getMore() throws E {
    this.ensureState(State.GET_MORE);
    Check.isNotNull(this.batchCursor, "batchCursor");

    return this.exceptionWrapper.wrapIfThrows(
        () -> {
          var nextBatch = this.batchCursor.tryNext();
          var postBatchResumeToken =
              throwIfNull(this.batchCursor.getPostBatchResumeToken(), "getPostBatchResumeToken");
          return new ChangeStreamBatch(
              nextBatch != null ? nextBatch : List.of(),
              postBatchResumeToken,
              currentClusterOperationTime());
        });
  }

  /**
   * Returns the cluster time most recently gossiped by the server for this session, used as the
   * "now" reference when recording replication lag.
   *
   * <p>In driver 5.x, change-stream {@code getMore} responses no longer surface a top-level {@code
   * operationTime} (so {@link AggregateResponseBatchCursor#getOperationTime()} returns null), but
   * they do gossip {@code $clusterTime}, which the driver tracks on the {@link ClientSession}. We
   * source the lag "now" from there. Empty if the session has not yet observed a cluster time (e.g.
   * before the first command); callers must tolerate an absent result.
   */
  private Optional<BsonTimestamp> currentClusterOperationTime() throws Exception {
    // refreshingSession is @LazyInit and assigned in openCursor() before the state transitions to
    // GET_MORE; currentClusterOperationTime() is only reachable in GET_MORE (via getMore(), guarded
    // by ensureState), so it is non-null here. Assert to make that invariant explicit.
    Check.isNotNull(this.refreshingSession, "refreshingSession");

    // A null cluster-time document is a legitimate state: the session may not have gossiped a
    // $clusterTime yet (e.g. before the first command). Tolerate it as absent rather than throwing,
    // since throwing here would restart the change stream on every getMore (the driver 5.x
    // regression this method works around).
    BsonDocument clusterTime = this.refreshingSession.getSession().getClusterTime();
    if (clusterTime == null) {
      return Optional.empty();
    }

    // A present-but-malformed clusterTime (missing or non-timestamp "clusterTime" field) is
    // different: that indicates an unexpected response shape, which we surface as a
    // ChangeStreamCursorClientException (wrapped to SteadyStateException Type.TRANSIENT) rather
    // than silently dropping the lag metric forever.
    BsonValue value = clusterTime.get("clusterTime");
    if (value == null || !value.isTimestamp()) {
      throw new ChangeStreamCursorClientException(
          new IllegalStateException(
              "session clusterTime document has a missing or non-timestamp 'clusterTime' field: "
                  + clusterTime.toJson()));
    }
    return Optional.of(value.asTimestamp());
  }

  @Override
  public void close() {
    // client is not thread-safe and close() is expected to be called once, regardless
    // ensure underlying resources are closed only once.
    if (!this.closed.getAndSet(true)) {
      this.logger.info("Closing change stream cursor for index");
      if (this.batchCursor != null) {
        this.serverAddress.ifPresent(
            server -> {
              this.ftdcMetricsFactory
                  .numGauge("syncSource", Tags.of("server", server.toString()))
                  .decrementAndGet();
            });
        this.batchCursor.close();
      }
      if (this.refreshingSession != null) {
        this.refreshingSession.close();
      }
      this.state = State.CLOSED;
    }
  }

  private static <T extends BsonValue> T throwIfNull(@Nullable T value, String fieldName)
      throws Exception {
    if (value == null) {
      throw new ChangeStreamCursorClientException(new NullPointerException(fieldName));
    }
    return value;
  }

  private void ensureState(State expectedState) {
    checkState(
        this.state == expectedState,
        "Unexpected state: %s! [Expected: %s]",
        this.state,
        expectedState);
  }

  private enum State {
    OPEN_CURSOR,
    GET_MORE,
    CLOSED,
  }
}
