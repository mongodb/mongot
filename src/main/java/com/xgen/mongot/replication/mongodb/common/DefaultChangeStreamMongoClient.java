package com.xgen.mongot.replication.mongodb.common;

import static com.xgen.mongot.util.Check.checkState;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Stopwatch;
import com.mongodb.MongoNamespace;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.xgen.mongot.index.IndexMetricsUpdater.ReplicationMetricsUpdater.InitialSyncMetrics;
import com.xgen.mongot.metrics.MetricsFactory;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.mongodb.ChangeStreamAggregateCommand;
import com.xgen.mongot.util.mongodb.ChangeStreamResponse;
import com.xgen.mongot.util.mongodb.GetMoreCommand;
import com.xgen.mongot.util.mongodb.KillCursorsCommand;
import com.xgen.mongot.util.mongodb.serialization.ChangeStreamAggregateResponseProxy;
import com.xgen.mongot.util.mongodb.serialization.ChangeStreamGetMoreResponseProxy;
import com.xgen.mongot.util.mongodb.serialization.CodecRegistry;
import io.micrometer.core.instrument.Timer;
import java.time.Duration;
import java.util.Optional;
import java.util.OptionalLong;
import org.bson.conversions.Bson;

public class DefaultChangeStreamMongoClient<E extends Exception>
    implements ChangeStreamMongoClient<E> {

  private final ChangeStreamAggregateCommand aggregateCommand;
  private final RefreshingClientSession<ClientSession> refreshingSession;
  private final MongoClient mongoClient;
  private final MongoNamespace namespace;
  private final WrapIfThrows<E> exceptionWrapper;
  private final Optional<InitialSyncMetrics> initialSyncMetricsUpdaterOpt;
  private final Timer getMoreTimer;
  private final Optional<Integer> batchSize;
  private OptionalLong cursorId;
  private State state;

  @VisibleForTesting
  DefaultChangeStreamMongoClient(
      ChangeStreamAggregateCommand aggregateCommand,
      RefreshingClientSession<ClientSession> refreshingSession,
      MongoClient mongoClient,
      MetricsFactory metricsFactory,
      MongoNamespace namespace,
      WrapIfThrows<E> exceptionWrapper,
      Optional<InitialSyncMetrics> initialSyncMetricsUpdaterOpt,
      Optional<Integer> batchSize) {
    this.aggregateCommand = aggregateCommand;
    this.refreshingSession = refreshingSession;
    this.mongoClient = mongoClient;
    this.namespace = namespace;
    this.exceptionWrapper = exceptionWrapper;
    this.initialSyncMetricsUpdaterOpt = initialSyncMetricsUpdaterOpt;
    // TODO(CLOUDP-289914): Remove this getMoreTimer after switching to new one by
    // IndexMetricsUpdater
    this.getMoreTimer = metricsFactory.timerSec("getMoreDurations");
    this.batchSize = batchSize;
    this.cursorId = OptionalLong.empty();
    this.state = State.OPEN_CURSOR;
  }

  /**
   * Creates a new DefaultChangeStreamMongoClient for initial sync using the supplied
   * ChangeStreamProvider on the supplied database and collection.
   */
  public static DefaultChangeStreamMongoClient<InitialSyncException> createInitialSync(
      ChangeStreamAggregateCommand aggregateCommand,
      MongoClient mongoClient,
      SessionRefresher sessionRefresher,
      MongoNamespace namespace,
      MetricsFactory metricsFactory,
      InitialSyncMetrics initialSyncMetricsUpdater,
      Optional<Integer> batchSize)
      throws InitialSyncException {
    return create(
        aggregateCommand,
        mongoClient,
        sessionRefresher,
        metricsFactory,
        namespace,
        InitialSyncException::wrapIfThrowsChangeStream,
        Optional.of(initialSyncMetricsUpdater),
        batchSize);
  }

  /**
   * Creates a new DefaultChangeStreamMongoClient for synonym sync using the supplied
   * ChangeStreamProvider on the supplied database and collection.
   */
  public static DefaultChangeStreamMongoClient<SynonymSyncException> createSynonymSync(
      ChangeStreamAggregateCommand aggregateCommand,
      MongoClient mongoClient,
      SessionRefresher sessionRefresher,
      MongoNamespace namespace,
      MetricsFactory metricsFactory)
      throws SynonymSyncException {
    return create(
        aggregateCommand,
        mongoClient,
        sessionRefresher,
        metricsFactory,
        namespace,
        SynonymSyncException::wrapIfThrows,
        Optional.empty(),
        Optional.empty());
  }

  private static <T extends Exception> DefaultChangeStreamMongoClient<T> create(
      ChangeStreamAggregateCommand aggregateCommand,
      MongoClient mongoClient,
      SessionRefresher sessionRefresher,
      MetricsFactory metricsFactory,
      MongoNamespace namespace,
      WrapIfThrows<T> exceptionWrapper,
      Optional<InitialSyncMetrics> initialSyncMetricsUpdaterOpt,
      Optional<Integer> batchSize)
      throws T {
    return exceptionWrapper.wrapIfThrows(
        () ->
            new DefaultChangeStreamMongoClient<>(
                aggregateCommand,
                sessionRefresher.register(mongoClient.startSession()),
                mongoClient,
                metricsFactory,
                namespace,
                exceptionWrapper,
                initialSyncMetricsUpdaterOpt,
                batchSize));
  }

  @Override
  public ChangeStreamBatch getNext() throws E {
    return switch (this.state) {
      case OPEN_CURSOR -> this.openCursor();
      case GET_MORE -> this.getMore();
      case CLOSED -> {
        throw new IllegalStateException("getNext() called in CLOSED state");
      }
    };
  }

  private ChangeStreamBatch openCursor() throws E {
    this.ensureState(State.OPEN_CURSOR);

    ChangeStreamResponse response =
        ChangeStreamResponse.fromProxy(
            this.runChangeStreamCommand(
                this.aggregateCommand.toProxy(), ChangeStreamAggregateResponseProxy.class));

    // Assert response batch size is zero if aggregation command set batch size to zero.
    if (this.aggregateCommand.getBatchSize().orElse(1) == 0) {
      Check.checkState(
          response.getBatch().isEmpty(),
          "aggregation response cursor had events even though batchSize 0 was specified");
    }

    this.state = State.GET_MORE;
    this.cursorId = OptionalLong.of(response.getId());

    return ChangeStreamBatch.fromResponse(response);
  }

  private ChangeStreamBatch getMore() throws E {
    this.ensureState(State.GET_MORE);
    checkState(this.cursorId.isPresent(), "cursorId not present when in GET_MORE");

    GetMoreCommand command =
        new GetMoreCommand(
            this.cursorId.getAsLong(),
            this.namespace.getCollectionName(),
            this.batchSize,
            Optional.empty());

    Stopwatch timer = Stopwatch.createStarted();
    ChangeStreamResponse response =
        ChangeStreamResponse.fromProxy(
            this.runChangeStreamCommand(command.toProxy(), ChangeStreamGetMoreResponseProxy.class));
    Duration duration = timer.elapsed();
    this.getMoreTimer.record(duration);
    this.initialSyncMetricsUpdaterOpt.ifPresent(
        metricUpdater -> metricUpdater.getChangeStreamBatchGetMoreTimer().record(duration));

    return ChangeStreamBatch.fromResponse(response);
  }

  private <T> T runChangeStreamCommand(Bson command, Class<T> resultClass) throws E {
    return this.exceptionWrapper.wrapIfThrows(
        () ->
            this.mongoClient
                .getDatabase(this.namespace.getDatabaseName())
                .withCodecRegistry(CodecRegistry.PACKAGE_CODEC_REGISTRY)
                .runCommand(this.refreshingSession.getSession(), command, resultClass));
  }

  @Override
  public void close() {
    this.killCursor();
    this.refreshingSession.close();
    this.state = State.CLOSED;
  }

  private void killCursor() {
    if (this.cursorId.isEmpty()) {
      return;
    }

    KillCursorsCommand command =
        new KillCursorsCommand(this.namespace.getCollectionName(), this.cursorId.getAsLong());

    try {
      this.mongoClient.getDatabase(this.namespace.getDatabaseName()).runCommand(command.toProxy());
    } catch (Exception e) {
      // Ignore any exceptions killing the cursor, we do it as best-effort cleanup.
    }
  }

  private void ensureState(State expected) {
    checkState(this.state == expected, "state must be %s but is %s", expected, this.state);
  }

  private enum State {
    OPEN_CURSOR,
    GET_MORE,
    CLOSED,
  }
}
