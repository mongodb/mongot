package com.xgen.mongot.replication.mongodb.common;

import static com.xgen.mongot.util.Check.checkState;
import static java.lang.String.format;

import com.google.common.base.Stopwatch;
import com.mongodb.MongoCommandException;
import com.mongodb.MongoException;
import com.mongodb.MongoNamespace;
import com.mongodb.ServerAddress;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.xgen.mongot.index.IndexMetricsUpdater.ReplicationMetricsUpdater.InitialSyncMetrics;
import com.xgen.mongot.metrics.MetricsFactory;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.mongodb.Errors;
import com.xgen.mongot.util.mongodb.GetMoreCommand;
import com.xgen.mongot.util.mongodb.GetMoreResponse;
import com.xgen.mongot.util.mongodb.KillCursorsCommand;
import com.xgen.mongot.util.mongodb.MongoDbReplSetStatus;
import com.xgen.mongot.util.mongodb.serialization.CodecRegistry;
import com.xgen.mongot.util.mongodb.serialization.GetMoreResponseProxy;
import com.xgen.mongot.util.mongodb.serialization.MongoDbInvalidReplStatusFormatException;
import io.micrometer.core.instrument.Timer;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.Callable;
import java.util.function.Function;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.BsonTimestamp;
import org.bson.RawBsonDocument;

public abstract class CollectionScanCommandMongoClient<E extends Exception>
    implements CollectionScanMongoClient<E> {

  protected enum State {
    OPEN_CURSOR,
    GET_MORE,
    CLOSED,
  }

  @FunctionalInterface
  public interface FutureWrapper<E extends Exception> {
    <T> T getOrWrapThrowable(Callable<T> future) throws E;
  }

  /**
   * Checks that the collection name is still the same as specified in the Index Definition or
   * throws {@link E} otherwise.
   */
  @FunctionalInterface
  public interface NamespaceChangeCheck<E extends Exception> {
    void execute(MongoNamespace namespace) throws E;
  }

  protected final MongoNamespace namespace;
  private final Timer getMoreTimer;
  final MongoClient mongoClient;
  final RefreshingClientSession<ClientSession> refreshingSession;
  final FutureWrapper<E> futureWrapper;
  final Function<Exception, E> exceptionWrapper;
  final NamespaceChangeCheck<E> namespaceChangeCheck;
  final Optional<InitialSyncMetrics> initialSyncMetricsUpdaterOpt;
  private final Optional<Integer> collectionScanGetMoreBatchSize;

  OptionalLong cursorId;
  Optional<BsonTimestamp> operationTime;
  Optional<BsonDocument> postBatchResumeToken;
  State state;

  protected CollectionScanCommandMongoClient(
      MongoClient mongoClient,
      RefreshingClientSession<ClientSession> refreshingSession,
      MetricsFactory metricsFactory,
      MongoNamespace namespace,
      NamespaceChangeCheck<E> namespaceChangeCheck,
      FutureWrapper<E> futureWrapper,
      Function<Exception, E> exceptionWrapper,
      Optional<InitialSyncMetrics> initialSyncMetricsUpdaterOpt,
      Optional<Integer> collectionScanGetMoreBatchSize) {
    Check.argNotNull(collectionScanGetMoreBatchSize, "collectionScanGetMoreBatchSize");
    this.mongoClient = mongoClient;
    this.refreshingSession = refreshingSession;
    // TODO(CLOUDP-289914): Remove this getMoreTimer after switching to new one by
    // IndexMetricsUpdater
    this.getMoreTimer = metricsFactory.timerSec("getMoreDurations");
    this.namespace = namespace;
    this.namespaceChangeCheck = namespaceChangeCheck;
    this.futureWrapper = futureWrapper;
    this.exceptionWrapper = exceptionWrapper;
    this.initialSyncMetricsUpdaterOpt = initialSyncMetricsUpdaterOpt;
    this.collectionScanGetMoreBatchSize = collectionScanGetMoreBatchSize;
    this.cursorId = OptionalLong.empty();
    this.operationTime = Optional.empty();
    this.postBatchResumeToken = Optional.empty();
    this.state = State.OPEN_CURSOR;
  }

  @Override
  public boolean hasNext() throws E {
    if (this.state == State.OPEN_CURSOR) {
      // if we haven't sent our initial find command yet, check if the collection exists
      this.namespaceChangeCheck.execute(this.namespace);
    }

    return this.state == State.OPEN_CURSOR || this.state == State.GET_MORE;
  }

  @Override
  public List<RawBsonDocument> getNext() throws E {
    return switch (this.state) {
      case OPEN_CURSOR -> openScan();
      case GET_MORE -> getMore();
      case CLOSED -> Check.unreachable("getNext() on CLOSED");
    };
  }

  @Override
  public BsonTimestamp getMinValidOpTime() throws E {
    // If the session provides an operationTime, use it.
    Optional<BsonTimestamp> sessionOpTime =
        Optional.ofNullable(this.refreshingSession.getSession().getOperationTime());
    if (sessionOpTime.isPresent()) {
      return sessionOpTime.get();
    }

    // For some reason, sometimes the session will not provide an optime (see SERVER-43086). If
    // this is the case look up the lastCommittedOpTime for the mongod. This value must be greater
    // than or equal to the opTime of the session, since the session was used to do a collection
    // scan with readConcern majority on this node.
    try {
      return MongoDbReplSetStatus.getLastCommittedOptime(this.mongoClient);
    } catch (MongoException | MongoDbInvalidReplStatusFormatException e) {
      // If we happen to not have the opTime in the session and we're unable to retrieve the
      // rs.status(), retry the initial sync.
      throw this.exceptionWrapper.apply(e);
    }
  }

  // operationTime of this find command. The time at which this operation began on mongod.
  @Override
  public BsonTimestamp getOperationTime() throws E {
    if (this.operationTime.isPresent()) {
      return this.operationTime.get();
    }

    if (this.state == State.GET_MORE) {
      throw new AssertionError("operation time should always be present in GET_MORE state");
    }
    throw this.exceptionWrapper.apply(new MongoException("operation time missing"));
  }

  @Override
  public void close() {
    this.refreshingSession.close();
    if (this.state != State.CLOSED) {
      this.killCursor();
      this.state = State.CLOSED;
    }
  }

  void ensureState(State expected) {
    checkState(this.state == expected, "state must be %s but is %s", expected, this.state);
  }

  abstract List<RawBsonDocument> openScan() throws E;

  private List<RawBsonDocument> getMore() throws E {
    ensureState(State.GET_MORE);
    checkState(this.cursorId.isPresent(), "cursorId not present when in GET_MORE");

    GetMoreCommand command =
        new GetMoreCommand(
            this.cursorId.getAsLong(),
            this.namespace.getCollectionName(),
            this.collectionScanGetMoreBatchSize,
            Optional.empty());

    Stopwatch timer = Stopwatch.createStarted();
    GetMoreResponse response =
        this.futureWrapper.getOrWrapThrowable(
            () -> {
              try {
                GetMoreResponse getMoreResponse =
                    GetMoreResponse.fromProxy(
                        this.mongoClient
                            .getDatabase(this.namespace.getDatabaseName())
                            .withCodecRegistry(CodecRegistry.PACKAGE_CODEC_REGISTRY)
                            .runCommand(
                                this.refreshingSession.getSession(),
                                command.toProxy(),
                                GetMoreResponseProxy.class));
                checkResponse(getMoreResponse.getPostBatchResumeToken());
                return getMoreResponse;
              } catch (MongoException e) {
                // check if the issue was a rename or drop, otherwise rethrow
                this.namespaceChangeCheck.execute(this.namespace);
                throw e;
              }
            });
    Duration duration = timer.elapsed();
    this.getMoreTimer.record(duration);
    this.initialSyncMetricsUpdaterOpt.ifPresent(
        metricUpdater -> metricUpdater.getCollectionScanBatchGetMoreTimer().record(duration));
    this.postBatchResumeToken = response.getPostBatchResumeToken();

    if (response.getId() == 0L) {
      // When we get here, there are no more results in the collection.  If it were renamed or
      // dropped, our getMore would have failed.
      this.state = State.CLOSED;
    }

    return response.getBatch();
  }

  @Override
  public Optional<BsonDocument> getPostBatchResumeToken() {
    return this.postBatchResumeToken;
  }

  private void killCursor() {
    if (this.cursorId.isEmpty() || this.cursorId.getAsLong() == 0) {
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

  // checking the natural order response to make sure we have valid resume token returned, otherwise
  // throw an MongoCommandException to restart the initial sync with _id scan
  // This step makes mongot fault-tolerant from server bugs
  void checkResponse(Optional<BsonDocument> resumeToken) throws MongoCommandException {
    if (this.postBatchResumeToken.isPresent()) {
      if (resumeToken.isEmpty()) {
        throw getMissingFieldMongoCommandException("postBatchResumeToken");
      }
      if (resumeToken.get().getBinary("$initialSyncId").isNull()) {
        throw getMissingFieldMongoCommandException("initialSyncId");
      }
      if (!resumeToken
          .get()
          .getBinary("$initialSyncId")
          .equals(this.postBatchResumeToken.get().getBinary("$initialSyncId"))) {
        throw InitialSyncException.initialSyncIdMismatch();
      }
    }
  }

  public MongoCommandException getMissingFieldMongoCommandException(String field) {
    return new MongoCommandException(
        new BsonDocument()
            .append("code", new BsonInt32(Errors.IDL_FAILED_TO_PARSE.code))
            .append("codeName", new BsonString(Errors.IDL_FAILED_TO_PARSE.name))
            .append(
                "errmsg",
                new BsonString(format(InitialSyncException.MISSING_FIELD_MESSAGE + " %s", field))),
        new ServerAddress("mongot", 0));
  }
}
