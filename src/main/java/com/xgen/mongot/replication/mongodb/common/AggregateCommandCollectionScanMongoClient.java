package com.xgen.mongot.replication.mongodb.common;

import com.google.common.base.Stopwatch;
import com.mongodb.MongoNamespace;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.xgen.mongot.index.IndexMetricsUpdater.ReplicationMetricsUpdater.InitialSyncMetrics;
import com.xgen.mongot.metrics.MetricsFactory;
import com.xgen.mongot.util.mongodb.CollectionScanAggregateCommand;
import com.xgen.mongot.util.mongodb.MongoDbDatabase;
import com.xgen.mongot.util.mongodb.MongoDbVersion;
import com.xgen.mongot.util.mongodb.serialization.AggregateResponseProxy;
import com.xgen.mongot.util.mongodb.serialization.CodecRegistry;
import io.micrometer.core.instrument.Timer;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.function.Function;
import org.bson.RawBsonDocument;

public class AggregateCommandCollectionScanMongoClient<E extends Exception>
    extends CollectionScanCommandMongoClient<E> {

  private final CollectionScanAggregateCommand command;
  private final Timer openScanTimer;

  public AggregateCommandCollectionScanMongoClient(
      CollectionScanAggregateCommand command,
      MongoClient mongoClient,
      RefreshingClientSession<ClientSession> sessionRefresher,
      MetricsFactory metricsFactory,
      MongoNamespace namespace,
      NamespaceChangeCheck<E> namespaceChangeCheck,
      FutureWrapper<E> futureWrapper,
      Function<Exception, E> exceptionWrapper,
      Optional<InitialSyncMetrics> initialSyncMetricsUpdaterOpt,
      Optional<Integer> collectionScanGetMoreBatchSize) {
    super(
        mongoClient,
        sessionRefresher,
        metricsFactory,
        namespace,
        namespaceChangeCheck,
        futureWrapper,
        exceptionWrapper,
        initialSyncMetricsUpdaterOpt,
        collectionScanGetMoreBatchSize);
    this.command = command;
    this.openScanTimer = metricsFactory.timerSec("openScanDurations");
  }

  @Override
  List<RawBsonDocument> openScan() throws E {
    ensureState(State.OPEN_CURSOR);
    Stopwatch timer = Stopwatch.createStarted();

    // A DB version check will be applied once before opening the cursor to make sure natural order
    // scan is supported, or an InitialSyncException for a restart with _id scan
    if (this.command.getRequestResumeToken().isPresent()) {
      this.futureWrapper.getOrWrapThrowable(
          () -> {
            if (!isNaturalOrderScanSupported(MongoDbDatabase.getCurrentVersion(this.mongoClient))) {
              throw InitialSyncException.createRequiresResync(
                  new UnsupportedMongoDbVersionException(
                      UnsupportedMongoDbVersionException
                          .UNSUPPORTED_MONGODB_VERSION_FOR_NATURAL_ORDER_MESSAGE),
                  "unsupported mongodb version");
            }
            return true;
          });
    }

    AggregateResponseProxy response =
        this.futureWrapper.getOrWrapThrowable(
            () -> {
              AggregateResponseProxy responseProxy =
                  this.mongoClient
                      .getDatabase(this.namespace.getDatabaseName())
                      .withCodecRegistry(CodecRegistry.PACKAGE_CODEC_REGISTRY)
                      .runCommand(
                          this.refreshingSession.getSession(),
                          this.command.toProxy(),
                          AggregateResponseProxy.class);
              this.state = State.GET_MORE;
              // Checking cursorId and namespaceChangeCheck before postBatchResumeToken, an
              // InitialSyncException.Type.DROPPED should be thrown if a collection is dropped in
              // which case postBatchResumeToken will be empty
              if (responseProxy.getCursor().getId() == 0L) {
                // When we get here, either the collection is empty, or it has been renamed/dropped
                this.state = State.CLOSED;
                this.namespaceChangeCheck.execute(this.namespace);
              }

              if (this.command.getRequestResumeToken().isPresent()
                  && this.command.getRequestResumeToken().get().getValue()
                  && responseProxy.getCursor().getPostResumeToken().isEmpty()) {
                throw getMissingFieldMongoCommandException("postBatchResumeToken");
              }
              checkResponse(responseProxy.getCursor().getPostResumeToken());
              return responseProxy;
            });

    this.operationTime = Optional.of(response.getOperationTime());
    this.cursorId = OptionalLong.of(response.getCursor().getId());
    this.postBatchResumeToken = response.getCursor().getPostResumeToken();
    this.openScanTimer.record(timer.elapsed());

    return response.getCursor().getFirstBatch();
  }

  private static boolean isNaturalOrderScanSupported(Optional<MongoDbVersion> mongoDbVersion) {
    if (mongoDbVersion.isEmpty()) {
      return false;
    }

    MongoDbVersion version = mongoDbVersion.get();
    if (version.major() < 8) {
      return false; // All 7.x.x are invalid
    }
    if (version.major() > 8) {
      return true; // 9.x.x and above are valid
    }
    if (version.minor() >= 3) {
      return true; // 8.3.x and above are valid
    }
    if (version.minor() == 0) {
      return version.patch() >= 14; // only 8.0.14 and above are valid
    }
    if (version.minor() == 1) {
      return false; // All 8.1.x are invalid
    }
    if (version.minor() == 2) {
      return version.patch() >= 1; // Only 8.2.1 and above are invalid
    }
    return false; // fallback
  }
}
