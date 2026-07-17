package com.xgen.mongot.replication.mongodb.common;

import com.google.common.base.Stopwatch;
import com.mongodb.MongoNamespace;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.xgen.mongot.index.IndexMetricsUpdater.ReplicationMetricsUpdater.InitialSyncMetrics;
import com.xgen.mongot.metrics.MetricsFactory;
import com.xgen.mongot.util.mongodb.CollectionScanFindCommand;
import com.xgen.mongot.util.mongodb.FindCommandResponse;
import com.xgen.mongot.util.mongodb.serialization.CodecRegistry;
import com.xgen.mongot.util.mongodb.serialization.FindCommandResponseProxy;
import io.micrometer.core.instrument.Timer;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.function.Function;
import org.bson.RawBsonDocument;

public class FindCommandCollectionScanMongoClient<E extends Exception>
    extends CollectionScanCommandMongoClient<E> {

  private final CollectionScanFindCommand command;
  private final Timer openScanTimer;

  public FindCommandCollectionScanMongoClient(
      CollectionScanFindCommand command,
      MongoClient mongoClient,
      RefreshingClientSession<ClientSession> sessionRefresher,
      MetricsFactory metricsFactory,
      MongoNamespace namespace,
      NamespaceChangeCheck<E> namespaceChangeCheck,
      FutureWrapper<E> futureWrapper,
      Function<Exception, E> exceptionWrapper,
      Optional<InitialSyncMetrics> initialSyncMetricsUpdaterOpt) {
    super(
        mongoClient,
        sessionRefresher,
        metricsFactory,
        namespace,
        namespaceChangeCheck,
        futureWrapper,
        exceptionWrapper,
        initialSyncMetricsUpdaterOpt,
        Optional.empty());
    this.command = command;
    this.openScanTimer = metricsFactory.timerSec("openScanDurations");
  }

  @Override
  List<RawBsonDocument> openScan() throws E {

    ensureState(State.OPEN_CURSOR);
    Stopwatch timer = Stopwatch.createStarted();

    FindCommandResponse response =
        this.futureWrapper.getOrWrapThrowable(
            () ->
                FindCommandResponse.fromProxy(
                    this.mongoClient
                        .getDatabase(this.namespace.getDatabaseName())
                        .withCodecRegistry(CodecRegistry.PACKAGE_CODEC_REGISTRY)
                        .runCommand(
                            this.refreshingSession.getSession(),
                            this.command.toProxy(),
                            FindCommandResponseProxy.class)));

    this.operationTime = Optional.of(response.getOperationTime());
    this.state = State.GET_MORE;
    this.cursorId = OptionalLong.of(response.getId());

    if (response.getId() == 0L) {
      // When we get here, either the collection is empty, or it has been renamed/dropped
      this.state = State.CLOSED;
      namespaceChangeCheck.execute(this.namespace);
    }

    this.openScanTimer.record(timer.elapsed());

    return response.getBatch();
  }
}
