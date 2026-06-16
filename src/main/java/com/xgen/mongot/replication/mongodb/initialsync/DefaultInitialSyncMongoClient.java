package com.xgen.mongot.replication.mongodb.initialsync;

import com.mongodb.MongoNamespace;
import com.mongodb.client.MongoClient;
import com.xgen.mongot.index.IndexMetricsUpdater.ReplicationMetricsUpdater.InitialSyncMetrics;
import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.version.GenerationId;
import com.xgen.mongot.metrics.MetricsFactory;
import com.xgen.mongot.replication.mongodb.common.AggregateCommandCollectionScanMongoClient;
import com.xgen.mongot.replication.mongodb.common.ChangeStreamMongoClient;
import com.xgen.mongot.replication.mongodb.common.CollectionScanMongoClient;
import com.xgen.mongot.replication.mongodb.common.DefaultChangeStreamMongoClient;
import com.xgen.mongot.replication.mongodb.common.DefaultNamespaceResolver;
import com.xgen.mongot.replication.mongodb.common.FindCommandCollectionScanMongoClient;
import com.xgen.mongot.replication.mongodb.common.InitialSyncException;
import com.xgen.mongot.replication.mongodb.common.NamespaceResolutionException;
import com.xgen.mongot.replication.mongodb.common.NamespaceResolver;
import com.xgen.mongot.replication.mongodb.common.SessionRefresher;
import com.xgen.mongot.replication.mongodb.common.SplitEventChangeStreamClient;
import com.xgen.mongot.util.mongodb.ChangeStreamAggregateCommand;
import com.xgen.mongot.util.mongodb.CollectionScanAggregateCommand;
import com.xgen.mongot.util.mongodb.CollectionScanFindCommand;
import com.xgen.mongot.util.mongodb.MongoDbFsync;
import com.xgen.mongot.util.mongodb.MongoDbReplSetStatus;
import com.xgen.mongot.util.mongodb.serialization.MongoDbInvalidReplStatusFormatException;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.Optional;
import org.bson.BsonDocument;
import org.bson.BsonTimestamp;

/**
 * DefaultInitialSyncMongoClient implements InitialSyncMongoClient by interfacing with a MongoDB
 * deployment via a MongoClient.
 */
class DefaultInitialSyncMongoClient implements InitialSyncMongoClient {
  private final MongoClient mongoClient;
  private final SessionRefresher sessionRefresher;
  private final NamespaceResolver namespaceResolver;
  private final MeterRegistry meterRegistry;
  private final String syncSourceHost;
  private final boolean splitLargeChangeStreamEvents;

  DefaultInitialSyncMongoClient(
      MongoClient mongoClient,
      SessionRefresher sessionRefresher,
      MeterRegistry meterRegistry,
      NamespaceResolver namespaceResolver,
      String syncSourceHost,
      boolean splitLargeChangeStreamEvents) {
    this.mongoClient = mongoClient;
    this.sessionRefresher = sessionRefresher;
    this.namespaceResolver = namespaceResolver;
    this.meterRegistry = meterRegistry;
    this.syncSourceHost = syncSourceHost;
    this.splitLargeChangeStreamEvents = splitLargeChangeStreamEvents;
  }

  public static DefaultInitialSyncMongoClient create(
      MongoClient mongoClient,
      SessionRefresher sessionRefresher,
      MeterRegistry meterRegistry,
      String syncSourceHost,
      boolean splitLargeChangeStreamEvents) {
    return new DefaultInitialSyncMongoClient(
        mongoClient,
        sessionRefresher,
        meterRegistry,
        new DefaultNamespaceResolver(mongoClient),
        syncSourceHost,
        splitLargeChangeStreamEvents);
  }

  @Override
  public String getSyncSourceHost() {
    return this.syncSourceHost;
  }

  @Override
  public void ensureCollectionNameUnchanged(
      IndexDefinition indexDefinition, String expectedCollectionName) throws InitialSyncException {
    boolean nameChanged =
        InitialSyncException.wrapIfThrowsChangeStream(
            () ->
                this.namespaceResolver.isCollectionNameChanged(
                    indexDefinition, expectedCollectionName));

    if (nameChanged) {
      throw InitialSyncException.createRequiresResync(
          "collection name changed after cursor was opened");
    }
  }

  @Override
  public BsonTimestamp getMaxValidMajorityReadOptime() throws InitialSyncException {
    return InitialSyncException.wrapIfThrows(
        () -> {
          try {
            return MongoDbReplSetStatus.getReadConcernMajorityOpTime(this.mongoClient);
          } catch (MongoDbInvalidReplStatusFormatException e) {
            // Only wrap invalid format exceptions here, let wrapIfThrows handle the rest.
            throw InitialSyncException.createRequiresResync(e);
          }
        },
        InitialSyncException.Phase.MAIN);
  }

  @Override
  public BsonDocument fsync() {
    return MongoDbFsync.fsync(this.mongoClient);
  }

  @Override
  public String resolveAndUpdateCollectionName(IndexDefinition indexDefinition)
      throws InitialSyncException {
    return InitialSyncException.wrapIfThrows(
        () -> {
          try {
            return this.namespaceResolver.resolveAndUpdateCollectionName(indexDefinition);
          } catch (NamespaceResolutionException e) {
            // The collection does not exist. This exception will reschedule an initial sync
            // when it is handled by ReplicationIndexManager, in case the cluster is sharded and
            // the collection begins to exist on this shard in the future.
            throw InitialSyncException.createDoesNotExist(
                String.format("from NamespaceResolutionException (%s)", e.getMessage()));
          }
        },
        InitialSyncException.Phase.MAIN);
  }

  @Override
  public ChangeStreamMongoClient<InitialSyncException> getChangeStreamMongoClient(
      ChangeStreamAggregateCommand aggregateCommand,
      MongoNamespace namespace,
      InitialSyncMetrics initialSyncMetricsUpdater,
      Optional<Integer> batchSize,
      GenerationId generationId)
      throws InitialSyncException {
    var metricsFactory =
        new MetricsFactory("indexing.initialSyncChangeStream", this.meterRegistry);
    ChangeStreamMongoClient<InitialSyncException> client =
        DefaultChangeStreamMongoClient.createInitialSync(
            aggregateCommand,
            this.mongoClient,
            this.sessionRefresher,
            namespace,
            metricsFactory,
            initialSyncMetricsUpdater,
            batchSize);

    if (this.splitLargeChangeStreamEvents) {
      return new SplitEventChangeStreamClient<>(
          client, InitialSyncException::wrapIfThrowsChangeStream, metricsFactory, generationId);
    }
    return client;
  }

  @Override
  public CollectionScanMongoClient<InitialSyncException> getCollectionFindCommandMongoClient(
      CollectionScanFindCommand findCommand,
      IndexDefinition indexDefinition,
      InitialSyncMetrics initialSyncMetricsUpdater)
      throws InitialSyncException {

    var session = InitialSyncException.wrapIfThrowsCollectionScan(this.mongoClient::startSession);
    var refreshingSession = this.sessionRefresher.register(session);
    var metricsFactory =
        new MetricsFactory("indexing.initialSyncCollectionScan", this.meterRegistry);
    var namespaceChangeCheck =
        new InitialSyncNamespaceChangeCheck(this.namespaceResolver, indexDefinition);

    return new FindCommandCollectionScanMongoClient<>(
        findCommand,
        this.mongoClient,
        refreshingSession,
        metricsFactory,
        indexDefinition.getLastObservedNamespace(),
        namespaceChangeCheck,
        InitialSyncException::wrapIfThrowsCollectionScan,
        InitialSyncException::createRequiresResync,
        Optional.of(initialSyncMetricsUpdater));
  }

  @Override
  public CollectionScanMongoClient<InitialSyncException> getCollectionAggregateCommandMongoClient(
      CollectionScanAggregateCommand aggregateCommand,
      IndexDefinition indexDefinition,
      InitialSyncMetrics initialSyncMetricsUpdater,
      Optional<Integer> collectionScanGetMoreBatchSize)
      throws InitialSyncException {

    var session = InitialSyncException.wrapIfThrowsCollectionScan(this.mongoClient::startSession);
    var refreshingSession = this.sessionRefresher.register(session);
    var metricsFactory =
        new MetricsFactory("indexing.initialSyncCollectionScan", this.meterRegistry);
    var namespaceChangeCheck =
        new InitialSyncNamespaceChangeCheck(this.namespaceResolver, indexDefinition);

    return new AggregateCommandCollectionScanMongoClient<>(
        aggregateCommand,
        this.mongoClient,
        refreshingSession,
        metricsFactory,
        indexDefinition.getLastObservedNamespace(),
        namespaceChangeCheck,
        InitialSyncException::wrapIfThrowsCollectionScan,
        InitialSyncException::createRequiresResync,
        Optional.of(initialSyncMetricsUpdater),
        collectionScanGetMoreBatchSize);
  }

  @Override
  public CollectionScanMongoClient<InitialSyncException> getAutoEmbeddingResyncMongoClient(
      CollectionScanFindCommand findCommand,
      IndexDefinition indexDefinition,
      MongoNamespace namespace,
      InitialSyncMetrics initialSyncMetricsUpdater)
      throws InitialSyncException {

    var session = InitialSyncException.wrapIfThrowsCollectionScan(this.mongoClient::startSession);
    var refreshingSession = this.sessionRefresher.register(session);
    var metricsFactory =
        new MetricsFactory("autoEmbedding.resyncCollectionScan", this.meterRegistry);

    return new FindCommandCollectionScanMongoClient<>(
        findCommand,
        this.mongoClient,
        refreshingSession,
        metricsFactory,
        namespace,
        (ns) -> {}, // no namespace change check needed for auto-embedding resync
        InitialSyncException::wrapIfThrowsCollectionScan,
        InitialSyncException::createRequiresResync,
        Optional.of(initialSyncMetricsUpdater));
  }
}
