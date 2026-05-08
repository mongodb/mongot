package com.xgen.mongot.embedding.mongodb.common;

import com.google.common.annotations.VisibleForTesting;
import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.xgen.mongot.replication.mongodb.common.AutoEmbeddingMaterializedViewConfig;
import com.xgen.mongot.util.mongodb.MongoClientBuilder;
import com.xgen.mongot.util.mongodb.SyncSourceConfig;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import javax.net.ssl.SSLContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * MongoClient wrapper for all AutoEmbedding MongoClients used for LeaseManager, CollectionResolver
 * and MaterializedViewWriter.
 */
public class AutoEmbeddingMongoClient {
  private static final Logger LOG = LoggerFactory.getLogger(AutoEmbeddingMongoClient.class);
  private static final int RESOLVER_MONGO_CLIENT_MAX_CONNECTIONS = 1;
  private static final int LEASE_MANAGER_MONGO_CLIENT_MAX_CONNECTIONS = 2;

  // Use for createCollection and listCollections for MaterializedView Collections.
  private final AtomicReference<MongoClient> materializedViewResolverMongoClient =
      new AtomicReference<>();
  // Use for MaterializedViewWriter for bulkWrite and dropCollection.
  private final AtomicReference<MongoClient> materializedViewWriterMongoClient =
      new AtomicReference<>();
  // Use for LeaseManager for managing leases and metadata.
  private final AtomicReference<MongoClient> leaseManagerMongoClient = new AtomicReference<>();
  private final MeterRegistry meterRegistry;

  /**
   * Set to true by {@link #close()} so callers (e.g. retry loops in
   * {@link com.xgen.mongot.embedding.utils.MongoClientOperationExecutor}) can abort in-flight work
   * during mongot shutdown rather than burning their full retry budget against torn-down clients.
   */
  private final AtomicBoolean closed = new AtomicBoolean(false);

  private Optional<SyncSourceConfig> syncSourceConfig;

  private final long materializedViewWriterSocketTimeoutMs;
  private final int matViewWriterMaxConnections;

  @VisibleForTesting
  public AutoEmbeddingMongoClient(
      SyncSourceConfig syncSourceConfig,
      MongoClient materializedViewResolverMongoClient,
      MongoClient leaseManagerMongoClient,
      MongoClient materializedViewWriterMongoClient,
      MeterRegistry meterRegistry) {
    this.syncSourceConfig = Optional.of(syncSourceConfig);
    this.materializedViewResolverMongoClient.set(materializedViewResolverMongoClient);
    this.leaseManagerMongoClient.set(leaseManagerMongoClient);
    this.materializedViewWriterMongoClient.set(materializedViewWriterMongoClient);
    this.meterRegistry = meterRegistry;
    var defaults = AutoEmbeddingMaterializedViewConfig.getDefault();
    this.materializedViewWriterSocketTimeoutMs = defaults.materializedViewWriterSocketTimeoutMs;
    this.matViewWriterMaxConnections = defaults.matViewWriterMaxConnections;
  }

  public AutoEmbeddingMongoClient(
      Optional<SyncSourceConfig> syncSourceConfig,
      MeterRegistry meterRegistry,
      AutoEmbeddingMaterializedViewConfig autoEmbeddingMaterializedViewConfig) {
    this.meterRegistry = meterRegistry;
    this.materializedViewWriterSocketTimeoutMs =
        autoEmbeddingMaterializedViewConfig.materializedViewWriterSocketTimeoutMs;
    this.matViewWriterMaxConnections =
        autoEmbeddingMaterializedViewConfig.matViewWriterMaxConnections;
    this.syncSourceConfig = syncSourceConfig;
    this.syncSourceConfig.ifPresent(this::initClients);
  }

  /** Updates SyncSource for mongo clients. */
  public synchronized void updateSyncSource(SyncSourceConfig syncSourceConfig) {
    // Only recreate clients when the cluster URI changes. initClients() exclusively uses
    // mongodClusterReadWriteUri to build all three clients, so that is the only field we need
    // to compare. Single-host replication URIs can flip between empty and present (e.g. the
    // startup NoOp cycle in community) without affecting the cluster URI, and recreating clients
    // in that case would close connections still held by collaborators such as
    // StaticLeaderLeaseManager.
    if (this.syncSourceConfig.isPresent()
        && this.syncSourceConfig
            .get()
            .mongodClusterReadWriteUri
            .equals(syncSourceConfig.mongodClusterReadWriteUri)) {
      this.syncSourceConfig = Optional.of(syncSourceConfig);
      return;
    }
    this.syncSourceConfig = Optional.of(syncSourceConfig);
    var resolverClient = Optional.ofNullable(this.materializedViewResolverMongoClient.get());
    var leaseManagerClient = Optional.ofNullable(this.leaseManagerMongoClient.get());
    var writerClient = Optional.ofNullable(this.materializedViewWriterMongoClient.get());

    initClients(syncSourceConfig);

    // Close old clients after new ones are in place.
    resolverClient.ifPresent(MongoClient::close);
    leaseManagerClient.ifPresent(MongoClient::close);
    writerClient.ifPresent(MongoClient::close);
  }

  private synchronized void initClients(SyncSourceConfig syncSourceConfig) {
    // We connect to the mongod endpoint directly instead of using mongos, even for sharded
    // clusters. This is to ensure all MV/lease operations happen at a shard-local level rather than
    // a global cluster level.
    // We use mongodClusterReadWriteUri instead of mongodUri because mongodUri is a direct
    // connection to a specific node (often a secondary), while mongodClusterReadWriteUri contains
    // all replica set members and allows the driver to route to the primary. This is required for
    // LINEARIZABLE read concern and write operations.
    var connectionInfo = syncSourceConfig.mongodClusterReadWriteUri;
    // Update sync source first
    this.materializedViewResolverMongoClient.set(
        createMaterializedViewResolverMongoClient(
            connectionInfo.uri(), connectionInfo.sslContext()));
    this.leaseManagerMongoClient.set(
        createLeaseManagerMongoClient(connectionInfo.uri(), connectionInfo.sslContext()));
    this.materializedViewWriterMongoClient.set(
        createMaterializedViewWriterMongoClient(connectionInfo.uri(), connectionInfo.sslContext()));
  }

  public Optional<MongoClient> getMaterializedViewResolverMongoClient() {
    return Optional.ofNullable(this.materializedViewResolverMongoClient.get());
  }

  public Optional<MongoClient> getLeaseManagerMongoClient() {
    return Optional.ofNullable(this.leaseManagerMongoClient.get());
  }

  public Optional<MongoClient> getMaterializedViewWriterMongoClient() {
    return Optional.ofNullable(this.materializedViewWriterMongoClient.get());
  }

  /** Closes all mongo clients, only triggered by ConfigManager::close */
  public void close() {
    this.closed.set(true);
    getMaterializedViewResolverMongoClient().ifPresent(MongoClient::close);
    getLeaseManagerMongoClient().ifPresent(MongoClient::close);
    getMaterializedViewWriterMongoClient().ifPresent(MongoClient::close);
  }

  /** Returns true once {@link #close()} has been invoked. */
  public boolean isClosed() {
    return this.closed.get();
  }

  public Optional<SyncSourceConfig> getSyncSourceConfig() {
    return this.syncSourceConfig;
  }

  private MongoClient createLeaseManagerMongoClient(
      ConnectionString connectionString, Optional<SSLContext> sslContext) {
    LOG.atInfo()
        .addKeyValue("hosts", connectionString.getHosts())
        .addKeyValue("replicaSet", connectionString.getRequiredReplicaSetName())
        .log("Creating MongoClient for DynamicLeaderLeaseManager");
    return MongoClientBuilder.buildNonReplicationWithDefaults(
        connectionString,
        "Dynamic Lease Manager mongo client",
        LEASE_MANAGER_MONGO_CLIENT_MAX_CONNECTIONS,
        sslContext,
        this.meterRegistry);
  }

  private MongoClient createMaterializedViewResolverMongoClient(
      ConnectionString connectionString, Optional<SSLContext> sslContext) {
    LOG.atInfo()
        .addKeyValue("hosts", connectionString.getHosts())
        .log("Creating MongoClient for MaterializedViewCollectionResolver");
    return MongoClientBuilder.buildNonReplicationWithDefaults(
        connectionString,
        "AutoEmbedding Materialized View Collection Resolver",
        RESOLVER_MONGO_CLIENT_MAX_CONNECTIONS,
        sslContext,
        this.meterRegistry);
  }

  private MongoClient createMaterializedViewWriterMongoClient(
      ConnectionString connectionString, Optional<SSLContext> sslContext) {
    LOG.atInfo()
        .addKeyValue("hosts", connectionString.getHosts())
        .addKeyValue("socketTimeoutSeconds", this.materializedViewWriterSocketTimeoutMs / 1000)
        .addKeyValue("maxConnections", this.matViewWriterMaxConnections)
        .log("Creating MongoClient for MaterializedViewWriter");
    return MongoClientBuilder.builder(connectionString, this.meterRegistry)
        .sslContext(sslContext)
        .description("AutoEmbedding Materialized View Writer")
        .maxConnections(this.matViewWriterMaxConnections)
        .socketTimeoutMs(Math.toIntExact(this.materializedViewWriterSocketTimeoutMs))
        .buildNonReplicationClient();
  }
}
