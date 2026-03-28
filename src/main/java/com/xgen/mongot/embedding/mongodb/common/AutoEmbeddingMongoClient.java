package com.xgen.mongot.embedding.mongodb.common;

import com.google.common.annotations.VisibleForTesting;
import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.xgen.mongot.util.mongodb.MongoClientBuilder;
import com.xgen.mongot.util.mongodb.SyncSourceConfig;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.Optional;
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
  private static final int WRITER_MONGO_CLIENT_MAX_CONNECTIONS = 2;
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

  private Optional<SyncSourceConfig> syncSourceConfig;

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
  }

  public AutoEmbeddingMongoClient(
      Optional<SyncSourceConfig> syncSourceConfig, MeterRegistry meterRegistry) {
    this.syncSourceConfig = syncSourceConfig;
    this.meterRegistry = meterRegistry;
    syncSourceConfig.ifPresent(this::updateSyncSource);
  }

  /** Updates SyncSource for mongo clients. */
  public synchronized void updateSyncSource(SyncSourceConfig syncSourceConfig) {
    this.syncSourceConfig = Optional.of(syncSourceConfig);
    var resolverClient = Optional.ofNullable(this.materializedViewResolverMongoClient.get());
    var leaseManagerClient = Optional.ofNullable(this.leaseManagerMongoClient.get());
    var writerClient = Optional.ofNullable(this.materializedViewWriterMongoClient.get());
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
    // Then close old clients.
    resolverClient.ifPresent(MongoClient::close);
    leaseManagerClient.ifPresent(MongoClient::close);
    writerClient.ifPresent(MongoClient::close);
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
    getMaterializedViewResolverMongoClient().ifPresent(MongoClient::close);
    getLeaseManagerMongoClient().ifPresent(MongoClient::close);
    getMaterializedViewWriterMongoClient().ifPresent(MongoClient::close);
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
        .log("Creating MongoClient for MaterializedViewWriter");
    return MongoClientBuilder.buildNonReplicationWithDefaults(
        connectionString,
        "AutoEmbedding Materialized View Writer",
        WRITER_MONGO_CLIENT_MAX_CONNECTIONS,
        sslContext,
        this.meterRegistry);
  }
}
