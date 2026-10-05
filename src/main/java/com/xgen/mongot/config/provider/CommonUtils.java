package com.xgen.mongot.config.provider;

import com.google.common.base.Supplier;
import com.xgen.mongot.catalog.IndexCatalog;
import com.xgen.mongot.catalog.InitializedIndexCatalog;
import com.xgen.mongot.config.manager.DefaultConfigManager;
import com.xgen.mongot.cursor.MongotCursorManager;
import com.xgen.mongot.embedding.config.MaterializedViewCollectionMetadataCatalog;
import com.xgen.mongot.embedding.mongodb.MaterializedViewCollectionResolver;
import com.xgen.mongot.embedding.mongodb.common.AutoEmbeddingMongoClient;
import com.xgen.mongot.embedding.mongodb.common.InternalDatabaseResolver;
import com.xgen.mongot.embedding.mongodb.leasing.DynamicLeaderLeaseManager;
import com.xgen.mongot.embedding.mongodb.leasing.LeaseManager;
import com.xgen.mongot.embedding.mongodb.leasing.LeaseManagerOpsCommands;
import com.xgen.mongot.embedding.mongodb.leasing.StaticLeaderLeaseManager;
import com.xgen.mongot.embedding.providers.EmbeddingServiceManager;
import com.xgen.mongot.featureflag.Feature;
import com.xgen.mongot.featureflag.FeatureFlags;
import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlagRegistry;
import com.xgen.mongot.index.IndexFactory;
import com.xgen.mongot.index.autoembedding.MaterializedViewIndexFactory;
import com.xgen.mongot.index.blobstore.BlobstoreSnapshotterManager;
import com.xgen.mongot.lifecycle.DefaultLifecycleManager;
import com.xgen.mongot.lifecycle.LifecycleConfig;
import com.xgen.mongot.metrics.MeterAndFtdcRegistry;
import com.xgen.mongot.metrics.ThreadPoolResourceMetrics;
import com.xgen.mongot.monitor.Gate;
import com.xgen.mongot.monitor.ReplicationStateMonitor;
import com.xgen.mongot.monitor.ToggleGate;
import com.xgen.mongot.replication.ReplicationManagerFactory;
import com.xgen.mongot.replication.mongodb.DurabilityConfig;
import com.xgen.mongot.replication.mongodb.LeaderStatusRegistrar;
import com.xgen.mongot.replication.mongodb.MongoDbNoOpReplicationManager;
import com.xgen.mongot.replication.mongodb.MongoDbReplicationManager;
import com.xgen.mongot.replication.mongodb.autoembedding.AutoEmbeddingMaterializedViewManagerFactory;
import com.xgen.mongot.replication.mongodb.autoembedding.MaterializedViewManager;
import com.xgen.mongot.replication.mongodb.common.AutoEmbeddingMaterializedViewConfig;
import com.xgen.mongot.replication.mongodb.common.MongoDbReplicationConfig;
import com.xgen.mongot.replication.mongodb.initialsync.config.InitialSyncConfig;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.mongodb.SyncSourceConfig;
import io.micrometer.core.instrument.MeterRegistry;
import java.nio.file.Path;
import java.util.Optional;
import java.util.function.BooleanSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CommonUtils {
  private static final Logger LOG = LoggerFactory.getLogger(CommonUtils.class);

  /** Creates ReplicationManagerFactory. */
  public static ReplicationManagerFactory getReplicationManagerFactory(
      Path dataPath,
      MongoDbReplicationConfig replicationConfig,
      InitialSyncConfig initialSyncConfig,
      DurabilityConfig durabilityConfig,
      FeatureFlags featureFlags,
      MongotCursorManager cursorManager,
      IndexCatalog indexCatalog,
      InitializedIndexCatalog initializedIndexCatalog,
      MeterAndFtdcRegistry meterAndFtdcRegistry,
      DefaultConfigManager.ReplicationMode replicationMode,
      ReplicationStateMonitor replicationStateMonitor,
      Optional<Supplier<EmbeddingServiceManager>> embeddingServiceManagerSupplier,
      Optional<LeaderStatusRegistrar> leaderStatusRegistrar) {

    return (Optional<SyncSourceConfig> syncConfig) -> {
      var pauseReplication = replicationStateMonitor.getReplicationGate().isClosed();

      // Create a no-op replication manager factory if replication mode is disabled or replication
      // is set to be shutdown
      Check.checkState(
          !((replicationMode.equals(DefaultConfigManager.ReplicationMode.ENABLE)
                  || replicationMode.equals(
                      DefaultConfigManager.ReplicationMode.MONGOD_DISK_UTILIZATION_BASED))
              && pauseReplication),
          "Lucene replication can not be paused in ENABLE or MONGOD_DISK_UTILIZATION_BASED mode");

      if (pauseReplication) {
        LOG.atWarn()
            .addKeyValue("replicationGate", replicationStateMonitor.getReplicationGate().toString())
            .log("Disk usage exceeded pause threshold, pausing replication on initialization.");
      }

      boolean missingReplicationUris =
          syncConfig.isPresent() && !syncConfig.get().hasReplicationUrisAvailable();

      if (missingReplicationUris) {
        LOG.atInfo()
            .addKeyValue(
                "mongodSingleHostReplicationUri",
                syncConfig
                    .get()
                    .mongodSingleHostReplicationUri
                    .map(info -> info.uri().getHosts())
                    .orElse(null))
            .addKeyValue(
                "mongosSingleHostReplicationUri",
                syncConfig
                    .get()
                    .mongosSingleHostReplicationUri
                    .map(info -> info.uri().getHosts())
                    .orElse(null))
            .addKeyValue("sharded", syncConfig.get().isSharded)
            .log("Single host URIs not set, initializing with MongoDbNoOpReplicationManager.");
      }

      if (syncConfig.isEmpty()
          || missingReplicationUris
          || replicationMode.equals(DefaultConfigManager.ReplicationMode.DISABLE)
          || pauseReplication) {

        return MongoDbNoOpReplicationManager.create(
            syncConfig,
            cursorManager,
            indexCatalog,
            initializedIndexCatalog,
            featureFlags,
            meterAndFtdcRegistry.meterRegistry(),
            meterAndFtdcRegistry.threadPoolResourceBindings());
      }

      return MongoDbReplicationManager.create(
          dataPath,
          syncConfig,
          replicationConfig,
          durabilityConfig,
          initialSyncConfig,
          featureFlags,
          cursorManager,
          indexCatalog,
          initializedIndexCatalog,
          meterAndFtdcRegistry,
          replicationStateMonitor.getInitialSyncGate(),
          embeddingServiceManagerSupplier,
          leaderStatusRegistrar);
    };
  }

  public static DefaultLifecycleManager getLifecycleManager(
      LifecycleConfig lifecycleConfig,
      Optional<SyncSourceConfig> syncConfig,
      ReplicationManagerFactory factory,
      InitializedIndexCatalog initializedIndexCatalog,
      IndexFactory indexFactory,
      Optional<? extends BlobstoreSnapshotterManager> snapshotterManager,
      AutoEmbeddingMaterializedViewManagerFactory autoEmbeddingMatViewManagerFactory,
      MeterRegistry meterRegistry,
      Gate replicationGate,
      FeatureFlags featureFlags,
      ThreadPoolResourceMetrics.Bindings threadPoolResourceBindings) {
    return new DefaultLifecycleManager(
        factory,
        syncConfig,
        initializedIndexCatalog,
        indexFactory,
        snapshotterManager,
        autoEmbeddingMatViewManagerFactory,
        meterRegistry,
        replicationGate,
        lifecycleConfig,
        featureFlags.isEnabled(Feature.LIFECYCLE_ATTRIBUTION_METRICS),
        threadPoolResourceBindings);
  }

  public static DefaultLifecycleManager getLifecycleManager(
      LifecycleConfig lifecycleConfig,
      Optional<SyncSourceConfig> syncConfig,
      ReplicationManagerFactory factory,
      InitializedIndexCatalog initializedIndexCatalog,
      IndexFactory indexFactory,
      Optional<? extends BlobstoreSnapshotterManager> snapshotterManager,
      AutoEmbeddingMaterializedViewManagerFactory autoEmbeddingMatViewManagerFactory,
      MeterRegistry meterRegistry,
      ReplicationStateMonitor replicationStateMonitor,
      FeatureFlags featureFlags,
      ThreadPoolResourceMetrics.Bindings threadPoolResourceBindings) {
    return new DefaultLifecycleManager(
        factory,
        syncConfig,
        initializedIndexCatalog,
        indexFactory,
        snapshotterManager,
        autoEmbeddingMatViewManagerFactory,
        meterRegistry,
        replicationStateMonitor,
        lifecycleConfig,
        featureFlags.isEnabled(Feature.LIFECYCLE_ATTRIBUTION_METRICS),
        threadPoolResourceBindings);
  }

  /**
   * Creates an AutoEmbeddingMaterializedViewManager for Materialized View based auto-embedding
   * index. Leadership for each index is determined dynamically via the LeaseManager.
   */
  public static AutoEmbeddingMaterializedViewManagerFactory
      getAutoEmbeddingMaterializedViewManagerFactory(
          Path dataPath,
          AutoEmbeddingMaterializedViewConfig autoEmbeddingMaterializedViewConfig,
          InitialSyncConfig initialSyncConfig,
          FeatureFlags featureFlags,
          MongotCursorManager cursorManager,
          MeterAndFtdcRegistry meterAndFtdcRegistry,
          DefaultConfigManager.ReplicationMode replicationMode,
          Optional<Supplier<EmbeddingServiceManager>> embeddingServiceManagerSupplier,
          LeaseManager leaseManager,
          IndexCatalog indexCatalog,
          MaterializedViewCollectionMetadataCatalog mvMetadataCatalog,
          AutoEmbeddingMongoClient autoEmbeddingMongoClient,
          DynamicFeatureFlagRegistry dynamicFeatureFlagRegistry) {
    return getAutoEmbeddingMaterializedViewManagerFactory(
        dataPath,
        autoEmbeddingMaterializedViewConfig,
        initialSyncConfig,
        featureFlags,
        cursorManager,
        meterAndFtdcRegistry,
        replicationMode,
        embeddingServiceManagerSupplier,
        leaseManager,
        indexCatalog,
        mvMetadataCatalog,
        autoEmbeddingMongoClient,
        ToggleGate.opened(),
        dynamicFeatureFlagRegistry);
  }

  public static AutoEmbeddingMaterializedViewManagerFactory
      getAutoEmbeddingMaterializedViewManagerFactory(
          Path dataPath,
          AutoEmbeddingMaterializedViewConfig autoEmbeddingMaterializedViewConfig,
          InitialSyncConfig initialSyncConfig,
          FeatureFlags featureFlags,
          MongotCursorManager cursorManager,
          MeterAndFtdcRegistry meterAndFtdcRegistry,
          DefaultConfigManager.ReplicationMode replicationMode,
          Optional<Supplier<EmbeddingServiceManager>> embeddingServiceManagerSupplier,
          LeaseManager leaseManager,
          IndexCatalog indexCatalog,
          MaterializedViewCollectionMetadataCatalog mvMetadataCatalog,
          AutoEmbeddingMongoClient autoEmbeddingMongoClient,
          ReplicationStateMonitor replicationStateMonitor,
          DynamicFeatureFlagRegistry dynamicFeatureFlagRegistry) {
    return getAutoEmbeddingMaterializedViewManagerFactory(
        dataPath,
        autoEmbeddingMaterializedViewConfig,
        initialSyncConfig,
        featureFlags,
        cursorManager,
        meterAndFtdcRegistry,
        replicationMode,
        embeddingServiceManagerSupplier,
        leaseManager,
        indexCatalog,
        mvMetadataCatalog,
        autoEmbeddingMongoClient,
        replicationStateMonitor.getAutoEmbeddingReplicationGate(),
        dynamicFeatureFlagRegistry);
  }

  private static AutoEmbeddingMaterializedViewManagerFactory
      getAutoEmbeddingMaterializedViewManagerFactory(
          Path dataPath,
          AutoEmbeddingMaterializedViewConfig autoEmbeddingMaterializedViewConfig,
          InitialSyncConfig initialSyncConfig,
          FeatureFlags featureFlags,
          MongotCursorManager cursorManager,
          MeterAndFtdcRegistry meterAndFtdcRegistry,
          DefaultConfigManager.ReplicationMode replicationMode,
          Optional<Supplier<EmbeddingServiceManager>> embeddingServiceManagerSupplier,
          LeaseManager leaseManager,
          IndexCatalog indexCatalog,
          MaterializedViewCollectionMetadataCatalog mvMetadataCatalog,
          AutoEmbeddingMongoClient autoEmbeddingMongoClient,
          Gate autoEmbeddingReplicationGate,
          DynamicFeatureFlagRegistry dynamicFeatureFlagRegistry) {
    return (Optional<SyncSourceConfig> syncSourceConfig) -> {
      // Create a no-op replication manager factory if replication mode is disabled.
      if (replicationMode.equals(DefaultConfigManager.ReplicationMode.DISABLE)) {
        LOG.atWarn()
            .log(
                "Replication is disabled for auto-embedding, "
                    + "skips creating MaterializedViewManager.");
        return Optional.empty();
      }

      // Auto-embedding disk modes still create the manager; lifecycle gates pause work.
      var matViewManager =
          MaterializedViewManager.create(
              dataPath,
              autoEmbeddingMaterializedViewConfig,
              initialSyncConfig,
              featureFlags,
              cursorManager,
              embeddingServiceManagerSupplier,
              meterAndFtdcRegistry,
              leaseManager,
              indexCatalog,
              mvMetadataCatalog,
              autoEmbeddingMongoClient,
              dynamicFeatureFlagRegistry);
      syncSourceConfig.ifPresent(
          syncSource -> {
            if (matViewManager.updateSyncSource(syncSource)
                && autoEmbeddingReplicationGate.isOpen()) {
              matViewManager.setIsReplicationEnabled(true);
            }
          });
      return Optional.of(matViewManager);
    };
  }

  /** Creates MaterializedViewIndexFactory from sync source config */
  public static MaterializedViewIndexFactory getMaterializedViewIndexFactory(
      AutoEmbeddingMongoClient autoEmbeddingMongoClient,
      FeatureFlags featureFlags,
      MeterAndFtdcRegistry meterAndFtdcRegistry,
      LeaseManager leaseManager,
      MaterializedViewCollectionResolver collectionResolver,
      InternalDatabaseResolver dbResolver,
      AutoEmbeddingMaterializedViewConfig materializedViewConfig) {
    return new MaterializedViewIndexFactory(
        autoEmbeddingMongoClient,
        featureFlags,
        meterAndFtdcRegistry,
        leaseManager,
        collectionResolver,
        dbResolver,
        materializedViewConfig.getMvWriteRateLimitRps());
  }

  /**
   * Creates a LeaseManager with explicit leader status from config.
   *
   * @param autoEmbeddingMongoClient the mongo client for auto embedding leases
   * @param meterAndFtdcRegistry the meter and FTDC registry
   * @param isAutoEmbeddingViewWriter true if this instance is the auto-embedding view writer
   *     (leader)
   * @return a LeaseManager configured with the specified leader status
   * @deprecated Use {@link #getDynamicLeaseManager} instead for dynamic leader election.
   */
  @Deprecated
  public static LeaseManager getLeaseManager(
      InternalDatabaseResolver dbResolver,
      AutoEmbeddingMongoClient autoEmbeddingMongoClient,
      MeterAndFtdcRegistry meterAndFtdcRegistry,
      boolean isAutoEmbeddingViewWriter,
      MaterializedViewCollectionMetadataCatalog mvMetadataCatalog) {
    LOG.info("Auto-embedding leader mode set via config: {}", isAutoEmbeddingViewWriter);
    return StaticLeaderLeaseManager.create(
        Check.isPresent(
            autoEmbeddingMongoClient.getLeaseManagerMongoClient(),
            "autoEmbeddingLeaseManagerMongoClient"),
        meterAndFtdcRegistry,
        "localhost",
        dbResolver,
        isAutoEmbeddingViewWriter,
        mvMetadataCatalog);
  }

  public static MaterializedViewCollectionResolver getMaterializedViewCollectionResolver(
      InternalDatabaseResolver dbResolver,
      AutoEmbeddingMongoClient autoEmbeddingMongoClient,
      MaterializedViewCollectionMetadataCatalog metadataCatalog,
      LeaseManager leaseManager,
      AutoEmbeddingMaterializedViewConfig materializedViewConfig,
      MeterRegistry meterRegistry) {
    return MaterializedViewCollectionResolver.create(
        dbResolver,
        autoEmbeddingMongoClient,
        metadataCatalog,
        leaseManager,
        materializedViewConfig,
        meterRegistry);
  }

  /**
   * Creates a DynamicLeaderLeaseManager for dynamic per-index leader election.
   *
   * <p>This is used in deployments with multiple mongot instances where leadership is determined
   * dynamically at the index level through lease acquisition and renewal, rather than being
   * statically configured.
   *
   * @param autoEmbeddingMongoClient the mongo client for auto embedding leases
   * @param meterAndFtdcRegistry the meter and FTDC registry
   * @param hostname the hostname of this mongot instance
   * @param mvMetadataCatalog the materialized view collection metadata catalog
   * @param opsCommands ops commands for lease manager
   * @param leaseHeartbeatPartialUpdateEnabled evaluated per heartbeat tick, never cached; wires the
   *     {@link DynamicFeatureFlags#LEASE_HEARTBEAT_PARTIAL_UPDATE} gating of the lease renewal
   * @return a DynamicLeaderLeaseManager for dynamic leader election
   */
  public static LeaseManager getDynamicLeaseManager(
      InternalDatabaseResolver dbResolver,
      AutoEmbeddingMongoClient autoEmbeddingMongoClient,
      MeterAndFtdcRegistry meterAndFtdcRegistry,
      String hostname,
      MaterializedViewCollectionMetadataCatalog mvMetadataCatalog,
      LeaseManagerOpsCommands opsCommands,
      BooleanSupplier leaseHeartbeatPartialUpdateEnabled) {
    LOG.info("Creating DynamicLeaderLeaseManager for dynamic per-index leader election");
    return DynamicLeaderLeaseManager.create(
        autoEmbeddingMongoClient,
        meterAndFtdcRegistry,
        hostname,
        dbResolver,
        mvMetadataCatalog,
        opsCommands,
        leaseHeartbeatPartialUpdateEnabled);
  }
}
