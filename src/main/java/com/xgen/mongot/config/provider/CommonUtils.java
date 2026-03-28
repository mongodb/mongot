package com.xgen.mongot.config.provider;

import com.google.common.base.Supplier;
import com.xgen.mongot.catalog.IndexCatalog;
import com.xgen.mongot.catalog.InitializedIndexCatalog;
import com.xgen.mongot.config.manager.DefaultConfigManager;
import com.xgen.mongot.cursor.MongotCursorManager;
import com.xgen.mongot.embedding.config.MaterializedViewCollectionMetadataCatalog;
import com.xgen.mongot.embedding.mongodb.MaterializedViewCollectionResolver;
import com.xgen.mongot.embedding.mongodb.common.AutoEmbeddingMongoClient;
import com.xgen.mongot.embedding.mongodb.leasing.DynamicLeaderLeaseManager;
import com.xgen.mongot.embedding.mongodb.leasing.LeaseManager;
import com.xgen.mongot.embedding.mongodb.leasing.LeaseManagerOpsCommands;
import com.xgen.mongot.embedding.mongodb.leasing.StaticLeaderLeaseManager;
import com.xgen.mongot.embedding.providers.EmbeddingServiceManager;
import com.xgen.mongot.featureflag.FeatureFlags;
import com.xgen.mongot.index.IndexFactory;
import com.xgen.mongot.index.autoembedding.MaterializedViewIndexFactory;
import com.xgen.mongot.index.blobstore.BlobstoreSnapshotterManager;
import com.xgen.mongot.lifecycle.DefaultLifecycleManager;
import com.xgen.mongot.lifecycle.LifecycleConfig;
import com.xgen.mongot.metrics.MeterAndFtdcRegistry;
import com.xgen.mongot.monitor.Gate;
import com.xgen.mongot.monitor.ReplicationStateMonitor;
import com.xgen.mongot.replication.ReplicationManagerFactory;
import com.xgen.mongot.replication.mongodb.DurabilityConfig;
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
      Optional<Supplier<EmbeddingServiceManager>> embeddingServiceManagerSupplier) {

    return (Optional<SyncSourceConfig> syncConfig) -> {
      var pauseReplication = replicationStateMonitor.getReplicationGate().isClosed();

      // Create a no-op replication manager factory if replication mode is disabled or replication
      // is set to be shutdown
      Check.checkState(
          !(replicationMode.equals(DefaultConfigManager.ReplicationMode.ENABLE)
              && pauseReplication),
          "replication can not be paused when replication mode is set to be enabled");

      if (pauseReplication) {
        LOG.atWarn()
            .addKeyValue("replicationGate", replicationStateMonitor.getReplicationGate().toString())
            .log("Disk usage exceeded pause threshold, pausing replication on initialization.");
      }

      if (syncConfig.isEmpty()
          || replicationMode.equals(DefaultConfigManager.ReplicationMode.DISABLE)
          || pauseReplication) {

        return MongoDbNoOpReplicationManager.create(
            syncConfig,
            cursorManager,
            indexCatalog,
            initializedIndexCatalog,
            featureFlags,
            meterAndFtdcRegistry.meterRegistry());
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
          embeddingServiceManagerSupplier);
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
      Gate replicationGate) {
    return new DefaultLifecycleManager(
        factory,
        syncConfig,
        initializedIndexCatalog,
        indexFactory,
        snapshotterManager,
        autoEmbeddingMatViewManagerFactory,
        meterRegistry,
        replicationGate,
        lifecycleConfig);
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
          MaterializedViewCollectionMetadataCatalog mvMetadataCatalog,
          AutoEmbeddingMongoClient autoEmbeddingMongoClient) {

    return (Optional<SyncSourceConfig> syncSourceConfig) -> {
      // Create a no-op replication manager factory if replication mode is disabled.
      if (replicationMode.equals(DefaultConfigManager.ReplicationMode.DISABLE)) {
        LOG.atWarn()
            .log(
                "Replication is disabled for auto-embedding, "
                    + "skips creating MaterializedViewManager.");
        return Optional.empty();
      }

      if (replicationMode.equals(DefaultConfigManager.ReplicationMode.DISK_UTILIZATION_BASED)) {
        // Note: DISK_UTILIZATION_BASED mode is treated as ENABLE for auto-embedding because
        // auto-embedding writes to the MongoDB materialized view collection (not local disk),
        // so local disk utilization concerns don't apply.
        // TODO(CLOUDP-360913): Implement customized disk monitor for mat view.
        LOG.atWarn()
            .log(
                "Disk utilization based replication is not supported for auto-embedding, "
                    + "create MaterializedViewManager anyway.");
      }

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
              mvMetadataCatalog,
              autoEmbeddingMongoClient);
      syncSourceConfig.ifPresent(
          syncSource -> {
            matViewManager.updateSyncSource(syncSource);
            matViewManager.setIsReplicationEnabled(true);
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
      AutoEmbeddingMaterializedViewConfig materializedViewConfig) {
    return new MaterializedViewIndexFactory(
        autoEmbeddingMongoClient,
        featureFlags,
        meterAndFtdcRegistry,
        leaseManager,
        collectionResolver,
        materializedViewConfig.getMvWriteRateLimitRps(),
        materializedViewConfig.matViewWriterMaxConnections);
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
      AutoEmbeddingMongoClient autoEmbeddingMongoClient,
      MeterAndFtdcRegistry meterAndFtdcRegistry,
      boolean isAutoEmbeddingViewWriter,
      MaterializedViewCollectionMetadataCatalog mvMetadataCatalog) {
    LOG.info("Auto-embedding leader mode set via config: {}", isAutoEmbeddingViewWriter);
    // hostname is an unused parameter for now, so passing in a placeholder.
    return StaticLeaderLeaseManager.create(
        Check.isPresent(
            autoEmbeddingMongoClient.getLeaseManagerMongoClient(),
            "autoEmbeddingLeaseManagerMongoClient"),
        meterAndFtdcRegistry,
        "localhost",
        isAutoEmbeddingViewWriter,
        mvMetadataCatalog);
  }

  public static MaterializedViewCollectionResolver getMaterializedViewCollectionResolver(
      AutoEmbeddingMongoClient autoEmbeddingMongoClient,
      MaterializedViewCollectionMetadataCatalog metadataCatalog,
      LeaseManager leaseManager,
      AutoEmbeddingMaterializedViewConfig materializedViewConfig) {
    return MaterializedViewCollectionResolver.create(
        autoEmbeddingMongoClient,
        metadataCatalog,
        leaseManager,
        materializedViewConfig);
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
   * @return a DynamicLeaderLeaseManager for dynamic leader election
   */
  public static LeaseManager getDynamicLeaseManager(
      AutoEmbeddingMongoClient autoEmbeddingMongoClient,
      MeterAndFtdcRegistry meterAndFtdcRegistry,
      String hostname,
      MaterializedViewCollectionMetadataCatalog mvMetadataCatalog,
      LeaseManagerOpsCommands opsCommands) {
    LOG.info("Creating DynamicLeaderLeaseManager for dynamic per-index leader election");
    return DynamicLeaderLeaseManager.create(
        autoEmbeddingMongoClient, meterAndFtdcRegistry, hostname, mvMetadataCatalog, opsCommands);
  }
}
