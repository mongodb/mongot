package com.xgen.mongot.config.provider;

import com.google.common.base.Supplier;
import com.xgen.mongot.catalog.IndexCatalog;
import com.xgen.mongot.catalog.InitializedIndexCatalog;
import com.xgen.mongot.config.manager.DefaultConfigManager;
import com.xgen.mongot.cursor.MongotCursorManager;
import com.xgen.mongot.embedding.mongodb.leasing.LeaseManager;
import com.xgen.mongot.embedding.mongodb.leasing.StaticLeaderLeaseManager;
import com.xgen.mongot.embedding.providers.EmbeddingServiceManager;
import com.xgen.mongot.featureflag.FeatureFlags;
import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlagRegistry;
import com.xgen.mongot.index.IndexFactory;
import com.xgen.mongot.index.analyzer.AnalyzerRegistry;
import com.xgen.mongot.index.autoembedding.MaterializedViewIndexFactory;
import com.xgen.mongot.index.blobstore.BlobstoreSnapshotterManager;
import com.xgen.mongot.index.lucene.LuceneGlobalSettings;
import com.xgen.mongot.index.lucene.LuceneIndexFactory;
import com.xgen.mongot.index.lucene.blobstore.LuceneIndexSnapshotterManager;
import com.xgen.mongot.index.lucene.config.LuceneConfig;
import com.xgen.mongot.index.lucene.directory.EnvironmentVariantPerfConfig;
import com.xgen.mongot.lifecycle.DefaultLifecycleManager;
import com.xgen.mongot.lifecycle.LifecycleConfig;
import com.xgen.mongot.metrics.MeterAndFtdcRegistry;
import com.xgen.mongot.monitor.DiskMonitor;
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
import com.xgen.mongot.util.Crash;
import com.xgen.mongot.util.mongodb.SyncSourceConfig;
import io.micrometer.core.instrument.MeterRegistry;
import java.nio.file.Path;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CommonUtils {
  private static final Logger LOG = LoggerFactory.getLogger(CommonUtils.class);

  public static IndexFactory getIndexFactory(
      LuceneConfig luceneConfig,
      FeatureFlags featureFlags,
      DynamicFeatureFlagRegistry dynamicFeatureFlagRegistry,
      EnvironmentVariantPerfConfig environmentVariantPerfConfig,
      MeterAndFtdcRegistry meterAndFtdcRegistry,
      Optional<LuceneIndexSnapshotterManager> snapshotterManager,
      DiskMonitor diskMonitor) {
    LuceneGlobalSettings.apply(luceneConfig);

    var analyzerRegistryFactory =
        Crash.because("failed to get AnalyzerRegistry.Factory instance")
            .ifThrows(AnalyzerRegistry::factory);

    return Crash.because("failed to get LuceneIndexFactory instance")
        .ifThrows(
            () ->
                LuceneIndexFactory.fromConfig(
                    luceneConfig,
                    featureFlags,
                    dynamicFeatureFlagRegistry,
                    environmentVariantPerfConfig,
                    meterAndFtdcRegistry,
                    snapshotterManager,
                    analyzerRegistryFactory,
                    diskMonitor));
  }

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
          LeaseManager leaseManager) {

    return (Optional<SyncSourceConfig> syncConfig) -> {
      // Create a no-op replication manager factory if replication mode is disabled or replication
      // is set to be shutdown
      if (syncConfig.isEmpty()
          || replicationMode.equals(DefaultConfigManager.ReplicationMode.DISABLE)) {
        return Optional.empty();
      }

      Check.checkState(
          !replicationMode.equals(DefaultConfigManager.ReplicationMode.DISK_UTILIZATION_BASED),
          "Materialized View Manager doesn't support disk utilization based processing.");

      return Optional.of(
          MaterializedViewManager.create(
              dataPath,
              // TODO(CLOUDP-360542): Support MaterializedViewManager without syncConfig in Atlas.
              Check.isPresent(syncConfig, "syncConfig"),
              autoEmbeddingMaterializedViewConfig,
              initialSyncConfig,
              featureFlags,
              cursorManager,
              embeddingServiceManagerSupplier,
              meterAndFtdcRegistry,
              leaseManager));
    };
  }

  /** Creates MaterializedViewIndexFactory from sync source config */
  public static MaterializedViewIndexFactory getMaterializedViewIndexFactory(
      SyncSourceConfig syncSourceConfig,
      FeatureFlags featureFlags,
      MeterAndFtdcRegistry meterAndFtdcRegistry,
      LeaseManager leaseManager) {
    return new MaterializedViewIndexFactory(
        syncSourceConfig, featureFlags, meterAndFtdcRegistry, leaseManager);
  }

  /**
   * Creates a LeaseManager with explicit leader status from config.
   *
   * @param syncSourceConfig the sync source configuration
   * @param meterAndFtdcRegistry the meter and FTDC registry
   * @param isAutoEmbeddingViewWriter true if this instance is the auto-embedding view writer
   *     (leader)
   * @return a LeaseManager configured with the specified leader status
   */
  public static LeaseManager getLeaseManager(
      SyncSourceConfig syncSourceConfig,
      MeterAndFtdcRegistry meterAndFtdcRegistry,
      boolean isAutoEmbeddingViewWriter) {
    LOG.info("Auto-embedding leader mode set via config: {}", isAutoEmbeddingViewWriter);
    // hostname is an unused parameter for now, so passing in a placeholder.
    return StaticLeaderLeaseManager.create(
        syncSourceConfig, meterAndFtdcRegistry, "localhost", isAutoEmbeddingViewWriter);
  }
}
