package com.xgen.mongot.lifecycle;

import com.xgen.mongot.index.IndexGeneration;
import com.xgen.mongot.index.version.GenerationId;
import com.xgen.mongot.replication.ReplicationManager;
import com.xgen.mongot.util.mongodb.SyncSourceConfig;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * LifecycleManager is in charge of the lifecycle of Indexes - initializing them for serving
 * queries, replicating them from their source, and handling events like the source being dropped.
 */
public interface LifecycleManager {

  /** Starts managing the lifecycle of the supplied Index (including replication). */
  void add(IndexGeneration indexGeneration);

  /** (Re)starts replication for all indexes. */
  void restartReplication();

  /**
   * Stops replication of the index with the supplied id, and drops the index.
   *
   * @param generationId the id of the index to drop
   * @return a future that completes when the index has been dropped. The future will only ever
   *     complete successfully.
   */
  CompletableFuture<Void> dropIndex(GenerationId generationId);

  /**
   * Returns the MongoDb and MongoS ConnectionStrings that this LifecycleManager is replicating
   * from.
   */
  Optional<SyncSourceConfig> getSyncSourceConfig();

  boolean isInitialized();

  /**
   * Stops the LifecycleManager, gracefully shutting down all indexes.
   *
   * @return a future that completes when the LifecycleManager has completed shutting down. The
   *     future will only ever complete successfully.
   */
  CompletableFuture<Void> shutdown();

  /**
   * Shuts down replication for all indexes
   *
   * <p>TODO(CLOUDP-231027): Replace this with a restartReplication() method.
   *
   * @return a future that completes when replication has completed shutting down. The future will
   *     only ever complete successfully.
   */
  CompletableFuture<Void> shutdownReplication();

  /** Shuts down only auto-embedding materialized-view replication. */
  default CompletableFuture<Void> shutdownAutoEmbeddingReplication() {
    return CompletableFuture.completedFuture(null);
  }

  /** Restarts only auto-embedding materialized-view replication using the supplied sync source. */
  default void restartAutoEmbeddingReplication(SyncSourceConfig syncSourceConfig) {}

  /** Get the associated ReplicationManager for the LifecycleManager. Only used for testing. */
  ReplicationManager getReplicationManager();

  /**
   * Updates the syncSource that the ReplicationManager uses to replicate data from. A no-op
   * ReplicationManager is created if pauseReplication is true
   */
  void updateSyncSource(SyncSourceConfig syncSourceConfig);

  /**
   * Return whether the replication is supported. No-op ReplicationManager will always return false
   * and normal ReplicationManager will always return true
   */
  boolean isReplicationSupported();

  /** Return whether auto-embedding materialized-view replication is currently supported. */
  default boolean isAutoEmbeddingReplicationSupported() {
    return true;
  }

  /** Return whether this node runs auto-embedding materialized-view replication to manage. */
  default boolean hasAutoEmbeddingReplication() {
    return false;
  }
}
