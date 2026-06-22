package com.xgen.mongot.replication.mongodb.common;

import java.util.List;
import org.bson.types.ObjectId;

/**
 * Common ReplicationConfig contains all global parameters and overridable parameters used in sub
 * classes.
 */
public abstract sealed class CommonReplicationConfig
    permits MongoDbReplicationConfig, AutoEmbeddingMaterializedViewConfig {

  /** Global Replication Parameters that controls all replication modules and unoverridable. */
  public record GlobalReplicationConfig(
      boolean pauseAllInitialSyncs,
      List<ObjectId> pauseInitialSyncOnIndexIds,
      boolean enableSplitLargeChangeStreamEvents,
      boolean splitLargeChangeStreamEventsForInitialSync,
      List<String> excludedChangestreamFields,
      boolean matchCollectionUuidForUpdateLookup) {}

  public enum Type {
    DEFAULT("", "replication"),
    AUTO_EMBEDDING("autoEmbedding.", "autoembedding");
    public final String metricsNamespacePrefix;
    public final String resourceAttributionSubsystem;

    Type(String metricsNamespacePrefix, String resourceAttributionSubsystem) {
      this.metricsNamespacePrefix = metricsNamespacePrefix;
      this.resourceAttributionSubsystem = resourceAttributionSubsystem;
    }
  }

  /** Boolean field to pause replication for all indexes in initial sync. */
  final boolean pauseAllInitialSyncs;

  /**
   * Whether to enable SplitEventChangeStreamClient wrapper for handling large change stream events
   * that exceed 16MB limit. When enabled, events will be automatically fragmented and reassembled.
   */
  final boolean enableSplitLargeChangeStreamEvents;

  /**
   * Whether to enable support for large change stream events that exceed the 16MB limit during
   * initial sync.
   */
  final boolean splitLargeChangeStreamEventsForInitialSync;

  /**
   * When pauseAllInitialSyncs is set to false, we will pause initial sync for indexes in this list.
   */
  final List<ObjectId> pauseInitialSyncOnIndexIds;

  /** Boolean field to use the matchCollectionUuidForUpdateLookup change stream parameter. */
  final boolean matchCollectionUuidForUpdateLookup;

  /**
   * Fields which we want to exclude from changestream. This is generally metadata that we do not
   * use, but increases change stream event size that may cause events to hit the 16MB limit. Note
   * that this applies to both initial sync and steady state.
   */
  final List<String> excludedChangestreamFields;

  public CommonReplicationConfig(
      boolean pauseAllInitialSyncs,
      List<ObjectId> pauseInitialSyncOnIndexIds,
      boolean enableSplitLargeChangeStreamEvents,
      boolean splitLargeChangeStreamEventsForInitialSync,
      List<String> excludedChangestreamFields,
      boolean matchCollectionUuidForUpdateLookup) {
    this.pauseAllInitialSyncs = pauseAllInitialSyncs;
    this.pauseInitialSyncOnIndexIds = pauseInitialSyncOnIndexIds;
    this.enableSplitLargeChangeStreamEvents = enableSplitLargeChangeStreamEvents;
    this.splitLargeChangeStreamEventsForInitialSync = splitLargeChangeStreamEventsForInitialSync;
    this.excludedChangestreamFields = excludedChangestreamFields;
    this.matchCollectionUuidForUpdateLookup = matchCollectionUuidForUpdateLookup;
  }

  public static GlobalReplicationConfig defaultGlobalReplicationConfig() {
    return new GlobalReplicationConfig(false, List.of(), false, false, List.of(), false);
  }

  public static GlobalReplicationConfig communityDefaultGlobalReplicationConfig() {
    // Enable splitLargeChangeStreamEvents and matchCollectionUuidForUpdateLookup for community
    return new GlobalReplicationConfig(false, List.of(), true, false, List.of(), true);
  }

  // Overridable parameters by subclasses.
  public abstract int getNumConcurrentInitialSyncs();

  public abstract int getNumConcurrentChangeStreams();

  public abstract int getNumIndexingThreads();

  public abstract int getChangeStreamMaxTimeMs();

  public abstract int getChangeStreamCursorMaxTimeSec();

  public abstract int getNumChangeStreamDecodingThreads();

  public abstract int getRequestRateLimitBackoffMs();

  public final boolean getPauseAllInitialSyncs() {
    return this.pauseAllInitialSyncs;
  }

  public final boolean getEnableSplitLargeChangeStreamEvents() {
    return this.enableSplitLargeChangeStreamEvents;
  }

  public final boolean getSplitLargeChangeStreamEventsForInitialSync() {
    return this.splitLargeChangeStreamEventsForInitialSync;
  }

  public final List<ObjectId> getPauseInitialSyncOnIndexIds() {
    return this.pauseInitialSyncOnIndexIds;
  }

  public final boolean getMatchCollectionUuidForUpdateLookup() {
    return this.matchCollectionUuidForUpdateLookup;
  }

  public final List<String> getExcludedChangestreamFields() {
    return this.excludedChangestreamFields;
  }

  public abstract Type getReplicationType();
}
