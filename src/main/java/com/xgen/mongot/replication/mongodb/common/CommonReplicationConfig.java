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
      List<String> excludedChangestreamFields,
      boolean matchCollectionUuidForUpdateLookup) {}

  /** Boolean field to pause replication for all indexes in initial sync. */
  final boolean pauseAllInitialSyncs;

  /**
   * Whether to enable SplitEventChangeStreamClient wrapper for handling large change stream events
   * that exceed 16MB limit. When enabled, events will be automatically fragmented and reassembled.
   */
  final boolean enableSplitLargeChangeStreamEvents;

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
      List<String> excludedChangestreamFields,
      boolean matchCollectionUuidForUpdateLookup) {
    this.pauseAllInitialSyncs = pauseAllInitialSyncs;
    this.pauseInitialSyncOnIndexIds = pauseInitialSyncOnIndexIds;
    this.enableSplitLargeChangeStreamEvents = enableSplitLargeChangeStreamEvents;
    this.excludedChangestreamFields = excludedChangestreamFields;
    this.matchCollectionUuidForUpdateLookup = matchCollectionUuidForUpdateLookup;
  }

  public static GlobalReplicationConfig defaultGlobalReplicationConfig() {
    return new GlobalReplicationConfig(false, List.of(), false, List.of(), false);
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

  public final List<ObjectId> getPauseInitialSyncOnIndexIds() {
    return this.pauseInitialSyncOnIndexIds;
  }

  public final boolean getMatchCollectionUuidForUpdateLookup() {
    return this.matchCollectionUuidForUpdateLookup;
  }

  public final List<String> getExcludedChangestreamFields() {
    return this.excludedChangestreamFields;
  }
}
