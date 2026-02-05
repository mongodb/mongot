package com.xgen.testing.mongot.replication.mongodb.common;

import com.xgen.mongot.replication.mongodb.common.CommonReplicationConfig;
import com.xgen.mongot.replication.mongodb.common.MongoDbReplicationConfig;
import com.xgen.mongot.util.Runtime;
import java.util.List;
import java.util.Optional;
import org.bson.types.ObjectId;

public class MongoDbReplicationConfigBuilder {
  private Runtime runtime = Runtime.INSTANCE;
  private Optional<Integer> optionalNumConcurrentInitialSyncs = Optional.empty();
  private Optional<Integer> optionalNumConcurrentChangeStreams = Optional.empty();
  private Optional<Integer> optionalNumIndexingThreads = Optional.empty();
  private Optional<Integer> optionalChangeStreamMaxTimeMs = Optional.empty();
  private Optional<Integer> optionalNumConcurrentSynonymSyncs = Optional.empty();
  private Optional<Integer> optionalChangeStreamCursorMaxTimeSec = Optional.empty();
  private Optional<Integer> optionalNumChangeStreamDecodingThreads = Optional.empty();
  private Optional<Boolean> enableSteadyStateChangeStreamProjection = Optional.empty();
  private Optional<Boolean> optionalPauseAllInitialSyncs = Optional.empty();
  private Optional<List<ObjectId>> optionalPauseInitialSyncOnIndexIds = Optional.empty();
  private Optional<Integer> optionalMaxInFlightEmbeddingGetMores = Optional.empty();
  private Optional<Integer> optionalMaxConcurrentEmbeddingInitialSyncs = Optional.empty();
  private Optional<Integer> embeddingGetMoreBatchSize = Optional.empty();
  private List<String> excludedChangestreamFields = List.of();
  private Optional<Boolean> optionalMatchCollectionUuidForUpdateLookup = Optional.empty();
  private Optional<Boolean> enableSplitLargeChangeStreamEvents = Optional.empty();
  private Optional<Integer> optionalRequestRateLimitBackoffMs = Optional.empty();

  public static MongoDbReplicationConfigBuilder builder() {
    return new MongoDbReplicationConfigBuilder();
  }

  public MongoDbReplicationConfigBuilder runtime(Runtime runtime) {
    this.runtime = runtime;
    return this;
  }

  public MongoDbReplicationConfigBuilder optionalNumConcurrentInitialSyncs(
      Optional<Integer> optionalNumConcurrentInitialSyncs) {
    this.optionalNumConcurrentInitialSyncs = optionalNumConcurrentInitialSyncs;
    return this;
  }

  public MongoDbReplicationConfigBuilder optionalNumConcurrentChangeStreams(
      Optional<Integer> optionalNumConcurrentChangeStreams) {
    this.optionalNumConcurrentChangeStreams = optionalNumConcurrentChangeStreams;
    return this;
  }

  public MongoDbReplicationConfigBuilder optionalNumIndexingThreads(
      Optional<Integer> optionalNumIndexingThreads) {
    this.optionalNumIndexingThreads = optionalNumIndexingThreads;
    return this;
  }

  public MongoDbReplicationConfigBuilder optionalChangeStreamMaxTimeMs(
      Optional<Integer> optionalChangeStreamMaxTimeMs) {
    this.optionalChangeStreamMaxTimeMs = optionalChangeStreamMaxTimeMs;
    return this;
  }

  public MongoDbReplicationConfigBuilder optionalNumConcurrentSynonymSyncs(
      Optional<Integer> optionalNumConcurrentSynonymSyncs) {
    this.optionalNumConcurrentSynonymSyncs = optionalNumConcurrentSynonymSyncs;
    return this;
  }

  public MongoDbReplicationConfigBuilder optionalChangeStreamCursorMaxTimeSec(
      Optional<Integer> optionalChangeStreamCursorMaxTimeSec) {
    this.optionalChangeStreamCursorMaxTimeSec = optionalChangeStreamCursorMaxTimeSec;
    return this;
  }

  public MongoDbReplicationConfigBuilder optionalNumChangeStreamDecodingThreads(
      Optional<Integer> optionalNumChangeStreamDecodingThreads) {
    this.optionalNumChangeStreamDecodingThreads = optionalNumChangeStreamDecodingThreads;
    return this;
  }

  public MongoDbReplicationConfigBuilder enableSteadyStateChangeStreamProjection(
      Optional<Boolean> enableSteadyStateChangeStreamProjection) {
    this.enableSteadyStateChangeStreamProjection = enableSteadyStateChangeStreamProjection;
    return this;
  }

  public MongoDbReplicationConfigBuilder pauseAllInitialSyncs(
      Optional<Boolean> optionalPauseAllInitialSyncs) {
    this.optionalPauseAllInitialSyncs = optionalPauseAllInitialSyncs;
    return this;
  }

  public MongoDbReplicationConfigBuilder optionalPauseInitialSyncOnIndexIds(
      Optional<List<ObjectId>> optionalPauseInitialSyncOnIndexIds) {
    this.optionalPauseInitialSyncOnIndexIds = optionalPauseInitialSyncOnIndexIds;
    return this;
  }

  public MongoDbReplicationConfigBuilder optionalMaxInFlightEmbeddingGetMores(
      Optional<Integer> optionalMaxInFlightEmbeddingGetMores) {
    this.optionalMaxInFlightEmbeddingGetMores = optionalMaxInFlightEmbeddingGetMores;
    return this;
  }

  public MongoDbReplicationConfigBuilder optionalMaxConcurrentEmbeddingInitialSyncs(
      Optional<Integer> optionalMaxConcurrentEmbeddingInitialSyncs) {
    this.optionalMaxConcurrentEmbeddingInitialSyncs = optionalMaxConcurrentEmbeddingInitialSyncs;
    return this;
  }

  public MongoDbReplicationConfigBuilder embeddingGetMoreBatchSize(
      Optional<Integer> embeddingGetMoreBatchSize) {
    this.embeddingGetMoreBatchSize = embeddingGetMoreBatchSize;
    return this;
  }

  public MongoDbReplicationConfigBuilder excludedChangestreamFields(
      List<String> excludedChangestreamFields) {
    this.excludedChangestreamFields = excludedChangestreamFields;
    return this;
  }

  public MongoDbReplicationConfigBuilder optionalMatchCollectionUuidForUpdateLookup(
      Optional<Boolean> optionalMatchCollectionUuidForUpdateLookup) {
    this.optionalMatchCollectionUuidForUpdateLookup = optionalMatchCollectionUuidForUpdateLookup;
    return this;
  }

  public MongoDbReplicationConfigBuilder enableSplitLargeChangeStreamEvents(
      Optional<Boolean> enableSplitLargeChangeStreamEvents) {
    this.enableSplitLargeChangeStreamEvents = enableSplitLargeChangeStreamEvents;
    return this;
  }

  public MongoDbReplicationConfigBuilder optionalRequestRateLimitBackoffMs(
      Optional<Integer> requestRateLimitBackoffMs) {
    this.optionalRequestRateLimitBackoffMs = requestRateLimitBackoffMs;
    return this;
  }

  public MongoDbReplicationConfig build() {
    return MongoDbReplicationConfig.create(
        this.runtime,
        new CommonReplicationConfig.GlobalReplicationConfig(
            this.optionalPauseAllInitialSyncs.orElse(false),
            this.optionalPauseInitialSyncOnIndexIds.orElse(List.of()),
            this.enableSplitLargeChangeStreamEvents.orElse(false),
            this.excludedChangestreamFields,
            this.optionalMatchCollectionUuidForUpdateLookup.orElse(false)),
        this.optionalNumConcurrentInitialSyncs,
        this.optionalNumConcurrentChangeStreams,
        this.optionalNumIndexingThreads,
        this.optionalChangeStreamMaxTimeMs,
        this.optionalNumConcurrentSynonymSyncs,
        this.optionalChangeStreamCursorMaxTimeSec,
        this.optionalNumChangeStreamDecodingThreads,
        this.enableSteadyStateChangeStreamProjection,
        this.optionalMaxInFlightEmbeddingGetMores,
        this.optionalMaxConcurrentEmbeddingInitialSyncs,
        this.embeddingGetMoreBatchSize,
        this.optionalRequestRateLimitBackoffMs);
  }
}
