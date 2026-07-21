package com.xgen.mongot.replication.mongodb.steadystate.changestream;

import com.xgen.mongot.replication.mongodb.common.AutoEmbeddingMaterializedViewConfig;
import com.xgen.mongot.replication.mongodb.common.CommonReplicationConfig;
import com.xgen.mongot.replication.mongodb.common.MongoDbReplicationConfig;
import com.xgen.mongot.replication.mongodb.common.SteadyStateSchedulingMethod;
import com.xgen.mongot.util.Check;
import java.util.List;
import java.util.Optional;

public class SteadyStateReplicationConfig {

  private final int numConcurrentChangeStreams;
  private final int changeStreamQueryMaxTimeMs;
  private final int changeStreamCursorMaxTimeSec;
  private final Optional<Boolean> enableChangeStreamProjection;
  private final int maxInFlightEmbeddingGetMores;
  private final Optional<Integer> embeddingGetMoreBatchSize;
  private final List<String> excludedChangestreamFields;
  private final boolean matchCollectionUuidForUpdateLookup;
  private final boolean enableSplitLargeChangeStreamEvents;
  private final Optional<AutoEmbeddingMaterializedViewConfig> autoEmbeddingMaterializedViewConfig;
  private final int changeStreamInactivityThresholdSec;
  private final CommonReplicationConfig.Type replicationType;
  private final SteadyStateSchedulingMethod steadyStateSchedulingMethod;
  private final int steadyStatePriorityRatio;
  private final int steadyStatePriorityOptimeThresholdSeconds;

  private SteadyStateReplicationConfig(
      int numConcurrentChangeStreams,
      int changeStreamQueryMaxTimeMs,
      int changeStreamCursorMaxTimeSec,
      Optional<Boolean> enableChangeStreamProjection,
      int maxInFlightEmbeddingGetMores,
      Optional<Integer> embeddingGetMoreBatchSize,
      List<String> excludedChangestreamFields,
      boolean matchCollectionUuidForUpdateLookup,
      boolean enableSplitLargeChangeStreamEvents,
      Optional<AutoEmbeddingMaterializedViewConfig> autoEmbeddingMaterializedViewConfig,
      int changeStreamInactivityThresholdSec,
      CommonReplicationConfig.Type replicationType,
      SteadyStateSchedulingMethod steadyStateSchedulingMethod,
      int steadyStatePriorityRatio,
      int steadyStatePriorityOptimeThresholdSeconds) {
    this.numConcurrentChangeStreams = numConcurrentChangeStreams;
    this.changeStreamQueryMaxTimeMs = changeStreamQueryMaxTimeMs;
    this.changeStreamCursorMaxTimeSec = changeStreamCursorMaxTimeSec;
    this.enableChangeStreamProjection = enableChangeStreamProjection;
    this.maxInFlightEmbeddingGetMores = maxInFlightEmbeddingGetMores;
    this.embeddingGetMoreBatchSize = embeddingGetMoreBatchSize;
    this.excludedChangestreamFields = excludedChangestreamFields;
    this.matchCollectionUuidForUpdateLookup = matchCollectionUuidForUpdateLookup;
    this.enableSplitLargeChangeStreamEvents = enableSplitLargeChangeStreamEvents;
    this.autoEmbeddingMaterializedViewConfig = autoEmbeddingMaterializedViewConfig;
    this.changeStreamInactivityThresholdSec = changeStreamInactivityThresholdSec;
    this.replicationType = replicationType;
    this.steadyStateSchedulingMethod = steadyStateSchedulingMethod;
    this.steadyStatePriorityRatio = steadyStatePriorityRatio;
    this.steadyStatePriorityOptimeThresholdSeconds = steadyStatePriorityOptimeThresholdSeconds;
  }

  public static SteadyStateReplicationConfig.Builder builder() {
    return new SteadyStateReplicationConfig.Builder();
  }

  public int getNumConcurrentChangeStreams() {
    return this.numConcurrentChangeStreams;
  }

  public int getChangeStreamQueryMaxTimeMs() {
    return this.changeStreamQueryMaxTimeMs;
  }

  public int getChangeStreamCursorMaxTimeSec() {
    return this.changeStreamCursorMaxTimeSec;
  }

  public Optional<Boolean> getEnableChangeStreamProjection() {
    return this.enableChangeStreamProjection;
  }

  public int getMaxInFlightEmbeddingGetMores() {
    return this.maxInFlightEmbeddingGetMores;
  }

  public Optional<Integer> getEmbeddingGetMoreBatchSize() {
    return this.embeddingGetMoreBatchSize;
  }

  public List<String> getExcludedChangestreamFields() {
    return this.excludedChangestreamFields;
  }

  public boolean getMatchCollectionUuidForUpdateLookup() {
    return this.matchCollectionUuidForUpdateLookup;
  }

  public boolean getEnableSplitLargeChangeStreamEvents() {
    return this.enableSplitLargeChangeStreamEvents;
  }

  public Optional<AutoEmbeddingMaterializedViewConfig> getAutoEmbeddingMaterializedViewConfig() {
    return this.autoEmbeddingMaterializedViewConfig;
  }

  public int getChangeStreamInactivityThresholdSec() {
    return this.changeStreamInactivityThresholdSec;
  }

  public CommonReplicationConfig.Type getReplicationType() {
    return this.replicationType;
  }

  public SteadyStateSchedulingMethod getSteadyStateSchedulingMethod() {
    return this.steadyStateSchedulingMethod;
  }

  public int getSteadyStatePriorityRatio() {
    return this.steadyStatePriorityRatio;
  }

  public int getSteadyStatePriorityOptimeThresholdSeconds() {
    return this.steadyStatePriorityOptimeThresholdSeconds;
  }

  public static class Builder {
    private Optional<Integer> numConcurrentChangeStreams;
    private Optional<Integer> changeStreamQueryMaxTimeMs;
    private Optional<Integer> changeStreamCursorMaxTimeSec;
    private Optional<Boolean> enableChangeStreamProjection;
    private Optional<Integer> maxInFlightEmbeddingGetMores;
    private Optional<Integer> embeddingGetMoreBatchSize;
    private List<String> excludedChangestreamFields;
    private boolean matchCollectionUuidForUpdateLookup;
    private boolean enableSplitLargeChangeStreamEvents;
    private Optional<AutoEmbeddingMaterializedViewConfig> autoEmbeddingMaterializedViewConfig;
    private int changeStreamInactivityThresholdSec;
    private CommonReplicationConfig.Type replicationType;
    private SteadyStateSchedulingMethod steadyStateSchedulingMethod;
    private int steadyStatePriorityRatio;
    private int steadyStatePriorityOptimeThresholdSeconds;

    public Builder() {
      this.numConcurrentChangeStreams = Optional.empty();
      this.changeStreamQueryMaxTimeMs = Optional.empty();
      this.changeStreamCursorMaxTimeSec = Optional.empty();
      this.enableChangeStreamProjection = Optional.empty();
      this.maxInFlightEmbeddingGetMores = Optional.empty();
      this.embeddingGetMoreBatchSize = Optional.empty();
      this.excludedChangestreamFields = List.of();
      this.matchCollectionUuidForUpdateLookup = false;
      this.enableSplitLargeChangeStreamEvents = false;
      this.autoEmbeddingMaterializedViewConfig = Optional.empty();
      this.changeStreamInactivityThresholdSec =
          MongoDbReplicationConfig.DEFAULT_CHANGE_STREAM_INACTIVITY_THRESHOLD_SEC;
      this.replicationType = CommonReplicationConfig.Type.DEFAULT;
      this.steadyStateSchedulingMethod =
          MongoDbReplicationConfig.DEFAULT_STEADY_STATE_SCHEDULING_METHOD;
      this.steadyStatePriorityRatio = MongoDbReplicationConfig.DEFAULT_STEADY_STATE_PRIORITY_RATIO;
      this.steadyStatePriorityOptimeThresholdSeconds =
          MongoDbReplicationConfig.DEFAULT_STEADY_STATE_PRIORITY_OPTIME_THRESHOLD_SECONDS;
    }

    public Builder setNumConcurrentChangeStreams(int value) {
      this.numConcurrentChangeStreams = Optional.of(value);
      return this;
    }

    public Builder setChangeStreamQueryMaxTimeMs(int value) {
      this.changeStreamQueryMaxTimeMs = Optional.of(value);
      return this;
    }

    public Builder setChangeStreamCursorMaxTimeSec(int value) {
      this.changeStreamCursorMaxTimeSec = Optional.of(value);
      return this;
    }

    public Builder setEnableChangeStreamProjection(Optional<Boolean> value) {
      this.enableChangeStreamProjection = value;
      return this;
    }

    public Builder setMaxInFlightEmbeddingGetMores(int value) {
      this.maxInFlightEmbeddingGetMores = Optional.of(value);
      return this;
    }

    public Builder setEmbeddingGetMoreBatchSize(Optional<Integer> value) {
      this.embeddingGetMoreBatchSize = value;
      return this;
    }

    public Builder setExcludedChangestreamFields(List<String> excludedChangestreamFields) {
      this.excludedChangestreamFields = excludedChangestreamFields;
      return this;
    }

    public Builder setMatchCollectionUuidForUpdateLookup(boolean value) {
      this.matchCollectionUuidForUpdateLookup = value;
      return this;
    }

    public Builder setEnableSplitLargeChangeStreamEvents(
        boolean enableSplitLargeChangeStreamEvents) {
      this.enableSplitLargeChangeStreamEvents = enableSplitLargeChangeStreamEvents;
      return this;
    }

    public Builder setAutoEmbeddingMaterializedViewConfig(
        AutoEmbeddingMaterializedViewConfig autoEmbeddingMaterializedViewConfig) {
      this.autoEmbeddingMaterializedViewConfig = Optional.of(autoEmbeddingMaterializedViewConfig);
      return this;
    }

    public Builder setChangeStreamInactivityThresholdSec(int value) {
      this.changeStreamInactivityThresholdSec = value;
      return this;
    }

    public Builder setReplicationType(CommonReplicationConfig.Type replicationType) {
      this.replicationType = replicationType;
      return this;
    }

    public Builder setSteadyStateSchedulingMethod(
        SteadyStateSchedulingMethod steadyStateSchedulingMethod) {
      this.steadyStateSchedulingMethod = steadyStateSchedulingMethod;
      return this;
    }

    public Builder setSteadyStatePriorityRatio(int steadyStatePriorityRatio) {
      this.steadyStatePriorityRatio = steadyStatePriorityRatio;
      return this;
    }

    public Builder setSteadyStatePriorityOptimeThresholdSeconds(
        int steadyStatePriorityOptimeThresholdSeconds) {
      this.steadyStatePriorityOptimeThresholdSeconds = steadyStatePriorityOptimeThresholdSeconds;
      return this;
    }

    public SteadyStateReplicationConfig build() {
      Check.argIsPositive(
          this.changeStreamInactivityThresholdSec, "changeStreamInactivityThresholdSec");
      return new SteadyStateReplicationConfig(
          Check.isPresent(this.numConcurrentChangeStreams, "numConcurrentChangeStreams"),
          Check.isPresent(this.changeStreamQueryMaxTimeMs, "changeStreamQueryMaxTimeMs"),
          Check.isPresent(this.changeStreamCursorMaxTimeSec, "changeStreamCursorMaxTimeSec"),
          this.enableChangeStreamProjection,
          Check.isPresent(this.maxInFlightEmbeddingGetMores, "maxInFlightEmbeddingGetMores"),
          this.embeddingGetMoreBatchSize,
          this.excludedChangestreamFields,
          this.matchCollectionUuidForUpdateLookup,
          this.enableSplitLargeChangeStreamEvents,
          this.autoEmbeddingMaterializedViewConfig,
          this.changeStreamInactivityThresholdSec,
          this.replicationType,
          this.steadyStateSchedulingMethod,
          this.steadyStatePriorityRatio,
          this.steadyStatePriorityOptimeThresholdSeconds);
    }
  }
}
