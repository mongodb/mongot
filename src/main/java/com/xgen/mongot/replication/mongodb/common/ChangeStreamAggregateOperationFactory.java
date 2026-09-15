package com.xgen.mongot.replication.mongodb.common;

import com.google.common.annotations.VisibleForTesting;
import com.mongodb.MongoNamespace;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Projections;
import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.replication.mongodb.common.ChangeStreamModeSelector.ChangeStreamMode;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.mongodb.ChangeStreamAggregateCommand;
import java.util.List;
import org.bson.BsonTimestamp;

public class ChangeStreamAggregateOperationFactory {

  private final IndexDefinition indexDefinition;
  private final int maxTimeMs;
  private final List<String> excludedFields;
  private final boolean matchCollectionUuidForUpdateLookup;
  private final boolean enableSplitLargeChangeStreamEvents;

  public ChangeStreamAggregateOperationFactory(
      IndexDefinition indexDefinition,
      int maxTimeMs,
      List<String> excludedFields,
      boolean matchCollectionUuidForUpdateLookup,
      boolean enableSplitLargeChangeStreamEvents) {
    this.indexDefinition = indexDefinition;
    this.maxTimeMs = maxTimeMs;
    this.excludedFields = excludedFields;
    this.matchCollectionUuidForUpdateLookup = matchCollectionUuidForUpdateLookup;
    this.enableSplitLargeChangeStreamEvents = enableSplitLargeChangeStreamEvents;
  }

  /**
   * Note that we request that a cursor returned from the aggregate command have a batch size of 0.
   *
   * <p>If you don't specify a batch size of 0, the mongod will scan the oplog until either it finds
   * a matching event, or reaches the end of the oplog. This can be problematic if you are resuming
   * a change stream at a point far back in the oplog, with many entries not pertaining to this
   * collection between the resume point and either the next entry pertaining to this collection or
   * the end of the oplog, as it would block for an unpredictably long time.
   *
   * <p>Additionally, using maxTimeMS does not limit the amount of time spent on this initial scan,
   * but instead makes the cursor fail if it exceeds the allotted time (see
   * https://jira.mongodb.org/browse/SERVER-44110).
   *
   * <p>So to make the behavior predictable, we run the initial aggregation with a batch size of 0
   * to quickly establish the cursors, then call getMores on the cursor, which are subject to a
   * maxTimeMS.
   */
  @VisibleForTesting
  public ChangeStreamAggregateOperationBuilder builder(
      ChangeStreamMode mode, MongoNamespace namespace) {

    ChangeStreamAggregateOperationBuilder builder =
        new ChangeStreamAggregateOperationBuilder(namespace)
            .batchSize(0)
            .maxTimeMs(this.maxTimeMs)
            .fullDocument(ChangeStreamAggregateCommand.FullDocument.UPDATE_LOOKUP)
            .splitLargeEvent(this.enableSplitLargeChangeStreamEvents)
            .metadataAddFieldsStage(
                MetadataNamespace.forChangeStream(this.indexDefinition.getIndexId()))
            .showMigrationEvents(true)
            .matchCollectionUuidForUpdateLookup(this.matchCollectionUuidForUpdateLookup);

    if (mode == ChangeStreamMode.INDEXED_FIELDS) {
      Projection.forChangeStream(this.indexDefinition)
          .ifPresent(
              projectStage ->
                  builder.indexedFieldsProjectionStage(
                      Aggregates.project(projectStage).toBsonDocument()));
    }

    this.indexDefinition
        .getView()
        .ifPresent(
            view ->
                builder.viewDefinedStages(
                    ViewPipeline.forChangeStream(view, this.indexDefinition.getIndexId())));

    if (!this.excludedFields.isEmpty()) {
      builder.changeStreamMetadataExclusionProjectionStage(
          Aggregates.project(
                  Projections.fields(
                      this.excludedFields.stream().map(Projections::exclude).toList()))
              .toBsonDocument());
    }

    return builder;
  }

  public ChangeStreamAggregateOperationBuilder.AggregateOperationTemplate fromResumeInfo(
      ChangeStreamResumeInfo resumeInfo, ChangeStreamMode mode) {
    return builder(mode, resumeInfo.getNamespace()).startAfter(resumeInfo.getResumeToken()).build();
  }

  /**
   * Like {@link #fromResumeInfo(ChangeStreamResumeInfo, ChangeStreamMode)} but adds a partition
   * {@code $match} for one partial stream.
   */
  public ChangeStreamAggregateOperationBuilder.AggregateOperationTemplate fromResumeInfo(
      ChangeStreamResumeInfo resumeInfo, ChangeStreamMode mode, ChangeStreamPartition partition) {
    Check.argNotNull(partition, "partition");
    return builder(mode, resumeInfo.getNamespace())
        .startAfter(resumeInfo.getResumeToken())
        .partition(partition)
        .build();
  }

  /**
   * Starts a change stream at {@code operationTime} (inclusive) without a partition match (the
   * single-stream pipeline shape).
   */
  public ChangeStreamAggregateOperationBuilder.AggregateOperationTemplate fromOperationTime(
      MongoNamespace namespace, BsonTimestamp operationTime, ChangeStreamMode mode) {
    Check.argNotNull(namespace, "namespace");
    Check.argNotNull(operationTime, "operationTime");
    return builder(mode, namespace).startAtOperationTime(operationTime).build();
  }

  /**
   * Like {@link #fromOperationTime(MongoNamespace, BsonTimestamp, ChangeStreamMode)} but adds a
   * partition {@code $match} for one partial stream.
   */
  public ChangeStreamAggregateOperationBuilder.AggregateOperationTemplate fromOperationTime(
      MongoNamespace namespace,
      BsonTimestamp operationTime,
      ChangeStreamMode mode,
      ChangeStreamPartition partition) {
    Check.argNotNull(namespace, "namespace");
    Check.argNotNull(operationTime, "operationTime");
    Check.argNotNull(partition, "partition");
    return builder(mode, namespace)
        .startAtOperationTime(operationTime)
        .partition(partition)
        .build();
  }
}
