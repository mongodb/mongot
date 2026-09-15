package com.xgen.mongot.replication.mongodb.common;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.mongodb.MongoNamespace;
import com.mongodb.client.model.changestream.OperationType;
import com.mongodb.internal.client.model.AggregationLevel;
import com.mongodb.internal.operation.AggregateOperation;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.mongodb.AggregationPipelineBuilder;
import com.xgen.mongot.util.mongodb.ChangeStreamAggregateCommand.FullDocument;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.stream.Collectors;
import org.bson.BsonArray;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.BsonTimestamp;
import org.bson.codecs.Decoder;
import org.bson.conversions.Bson;

public class ChangeStreamAggregateOperationBuilder {

  private static final ImmutableSet<OperationType> DOCUMENT_OPERATION_TYPES =
      ImmutableSet.of(
          OperationType.INSERT, OperationType.UPDATE, OperationType.REPLACE, OperationType.DELETE);

  private final MongoNamespace namespace;
  private final ChangeStreamPipelineBuilder pipelineBuilder;

  private OptionalInt batchSize;

  /** $project that filters our fields that are not used for indexing. */
  private Optional<Bson> indexedFieldsProjectionStage;

  /* $project that filters out unused changestream metadata fields. */
  private Optional<Bson> changeStreamMetadataExclusionProjectionStage;

  /**
   * $addFields that adds metadata fields like _id and deleted flag under a separate namespace under
   * indexId field. Required to avoid collision with user-defined fields.
   */
  private Optional<Bson> metadataAddFieldsStage;

  /**
   * Aggregation stages defined in the view. Empty if the index is created on a collection or the
   * view has an empty pipeline.
   */
  private Optional<List<Bson>> viewDefinedStages;

  /** $changeStreamSplitLargeEvent stage for large event splitting. */
  private Optional<Bson> changeStreamSplitLargeEventStage;

  /**
   * Optional $match that keeps document events whose {@code documentKey._id} hashes into this
   * partition and broadcasts document-less lifecycle events. This is immediately after
   * $changeStream.
   */
  private Optional<Bson> partitionMatchStage;

  public ChangeStreamAggregateOperationBuilder(MongoNamespace namespace) {
    Check.argNotNull(namespace, "namespace");
    this.batchSize = OptionalInt.empty();
    this.indexedFieldsProjectionStage = Optional.empty();
    this.changeStreamMetadataExclusionProjectionStage = Optional.empty();
    this.viewDefinedStages = Optional.empty();
    this.metadataAddFieldsStage = Optional.empty();
    this.changeStreamSplitLargeEventStage = Optional.empty();
    this.partitionMatchStage = Optional.empty();
    this.namespace = namespace;
    this.pipelineBuilder = new ChangeStreamPipelineBuilder();
  }

  public ChangeStreamAggregateOperationBuilder batchSize(int value) {
    this.batchSize = OptionalInt.of(value);
    return this;
  }

  public ChangeStreamAggregateOperationBuilder indexedFieldsProjectionStage(Bson value) {
    this.indexedFieldsProjectionStage = Optional.of(value);
    return this;
  }

  public ChangeStreamAggregateOperationBuilder changeStreamMetadataExclusionProjectionStage(
      Bson value) {
    this.changeStreamMetadataExclusionProjectionStage = Optional.of(value);
    return this;
  }

  public ChangeStreamAggregateOperationBuilder viewDefinedStages(List<Bson> viewDefinedStages) {
    this.viewDefinedStages = Optional.of(viewDefinedStages);
    return this;
  }

  public ChangeStreamAggregateOperationBuilder metadataAddFieldsStage(
      BsonDocument metadataAddFieldsStage) {
    this.metadataAddFieldsStage = Optional.of(metadataAddFieldsStage);
    return this;
  }

  public ChangeStreamAggregateOperationBuilder fullDocument(FullDocument value) {
    this.pipelineBuilder.fullDocument(value);
    return this;
  }

  public ChangeStreamAggregateOperationBuilder startAfter(BsonDocument value) {
    this.pipelineBuilder.startAfter(value);
    return this;
  }

  public ChangeStreamAggregateOperationBuilder startAtOperationTime(BsonTimestamp value) {
    this.pipelineBuilder.startAtOperationTime(value);
    return this;
  }

  public ChangeStreamAggregateOperationBuilder showMigrationEvents(Boolean value) {
    this.pipelineBuilder.showMigrationEvents(value);
    return this;
  }

  public ChangeStreamAggregateOperationBuilder showExpandedEvents(Boolean value) {
    this.pipelineBuilder.showExpandedEvents(value);
    return this;
  }

  public ChangeStreamAggregateOperationBuilder splitLargeEvent(boolean value) {
    if (value) {
      this.changeStreamSplitLargeEventStage =
          Optional.of(new BsonDocument("$changeStreamSplitLargeEvent", new BsonDocument()));
    }
    return this;
  }

  public ChangeStreamAggregateOperationBuilder matchCollectionUuidForUpdateLookup(Boolean value) {
    this.pipelineBuilder.matchCollectionUuidForUpdateLookup(value);
    return this;
  }

  /**
   * Adds a {@code $match} that routes document events to one change stream reader by hashing {@code
   * documentKey._id}. Lifecycle events are broadcast because they do not have a document key.
   */
  public ChangeStreamAggregateOperationBuilder partition(ChangeStreamPartition partition) {
    Check.argNotNull(partition, "partition");
    this.partitionMatchStage =
        partition.partitionCount() == 1
            ? Optional.empty()
            : Optional.of(buildPartitionMatchStage(partition));
    return this;
  }

  /** Builds the document-key partition match, including lifecycle-event broadcasting. */
  @VisibleForTesting
  static BsonDocument buildPartitionMatchStage(ChangeStreamPartition partition) {
    BsonArray documentOperationTypes =
        DOCUMENT_OPERATION_TYPES.stream()
            .map(OperationType::getValue)
            .map(BsonString::new)
            .collect(Collectors.toCollection(BsonArray::new));
    BsonDocument isDocumentEvent =
        new BsonDocument(
            "$in",
            new BsonArray(List.of(new BsonString("$operationType"), documentOperationTypes)));

    // Encode the _id as a KeyString before hashing because $toHashedIndexKey will coerce doubles to
    // int64. Without this, double _ids outside the int64 range can all hash to the same value.
    // See https://jira.mongodb.org/browse/SERVER-82815 for more details.
    BsonDocument keyString =
        new BsonDocument(
            "$_internalKeyStringValue",
            new BsonDocument("input", new BsonString("$documentKey._id")));
    BsonArray modArgs = new BsonArray();
    modArgs.add(new BsonDocument("$toHashedIndexKey", keyString));
    modArgs.add(new BsonInt32(partition.partitionCount()));

    BsonArray partitionMatchArgs = new BsonArray();
    partitionMatchArgs.add(new BsonDocument("$abs", new BsonDocument("$mod", modArgs)));
    partitionMatchArgs.add(new BsonInt32(partition.partitionId()));

    BsonArray condition =
        new BsonArray(
            List.of(
                isDocumentEvent, new BsonDocument("$eq", partitionMatchArgs), BsonBoolean.TRUE));
    return new BsonDocument(
        "$match", new BsonDocument("$expr", new BsonDocument("$cond", condition)));
  }

  public AggregateOperationTemplate build() {
    return new AggregateOperationTemplate(this);
  }

  private <T> AggregateOperation<T> build(Decoder<T> decoder) {
    Check.argNotNull(decoder, "decoder");

    AggregateOperation<T> aggregate =
        new AggregateOperation<>(
            this.namespace, createChangeStreamPipeline(), decoder, AggregationLevel.COLLECTION);

    this.batchSize.ifPresent(aggregate::batchSize);

    return aggregate;
  }

  private List<BsonDocument> createChangeStreamPipeline() {
    var changeStreamChange = new BsonDocument("$changeStream", this.pipelineBuilder.build());

    return new AggregationPipelineBuilder()
            .addStage(changeStreamChange)
            // Partition match must follow $changeStream so it sees documentKey, and precede
            // metadata/view/projection stages so those only process this partial's events.
            .addStage(this.partitionMatchStage)
            .addStage(this.metadataAddFieldsStage)
            .addMultipleStages(this.viewDefinedStages)
            // projection of the fields used in the index appears after view-defined stages
            // as view-defined stages might need access to the full document
            .addStage(this.indexedFieldsProjectionStage)
            .addStage(this.changeStreamMetadataExclusionProjectionStage)
            // changeStreamSplitLargeEvent must be the last stage to minimize data being split
            .addStage(this.changeStreamSplitLargeEventStage)
            .build()
            .stream()
            .map(Bson::toBsonDocument)
            .collect(Collectors.toList());
  }

  private static class ChangeStreamPipelineBuilder {

    private final BsonDocument pipeline = new BsonDocument();

    public void fullDocument(FullDocument value) {
      if (value == FullDocument.UPDATE_LOOKUP) {
        this.pipeline.put("fullDocument", new BsonString("updateLookup"));
      } else {
        this.pipeline.remove("fullDocument");
      }
    }

    public void startAfter(BsonDocument value) {
      this.pipeline.put("startAfter", value);
    }

    public void startAtOperationTime(BsonTimestamp value) {
      this.pipeline.put("startAtOperationTime", value);
    }

    public void showMigrationEvents(Boolean value) {
      this.pipeline.put("showMigrationEvents", new BsonBoolean(value));
    }

    public void showExpandedEvents(Boolean value) {
      this.pipeline.put("showExpandedEvents", new BsonBoolean(value));
    }

    public void matchCollectionUuidForUpdateLookup(Boolean value) {
      // matchCollectionUuidForUpdateLookup is not recognized in older MongoDB versions, so we only
      // append it if it is present. MMS will not pass in matchCollectionUuidForUpdateLookup if the
      // MongoDB version is unsupported.
      if (value) {
        this.pipeline.put("matchCollectionUUIDForUpdateLookup", new BsonBoolean(true));
      }
    }

    public BsonDocument build() {
      return this.pipeline;
    }
  }

  public static class AggregateOperationTemplate {

    private final ChangeStreamAggregateOperationBuilder builder;

    private AggregateOperationTemplate(ChangeStreamAggregateOperationBuilder builder) {
      this.builder = builder;
    }

    public <T> AggregateOperation<T> create(Decoder<T> decoder) {
      return this.builder.build(decoder);
    }
  }
}
