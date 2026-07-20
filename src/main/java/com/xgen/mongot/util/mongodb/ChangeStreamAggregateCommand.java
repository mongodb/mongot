package com.xgen.mongot.util.mongodb;

import static com.xgen.mongot.util.Check.checkArg;

import com.mongodb.internal.VisibleForTesting;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.mongodb.serialization.AggregateCommandProxy;
import com.xgen.mongot.util.mongodb.serialization.ChangeStreamPipelineStageOptionsProxy;
import com.xgen.mongot.util.mongodb.serialization.ChangeStreamPipelineStageProxy;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.BsonTimestamp;
import org.bson.BsonValue;
import org.bson.conversions.Bson;

/**
 * Represents an aggregate command (https://docs.mongodb.com/manual/reference/command/aggregate/),
 * which contains at least a $changeStream pipeline stage (see here:
 * https://github.com/mongodb/specifications/blob/master/source/change-streams/change-streams.rst).
 */
public class ChangeStreamAggregateCommand {

  public enum FullDocument {
    DEFAULT,
    UPDATE_LOOKUP,
  }

  private final FullDocument fullDocument;
  private final Optional<String> collectionName;
  private final OptionalLong batchSize;
  private final Optional<BsonDocument> startAfter;
  private final Optional<BsonTimestamp> startAtOperationTime;
  private final Optional<Boolean> showMigrationEvents;
  private final Optional<Boolean> showExpandedEvents;
  private final Optional<Boolean> matchCollectionUuidForUpdateLookup;
  /* $project that filters our fields that are not used for indexing. */
  private final Optional<Bson> indexedFieldsProjectionStage;
  /* $project that filters out unused changestream metadata fields. */
  private final Optional<Bson> changeStreamMetadataExclusionProjectionStage;

  /** $changeStreamSplitLargeEvent stage for large event splitting. */
  private final Optional<Bson> splitLargeEventStage;

  /**
   * $addFields that adds metadata fields like _id and deleted flag under a separate namespace under
   * indexId field. Required to avoid collision with user-defined fields.
   */
  private final Optional<Bson> metadataAddFieldsStage;

  /**
   * Aggregation stages defined in the view. Empty if the index is created on a collection or the
   * view has an empty pipeline.
   */
  private final Optional<List<Bson>> viewDefinedStages;

  private ChangeStreamAggregateCommand(
      FullDocument fullDocument,
      Optional<String> collectionName,
      OptionalLong batchSize,
      Optional<BsonDocument> startAfter,
      Optional<BsonTimestamp> startAtOperationTime,
      Optional<Boolean> showMigrationEvents,
      Optional<Boolean> showExpandedEvents,
      Optional<Boolean> matchCollectionUuidForUpdateLookup,
      Optional<Bson> indexedFieldsProjectionStage,
      Optional<Bson> metadataAddFieldsStage,
      Optional<List<Bson>> viewDefinedStages,
      Optional<Bson> changeStreamMetadataExclusionProjectionStage,
      Optional<Bson> splitLargeEventStage) {
    checkArg(
        startAfter.isEmpty() || startAtOperationTime.isEmpty(),
        "only one of startAfter or startAtOperation may be specified");

    this.fullDocument = fullDocument;
    this.collectionName = collectionName;
    this.batchSize = batchSize;
    this.startAfter = startAfter;
    this.startAtOperationTime = startAtOperationTime;
    this.showMigrationEvents = showMigrationEvents;
    this.showExpandedEvents = showExpandedEvents;
    this.matchCollectionUuidForUpdateLookup = matchCollectionUuidForUpdateLookup;
    this.indexedFieldsProjectionStage = indexedFieldsProjectionStage;
    this.metadataAddFieldsStage = metadataAddFieldsStage;
    this.viewDefinedStages = viewDefinedStages;
    this.changeStreamMetadataExclusionProjectionStage =
        changeStreamMetadataExclusionProjectionStage;
    this.splitLargeEventStage = splitLargeEventStage;
  }

  public static class Builder {

    private Optional<String> collectionName;
    private Optional<FullDocument> fullDocument;
    private OptionalLong batchSize;
    private Optional<BsonDocument> startAfter;
    private Optional<BsonTimestamp> startAtOperationTime;
    private Optional<Boolean> showMigrationEvents;
    private Optional<Boolean> showExpandedEvents;
    private Optional<Boolean> matchCollectionUuidForUpdateLookup;
    private Optional<Bson> indexedFieldsProjectionStage;
    private Optional<Bson> changeStreamMetadataExclusionProjectionStage;
    private Optional<Bson> splitLargeEventStage;
    private Optional<Bson> metadataAddFieldsStage;
    private Optional<List<Bson>> viewDefinedStages;

    /** Constructs a new Builder. */
    public Builder() {
      this.collectionName = Optional.empty();
      this.fullDocument = Optional.empty();
      this.batchSize = OptionalLong.empty();
      this.startAfter = Optional.empty();
      this.startAtOperationTime = Optional.empty();
      this.showMigrationEvents = Optional.empty();
      this.showExpandedEvents = Optional.empty();
      this.matchCollectionUuidForUpdateLookup = Optional.empty();
      this.indexedFieldsProjectionStage = Optional.empty();
      this.metadataAddFieldsStage = Optional.empty();
      this.changeStreamMetadataExclusionProjectionStage = Optional.empty();
      this.splitLargeEventStage = Optional.empty();
      this.viewDefinedStages = Optional.empty();
    }

    /** Builds the configured ChangeStreamAggregateCommand. */
    public ChangeStreamAggregateCommand build() {
      return new ChangeStreamAggregateCommand(
          this.fullDocument.orElse(FullDocument.DEFAULT),
          this.collectionName,
          this.batchSize,
          this.startAfter,
          this.startAtOperationTime,
          this.showMigrationEvents,
          this.showExpandedEvents,
          this.matchCollectionUuidForUpdateLookup,
          this.indexedFieldsProjectionStage,
          this.metadataAddFieldsStage,
          this.viewDefinedStages,
          this.changeStreamMetadataExclusionProjectionStage,
          this.splitLargeEventStage);
    }

    /** Sets the collection name. */
    public Builder collection(String collectionName) {
      this.collectionName = Optional.of(collectionName);
      return this;
    }

    /** Sets the indexId. */
    public Builder metadataAddFieldsStage(BsonDocument metadataAddFieldsStage) {
      this.metadataAddFieldsStage = Optional.of(metadataAddFieldsStage);
      return this;
    }

    /** Sets the FullDocument option. */
    public Builder fullDocument(FullDocument fullDocument) {
      this.fullDocument = Optional.of(fullDocument);
      return this;
    }

    /** Sets the batch size. */
    public Builder batchSize(long batchSize) {
      Check.argNotNegative(batchSize, "batchSize");
      this.batchSize = OptionalLong.of(batchSize);
      return this;
    }

    /** Sets the resumeToken to use for the startAfter option. */
    public Builder startAfter(BsonDocument resumeToken) {
      this.startAfter = Optional.of(resumeToken);
      return this;
    }

    /** Sets the opTime to use for the startAtOperationTime option. */
    public Builder startAtOperationTime(BsonTimestamp opTime) {
      this.startAtOperationTime = Optional.of(opTime);
      return this;
    }

    /** Sets the showMigrationEvents flag. */
    public Builder showMigrationEvents(boolean showMigrationEvents) {
      this.showMigrationEvents = Optional.of(showMigrationEvents);
      return this;
    }

    /** Sets the showExpandedEvents flag. */
    public Builder showExpandedEvents(boolean showExpandedEvents) {
      this.showExpandedEvents = Optional.of(showExpandedEvents);
      return this;
    }

    /** Sets the matchCollectionUuidForUpdateLookup flag. */
    public Builder matchCollectionUuidForUpdateLookup(boolean matchCollectionUuidForUpdateLookup) {
      this.matchCollectionUuidForUpdateLookup = Optional.of(matchCollectionUuidForUpdateLookup);
      return this;
    }

    /** Sets the $project stage for filtering out unindexed fields. */
    public Builder indexedFieldsProjectionStage(BsonDocument indexedFieldsProjectionStage) {
      this.indexedFieldsProjectionStage = Optional.of(indexedFieldsProjectionStage);
      return this;
    }

    /** Sets the stages defined by views. */
    public Builder viewDefinedStages(List<Bson> viewDefinedStages) {
      this.viewDefinedStages = Optional.of(viewDefinedStages);
      return this;
    }

    /** Sets the $project stage for filtering out changestream metadata fields. */
    public Builder changeStreamMetadataExclusionProjectionStage(
        BsonDocument changeStreamMetadataExclusionProjectionStage) {
      this.changeStreamMetadataExclusionProjectionStage =
          Optional.of(changeStreamMetadataExclusionProjectionStage);
      return this;
    }

    /** Sets the $changeStreamSplitLargeEvent stage for large event splitting. */
    public Builder splitLargeEvent(boolean value) {
      if (value) {
        this.splitLargeEventStage =
            Optional.of(new BsonDocument("$changeStreamSplitLargeEvent", new BsonDocument()));
      }
      return this;
    }
  }

  /**
   * Whether this command targets the whole database ({@code aggregate: 1}) rather than a specific
   * collection. Mirrors the {@code aggregate} value chosen in {@link #toProxy()}.
   */
  public boolean isCollectionLess() {
    return this.collectionName.isEmpty();
  }

  /** Constructs the proper AggregateCommandProxy for the ChangeStreamAggregateCommand. */
  public AggregateCommandProxy toProxy() {
    BsonValue aggregate =
        this.collectionName
            .map(BsonString::new)
            .map(BsonValue.class::cast)
            .orElseGet(() -> new BsonInt32(1));

    Optional<String> fullDocument =
        this.fullDocument == FullDocument.UPDATE_LOOKUP
            ? Optional.of(ChangeStreamPipelineStageOptionsProxy.FULL_DOCUMENT_UPDATE_LOOKUP)
            : Optional.empty();

    ChangeStreamPipelineStageOptionsProxy changeStreamOptions =
        new ChangeStreamPipelineStageOptionsProxy(
            this.startAfter,
            this.startAtOperationTime,
            fullDocument,
            this.showMigrationEvents,
            this.showExpandedEvents,
            this.matchCollectionUuidForUpdateLookup);

    ChangeStreamPipelineStageProxy changeStreamStage =
        new ChangeStreamPipelineStageProxy(changeStreamOptions);

    List<Bson> pipeline =
        new AggregationPipelineBuilder()
            .addStage(changeStreamStage.toBsonDocument())
            .addStage(this.metadataAddFieldsStage)
            .addMultipleStages(this.viewDefinedStages)
            // projection of the fields used in the index appears as the last stage as view-defined
            // stages might need access to the full document
            .addStage(this.indexedFieldsProjectionStage)
            .addStage(this.changeStreamMetadataExclusionProjectionStage)
            // $changeStreamSplitLargeEvent must be the last stage to minimize data being split
            .addStage(this.splitLargeEventStage)
            .build();

    return new AggregateCommandProxy(
        aggregate,
        pipeline,
        new AggregateCommandProxy.CursorProxy(this.batchSize),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty());
  }

  public OptionalLong getBatchSize() {
    return this.batchSize;
  }

  @VisibleForTesting(otherwise = VisibleForTesting.AccessModifier.PRIVATE)
  public Optional<BsonTimestamp> getStartAtOperationTime() {
    return this.startAtOperationTime;
  }
}
