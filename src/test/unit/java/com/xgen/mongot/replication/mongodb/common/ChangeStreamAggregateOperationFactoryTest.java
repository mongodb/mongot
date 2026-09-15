package com.xgen.mongot.replication.mongodb.common;

import static com.google.common.truth.Truth.assertThat;
import static com.xgen.testing.mongot.mock.index.SearchIndex.MOCK_INDEX_DEFINITION;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import com.mongodb.MongoNamespace;
import java.util.List;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonTimestamp;
import org.bson.RawBsonDocument;
import org.bson.codecs.Decoder;
import org.junit.Test;

public class ChangeStreamAggregateOperationFactoryTest {

  private static final MongoNamespace NAMESPACE =
      new MongoNamespace("testDatabase", "testCollection");
  private static final ChangeStreamModeSelector.ChangeStreamMode DEFAULT_MODE =
      ChangeStreamModeSelector.ChangeStreamMode.getDefault();

  @Test
  public void testChangeStreamMetadataExclusionProjectionStage() {
    String databaseName = "testDatabase";
    String collectionName = "testChangeStreamMetadataExclusionProjectionStage";

    // Create a command factory with excluded fields and check the generated pipeline
    List<String> excludedFields = List.of("excluded1", "excluded2", "nested.field");
    ChangeStreamAggregateOperationFactory factory =
        new ChangeStreamAggregateOperationFactory(
            MOCK_INDEX_DEFINITION,
            0, // maxTimeMs
            excludedFields,
            false,
            false);
    List<BsonDocument> pipeline =
        factory
            .fromResumeInfo(
                ChangeStreamResumeInfo.create(
                    new MongoNamespace(databaseName, collectionName), new BsonDocument()),
                DEFAULT_MODE)
            .create((Decoder<RawBsonDocument>) mock(Decoder.class))
            .getPipeline();
    assertThat(pipeline).isNotEmpty();

    // The exclusion projection stage should be the last one
    BsonDocument exclusionStage = pipeline.getLast();
    assertTrue("Expected last stage to be $project", exclusionStage.containsKey("$project"));
    BsonDocument projectFields = exclusionStage.getDocument("$project");
    excludedFields.forEach(
        field -> assertTrue(field + " should be excluded", projectFields.containsKey(field)));
  }

  @Test
  public void testMatchCollectionUuidForUpdateLookup() {
    String databaseName = "testDatabase";
    String collectionName = "testMatchCollectionUuidForUpdateLookup";

    // Create a command factory with matchCollectionUuidForUpdateLookup and check the generated
    // pipeline
    ChangeStreamAggregateOperationFactory factory =
        new ChangeStreamAggregateOperationFactory(
            MOCK_INDEX_DEFINITION,
            0, // maxTimeMs
            List.of(),
            true,
            false);
    List<BsonDocument> pipeline =
        factory
            .fromResumeInfo(
                ChangeStreamResumeInfo.create(
                    new MongoNamespace(databaseName, collectionName), new BsonDocument()),
                DEFAULT_MODE)
            .create((Decoder<RawBsonDocument>) mock(Decoder.class))
            .getPipeline();
    assertThat(pipeline).isNotEmpty();
    assertThat(
            pipeline
                .getFirst()
                .get("$changeStream")
                .asDocument()
                .get("matchCollectionUUIDForUpdateLookup")
                .asBoolean()
                .getValue())
        .isTrue();
  }

  @Test
  public void testChangeStreamSplitLargeEventStage() {
    String databaseName = "testDatabase";
    String collectionName = "testChangeStreamSplitLargeEventStage";

    ChangeStreamAggregateOperationFactory factory =
        new ChangeStreamAggregateOperationFactory(
            MOCK_INDEX_DEFINITION,
            0, // maxTimeMs
            List.of(),
            false,
            true);
    List<BsonDocument> pipeline =
        factory
            .fromResumeInfo(
                ChangeStreamResumeInfo.create(
                    new MongoNamespace(databaseName, collectionName), new BsonDocument()),
                DEFAULT_MODE)
            .create((Decoder<RawBsonDocument>) mock(Decoder.class))
            .getPipeline();
    assertThat(pipeline).isNotEmpty();
    BsonDocument splitLargeEventStage = pipeline.getLast();
    assertTrue(
        "Expected last stage to be $changeStreamSplitLargeEvent",
        splitLargeEventStage.containsKey("$changeStreamSplitLargeEvent"));
  }

  @Test
  public void fromResumeInfo_withoutPartition_omitsPartitionMatch() {
    List<BsonDocument> pipeline =
        pipelineFrom(
            factory(/* splitLargeEvent= */ false)
                .fromResumeInfo(
                    ChangeStreamResumeInfo.create(NAMESPACE, new BsonDocument()), DEFAULT_MODE));

    assertThat(pipeline.getFirst().containsKey("$changeStream")).isTrue();
    assertThat(findStageIndex(pipeline, "$match")).isEqualTo(-1);
  }

  @Test
  public void fromResumeInfo_withOnePartition_omitsPartitionMatch() {
    List<BsonDocument> pipeline =
        pipelineFrom(
            factory(/* splitLargeEvent= */ false)
                .fromResumeInfo(
                    ChangeStreamResumeInfo.create(NAMESPACE, new BsonDocument()),
                    DEFAULT_MODE,
                    new ChangeStreamPartition(0, 1)));

    assertThat(findStageIndex(pipeline, "$match")).isEqualTo(-1);
  }

  @Test
  public void fromResumeInfo_withPartition_insertsMatchImmediatelyAfterChangeStream() {
    ChangeStreamPartition partition = new ChangeStreamPartition(1, 3);
    List<BsonDocument> pipeline =
        pipelineFrom(
            factory(/* splitLargeEvent= */ false)
                .fromResumeInfo(
                    ChangeStreamResumeInfo.create(NAMESPACE, new BsonDocument()),
                    DEFAULT_MODE,
                    partition));

    assertThat(pipeline.get(0).containsKey("$changeStream")).isTrue();
    assertThat(pipeline.get(0).getDocument("$changeStream").containsKey("startAfter")).isTrue();
    assertThat(pipeline.get(1)).isEqualTo(expectedPartitionMatch(partition));
    assertThat(pipeline.get(2).containsKey("$addFields")).isTrue();
  }

  @Test
  public void fromResumeInfo_withPartitionAndSplitLargeEvent_keepsSplitStageLast() {
    ChangeStreamPartition partition = new ChangeStreamPartition(0, 2);
    List<BsonDocument> pipeline =
        pipelineFrom(
            factory(/* splitLargeEvent= */ true)
                .fromResumeInfo(
                    ChangeStreamResumeInfo.create(NAMESPACE, new BsonDocument()),
                    DEFAULT_MODE,
                    partition));

    assertThat(pipeline.get(1)).isEqualTo(expectedPartitionMatch(partition));
    assertThat(pipeline.getLast().containsKey("$changeStreamSplitLargeEvent")).isTrue();
  }

  @Test
  public void fromOperationTime_withoutPartition_setsStartAtOperationTimeOnly() {
    BsonTimestamp operationTime = new BsonTimestamp(100, 1);
    List<BsonDocument> pipeline =
        pipelineFrom(
            factory(/* splitLargeEvent= */ false)
                .fromOperationTime(NAMESPACE, operationTime, DEFAULT_MODE));

    BsonDocument changeStream = pipeline.getFirst().getDocument("$changeStream");
    assertThat(changeStream.getTimestamp("startAtOperationTime")).isEqualTo(operationTime);
    assertThat(changeStream.containsKey("startAfter")).isFalse();
    assertThat(findStageIndex(pipeline, "$match")).isEqualTo(-1);
  }

  @Test
  public void fromOperationTime_withPartition_setsStartAtOperationTimeAndMatch() {
    BsonTimestamp operationTime = new BsonTimestamp(200, 2);
    ChangeStreamPartition partition = new ChangeStreamPartition(2, 4);
    List<BsonDocument> pipeline =
        pipelineFrom(
            factory(/* splitLargeEvent= */ false)
                .fromOperationTime(NAMESPACE, operationTime, DEFAULT_MODE, partition));

    BsonDocument changeStream = pipeline.getFirst().getDocument("$changeStream");
    assertThat(changeStream.getTimestamp("startAtOperationTime")).isEqualTo(operationTime);
    assertThat(pipeline.get(1)).isEqualTo(expectedPartitionMatch(partition));
  }

  @Test
  public void partitionMatchStage_hashesDocumentIdAndBroadcastsLifecycleEvents() {
    ChangeStreamPartition partition = new ChangeStreamPartition(1, 3);
    BsonDocument match = ChangeStreamAggregateOperationBuilder.buildPartitionMatchStage(partition);

    BsonArray condition = match.getDocument("$match").getDocument("$expr").getArray("$cond");
    assertThat(condition.get(0))
        .isEqualTo(
            BsonDocument.parse(
                "{\"$in\":[\"$operationType\","
                    + "[\"insert\",\"update\",\"replace\",\"delete\"]]}"));

    BsonArray partitionMatchArgs = condition.get(1).asDocument().getArray("$eq");
    BsonArray modArgs = partitionMatchArgs.get(0).asDocument().getDocument("$abs").getArray("$mod");
    assertThat(modArgs.get(0))
        .isEqualTo(
            BsonDocument.parse(
                "{\"$toHashedIndexKey\":{\"$_internalKeyStringValue\":"
                    + "{\"input\":\"$documentKey._id\"}}}"));
    assertThat(modArgs.get(1).asInt32().getValue()).isEqualTo(partition.partitionCount());
    assertThat(partitionMatchArgs.get(1).asInt32().getValue()).isEqualTo(partition.partitionId());

    // The false branch of the document-event condition is true, broadcasting lifecycle events.
    assertThat(condition.get(2).asBoolean().getValue()).isTrue();
  }

  @Test
  public void builder_partition_nullPartition_throws() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new ChangeStreamAggregateOperationBuilder(NAMESPACE).partition(null));
  }

  private static ChangeStreamAggregateOperationFactory factory(boolean splitLargeEvent) {
    return new ChangeStreamAggregateOperationFactory(
        MOCK_INDEX_DEFINITION, 0, List.of(), false, splitLargeEvent);
  }

  private static List<BsonDocument> pipelineFrom(
      ChangeStreamAggregateOperationBuilder.AggregateOperationTemplate template) {
    return template.create((Decoder<RawBsonDocument>) mock(Decoder.class)).getPipeline();
  }

  private static BsonDocument expectedPartitionMatch(ChangeStreamPartition partition) {
    return ChangeStreamAggregateOperationBuilder.buildPartitionMatchStage(partition);
  }

  private static int findStageIndex(List<BsonDocument> pipeline, String stageName) {
    for (int i = 0; i < pipeline.size(); i++) {
      if (pipeline.get(i).containsKey(stageName)) {
        return i;
      }
    }
    return -1;
  }
}
