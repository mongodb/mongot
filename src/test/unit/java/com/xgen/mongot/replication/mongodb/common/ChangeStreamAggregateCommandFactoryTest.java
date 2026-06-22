package com.xgen.mongot.replication.mongodb.common;

import static com.google.common.truth.Truth.assertThat;
import static com.xgen.testing.mongot.mock.index.SearchIndex.MOCK_INDEX_DEFINITION;
import static org.junit.Assert.assertTrue;

import com.mongodb.MongoNamespace;
import com.xgen.mongot.util.mongodb.ChangeStreamAggregateCommand;
import java.util.List;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.junit.Test;

public class ChangeStreamAggregateCommandFactoryTest {
  @Test
  public void testChangeStreamMetadataExclusionProjectionStage() {
    String databaseName = "testDatabase";
    String collectionName = "testChangeStreamMetadataExclusionProjectionStage";

    // Create a ChangeStreamAggregateCommandFactory with excluded fields and generate a command
    List<String> excludedFields = List.of("excluded1", "excluded2", "nested.field");
    ChangeStreamAggregateCommandFactory factory =
        new ChangeStreamAggregateCommandFactory(
            MOCK_INDEX_DEFINITION,
            new MongoNamespace(databaseName, collectionName),
            excludedFields,
            false,
            false);
    ChangeStreamAggregateCommand command =
        factory.unpinned(
            MOCK_INDEX_DEFINITION.getIndexId(),
            ChangeStreamModeSelector.ChangeStreamMode.getDefault());

    // Check the pipeline
    List<BsonDocument> pipeline =
        command.toProxy().toBsonDocument().getArray("pipeline").getValues().stream()
            .map(BsonValue::asDocument)
            .toList();
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

    // Create a ChangeStreamAggregateCommandFactory with matchCollectionUuidForUpdateLookup and
    // generate a command
    ChangeStreamAggregateCommandFactory factory =
        new ChangeStreamAggregateCommandFactory(
            MOCK_INDEX_DEFINITION,
            new MongoNamespace(databaseName, collectionName),
            List.of(),
            true,
            false);
    ChangeStreamAggregateCommand command =
        factory.unpinned(
            MOCK_INDEX_DEFINITION.getIndexId(),
            ChangeStreamModeSelector.ChangeStreamMode.getDefault());

    // Check the pipeline
    List<BsonDocument> pipeline =
        command.toProxy().toBsonDocument().getArray("pipeline").getValues().stream()
            .map(BsonValue::asDocument)
            .toList();
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
  public void testSplitLargeEventStageEnabled() {
    String databaseName = "testDatabase";
    String collectionName = "testSplitLargeEventEnabled";

    // Create a ChangeStreamAggregateCommandFactory with split large event enabled
    ChangeStreamAggregateCommandFactory factory =
        new ChangeStreamAggregateCommandFactory(
            MOCK_INDEX_DEFINITION,
            new MongoNamespace(databaseName, collectionName),
            List.of(),
            false,
            true);
    ChangeStreamAggregateCommand command =
        factory.unpinned(
            MOCK_INDEX_DEFINITION.getIndexId(),
            ChangeStreamModeSelector.ChangeStreamMode.getDefault());

    // Check the pipeline
    List<BsonDocument> pipeline =
        command.toProxy().toBsonDocument().getArray("pipeline").getValues().stream()
            .map(BsonValue::asDocument)
            .toList();
    assertThat(pipeline).isNotEmpty();

    // The $changeStreamSplitLargeEvent stage should be the last one
    assertTrue(
        "Expected last stage to be $changeStreamSplitLargeEvent",
        pipeline.getLast().containsKey("$changeStreamSplitLargeEvent"));
  }

  @Test
  public void testSplitLargeEventStageDisabled() {
    String databaseName = "testDatabase";
    String collectionName = "testSplitLargeEventDisabled";

    // Create a ChangeStreamAggregateCommandFactory with split large event disabled
    ChangeStreamAggregateCommandFactory factory =
        new ChangeStreamAggregateCommandFactory(
            MOCK_INDEX_DEFINITION,
            new MongoNamespace(databaseName, collectionName),
            List.of(),
            false,
            false);
    ChangeStreamAggregateCommand command =
        factory.unpinned(
            MOCK_INDEX_DEFINITION.getIndexId(),
            ChangeStreamModeSelector.ChangeStreamMode.getDefault());

    // Check the pipeline
    List<BsonDocument> pipeline =
        command.toProxy().toBsonDocument().getArray("pipeline").getValues().stream()
            .map(BsonValue::asDocument)
            .toList();
    assertThat(pipeline).isNotEmpty();

    // The $changeStreamSplitLargeEvent stage should not be present
    assertTrue(
        "Expected no $changeStreamSplitLargeEvent stage",
        pipeline.stream().noneMatch(s -> s.containsKey("$changeStreamSplitLargeEvent")));
  }
}
