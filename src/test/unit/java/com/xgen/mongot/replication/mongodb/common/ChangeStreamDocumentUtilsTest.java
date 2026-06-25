package com.xgen.mongot.replication.mongodb.common;

import static com.google.common.truth.Truth.assertThat;
import static com.xgen.testing.mongot.mock.index.SearchIndex.MOCK_INDEX_DEFINITION;
import static com.xgen.testing.mongot.mock.index.SearchIndex.mockDefinitionBuilder;

import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.UpdateDescription;
import com.xgen.mongot.index.definition.IllegalEmbeddedFieldException;
import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.version.IndexFormatVersion;
import com.xgen.testing.mongot.index.definition.DocumentFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.FieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.VectorIndexDefinitionBuilder;
import com.xgen.testing.mongot.replication.mongodb.ChangeStreamUtils;
import java.util.List;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.RawBsonDocument;
import org.junit.Assert;
import org.junit.Test;

public class ChangeStreamDocumentUtilsTest {

  @Test
  public void testInsertEvent() {
    runEncodeDecodeTest(ChangeStreamUtils.insertEvent(0, MOCK_INDEX_DEFINITION));
  }

  @Test
  public void testUpdateEvent() {
    runEncodeDecodeTest(
        ChangeStreamUtils.updateEvent(
            0,
            new UpdateDescription(null, new BsonDocument("123", new BsonString("new"))),
            MOCK_INDEX_DEFINITION));
  }

  @Test
  public void testUpdateEventWithEmptyMetadata() {
    runEncodeDecodeTest(
        ChangeStreamUtils.updateEventWithEmptyMetadata(
            0,
            new UpdateDescription(null, new BsonDocument("123", new BsonString("new"))),
            MOCK_INDEX_DEFINITION));
  }

  @Test
  public void testReplaceEvent() {
    runEncodeDecodeTest(ChangeStreamUtils.replaceEvent(0, MOCK_INDEX_DEFINITION));
  }

  @Test
  public void testDeleteEvent() {
    runEncodeDecodeTest(ChangeStreamUtils.deleteEvent(0, MOCK_INDEX_DEFINITION));
  }

  @Test
  public void testInvalidateEvent() {
    runEncodeDecodeTest(ChangeStreamUtils.invalidateEvent(0));
  }

  @Test
  public void testRenameEvent() {
    runEncodeDecodeTest(ChangeStreamUtils.renameEvent(0));
  }

  @Test
  public void testDropEvent() {
    runEncodeDecodeTest(ChangeStreamUtils.dropEvent(0));
  }

  @Test
  public void testDropDatabaseEvent() {
    runEncodeDecodeTest(ChangeStreamUtils.dropDatabaseEvent(0));
  }

  @Test
  public void testOtherEvent() {
    runEncodeDecodeTest(ChangeStreamUtils.otherEvent(0));
  }

  @Test
  public void testIndexOfLifecycleEvent() {
    {
      var allIndexable =
          ChangeStreamUtils.toRawBsonDocuments(
              List.of(
                  ChangeStreamUtils.insertEvent(0, MOCK_INDEX_DEFINITION),
                  ChangeStreamUtils.insertEvent(1, MOCK_INDEX_DEFINITION),
                  ChangeStreamUtils.updateEvent(2, null, MOCK_INDEX_DEFINITION),
                  ChangeStreamUtils.deleteEvent(3, MOCK_INDEX_DEFINITION),
                  ChangeStreamUtils.replaceEvent(4, MOCK_INDEX_DEFINITION)));
      Assert.assertEquals(5, ChangeStreamDocumentUtils.indexOfLifecycleEvent(allIndexable));
    }

    {
      var empty = ChangeStreamUtils.toRawBsonDocuments(List.of());
      Assert.assertEquals(0, ChangeStreamDocumentUtils.indexOfLifecycleEvent(empty));
    }

    {
      var singleIndexable =
          ChangeStreamUtils.toRawBsonDocuments(
              List.of(ChangeStreamUtils.insertEvent(0, MOCK_INDEX_DEFINITION)));
      Assert.assertEquals(1, ChangeStreamDocumentUtils.indexOfLifecycleEvent(singleIndexable));
    }

    {
      var singleNonIndexable =
          ChangeStreamUtils.toRawBsonDocuments(List.of(ChangeStreamUtils.invalidateEvent(0)));
      Assert.assertEquals(0, ChangeStreamDocumentUtils.indexOfLifecycleEvent(singleNonIndexable));
    }

    {
      var nonIndexable =
          ChangeStreamUtils.toRawBsonDocuments(
              List.of(ChangeStreamUtils.renameEvent(0), ChangeStreamUtils.invalidateEvent(1)));
      Assert.assertEquals(0, ChangeStreamDocumentUtils.indexOfLifecycleEvent(nonIndexable));
    }

    {
      var dropAtEnd =
          ChangeStreamUtils.toRawBsonDocuments(
              List.of(
                  ChangeStreamUtils.insertEvent(0, MOCK_INDEX_DEFINITION),
                  ChangeStreamUtils.insertEvent(1, MOCK_INDEX_DEFINITION),
                  ChangeStreamUtils.dropEvent(2),
                  ChangeStreamUtils.invalidateEvent(3)));
      Assert.assertEquals(2, ChangeStreamDocumentUtils.indexOfLifecycleEvent(dropAtEnd));
    }

    {
      var dropDbAtEnd =
          ChangeStreamUtils.toRawBsonDocuments(
              List.of(
                  ChangeStreamUtils.insertEvent(0, MOCK_INDEX_DEFINITION),
                  ChangeStreamUtils.deleteEvent(1, MOCK_INDEX_DEFINITION),
                  ChangeStreamUtils.updateEvent(2, null, MOCK_INDEX_DEFINITION),
                  ChangeStreamUtils.dropDatabaseEvent(3),
                  ChangeStreamUtils.invalidateEvent(4)));
      Assert.assertEquals(3, ChangeStreamDocumentUtils.indexOfLifecycleEvent(dropDbAtEnd));
    }

    {
      var renameAtEnd =
          ChangeStreamUtils.toRawBsonDocuments(
              List.of(
                  ChangeStreamUtils.insertEvent(0, MOCK_INDEX_DEFINITION),
                  ChangeStreamUtils.renameEvent(1),
                  ChangeStreamUtils.invalidateEvent(2)));
      Assert.assertEquals(1, ChangeStreamDocumentUtils.indexOfLifecycleEvent(renameAtEnd));
    }

    {
      var invalidateAtEnd =
          ChangeStreamUtils.toRawBsonDocuments(
              List.of(
                  ChangeStreamUtils.insertEvent(0, MOCK_INDEX_DEFINITION),
                  ChangeStreamUtils.updateEvent(1, null, MOCK_INDEX_DEFINITION),
                  ChangeStreamUtils.invalidateEvent(2)));
      Assert.assertEquals(2, ChangeStreamDocumentUtils.indexOfLifecycleEvent(invalidateAtEnd));
    }

    {
      var otherAtEnd =
          ChangeStreamUtils.toRawBsonDocuments(
              List.of(
                  ChangeStreamUtils.insertEvent(0, MOCK_INDEX_DEFINITION),
                  ChangeStreamUtils.insertEvent(1, MOCK_INDEX_DEFINITION),
                  ChangeStreamUtils.deleteEvent(2, MOCK_INDEX_DEFINITION),
                  ChangeStreamUtils.otherEvent(3)));
      Assert.assertEquals(3, ChangeStreamDocumentUtils.indexOfLifecycleEvent(otherAtEnd));
    }

    {
      var renameAtMiddle =
          ChangeStreamUtils.toRawBsonDocuments(
              List.of(
                  ChangeStreamUtils.insertEvent(0, MOCK_INDEX_DEFINITION),
                  ChangeStreamUtils.renameEvent(1),
                  ChangeStreamUtils.insertEvent(2, MOCK_INDEX_DEFINITION)));
      Assert.assertEquals(3, ChangeStreamDocumentUtils.indexOfLifecycleEvent(renameAtMiddle));
    }

    {
      var invalidateAtMiddle =
          ChangeStreamUtils.toRawBsonDocuments(
              List.of(
                  ChangeStreamUtils.insertEvent(0, MOCK_INDEX_DEFINITION),
                  ChangeStreamUtils.invalidateEvent(1),
                  ChangeStreamUtils.insertEvent(2, MOCK_INDEX_DEFINITION)));
      Assert.assertEquals(3, ChangeStreamDocumentUtils.indexOfLifecycleEvent(invalidateAtMiddle));
    }

    {
      var otherAtMiddle =
          ChangeStreamUtils.toRawBsonDocuments(
              List.of(
                  ChangeStreamUtils.insertEvent(0, MOCK_INDEX_DEFINITION),
                  ChangeStreamUtils.otherEvent(1),
                  ChangeStreamUtils.insertEvent(2, MOCK_INDEX_DEFINITION)));
      Assert.assertEquals(3, ChangeStreamDocumentUtils.indexOfLifecycleEvent(otherAtMiddle));
    }
  }

  @Test
  public void testUpdateEventsNotPrefiltered() throws IllegalEmbeddedFieldException {
    var indexDefinition = createMockIndexDefinitionWithField("field");
    var inapplicableUpdate = createInapplicableUpdateEvent(0, indexDefinition);
    var applicableUpdate = createApplicableUpdateEvent(1, indexDefinition);

    var batch =
        ChangeStreamDocumentUtils.handleDocumentEvents(
            List.of(inapplicableUpdate, applicableUpdate),
            indexDefinition,
            indexDefinition.createFieldDefinitionResolver(IndexFormatVersion.CURRENT),
            false /* areUpdateEventsPrefiltered */,
            ChangeStreamDocumentUtils.deriveViewFieldDependencies(indexDefinition));
    // Only the applicable update should be present if we indicate that the batch has not been
    // prefiltered.
    assertThat(batch.finalChangeEvents).hasSize(1);
    assertThat(batch.finalChangeEvents.getFirst().getDocument()).isPresent();
    assertThat(batch.finalChangeEvents.getFirst().getDocument().get())
        .isEqualTo(applicableUpdate.getFullDocument());
  }

  @Test
  public void testUpdateEventsPrefiltered() throws IllegalEmbeddedFieldException {
    var indexDefinition = createMockIndexDefinitionWithField("field");
    var inapplicableUpdate = createInapplicableUpdateEvent(0, indexDefinition);
    var applicableUpdate = createApplicableUpdateEvent(1, indexDefinition);

    var batch =
        ChangeStreamDocumentUtils.handleDocumentEvents(
            List.of(inapplicableUpdate, applicableUpdate),
            indexDefinition,
            indexDefinition.createFieldDefinitionResolver(IndexFormatVersion.CURRENT),
            true /* areUpdateEventsPrefiltered */,
            ChangeStreamDocumentUtils.deriveViewFieldDependencies(indexDefinition));
    // Both updates should be present if we indicate that the batch has been prefiltered.
    assertThat(batch.finalChangeEvents).hasSize(2);
    assertThat(batch.finalChangeEvents.getFirst().getDocument()).isPresent();
    assertThat(batch.finalChangeEvents.getLast().getDocument()).isPresent();
    assertThat(batch.finalChangeEvents.stream().map(e -> e.getDocument().get()).toList())
        .containsExactly(inapplicableUpdate.getFullDocument(), applicableUpdate.getFullDocument());
  }

  private IndexDefinition createMockIndexDefinitionWithField(String fieldName)
      throws IllegalEmbeddedFieldException {
    return mockDefinitionBuilder()
        .mappings(
            DocumentFieldDefinitionBuilder.builder()
                .dynamic(false)
                .field(
                    fieldName,
                    FieldDefinitionBuilder.builder()
                        .document(DocumentFieldDefinitionBuilder.builder().dynamic(true).build())
                        .build())
                .build())
        .build();
  }

  private ChangeStreamDocument<RawBsonDocument> createApplicableUpdateEvent(
      int id, IndexDefinition indexDefinition) {
    assertThat(indexDefinition.asSearchDefinition().getMappings().fields().keySet()).isNotEmpty();
    return ChangeStreamUtils.updateEvent(
        id,
        new UpdateDescription(
            indexDefinition.asSearchDefinition().getMappings().fields().keySet().stream().toList(),
            null),
        indexDefinition);
  }

  private ChangeStreamDocument<RawBsonDocument> createInapplicableUpdateEvent(
      int id, IndexDefinition indexDefinition) {
    return ChangeStreamUtils.updateEvent(id, new UpdateDescription(null, null), indexDefinition);
  }

  private void runEncodeDecodeTest(ChangeStreamDocument<RawBsonDocument> doc) {
    RawBsonDocument bsonDoc = ChangeStreamDocumentUtils.changeStreamDocumentToBsonDocument(doc);
    ChangeStreamDocument<RawBsonDocument> recreatedChangeStreamDoc =
        ChangeStreamDocumentUtils.bsonDocumentToChangeStreamDocument(bsonDoc);
    Assert.assertEquals(doc, recreatedChangeStreamDoc);
  }

  /**
   * Tests that updates to non-indexed fields in auto-embedding indexes are skipped entirely. This
   * is a defensive check for non-indexed field updates that may still arrive in ALL_FIELDS mode.
   */
  @Test
  public void testAutoEmbeddingSkipsNonIndexedFieldUpdates() throws IllegalEmbeddedFieldException {
    // Create an auto-embedding index with an auto-embed field and a filter field
    var autoEmbedIndexDefinition =
        VectorIndexDefinitionBuilder.builder()
            .withAutoEmbedField("description")
            .withFilterPath("category")
            .build();

    // Create an update event where only a non-indexed field changed (not description or category)
    var nonIndexedFieldUpdate =
        createNonIndexedFieldUpdateEvent(0, autoEmbedIndexDefinition, "nonIndexedField");

    var batch =
        ChangeStreamDocumentUtils.handleDocumentEvents(
            List.of(nonIndexedFieldUpdate),
            autoEmbedIndexDefinition,
            autoEmbedIndexDefinition.createFieldDefinitionResolver(IndexFormatVersion.CURRENT),
            false /* areUpdateEventsPrefiltered */,
            ChangeStreamDocumentUtils.deriveViewFieldDependencies(autoEmbedIndexDefinition));

    // The update should be skipped entirely since only non-indexed fields changed
    assertThat(batch.finalChangeEvents).isEmpty();
  }

  private ChangeStreamDocument<RawBsonDocument> createNonIndexedFieldUpdateEvent(
      int id, IndexDefinition indexDefinition, String nonIndexedFieldName) {
    // Create an update description that only updates a non-indexed field
    var updateDescription =
        new UpdateDescription(null, new BsonDocument(nonIndexedFieldName, new BsonString("value")));
    return ChangeStreamUtils.updateEvent(id, updateDescription, indexDefinition);
  }
}
