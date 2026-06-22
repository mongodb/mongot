package com.xgen.mongot.replication.mongodb.common;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.OperationType;
import com.mongodb.client.model.changestream.UpdateDescription;
import com.xgen.mongot.index.DocumentEvent;
import com.xgen.mongot.index.definition.MaterializedViewIndexDefinitionGeneration;
import com.xgen.mongot.index.definition.VectorIndexDefinition;
import com.xgen.mongot.index.definition.ViewDefinition;
import com.xgen.mongot.index.version.IndexFormatVersion;
import com.xgen.testing.mongot.index.definition.VectorIndexDefinitionBuilder;
import com.xgen.testing.mongot.replication.mongodb.ChangeStreamUtils;
import java.util.List;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.RawBsonDocument;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for filter-only update optimization in ChangeStreamDocumentUtils.
 *
 * <p>Verifies that filter-only updates are only created for EMBEDDING_MATERIALIZED_VIEW strategy
 * (AUTO_EMBED fields, version >= 2) and NOT for EMBEDDING strategy (TEXT fields, version 1).
 */
@RunWith(JUnit4.class)
public class ChangeStreamDocumentUtilsFilterOnlyUpdateTest {

  @Test
  public void testFilterOnlyUpdate_TextFieldIndex_NotCreated() {
    VectorIndexDefinition indexDef =
        VectorIndexDefinitionBuilder.builder()
            .withTextField("description")
            .withFilterPath("category")
            .build();
    assertEquals(1, indexDef.getParsedAutoEmbeddingFeatureVersion());
    assertFalse(MaterializedViewIndexDefinitionGeneration.isMaterializedViewBasedIndex(indexDef));

    ChangeStreamDocument<RawBsonDocument> changeEvent =
        createFilterOnlyUpdateEvent(indexDef, "category");
    var batch =
        ChangeStreamDocumentUtils.handleDocumentEvents(
            List.of(changeEvent),
            indexDef,
            indexDef.createFieldDefinitionResolver(IndexFormatVersion.CURRENT),
            false);

    assertEquals(1, batch.finalChangeEvents.size());
    assertFalse(batch.finalChangeEvents.getFirst().getFilterFieldUpdates().isPresent());
  }

  @Test
  public void testFilterOnlyUpdate_AutoEmbedFieldIndex_Created() {
    VectorIndexDefinition indexDef =
        VectorIndexDefinitionBuilder.builder()
            .withAutoEmbedField("description")
            .withFilterPath("category")
            .build();
    assertEquals(2, indexDef.getParsedAutoEmbeddingFeatureVersion());
    assertTrue(MaterializedViewIndexDefinitionGeneration.isMaterializedViewBasedIndex(indexDef));

    ChangeStreamDocument<RawBsonDocument> changeEvent =
        createFilterOnlyUpdateEvent(indexDef, "category");
    var batch =
        ChangeStreamDocumentUtils.handleDocumentEvents(
            List.of(changeEvent),
            indexDef,
            indexDef.createFieldDefinitionResolver(IndexFormatVersion.CURRENT),
            false);

    assertEquals(1, batch.finalChangeEvents.size());
    DocumentEvent event = batch.finalChangeEvents.getFirst();
    assertTrue(event.getFilterFieldUpdates().isPresent());
    assertEquals(new BsonString("updated"), event.getFilterFieldUpdates().get().get("category"));
  }

  @Test
  public void testVectorFieldUpdate_TextFieldIndex_NoFilterOnlyUpdate() {
    VectorIndexDefinition indexDef =
        VectorIndexDefinitionBuilder.builder()
            .withTextField("description")
            .withFilterPath("category")
            .build();
    assertFalse(MaterializedViewIndexDefinitionGeneration.isMaterializedViewBasedIndex(indexDef));

    ChangeStreamDocument<RawBsonDocument> changeEvent =
        createVectorFieldUpdateEvent(indexDef, "description");
    var batch =
        ChangeStreamDocumentUtils.handleDocumentEvents(
            List.of(changeEvent),
            indexDef,
            indexDef.createFieldDefinitionResolver(IndexFormatVersion.CURRENT),
            false);

    assertEquals(1, batch.finalChangeEvents.size());
    assertFalse(batch.finalChangeEvents.getFirst().getFilterFieldUpdates().isPresent());
  }

  @Test
  public void testVectorFieldUpdate_AutoEmbedFieldIndex_NoFilterOnlyUpdate() {
    VectorIndexDefinition indexDef =
        VectorIndexDefinitionBuilder.builder()
            .withAutoEmbedField("description")
            .withFilterPath("category")
            .build();
    assertTrue(MaterializedViewIndexDefinitionGeneration.isMaterializedViewBasedIndex(indexDef));

    ChangeStreamDocument<RawBsonDocument> changeEvent =
        createVectorFieldUpdateEvent(indexDef, "description");
    var batch =
        ChangeStreamDocumentUtils.handleDocumentEvents(
            List.of(changeEvent),
            indexDef,
            indexDef.createFieldDefinitionResolver(IndexFormatVersion.CURRENT),
            false);

    assertEquals(1, batch.finalChangeEvents.size());
    assertFalse(batch.finalChangeEvents.getFirst().getFilterFieldUpdates().isPresent());
  }

  @Test
  public void testFilterOnlyUpdate_AutoEmbedFieldIndexOnView_NotCreated() {
    // For view-based indexes, updateDescription reflects base-collection fields, so a
    // filter-field-only change may also be a $match transition into the view. The partial
    // update path issues a non-upsert write that silently drops such documents, so the
    // optimization must be disabled and a full update produced instead.
    VectorIndexDefinition indexDef =
        VectorIndexDefinitionBuilder.builder()
            .withAutoEmbedField("description")
            .withFilterPath("category")
            .view(
                ViewDefinition.existing(
                    "myView",
                    List.of(
                        BsonDocument.parse(
                            "{\"$match\": {\"$expr\": {\"$eq\": [\"$category\", \"updated\"]}}}"))))
            .build();
    assertTrue(MaterializedViewIndexDefinitionGeneration.isMaterializedViewBasedIndex(indexDef));

    ChangeStreamDocument<RawBsonDocument> changeEvent =
        createFilterOnlyUpdateEvent(indexDef, "category");
    var batch =
        ChangeStreamDocumentUtils.handleDocumentEvents(
            List.of(changeEvent),
            indexDef,
            indexDef.createFieldDefinitionResolver(IndexFormatVersion.CURRENT),
            false);

    assertEquals(1, batch.finalChangeEvents.size());
    assertFalse(batch.finalChangeEvents.getFirst().getFilterFieldUpdates().isPresent());
  }

  @Test
  public void testNonIndexedFieldUpdate_AutoEmbedFieldIndexOnView_NotCreated() {
    // The embed source can be computed by the view pipeline (e.g. $set from base fields),
    // in which case updateDescription never contains the embed field's name. An update to
    // a base field feeding the computed embed source must produce a full update so the
    // document is re-embedded.
    VectorIndexDefinition indexDef =
        VectorIndexDefinitionBuilder.builder()
            .withAutoEmbedField("description")
            .withFilterPath("category")
            .view(
                ViewDefinition.existing(
                    "myView",
                    List.of(
                        BsonDocument.parse(
                            "{\"$set\": {\"description\": {\"$concat\": [\"$title\","
                                + " \"$summary\"]}}}"))))
            .build();
    assertTrue(MaterializedViewIndexDefinitionGeneration.isMaterializedViewBasedIndex(indexDef));

    UpdateDescription updateDesc =
        new UpdateDescription(null, new BsonDocument("title", new BsonString("new title")));
    BsonDocument fullDocument =
        new BsonDocument()
            .append("_id", new BsonInt32(0))
            .append("title", new BsonString("new title"))
            .append("description", new BsonString("new title computed"))
            .append("category", new BsonString("updated"))
            .append(indexDef.getIndexId().toString(), new BsonDocument("_id", new BsonInt32(0)));
    ChangeStreamDocument<RawBsonDocument> changeEvent =
        ChangeStreamUtils.dataEvent(OperationType.UPDATE, 0, updateDesc, fullDocument);

    var batch =
        ChangeStreamDocumentUtils.handleDocumentEvents(
            List.of(changeEvent),
            indexDef,
            indexDef.createFieldDefinitionResolver(IndexFormatVersion.CURRENT),
            false);

    assertEquals(1, batch.finalChangeEvents.size());
    assertFalse(batch.finalChangeEvents.getFirst().getFilterFieldUpdates().isPresent());
  }

  /**
   * Creates a change stream update event where only a filter field changed. The fullDocument must
   * contain the filter field value for extractFilterFieldValues() to work.
   */
  private ChangeStreamDocument<RawBsonDocument> createFilterOnlyUpdateEvent(
      VectorIndexDefinition indexDef, String filterFieldName) {
    UpdateDescription updateDesc =
        new UpdateDescription(null, new BsonDocument(filterFieldName, new BsonString("updated")));
    // Create fullDocument with filter field value -
    // extractFilterFieldValues reads from fullDocument
    BsonDocument fullDocument =
        new BsonDocument()
            .append("_id", new BsonInt32(0))
            .append("description", new BsonString("machine learning"))
            .append(filterFieldName, new BsonString("updated"))
            .append(indexDef.getIndexId().toString(), new BsonDocument("_id", new BsonInt32(0)));
    return ChangeStreamUtils.dataEvent(OperationType.UPDATE, 0, updateDesc, fullDocument);
  }

  /** Creates a change stream update event where the vector/text field changed. */
  private ChangeStreamDocument<RawBsonDocument> createVectorFieldUpdateEvent(
      VectorIndexDefinition indexDef, String vectorFieldName) {
    UpdateDescription updateDesc =
        new UpdateDescription(
            null, new BsonDocument(vectorFieldName, new BsonString("deep learning")));
    return ChangeStreamUtils.updateEvent(0, updateDesc, indexDef);
  }
}
