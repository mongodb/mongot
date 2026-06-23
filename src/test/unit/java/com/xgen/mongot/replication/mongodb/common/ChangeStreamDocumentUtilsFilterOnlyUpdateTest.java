package com.xgen.mongot.replication.mongodb.common;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.OperationType;
import com.mongodb.client.model.changestream.UpdateDescription;
import com.xgen.mongot.embedding.utils.PartialEmbedFallbackReason;
import com.xgen.mongot.index.DocumentEvent;
import com.xgen.mongot.index.definition.MaterializedViewIndexDefinitionGeneration;
import com.xgen.mongot.index.definition.VectorIndexDefinition;
import com.xgen.mongot.index.definition.ViewDefinition;
import com.xgen.mongot.index.version.IndexFormatVersion;
import com.xgen.mongot.metrics.MetricsFactory;
import com.xgen.mongot.util.FieldPath;
import com.xgen.testing.mongot.index.definition.VectorIndexDefinitionBuilder;
import com.xgen.testing.mongot.replication.mongodb.ChangeStreamUtils;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.List;
import org.bson.BsonArray;
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
    // Filter-only changes need no embedding work — neither applied nor a full-re-index fallback.
    assertEquals(0, batch.partialEmbedApplied);
    assertTrue(batch.partialEmbedFallbacks.isEmpty());
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
  public void testPartialEmbedUpdate_EmbedOnlyIndex_EmptyFilterBodyStillPartial() {
    // An index with no filter fields: extractFilterFieldValues succeeds with an empty body. That's
    // distinct from extraction failing (empty Optional → full replace), so an embed-field change
    // must still produce a partial-embed update, not fall back to a full re-index.
    VectorIndexDefinition indexDef = VectorIndexDefinitionBuilder.builder()
        .withAutoEmbedField("description")
        .build();
    assertTrue(MaterializedViewIndexDefinitionGeneration.isMaterializedViewBasedIndex(indexDef));

    BsonDocument fullDocument = new BsonDocument()
        .append("_id", new BsonInt32(0))
        .append("description", new BsonString("deep learning"))
        .append(indexDef.getIndexId().toString(), new BsonDocument("_id", new BsonInt32(0)));
    UpdateDescription updateDesc = new UpdateDescription(
        null, new BsonDocument("description", new BsonString("deep learning")));
    ChangeStreamDocument<RawBsonDocument> changeEvent =
        ChangeStreamUtils.dataEvent(OperationType.UPDATE, 0, updateDesc, fullDocument);

    var batch = ChangeStreamDocumentUtils.handleDocumentEvents(
        List.of(changeEvent), indexDef,
        indexDef.createFieldDefinitionResolver(IndexFormatVersion.CURRENT), false);

    assertEquals(1, batch.finalChangeEvents.size());
    DocumentEvent event = batch.finalChangeEvents.getFirst();
    // Partial-embed: empty filter body but the changed embed path is carried for re-embedding.
    assertTrue(event.getFilterFieldUpdates().isPresent());
    assertTrue(event.getFilterFieldUpdates().get().isEmpty());
    assertTrue(event.getChangedEmbedTexts().containsKey(FieldPath.parse("description")));
    assertEquals(1, batch.partialEmbedApplied);
    assertTrue(batch.partialEmbedFallbacks.isEmpty());
  }

  @Test
  public void testVectorFieldUpdate_AutoEmbedFieldIndex_PartialEmbedInsteadOfFilterOnly() {
    // When a single auto-embed field changes on a MV-backed AUTO_EMBED index, the optimization
    // produces a partial-embed update — still does an embedding call (only for the changed field),
    // and the writer issues a $set instead of a full replacement so the unchanged embed fields
    // (if any) and unrelated MV fields are preserved verbatim.
    VectorIndexDefinition indexDef = VectorIndexDefinitionBuilder.builder()
        .withAutoEmbedField("description")
        .withFilterPath("category")
        .build();
    assertTrue(MaterializedViewIndexDefinitionGeneration.isMaterializedViewBasedIndex(indexDef));

    // fullDocument must carry the new embed text — classifier rejects partial-embed when the new
    // value is empty (would leave a stale MV vector).
    BsonDocument fullDocument = new BsonDocument()
        .append("_id", new BsonInt32(0))
        .append("description", new BsonString("deep learning"))
        .append("category", new BsonString("books"))
        .append(indexDef.getIndexId().toString(), new BsonDocument("_id", new BsonInt32(0)));
    UpdateDescription updateDesc = new UpdateDescription(
        null, new BsonDocument("description", new BsonString("deep learning")));
    ChangeStreamDocument<RawBsonDocument> changeEvent =
        ChangeStreamUtils.dataEvent(OperationType.UPDATE, 0, updateDesc, fullDocument);

    var batch = ChangeStreamDocumentUtils.handleDocumentEvents(
        List.of(changeEvent), indexDef,
        indexDef.createFieldDefinitionResolver(IndexFormatVersion.CURRENT), false);

    assertEquals(1, batch.finalChangeEvents.size());
    DocumentEvent event = batch.finalChangeEvents.getFirst();
    assertTrue(event.getFilterFieldUpdates().isPresent());
    assertFalse(event.getChangedEmbedTexts().isEmpty());
    assertTrue(event.getChangedEmbedTexts().containsKey(FieldPath.parse("description")));
    // Classified partial-embed-safe → counted as applied, no fallback.
    assertEquals(1, batch.partialEmbedApplied);
    assertTrue(batch.partialEmbedFallbacks.isEmpty());
  }

  @Test
  public void testPartialEmbedUpdate_FallsBackWhenNewEmbedValueIsEmpty() {
    // When the new value for the changed embed field is empty, the optimization can't $unset the
    // existing MV vector — fall back to full ReplaceOne.
    VectorIndexDefinition indexDef = VectorIndexDefinitionBuilder.builder()
        .withAutoEmbedField("description")
        .withFilterPath("category")
        .build();

    BsonDocument fullDocument = new BsonDocument()
        .append("_id", new BsonInt32(0))
        .append("description", new BsonString(""))
        .append("category", new BsonString("books"))
        .append(indexDef.getIndexId().toString(), new BsonDocument("_id", new BsonInt32(0)));
    UpdateDescription updateDesc = new UpdateDescription(
        null, new BsonDocument("description", new BsonString("")));
    ChangeStreamDocument<RawBsonDocument> changeEvent =
        ChangeStreamUtils.dataEvent(OperationType.UPDATE, 0, updateDesc, fullDocument);

    var batch = ChangeStreamDocumentUtils.handleDocumentEvents(
        List.of(changeEvent), indexDef,
        indexDef.createFieldDefinitionResolver(IndexFormatVersion.CURRENT), false);

    DocumentEvent event = batch.finalChangeEvents.getFirst();
    assertFalse(event.getFilterFieldUpdates().isPresent());
    assertTrue(event.getChangedEmbedTexts().isEmpty());
  }

  @Test
  public void testPartialEmbedUpdate_OnlyOneAutoEmbedFieldChanged() {
    VectorIndexDefinition indexDef = VectorIndexDefinitionBuilder.builder()
        .withAutoEmbedField("title")
        .withAutoEmbedField("description")
        .withFilterPath("category")
        .build();
    assertTrue(MaterializedViewIndexDefinitionGeneration.isMaterializedViewBasedIndex(indexDef));

    BsonDocument fullDocument = new BsonDocument()
        .append("_id", new BsonInt32(0))
        .append("title", new BsonString("changed title"))
        .append("description", new BsonString("unchanged description"))
        .append("category", new BsonString("books"))
        .append(indexDef.getIndexId().toString(), new BsonDocument("_id", new BsonInt32(0)));
    UpdateDescription updateDesc = new UpdateDescription(
        null, new BsonDocument("title", new BsonString("changed title")));
    ChangeStreamDocument<RawBsonDocument> changeEvent =
        ChangeStreamUtils.dataEvent(OperationType.UPDATE, 0, updateDesc, fullDocument);

    var batch = ChangeStreamDocumentUtils.handleDocumentEvents(
        List.of(changeEvent), indexDef,
        indexDef.createFieldDefinitionResolver(IndexFormatVersion.CURRENT), false);

    assertEquals(1, batch.finalChangeEvents.size());
    DocumentEvent event = batch.finalChangeEvents.getFirst();
    // Partial-embed events carry both a $set body (filter fields) AND the set of changed embed
    // paths whose vectors+hashes still need to be filled in by the embedding scheduler.
    assertTrue(event.getFilterFieldUpdates().isPresent());
    assertEquals(new BsonString("books"), event.getFilterFieldUpdates().get().get("category"));
    assertEquals(1, event.getChangedEmbedTexts().size());
    assertTrue(event.getChangedEmbedTexts().containsKey(FieldPath.parse("title")));
  }

  @Test
  public void testPartialEmbedUpdate_NotCreatedForTextFieldIndex() {
    VectorIndexDefinition indexDef = VectorIndexDefinitionBuilder.builder()
        .withTextField("title")
        .withTextField("description")
        .withFilterPath("category")
        .build();
    // version 1 → not MV-based, optimization disabled.
    assertEquals(1, indexDef.getParsedAutoEmbeddingFeatureVersion());
    assertFalse(MaterializedViewIndexDefinitionGeneration.isMaterializedViewBasedIndex(indexDef));

    BsonDocument fullDocument = new BsonDocument()
        .append("_id", new BsonInt32(0))
        .append("title", new BsonString("changed"))
        .append("description", new BsonString("unchanged"))
        .append("category", new BsonString("books"))
        .append(indexDef.getIndexId().toString(), new BsonDocument("_id", new BsonInt32(0)));
    UpdateDescription updateDesc = new UpdateDescription(
        null, new BsonDocument("title", new BsonString("changed")));
    ChangeStreamDocument<RawBsonDocument> changeEvent =
        ChangeStreamUtils.dataEvent(OperationType.UPDATE, 0, updateDesc, fullDocument);

    var batch = ChangeStreamDocumentUtils.handleDocumentEvents(
        List.of(changeEvent), indexDef,
        indexDef.createFieldDefinitionResolver(IndexFormatVersion.CURRENT), false);

    DocumentEvent event = batch.finalChangeEvents.getFirst();
    assertFalse(event.getFilterFieldUpdates().isPresent());
    assertTrue(event.getChangedEmbedTexts().isEmpty());
  }

  @Test
  public void testPartialEmbedUpdate_FallsBackToFullReplaceWhenEmbedFieldRemoved() {
    VectorIndexDefinition indexDef = VectorIndexDefinitionBuilder.builder()
        .withAutoEmbedField("title")
        .withAutoEmbedField("description")
        .withFilterPath("category")
        .build();

    BsonDocument fullDocument = new BsonDocument()
        .append("_id", new BsonInt32(0))
        .append("description", new BsonString("kept"))
        .append("category", new BsonString("books"))
        .append(indexDef.getIndexId().toString(), new BsonDocument("_id", new BsonInt32(0)));
    UpdateDescription updateDesc =
        new UpdateDescription(List.of("title"), new BsonDocument());
    ChangeStreamDocument<RawBsonDocument> changeEvent =
        ChangeStreamUtils.dataEvent(OperationType.UPDATE, 0, updateDesc, fullDocument);

    var batch = ChangeStreamDocumentUtils.handleDocumentEvents(
        List.of(changeEvent), indexDef,
        indexDef.createFieldDefinitionResolver(IndexFormatVersion.CURRENT), false);

    DocumentEvent event = batch.finalChangeEvents.getFirst();
    // Removed embed field → fall back to full ReplaceOne, no $set, no pending embed paths.
    assertFalse(event.getFilterFieldUpdates().isPresent());
    assertTrue(event.getChangedEmbedTexts().isEmpty());
    // ...and the fallback is counted under its reason (a removed indexed field is destructive).
    assertEquals(0, batch.partialEmbedApplied);
    assertEquals(
        Integer.valueOf(1),
        batch.partialEmbedFallbacks.get(PartialEmbedFallbackReason.DESTRUCTIVE_INDEXED_TOUCH));
  }

  @Test
  public void testPartialEmbedUpdate_FallsBackWhenEmbedPathTraversesArrayInSource() {
    // Declared embed path is "items.title"; source doc has items as an array.
    // Dotted-key $set against _autoEmbed.items.title would fail on arrays, so the classifier
    // should refuse partial-embed and fall back to a full ReplaceOne.
    VectorIndexDefinition indexDef = VectorIndexDefinitionBuilder.builder()
        .withAutoEmbedField("items.title")
        .withFilterPath("category")
        .build();

    BsonArray itemsArray = new BsonArray();
    itemsArray.add(new BsonDocument("title", new BsonString("a")));
    itemsArray.add(new BsonDocument("title", new BsonString("b")));
    BsonDocument fullDocument = new BsonDocument()
        .append("_id", new BsonInt32(0))
        .append("items", itemsArray)
        .append("category", new BsonString("books"))
        .append(indexDef.getIndexId().toString(), new BsonDocument("_id", new BsonInt32(0)));
    UpdateDescription updateDesc = new UpdateDescription(
        null, new BsonDocument("items.title", new BsonString("b")));
    ChangeStreamDocument<RawBsonDocument> changeEvent =
        ChangeStreamUtils.dataEvent(OperationType.UPDATE, 0, updateDesc, fullDocument);

    var batch = ChangeStreamDocumentUtils.handleDocumentEvents(
        List.of(changeEvent), indexDef,
        indexDef.createFieldDefinitionResolver(IndexFormatVersion.CURRENT), false);

    DocumentEvent event = batch.finalChangeEvents.getFirst();
    assertFalse(event.getFilterFieldUpdates().isPresent());
    assertTrue(event.getChangedEmbedTexts().isEmpty());
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

  @Test
  public void testReplaceEvent_AutoEmbedFieldIndex_CountedAsFallback() {
    // A REPLACE re-embeds the whole document — the same full re-index the partial-embed
    // optimization avoids on UPDATE. It's counted under REPLACE_EVENT so the metric captures every
    // full re-index, and the PartialEmbedMetricCounters holder surfaces it as a process-wide
    // Prometheus counter tagged by reason (CLOUDP-412617).
    VectorIndexDefinition indexDef = VectorIndexDefinitionBuilder.builder()
        .withAutoEmbedField("description")
        .withFilterPath("category")
        .build();
    BsonDocument fullDocument = new BsonDocument()
        .append("_id", new BsonInt32(0))
        .append("description", new BsonString("deep learning"))
        .append("category", new BsonString("books"))
        .append(indexDef.getIndexId().toString(), new BsonDocument("_id", new BsonInt32(0)));
    ChangeStreamDocument<RawBsonDocument> changeEvent =
        ChangeStreamUtils.dataEvent(OperationType.REPLACE, 0, null, fullDocument);

    var batch = ChangeStreamDocumentUtils.handleDocumentEvents(
        List.of(changeEvent), indexDef,
        indexDef.createFieldDefinitionResolver(IndexFormatVersion.CURRENT), false);

    // REPLACE always produces a full update, never a partial $set.
    assertEquals(1, batch.finalChangeEvents.size());
    assertFalse(batch.finalChangeEvents.getFirst().getFilterFieldUpdates().isPresent());
    assertEquals(0, batch.partialEmbedApplied);
    assertEquals(
        Integer.valueOf(1),
        batch.partialEmbedFallbacks.get(PartialEmbedFallbackReason.REPLACE_EVENT));

    // The holder turns the per-batch tally into a process-wide counter tagged by reason. Asserting
    // through MetricsFactory pins the Prometheus-visible metric name + reason tag.
    MetricsFactory metricsFactory = new MetricsFactory("test", new SimpleMeterRegistry());
    new PartialEmbedMetricCounters(metricsFactory, Tags.empty()).recordBatch(batch);
    assertEquals(
        1.0,
        metricsFactory
            .get("partialEmbedFallback", Tags.of("reason", "replace_event"))
            .counter()
            .count(),
        0.0);
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
