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
import org.bson.BsonBoolean;
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
            false,
            ChangeStreamDocumentUtils.deriveViewFieldDependencies(indexDef));

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
            false,
            ChangeStreamDocumentUtils.deriveViewFieldDependencies(indexDef));

    assertEquals(1, batch.finalChangeEvents.size());
    DocumentEvent event = batch.finalChangeEvents.getFirst();
    assertTrue(event.getFilterFieldUpdates().isPresent());
    assertEquals(new BsonString("updated"), event.getFilterFieldUpdates().get().get("category"));
    // Filter-only changes need no embedding work — neither applied nor a full-re-index fallback.
    assertEquals(0, batch.partialEmbedApplied);
    assertTrue(batch.partialEmbedFallbacks.isEmpty());
  }

  @Test
  public void testFilterOnlyUpdate_NestedFilterUnderNestedRoot_NotCreated() {
    // CLOUDP-402522: filter fields under a nestedRoot are co-located with the embedded vector in
    // the materialized view (e.g. _autoEmbed.sections.section_name). The filter-only partial-update
    // path writes filters at their source paths only, so a partial $set would leave the co-located
    // filter stale and break nested $vectorSearch queries that combine `filter` with a path under
    // the nestedRoot. The optimization must be disabled and a full update produced instead.
    VectorIndexDefinition indexDef =
        VectorIndexDefinitionBuilder.builder()
            .withAutoEmbedField("sections.section_content")
            .withFilterPath("sections.section_name")
            .nestedRoot("sections")
            .build();
    assertTrue(MaterializedViewIndexDefinitionGeneration.isMaterializedViewBasedIndex(indexDef));

    UpdateDescription updateDesc =
        new UpdateDescription(
            null, new BsonDocument("sections.0.section_name", new BsonString("updated")));
    BsonDocument fullDocument =
        new BsonDocument()
            .append("_id", new BsonInt32(0))
            .append(
                "sections",
                new BsonArray(
                    List.of(
                        new BsonDocument()
                            .append("section_content", new BsonString("machine learning"))
                            .append("section_name", new BsonString("updated")))))
            .append(indexDef.getIndexId().toString(), new BsonDocument("_id", new BsonInt32(0)));
    ChangeStreamDocument<RawBsonDocument> changeEvent =
        ChangeStreamUtils.dataEvent(OperationType.UPDATE, 0, updateDesc, fullDocument);

    var batch =
        ChangeStreamDocumentUtils.handleDocumentEvents(
            List.of(changeEvent),
            indexDef,
            indexDef.createFieldDefinitionResolver(IndexFormatVersion.CURRENT),
            false,
            ChangeStreamDocumentUtils.deriveViewFieldDependencies(indexDef));

    assertEquals(1, batch.finalChangeEvents.size());
    assertFalse(batch.finalChangeEvents.getFirst().getFilterFieldUpdates().isPresent());
    // The skipped partial update is tracked as a NESTED_FILTER_REMAPPING full-replace fallback.
    assertEquals(
        Integer.valueOf(1),
        batch.partialEmbedFallbacks.get(PartialEmbedFallbackReason.NESTED_FILTER_REMAPPING));
  }

  @Test
  public void testFilterOnlyUpdate_TopLevelFilterWithNestedRoot_Created() {
    // A top-level filter is not co-located under the MV nestedRoot, so the partial-update
    // optimization stays safe even when the index has a nestedRoot for its embed field. The guard
    // must only disable the optimization for filters that live under the nestedRoot.
    VectorIndexDefinition indexDef =
        VectorIndexDefinitionBuilder.builder()
            .withAutoEmbedField("sections.section_content")
            .withFilterPath("category")
            .nestedRoot("sections")
            .build();
    assertTrue(MaterializedViewIndexDefinitionGeneration.isMaterializedViewBasedIndex(indexDef));

    UpdateDescription updateDesc =
        new UpdateDescription(null, new BsonDocument("category", new BsonString("updated")));
    BsonDocument fullDocument =
        new BsonDocument()
            .append("_id", new BsonInt32(0))
            .append(
                "sections",
                new BsonArray(
                    List.of(
                        new BsonDocument()
                            .append("section_content", new BsonString("machine learning")))))
            .append("category", new BsonString("updated"))
            .append(indexDef.getIndexId().toString(), new BsonDocument("_id", new BsonInt32(0)));
    ChangeStreamDocument<RawBsonDocument> changeEvent =
        ChangeStreamUtils.dataEvent(OperationType.UPDATE, 0, updateDesc, fullDocument);

    var batch =
        ChangeStreamDocumentUtils.handleDocumentEvents(
            List.of(changeEvent),
            indexDef,
            indexDef.createFieldDefinitionResolver(IndexFormatVersion.CURRENT),
            false,
            ChangeStreamDocumentUtils.deriveViewFieldDependencies(indexDef));

    assertEquals(1, batch.finalChangeEvents.size());
    DocumentEvent event = batch.finalChangeEvents.getFirst();
    assertTrue(event.getFilterFieldUpdates().isPresent());
    assertEquals(new BsonString("updated"), event.getFilterFieldUpdates().get().get("category"));
    // A top-level filter stays partial, so no NESTED_FILTER_REMAPPING fallback is recorded.
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
            false,
            ChangeStreamDocumentUtils.deriveViewFieldDependencies(indexDef));

    assertEquals(1, batch.finalChangeEvents.size());
    assertFalse(batch.finalChangeEvents.getFirst().getFilterFieldUpdates().isPresent());
  }

  @Test
  public void testPartialEmbedUpdate_EmbedOnlyIndex_EmptyFilterBodyStillPartial() {
    // An index with no filter fields: extractFilterFieldValues succeeds with an empty body. That's
    // distinct from extraction failing (empty Optional → full replace), so an embed-field change
    // must still produce a partial-embed update, not fall back to a full re-index.
    VectorIndexDefinition indexDef =
        VectorIndexDefinitionBuilder.builder().withAutoEmbedField("description").build();
    assertTrue(MaterializedViewIndexDefinitionGeneration.isMaterializedViewBasedIndex(indexDef));

    BsonDocument fullDocument =
        new BsonDocument()
            .append("_id", new BsonInt32(0))
            .append("description", new BsonString("deep learning"))
            .append(indexDef.getIndexId().toString(), new BsonDocument("_id", new BsonInt32(0)));
    UpdateDescription updateDesc =
        new UpdateDescription(
            null, new BsonDocument("description", new BsonString("deep learning")));
    ChangeStreamDocument<RawBsonDocument> changeEvent =
        ChangeStreamUtils.dataEvent(OperationType.UPDATE, 0, updateDesc, fullDocument);

    var batch =
        ChangeStreamDocumentUtils.handleDocumentEvents(
            List.of(changeEvent),
            indexDef,
            indexDef.createFieldDefinitionResolver(IndexFormatVersion.CURRENT),
            false,
            ChangeStreamDocumentUtils.deriveViewFieldDependencies(indexDef));

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
    VectorIndexDefinition indexDef =
        VectorIndexDefinitionBuilder.builder()
            .withAutoEmbedField("description")
            .withFilterPath("category")
            .build();
    assertTrue(MaterializedViewIndexDefinitionGeneration.isMaterializedViewBasedIndex(indexDef));

    // fullDocument must carry the new embed text — classifier rejects partial-embed when the new
    // value is empty (would leave a stale MV vector).
    BsonDocument fullDocument =
        new BsonDocument()
            .append("_id", new BsonInt32(0))
            .append("description", new BsonString("deep learning"))
            .append("category", new BsonString("books"))
            .append(indexDef.getIndexId().toString(), new BsonDocument("_id", new BsonInt32(0)));
    UpdateDescription updateDesc =
        new UpdateDescription(
            null, new BsonDocument("description", new BsonString("deep learning")));
    ChangeStreamDocument<RawBsonDocument> changeEvent =
        ChangeStreamUtils.dataEvent(OperationType.UPDATE, 0, updateDesc, fullDocument);

    var batch =
        ChangeStreamDocumentUtils.handleDocumentEvents(
            List.of(changeEvent),
            indexDef,
            indexDef.createFieldDefinitionResolver(IndexFormatVersion.CURRENT),
            false,
            ChangeStreamDocumentUtils.deriveViewFieldDependencies(indexDef));

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
    VectorIndexDefinition indexDef =
        VectorIndexDefinitionBuilder.builder()
            .withAutoEmbedField("description")
            .withFilterPath("category")
            .build();

    BsonDocument fullDocument =
        new BsonDocument()
            .append("_id", new BsonInt32(0))
            .append("description", new BsonString(""))
            .append("category", new BsonString("books"))
            .append(indexDef.getIndexId().toString(), new BsonDocument("_id", new BsonInt32(0)));
    UpdateDescription updateDesc =
        new UpdateDescription(null, new BsonDocument("description", new BsonString("")));
    ChangeStreamDocument<RawBsonDocument> changeEvent =
        ChangeStreamUtils.dataEvent(OperationType.UPDATE, 0, updateDesc, fullDocument);

    var batch =
        ChangeStreamDocumentUtils.handleDocumentEvents(
            List.of(changeEvent),
            indexDef,
            indexDef.createFieldDefinitionResolver(IndexFormatVersion.CURRENT),
            false,
            ChangeStreamDocumentUtils.deriveViewFieldDependencies(indexDef));

    DocumentEvent event = batch.finalChangeEvents.getFirst();
    assertFalse(event.getFilterFieldUpdates().isPresent());
    assertTrue(event.getChangedEmbedTexts().isEmpty());
  }

  @Test
  public void testPartialEmbedUpdate_OnlyOneAutoEmbedFieldChanged() {
    VectorIndexDefinition indexDef =
        VectorIndexDefinitionBuilder.builder()
            .withAutoEmbedField("title")
            .withAutoEmbedField("description")
            .withFilterPath("category")
            .build();
    assertTrue(MaterializedViewIndexDefinitionGeneration.isMaterializedViewBasedIndex(indexDef));

    BsonDocument fullDocument =
        new BsonDocument()
            .append("_id", new BsonInt32(0))
            .append("title", new BsonString("changed title"))
            .append("description", new BsonString("unchanged description"))
            .append("category", new BsonString("books"))
            .append(indexDef.getIndexId().toString(), new BsonDocument("_id", new BsonInt32(0)));
    UpdateDescription updateDesc =
        new UpdateDescription(null, new BsonDocument("title", new BsonString("changed title")));
    ChangeStreamDocument<RawBsonDocument> changeEvent =
        ChangeStreamUtils.dataEvent(OperationType.UPDATE, 0, updateDesc, fullDocument);

    var batch =
        ChangeStreamDocumentUtils.handleDocumentEvents(
            List.of(changeEvent),
            indexDef,
            indexDef.createFieldDefinitionResolver(IndexFormatVersion.CURRENT),
            false,
            ChangeStreamDocumentUtils.deriveViewFieldDependencies(indexDef));

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
    VectorIndexDefinition indexDef =
        VectorIndexDefinitionBuilder.builder()
            .withTextField("title")
            .withTextField("description")
            .withFilterPath("category")
            .build();
    // version 1 → not MV-based, optimization disabled.
    assertEquals(1, indexDef.getParsedAutoEmbeddingFeatureVersion());
    assertFalse(MaterializedViewIndexDefinitionGeneration.isMaterializedViewBasedIndex(indexDef));

    BsonDocument fullDocument =
        new BsonDocument()
            .append("_id", new BsonInt32(0))
            .append("title", new BsonString("changed"))
            .append("description", new BsonString("unchanged"))
            .append("category", new BsonString("books"))
            .append(indexDef.getIndexId().toString(), new BsonDocument("_id", new BsonInt32(0)));
    UpdateDescription updateDesc =
        new UpdateDescription(null, new BsonDocument("title", new BsonString("changed")));
    ChangeStreamDocument<RawBsonDocument> changeEvent =
        ChangeStreamUtils.dataEvent(OperationType.UPDATE, 0, updateDesc, fullDocument);

    var batch =
        ChangeStreamDocumentUtils.handleDocumentEvents(
            List.of(changeEvent),
            indexDef,
            indexDef.createFieldDefinitionResolver(IndexFormatVersion.CURRENT),
            false,
            ChangeStreamDocumentUtils.deriveViewFieldDependencies(indexDef));

    DocumentEvent event = batch.finalChangeEvents.getFirst();
    assertFalse(event.getFilterFieldUpdates().isPresent());
    assertTrue(event.getChangedEmbedTexts().isEmpty());
  }

  @Test
  public void testPartialEmbedUpdate_FallsBackToFullReplaceWhenEmbedFieldRemoved() {
    VectorIndexDefinition indexDef =
        VectorIndexDefinitionBuilder.builder()
            .withAutoEmbedField("title")
            .withAutoEmbedField("description")
            .withFilterPath("category")
            .build();

    BsonDocument fullDocument =
        new BsonDocument()
            .append("_id", new BsonInt32(0))
            .append("description", new BsonString("kept"))
            .append("category", new BsonString("books"))
            .append(indexDef.getIndexId().toString(), new BsonDocument("_id", new BsonInt32(0)));
    UpdateDescription updateDesc = new UpdateDescription(List.of("title"), new BsonDocument());
    ChangeStreamDocument<RawBsonDocument> changeEvent =
        ChangeStreamUtils.dataEvent(OperationType.UPDATE, 0, updateDesc, fullDocument);

    var batch =
        ChangeStreamDocumentUtils.handleDocumentEvents(
            List.of(changeEvent),
            indexDef,
            indexDef.createFieldDefinitionResolver(IndexFormatVersion.CURRENT),
            false,
            ChangeStreamDocumentUtils.deriveViewFieldDependencies(indexDef));

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
    VectorIndexDefinition indexDef =
        VectorIndexDefinitionBuilder.builder()
            .withAutoEmbedField("items.title")
            .withFilterPath("category")
            .build();

    BsonArray itemsArray = new BsonArray();
    itemsArray.add(new BsonDocument("title", new BsonString("a")));
    itemsArray.add(new BsonDocument("title", new BsonString("b")));
    BsonDocument fullDocument =
        new BsonDocument()
            .append("_id", new BsonInt32(0))
            .append("items", itemsArray)
            .append("category", new BsonString("books"))
            .append(indexDef.getIndexId().toString(), new BsonDocument("_id", new BsonInt32(0)));
    UpdateDescription updateDesc =
        new UpdateDescription(null, new BsonDocument("items.title", new BsonString("b")));
    ChangeStreamDocument<RawBsonDocument> changeEvent =
        ChangeStreamUtils.dataEvent(OperationType.UPDATE, 0, updateDesc, fullDocument);

    var batch =
        ChangeStreamDocumentUtils.handleDocumentEvents(
            List.of(changeEvent),
            indexDef,
            indexDef.createFieldDefinitionResolver(IndexFormatVersion.CURRENT),
            false,
            ChangeStreamDocumentUtils.deriveViewFieldDependencies(indexDef));

    DocumentEvent event = batch.finalChangeEvents.getFirst();
    assertFalse(event.getFilterFieldUpdates().isPresent());
    assertTrue(event.getChangedEmbedTexts().isEmpty());
  }

  @Test
  public void testFilterOnlyUpdate_AutoEmbedFieldIndexOnView_NotCreated() {
    // The changed filter field is referenced by the view's $match, so the update may move the
    // document into the view. The partial-update path issues a non-upsert write that would
    // silently drop such documents; a full update must be produced instead.
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
            false,
            ChangeStreamDocumentUtils.deriveViewFieldDependencies(indexDef));

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
            false,
            ChangeStreamDocumentUtils.deriveViewFieldDependencies(indexDef));

    assertEquals(1, batch.finalChangeEvents.size());
    assertFalse(batch.finalChangeEvents.getFirst().getFilterFieldUpdates().isPresent());
  }

  @Test
  public void testReplaceEvent_AutoEmbedFieldIndex_CountedAsFallback() {
    // A REPLACE re-embeds the whole document — the same full re-index the partial-embed
    // optimization avoids on UPDATE. It's counted under REPLACE_EVENT so the metric captures every
    // full re-index, and the PartialEmbedMetricCounters holder surfaces it as a process-wide
    // Prometheus counter tagged by reason (CLOUDP-412617).
    VectorIndexDefinition indexDef =
        VectorIndexDefinitionBuilder.builder()
            .withAutoEmbedField("description")
            .withFilterPath("category")
            .build();
    BsonDocument fullDocument =
        new BsonDocument()
            .append("_id", new BsonInt32(0))
            .append("description", new BsonString("deep learning"))
            .append("category", new BsonString("books"))
            .append(indexDef.getIndexId().toString(), new BsonDocument("_id", new BsonInt32(0)));
    ChangeStreamDocument<RawBsonDocument> changeEvent =
        ChangeStreamUtils.dataEvent(OperationType.REPLACE, 0, null, fullDocument);

    var batch =
        ChangeStreamDocumentUtils.handleDocumentEvents(
            List.of(changeEvent),
            indexDef,
            indexDef.createFieldDefinitionResolver(IndexFormatVersion.CURRENT),
            false,
            ChangeStreamDocumentUtils.deriveViewFieldDependencies(indexDef));

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

  @Test
  public void testFilterOnlyUpdate_AutoEmbedFieldIndexOnView_UnrelatedField_Created() {
    // The changed filter field is neither referenced by the view's $match nor feeds the embed
    // source, so the view-resolved document's membership and embed source are unaffected and
    // the filter-only partial update is retained.
    VectorIndexDefinition indexDef =
        VectorIndexDefinitionBuilder.builder()
            .withAutoEmbedField("description")
            .withFilterPath("category")
            .withFilterPath("isPublished")
            .view(
                ViewDefinition.existing(
                    "myView",
                    List.of(
                        BsonDocument.parse(
                            "{\"$match\": {\"$expr\": {\"$eq\": [\"$isPublished\", true]}}}"))))
            .build();
    assertTrue(MaterializedViewIndexDefinitionGeneration.isMaterializedViewBasedIndex(indexDef));

    ChangeStreamDocument<RawBsonDocument> changeEvent =
        createFilterOnlyUpdateEvent(indexDef, "category");
    var batch =
        ChangeStreamDocumentUtils.handleDocumentEvents(
            List.of(changeEvent),
            indexDef,
            indexDef.createFieldDefinitionResolver(IndexFormatVersion.CURRENT),
            false,
            ChangeStreamDocumentUtils.deriveViewFieldDependencies(indexDef));

    assertEquals(1, batch.finalChangeEvents.size());
    DocumentEvent event = batch.finalChangeEvents.getFirst();
    assertTrue(event.getFilterFieldUpdates().isPresent());
    assertEquals(new BsonString("updated"), event.getFilterFieldUpdates().get().get("category"));
  }

  @Test
  public void textEmbedFeederUpdate_AutoEmbedFieldIndexOnView_NotCreated() {
    // A v2 mat-view index may contain a TEXT field alongside an AUTO_EMBED field; both are embed
    // sources (AutoEmbedFieldMappingCreator maps TEXT -> EmbedField). When the TEXT field's value
    // is computed by the view, an update to its base feeder must re-embed; otherwise the TEXT
    // embedding goes stale (under-collection).
    VectorIndexDefinition indexDef =
        VectorIndexDefinitionBuilder.builder()
            .withAutoEmbedField("title")
            .withTextField("combined")
            .withFilterPath("category")
            .view(
                ViewDefinition.existing(
                    "myView",
                    List.of(
                        BsonDocument.parse(
                            "{\"$set\": {\"combined\": {\"$concat\": [\"$bodyA\","
                                + " \"$bodyB\"]}}}"))))
            .build();
    assertTrue(MaterializedViewIndexDefinitionGeneration.isMaterializedViewBasedIndex(indexDef));

    UpdateDescription updateDesc =
        new UpdateDescription(null, new BsonDocument("bodyA", new BsonString("new body")));
    BsonDocument fullDocument =
        new BsonDocument()
            .append("_id", new BsonInt32(0))
            .append("title", new BsonString("t"))
            .append("combined", new BsonString("new body and b"))
            .append("category", new BsonString("updated"))
            .append(indexDef.getIndexId().toString(), new BsonDocument("_id", new BsonInt32(0)));
    ChangeStreamDocument<RawBsonDocument> changeEvent =
        ChangeStreamUtils.dataEvent(OperationType.UPDATE, 0, updateDesc, fullDocument);

    var batch =
        ChangeStreamDocumentUtils.handleDocumentEvents(
            List.of(changeEvent),
            indexDef,
            indexDef.createFieldDefinitionResolver(IndexFormatVersion.CURRENT),
            false,
            ChangeStreamDocumentUtils.deriveViewFieldDependencies(indexDef));

    assertEquals(1, batch.finalChangeEvents.size());
    assertFalse(batch.finalChangeEvents.getFirst().getFilterFieldUpdates().isPresent());
  }

  @Test
  public void computedFilterFieldCanVanish_AutoEmbedFieldIndexOnView_NotCreated() {
    // The view computes a declared FILTER field that can evaluate to $$REMOVE (disappear). A base
    // update that makes it vanish must take the full path; the partial $set path cannot delete a
    // now-absent filter field, leaving a stale value in the materialized view (under-collection).
    VectorIndexDefinition indexDef =
        VectorIndexDefinitionBuilder.builder()
            .withAutoEmbedField("description")
            .withFilterPath("flag")
            .withFilterPath("category")
            .view(
                ViewDefinition.existing(
                    "myView",
                    List.of(
                        BsonDocument.parse(
                            "{\"$set\": {\"flag\": {\"$cond\": [{\"$gt\": [\"$score\", 5]},"
                                + " \"$$REMOVE\", \"low\"]}}}"))))
            .build();
    assertTrue(MaterializedViewIndexDefinitionGeneration.isMaterializedViewBasedIndex(indexDef));

    // score 3 -> flag="low" was in the MV; now score 9 -> flag vanishes. `category` (an unrelated,
    // always-present filter field) keeps extractFilterFieldValues non-empty, so the partial $set
    // path is taken and cannot delete the now-absent `flag`.
    UpdateDescription updateDesc =
        new UpdateDescription(null, new BsonDocument("score", new BsonInt32(9)));
    BsonDocument fullDocument =
        new BsonDocument()
            .append("_id", new BsonInt32(0))
            .append("description", new BsonString("d"))
            .append("score", new BsonInt32(9))
            .append("category", new BsonString("c"))
            .append(indexDef.getIndexId().toString(), new BsonDocument("_id", new BsonInt32(0)));
    ChangeStreamDocument<RawBsonDocument> changeEvent =
        ChangeStreamUtils.dataEvent(OperationType.UPDATE, 0, updateDesc, fullDocument);

    var batch =
        ChangeStreamDocumentUtils.handleDocumentEvents(
            List.of(changeEvent),
            indexDef,
            indexDef.createFieldDefinitionResolver(IndexFormatVersion.CURRENT),
            false,
            ChangeStreamDocumentUtils.deriveViewFieldDependencies(indexDef));

    assertEquals(1, batch.finalChangeEvents.size());
    assertFalse(batch.finalChangeEvents.getFirst().getFilterFieldUpdates().isPresent());
  }

  @Test
  public void unsupportedViewStage_AutoEmbedFieldIndexOnView_NotCreated() {
    // The view pipeline contains a stage the analyzer cannot interpret ($project), so
    // ViewFieldDependencies bails to dependsOnEverything=true. Every update — even one touching
    // a field that has no relation to the embed source or any $match — must take the full path.
    VectorIndexDefinition indexDef =
        VectorIndexDefinitionBuilder.builder()
            .withAutoEmbedField("description")
            .withFilterPath("category")
            .view(
                ViewDefinition.existing(
                    "myView",
                    List.of(
                        BsonDocument.parse(
                            "{\"$project\": {\"description\": 1, \"category\": 1}}"))))
            .build();
    assertTrue(MaterializedViewIndexDefinitionGeneration.isMaterializedViewBasedIndex(indexDef));

    ChangeStreamDocument<RawBsonDocument> changeEvent =
        createFilterOnlyUpdateEvent(indexDef, "category");
    var batch =
        ChangeStreamDocumentUtils.handleDocumentEvents(
            List.of(changeEvent),
            indexDef,
            indexDef.createFieldDefinitionResolver(IndexFormatVersion.CURRENT),
            false,
            ChangeStreamDocumentUtils.deriveViewFieldDependencies(indexDef));

    assertEquals(1, batch.finalChangeEvents.size());
    assertFalse(batch.finalChangeEvents.getFirst().getFilterFieldUpdates().isPresent());
  }

  @Test
  public void intraBatchCollapse_EmbedSourceThenFilterOnly_RequiresFullUpdate_NonView() {
    // Two events on the same doc in one batch: an embed-source update followed by a filter-only
    // update. Without the batch-aware decision, dedup would keep only the filter-only event and
    // emit a partial $set, leaving the MV vector encoding the old text (stale embedding).
    VectorIndexDefinition indexDef =
        VectorIndexDefinitionBuilder.builder()
            .withAutoEmbedField("description")
            .withFilterPath("category")
            .build();
    assertTrue(MaterializedViewIndexDefinitionGeneration.isMaterializedViewBasedIndex(indexDef));

    // Event 1 (older): update the embed source.
    UpdateDescription embedUpdateDesc =
        new UpdateDescription(
            null, new BsonDocument("description", new BsonString("new description")));
    BsonDocument embedFullDocument =
        new BsonDocument()
            .append("_id", new BsonInt32(0))
            .append("description", new BsonString("new description"))
            .append("category", new BsonString("old"))
            .append(indexDef.getIndexId().toString(), new BsonDocument("_id", new BsonInt32(0)));
    ChangeStreamDocument<RawBsonDocument> embedSourceEvent =
        ChangeStreamUtils.dataEvent(OperationType.UPDATE, 0, embedUpdateDesc, embedFullDocument);

    // Event 2 (newer, surviving): filter-only update on the same doc.
    ChangeStreamDocument<RawBsonDocument> filterOnlyEvent =
        createFilterOnlyUpdateEvent(indexDef, "category");

    var batch =
        ChangeStreamDocumentUtils.handleDocumentEvents(
            List.of(embedSourceEvent, filterOnlyEvent),
            indexDef,
            indexDef.createFieldDefinitionResolver(IndexFormatVersion.CURRENT),
            false,
            ChangeStreamDocumentUtils.deriveViewFieldDependencies(indexDef));

    assertEquals(1, batch.finalChangeEvents.size());
    assertFalse(batch.finalChangeEvents.getFirst().getFilterFieldUpdates().isPresent());
  }

  @Test
  public void intraBatchCollapse_MatchTransitionThenFilterOnly_RequiresFullUpdate_View() {
    // Two events on the same doc: a $match-transition update (isPublished false -> true,
    // entering the view) followed by a filter-only update. Without the batch-aware decision,
    // dedup would keep the filter-only event and emit a non-upsert partial $set, silently
    // dropping the transitioned-in document (HELP-94834 class).
    VectorIndexDefinition indexDef =
        VectorIndexDefinitionBuilder.builder()
            .withAutoEmbedField("description")
            .withFilterPath("category")
            .view(
                ViewDefinition.existing(
                    "myView",
                    List.of(
                        BsonDocument.parse(
                            "{\"$match\": {\"$expr\": {\"$eq\": [\"$isPublished\", true]}}}"))))
            .build();
    assertTrue(MaterializedViewIndexDefinitionGeneration.isMaterializedViewBasedIndex(indexDef));

    // Event 1 (older): matchField transition into the view.
    UpdateDescription matchTransitionDesc =
        new UpdateDescription(null, new BsonDocument("isPublished", BsonBoolean.TRUE));
    BsonDocument matchFullDocument =
        new BsonDocument()
            .append("_id", new BsonInt32(0))
            .append("description", new BsonString("d"))
            .append("isPublished", BsonBoolean.TRUE)
            .append("category", new BsonString("old"))
            .append(indexDef.getIndexId().toString(), new BsonDocument("_id", new BsonInt32(0)));
    ChangeStreamDocument<RawBsonDocument> matchTransitionEvent =
        ChangeStreamUtils.dataEvent(
            OperationType.UPDATE, 0, matchTransitionDesc, matchFullDocument);

    // Event 2 (newer, surviving): filter-only update on the same doc.
    ChangeStreamDocument<RawBsonDocument> filterOnlyEvent =
        createFilterOnlyUpdateEvent(indexDef, "category");

    var batch =
        ChangeStreamDocumentUtils.handleDocumentEvents(
            List.of(matchTransitionEvent, filterOnlyEvent),
            indexDef,
            indexDef.createFieldDefinitionResolver(IndexFormatVersion.CURRENT),
            false,
            ChangeStreamDocumentUtils.deriveViewFieldDependencies(indexDef));

    assertEquals(1, batch.finalChangeEvents.size());
    assertFalse(batch.finalChangeEvents.getFirst().getFilterFieldUpdates().isPresent());
  }

  @Test
  public void intraBatchCollapse_AllFilterOnly_OptimizationRetained_AggregatesIntoSurvivor() {
    // Two filter-only updates on the same doc, touching different filter fields. Neither event
    // requires full processing, so the surviving event must take the partial path. Additionally,
    // the predecessor's filter change must be aggregated into the survivor's $set: the survivor's
    // fullDocument reflects the post-batch state of every field (via updateLookup), and
    // extractFilterFieldValues reads all declared filter values from that fullDocument — not just
    // the ones in the survivor's updateDescription.
    VectorIndexDefinition indexDef =
        VectorIndexDefinitionBuilder.builder()
            .withAutoEmbedField("description")
            .withFilterPath("category")
            .withFilterPath("isPublished")
            .build();
    assertTrue(MaterializedViewIndexDefinitionGeneration.isMaterializedViewBasedIndex(indexDef));

    // Event 1 (older): filter-only update on isPublished.
    UpdateDescription firstFilterDesc =
        new UpdateDescription(null, new BsonDocument("isPublished", BsonBoolean.TRUE));
    BsonDocument firstFullDocument =
        new BsonDocument()
            .append("_id", new BsonInt32(0))
            .append("description", new BsonString("d"))
            .append("isPublished", BsonBoolean.TRUE)
            .append("category", new BsonString("old"))
            .append(indexDef.getIndexId().toString(), new BsonDocument("_id", new BsonInt32(0)));
    ChangeStreamDocument<RawBsonDocument> firstFilterEvent =
        ChangeStreamUtils.dataEvent(OperationType.UPDATE, 0, firstFilterDesc, firstFullDocument);

    // Event 2 (newer, surviving): filter-only update on category. Its fullDocument mirrors what
    // updateLookup would produce — the post-batch state of every field, including isPublished
    // from event 1.
    UpdateDescription secondFilterDesc =
        new UpdateDescription(null, new BsonDocument("category", new BsonString("updated")));
    BsonDocument secondFullDocument =
        new BsonDocument()
            .append("_id", new BsonInt32(0))
            .append("description", new BsonString("d"))
            .append("isPublished", BsonBoolean.TRUE)
            .append("category", new BsonString("updated"))
            .append(indexDef.getIndexId().toString(), new BsonDocument("_id", new BsonInt32(0)));
    ChangeStreamDocument<RawBsonDocument> secondFilterEvent =
        ChangeStreamUtils.dataEvent(OperationType.UPDATE, 0, secondFilterDesc, secondFullDocument);

    var batch =
        ChangeStreamDocumentUtils.handleDocumentEvents(
            List.of(firstFilterEvent, secondFilterEvent),
            indexDef,
            indexDef.createFieldDefinitionResolver(IndexFormatVersion.CURRENT),
            false,
            ChangeStreamDocumentUtils.deriveViewFieldDependencies(indexDef));

    assertEquals(1, batch.finalChangeEvents.size());
    DocumentEvent event = batch.finalChangeEvents.getFirst();
    assertTrue(event.getFilterFieldUpdates().isPresent());
    BsonDocument filterFieldUpdates = event.getFilterFieldUpdates().get();
    // The survivor's $set carries BOTH the survivor's own filter change AND the predecessor's.
    assertEquals(new BsonString("updated"), filterFieldUpdates.get("category"));
    assertEquals(BsonBoolean.TRUE, filterFieldUpdates.get("isPublished"));
  }

  @Test
  public void intraBatchCollapse_InsertThenFilterOnly_RequiresFullUpdate() {
    // INSERT followed by a filter-only update on the same doc. Without the batch-aware decision,
    // dedup would keep the filter-only event and emit a non-upsert partial $set against a row
    // that does not yet exist in the MV, silently dropping the inserted document.
    VectorIndexDefinition indexDef =
        VectorIndexDefinitionBuilder.builder()
            .withAutoEmbedField("description")
            .withFilterPath("category")
            .build();
    assertTrue(MaterializedViewIndexDefinitionGeneration.isMaterializedViewBasedIndex(indexDef));

    // Event 1 (older): INSERT.
    ChangeStreamDocument<RawBsonDocument> insertEvent =
        ChangeStreamUtils.dataEvent(OperationType.INSERT, 0, null, indexDef);

    // Event 2 (newer, surviving): filter-only update on the same doc.
    ChangeStreamDocument<RawBsonDocument> filterOnlyEvent =
        createFilterOnlyUpdateEvent(indexDef, "category");

    var batch =
        ChangeStreamDocumentUtils.handleDocumentEvents(
            List.of(insertEvent, filterOnlyEvent),
            indexDef,
            indexDef.createFieldDefinitionResolver(IndexFormatVersion.CURRENT),
            false,
            ChangeStreamDocumentUtils.deriveViewFieldDependencies(indexDef));

    assertEquals(1, batch.finalChangeEvents.size());
    assertFalse(batch.finalChangeEvents.getFirst().getFilterFieldUpdates().isPresent());
  }

  @Test
  public void intraBatchCollapse_ReplaceThenFilterOnly_RequiresFullUpdate() {
    // REPLACE followed by a filter-only update on the same doc. Without the batch-aware decision,
    // dedup would keep the filter-only event and emit a partial $set that would not reflect the
    // full-document replacement, silently corrupting the MV row.
    VectorIndexDefinition indexDef =
        VectorIndexDefinitionBuilder.builder()
            .withAutoEmbedField("description")
            .withFilterPath("category")
            .build();
    assertTrue(MaterializedViewIndexDefinitionGeneration.isMaterializedViewBasedIndex(indexDef));

    // Event 1 (older): REPLACE.
    ChangeStreamDocument<RawBsonDocument> replaceEvent =
        ChangeStreamUtils.dataEvent(OperationType.REPLACE, 0, null, indexDef);

    // Event 2 (newer, surviving): filter-only update on the same doc.
    ChangeStreamDocument<RawBsonDocument> filterOnlyEvent =
        createFilterOnlyUpdateEvent(indexDef, "category");

    var batch =
        ChangeStreamDocumentUtils.handleDocumentEvents(
            List.of(replaceEvent, filterOnlyEvent),
            indexDef,
            indexDef.createFieldDefinitionResolver(IndexFormatVersion.CURRENT),
            false,
            ChangeStreamDocumentUtils.deriveViewFieldDependencies(indexDef));

    assertEquals(1, batch.finalChangeEvents.size());
    assertFalse(batch.finalChangeEvents.getFirst().getFilterFieldUpdates().isPresent());
  }

  @Test
  public void intraBatchCollapse_NonApplicableSurvivor_PredecessorRequiredFull_FullEmitted() {
    // The surviving event's updateDescription touches no index-relevant field, so on its own it
    // would return Optional.empty() (no DocumentEvent). But an earlier same-doc event required
    // full processing; without an emission here, dedup silently drops the predecessor's embed
    // change. (CLOUDP-413687, same class as HELP-94834.) Verify a full replacement is emitted.
    VectorIndexDefinition indexDef =
        VectorIndexDefinitionBuilder.builder()
            .withAutoEmbedField("description")
            .withFilterPath("category")
            .build();
    assertTrue(MaterializedViewIndexDefinitionGeneration.isMaterializedViewBasedIndex(indexDef));

    // Event 1 (older): embed-source UPDATE (applies to index, requires full processing).
    UpdateDescription embedUpdateDesc =
        new UpdateDescription(
            null, new BsonDocument("description", new BsonString("new description")));
    BsonDocument embedFullDocument =
        new BsonDocument()
            .append("_id", new BsonInt32(0))
            .append("description", new BsonString("new description"))
            .append("category", new BsonString("c"))
            .append("foo", new BsonString("old"))
            .append(indexDef.getIndexId().toString(), new BsonDocument("_id", new BsonInt32(0)));
    ChangeStreamDocument<RawBsonDocument> embedSourceEvent =
        ChangeStreamUtils.dataEvent(OperationType.UPDATE, 0, embedUpdateDesc, embedFullDocument);

    // Event 2 (newer, surviving): UPDATE touching only "foo", which is not in the index.
    // updateDescriptionAppliesToIndex returns false for this event in isolation.
    UpdateDescription nonApplicableDesc =
        new UpdateDescription(null, new BsonDocument("foo", new BsonString("new")));
    BsonDocument survivorFullDocument =
        new BsonDocument()
            .append("_id", new BsonInt32(0))
            .append("description", new BsonString("new description"))
            .append("category", new BsonString("c"))
            .append("foo", new BsonString("new"))
            .append(indexDef.getIndexId().toString(), new BsonDocument("_id", new BsonInt32(0)));
    ChangeStreamDocument<RawBsonDocument> nonApplicableSurvivor =
        ChangeStreamUtils.dataEvent(
            OperationType.UPDATE, 0, nonApplicableDesc, survivorFullDocument);

    var batch =
        ChangeStreamDocumentUtils.handleDocumentEvents(
            List.of(embedSourceEvent, nonApplicableSurvivor),
            indexDef,
            indexDef.createFieldDefinitionResolver(IndexFormatVersion.CURRENT),
            false,
            ChangeStreamDocumentUtils.deriveViewFieldDependencies(indexDef));

    assertEquals(1, batch.finalChangeEvents.size());
    // A full replacement is emitted (not a filter-only partial update).
    assertFalse(batch.finalChangeEvents.getFirst().getFilterFieldUpdates().isPresent());
  }

  @Test
  public void intraBatchCollapse_DistantPredecessor_ThreeEvents_RequiresFullUpdate() {
    // 3 same-doc events: embed-source UPDATE, then two filter-only UPDATEs. The predecessor
    // requiring full is not adjacent to the survivor — verifies the pre-pass scans the entire
    // batch, not just the event immediately before the survivor.
    VectorIndexDefinition indexDef =
        VectorIndexDefinitionBuilder.builder()
            .withAutoEmbedField("description")
            .withFilterPath("category")
            .build();
    assertTrue(MaterializedViewIndexDefinitionGeneration.isMaterializedViewBasedIndex(indexDef));

    UpdateDescription embedUpdateDesc =
        new UpdateDescription(
            null, new BsonDocument("description", new BsonString("new description")));
    BsonDocument embedFullDocument =
        new BsonDocument()
            .append("_id", new BsonInt32(0))
            .append("description", new BsonString("new description"))
            .append("category", new BsonString("c"))
            .append(indexDef.getIndexId().toString(), new BsonDocument("_id", new BsonInt32(0)));
    ChangeStreamDocument<RawBsonDocument> embedSourceEvent =
        ChangeStreamUtils.dataEvent(OperationType.UPDATE, 0, embedUpdateDesc, embedFullDocument);
    ChangeStreamDocument<RawBsonDocument> middleFilterEvent =
        createFilterOnlyUpdateEvent(indexDef, "category");
    ChangeStreamDocument<RawBsonDocument> survivingFilterEvent =
        createFilterOnlyUpdateEvent(indexDef, "category");

    var batch =
        ChangeStreamDocumentUtils.handleDocumentEvents(
            List.of(embedSourceEvent, middleFilterEvent, survivingFilterEvent),
            indexDef,
            indexDef.createFieldDefinitionResolver(IndexFormatVersion.CURRENT),
            false,
            ChangeStreamDocumentUtils.deriveViewFieldDependencies(indexDef));

    assertEquals(1, batch.finalChangeEvents.size());
    assertFalse(batch.finalChangeEvents.getFirst().getFilterFieldUpdates().isPresent());
  }

  @Test
  public void intraBatchCollapse_PerDocumentIsolation() {
    // Two distinct docs in one batch: doc 0 has [embed-source UPDATE, filter-only UPDATE] and
    // must take full; doc 1 has [filter-only UPDATE] only and must keep the partial-update
    // optimization. Verifies the per-doc decision is keyed per documentKey and doesn't bleed.
    VectorIndexDefinition indexDef =
        VectorIndexDefinitionBuilder.builder()
            .withAutoEmbedField("description")
            .withFilterPath("category")
            .build();
    assertTrue(MaterializedViewIndexDefinitionGeneration.isMaterializedViewBasedIndex(indexDef));

    // Doc 0 predecessor: embed-source UPDATE.
    UpdateDescription doc0EmbedDesc =
        new UpdateDescription(
            null, new BsonDocument("description", new BsonString("new description")));
    BsonDocument doc0EmbedFullDoc =
        new BsonDocument()
            .append("_id", new BsonInt32(0))
            .append("description", new BsonString("new description"))
            .append("category", new BsonString("c"))
            .append(indexDef.getIndexId().toString(), new BsonDocument("_id", new BsonInt32(0)));
    ChangeStreamDocument<RawBsonDocument> doc0EmbedEvent =
        ChangeStreamUtils.dataEvent(OperationType.UPDATE, 0, doc0EmbedDesc, doc0EmbedFullDoc);

    // Doc 0 survivor: filter-only UPDATE.
    ChangeStreamDocument<RawBsonDocument> doc0FilterEvent =
        createFilterOnlyUpdateEvent(indexDef, "category");

    // Doc 1: filter-only UPDATE only (no predecessor).
    UpdateDescription doc1FilterDesc =
        new UpdateDescription(null, new BsonDocument("category", new BsonString("updated")));
    BsonDocument doc1FullDoc =
        new BsonDocument()
            .append("_id", new BsonInt32(1))
            .append("description", new BsonString("d"))
            .append("category", new BsonString("updated"))
            .append(indexDef.getIndexId().toString(), new BsonDocument("_id", new BsonInt32(1)));
    ChangeStreamDocument<RawBsonDocument> doc1FilterEvent =
        ChangeStreamUtils.dataEvent(OperationType.UPDATE, 1, doc1FilterDesc, doc1FullDoc);

    var batch =
        ChangeStreamDocumentUtils.handleDocumentEvents(
            List.of(doc0EmbedEvent, doc0FilterEvent, doc1FilterEvent),
            indexDef,
            indexDef.createFieldDefinitionResolver(IndexFormatVersion.CURRENT),
            false,
            ChangeStreamDocumentUtils.deriveViewFieldDependencies(indexDef));

    assertEquals(2, batch.finalChangeEvents.size());
    // finalChangeEvents are appended in Lists.reverse(batch) order, so doc 1 (last in batch) is
    // emitted first.
    DocumentEvent doc1Event = batch.finalChangeEvents.get(0);
    DocumentEvent doc0Event = batch.finalChangeEvents.get(1);
    // Doc 1 retains the filter-only partial update.
    assertTrue(doc1Event.getFilterFieldUpdates().isPresent());
    assertEquals(
        new BsonString("updated"), doc1Event.getFilterFieldUpdates().get().get("category"));
    // Doc 0 takes the full path because of its embed-source predecessor.
    assertFalse(doc0Event.getFilterFieldUpdates().isPresent());
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
