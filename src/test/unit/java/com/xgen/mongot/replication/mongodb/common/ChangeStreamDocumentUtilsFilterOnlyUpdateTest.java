package com.xgen.mongot.replication.mongodb.common;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.ImmutableSet;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.OperationType;
import com.mongodb.client.model.changestream.UpdateDescription;
import com.xgen.mongot.embedding.utils.PartialEmbedFallbackReason;
import com.xgen.mongot.index.DocumentEvent;
import com.xgen.mongot.index.definition.BooleanFieldDefinition;
import com.xgen.mongot.index.definition.MaterializedViewIndexDefinitionGeneration;
import com.xgen.mongot.index.definition.SearchAutoEmbedFieldDefinition;
import com.xgen.mongot.index.definition.SearchIndexDefinition;
import com.xgen.mongot.index.definition.VectorIndexDefinition;
import com.xgen.mongot.index.definition.ViewDefinition;
import com.xgen.mongot.index.version.IndexFormatVersion;
import com.xgen.mongot.metrics.MetricsFactory;
import com.xgen.mongot.replication.mongodb.common.ViewFieldDependencies.ViewDerivationFallbackReason;
import com.xgen.mongot.replication.mongodb.common.ViewFieldDependencies.ViewFullProcessingReason;
import com.xgen.mongot.util.FieldPath;
import com.xgen.testing.mongot.index.definition.DocumentFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.FieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.SearchIndexDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.VectorIndexDefinitionBuilder;
import com.xgen.testing.mongot.replication.mongodb.ChangeStreamUtils;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import org.bson.BsonArray;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonNull;
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
    assertEquals(1, batch.filterOnlyPartialApplied);
  }

  @Test
  public void testFilterOnlyUpdate_AutoEmbedFieldIndex_FeatureFlagEvaluatedForEachBatch() {
    VectorIndexDefinition indexDef =
        VectorIndexDefinitionBuilder.builder()
            .withAutoEmbedField("description")
            .withFilterPath("category")
            .build();
    ChangeStreamDocument<RawBsonDocument> changeEvent =
        createFilterOnlyUpdateEvent(indexDef, "category");
    AtomicBoolean forceFullUpdate = new AtomicBoolean(false);
    AutoEmbeddingMaterializedViewConfig materializedViewConfig =
        AutoEmbeddingMaterializedViewConfig.getDefault();
    materializedViewConfig.setControlKnobEvaluator(controlKnob -> forceFullUpdate.get());

    var partialUpdateBatch =
        ChangeStreamDocumentUtils.handleDocumentEvents(
            List.of(changeEvent),
            indexDef,
            indexDef.createFieldDefinitionResolver(IndexFormatVersion.CURRENT),
            false,
            ChangeStreamDocumentUtils.deriveViewFieldDependencies(indexDef),
            Optional.of(materializedViewConfig));
    assertTrue(partialUpdateBatch.finalChangeEvents.getFirst().getFilterFieldUpdates().isPresent());

    forceFullUpdate.set(true);
    var fullUpdateBatch =
        ChangeStreamDocumentUtils.handleDocumentEvents(
            List.of(changeEvent),
            indexDef,
            indexDef.createFieldDefinitionResolver(IndexFormatVersion.CURRENT),
            false,
            ChangeStreamDocumentUtils.deriveViewFieldDependencies(indexDef),
            Optional.of(materializedViewConfig));

    DocumentEvent event = fullUpdateBatch.finalChangeEvents.getFirst();
    assertFalse(event.getFilterFieldUpdates().isPresent());
    assertTrue(event.getChangedEmbedTexts().isEmpty());
    assertEquals(changeEvent.getFullDocument(), event.getDocument().orElseThrow());
    assertEquals(
        Integer.valueOf(1),
        fullUpdateBatch.partialEmbedFallbacks.get(
            PartialEmbedFallbackReason.FEATURE_FLAG_FORCED_FULL_UPDATE));
  }

  @Test
  public void testFilterOnlyUpdate_VectorIndex_RemainsFullUpdateWhenFeatureFlagEnabled() {
    VectorIndexDefinition indexDef =
        VectorIndexDefinitionBuilder.builder()
            .withCosineVectorField("embedding", 3)
            .withFilterPath("category")
            .build();
    assertFalse(indexDef.isAutoEmbeddingIndex());
    assertFalse(MaterializedViewIndexDefinitionGeneration.isMaterializedViewBasedIndex(indexDef));

    ChangeStreamDocument<RawBsonDocument> changeEvent =
        createFilterOnlyUpdateEvent(indexDef, "category");
    AutoEmbeddingMaterializedViewConfig materializedViewConfig =
        AutoEmbeddingMaterializedViewConfig.getDefault();
    materializedViewConfig.setControlKnobEvaluator(controlKnob -> true);

    var batch =
        ChangeStreamDocumentUtils.handleDocumentEvents(
            List.of(changeEvent),
            indexDef,
            indexDef.createFieldDefinitionResolver(IndexFormatVersion.CURRENT),
            false,
            ChangeStreamDocumentUtils.deriveViewFieldDependencies(indexDef),
            Optional.of(materializedViewConfig));

    DocumentEvent event = batch.finalChangeEvents.getFirst();
    assertFalse(event.getFilterFieldUpdates().isPresent());
    assertTrue(event.getChangedEmbedTexts().isEmpty());
    assertEquals(changeEvent.getFullDocument(), event.getDocument().orElseThrow());
    assertTrue(batch.partialEmbedFallbacks.isEmpty());
  }

  @Test
  public void searchAutoEmbed_sourceFieldUpdate_featureFlagEnabled_forcesFullUpdate() {
    SearchIndexDefinition indexDef = searchAutoEmbedIndex();
    ChangeStreamDocument<RawBsonDocument> changeEvent = createSearchUpdateEvent(indexDef, "body");
    AutoEmbeddingMaterializedViewConfig materializedViewConfig =
        AutoEmbeddingMaterializedViewConfig.getDefault();
    materializedViewConfig.setControlKnobEvaluator(controlKnob -> true);

    var batch =
        ChangeStreamDocumentUtils.handleDocumentEvents(
            List.of(changeEvent),
            indexDef,
            indexDef.createFieldDefinitionResolver(IndexFormatVersion.CURRENT),
            false,
            ChangeStreamDocumentUtils.deriveViewFieldDependencies(indexDef),
            Optional.of(materializedViewConfig));

    assertEquals(1, batch.finalChangeEvents.size());
    DocumentEvent event = batch.finalChangeEvents.getFirst();
    assertFalse(event.getFilterFieldUpdates().isPresent());
    assertTrue(event.getChangedEmbedTexts().isEmpty());
    assertEquals(changeEvent.getFullDocument(), event.getDocument().orElseThrow());
    assertEquals(
        Integer.valueOf(1),
        batch.partialEmbedFallbacks.get(
            PartialEmbedFallbackReason.FEATURE_FLAG_FORCED_FULL_UPDATE));
  }

  @Test
  public void searchAutoEmbed_derivesMappingFromSourceField() {
    SearchIndexDefinition indexDef = searchAutoEmbedIndex();

    var mapping = ChangeStreamDocumentUtils.deriveAutoEmbedFieldMapping(indexDef).orElseThrow();

    assertEquals(ImmutableSet.of(FieldPath.parse("body")), mapping.embedFields().keySet());
    assertTrue(mapping.isPassthrough(FieldPath.parse("category")));
  }

  @Test
  public void searchAutoEmbed_unrelatedUpdate_featureFlagEnabled_isDiscardedBeforeFullUpdate() {
    SearchIndexDefinition indexDef = searchAutoEmbedIndex();
    ChangeStreamDocument<RawBsonDocument> changeEvent =
        createSearchUpdateEvent(indexDef, "unrelated");
    AutoEmbeddingMaterializedViewConfig materializedViewConfig =
        AutoEmbeddingMaterializedViewConfig.getDefault();
    materializedViewConfig.setControlKnobEvaluator(controlKnob -> true);

    var batch =
        ChangeStreamDocumentUtils.handleDocumentEvents(
            List.of(changeEvent),
            indexDef,
            indexDef.createFieldDefinitionResolver(IndexFormatVersion.CURRENT),
            false,
            ChangeStreamDocumentUtils.deriveViewFieldDependencies(indexDef),
            Optional.of(materializedViewConfig));

    assertTrue(batch.finalChangeEvents.isEmpty());
    assertTrue(batch.partialEmbedFallbacks.isEmpty());
  }

  @Test
  public void testFilterOnlyUpdate_NestedFilterUnderNestedRoot_CreatedWithSourcePathBody() {
    // CLOUDP-402522: filter fields under a nestedRoot are co-located with the embedded vector in
    // the materialized view (e.g. _autoEmbed.sections.section_name). At the classification stage
    // the
    // partial update is still created with the source-path filter copy; the co-located _autoEmbed.*
    // value is added later by EmbeddingIndexingWorkScheduler (see buildNestedFilterPartialSetBody),
    // where the MV schema metadata is available. This test only asserts the source-path body here.
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
    DocumentEvent event = batch.finalChangeEvents.getFirst();
    assertTrue(event.getFilterFieldUpdates().isPresent());
    // Source-path copy only at this stage; the scheduler adds _autoEmbed.sections.0.section_name.
    BsonArray sections = event.getFilterFieldUpdates().get().getArray("sections");
    assertEquals(new BsonString("updated"), sections.get(0).asDocument().getString("section_name"));
    assertEquals(0, batch.partialEmbedApplied);
    assertTrue(batch.partialEmbedFallbacks.isEmpty());
  }

  @Test
  public void testFilterOnlyUpdate_MultiElementArray_OneElementFilterChanged_PatchesThatElement() {
    // Reviewer question (PR6764): a change-stream filter-only update that touches one of N array
    // elements. Here `sections` has three elements and only element 1's filter (`section_name`) is
    // updated; the embed source (`section_content`) is unchanged, so the update must take the
    // filter-only partial path (not a re-embed). The $set body is built from the whole post-image
    // sections array index-aligned, so MV child i is rebuilt from source element i: element 1 gets
    // the new name while elements 0 and 2 keep their originals (no clobbering of siblings).
    VectorIndexDefinition indexDef =
        VectorIndexDefinitionBuilder.builder()
            .withAutoEmbedField("sections.section_content")
            .withFilterPath("sections.section_name")
            .nestedRoot("sections")
            .build();
    assertTrue(MaterializedViewIndexDefinitionGeneration.isMaterializedViewBasedIndex(indexDef));

    UpdateDescription updateDesc =
        new UpdateDescription(
            null, new BsonDocument("sections.1.section_name", new BsonString("name_b_updated")));
    BsonDocument fullDocument =
        new BsonDocument()
            .append("_id", new BsonInt32(0))
            .append(
                "sections",
                new BsonArray(
                    List.of(
                        new BsonDocument()
                            .append("section_content", new BsonString("machine learning"))
                            .append("section_name", new BsonString("name_a")),
                        new BsonDocument()
                            .append("section_content", new BsonString("vector search"))
                            .append("section_name", new BsonString("name_b_updated")),
                        new BsonDocument()
                            .append("section_content", new BsonString("graph databases"))
                            .append("section_name", new BsonString("name_c")))))
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
    // Filter-only change: a $set body is produced and no embedding work is required.
    assertTrue(event.getFilterFieldUpdates().isPresent());
    assertTrue(event.getChangedEmbedTexts().isEmpty());
    assertEquals(0, batch.partialEmbedApplied);
    assertTrue(batch.partialEmbedFallbacks.isEmpty());

    // Every element is present in the body, index-aligned with the source array: element 1 has the
    // new filter value; elements 0 and 2 keep their originals.
    BsonArray updatedSections = event.getFilterFieldUpdates().get().getArray("sections");
    assertEquals(3, updatedSections.size());
    assertEquals(
        new BsonString("name_a"), updatedSections.get(0).asDocument().getString("section_name"));
    assertEquals(
        new BsonString("name_b_updated"),
        updatedSections.get(1).asDocument().getString("section_name"));
    assertEquals(
        new BsonString("name_c"), updatedSections.get(2).asDocument().getString("section_name"));
  }

  @Test
  public void testFilterOnlyUpdate_DestructiveNestedFilter_FallsBackToFullReplace() {
    // CLOUDP-402522: the co-located _autoEmbed.* filter is patched with an additive per-element
    // positional $set that can't clear a removed value. An $unset of a nested filter would leave a
    // stale _autoEmbed.sections.0.section_name on the MV, so the update must fall back to a full
    // replace (no partial $set), recorded under NESTED_FILTER_REMAPPING.
    VectorIndexDefinition indexDef =
        VectorIndexDefinitionBuilder.builder()
            .withAutoEmbedField("sections.section_content")
            .withFilterPath("sections.section_name")
            .nestedRoot("sections")
            .build();
    assertTrue(MaterializedViewIndexDefinitionGeneration.isMaterializedViewBasedIndex(indexDef));

    UpdateDescription updateDesc =
        new UpdateDescription(List.of("sections.0.section_name"), new BsonDocument());
    BsonDocument fullDocument =
        new BsonDocument()
            .append("_id", new BsonInt32(0))
            .append(
                "sections",
                new BsonArray(
                    List.of(
                        new BsonDocument()
                            .append("section_content", new BsonString("machine learning")))))
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
    assertFalse(event.getFilterFieldUpdates().isPresent());
    assertTrue(event.getChangedEmbedTexts().isEmpty());
    assertEquals(0, batch.partialEmbedApplied);
    assertEquals(
        Integer.valueOf(1),
        batch.partialEmbedFallbacks.get(PartialEmbedFallbackReason.NESTED_FILTER_REMAPPING));
  }

  @Test
  public void testFilterOnlyUpdate_NestedFilterAncestorOverwrite_FallsBackToFullReplace() {
    // Filter lives one level below the array element (sections.meta.tag). A plain $set that
    // overwrites the intermediate "meta" object drops "tag" from the post-image — reported as an
    // updatedField, not a removedField. The additive positional $set would then write nothing for
    // that element, leaving _autoEmbed.sections.0.meta.tag stale. Falls back to a full replace,
    // recorded under NESTED_FILTER_REMAPPING.
    VectorIndexDefinition indexDef =
        VectorIndexDefinitionBuilder.builder()
            .withAutoEmbedField("sections.content")
            .withFilterPath("sections.meta.tag")
            .nestedRoot("sections")
            .build();
    assertTrue(MaterializedViewIndexDefinitionGeneration.isMaterializedViewBasedIndex(indexDef));

    UpdateDescription updateDesc =
        new UpdateDescription(
            null,
            new BsonDocument("sections.0.meta", new BsonDocument("color", new BsonString("blue"))));
    BsonDocument fullDocument =
        new BsonDocument()
            .append("_id", new BsonInt32(0))
            .append(
                "sections",
                new BsonArray(
                    List.of(
                        new BsonDocument()
                            .append("content", new BsonString("hello"))
                            .append("meta", new BsonDocument("color", new BsonString("blue"))))))
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
    assertFalse(event.getFilterFieldUpdates().isPresent());
    assertTrue(event.getChangedEmbedTexts().isEmpty());
    assertEquals(0, batch.partialEmbedApplied);
    assertEquals(
        Integer.valueOf(1),
        batch.partialEmbedFallbacks.get(PartialEmbedFallbackReason.NESTED_FILTER_REMAPPING));
  }

  @Test
  public void testFilterOnlyUpdate_TopLevelFilterAncestorOverwrite_FallsBackToFullReplace() {
    // Same ancestor-overwrite hazard for a top-level filter: $set of "meta" drops the co-located
    // "meta.tag" from the post-image, so the rebuilt filter $set body carries nothing to overwrite
    // the stale MV value. Must fall back to a full replace, recorded under ANCESTOR_OVERWRITE.
    VectorIndexDefinition indexDef =
        VectorIndexDefinitionBuilder.builder()
            .withAutoEmbedField("body")
            .withFilterPath("meta.tag")
            .build();
    assertTrue(MaterializedViewIndexDefinitionGeneration.isMaterializedViewBasedIndex(indexDef));

    UpdateDescription updateDesc =
        new UpdateDescription(
            null, new BsonDocument("meta", new BsonDocument("color", new BsonString("blue"))));
    BsonDocument fullDocument =
        new BsonDocument()
            .append("_id", new BsonInt32(0))
            .append("body", new BsonString("hello"))
            .append("meta", new BsonDocument("color", new BsonString("blue")))
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
    assertFalse(event.getFilterFieldUpdates().isPresent());
    assertTrue(event.getChangedEmbedTexts().isEmpty());
    assertEquals(0, batch.partialEmbedApplied);
    assertEquals(
        Integer.valueOf(1),
        batch.partialEmbedFallbacks.get(PartialEmbedFallbackReason.ANCESTOR_OVERWRITE));
  }

  @Test
  public void testFilterOnlyUpdate_UnrelatedDestructiveChange_StaysPartial() {
    // The destructive-change guard is scoped to the nestedRoot subtree: an $unset of a field that
    // is neither under the nestedRoot nor otherwise indexed must NOT force a full replace, even
    // when the index declares a filter under the nestedRoot. The concurrent nested-filter $set
    // should still be applied as a partial update.
    VectorIndexDefinition indexDef =
        VectorIndexDefinitionBuilder.builder()
            .withAutoEmbedField("sections.section_content")
            .withFilterPath("sections.section_name")
            .nestedRoot("sections")
            .build();
    assertTrue(MaterializedViewIndexDefinitionGeneration.isMaterializedViewBasedIndex(indexDef));

    UpdateDescription updateDesc =
        new UpdateDescription(
            List.of("unrelatedField"),
            new BsonDocument("sections.0.section_name", new BsonString("updated")));
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
    DocumentEvent event = batch.finalChangeEvents.getFirst();
    assertTrue(event.getFilterFieldUpdates().isPresent());
    BsonArray sections = event.getFilterFieldUpdates().get().getArray("sections");
    assertEquals(new BsonString("updated"), sections.get(0).asDocument().getString("section_name"));
    assertEquals(0, batch.partialEmbedApplied);
    assertTrue(batch.partialEmbedFallbacks.isEmpty());
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
    // A top-level filter stays partial, so no full-replace fallback is recorded.
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
  public void testComputedEmbedFeederUpdate_AutoEmbedFieldIndexOnView_PartialEmbeds() {
    // The embed source is computed by the view pipeline (e.g. $set from base fields), so
    // updateDescription carries the base feeder (title), not the embed name. The change re-embeds
    // the computed source via the partial-embed path (reading the view-resolved text), rather than
    // a full replace.
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
    DocumentEvent event = batch.finalChangeEvents.getFirst();
    assertTrue(event.getFilterFieldUpdates().isPresent());
    assertEquals(
        ImmutableSet.of(FieldPath.parse("description")), event.getChangedEmbedTexts().keySet());
    assertEquals(
        "new title computed", event.getChangedEmbedTexts().get(FieldPath.parse("description")));
    assertEquals(1, batch.partialEmbedApplied);
  }

  @Test
  public void
      testNestedComputedEmbedContainerOverwrite_AutoEmbedFieldIndexOnView_UsesFullReplace() {
    VectorIndexDefinition indexDef =
        VectorIndexDefinitionBuilder.builder()
            .withAutoEmbedField("items.text")
            .withFilterPath("items.category")
            .nestedRoot("items")
            .view(
                ViewDefinition.existing(
                    "myView",
                    List.of(BsonDocument.parse("{\"$set\": {\"items.text\": \"$seed\"}}"))))
            .build();
    assertTrue(MaterializedViewIndexDefinitionGeneration.isMaterializedViewBasedIndex(indexDef));

    UpdateDescription updateDesc =
        new UpdateDescription(null, new BsonDocument("items", new BsonArray()));
    BsonDocument fullDocument =
        new BsonDocument()
            .append("_id", new BsonInt32(0))
            .append("items", new BsonArray())
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
    assertEquals(
        Integer.valueOf(1),
        batch.viewDecisions.get(ViewFullProcessingReason.COMPUTED_EMBED_CONTAINER_OVERWRITE));
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
  public void testReplaceEvent_AutoEmbedFieldIndex_ForcedFullDoesNotCountAsFallback() {
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
    AutoEmbeddingMaterializedViewConfig config = AutoEmbeddingMaterializedViewConfig.getDefault();
    config.setControlKnobEvaluator(ignored -> true);

    var batch =
        ChangeStreamDocumentUtils.handleDocumentEvents(
            List.of(changeEvent),
            indexDef,
            indexDef.createFieldDefinitionResolver(IndexFormatVersion.CURRENT),
            false,
            ChangeStreamDocumentUtils.deriveViewFieldDependencies(indexDef),
            Optional.of(config));

    assertEquals(1, batch.finalChangeEvents.size());
    assertTrue(batch.partialEmbedFallbacks.isEmpty());
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
  public void testFilterOnlyUpdate_AutoEmbedFieldIndexOnView_FullUpdateWhenFeatureFlagEnabled() {
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
    assertTrue(indexDef.isAutoEmbeddingIndex());
    assertTrue(indexDef.getView().isPresent());
    assertTrue(MaterializedViewIndexDefinitionGeneration.isMaterializedViewBasedIndex(indexDef));

    ChangeStreamDocument<RawBsonDocument> changeEvent =
        createFilterOnlyUpdateEvent(indexDef, "category");
    AutoEmbeddingMaterializedViewConfig materializedViewConfig =
        AutoEmbeddingMaterializedViewConfig.getDefault();
    materializedViewConfig.setControlKnobEvaluator(controlKnob -> true);

    var batch =
        ChangeStreamDocumentUtils.handleDocumentEvents(
            List.of(changeEvent),
            indexDef,
            indexDef.createFieldDefinitionResolver(IndexFormatVersion.CURRENT),
            false,
            ChangeStreamDocumentUtils.deriveViewFieldDependencies(indexDef),
            Optional.of(materializedViewConfig));

    assertEquals(1, batch.finalChangeEvents.size());
    DocumentEvent event = batch.finalChangeEvents.getFirst();
    assertFalse(event.getFilterFieldUpdates().isPresent());
    assertTrue(event.getChangedEmbedTexts().isEmpty());
    assertEquals(changeEvent.getFullDocument(), event.getDocument().orElseThrow());
    assertEquals(
        Integer.valueOf(1),
        batch.partialEmbedFallbacks.get(
            PartialEmbedFallbackReason.FEATURE_FLAG_FORCED_FULL_UPDATE));
  }

  @Test
  public void textEmbedFeederUpdate_AutoEmbedFieldIndexOnView_PartialEmbedsChangedSourceOnly() {
    // A v2 mat-view index may contain a TEXT field alongside an AUTO_EMBED field; both are embed
    // sources (AutoEmbedFieldMappingCreator maps TEXT -> EmbedField). When only the computed TEXT
    // field's base feeder changes, the partial-embed path re-embeds just that source (read from
    // the view-resolved fullDocument); the untouched AUTO_EMBED field keeps its existing vector.
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
    DocumentEvent event = batch.finalChangeEvents.getFirst();
    assertTrue(event.getFilterFieldUpdates().isPresent());
    // Only `combined` was re-embedded; `title` was spared.
    assertEquals(
        ImmutableSet.of(FieldPath.parse("combined")), event.getChangedEmbedTexts().keySet());
    assertEquals("new body and b", event.getChangedEmbedTexts().get(FieldPath.parse("combined")));
    assertEquals(1, batch.partialEmbedApplied);
  }

  @Test
  public void viewTwoComputedEmbeds_singleFeederChange_reEmbedsOnlyThatSource() {
    // Two computed embed sources with disjoint feeders. Changing only embB's feeder must re-embed
    // embB alone — embA keeps its existing vector (the N×->1× cost win this ticket delivers).
    VectorIndexDefinition indexDef =
        VectorIndexDefinitionBuilder.builder()
            .withAutoEmbedField("embA")
            .withAutoEmbedField("embB")
            .withFilterPath("category")
            .view(
                ViewDefinition.existing(
                    "myView",
                    List.of(
                        BsonDocument.parse(
                            "{\"$set\": {\"embA\": {\"$concat\": [\"$a1\", \" \", \"$a2\"]},"
                                + " \"embB\": {\"$concat\": [\"$b1\", \" \", \"$b2\"]}}}"))))
            .build();
    assertTrue(MaterializedViewIndexDefinitionGeneration.isMaterializedViewBasedIndex(indexDef));

    UpdateDescription updateDesc =
        new UpdateDescription(null, new BsonDocument("b1", new BsonString("new b1")));
    BsonDocument fullDocument =
        new BsonDocument()
            .append("_id", new BsonInt32(0))
            .append("embA", new BsonString("a1 a2"))
            .append("embB", new BsonString("new b1 b2"))
            .append("category", new BsonString("books"))
            .append(indexDef.getIndexId().toString(), new BsonDocument("_id", new BsonInt32(0)));

    var batch =
        ChangeStreamDocumentUtils.handleDocumentEvents(
            List.of(ChangeStreamUtils.dataEvent(OperationType.UPDATE, 0, updateDesc, fullDocument)),
            indexDef,
            indexDef.createFieldDefinitionResolver(IndexFormatVersion.CURRENT),
            false,
            ChangeStreamDocumentUtils.deriveViewFieldDependencies(indexDef));

    assertEquals(1, batch.finalChangeEvents.size());
    DocumentEvent event = batch.finalChangeEvents.getFirst();
    assertTrue(event.getFilterFieldUpdates().isPresent());
    assertEquals(ImmutableSet.of(FieldPath.parse("embB")), event.getChangedEmbedTexts().keySet());
    assertEquals("new b1 b2", event.getChangedEmbedTexts().get(FieldPath.parse("embB")));
    assertEquals(1, batch.partialEmbedApplied);
    assertTrue(batch.partialEmbedFallbacks.isEmpty());
  }

  @Test
  public void viewComputedEmbed_emptyNewValue_fallsBackToFullReplace() {
    // A partial $set can't $unset a now-empty embed vector, so an empty resolved embed text on the
    // view path falls back to a full replace, tagged EMPTY_NEW_VALUE (same as the non-view path).
    VectorIndexDefinition indexDef =
        VectorIndexDefinitionBuilder.builder()
            .withAutoEmbedField("embA")
            .withFilterPath("category")
            .view(
                ViewDefinition.existing(
                    "myView",
                    List.of(
                        BsonDocument.parse(
                            "{\"$set\": {\"embA\": {\"$concat\": [\"$a1\", \"$a2\"]}}}"))))
            .build();

    UpdateDescription updateDesc =
        new UpdateDescription(null, new BsonDocument("a1", new BsonString("")));
    BsonDocument fullDocument =
        new BsonDocument()
            .append("_id", new BsonInt32(0))
            .append("embA", new BsonString(""))
            .append("category", new BsonString("books"))
            .append(indexDef.getIndexId().toString(), new BsonDocument("_id", new BsonInt32(0)));

    var batch =
        ChangeStreamDocumentUtils.handleDocumentEvents(
            List.of(ChangeStreamUtils.dataEvent(OperationType.UPDATE, 0, updateDesc, fullDocument)),
            indexDef,
            indexDef.createFieldDefinitionResolver(IndexFormatVersion.CURRENT),
            false,
            ChangeStreamDocumentUtils.deriveViewFieldDependencies(indexDef));

    assertEquals(1, batch.finalChangeEvents.size());
    assertFalse(batch.finalChangeEvents.getFirst().getFilterFieldUpdates().isPresent());
    assertEquals(0, batch.partialEmbedApplied);
    assertEquals(
        Integer.valueOf(1),
        batch.partialEmbedFallbacks.get(PartialEmbedFallbackReason.EMPTY_NEW_VALUE));
  }

  @Test
  public void viewEmbedChangeThatDropsPassthroughFilter_fallsBackToFull() {
    // An embed-feeder change alongside a $set that overwrites a passthrough filter's ancestor
    // ({meta: null} drops meta.category) must NOT take the partial path: a $set built from the
    // post-image can't clear the now-absent meta.category, so it forces a full replace.
    VectorIndexDefinition indexDef =
        VectorIndexDefinitionBuilder.builder()
            .withAutoEmbedField("embA")
            .withFilterPath("meta.category")
            .view(
                ViewDefinition.existing(
                    "myView",
                    List.of(
                        BsonDocument.parse(
                            "{\"$set\": {\"embA\": {\"$concat\": [\"$a1\", \" \", \"$a2\"]}}}"))))
            .build();
    assertTrue(MaterializedViewIndexDefinitionGeneration.isMaterializedViewBasedIndex(indexDef));

    UpdateDescription updateDesc =
        new UpdateDescription(
            null,
            new BsonDocument().append("a1", new BsonString("new")).append("meta", new BsonNull()));
    BsonDocument fullDocument =
        new BsonDocument()
            .append("_id", new BsonInt32(0))
            .append("embA", new BsonString("new a2"))
            .append("meta", new BsonNull())
            .append(indexDef.getIndexId().toString(), new BsonDocument("_id", new BsonInt32(0)));

    var batch =
        ChangeStreamDocumentUtils.handleDocumentEvents(
            List.of(ChangeStreamUtils.dataEvent(OperationType.UPDATE, 0, updateDesc, fullDocument)),
            indexDef,
            indexDef.createFieldDefinitionResolver(IndexFormatVersion.CURRENT),
            false,
            ChangeStreamDocumentUtils.deriveViewFieldDependencies(indexDef));

    assertEquals(1, batch.finalChangeEvents.size());
    assertFalse(batch.finalChangeEvents.getFirst().getFilterFieldUpdates().isPresent());
    assertEquals(0, batch.partialEmbedApplied);
    assertEquals(
        Integer.valueOf(1),
        batch.partialEmbedFallbacks.get(PartialEmbedFallbackReason.ANCESTOR_OVERWRITE));
  }

  @Test
  public void viewDedup_collapsedFilterDrop_forcesSurvivorFull() {
    // Batch dedup: an older event drops a filter (meta: null) and a newer event changes an embed
    // feeder. The older event collapses behind the survivor; the pre-pass must force the survivor
    // to a full replace so the dropped meta.category doesn't survive as a stale partial-update.
    VectorIndexDefinition indexDef =
        VectorIndexDefinitionBuilder.builder()
            .withAutoEmbedField("embA")
            .withFilterPath("meta.category")
            .view(
                ViewDefinition.existing(
                    "myView",
                    List.of(
                        BsonDocument.parse(
                            "{\"$set\": {\"embA\": {\"$concat\": [\"$a1\", \" \", \"$a2\"]}}}"))))
            .build();

    BsonDocument metadataNamespace = new BsonDocument("_id", new BsonInt32(0));
    ChangeStreamDocument<RawBsonDocument> olderDropsFilter =
        ChangeStreamUtils.dataEvent(
            OperationType.UPDATE,
            0,
            new UpdateDescription(null, new BsonDocument("meta", new BsonNull())),
            new BsonDocument()
                .append("_id", new BsonInt32(0))
                .append("embA", new BsonString("a1 a2"))
                .append("meta", new BsonNull())
                .append(indexDef.getIndexId().toString(), metadataNamespace));
    ChangeStreamDocument<RawBsonDocument> newerEmbedChange =
        ChangeStreamUtils.dataEvent(
            OperationType.UPDATE,
            0,
            new UpdateDescription(null, new BsonDocument("a1", new BsonString("new"))),
            new BsonDocument()
                .append("_id", new BsonInt32(0))
                .append("embA", new BsonString("new a2"))
                .append("meta", new BsonNull())
                .append(indexDef.getIndexId().toString(), metadataNamespace));

    var batch =
        ChangeStreamDocumentUtils.handleDocumentEvents(
            List.of(olderDropsFilter, newerEmbedChange),
            indexDef,
            indexDef.createFieldDefinitionResolver(IndexFormatVersion.CURRENT),
            false,
            ChangeStreamDocumentUtils.deriveViewFieldDependencies(indexDef));

    // One surviving event, forced to a full replace (not a partial $set).
    assertEquals(1, batch.finalChangeEvents.size());
    assertFalse(batch.finalChangeEvents.getFirst().getFilterFieldUpdates().isPresent());
    assertEquals(0, batch.partialEmbedApplied);
    assertEquals(
        Integer.valueOf(1),
        batch.partialEmbedFallbacks.get(
            PartialEmbedFallbackReason.INTRA_BATCH_PREDECESSOR_REQUIRES_FULL));
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
    assertEquals(
        Integer.valueOf(1),
        batch.partialEmbedFallbacks.get(
            PartialEmbedFallbackReason.INTRA_BATCH_PREDECESSOR_REQUIRES_FULL));
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
    assertEquals(
        Integer.valueOf(1),
        batch.partialEmbedFallbacks.get(
            PartialEmbedFallbackReason.INTRA_BATCH_PREDECESSOR_REQUIRES_FULL));
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
    assertTrue(
        batch.partialEmbedFallbacks.get(
                PartialEmbedFallbackReason.INTRA_BATCH_PREDECESSOR_REQUIRES_FULL)
            == null);
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
    assertEquals(
        Integer.valueOf(1),
        batch.partialEmbedFallbacks.get(
            PartialEmbedFallbackReason.INTRA_BATCH_PREDECESSOR_REQUIRES_FULL));
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
    assertEquals(
        Integer.valueOf(1),
        batch.partialEmbedFallbacks.get(
            PartialEmbedFallbackReason.INTRA_BATCH_PREDECESSOR_REQUIRES_FULL));
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
    assertEquals(
        Integer.valueOf(1),
        batch.partialEmbedFallbacks.get(
            PartialEmbedFallbackReason.INTRA_BATCH_PREDECESSOR_REQUIRES_FULL));
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
    assertEquals(
        Integer.valueOf(1),
        batch.partialEmbedFallbacks.get(
            PartialEmbedFallbackReason.INTRA_BATCH_PREDECESSOR_REQUIRES_FULL));
  }

  @Test
  public void nonViewDedup_collapsedFilterDrop_forcesSurvivorFull() {
    // Non-view mirror of viewDedup_collapsedFilterDrop_forcesSurvivorFull.
    // Older event $unsets a declared filter; newer survivor is a different filter-only $set.
    // The pre-pass must force the survivor to a full replace — otherwise the partial $set from
    // the survivor's fullDocument omits the removed filter and leaves a stale MV value.
    VectorIndexDefinition indexDef =
        VectorIndexDefinitionBuilder.builder()
            .withAutoEmbedField("description")
            .withFilterPath("category")
            .withFilterPath("status")
            .build();
    assertTrue(MaterializedViewIndexDefinitionGeneration.isMaterializedViewBasedIndex(indexDef));

    UpdateDescription unsetDesc = new UpdateDescription(List.of("category"), new BsonDocument());
    BsonDocument afterUnset =
        new BsonDocument()
            .append("_id", new BsonInt32(0))
            .append("description", new BsonString("d"))
            .append("status", new BsonString("draft"))
            .append(indexDef.getIndexId().toString(), new BsonDocument("_id", new BsonInt32(0)));
    ChangeStreamDocument<RawBsonDocument> olderUnsetsFilter =
        ChangeStreamUtils.dataEvent(OperationType.UPDATE, 0, unsetDesc, afterUnset);

    UpdateDescription statusDesc =
        new UpdateDescription(null, new BsonDocument("status", new BsonString("published")));
    BsonDocument afterStatus =
        new BsonDocument()
            .append("_id", new BsonInt32(0))
            .append("description", new BsonString("d"))
            .append("status", new BsonString("published"))
            .append(indexDef.getIndexId().toString(), new BsonDocument("_id", new BsonInt32(0)));
    ChangeStreamDocument<RawBsonDocument> newerFilterOnly =
        ChangeStreamUtils.dataEvent(OperationType.UPDATE, 0, statusDesc, afterStatus);

    var batch =
        ChangeStreamDocumentUtils.handleDocumentEvents(
            List.of(olderUnsetsFilter, newerFilterOnly),
            indexDef,
            indexDef.createFieldDefinitionResolver(IndexFormatVersion.CURRENT),
            false,
            ChangeStreamDocumentUtils.deriveViewFieldDependencies(indexDef));

    assertEquals(1, batch.finalChangeEvents.size());
    assertFalse(
        "collapsed $unset of a filter must force full replace on non-view AE indexes",
        batch.finalChangeEvents.getFirst().getFilterFieldUpdates().isPresent());
    assertEquals(
        Integer.valueOf(1),
        batch.partialEmbedFallbacks.get(
            PartialEmbedFallbackReason.INTRA_BATCH_PREDECESSOR_REQUIRES_FULL));
  }

  @Test
  public void nonViewDedup_collapsedAncestorOverwrite_forcesSurvivorFull() {
    // Older event $sets a strict ancestor of a nested filter (drops meta.category); newer
    // survivor is an unrelated filter-only update.
    VectorIndexDefinition indexDef =
        VectorIndexDefinitionBuilder.builder()
            .withAutoEmbedField("description")
            .withFilterPath("meta.category")
            .withFilterPath("status")
            .build();
    assertTrue(MaterializedViewIndexDefinitionGeneration.isMaterializedViewBasedIndex(indexDef));

    UpdateDescription ancestorOverwrite =
        new UpdateDescription(null, new BsonDocument("meta", new BsonNull()));
    BsonDocument afterOverwrite =
        new BsonDocument()
            .append("_id", new BsonInt32(0))
            .append("description", new BsonString("d"))
            .append("meta", new BsonNull())
            .append("status", new BsonString("draft"))
            .append(indexDef.getIndexId().toString(), new BsonDocument("_id", new BsonInt32(0)));
    ChangeStreamDocument<RawBsonDocument> olderDropsNestedFilter =
        ChangeStreamUtils.dataEvent(OperationType.UPDATE, 0, ancestorOverwrite, afterOverwrite);

    UpdateDescription statusDesc =
        new UpdateDescription(null, new BsonDocument("status", new BsonString("published")));
    BsonDocument afterStatus =
        new BsonDocument()
            .append("_id", new BsonInt32(0))
            .append("description", new BsonString("d"))
            .append("meta", new BsonNull())
            .append("status", new BsonString("published"))
            .append(indexDef.getIndexId().toString(), new BsonDocument("_id", new BsonInt32(0)));
    ChangeStreamDocument<RawBsonDocument> newerFilterOnly =
        ChangeStreamUtils.dataEvent(OperationType.UPDATE, 0, statusDesc, afterStatus);

    var batch =
        ChangeStreamDocumentUtils.handleDocumentEvents(
            List.of(olderDropsNestedFilter, newerFilterOnly),
            indexDef,
            indexDef.createFieldDefinitionResolver(IndexFormatVersion.CURRENT),
            false,
            ChangeStreamDocumentUtils.deriveViewFieldDependencies(indexDef));

    assertEquals(1, batch.finalChangeEvents.size());
    assertFalse(
        "collapsed ancestor overwrite of a filter must force full replace on non-view AE indexes",
        batch.finalChangeEvents.getFirst().getFilterFieldUpdates().isPresent());
    assertEquals(
        Integer.valueOf(1),
        batch.partialEmbedFallbacks.get(
            PartialEmbedFallbackReason.INTRA_BATCH_PREDECESSOR_REQUIRES_FULL));
  }

  @Test
  public void nonViewDedup_collapsedFilterDrop_thenPartialEmbed_forcesSurvivorFull() {
    // Older $unset of a filter, newer survivor is a partial-embed update. Without forcing full
    // replace the embed $set would land while the removed filter stayed stale on the MV.
    VectorIndexDefinition indexDef =
        VectorIndexDefinitionBuilder.builder()
            .withAutoEmbedField("description")
            .withFilterPath("category")
            .build();
    assertTrue(MaterializedViewIndexDefinitionGeneration.isMaterializedViewBasedIndex(indexDef));

    UpdateDescription unsetDesc = new UpdateDescription(List.of("category"), new BsonDocument());
    BsonDocument afterUnset =
        new BsonDocument()
            .append("_id", new BsonInt32(0))
            .append("description", new BsonString("old text"))
            .append(indexDef.getIndexId().toString(), new BsonDocument("_id", new BsonInt32(0)));
    ChangeStreamDocument<RawBsonDocument> olderUnsetsFilter =
        ChangeStreamUtils.dataEvent(OperationType.UPDATE, 0, unsetDesc, afterUnset);

    UpdateDescription embedDesc =
        new UpdateDescription(null, new BsonDocument("description", new BsonString("new text")));
    BsonDocument afterEmbed =
        new BsonDocument()
            .append("_id", new BsonInt32(0))
            .append("description", new BsonString("new text"))
            .append(indexDef.getIndexId().toString(), new BsonDocument("_id", new BsonInt32(0)));
    ChangeStreamDocument<RawBsonDocument> newerEmbedChange =
        ChangeStreamUtils.dataEvent(OperationType.UPDATE, 0, embedDesc, afterEmbed);

    var batch =
        ChangeStreamDocumentUtils.handleDocumentEvents(
            List.of(olderUnsetsFilter, newerEmbedChange),
            indexDef,
            indexDef.createFieldDefinitionResolver(IndexFormatVersion.CURRENT),
            false,
            ChangeStreamDocumentUtils.deriveViewFieldDependencies(indexDef));

    assertEquals(1, batch.finalChangeEvents.size());
    assertFalse(
        "collapsed $unset of a filter before a partial-embed survivor must force full replace",
        batch.finalChangeEvents.getFirst().getFilterFieldUpdates().isPresent());
    assertEquals(
        Integer.valueOf(1),
        batch.partialEmbedFallbacks.get(
            PartialEmbedFallbackReason.INTRA_BATCH_PREDECESSOR_REQUIRES_FULL));
  }

  @Test
  public void nonViewDedup_additiveFilterOnlyCollapse_keepsPartialOptimization() {
    // Two additive filter-only updates must still collapse to a partial $set that aggregates
    // both filter values from the survivor's fullDocument — the fix must not force full replace
    // when no predecessor is filter-destructive.
    VectorIndexDefinition indexDef =
        VectorIndexDefinitionBuilder.builder()
            .withAutoEmbedField("description")
            .withFilterPath("category")
            .withFilterPath("status")
            .build();
    assertTrue(MaterializedViewIndexDefinitionGeneration.isMaterializedViewBasedIndex(indexDef));

    UpdateDescription firstFilterDesc =
        new UpdateDescription(null, new BsonDocument("category", new BsonString("electronics")));
    BsonDocument afterFirst =
        new BsonDocument()
            .append("_id", new BsonInt32(0))
            .append("description", new BsonString("d"))
            .append("category", new BsonString("electronics"))
            .append("status", new BsonString("draft"))
            .append(indexDef.getIndexId().toString(), new BsonDocument("_id", new BsonInt32(0)));
    ChangeStreamDocument<RawBsonDocument> firstFilterEvent =
        ChangeStreamUtils.dataEvent(OperationType.UPDATE, 0, firstFilterDesc, afterFirst);

    UpdateDescription secondFilterDesc =
        new UpdateDescription(null, new BsonDocument("status", new BsonString("published")));
    BsonDocument afterSecond =
        new BsonDocument()
            .append("_id", new BsonInt32(0))
            .append("description", new BsonString("d"))
            .append("category", new BsonString("electronics"))
            .append("status", new BsonString("published"))
            .append(indexDef.getIndexId().toString(), new BsonDocument("_id", new BsonInt32(0)));
    ChangeStreamDocument<RawBsonDocument> secondFilterEvent =
        ChangeStreamUtils.dataEvent(OperationType.UPDATE, 0, secondFilterDesc, afterSecond);

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
    assertEquals(
        new BsonString("electronics"), event.getFilterFieldUpdates().get().get("category"));
    assertEquals(new BsonString("published"), event.getFilterFieldUpdates().get().get("status"));
    assertTrue(
        batch.partialEmbedFallbacks.get(
                PartialEmbedFallbackReason.INTRA_BATCH_PREDECESSOR_REQUIRES_FULL)
            == null);
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
    assertEquals(
        Integer.valueOf(1),
        batch.partialEmbedFallbacks.get(
            PartialEmbedFallbackReason.INTRA_BATCH_PREDECESSOR_REQUIRES_FULL));
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

  private static SearchIndexDefinition searchAutoEmbedIndex() {
    SearchAutoEmbedFieldDefinition autoEmbed =
        new SearchAutoEmbedFieldDefinition("voyage-3-large", FieldPath.parse("body"));
    return SearchIndexDefinitionBuilder.builder()
        .defaultMetadata()
        .mappings(
            DocumentFieldDefinitionBuilder.builder()
                .dynamic(false)
                .field(
                    "embedding",
                    FieldDefinitionBuilder.builder().searchAutoEmbed(autoEmbed).build())
                .field(
                    "category",
                    FieldDefinitionBuilder.builder().bool(new BooleanFieldDefinition()).build())
                .build())
        .build();
  }

  private static ChangeStreamDocument<RawBsonDocument> createSearchUpdateEvent(
      SearchIndexDefinition indexDef, String fieldName) {
    UpdateDescription updateDescription =
        new UpdateDescription(null, new BsonDocument(fieldName, new BsonString("updated")));
    return ChangeStreamUtils.updateEvent(0, updateDescription, indexDef);
  }

  // View auto-embed decision metrics.

  private static VectorIndexDefinition viewIndex(List<BsonDocument> pipeline) {
    return VectorIndexDefinitionBuilder.builder()
        .withAutoEmbedField("description")
        .withFilterPath("category")
        .withFilterPath("isPublished")
        .view(ViewDefinition.existing("myView", pipeline))
        .build();
  }

  private static ChangeStreamDocumentUtils.DocumentEventBatch handleView(
      VectorIndexDefinition indexDef, ChangeStreamDocument<RawBsonDocument> event) {
    return ChangeStreamDocumentUtils.handleDocumentEvents(
        List.of(event),
        indexDef,
        indexDef.createFieldDefinitionResolver(IndexFormatVersion.CURRENT),
        false,
        ChangeStreamDocumentUtils.deriveViewFieldDependencies(indexDef));
  }

  @Test
  public void viewDecisionMetrics_partialAppliedAndTriggerHit() {
    // Unrelated filter change -> partial path: decision NONE + one applied.
    VectorIndexDefinition matchView =
        viewIndex(
            List.of(
                BsonDocument.parse(
                    "{\"$match\": {\"$expr\": {\"$eq\": [\"$isPublished\", true]}}}")));
    var partial = handleView(matchView, createFilterOnlyUpdateEvent(matchView, "category"));
    assertEquals(Integer.valueOf(1), partial.viewDecisions.get(ViewFullProcessingReason.NONE));
    assertEquals(1, partial.viewFilterOnlyPartialApplied);

    // Membership change (isPublished feeds the $match) -> full path: decision TRIGGER_HIT. (An
    // embed-feeder change now takes the partial-embed path; that's covered by the partial-embed
    // tests, so this uses a membership change for the genuine full/trigger case.)
    BsonDocument membershipDoc =
        new BsonDocument()
            .append("_id", new BsonInt32(0))
            .append("description", new BsonString("d"))
            .append("category", new BsonString("updated"))
            .append("isPublished", new BsonBoolean(false))
            .append(matchView.getIndexId().toString(), new BsonDocument("_id", new BsonInt32(0)));
    var full =
        handleView(
            matchView,
            ChangeStreamUtils.dataEvent(
                OperationType.UPDATE,
                0,
                new UpdateDescription(
                    null, new BsonDocument("isPublished", new BsonBoolean(false))),
                membershipDoc));
    assertEquals(Integer.valueOf(1), full.viewDecisions.get(ViewFullProcessingReason.TRIGGER_HIT));
    assertEquals(0, full.viewFilterOnlyPartialApplied);

    // Pin the Prometheus-visible names + tags across both batches.
    MetricsFactory metricsFactory = new MetricsFactory("test", new SimpleMeterRegistry());
    ViewFieldDependenciesMetricCounters counters =
        new ViewFieldDependenciesMetricCounters(metricsFactory, Tags.empty());
    counters.recordBatch(partial);
    counters.recordBatch(full);
    assertEquals(
        1.0,
        metricsFactory
            .get("viewFieldDependenciesDecision", Tags.of("result", "partial", "reason", "none"))
            .counter()
            .count(),
        0.0);
    assertEquals(
        1.0,
        metricsFactory.get("viewFilterOnlyPartialApplied", Tags.empty()).counter().count(),
        0.0);
    assertEquals(
        1.0,
        metricsFactory
            .get(
                "viewFieldDependenciesDecision", Tags.of("result", "full", "reason", "trigger_hit"))
            .counter()
            .count(),
        0.0);
  }

  @Test
  public void viewDerivationFallback_reportedOnlyWhenFlagSet() {
    // A $project view can't be analyzed: DEPENDS_ON_EVERYTHING each event, and the fallback reason
    // rides the batch only when the reporting flag is set (avoids counting it every batch).
    VectorIndexDefinition indexDef =
        viewIndex(List.of(BsonDocument.parse("{\"$project\": {\"description\": 1}}")));
    var reported =
        ChangeStreamDocumentUtils.handleDocumentEvents(
            List.of(createFilterOnlyUpdateEvent(indexDef, "category")),
            indexDef,
            indexDef.createFieldDefinitionResolver(IndexFormatVersion.CURRENT),
            false,
            ChangeStreamDocumentUtils.deriveViewFieldDependencies(indexDef),
            Optional.empty(),
            true /* reportDerivationFallback */);
    assertEquals(
        Integer.valueOf(1),
        reported.viewDecisions.get(ViewFullProcessingReason.DEPENDS_ON_EVERYTHING));
    assertEquals(
        Optional.of(ViewDerivationFallbackReason.UNSUPPORTED_SHAPE),
        reported.viewDerivationFallbackReason);

    var notReported = handleView(indexDef, createFilterOnlyUpdateEvent(indexDef, "category"));
    assertTrue(notReported.viewDerivationFallbackReason.isEmpty());

    MetricsFactory metricsFactory = new MetricsFactory("test", new SimpleMeterRegistry());
    ViewFieldDependenciesMetricCounters counters =
        new ViewFieldDependenciesMetricCounters(metricsFactory, Tags.empty());
    counters.recordBatch(reported);
    counters.recordBatch(notReported);
    assertEquals(
        1.0,
        metricsFactory
            .get("viewFieldDependenciesDerivationFallback", Tags.of("reason", "unsupported_shape"))
            .counter()
            .count(),
        0.0);
  }
}
