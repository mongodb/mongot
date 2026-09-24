package com.xgen.mongot.replication.mongodb.common;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.OperationType;
import com.mongodb.client.model.changestream.TruncatedArray;
import com.mongodb.client.model.changestream.UpdateDescription;
import com.xgen.mongot.embedding.utils.PartialEmbedFallbackReason;
import com.xgen.mongot.index.DocumentEvent;
import com.xgen.mongot.index.definition.DocumentFieldDefinition;
import com.xgen.mongot.index.definition.SearchAutoEmbedFieldDefinition;
import com.xgen.mongot.index.definition.SearchIndexDefinition;
import com.xgen.mongot.index.definition.ViewDefinition;
import com.xgen.mongot.index.version.IndexFormatVersion;
import com.xgen.mongot.util.FieldPath;
import com.xgen.testing.mongot.index.definition.DocumentFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.EmbeddedDocumentsFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.EmbeddingModelCatalogTestUtils;
import com.xgen.testing.mongot.index.definition.FieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.SearchIndexDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.StringFieldDefinitionBuilder;
import com.xgen.testing.mongot.replication.mongodb.ChangeStreamUtils;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonNull;
import org.bson.BsonString;
import org.bson.RawBsonDocument;
import org.junit.Before;
import org.junit.Test;

/** Tests non-view Search autoEmbed partial-update routing and safety. */
public class ChangeStreamDocumentUtilsSearchPartialUpdateTest {

  private static final FieldPath TITLE = FieldPath.parse("title");
  private static final FieldPath SUMMARY = FieldPath.parse("summary");

  @Before
  public void registerEmbeddingModels() {
    EmbeddingModelCatalogTestUtils.resetCatalogAndRegisterAutoEmbedModel("voyage-3-large");
  }

  @Test
  public void staticSearch_pathUpdate_selectivelyReembedsChangedField() {
    SearchIndexDefinition indexDefinition = staticSearchIndex();
    ChangeStreamDocument<RawBsonDocument> event =
        updateEvent(
            indexDefinition,
            new UpdateDescription(
                null, new BsonDocument(TITLE.toString(), new BsonString("new title"))),
            standardDocument("new title"));

    var batch = handle(indexDefinition, List.of(event));

    assertThat(batch.finalChangeEvents).hasSize(1);
    DocumentEvent documentEvent = batch.finalChangeEvents.getFirst();
    assertThat(documentEvent.getChangedEmbedTexts()).containsExactly(TITLE, "new title");
    assertThat(documentEvent.getFilterFieldUpdates().orElseThrow().getString("category").getValue())
        .isEqualTo("news");
    assertThat(documentEvent.getChangedEmbedTexts()).doesNotContainKey(SUMMARY);
    assertThat(batch.partialEmbedApplied).isEqualTo(1);
  }

  @Test
  public void staticSearch_lexicalOnlyUpdate_usesPartialSetWithoutEmbedding() {
    SearchIndexDefinition indexDefinition = staticSearchIndex();
    ChangeStreamDocument<RawBsonDocument> event =
        updateEvent(
            indexDefinition,
            new UpdateDescription(null, new BsonDocument("category", new BsonString("updated"))),
            standardDocument("old title").append("category", new BsonString("updated")));

    var batch = handle(indexDefinition, List.of(event));

    DocumentEvent documentEvent = batch.finalChangeEvents.getFirst();
    assertTrue(documentEvent.isFilterOnlyUpdate());
    assertThat(documentEvent.getFilterFieldUpdates().orElseThrow().getString("category").getValue())
        .isEqualTo("updated");
    assertThat(batch.filterOnlyPartialApplied).isEqualTo(1);
  }

  @Test
  public void staticSearch_documentValuedProfileReplacement_setsDeclaredSubtree() {
    SearchIndexDefinition indexDefinition = documentPassthroughSearchIndex();
    BsonDocument replacementProfile =
        new BsonDocument("label", new BsonString("updated"))
            .append("unmapped", new BsonString("preserved"));
    ChangeStreamDocument<RawBsonDocument> event =
        updateEvent(
            indexDefinition,
            new UpdateDescription(null, new BsonDocument("profile", replacementProfile)),
            standardDocument("old title").append("profile", replacementProfile));

    var batch = handle(indexDefinition, List.of(event));

    assertThat(batch.finalChangeEvents).hasSize(1);
    DocumentEvent documentEvent = batch.finalChangeEvents.getFirst();
    assertTrue(documentEvent.isFilterOnlyUpdate());
    assertThat(documentEvent.getFilterFieldUpdates().orElseThrow().getDocument("profile"))
        .isEqualTo(new BsonDocument("label", new BsonString("updated")));
    assertThat(batch.filterOnlyPartialApplied).isEqualTo(1);
  }

  @Test
  public void
      staticSearch_documentValuedProfileReplacementDropsDeclaredChild_fallsBackToFullReplace() {
    SearchIndexDefinition indexDefinition = documentPassthroughSearchIndex();
    BsonDocument replacementProfile = new BsonDocument("unmapped", new BsonString("preserved"));
    ChangeStreamDocument<RawBsonDocument> event =
        updateEvent(
            indexDefinition,
            new UpdateDescription(null, new BsonDocument("profile", replacementProfile)),
            standardDocument("old title").append("profile", replacementProfile));

    assertFullFallback(
        handle(indexDefinition, List.of(event)), PartialEmbedFallbackReason.ANCESTOR_OVERWRITE);
  }

  @Test
  public void staticSearch_emptyItemsReplacement_setsCompleteArray() {
    SearchIndexDefinition indexDefinition = embeddedDocumentsPassthroughSearchIndex();
    BsonArray replacementItems = new BsonArray();
    ChangeStreamDocument<RawBsonDocument> event =
        updateEvent(
            indexDefinition,
            new UpdateDescription(null, new BsonDocument("items", replacementItems)),
            standardDocument("old title").append("items", replacementItems));

    var batch = handle(indexDefinition, List.of(event));

    assertThat(batch.finalChangeEvents).hasSize(1);
    DocumentEvent documentEvent = batch.finalChangeEvents.getFirst();
    assertTrue(documentEvent.isFilterOnlyUpdate());
    assertThat(documentEvent.getFilterFieldUpdates().orElseThrow().getArray("items"))
        .isEqualTo(replacementItems);
    assertThat(batch.filterOnlyPartialApplied).isEqualTo(1);
  }

  @Test
  public void staticSearch_indexedItemProfileReplacement_setsCompleteItemsRoot() {
    SearchIndexDefinition indexDefinition = embeddedDocumentsPassthroughSearchIndex();
    BsonDocument replacementProfile =
        new BsonDocument("name", new BsonString("updated"))
            .append("unmapped", new BsonString("preserved"));
    BsonArray items =
        new BsonArray(
            List.of(
                new BsonDocument("label", new BsonString("first"))
                    .append("profile", replacementProfile)
                    .append("unrelated", new BsonString("first value")),
                new BsonDocument("label", new BsonString("second"))
                    .append(
                        "profile",
                        new BsonDocument("name", new BsonString("unchanged"))
                            .append("unmapped", new BsonString("also preserved")))
                    .append("unrelated", new BsonString("second value"))));
    ChangeStreamDocument<RawBsonDocument> event =
        updateEvent(
            indexDefinition,
            new UpdateDescription(null, new BsonDocument("items.0.profile", replacementProfile)),
            standardDocument("old title").append("items", items));

    var batch = handle(indexDefinition, List.of(event));

    assertThat(batch.finalChangeEvents).hasSize(1);
    DocumentEvent documentEvent = batch.finalChangeEvents.getFirst();
    assertTrue(documentEvent.isFilterOnlyUpdate());
    assertThat(documentEvent.getFilterFieldUpdates().orElseThrow().getArray("items"))
        .isEqualTo(items);
    assertThat(batch.filterOnlyPartialApplied).isEqualTo(1);
  }

  @Test
  public void staticSearch_indexedItemLeafUpdate_setsCompleteItemsRoot() {
    SearchIndexDefinition indexDefinition = embeddedDocumentsPassthroughSearchIndex();
    BsonArray items =
        new BsonArray(
            List.of(
                new BsonDocument("label", new BsonString("renamed"))
                    .append("other", new BsonString("unchanged"))));
    ChangeStreamDocument<RawBsonDocument> event =
        updateEvent(
            indexDefinition,
            new UpdateDescription(
                null, new BsonDocument("items.0.label", new BsonString("renamed"))),
            standardDocument("old title").append("items", items));

    var batch = handle(indexDefinition, List.of(event));

    assertThat(batch.finalChangeEvents).hasSize(1);
    DocumentEvent documentEvent = batch.finalChangeEvents.getFirst();
    assertTrue(documentEvent.isFilterOnlyUpdate());
    assertThat(documentEvent.getFilterFieldUpdates().orElseThrow().getArray("items"))
        .isEqualTo(items);
    assertThat(batch.filterOnlyPartialApplied).isEqualTo(1);
  }

  @Test
  public void staticSearch_removedLexicalField_fallsBackToFullReplace() {
    SearchIndexDefinition indexDefinition = staticSearchIndex();
    BsonDocument fullDocument = standardDocument("old title");
    fullDocument.remove("category");
    ChangeStreamDocument<RawBsonDocument> event =
        updateEvent(
            indexDefinition,
            new UpdateDescription(List.of("category"), new BsonDocument()),
            fullDocument);

    assertFullFallback(
        handle(indexDefinition, List.of(event)),
        PartialEmbedFallbackReason.DESTRUCTIVE_INDEXED_TOUCH);
  }

  @Test
  public void staticSearch_removedPath_fallsBackToFullReplace() {
    SearchIndexDefinition indexDefinition = staticSearchIndex();
    BsonDocument fullDocument = standardDocument("old title");
    fullDocument.remove(TITLE.toString());
    ChangeStreamDocument<RawBsonDocument> event =
        updateEvent(
            indexDefinition,
            new UpdateDescription(List.of(TITLE.toString()), new BsonDocument()),
            fullDocument);

    assertFullFallback(
        handle(indexDefinition, List.of(event)),
        PartialEmbedFallbackReason.DESTRUCTIVE_INDEXED_TOUCH);
  }

  @Test
  public void staticSearch_truncatedLexicalArray_fallsBackToFullReplace() {
    SearchIndexDefinition indexDefinition = staticSearchIndex();
    UpdateDescription updateDescription =
        new UpdateDescription(
            List.of(), new BsonDocument(), List.of(new TruncatedArray("tags", 1)));
    ChangeStreamDocument<RawBsonDocument> event =
        updateEvent(
            indexDefinition,
            updateDescription,
            standardDocument("old title")
                .append("tags", new BsonArray(List.of(new BsonString("one")))));

    assertFullFallback(
        handle(indexDefinition, List.of(event)),
        PartialEmbedFallbackReason.DESTRUCTIVE_INDEXED_TOUCH);
  }

  @Test
  public void staticSearch_ancestorOverwriteOfLexicalField_fallsBackToFullReplace() {
    SearchIndexDefinition indexDefinition = staticSearchIndex();
    ChangeStreamDocument<RawBsonDocument> event =
        updateEvent(
            indexDefinition,
            new UpdateDescription(null, new BsonDocument("metadata", new BsonString("replaced"))),
            standardDocument("old title").append("metadata", new BsonString("replaced")));

    assertFullFallback(
        handle(indexDefinition, List.of(event)), PartialEmbedFallbackReason.ANCESTOR_OVERWRITE);
  }

  @Test
  public void staticSearch_exactDocumentOverwrite_fallsBackToFullReplace() {
    SearchIndexDefinition indexDefinition = documentPassthroughSearchIndex();
    ChangeStreamDocument<RawBsonDocument> event =
        updateEvent(
            indexDefinition,
            new UpdateDescription(null, new BsonDocument("profile", new BsonNull())),
            standardDocument("old title").append("profile", new BsonNull()));

    assertFullFallback(
        handle(indexDefinition, List.of(event)), PartialEmbedFallbackReason.ANCESTOR_OVERWRITE);
  }

  @Test
  public void staticSearch_exactEmbeddedDocumentsOverwrite_fallsBackToFullReplace() {
    SearchIndexDefinition indexDefinition = embeddedDocumentsPassthroughSearchIndex();
    ChangeStreamDocument<RawBsonDocument> event =
        updateEvent(
            indexDefinition,
            new UpdateDescription(null, new BsonDocument("items", new BsonNull())),
            standardDocument("old title").append("items", new BsonNull()));

    assertFullFallback(
        handle(indexDefinition, List.of(event)), PartialEmbedFallbackReason.ANCESTOR_OVERWRITE);
  }

  @Test
  public void staticSearch_exactEmbeddedDocumentsScalarOverwrite_fallsBackToFullReplace() {
    SearchIndexDefinition indexDefinition = embeddedDocumentsPassthroughSearchIndex();
    ChangeStreamDocument<RawBsonDocument> event =
        updateEvent(
            indexDefinition,
            new UpdateDescription(null, new BsonDocument("items", new BsonInt32(5))),
            standardDocument("old title").append("items", new BsonInt32(5)));

    assertFullFallback(
        handle(indexDefinition, List.of(event)), PartialEmbedFallbackReason.ANCESTOR_OVERWRITE);
  }

  @Test
  public void staticSearch_indirectEmbedTouch_fallsBackToFullReplace() {
    SearchIndexDefinition indexDefinition = staticSearchIndex();
    ChangeStreamDocument<RawBsonDocument> event =
        updateEvent(
            indexDefinition,
            new UpdateDescription(
                null, new BsonDocument("title.value", new BsonString("new title"))),
            standardDocument("old title")
                .append("title", new BsonDocument("value", new BsonString("new title"))));

    assertFullFallback(
        handle(indexDefinition, List.of(event)), PartialEmbedFallbackReason.INDIRECT_EMBED_TOUCH);
  }

  @Test
  public void staticSearch_embedPathTraversesArray_fallsBackToFullReplace() {
    SearchIndexDefinition indexDefinition = arrayEmbedSearchIndex();
    BsonArray items =
        new BsonArray(List.of(new BsonDocument("title", new BsonString("new title"))));
    ChangeStreamDocument<RawBsonDocument> event =
        updateEvent(
            indexDefinition,
            new UpdateDescription(
                null, new BsonDocument("items.title", new BsonString("new title"))),
            new BsonDocument("_id", new BsonInt32(0)).append("items", items));

    assertFullFallback(
        handle(indexDefinition, List.of(event)), PartialEmbedFallbackReason.ARRAY_TRAVERSAL);
  }

  @Test
  public void staticSearch_emptyEmbedText_fallsBackToFullReplace() {
    SearchIndexDefinition indexDefinition = staticSearchIndex();
    ChangeStreamDocument<RawBsonDocument> event =
        updateEvent(
            indexDefinition,
            new UpdateDescription(null, new BsonDocument("title", new BsonString(""))),
            standardDocument(""));

    assertFullFallback(
        handle(indexDefinition, List.of(event)), PartialEmbedFallbackReason.EMPTY_NEW_VALUE);
  }

  @Test
  public void staticSearch_replaceEvent_remainsFullReplace() {
    SearchIndexDefinition indexDefinition = staticSearchIndex();
    ChangeStreamDocument<RawBsonDocument> event =
        ChangeStreamUtils.dataEvent(
            OperationType.REPLACE, 0, null, standardDocument("replacement title"));

    assertFullFallback(
        handle(indexDefinition, List.of(event)), PartialEmbedFallbackReason.REPLACE_EVENT);
  }

  @Test
  public void staticSearch_insertDuringInitialSync_remainsFullWrite() {
    SearchIndexDefinition indexDefinition = staticSearchIndex();
    ChangeStreamDocument<RawBsonDocument> event =
        ChangeStreamUtils.dataEvent(
            OperationType.INSERT, 0, null, standardDocument("initial title"));

    var batch =
        ChangeStreamDocumentUtils.handleDocumentEvents(
            List.of(event),
            indexDefinition,
            indexDefinition.createFieldDefinitionResolver(IndexFormatVersion.CURRENT),
            true,
            Optional.empty());

    DocumentEvent documentEvent = batch.finalChangeEvents.getFirst();
    assertThat(documentEvent.getEventType()).isEqualTo(DocumentEvent.EventType.INSERT);
    assertFalse(documentEvent.getFilterFieldUpdates().isPresent());
    assertThat(documentEvent.getChangedEmbedTexts()).isEmpty();
  }

  @Test
  public void staticSearch_forceFullFlag_overridesPartialEligibility() {
    SearchIndexDefinition indexDefinition = staticSearchIndex();
    ChangeStreamDocument<RawBsonDocument> event =
        updateEvent(
            indexDefinition,
            new UpdateDescription(null, new BsonDocument("category", new BsonString("updated"))),
            standardDocument("old title").append("category", new BsonString("updated")));
    AtomicBoolean forceFull = new AtomicBoolean(true);
    AutoEmbeddingMaterializedViewConfig config = AutoEmbeddingMaterializedViewConfig.getDefault();
    config.setControlKnobEvaluator(ignored -> forceFull.get());

    var batch =
        ChangeStreamDocumentUtils.handleDocumentEvents(
            List.of(event),
            indexDefinition,
            indexDefinition.createFieldDefinitionResolver(IndexFormatVersion.CURRENT),
            false,
            Optional.empty(),
            Optional.of(config));

    assertFullFallback(batch, PartialEmbedFallbackReason.FEATURE_FLAG_FORCED_FULL_UPDATE);
  }

  @Test
  public void rootDynamicSearch_embedUpdate_remainsFullOnly() {
    SearchIndexDefinition indexDefinition = rootDynamicSearchIndex();
    ChangeStreamDocument<RawBsonDocument> event =
        updateEvent(
            indexDefinition,
            new UpdateDescription(
                null, new BsonDocument(TITLE.toString(), new BsonString("new title"))),
            standardDocument("new title"));

    assertFullWithoutPartialMetrics(handle(indexDefinition, List.of(event)));
  }

  @Test
  public void nestedDynamicSearch_embedUpdate_remainsFullOnly() {
    SearchIndexDefinition indexDefinition = nestedDynamicSearchIndex();
    ChangeStreamDocument<RawBsonDocument> event =
        updateEvent(
            indexDefinition,
            new UpdateDescription(
                null, new BsonDocument(TITLE.toString(), new BsonString("new title"))),
            standardDocument("new title")
                .append("profile", new BsonDocument("unmapped", new BsonString("value"))));

    assertFullWithoutPartialMetrics(handle(indexDefinition, List.of(event)));
  }

  @Test
  public void staticSearch_reservedNamespacePassthrough_embedUpdate_remainsFullOnly() {
    SearchIndexDefinition indexDefinition = reservedNamespacePassthroughSearchIndex();
    ChangeStreamDocument<RawBsonDocument> event =
        updateEvent(
            indexDefinition,
            new UpdateDescription(
                null, new BsonDocument(TITLE.toString(), new BsonString("new title"))),
            standardDocument("new title")
                .append(
                    "_autoEmbed", new BsonDocument("passthrough", new BsonString("preserved"))));

    assertFullWithoutPartialMetrics(handle(indexDefinition, List.of(event)));
  }

  @Test
  public void staticSearch_reservedNamespacePassthrough_lexicalUpdate_remainsFullOnly() {
    SearchIndexDefinition indexDefinition = reservedNamespacePassthroughSearchIndex();
    ChangeStreamDocument<RawBsonDocument> event =
        updateEvent(
            indexDefinition,
            new UpdateDescription(null, new BsonDocument("category", new BsonString("updated"))),
            standardDocument("old title")
                .append("category", new BsonString("updated"))
                .append(
                    "_autoEmbed", new BsonDocument("passthrough", new BsonString("preserved"))));

    assertFullWithoutPartialMetrics(handle(indexDefinition, List.of(event)));
  }

  @Test
  public void viewBackedStaticSearch_embedUpdate_remainsFullOnly() {
    SearchIndexDefinition baseDefinition = staticSearchIndex();
    SearchIndexDefinition indexDefinition =
        SearchIndexDefinitionBuilder.builder()
            .defaultMetadata()
            .mappings(baseDefinition.getMappings())
            .view(ViewDefinition.existing("searchView", List.of()))
            .build();
    ChangeStreamDocument<RawBsonDocument> event =
        updateEvent(
            indexDefinition,
            new UpdateDescription(
                null, new BsonDocument(TITLE.toString(), new BsonString("new title"))),
            standardDocument("new title"));

    assertFullWithoutPartialMetrics(handle(indexDefinition, List.of(event)));
  }

  @Test
  public void staticSearch_sameIdEmbedPredecessorAndLexicalSurvivor_forcesFullReplace() {
    SearchIndexDefinition indexDefinition = staticSearchIndex();
    ChangeStreamDocument<RawBsonDocument> embedPredecessor =
        updateEvent(
            indexDefinition,
            new UpdateDescription(
                null, new BsonDocument(TITLE.toString(), new BsonString("new title"))),
            standardDocument("new title"));
    ChangeStreamDocument<RawBsonDocument> lexicalSurvivor =
        updateEvent(
            indexDefinition,
            new UpdateDescription(null, new BsonDocument("category", new BsonString("updated"))),
            standardDocument("new title").append("category", new BsonString("updated")));

    assertFullFallback(
        handle(indexDefinition, List.of(embedPredecessor, lexicalSurvivor)),
        PartialEmbedFallbackReason.INTRA_BATCH_PREDECESSOR_REQUIRES_FULL);
  }

  @Test
  public void staticSearch_sameIdDestructivePredecessorAndEmbedSurvivor_forcesFullReplace() {
    SearchIndexDefinition indexDefinition = staticSearchIndex();
    BsonDocument afterDestructiveUpdate = standardDocument("old title");
    afterDestructiveUpdate.remove("category");
    ChangeStreamDocument<RawBsonDocument> destructivePredecessor =
        updateEvent(
            indexDefinition,
            new UpdateDescription(List.of("category"), new BsonDocument()),
            afterDestructiveUpdate);
    BsonDocument afterEmbedUpdate = standardDocument("new title");
    afterEmbedUpdate.remove("category");
    ChangeStreamDocument<RawBsonDocument> embedSurvivor =
        updateEvent(
            indexDefinition,
            new UpdateDescription(
                null, new BsonDocument(TITLE.toString(), new BsonString("new title"))),
            afterEmbedUpdate);

    assertFullFallback(
        handle(indexDefinition, List.of(destructivePredecessor, embedSurvivor)),
        PartialEmbedFallbackReason.INTRA_BATCH_PREDECESSOR_REQUIRES_FULL);
  }

  @Test
  public void staticSearch_sameIdDocumentOverwritePredecessor_forcesFullReplace() {
    SearchIndexDefinition indexDefinition = documentPassthroughSearchIndex();
    ChangeStreamDocument<RawBsonDocument> overwritePredecessor =
        updateEvent(
            indexDefinition,
            new UpdateDescription(null, new BsonDocument("profile", new BsonNull())),
            standardDocument("old title").append("profile", new BsonNull()));
    ChangeStreamDocument<RawBsonDocument> lexicalSurvivor =
        updateEvent(
            indexDefinition,
            new UpdateDescription(null, new BsonDocument("category", new BsonString("updated"))),
            standardDocument("old title")
                .append("profile", new BsonNull())
                .append("category", new BsonString("updated")));

    assertFullFallback(
        handle(indexDefinition, List.of(overwritePredecessor, lexicalSurvivor)),
        PartialEmbedFallbackReason.INTRA_BATCH_PREDECESSOR_REQUIRES_FULL);
  }

  @Test
  public void staticSearch_sameIdArrayTraversalPredecessor_forcesFullReplace() {
    SearchIndexDefinition indexDefinition = arrayEmbedSearchIndex();
    BsonArray items =
        new BsonArray(List.of(new BsonDocument("title", new BsonString("new title"))));
    BsonDocument afterArrayUpdate =
        new BsonDocument("_id", new BsonInt32(0))
            .append("items", items)
            .append("category", new BsonString("news"));
    ChangeStreamDocument<RawBsonDocument> arrayTraversalPredecessor =
        updateEvent(
            indexDefinition,
            new UpdateDescription(
                null, new BsonDocument("items.title", new BsonString("new title"))),
            afterArrayUpdate);
    ChangeStreamDocument<RawBsonDocument> lexicalSurvivor =
        updateEvent(
            indexDefinition,
            new UpdateDescription(null, new BsonDocument("category", new BsonString("updated"))),
            afterArrayUpdate.clone().append("category", new BsonString("updated")));

    assertPredecessorForcesFull(indexDefinition, arrayTraversalPredecessor, lexicalSurvivor);
  }

  @Test
  public void staticSearch_sameIdIndirectEmbedTouchPredecessor_forcesFullReplace() {
    SearchIndexDefinition indexDefinition = staticSearchIndex();
    BsonDocument afterIndirectUpdate =
        standardDocument("old title")
            .append("title", new BsonDocument("value", new BsonString("new title")));
    ChangeStreamDocument<RawBsonDocument> indirectEmbedTouchPredecessor =
        updateEvent(
            indexDefinition,
            new UpdateDescription(
                null, new BsonDocument("title.value", new BsonString("new title"))),
            afterIndirectUpdate);
    ChangeStreamDocument<RawBsonDocument> lexicalSurvivor =
        updateEvent(
            indexDefinition,
            new UpdateDescription(null, new BsonDocument("category", new BsonString("updated"))),
            afterIndirectUpdate.clone().append("category", new BsonString("updated")));

    assertPredecessorForcesFull(indexDefinition, indirectEmbedTouchPredecessor, lexicalSurvivor);
  }

  @Test
  public void staticSearch_sameIdEmptyEmbedTextPredecessor_forcesFullReplace() {
    SearchIndexDefinition indexDefinition = staticSearchIndex();
    ChangeStreamDocument<RawBsonDocument> emptyEmbedTextPredecessor =
        updateEvent(
            indexDefinition,
            new UpdateDescription(null, new BsonDocument("title", new BsonString(""))),
            standardDocument(""));
    ChangeStreamDocument<RawBsonDocument> lexicalSurvivor =
        updateEvent(
            indexDefinition,
            new UpdateDescription(null, new BsonDocument("category", new BsonString("updated"))),
            standardDocument("").append("category", new BsonString("updated")));

    assertPredecessorForcesFull(indexDefinition, emptyEmbedTextPredecessor, lexicalSurvivor);
  }

  private static ChangeStreamDocumentUtils.DocumentEventBatch handle(
      SearchIndexDefinition indexDefinition, List<ChangeStreamDocument<RawBsonDocument>> events) {
    return ChangeStreamDocumentUtils.handleDocumentEvents(
        events,
        indexDefinition,
        indexDefinition.createFieldDefinitionResolver(IndexFormatVersion.CURRENT),
        false,
        Optional.empty());
  }

  private static ChangeStreamDocument<RawBsonDocument> updateEvent(
      SearchIndexDefinition indexDefinition,
      UpdateDescription updateDescription,
      BsonDocument fullDocument) {
    assertTrue(indexDefinition.isAutoEmbeddingIndex());
    return ChangeStreamUtils.dataEvent(OperationType.UPDATE, 0, updateDescription, fullDocument);
  }

  private static void assertFullFallback(
      ChangeStreamDocumentUtils.DocumentEventBatch batch,
      PartialEmbedFallbackReason expectedReason) {
    assertThat(batch.finalChangeEvents).hasSize(1);
    DocumentEvent event = batch.finalChangeEvents.getFirst();
    assertFalse(event.getFilterFieldUpdates().isPresent());
    assertThat(event.getChangedEmbedTexts()).isEmpty();
    assertThat(batch.partialEmbedFallbacks).containsEntry(expectedReason, 1);
  }

  private static void assertPredecessorForcesFull(
      SearchIndexDefinition indexDefinition,
      ChangeStreamDocument<RawBsonDocument> predecessor,
      ChangeStreamDocument<RawBsonDocument> survivor) {
    var batch = handle(indexDefinition, List.of(predecessor, survivor));

    assertFullFallback(batch, PartialEmbedFallbackReason.INTRA_BATCH_PREDECESSOR_REQUIRES_FULL);
    assertThat(batch.partialEmbedFallbacks)
        .containsExactly(PartialEmbedFallbackReason.INTRA_BATCH_PREDECESSOR_REQUIRES_FULL, 1);
  }

  private static void assertFullWithoutPartialMetrics(
      ChangeStreamDocumentUtils.DocumentEventBatch batch) {
    assertThat(batch.finalChangeEvents).hasSize(1);
    assertFalse(batch.finalChangeEvents.getFirst().getFilterFieldUpdates().isPresent());
    assertThat(batch.partialEmbedApplied).isEqualTo(0);
    assertThat(batch.filterOnlyPartialApplied).isEqualTo(0);
    assertThat(batch.partialEmbedFallbacks).isEmpty();
  }

  private static BsonDocument standardDocument(String title) {
    return new BsonDocument()
        .append("_id", new BsonInt32(0))
        .append("title", new BsonString(title))
        .append("summary", new BsonString("unchanged summary"))
        .append("category", new BsonString("news"))
        .append("tags", new BsonArray(List.of(new BsonString("one"), new BsonString("two"))))
        .append("metadata", new BsonDocument("label", new BsonString("featured")));
  }

  private static SearchIndexDefinition staticSearchIndex() {
    DocumentFieldDefinition metadata =
        DocumentFieldDefinitionBuilder.builder()
            .dynamic(false)
            .field(
                "summaryVector",
                FieldDefinitionBuilder.builder()
                    .searchAutoEmbed(new SearchAutoEmbedFieldDefinition("voyage-3-large", SUMMARY))
                    .build())
            .field(
                "label",
                FieldDefinitionBuilder.builder()
                    .string(StringFieldDefinitionBuilder.builder().build())
                    .build())
            .build();
    DocumentFieldDefinition mappings =
        DocumentFieldDefinitionBuilder.builder()
            .dynamic(false)
            .field(
                "titleVector",
                FieldDefinitionBuilder.builder()
                    .searchAutoEmbed(new SearchAutoEmbedFieldDefinition("voyage-3-large", TITLE))
                    .build())
            .field(
                "category",
                FieldDefinitionBuilder.builder()
                    .string(StringFieldDefinitionBuilder.builder().build())
                    .build())
            .field(
                "tags",
                FieldDefinitionBuilder.builder()
                    .string(StringFieldDefinitionBuilder.builder().build())
                    .build())
            .field("metadata", FieldDefinitionBuilder.builder().document(metadata).build())
            .build();
    return SearchIndexDefinitionBuilder.builder().defaultMetadata().mappings(mappings).build();
  }

  private static SearchIndexDefinition rootDynamicSearchIndex() {
    DocumentFieldDefinition mappings =
        DocumentFieldDefinitionBuilder.builder()
            .dynamic(true)
            .field(
                "titleVector",
                FieldDefinitionBuilder.builder()
                    .searchAutoEmbed(new SearchAutoEmbedFieldDefinition("voyage-3-large", TITLE))
                    .build())
            .build();
    return SearchIndexDefinitionBuilder.builder().defaultMetadata().mappings(mappings).build();
  }

  private static SearchIndexDefinition nestedDynamicSearchIndex() {
    DocumentFieldDefinition profile =
        DocumentFieldDefinitionBuilder.builder().dynamic(true).build();
    DocumentFieldDefinition mappings =
        DocumentFieldDefinitionBuilder.builder()
            .dynamic(false)
            .field(
                "titleVector",
                FieldDefinitionBuilder.builder()
                    .searchAutoEmbed(new SearchAutoEmbedFieldDefinition("voyage-3-large", TITLE))
                    .build())
            .field("profile", FieldDefinitionBuilder.builder().document(profile).build())
            .build();
    return SearchIndexDefinitionBuilder.builder().defaultMetadata().mappings(mappings).build();
  }

  private static SearchIndexDefinition reservedNamespacePassthroughSearchIndex() {
    DocumentFieldDefinition autoEmbedNamespace =
        DocumentFieldDefinitionBuilder.builder()
            .dynamic(false)
            .field(
                "passthrough",
                FieldDefinitionBuilder.builder()
                    .string(StringFieldDefinitionBuilder.builder().build())
                    .build())
            .build();
    DocumentFieldDefinition mappings =
        DocumentFieldDefinitionBuilder.builder()
            .dynamic(false)
            .field(
                "titleVector",
                FieldDefinitionBuilder.builder()
                    .searchAutoEmbed(new SearchAutoEmbedFieldDefinition("voyage-3-large", TITLE))
                    .build())
            .field(
                "category",
                FieldDefinitionBuilder.builder()
                    .string(StringFieldDefinitionBuilder.builder().build())
                    .build())
            .field(
                "_autoEmbed", FieldDefinitionBuilder.builder().document(autoEmbedNamespace).build())
            .build();
    return SearchIndexDefinitionBuilder.builder().defaultMetadata().mappings(mappings).build();
  }

  private static SearchIndexDefinition documentPassthroughSearchIndex() {
    DocumentFieldDefinition profile =
        DocumentFieldDefinitionBuilder.builder()
            .dynamic(false)
            .field(
                "label",
                FieldDefinitionBuilder.builder()
                    .string(StringFieldDefinitionBuilder.builder().build())
                    .build())
            .build();
    DocumentFieldDefinition mappings =
        DocumentFieldDefinitionBuilder.builder()
            .dynamic(false)
            .field(
                "titleVector",
                FieldDefinitionBuilder.builder()
                    .searchAutoEmbed(new SearchAutoEmbedFieldDefinition("voyage-3-large", TITLE))
                    .build())
            .field(
                "category",
                FieldDefinitionBuilder.builder()
                    .string(StringFieldDefinitionBuilder.builder().build())
                    .build())
            .field("profile", FieldDefinitionBuilder.builder().document(profile).build())
            .build();
    return SearchIndexDefinitionBuilder.builder().defaultMetadata().mappings(mappings).build();
  }

  private static SearchIndexDefinition embeddedDocumentsPassthroughSearchIndex() {
    DocumentFieldDefinition profile =
        DocumentFieldDefinitionBuilder.builder()
            .dynamic(false)
            .field(
                "name",
                FieldDefinitionBuilder.builder()
                    .string(StringFieldDefinitionBuilder.builder().build())
                    .build())
            .build();
    var items =
        EmbeddedDocumentsFieldDefinitionBuilder.builder()
            .dynamic(false)
            .field(
                "label",
                FieldDefinitionBuilder.builder()
                    .string(StringFieldDefinitionBuilder.builder().build())
                    .build())
            .field("profile", FieldDefinitionBuilder.builder().document(profile).build())
            .build();
    DocumentFieldDefinition mappings =
        DocumentFieldDefinitionBuilder.builder()
            .dynamic(false)
            .field(
                "titleVector",
                FieldDefinitionBuilder.builder()
                    .searchAutoEmbed(new SearchAutoEmbedFieldDefinition("voyage-3-large", TITLE))
                    .build())
            .field("items", FieldDefinitionBuilder.builder().embeddedDocuments(items).build())
            .build();
    return SearchIndexDefinitionBuilder.builder().defaultMetadata().mappings(mappings).build();
  }

  private static SearchIndexDefinition arrayEmbedSearchIndex() {
    DocumentFieldDefinition mappings =
        DocumentFieldDefinitionBuilder.builder()
            .dynamic(false)
            .field(
                "itemTitleVector",
                FieldDefinitionBuilder.builder()
                    .searchAutoEmbed(
                        new SearchAutoEmbedFieldDefinition(
                            "voyage-3-large", FieldPath.parse("items.title")))
                    .build())
            .field(
                "category",
                FieldDefinitionBuilder.builder()
                    .string(StringFieldDefinitionBuilder.builder().build())
                    .build())
            .build();
    return SearchIndexDefinitionBuilder.builder().defaultMetadata().mappings(mappings).build();
  }
}
