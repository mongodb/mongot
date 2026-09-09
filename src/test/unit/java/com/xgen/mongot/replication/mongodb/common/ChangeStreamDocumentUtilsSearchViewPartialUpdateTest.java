package com.xgen.mongot.replication.mongodb.common;

import static com.google.common.truth.Truth.assertThat;
import static com.xgen.mongot.index.IndexTypeData.INDEX_TYPE_TAG_NAME;
import static com.xgen.mongot.index.IndexTypeData.IndexTypeTag.TAG_SEARCH_AUTO_EMBEDDING;

import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.OperationType;
import com.mongodb.client.model.changestream.TruncatedArray;
import com.mongodb.client.model.changestream.UpdateDescription;
import com.xgen.mongot.embedding.utils.PartialEmbedFallbackReason;
import com.xgen.mongot.index.DocumentEvent;
import com.xgen.mongot.index.definition.BooleanFieldDefinition;
import com.xgen.mongot.index.definition.DocumentFieldDefinition;
import com.xgen.mongot.index.definition.SearchAutoEmbedFieldDefinition;
import com.xgen.mongot.index.definition.SearchIndexDefinition;
import com.xgen.mongot.index.definition.TokenFieldDefinition;
import com.xgen.mongot.index.definition.ViewDefinition;
import com.xgen.mongot.index.version.IndexFormatVersion;
import com.xgen.mongot.metrics.MetricsFactory;
import com.xgen.mongot.replication.mongodb.common.ViewFieldDependencies.ViewFullProcessingReason;
import com.xgen.mongot.util.FieldPath;
import com.xgen.testing.mongot.index.definition.DocumentFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.FieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.SearchIndexDefinitionBuilder;
import com.xgen.testing.mongot.replication.mongodb.ChangeStreamUtils;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.List;
import java.util.Optional;
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

@RunWith(JUnit4.class)
public class ChangeStreamDocumentUtilsSearchViewPartialUpdateTest {

  private static final String MODEL = "voyage-3-large";

  @Test
  public void staticView_singleEmbedFeederChange_reEmbedsOnlyAffectedField() {
    SearchIndexDefinition indexDefinition = staticSearchView(basePipeline());
    UpdateDescription update =
        new UpdateDescription(null, new BsonDocument("b1", new BsonString("new b1")));

    var batch =
        handle(
            indexDefinition,
            update,
            viewDocument(indexDefinition, "a1 a2", "new b1 b2", "books", true));

    DocumentEvent event = batch.finalChangeEvents.getFirst();
    assertThat(event.getFilterFieldUpdates()).isPresent();
    assertThat(event.getChangedEmbedTexts())
        .containsExactly(FieldPath.parse("viewTextB"), "new b1 b2");
    assertThat(event.getFilterFieldUpdates().orElseThrow().getString("category").getValue())
        .isEqualTo("books");
    assertThat(batch.partialEmbedApplied).isEqualTo(1);
    assertThat(batch.partialEmbedFallbacks).isEmpty();
  }

  @Test
  public void staticView_passthroughOnlyChange_keepsExistingVectorsUntouched() {
    SearchIndexDefinition indexDefinition = staticSearchView(basePipeline());
    UpdateDescription update =
        new UpdateDescription(null, new BsonDocument("category", new BsonString("archived")));

    var batch =
        handle(
            indexDefinition,
            update,
            viewDocument(indexDefinition, "a1 a2", "b1 b2", "archived", true));

    DocumentEvent event = batch.finalChangeEvents.getFirst();
    assertThat(event.getChangedEmbedTexts()).isEmpty();
    assertThat(event.getFilterFieldUpdates()).isPresent();
    assertThat(event.getFilterFieldUpdates().orElseThrow().getString("category").getValue())
        .isEqualTo("archived");
    assertThat(batch.viewFilterOnlyPartialApplied).isEqualTo(1);
    assertThat(batch.viewDecisions).containsExactly(ViewFullProcessingReason.NONE, 1);
  }

  @Test
  public void staticView_nestedComputedEmbedFeederChange_reEmbedsOnlyNestedSource() {
    SearchIndexDefinition indexDefinition =
        staticSearchViewWithNestedComputedEmbed(
            List.of(BsonDocument.parse("{\"$set\": {\"items.text\": \"$seed\"}}")));
    UpdateDescription update =
        new UpdateDescription(null, new BsonDocument("seed", new BsonString("updated")));
    BsonDocument fullDocument =
        nestedComputedEmbedViewDocument(indexDefinition, "updated", "books");

    var batch = handle(indexDefinition, update, fullDocument);

    DocumentEvent event = batch.finalChangeEvents.getFirst();
    assertThat(event.getChangedEmbedTexts())
        .containsExactly(FieldPath.parse("items.text"), "updated");
    assertThat(event.getFilterFieldUpdates()).isPresent();
    assertThat(batch.partialEmbedApplied).isEqualTo(1);
  }

  @Test
  public void staticView_computedNestedEmbedDestinationContainerOverwrite_usesFullReplacement() {
    SearchIndexDefinition indexDefinition =
        staticSearchViewWithNestedComputedEmbed(
            List.of(BsonDocument.parse("{\"$set\": {\"items.text\": \"$seed\"}}")));
    UpdateDescription update =
        new UpdateDescription(null, new BsonDocument("items", new BsonArray()));
    BsonDocument fullDocument =
        new BsonDocument()
            .append("_id", new BsonInt32(0))
            .append("items", new BsonArray())
            .append("category", new BsonString("books"))
            .append(
                indexDefinition.getIndexId().toString(), new BsonDocument("_id", new BsonInt32(0)));

    var batch = handle(indexDefinition, update, fullDocument);

    assertFullReplacement(batch);
    assertThat(batch.partialEmbedFallbacks).isEmpty();
    assertThat(batch.viewDecisions)
        .containsExactly(ViewFullProcessingReason.COMPUTED_EMBED_CONTAINER_OVERWRITE, 1);
  }

  @Test
  public void
      staticView_collapsedNestedComputedEmbedContainerPredecessor_forcesSurvivingUpdateFull() {
    SearchIndexDefinition indexDefinition =
        staticSearchViewWithNestedComputedEmbed(
            List.of(BsonDocument.parse("{\"$set\": {\"items.text\": \"$seed\"}}")));
    var batch =
        handle(
            indexDefinition,
            List.of(
                event(
                    new UpdateDescription(null, new BsonDocument("items", new BsonArray())),
                    nestedComputedEmbedViewDocument(indexDefinition, null, "books")),
                event(
                    new UpdateDescription(
                        null, new BsonDocument("category", new BsonString("archived"))),
                    nestedComputedEmbedViewDocument(indexDefinition, null, "archived"))));

    assertFullReplacement(batch);
    assertThat(batch.partialEmbedFallbacks)
        .containsExactly(PartialEmbedFallbackReason.INTRA_BATCH_PREDECESSOR_REQUIRES_FULL, 1);
    assertThat(batch.viewDecisions)
        .containsExactly(ViewFullProcessingReason.INTRA_BATCH_PREDECESSOR_REQUIRES_FULL, 1);
  }

  @Test
  public void
      staticView_collapsedNestedComputedEmbedContainerPredecessor_forcesFeederSurvivorFull() {
    SearchIndexDefinition indexDefinition =
        staticSearchViewWithNestedComputedEmbed(
            List.of(BsonDocument.parse("{\"$set\": {\"items.text\": \"$seed\"}}")));
    var batch =
        handle(
            indexDefinition,
            List.of(
                event(
                    new UpdateDescription(null, new BsonDocument("items", new BsonArray())),
                    nestedComputedEmbedViewDocument(indexDefinition, null, "books")),
                event(
                    new UpdateDescription(
                        null, new BsonDocument("seed", new BsonString("updated"))),
                    nestedComputedEmbedViewDocument(indexDefinition, null, "books"))));

    assertFullReplacement(batch);
    assertThat(batch.partialEmbedFallbacks)
        .containsExactly(PartialEmbedFallbackReason.INTRA_BATCH_PREDECESSOR_REQUIRES_FULL, 1);
  }

  @Test
  public void staticView_collapsedFullPredecessor_forcesSurvivingUpdateFull() {
    SearchIndexDefinition indexDefinition = staticSearchView(basePipeline());
    var batch =
        ChangeStreamDocumentUtils.handleDocumentEvents(
            List.of(
                event(
                    new UpdateDescription(
                        null, new BsonDocument("published", new BsonBoolean(false))),
                    viewDocument(indexDefinition, "a1 a2", "b1 b2", "books", false)),
                event(
                    new UpdateDescription(
                        null, new BsonDocument("category", new BsonString("archived"))),
                    viewDocument(indexDefinition, "a1 a2", "b1 b2", "archived", true))),
            indexDefinition,
            indexDefinition.createFieldDefinitionResolver(IndexFormatVersion.CURRENT),
            true,
            ChangeStreamDocumentUtils.deriveViewFieldDependencies(indexDefinition));

    assertFullReplacement(batch);
    assertThat(batch.partialEmbedFallbacks)
        .containsExactly(PartialEmbedFallbackReason.INTRA_BATCH_PREDECESSOR_REQUIRES_FULL, 1);
  }

  @Test
  public void staticView_collapsedRemovedPredecessor_forcesSurvivingUpdateFull() {
    SearchIndexDefinition indexDefinition = staticSearchView(basePipeline());
    ChangeStreamDocument<RawBsonDocument> removedPredecessor =
        event(
            new UpdateDescription(List.of("category"), new BsonDocument()),
            viewDocument(indexDefinition, "a1 a2", "b1 b2", null, true));
    ChangeStreamDocument<RawBsonDocument> partialEligibleSurvivor =
        event(
            new UpdateDescription(null, new BsonDocument("category", new BsonString("archived"))),
            viewDocument(indexDefinition, "a1 a2", "b1 b2", "archived", true));

    var batch = handle(indexDefinition, List.of(removedPredecessor, partialEligibleSurvivor));

    assertFullReplacement(batch);
    assertThat(batch.partialEmbedFallbacks)
        .containsExactly(PartialEmbedFallbackReason.INTRA_BATCH_PREDECESSOR_REQUIRES_FULL, 1);
  }

  @Test
  public void staticView_collapsedTruncatedPredecessor_forcesSurvivingUpdateFull() {
    SearchIndexDefinition indexDefinition = staticSearchView(basePipeline());
    ChangeStreamDocument<RawBsonDocument> truncatedPredecessor =
        event(
            new UpdateDescription(
                List.of(), new BsonDocument(), List.of(new TruncatedArray("b1", 1))),
            viewDocument(indexDefinition, "a1 a2", "b1", "books", true));
    ChangeStreamDocument<RawBsonDocument> partialEligibleSurvivor =
        event(
            new UpdateDescription(null, new BsonDocument("category", new BsonString("archived"))),
            viewDocument(indexDefinition, "a1 a2", "b1", "archived", true));

    var batch = handle(indexDefinition, List.of(truncatedPredecessor, partialEligibleSurvivor));

    assertFullReplacement(batch);
    assertThat(batch.partialEmbedFallbacks)
        .containsExactly(PartialEmbedFallbackReason.INTRA_BATCH_PREDECESSOR_REQUIRES_FULL, 1);
  }

  @Test
  public void staticView_membershipChange_usesFullReplacement() {
    SearchIndexDefinition indexDefinition = staticSearchView(basePipeline());
    UpdateDescription update =
        new UpdateDescription(null, new BsonDocument("published", new BsonBoolean(false)));

    var batch =
        handle(
            indexDefinition,
            update,
            viewDocument(indexDefinition, "a1 a2", "b1 b2", "books", false));

    assertFullReplacement(batch);
    assertThat(batch.viewDecisions).containsExactly(ViewFullProcessingReason.TRIGGER_HIT, 1);
  }

  @Test
  public void staticView_computedPassthroughFeederChange_usesFullReplacement() {
    SearchIndexDefinition indexDefinition =
        staticSearchView(
            List.of(
                BsonDocument.parse(
                    """
                    {"$set": {
                      "viewTextA": {"$concat": ["$a1", " ", "$a2"]},
                      "viewTextB": {"$concat": ["$b1", " ", "$b2"]},
                      "category": "$categorySource"
                    }}
                    """)));
    UpdateDescription update =
        new UpdateDescription(null, new BsonDocument("categorySource", new BsonString("archived")));

    var batch =
        handle(
            indexDefinition,
            update,
            viewDocument(indexDefinition, "a1 a2", "b1 b2", "archived", true));

    assertFullReplacement(batch);
    assertThat(batch.viewDecisions).containsExactly(ViewFullProcessingReason.TRIGGER_HIT, 1);
  }

  @Test
  public void staticView_removedOrTruncatedField_usesFullReplacement() {
    SearchIndexDefinition indexDefinition = staticSearchView(basePipeline());
    UpdateDescription removed = new UpdateDescription(List.of("category"), new BsonDocument());
    UpdateDescription truncated =
        new UpdateDescription(List.of(), new BsonDocument(), List.of(new TruncatedArray("b1", 1)));

    var removedBatch =
        handle(
            indexDefinition, removed, viewDocument(indexDefinition, "a1 a2", "b1 b2", null, true));
    var truncatedBatch =
        handle(
            indexDefinition,
            truncated,
            viewDocument(indexDefinition, "a1 a2", "b1 b2", "books", true));

    assertFullReplacement(removedBatch);
    assertFullReplacement(truncatedBatch);
    assertThat(removedBatch.viewDecisions)
        .containsExactly(ViewFullProcessingReason.REMOVED_OR_TRUNCATED_FIELDS, 1);
    assertThat(truncatedBatch.viewDecisions)
        .containsExactly(ViewFullProcessingReason.REMOVED_OR_TRUNCATED_FIELDS, 1);
  }

  @Test
  public void staticView_replaceEvent_recordsFallback() {
    SearchIndexDefinition indexDefinition = staticSearchView(basePipeline());

    var batch =
        handle(
            indexDefinition,
            List.of(
                event(
                    OperationType.REPLACE,
                    null,
                    viewDocument(indexDefinition, "a1 a2", "b1 b2", "books", true))));

    assertFullReplacement(batch);
    assertThat(batch.partialEmbedFallbacks)
        .containsExactly(PartialEmbedFallbackReason.REPLACE_EVENT, 1);
    assertThat(batch.viewDecisions).isEmpty();
  }

  @Test
  public void staticView_withUnknownDependencies_replaceEventDoesNotRecordPartialFallback() {
    SearchIndexDefinition indexDefinition =
        staticSearchView(
            List.of(BsonDocument.parse("{\"$project\": {\"viewTextA\": 1, \"category\": 1}}")));

    var batch =
        handle(
            indexDefinition,
            List.of(
                event(
                    OperationType.REPLACE,
                    null,
                    viewDocument(indexDefinition, "a1 a2", "b1 b2", "books", true))));

    assertFullReplacement(batch);
    assertThat(batch.partialEmbedFallbacks).isEmpty();
    assertThat(batch.viewDecisions).isEmpty();
  }

  @Test
  public void staticView_withoutDerivedDependencies_replaceEventDoesNotRecordPartialFallback() {
    SearchIndexDefinition indexDefinition = staticSearchView(basePipeline());

    var batch =
        ChangeStreamDocumentUtils.handleDocumentEvents(
            List.of(
                event(
                    OperationType.REPLACE,
                    null,
                    viewDocument(indexDefinition, "a1 a2", "b1 b2", "books", true))),
            indexDefinition,
            indexDefinition.createFieldDefinitionResolver(IndexFormatVersion.CURRENT),
            true,
            Optional.empty());

    assertFullReplacement(batch);
    assertThat(batch.partialEmbedFallbacks).isEmpty();
    assertThat(batch.viewDecisions).isEmpty();
  }

  @Test
  public void staticView_forcedFullControlKnob_replaceEventDoesNotRecordPartialFallback() {
    SearchIndexDefinition indexDefinition = staticSearchView(basePipeline());
    AutoEmbeddingMaterializedViewConfig config = AutoEmbeddingMaterializedViewConfig.getDefault();
    config.setControlKnobEvaluator(ignored -> true);

    var batch =
        ChangeStreamDocumentUtils.handleDocumentEvents(
            List.of(
                event(
                    OperationType.REPLACE,
                    null,
                    viewDocument(indexDefinition, "a1 a2", "b1 b2", "books", true))),
            indexDefinition,
            indexDefinition.createFieldDefinitionResolver(IndexFormatVersion.CURRENT),
            true,
            ChangeStreamDocumentUtils.deriveViewFieldDependencies(indexDefinition),
            Optional.of(config));

    assertFullReplacement(batch);
    assertThat(batch.partialEmbedFallbacks).isEmpty();
    assertThat(batch.viewDecisions).isEmpty();
  }

  @Test
  public void staticView_withoutDerivedDependencies_reportsUnavailableDecision() {
    SearchIndexDefinition indexDefinition = staticSearchView(basePipeline());
    UpdateDescription update =
        new UpdateDescription(null, new BsonDocument("b1", new BsonString("new b1")));

    var batch =
        ChangeStreamDocumentUtils.handleDocumentEvents(
            List.of(
                event(update, viewDocument(indexDefinition, "a1 a2", "new b1 b2", "books", true))),
            indexDefinition,
            indexDefinition.createFieldDefinitionResolver(IndexFormatVersion.CURRENT),
            true,
            Optional.empty());

    assertFullReplacement(batch);
    assertThat(batch.viewDecisions)
        .containsExactly(ViewFullProcessingReason.VIEW_DEPENDENCIES_UNAVAILABLE, 1);
  }

  @Test
  public void staticView_embedExtractionFailureOrEmptyText_usesFullReplacement() {
    SearchIndexDefinition indexDefinition = staticSearchView(basePipeline());
    UpdateDescription update =
        new UpdateDescription(null, new BsonDocument("b1", new BsonString("changed")));

    BsonDocument nonString = viewDocument(indexDefinition, "a1 a2", "ignored", "books", true);
    nonString.put("viewTextB", new BsonInt32(7));
    var nonStringBatch = handle(indexDefinition, update, nonString);
    var emptyBatch =
        handle(indexDefinition, update, viewDocument(indexDefinition, "a1 a2", "", "books", true));

    assertFullReplacement(nonStringBatch);
    assertFullReplacement(emptyBatch);
    assertThat(nonStringBatch.partialEmbedFallbacks)
        .containsExactly(PartialEmbedFallbackReason.EMPTY_NEW_VALUE, 1);
    assertThat(emptyBatch.partialEmbedFallbacks)
        .containsExactly(PartialEmbedFallbackReason.EMPTY_NEW_VALUE, 1);
    assertThat(nonStringBatch.viewDecisions)
        .containsExactly(ViewFullProcessingReason.PARTIAL_UPDATE_FALLBACK, 1);
    assertThat(emptyBatch.viewDecisions)
        .containsExactly(ViewFullProcessingReason.PARTIAL_UPDATE_FALLBACK, 1);

    Tags tags = Tags.of(INDEX_TYPE_TAG_NAME, TAG_SEARCH_AUTO_EMBEDDING.tagValue);
    MetricsFactory metricsFactory = new MetricsFactory("test", new SimpleMeterRegistry());
    ViewFieldDependenciesMetricCounters viewCounters =
        new ViewFieldDependenciesMetricCounters(metricsFactory, tags);
    viewCounters.recordBatch(nonStringBatch);
    viewCounters.recordBatch(emptyBatch);

    assertThat(
            metricsFactory
                .get(
                    "viewFieldDependenciesDecision",
                    tags.and("result", "full").and("reason", "partial_update_fallback"))
                .counter()
                .count())
        .isEqualTo(2.0);
    assertThat(
            metricsFactory
                .get(
                    "viewFieldDependenciesDecision",
                    tags.and("result", "partial").and("reason", "none"))
                .counter()
                .count())
        .isEqualTo(0.0);
  }

  @Test
  public void staticView_emptyPartialBody_usesFullReplacementWithoutFallbackError() {
    DocumentFieldDefinition mappings =
        DocumentFieldDefinitionBuilder.builder()
            .dynamic(false)
            .field(
                "embeddingA",
                FieldDefinitionBuilder.builder()
                    .searchAutoEmbed(
                        new SearchAutoEmbedFieldDefinition(MODEL, FieldPath.parse("viewTextA")))
                    .build())
            .build();
    SearchIndexDefinition indexDefinition = searchView(mappings, basePipeline());
    UpdateDescription update =
        new UpdateDescription(null, new BsonDocument("unrelated", new BsonString("changed")));

    var batch =
        handle(
            indexDefinition,
            update,
            viewDocument(indexDefinition, "a1 a2", "b1 b2", "books", true));

    assertFullReplacement(batch);
    assertThat(batch.partialEmbedFallbacks).isEmpty();
    assertThat(batch.viewDecisions)
        .containsExactly(ViewFullProcessingReason.PARTIAL_UPDATE_FALLBACK, 1);
  }

  @Test
  public void staticView_withoutChangedPaths_reportsMergedFullProcessingReason() {
    SearchIndexDefinition indexDefinition = staticSearchView(basePipeline());
    BsonDocument fullDocument = viewDocument(indexDefinition, "a1 a2", "b1 b2", "books", true);

    var absentDescriptionBatch = handle(indexDefinition, null, fullDocument);
    var emptyDescriptionBatch =
        handle(indexDefinition, new UpdateDescription(null, null), fullDocument);

    assertFullReplacement(absentDescriptionBatch);
    assertFullReplacement(emptyDescriptionBatch);
    assertThat(absentDescriptionBatch.viewDecisions)
        .containsExactly(ViewFullProcessingReason.NO_CHANGED_PATHS, 1);
    assertThat(emptyDescriptionBatch.viewDecisions)
        .containsExactly(ViewFullProcessingReason.NO_CHANGED_PATHS, 1);
  }

  @Test
  public void staticView_unknownDependencies_usesFullReplacementAndReportsDecision() {
    SearchIndexDefinition indexDefinition =
        staticSearchView(
            List.of(BsonDocument.parse("{\"$project\": {\"viewTextA\": 1, \"category\": 1}}")));
    UpdateDescription update =
        new UpdateDescription(null, new BsonDocument("category", new BsonString("archived")));

    var batch =
        ChangeStreamDocumentUtils.handleDocumentEvents(
            List.of(
                event(update, viewDocument(indexDefinition, "a1 a2", "b1 b2", "archived", true))),
            indexDefinition,
            indexDefinition.createFieldDefinitionResolver(IndexFormatVersion.CURRENT),
            true,
            ChangeStreamDocumentUtils.deriveViewFieldDependencies(indexDefinition),
            Optional.empty(),
            true);

    assertFullReplacement(batch);
    assertThat(batch.viewDecisions)
        .containsExactly(ViewFullProcessingReason.DEPENDS_ON_EVERYTHING, 1);
    assertThat(batch.viewDerivationFallbackReason).isPresent();
  }

  @Test
  public void staticView_destructivePassthroughAncestorOverwrite_usesFullReplacement() {
    SearchIndexDefinition indexDefinition = staticSearchViewWithNestedPassthrough(basePipeline());
    UpdateDescription update =
        new UpdateDescription(null, new BsonDocument("meta", new BsonNull()));
    BsonDocument fullDocument = viewDocument(indexDefinition, "a1 a2", "b1 b2", null, true);
    fullDocument.put("meta", new BsonNull());

    var batch = handle(indexDefinition, update, fullDocument);

    assertFullReplacement(batch);
    assertThat(batch.partialEmbedFallbacks)
        .containsExactly(PartialEmbedFallbackReason.ANCESTOR_OVERWRITE, 1);
    assertThat(batch.viewDecisions)
        .containsExactly(ViewFullProcessingReason.PARTIAL_UPDATE_FALLBACK, 1);
  }

  @Test
  public void dynamicViewMapping_usesFullReplacement() {
    SearchIndexDefinition indexDefinition = dynamicSearchView(basePipeline());
    UpdateDescription update =
        new UpdateDescription(null, new BsonDocument("b1", new BsonString("new b1")));

    var batch =
        handle(
            indexDefinition,
            update,
            viewDocument(indexDefinition, "a1 a2", "new b1 b2", "books", true));

    assertFullReplacement(batch);
  }

  @Test
  public void staticView_forcedFullControlKnob_usesFullReplacement() {
    SearchIndexDefinition indexDefinition = staticSearchView(basePipeline());
    UpdateDescription update =
        new UpdateDescription(null, new BsonDocument("b1", new BsonString("new b1")));
    AutoEmbeddingMaterializedViewConfig config = AutoEmbeddingMaterializedViewConfig.getDefault();
    config.setControlKnobEvaluator(ignored -> true);

    var batch =
        ChangeStreamDocumentUtils.handleDocumentEvents(
            List.of(
                event(update, viewDocument(indexDefinition, "a1 a2", "new b1 b2", "books", true))),
            indexDefinition,
            indexDefinition.createFieldDefinitionResolver(IndexFormatVersion.CURRENT),
            true,
            ChangeStreamDocumentUtils.deriveViewFieldDependencies(indexDefinition),
            Optional.of(config));

    assertFullReplacement(batch);
    assertThat(batch.partialEmbedFallbacks)
        .containsExactly(PartialEmbedFallbackReason.FEATURE_FLAG_FORCED_FULL_UPDATE, 1);
    assertThat(batch.viewDecisions)
        .containsExactly(ViewFullProcessingReason.FEATURE_FLAG_FORCED_FULL_UPDATE, 1);
  }

  @Test
  public void searchViewMetrics_reuseExistingNamesWithBoundedIndexTypeTag() {
    SearchIndexDefinition indexDefinition = staticSearchView(basePipeline());
    var partialBatch =
        handle(
            indexDefinition,
            new UpdateDescription(null, new BsonDocument("b1", new BsonString("new b1"))),
            viewDocument(indexDefinition, "a1 a2", "new b1 b2", "books", true));
    Tags tags = Tags.of(INDEX_TYPE_TAG_NAME, TAG_SEARCH_AUTO_EMBEDDING.tagValue);
    MetricsFactory metricsFactory = new MetricsFactory("test", new SimpleMeterRegistry());
    PartialEmbedMetricCounters partialCounters =
        new PartialEmbedMetricCounters(metricsFactory, tags);
    ViewFieldDependenciesMetricCounters viewCounters =
        new ViewFieldDependenciesMetricCounters(metricsFactory, tags);

    partialCounters.recordBatch(partialBatch);
    viewCounters.recordBatch(partialBatch);

    assertThat(metricsFactory.get("partialEmbedApplied", tags).counter().count()).isEqualTo(1.0);
    assertThat(
            metricsFactory
                .get(
                    "viewFieldDependenciesDecision",
                    tags.and("result", "partial").and("reason", "none"))
                .counter()
                .count())
        .isEqualTo(1.0);
  }

  private static ChangeStreamDocumentUtils.DocumentEventBatch handle(
      SearchIndexDefinition indexDefinition, UpdateDescription update, BsonDocument fullDocument) {
    return handle(indexDefinition, List.of(event(update, fullDocument)));
  }

  private static ChangeStreamDocumentUtils.DocumentEventBatch handle(
      SearchIndexDefinition indexDefinition, List<ChangeStreamDocument<RawBsonDocument>> events) {
    return ChangeStreamDocumentUtils.handleDocumentEvents(
        events,
        indexDefinition,
        indexDefinition.createFieldDefinitionResolver(IndexFormatVersion.CURRENT),
        true,
        ChangeStreamDocumentUtils.deriveViewFieldDependencies(indexDefinition));
  }

  private static ChangeStreamDocument<RawBsonDocument> event(
      UpdateDescription update, BsonDocument fullDocument) {
    return event(OperationType.UPDATE, update, fullDocument);
  }

  private static ChangeStreamDocument<RawBsonDocument> event(
      OperationType operation, UpdateDescription update, BsonDocument fullDocument) {
    return ChangeStreamUtils.dataEvent(operation, 0, update, fullDocument);
  }

  private static void assertFullReplacement(ChangeStreamDocumentUtils.DocumentEventBatch batch) {
    assertThat(batch.finalChangeEvents).hasSize(1);
    DocumentEvent event = batch.finalChangeEvents.getFirst();
    assertThat(event.getFilterFieldUpdates()).isEmpty();
    assertThat(event.getChangedEmbedTexts()).isEmpty();
  }

  private static SearchIndexDefinition staticSearchView(List<BsonDocument> pipeline) {
    return searchView(staticMappings(), pipeline);
  }

  private static SearchIndexDefinition staticSearchViewWithNestedComputedEmbed(
      List<BsonDocument> pipeline) {
    DocumentFieldDefinition mappings =
        DocumentFieldDefinitionBuilder.builder()
            .dynamic(false)
            .field(
                "embedding",
                FieldDefinitionBuilder.builder()
                    .searchAutoEmbed(
                        new SearchAutoEmbedFieldDefinition(MODEL, FieldPath.parse("items.text")))
                    .build())
            .field(
                "category",
                FieldDefinitionBuilder.builder()
                    .token(new TokenFieldDefinition(Optional.empty()))
                    .build())
            .build();
    return searchView(mappings, pipeline);
  }

  private static SearchIndexDefinition dynamicSearchView(List<BsonDocument> pipeline) {
    DocumentFieldDefinition mappings =
        DocumentFieldDefinitionBuilder.builder()
            .dynamic(true)
            .field(
                "embeddingA",
                FieldDefinitionBuilder.builder()
                    .searchAutoEmbed(
                        new SearchAutoEmbedFieldDefinition(MODEL, FieldPath.parse("viewTextA")))
                    .build())
            .field(
                "embeddingB",
                FieldDefinitionBuilder.builder()
                    .searchAutoEmbed(
                        new SearchAutoEmbedFieldDefinition(MODEL, FieldPath.parse("viewTextB")))
                    .build())
            .build();
    return searchView(mappings, pipeline);
  }

  private static SearchIndexDefinition staticSearchViewWithNestedPassthrough(
      List<BsonDocument> pipeline) {
    DocumentFieldDefinition meta =
        DocumentFieldDefinitionBuilder.builder()
            .dynamic(false)
            .field(
                "category",
                FieldDefinitionBuilder.builder()
                    .token(new TokenFieldDefinition(Optional.empty()))
                    .build())
            .build();
    DocumentFieldDefinition mappings =
        DocumentFieldDefinitionBuilder.builder()
            .dynamic(false)
            .field(
                "embeddingA",
                FieldDefinitionBuilder.builder()
                    .searchAutoEmbed(
                        new SearchAutoEmbedFieldDefinition(MODEL, FieldPath.parse("viewTextA")))
                    .build())
            .field(
                "embeddingB",
                FieldDefinitionBuilder.builder()
                    .searchAutoEmbed(
                        new SearchAutoEmbedFieldDefinition(MODEL, FieldPath.parse("viewTextB")))
                    .build())
            .field("meta", FieldDefinitionBuilder.builder().document(meta).build())
            .field(
                "published",
                FieldDefinitionBuilder.builder().bool(new BooleanFieldDefinition()).build())
            .build();
    return searchView(mappings, pipeline);
  }

  private static SearchIndexDefinition searchView(
      DocumentFieldDefinition mappings, List<BsonDocument> pipeline) {
    return SearchIndexDefinitionBuilder.builder()
        .defaultMetadata()
        .mappings(mappings)
        .view(ViewDefinition.existing("myView", pipeline))
        .build();
  }

  private static DocumentFieldDefinition staticMappings() {
    return DocumentFieldDefinitionBuilder.builder()
        .dynamic(false)
        .field(
            "embeddingA",
            FieldDefinitionBuilder.builder()
                .searchAutoEmbed(
                    new SearchAutoEmbedFieldDefinition(MODEL, FieldPath.parse("viewTextA")))
                .build())
        .field(
            "embeddingB",
            FieldDefinitionBuilder.builder()
                .searchAutoEmbed(
                    new SearchAutoEmbedFieldDefinition(MODEL, FieldPath.parse("viewTextB")))
                .build())
        .field(
            "category",
            FieldDefinitionBuilder.builder()
                .token(new TokenFieldDefinition(Optional.empty()))
                .build())
        .field(
            "published",
            FieldDefinitionBuilder.builder().bool(new BooleanFieldDefinition()).build())
        .build();
  }

  private static List<BsonDocument> basePipeline() {
    return List.of(
        BsonDocument.parse(
            """
            {"$set": {
              "viewTextA": {"$concat": ["$a1", " ", "$a2"]},
              "viewTextB": {"$concat": ["$b1", " ", "$b2"]}
            }}
            """),
        BsonDocument.parse("{\"$match\": {\"$expr\": {\"$eq\": [\"$published\", true]}}}"));
  }

  private static BsonDocument viewDocument(
      SearchIndexDefinition indexDefinition,
      String viewTextA,
      String viewTextB,
      String category,
      boolean published) {
    BsonDocument document =
        new BsonDocument()
            .append("_id", new BsonInt32(0))
            .append("viewTextA", new BsonString(viewTextA))
            .append("viewTextB", new BsonString(viewTextB))
            .append("published", new BsonBoolean(published))
            .append(
                indexDefinition.getIndexId().toString(), new BsonDocument("_id", new BsonInt32(0)));
    if (category != null) {
      document.append("category", new BsonString(category));
    }
    return document;
  }

  private static BsonDocument nestedComputedEmbedViewDocument(
      SearchIndexDefinition indexDefinition, String itemText, String category) {
    BsonDocument document =
        new BsonDocument()
            .append("_id", new BsonInt32(0))
            .append("category", new BsonString(category))
            .append(
                indexDefinition.getIndexId().toString(), new BsonDocument("_id", new BsonInt32(0)));
    if (itemText != null) {
      document.append("items", new BsonDocument("text", new BsonString(itemText)));
    } else {
      document.append("items", new BsonArray());
    }
    return document;
  }
}
