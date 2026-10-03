package com.xgen.mongot.replication.mongodb.initialsync;

import static com.google.common.truth.Truth.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.errorprone.annotations.Var;
import com.ibm.icu.text.RuleBasedCollator;
import com.xgen.mongot.embedding.config.MaterializedViewCollectionMetadata;
import com.xgen.mongot.embedding.config.MaterializedViewCollectionMetadata.MaterializedViewSchemaMetadata;
import com.xgen.mongot.embedding.config.MaterializedViewCollectionMetadataCatalog;
import com.xgen.mongot.featureflag.Feature;
import com.xgen.mongot.featureflag.FeatureFlags;
import com.xgen.mongot.index.DocumentEvent;
import com.xgen.mongot.index.autoembedding.AutoEmbeddingResyncHashJoinClassifier;
import com.xgen.mongot.index.definition.VectorIndexDefinition;
import com.xgen.mongot.index.definition.ViewDefinition;
import com.xgen.mongot.index.version.GenerationId;
import com.xgen.mongot.metrics.MetricsFactory;
import com.xgen.mongot.replication.mongodb.common.CollectionScanMongoClient;
import com.xgen.mongot.replication.mongodb.common.InitialSyncException;
import com.xgen.mongot.util.BsonUtils;
import com.xgen.testing.mongot.index.definition.VectorIndexDefinitionBuilder;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Clock;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.BsonTimestamp;
import org.bson.BsonValue;
import org.bson.RawBsonDocument;
import org.bson.types.ObjectId;
import org.junit.Test;

/**
 * Unit tests for {@link AutoEmbeddingSortedIdCollectionScanner#sortMergeEvents} that target the
 * view-specific metadata-extraction paths. For a view-based auto-embedding index, source docs carry
 * an {@code <indexId>} metadata namespace (added server-side during the scan) while materialized
 * view docs are keyed by {@code _id} at the root with no namespace.
 */
public class AutoEmbeddingSortedIdCollectionScannerTest {

  private static final ObjectId INDEX_ID = new ObjectId("507f1f77bcf86cd799439011");

  private static RawBsonDocument raw(BsonDocument document) {
    return BsonUtils.documentToRaw(document);
  }

  /** A source doc as it appears mid-scan: original _id preserved under the metadata namespace. */
  private static RawBsonDocument sourceDocWithNamespace(int id) {
    return raw(
        new BsonDocument("_id", new BsonInt32(id))
            .append(INDEX_ID.toString(), new BsonDocument("_id", new BsonInt32(id))));
  }

  /** A materialized view doc: view applied, keyed by _id at the root, no metadata namespace. */
  private static RawBsonDocument matViewDoc(int id) {
    return raw(new BsonDocument("_id", new BsonInt32(id)));
  }

  private static AutoEmbeddingSortedIdCollectionScanner newViewIndexScanner() {
    return newScanner(true);
  }

  private static AutoEmbeddingSortedIdCollectionScanner newCollectionIndexScanner() {
    return newScanner(false);
  }

  private static AutoEmbeddingSortedIdCollectionScanner newScanner(boolean onView) {
    return newScanner(
        onView,
        FeatureFlags.getDefault(),
        new MetricsFactory("test", new SimpleMeterRegistry()),
        mock(InitialSyncMongoClient.class));
  }

  private static AutoEmbeddingSortedIdCollectionScanner newScanner(
      boolean onView, MetricsFactory metricsFactory) {
    return newScanner(
        onView, FeatureFlags.getDefault(), metricsFactory, mock(InitialSyncMongoClient.class));
  }

  private static AutoEmbeddingSortedIdCollectionScanner newScanner(
      boolean onView,
      FeatureFlags featureFlags,
      MetricsFactory metricsFactory,
      InitialSyncMongoClient mongoClient) {
    VectorIndexDefinitionBuilder builder =
        VectorIndexDefinitionBuilder.builder()
            .indexId(INDEX_ID)
            .name("idx")
            .database("db")
            .lastObservedCollectionName("coll")
            .withAutoEmbedField("my.auto.embed.field")
            .withFilterPath("my.filter.field");
    if (onView) {
      // The pipeline body is irrelevant: sortMergeEvents only checks getView().isPresent() to pick
      // fromMetadataNamespace (source) vs fromOriginalDocument (mat view).
      builder.view(ViewDefinition.existing("vw", List.of()));
    }
    VectorIndexDefinition indexDefinition = builder.build();

    InitialSyncContext context = mock(InitialSyncContext.class);
    when(context.useNaturalOrderScan()).thenReturn(false);
    when(context.getIndexId()).thenReturn(INDEX_ID);
    when(context.getGenerationId()).thenReturn(mock(GenerationId.class));
    when(context.getIndexDefinition()).thenReturn(indexDefinition);
    when(context.getFeatureFlags()).thenReturn(featureFlags);
    when(context.getChangeStreamResumeOperationTime()).thenReturn(new BsonTimestamp(0));

    MaterializedViewCollectionMetadata metadata =
        new MaterializedViewCollectionMetadata(
            MaterializedViewSchemaMetadata.VERSION_ZERO, UUID.randomUUID(), "mvCollection");
    MaterializedViewCollectionMetadataCatalog catalog =
        mock(MaterializedViewCollectionMetadataCatalog.class);
    when(catalog.getMetadataIfPresent(any())).thenReturn(Optional.of(metadata));
    when(catalog.getDatabaseName(any())).thenReturn("db");

    return new AutoEmbeddingSortedIdCollectionScanner(
        Clock.systemUTC(), context, mongoClient, new BsonInt32(0), catalog, metricsFactory);
  }

  /**
   * Trailing mat-view docs left over after the source stream is exhausted must be deleted. For a
   * view index these docs have no {@code <indexId>} metadata namespace, so the trailing loop must
   * read their _id from the root rather than from the (absent) namespace.
   */
  @Test
  public void sortMergeEventsDeletesLeftoverMatViewDocsForViewIndex() {
    AutoEmbeddingSortedIdCollectionScanner scanner = newViewIndexScanner();

    // Source has doc 3; the mat view still holds a stale doc 7 whose source row was deleted while
    // the index was down. Doc 3 is inserted, then the stale doc 7 is left over after the source
    // stream ends and must be deleted — the trailing delete loop must read its _id from the root,
    // not the (absent) metadata namespace.
    List<DocumentEvent> events =
        scanner.sortMergeEvents(List.of(sourceDocWithNamespace(3)), List.of(matViewDoc(7)));

    assertThat(events).hasSize(2);
    assertThat(events.get(0).getEventType()).isEqualTo(DocumentEvent.EventType.INSERT);
    assertThat(events.get(0).getDocumentId()).isEqualTo(new BsonInt32(3));
    assertThat(events.get(1).getEventType()).isEqualTo(DocumentEvent.EventType.DELETE);
    assertThat(events.get(1).getDocumentId()).isEqualTo(new BsonInt32(7));
  }

  /**
   * A source doc whose metadata namespace is missing (the HELP-60413 duplicate-field case) must be
   * skipped, not surfaced or crash the scan. The well-formed docs around it must still resync.
   */
  @Test
  public void sortMergeEventsSkipsSourceDocMissingMetadataNamespaceForViewIndex() {
    AutoEmbeddingSortedIdCollectionScanner scanner = newViewIndexScanner();

    // The namespace-less doc gets the lowest _id so that a realistic _id-ascending scan order
    // still routes it through the main loop (where this bug lives), not the trailing insert loop.
    RawBsonDocument missingNamespaceSourceDoc = raw(new BsonDocument("_id", new BsonInt32(1)));
    RawBsonDocument wellFormedSourceDoc = sourceDocWithNamespace(10);

    List<DocumentEvent> events =
        scanner.sortMergeEvents(
            List.of(missingNamespaceSourceDoc, wellFormedSourceDoc), List.of(matViewDoc(5)));

    // The namespace-less doc (id 1) is skipped; the leftover mat-view doc (5) is deleted and the
    // well-formed source doc (10) is inserted, in that order.
    assertThat(events).hasSize(2);
    assertThat(events.get(0).getEventType()).isEqualTo(DocumentEvent.EventType.DELETE);
    assertThat(events.get(0).getDocumentId()).isEqualTo(new BsonInt32(5));
    assertThat(events.get(1).getEventType()).isEqualTo(DocumentEvent.EventType.INSERT);
    assertThat(events.get(1).getDocumentId()).isEqualTo(new BsonInt32(10));
  }

  /**
   * The same skip is required in the trailing insert loop: a namespace-less source doc whose _id
   * sorts after the mat-view range falls through to the remaining-baseBatch inserts, which must
   * skip it rather than crash. The main-loop guard alone does not cover this tail path.
   */
  @Test
  public void sortMergeEventsSkipsSourceDocMissingMetadataNamespaceInTrailingInsertForViewIndex() {
    AutoEmbeddingSortedIdCollectionScanner scanner = newViewIndexScanner();

    // Sorted source batch: a well-formed doc (6) then a namespace-less doc (9, HELP-60413). The
    // mat view holds a stale doc (2) that is deleted in the main loop, exhausting the mat-view
    // stream, so both source docs are handled by the trailing insert loop.
    RawBsonDocument wellFormedSourceDoc = sourceDocWithNamespace(6);
    RawBsonDocument missingNamespaceSourceDoc = raw(new BsonDocument("_id", new BsonInt32(9)));

    List<DocumentEvent> events =
        scanner.sortMergeEvents(
            List.of(wellFormedSourceDoc, missingNamespaceSourceDoc), List.of(matViewDoc(2)));

    // Mat-view doc 2 deleted; source doc 6 inserted; the namespace-less doc 9 is skipped.
    assertThat(events).hasSize(2);
    assertThat(events.get(0).getEventType()).isEqualTo(DocumentEvent.EventType.DELETE);
    assertThat(events.get(0).getDocumentId()).isEqualTo(new BsonInt32(2));
    assertThat(events.get(1).getEventType()).isEqualTo(DocumentEvent.EventType.INSERT);
    assertThat(events.get(1).getDocumentId()).isEqualTo(new BsonInt32(6));
  }

  /**
   * Regression guard for collection (non-view) indexes: the trailing-delete switch to
   * fromOriginalDocument and the new trailing-insert guard must be no-ops here, because
   * getDocumentMetadata already reads the root _id for a non-view index.
   */
  @Test
  public void sortMergeEventsHandlesInsertsAndDeletesForCollectionIndex() {
    AutoEmbeddingSortedIdCollectionScanner scanner = newCollectionIndexScanner();

    // Non-view docs are keyed by _id at the root (no namespace). Source has doc 3 (insert); the
    // mat view holds a stale doc 7 (delete), left over after the source stream ends.
    RawBsonDocument sourceDoc = raw(new BsonDocument("_id", new BsonInt32(3)));

    List<DocumentEvent> events =
        scanner.sortMergeEvents(List.of(sourceDoc), List.of(matViewDoc(7)));

    assertThat(events).hasSize(2);
    assertThat(events.get(0).getEventType()).isEqualTo(DocumentEvent.EventType.INSERT);
    assertThat(events.get(0).getDocumentId()).isEqualTo(new BsonInt32(3));
    assertThat(events.get(1).getEventType()).isEqualTo(DocumentEvent.EventType.DELETE);
    assertThat(events.get(1).getDocumentId()).isEqualTo(new BsonInt32(7));
  }

  /**
   * The sort-merge emits per-class divergence counters so the otherwise-silent self-heal is
   * observable: a doc missing from the MV (INSERT) and a leftover MV doc (DELETE) increment the
   * missing and surplus counters respectively, while stale stays zero (nothing is present in both).
   */
  @Test
  public void sortMergeEventsRecordsMissingAndSurplusDivergenceCounts() {
    MetricsFactory metricsFactory = new MetricsFactory("test", new SimpleMeterRegistry());
    AutoEmbeddingSortedIdCollectionScanner scanner = newScanner(true, metricsFactory);

    List<DocumentEvent> events =
        scanner.sortMergeEvents(List.of(sourceDocWithNamespace(3)), List.of(matViewDoc(7)));

    assertThat(events).hasSize(2);
    assertThat(metricsFactory.get("resyncMissingDocuments").counter().count()).isEqualTo(1.0);
    assertThat(metricsFactory.get("resyncSurplusDocuments").counter().count()).isEqualTo(1.0);
    assertThat(metricsFactory.get("resyncStaleDocuments").counter().count()).isEqualTo(0.0);
  }

  /**
   * A namespace-less source doc is skipped by advancing only the source cursor (not the mat-view
   * cursor), so its mat-view entry is left unmatched and deleted — the unreadable doc is dropped
   * from the index.
   */
  @Test
  public void sortMergeEventsDeletesMatViewEntryOfSkippedSourceDocForViewIndex() {
    AutoEmbeddingSortedIdCollectionScanner scanner = newViewIndexScanner();

    // Source doc id 5 lost its metadata namespace (HELP-60413); the mat view still holds its entry.
    RawBsonDocument brokenSourceDoc = raw(new BsonDocument("_id", new BsonInt32(5)));

    List<DocumentEvent> events =
        scanner.sortMergeEvents(List.of(brokenSourceDoc), List.of(matViewDoc(5)));

    // The unreadable doc is dropped and its mat-view entry (id 5) is deleted.
    assertThat(events).hasSize(1);
    assertThat(events.get(0).getEventType()).isEqualTo(DocumentEvent.EventType.DELETE);
    assertThat(events.get(0).getDocumentId()).isEqualTo(new BsonInt32(5));
  }

  /**
   * For a view index the resume id (mat-view query bound and resume token) must come from the
   * metadata namespace (the original pre-view _id), not the root, which the view pipeline may have
   * rewritten. The namespace id wins even when it differs from the root _id.
   */
  @Test
  public void lastScannedIdPrefersMetadataNamespaceForViewIndex() {
    AutoEmbeddingSortedIdCollectionScanner scanner = newViewIndexScanner();

    // The view rewrote the root _id to 999; the namespace preserves the original _id 7. The scan
    // sort/resume and the mat view are keyed by the original _id, so 7 must win.
    RawBsonDocument doc =
        raw(
            new BsonDocument("_id", new BsonInt32(999))
                .append(INDEX_ID.toString(), new BsonDocument("_id", new BsonInt32(7))));

    assertThat(scanner.lastScannedIdOf(doc)).hasValue(new BsonInt32(7));
  }

  /**
   * When a view-index doc is missing its metadata namespace (HELP-60413 duplicate fields), the
   * resume id falls back to the root _id so the scan advances instead of throwing.
   */
  @Test
  public void lastScannedIdFallsBackToRootWhenNamespaceMissingForViewIndex() {
    AutoEmbeddingSortedIdCollectionScanner scanner = newViewIndexScanner();

    RawBsonDocument namespaceLessDoc = raw(new BsonDocument("_id", new BsonInt32(42)));

    assertThat(scanner.lastScannedIdOf(namespaceLessDoc)).hasValue(new BsonInt32(42));
  }

  /** A collection (non-view) index reads the resume id from the root _id, with no namespace. */
  @Test
  public void lastScannedIdReadsRootForCollectionIndex() {
    AutoEmbeddingSortedIdCollectionScanner scanner = newCollectionIndexScanner();

    RawBsonDocument doc = raw(new BsonDocument("_id", new BsonInt32(5)));

    assertThat(scanner.lastScannedIdOf(doc)).hasValue(new BsonInt32(5));
  }

  /**
   * A binary-monotonic id sequence (the common ObjectId/number/simple-collation case) is not a
   * tripwire trigger: {@code findFirstInversion} returns empty.
   */
  @Test
  public void findFirstInversion_binaryMonotonicIds_returnsEmpty() {
    List<BsonValue> ids = List.of(new BsonInt32(1), new BsonInt32(2), new BsonInt32(10));

    assertThat(AutoEmbeddingSortedIdCollectionScanner.findFirstInversion(ids, Optional.empty()))
        .isEmpty();
  }

  /**
   * With no collator in scope ({@code Optional.empty()} = pre-7908 behavior) a collation-ordered
   * batch like {@code a, B} is not binary-monotonic, so the tripwire fires — the original bug
   * signal (CLOUDP-444644). Contrast with the collator-present test below, which is the case that
   * must NOT fire once the merge and tripwire share the same comparator.
   */
  @Test
  public void findFirstInversion_binaryComparatorOrder_detectsInversion() {
    List<BsonValue> ids = List.of(new BsonString("a"), new BsonString("B"));

    Optional<AutoEmbeddingSortedIdCollectionScanner.IdOrderInversion> inversion =
        AutoEmbeddingSortedIdCollectionScanner.findFirstInversion(ids, Optional.empty());

    assertThat(inversion).isPresent();
    assertThat(inversion.get().index()).isEqualTo(1);
    assertThat(inversion.get().previous()).isEqualTo(new BsonString("a"));
    assertThat(inversion.get().current()).isEqualTo(new BsonString("B"));
  }

  /**
   * Under a real ICU collator that matches the server, a collation-ordered batch is monotonic, so
   * the tripwire must NOT fire. This pins the joint property this PR guarantees with the merge
   * (which now shares {@code compareIds(..., stringCollator)}): the tripwire uses the same
   * comparator as the merge, so it no longer false-positives on legitimate collated resyncs (the QA
   * repro). It should only fire on an ICU-parity gap or an unresolvable collation that fell back to
   * binary.
   */
  @Test
  public void findFirstInversion_collationOrderedIdsUnderMatchingCollator_returnsEmpty() {
    Optional<RuleBasedCollator> collator =
        CollationUtils.toIcuCollator(
            new BsonDocument("locale", new BsonString("en")).append("strength", new BsonInt32(2)));
    List<BsonValue> ids =
        List.of(
            new BsonString("a00000"),
            new BsonString("B00001"),
            new BsonString("c00002"),
            new BsonString("D00003"));

    assertThat(AutoEmbeddingSortedIdCollectionScanner.findFirstInversion(ids, collator)).isEmpty();
  }

  /** Empty and single-id batches are trivially monotonic and never trigger. */
  @Test
  public void findFirstInversion_emptyOrSingleId_returnsEmpty() {
    assertThat(
            AutoEmbeddingSortedIdCollectionScanner.findFirstInversion(List.of(), Optional.empty()))
        .isEmpty();
    assertThat(
            AutoEmbeddingSortedIdCollectionScanner.findFirstInversion(
                List.of(new BsonString("x")), Optional.empty()))
        .isEmpty();
  }

  /**
   * The previous batch's last id is prepended so the boundary pair straddling a getMore is checked.
   */
  @Test
  public void withPreviousBatchLastId_prependsBoundaryId() {
    List<BsonValue> current = List.of(new BsonString("b"), new BsonString("c"));

    assertThat(
            AutoEmbeddingSortedIdCollectionScanner.withPreviousBatchLastId(
                Optional.of(new BsonString("a")), current))
        .containsExactly(new BsonString("a"), new BsonString("b"), new BsonString("c"))
        .inOrder();
  }

  /** No previous id (first batch) or an empty batch just returns the batch as-is. */
  @Test
  public void withPreviousBatchLastId_noPreviousOrEmptyBatch_returnsBatch() {
    List<BsonValue> current = List.of(new BsonString("b"));

    assertThat(
            AutoEmbeddingSortedIdCollectionScanner.withPreviousBatchLastId(
                Optional.empty(), current))
        .containsExactly(new BsonString("b"));
    assertThat(
            AutoEmbeddingSortedIdCollectionScanner.withPreviousBatchLastId(
                Optional.of(new BsonString("a")), List.of()))
        .isEmpty();
  }

  // Drives bufferNextBatch through a mocked source cursor + empty MV read, so the wiring from
  // "detected inversion -> runIdOrderTripwire" is pinned. The helper tests above don't cover it,
  // and
  // removing the bufferNextBatch wiring would keep them green. The default simple-collation
  // metadata
  // leaves stringCollator empty (binary), so [a, B] is an inversion.
  private static AutoEmbeddingSortedIdCollectionScanner scannerForBufferNextBatch(
      FeatureFlags featureFlags, MetricsFactory metricsFactory) throws InitialSyncException {
    InitialSyncMongoClient mongoClient = mock(InitialSyncMongoClient.class);
    CollectionScanMongoClient<InitialSyncException> emptyMvCursor =
        mock(CollectionScanMongoClient.class);
    when(emptyMvCursor.hasNext()).thenReturn(false);
    when(mongoClient.getAutoEmbeddingResyncMongoClient(any(), any(), any(), any()))
        .thenReturn(emptyMvCursor);
    return newScanner(false, featureFlags, metricsFactory, mongoClient);
  }

  private static CollectionScanMongoClient<InitialSyncException> sourceBatchCursor(String... ids)
      throws InitialSyncException {
    CollectionScanMongoClient<InitialSyncException> cursor = mock(CollectionScanMongoClient.class);
    when(cursor.hasNext()).thenReturn(false);
    when(cursor.getNext())
        .thenReturn(
            Arrays.stream(ids)
                .map(id -> raw(new BsonDocument("_id", new BsonString(id))))
                .collect(Collectors.toList()));
    return cursor;
  }

  @Test
  public void bufferNextBatch_reportMode_recordsMetricAndContinues() throws InitialSyncException {
    MetricsFactory metricsFactory = new MetricsFactory("test", new SimpleMeterRegistry());
    AutoEmbeddingSortedIdCollectionScanner scanner =
        scannerForBufferNextBatch(FeatureFlags.withDefaults().build(), metricsFactory);

    List<DocumentEvent> events = scanner.bufferNextBatch(sourceBatchCursor("a", "B"));

    // a > B under binary: detected inversion -> REPORT -> metric recorded, no abort. Empty MV ->
    // both
    // source docs are inserts.
    assertThat(events).hasSize(2);
    assertThat(metricsFactory.get("resyncIdOrderInversions").counter().count()).isEqualTo(1.0);
  }

  @Test
  public void bufferNextBatch_failMode_abortsResyncOnInversion() throws InitialSyncException {
    MetricsFactory metricsFactory = new MetricsFactory("test", new SimpleMeterRegistry());
    FeatureFlags flags =
        FeatureFlags.withDefaults().enable(Feature.AE_RESYNC_ID_ORDER_TRIPWIRE_FAIL_FAST).build();
    AutoEmbeddingSortedIdCollectionScanner scanner =
        scannerForBufferNextBatch(flags, metricsFactory);

    @Var InitialSyncException thrown = null;
    try {
      scanner.bufferNextBatch(sourceBatchCursor("a", "B"));
    } catch (InitialSyncException e) {
      thrown = e;
    }

    assertThat(thrown).isNotNull();
    assertThat(thrown.getMessage()).contains("id-order tripwire");
    assertThat(metricsFactory.get("resyncIdOrderInversions").counter().count()).isEqualTo(1.0);
  }

  private static List<BsonValue> matchedIdsOf(
      AutoEmbeddingResyncHashJoinClassifier.Classification classification) {
    return classification.matched().stream()
        .map(AutoEmbeddingResyncHashJoinClassifier.MatchedPair::id)
        .collect(Collectors.toList());
  }

  // CLOUDP-449412: the hash-join shadow, the acting hash-join path, and the mismatch detector.

  /** The event (type, id) pairs of a batch, so flag-on and flag-off batches compare as sets. */
  private static List<String> eventsOf(List<DocumentEvent> events) {
    return events.stream()
        .map(event -> event.getEventType() + ":" + event.getDocumentId())
        .collect(Collectors.toList());
  }

  private static List<BsonValue> idsOf(List<DocumentEvent> events, DocumentEvent.EventType type) {
    return events.stream()
        .filter(event -> event.getEventType().equals(type))
        .map(DocumentEvent::getDocumentId)
        .collect(Collectors.toList());
  }

  private static List<RawBsonDocument> sourceWindow(int... ids) {
    return Arrays.stream(ids)
        .mapToObj(AutoEmbeddingSortedIdCollectionScannerTest::sourceDocWithNamespace)
        .collect(Collectors.toList());
  }

  private static List<RawBsonDocument> matViewWindow(int... ids) {
    return Arrays.stream(ids)
        .mapToObj(AutoEmbeddingSortedIdCollectionScannerTest::matViewDoc)
        .collect(Collectors.toList());
  }

  /**
   * The set-based classification the sort-merge shadow computes must partition the window exactly
   * as the events the sort-merge acts on do: otherwise the mismatch detector compares the hash join
   * against a classification that does not describe the acting path.
   */
  @Test
  public void sortMergeClassification_partitionsTheWindowLikeTheSortMergeEvents() {
    AutoEmbeddingSortedIdCollectionScanner scanner = newViewIndexScanner();

    var source = sourceWindow(1, 2, 3);
    var matView = matViewWindow(2, 3, 4);
    var classification = scanner.sortMergeClassification(source, matView);
    List<DocumentEvent> events = scanner.sortMergeEvents(source, matView);

    assertThat(classification.insertIds()).containsExactly(new BsonInt32(1));
    assertThat(classification.deleteIds()).containsExactly(new BsonInt32(4));
    assertThat(matchedIdsOf(classification)).containsExactly(new BsonInt32(2), new BsonInt32(3));

    assertThat(idsOf(events, DocumentEvent.EventType.INSERT)).containsExactly(new BsonInt32(1));
    assertThat(idsOf(events, DocumentEvent.EventType.DELETE)).containsExactly(new BsonInt32(4));
  }

  /**
   * Acting on the hash join must produce the same event set as acting on the sort-merge, so
   * flipping the flag changes which comparator classifies the window but not what the resync does.
   * The hash join emits its events grouped by category rather than in the sort-merge's scan order,
   * so this compares them as sets.
   */
  @Test
  public void applyHashJoinEvents_producesTheSameEventsAsTheSortMerge() {
    AutoEmbeddingSortedIdCollectionScanner scanner = newViewIndexScanner();

    var source = sourceWindow(1, 2, 3);
    var matView = matViewWindow(2, 3, 4);
    List<DocumentEvent> hashJoinEvents =
        scanner.applyHashJoinEvents(scanner.hashJoinClassification(source, matView));
    List<DocumentEvent> sortMergeEvents = scanner.sortMergeEvents(source, matView);

    assertThat(eventsOf(hashJoinEvents)).containsExactlyElementsIn(eventsOf(sortMergeEvents));
  }

  /** Classifications that partition the window the same way are not a mismatch. */
  @Test
  public void differingIds_agreeingClassifications_returnsEmpty() {
    var source = sourceWindow(1, 2);
    var matView = matViewWindow(1, 2);
    var sortMerge = newViewIndexScanner().sortMergeClassification(source, matView);
    var hashJoin = newViewIndexScanner().hashJoinClassification(source, matView);

    assertThat(AutoEmbeddingSortedIdCollectionScanner.differingIds(sortMerge, hashJoin)).isEmpty();
  }

  /**
   * A disagreement is an id classified into a different category (insert/delete/match) by one
   * classification than by the other, or present in only one of them.
   */
  @Test
  public void differingIds_disagreeingClassifications_returnsTheDifferingIds() {
    // The sort-merge sees a source-only doc 1 and a mat-view-only doc 2; the hash join sees 1
    // present in both and nothing to delete.
    var sortMerge =
        newViewIndexScanner().sortMergeClassification(sourceWindow(1), matViewWindow(2));
    var hashJoin = newViewIndexScanner().hashJoinClassification(sourceWindow(1), matViewWindow(1));

    assertThat(AutoEmbeddingSortedIdCollectionScanner.differingIds(sortMerge, hashJoin))
        .containsExactly(new BsonInt32(1), new BsonInt32(2));
  }

  /** A mismatch between the two classifications increments the counter; agreement does not. */
  @Test
  public void recordClassificationMismatch_countersTrackTheClassificationAgreement() {
    MetricsFactory metricsFactory = new MetricsFactory("test", new SimpleMeterRegistry());
    AutoEmbeddingSortedIdCollectionScanner scanner = newScanner(true, metricsFactory);

    var agreeingSortMerge =
        newViewIndexScanner().sortMergeClassification(sourceWindow(1), matViewWindow(1));
    var agreeingHashJoin =
        newViewIndexScanner().hashJoinClassification(sourceWindow(1), matViewWindow(1));
    var disagreeingSortMerge =
        newViewIndexScanner().sortMergeClassification(sourceWindow(1), matViewWindow(2));

    scanner.recordClassificationMismatch(agreeingSortMerge, agreeingHashJoin);
    assertThat(metricsFactory.get("resyncMergeClassificationMismatches").counter().count())
        .isEqualTo(0.0);

    scanner.recordClassificationMismatch(disagreeingSortMerge, agreeingHashJoin);
    assertThat(metricsFactory.get("resyncMergeClassificationMismatches").counter().count())
        .isEqualTo(1.0);
  }
}
