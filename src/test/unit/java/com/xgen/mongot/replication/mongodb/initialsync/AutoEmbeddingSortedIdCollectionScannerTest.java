package com.xgen.mongot.replication.mongodb.initialsync;

import static com.google.common.truth.Truth.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.xgen.mongot.embedding.config.MaterializedViewCollectionMetadata;
import com.xgen.mongot.embedding.config.MaterializedViewCollectionMetadata.MaterializedViewSchemaMetadata;
import com.xgen.mongot.embedding.config.MaterializedViewCollectionMetadataCatalog;
import com.xgen.mongot.index.DocumentEvent;
import com.xgen.mongot.index.definition.VectorIndexDefinition;
import com.xgen.mongot.index.definition.ViewDefinition;
import com.xgen.mongot.index.version.GenerationId;
import com.xgen.mongot.metrics.MetricsFactory;
import com.xgen.mongot.util.BsonUtils;
import com.xgen.testing.mongot.index.definition.VectorIndexDefinitionBuilder;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Clock;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
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
    return newScanner(onView, new MetricsFactory("test", new SimpleMeterRegistry()));
  }

  private static AutoEmbeddingSortedIdCollectionScanner newScanner(
      boolean onView, MetricsFactory metricsFactory) {
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

    MaterializedViewCollectionMetadata metadata =
        new MaterializedViewCollectionMetadata(
            MaterializedViewSchemaMetadata.VERSION_ZERO, UUID.randomUUID(), "mvCollection");
    MaterializedViewCollectionMetadataCatalog catalog =
        mock(MaterializedViewCollectionMetadataCatalog.class);
    when(catalog.getMetadataIfPresent(any())).thenReturn(Optional.of(metadata));
    when(catalog.getDatabaseName(any())).thenReturn("db");

    return new AutoEmbeddingSortedIdCollectionScanner(
        Clock.systemUTC(),
        context,
        mock(InitialSyncMongoClient.class),
        new BsonInt32(0),
        catalog,
        metricsFactory);
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
}
