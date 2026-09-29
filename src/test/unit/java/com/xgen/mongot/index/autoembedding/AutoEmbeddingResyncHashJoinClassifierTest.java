package com.xgen.mongot.index.autoembedding;

import static com.google.common.truth.Truth.assertThat;

import com.xgen.mongot.index.DocumentMetadata;
import com.xgen.mongot.index.autoembedding.AutoEmbeddingResyncHashJoinClassifier.Classification;
import com.xgen.mongot.index.autoembedding.AutoEmbeddingResyncHashJoinClassifier.MatchedPair;
import com.xgen.mongot.index.autoembedding.AutoEmbeddingResyncHashJoinClassifier.SourceIdResolver;
import com.xgen.mongot.util.BsonUtils;
import java.util.List;
import java.util.Optional;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonObjectId;
import org.bson.BsonString;
import org.bson.BsonSymbol;
import org.bson.BsonValue;
import org.bson.RawBsonDocument;
import org.bson.types.ObjectId;
import org.junit.Test;

/**
 * Unit tests for {@link AutoEmbeddingResyncHashJoinClassifier}, the pure per-window hash-join
 * replacement for the sort-merge in {@code AutoEmbeddingSortedIdCollectionScanner}. These target
 * the set-membership classification: insert/delete/matched ids, exact-value {@code _id} keying,
 * exact string matching (the sort-merge's known gap), document ids, and the unreadable-source
 * (HELP-60413) case.
 */
public class AutoEmbeddingResyncHashJoinClassifierTest {

  private static final ObjectId INDEX_ID = new ObjectId("507f1f77bcf86cd799439011");

  private static RawBsonDocument raw(BsonDocument document) {
    return BsonUtils.documentToRaw(document);
  }

  /** A doc keyed by {@code _id} at the root, as a collection index or a mat-view doc uses. */
  private static RawBsonDocument docWithId(BsonValue id) {
    return raw(new BsonDocument("_id", id));
  }

  /** Resolves a source doc's id from the root (collection index / the simple case). */
  private static SourceIdResolver rootIdResolver() {
    return doc -> DocumentMetadata.fromOriginalDocument(Optional.of(doc)).getId();
  }

  /**
   * Resolves a source doc's id from the {@code <indexId>} metadata namespace, as a view index does.
   */
  private static SourceIdResolver metadataNamespaceResolver() {
    return doc ->
        Optional.ofNullable(doc.get(INDEX_ID.toString()))
            .map(BsonValue::asDocument)
            .map(DocumentMetadata::extractId);
  }

  private static List<BsonValue> matchedIds(Classification classification) {
    return classification.matched().stream().map(MatchedPair::id).toList();
  }

  @Test
  public void classify_inSyncWindow_everythingMatches() {
    Classification result =
        AutoEmbeddingResyncHashJoinClassifier.classify(
            List.of(
                docWithId(new BsonInt32(1)),
                docWithId(new BsonInt32(2)),
                docWithId(new BsonInt32(3))),
            List.of(
                docWithId(new BsonInt32(1)),
                docWithId(new BsonInt32(2)),
                docWithId(new BsonInt32(3))),
            rootIdResolver());

    assertThat(result.insertIds()).isEmpty();
    assertThat(result.deleteIds()).isEmpty();
    assertThat(matchedIds(result))
        .containsExactly(new BsonInt32(1), new BsonInt32(2), new BsonInt32(3));
  }

  @Test
  public void classify_differingWindow_partitionsIntoInsertsDeletesAndMatched() {
    Classification result =
        AutoEmbeddingResyncHashJoinClassifier.classify(
            List.of(
                docWithId(new BsonInt32(1)),
                docWithId(new BsonInt32(2)),
                docWithId(new BsonInt32(3))),
            List.of(
                docWithId(new BsonInt32(2)),
                docWithId(new BsonInt32(3)),
                docWithId(new BsonInt32(4))),
            rootIdResolver());

    assertThat(result.insertIds()).containsExactly(new BsonInt32(1));
    assertThat(result.deleteIds()).containsExactly(new BsonInt32(4));
    assertThat(matchedIds(result)).containsExactly(new BsonInt32(2), new BsonInt32(3));
  }

  @Test
  public void classify_preservesSourceAndMatViewMeaningfulOrder() {
    Classification result =
        AutoEmbeddingResyncHashJoinClassifier.classify(
            List.of(
                docWithId(new BsonInt32(1)),
                docWithId(new BsonInt32(2)),
                docWithId(new BsonInt32(4))),
            List.of(
                docWithId(new BsonInt32(2)),
                docWithId(new BsonInt32(3)),
                docWithId(new BsonInt32(4))),
            rootIdResolver());

    assertThat(result.insertIds()).containsExactly(new BsonInt32(1)).inOrder();
    assertThat(result.deleteIds()).containsExactly(new BsonInt32(3)).inOrder();
    assertThat(matchedIds(result)).containsExactly(new BsonInt32(2), new BsonInt32(4)).inOrder();
  }

  @Test
  public void classify_numericAndString_sameLexemeDoNotMatch() {
    Classification result =
        AutoEmbeddingResyncHashJoinClassifier.classify(
            List.of(docWithId(new BsonInt32(1))),
            List.of(docWithId(new BsonString("1"))),
            rootIdResolver());

    assertThat(result.insertIds()).containsExactly(new BsonInt32(1));
    assertThat(result.deleteIds()).containsExactly(new BsonString("1"));
    assertThat(matchedIds(result)).isEmpty();
  }

  @Test
  public void classify_stringIds_matchExactValue_notCollationEqual() {
    Classification equal =
        AutoEmbeddingResyncHashJoinClassifier.classify(
            List.of(docWithId(new BsonString("abc"))),
            List.of(docWithId(new BsonString("abc"))),
            rootIdResolver());
    Classification collationEqualButByteDistinct =
        AutoEmbeddingResyncHashJoinClassifier.classify(
            List.of(docWithId(new BsonString("abc"))),
            List.of(docWithId(new BsonString("ABC"))),
            rootIdResolver());

    assertThat(matchedIds(equal)).containsExactly(new BsonString("abc"));

    // "abc" and "ABC" are collation-equal under a case-insensitive collation but byte-distinct, so
    // exact membership must treat them as different. This is the case the sort-merge mishandles.
    assertThat(collationEqualButByteDistinct.insertIds()).containsExactly(new BsonString("abc"));
    assertThat(collationEqualButByteDistinct.deleteIds()).containsExactly(new BsonString("ABC"));
    assertThat(matchedIds(collationEqualButByteDistinct)).isEmpty();
  }

  @Test
  public void classify_documentIds_matchByValue_notByFieldCollation() {
    BsonDocument sourceId = new BsonDocument("k", new BsonString("x"));
    BsonDocument sameSourceId = new BsonDocument("k", new BsonString("x"));
    BsonDocument differentStringId = new BsonDocument("k", new BsonString("y"));

    Classification equal =
        AutoEmbeddingResyncHashJoinClassifier.classify(
            List.of(docWithId(sourceId)), List.of(docWithId(sameSourceId)), rootIdResolver());
    Classification different =
        AutoEmbeddingResyncHashJoinClassifier.classify(
            List.of(docWithId(sourceId)), List.of(docWithId(differentStringId)), rootIdResolver());

    assertThat(matchedIds(equal)).containsExactly(sourceId);
    assertThat(different.insertIds()).containsExactly(sourceId);
    assertThat(different.deleteIds()).containsExactly(differentStringId);
  }

  @Test
  public void classify_documentIdsDifferingOnlyInFieldOrder_doNotMatch() {
    // BsonDocument.equals is order-insensitive (it is a Map), but mongod treats {a:1, b:2} and
    // {b:2, a:1} as different _ids — and so does the sort-merge's own comparator, which compares
    // document fields positionally. Keying must therefore be order-sensitive too, or a stale
    // mat-view doc whose fields the view reordered would be classified as a match and never
    // deleted.
    BsonDocument ab = new BsonDocument("a", new BsonInt32(1)).append("b", new BsonInt32(2));
    BsonDocument ba = new BsonDocument("b", new BsonInt32(2)).append("a", new BsonInt32(1));

    Classification result =
        AutoEmbeddingResyncHashJoinClassifier.classify(
            List.of(docWithId(ba)), List.of(docWithId(ab)), rootIdResolver());

    assertThat(result.insertIds()).containsExactly(ba);
    assertThat(result.deleteIds()).containsExactly(ab);
    assertThat(matchedIds(result)).isEmpty();
  }

  @Test
  public void classify_unreadableSourceDoc_dropsDocAndDeletesItsMatViewEntry() {
    // A view-index source doc that lost its metadata namespace (HELP-60413). The resolver cannot
    // read its id, so it is dropped; its mat-view entry (root _id 5) is left unmatched and must be
    // a delete, not an insert.
    RawBsonDocument brokenSourceDoc = docWithId(new BsonInt32(5));
    Classification result =
        AutoEmbeddingResyncHashJoinClassifier.classify(
            List.of(brokenSourceDoc),
            List.of(docWithId(new BsonInt32(5))),
            metadataNamespaceResolver());

    assertThat(result.insertIds()).isEmpty();
    assertThat(matchedIds(result)).isEmpty();
    assertThat(result.deleteIds()).containsExactly(new BsonInt32(5));
  }

  @Test
  public void classify_wellFormedAndBrokenSourceDocs_resyncAroundBrokenDoc() {
    // The broken doc (1, no namespace) is skipped; the well-formed doc (10, namespace id 10) is
    // inserted; the leftover mat-view doc (5) is deleted.
    RawBsonDocument broken = docWithId(new BsonInt32(1));
    RawBsonDocument wellFormed =
        raw(
            new BsonDocument("_id", new BsonInt32(10))
                .append(INDEX_ID.toString(), new BsonDocument("_id", new BsonInt32(10))));
    Classification result =
        AutoEmbeddingResyncHashJoinClassifier.classify(
            List.of(broken, wellFormed),
            List.of(docWithId(new BsonInt32(5))),
            metadataNamespaceResolver());

    assertThat(result.insertIds()).containsExactly(new BsonInt32(10));
    assertThat(result.deleteIds()).containsExactly(new BsonInt32(5));
    assertThat(matchedIds(result)).isEmpty();
  }

  @Test
  public void classify_distinctNumericTypes_doNotMatch() {
    // The mat view preserves the source _id exactly, so a matched doc always carries the same BSON
    // type. Exact-value keying therefore treats NumberInt(1) and NumberLong(1) as distinct _ids,
    // which is the correct classification rather than an in-memory cross-type comparison.
    Classification result =
        AutoEmbeddingResyncHashJoinClassifier.classify(
            List.of(docWithId(new BsonInt32(1))),
            List.of(docWithId(new BsonInt64(1L))),
            rootIdResolver());

    assertThat(result.insertIds()).containsExactly(new BsonInt32(1));
    assertThat(result.deleteIds()).containsExactly(new BsonInt64(1L));
    assertThat(matchedIds(result)).isEmpty();
  }

  @Test
  public void classify_symbolAndStringIds_sameText_doNotMatch() {
    // MqlComparator (and mongod) put SYMBOL and STRING in the same type bracket and compare them by
    // their text, so the sort-merge would rate Symbol("a") and String("a") as the SAME _id. Byte
    // keying does not: the encodings differ in their BSON type byte. This is an intentional
    // divergence — the mat view materializes the source _id 1:1, so a matched doc carries the same
    // BSON type on both sides and a symbol never has to meet a string with the same text. If that
    // ever changes, this test is the tripwire.
    Classification result =
        AutoEmbeddingResyncHashJoinClassifier.classify(
            List.of(docWithId(new BsonSymbol("a"))),
            List.of(docWithId(new BsonString("a"))),
            rootIdResolver());

    assertThat(result.insertIds()).containsExactly(new BsonSymbol("a"));
    assertThat(result.deleteIds()).containsExactly(new BsonString("a"));
    assertThat(matchedIds(result)).isEmpty();
  }

  @Test
  public void classify_exoticIdTypes_matchByEncodedBytes() {
    // Types mongod compares within their own brackets: a same-typed pair matches, a differing pair
    // does not. Byte keying gives the same answer as MqlComparator for these without any per-type
    // comparator logic.
    BsonValue objectId = new BsonObjectId(new ObjectId("613d3acce8e2a2a8b9a44e77"));
    BsonValue otherObjectId = new BsonObjectId(new ObjectId("613d3acce8e2a2a8b9a44e78"));

    Classification sameObjectId =
        AutoEmbeddingResyncHashJoinClassifier.classify(
            List.of(docWithId(objectId)), List.of(docWithId(objectId)), rootIdResolver());
    Classification differentObjectIds =
        AutoEmbeddingResyncHashJoinClassifier.classify(
            List.of(docWithId(objectId)), List.of(docWithId(otherObjectId)), rootIdResolver());

    assertThat(matchedIds(sameObjectId)).containsExactly(objectId);
    assertThat(differentObjectIds.insertIds()).containsExactly(objectId);
    assertThat(differentObjectIds.deleteIds()).containsExactly(otherObjectId);
    assertThat(matchedIds(differentObjectIds)).isEmpty();
  }
}
