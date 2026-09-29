package com.xgen.mongot.index.autoembedding;

import com.xgen.mongot.index.DocumentMetadata;
import com.xgen.mongot.util.bson.ByteUtils;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.bson.RawBsonDocument;

/**
 * Classifies the delta between a source-collection window and a materialized-view window with a
 * hash join instead of an in-memory sort-merge.
 *
 * <p>Both the source scan and the mat-view scan are served in the same server {@code _id} order
 * (the mat view shares its source's collation since CLOUDP-444644 / PR #7908), so a document
 * present in both collections lands in the same window. Inside one window the merge needs only set
 * membership, not ordering: index the mat-view docs by {@code _id}, walk the source docs, and
 * classify each as an INSERT (no mat-view match), a DELETE (mat-view doc with no source match), or
 * a MATCH (present in both, handed to {@code compareDocuments}). This removes the in-memory
 * comparator (ICU collator / {@code MqlComparator}) from the resync, so ICU-vs-mongod parity holds
 * by construction.
 *
 * <p>This class is intentionally pure: it does not read from the network, mutate anything, or know
 * about feature flags. The scanner wires it in (shadow behind a flag) in a follow-up PR. It is not
 * called from anywhere yet.
 *
 * <p>Docs are keyed by their encoded {@code _id} bytes rather than {@code BsonValue} equality.
 * {@code BsonDocument.equals} is order-insensitive (it is a {@link Map}), but mongod treats {@code
 * {a:1, b:2}} and {@code {b:2, a:1}} as different {@code _id}s — as does the sort-merge's own
 * comparator, {@code MqlComparator}, which compares document fields positionally. Encoding
 * preserves field order and pins the BSON type, so the bytes give exactly mongod's {@code _id}
 * equality. The mat view is materialized 1:1 from the source document's {@code _id}, so a document
 * present in both collections normally carries a byte-identical {@code _id} on each side, and
 * exact-value membership is the right classification. The sort-merge's known gaps are exactly the
 * cases where the in-memory comparator mis-orders or mis-compares byte-distinct values (non-simple
 * collation in CLOUDP-444644, UTF-16-vs-UTF-8 ordering in CLOUDP-447877, document {@code _id}s
 * compared without the collation) — byte keying sidesteps all of them.
 */
public final class AutoEmbeddingResyncHashJoinClassifier {

  private AutoEmbeddingResyncHashJoinClassifier() {}

  /** Resolves the {@code _id} of a source document exactly as the scanner will. */
  @FunctionalInterface
  public interface SourceIdResolver {
    /**
     * Resolves a source document's {@code _id} from its metadata namespace (a view index) or its
     * root (a collection index).
     *
     * @return the source document's {@code _id}, or empty when the source doc has no readable id
     *     (the missing metadata-namespace / HELP-60413 case).
     */
    Optional<BsonValue> resolve(RawBsonDocument sourceDoc);
  }

  /** A source doc that matched a mat-view doc, ready for the {@code compareDocuments} step. */
  public record MatchedPair(BsonValue id, RawBsonDocument sourceDoc, RawBsonDocument matViewDoc) {}

  /**
   * The classification of one (source window, mat-view window): the ids (and docs) to insert and
   * delete, plus the matched pairs. Lists are ordered: inserts follow source order, deletes follow
   * mat-view order, matched follows source order. Ids and docs are companions so the acting path
   * can build the scan events without re-resolving ids.
   */
  public record Classification(
      List<BsonValue> insertIds,
      List<RawBsonDocument> insertDocs,
      List<BsonValue> deleteIds,
      List<RawBsonDocument> deleteDocs,
      List<MatchedPair> matched) {}

  /**
   * Classifies a (source window, mat-view window) pair.
   *
   * @param sourceWindow the source-collection docs in this window, in scan order.
   * @param matViewWindow the mat-view docs in this window, in scan order.
   * @param sourceIdResolver extracts a source doc's id (metadata namespace for a view index, root
   *     otherwise). An empty result marks the doc as unreadable (HELP-60413): it is dropped, and
   *     its mat-view entry (if one exists) becomes a delete.
   */
  public static Classification classify(
      List<RawBsonDocument> sourceWindow,
      List<RawBsonDocument> matViewWindow,
      SourceIdResolver sourceIdResolver) {
    // Index mat-view docs by _id, in mat-view (scan) order so leftover deletes come out in that
    // order. A key maps to a list defensively, but a mat view has unique _ids so it holds one doc.
    Map<ByteBuffer, List<RawBsonDocument>> matViewDocsByKey = new LinkedHashMap<>();
    for (RawBsonDocument matViewDoc : matViewWindow) {
      Optional<BsonValue> id =
          DocumentMetadata.fromOriginalDocument(Optional.of(matViewDoc)).getId();
      if (id.isEmpty()) {
        // A mat-view doc read via a raw find command always has a root _id; a missing id here means
        // we cannot classify it. Skip it rather than crash (matches the scanner's defensive
        // stance).
        continue;
      }
      matViewDocsByKey
          .computeIfAbsent(keyOf(id.get()), ignored -> new ArrayList<>())
          .add(matViewDoc);
    }

    List<BsonValue> insertIds = new ArrayList<>();
    List<RawBsonDocument> insertDocs = new ArrayList<>();
    List<BsonValue> deleteIds = new ArrayList<>();
    List<RawBsonDocument> deleteDocs = new ArrayList<>();
    List<MatchedPair> matched = new ArrayList<>();

    // Walk source docs in order. A source doc that matches a mat-view doc consumes that mat-view
    // doc; anything left in the mat-view map afterwards is a delete.
    for (RawBsonDocument sourceDoc : sourceWindow) {
      Optional<BsonValue> sourceId = sourceIdResolver.resolve(sourceDoc);
      if (sourceId.isEmpty()) {
        // Unreadable source doc (HELP-60413): drop it (no insert). Its mat-view entry, if present,
        // was never consumed and falls out of the map below as a delete.
        continue;
      }
      BsonValue id = sourceId.get();
      ByteBuffer key = keyOf(id);
      List<RawBsonDocument> candidates = matViewDocsByKey.get(key);
      if (candidates != null && !candidates.isEmpty()) {
        RawBsonDocument matViewDoc = candidates.remove(0);
        if (candidates.isEmpty()) {
          matViewDocsByKey.remove(key);
        }
        matched.add(new MatchedPair(id, sourceDoc, matViewDoc));
      } else {
        insertIds.add(id);
        insertDocs.add(sourceDoc);
      }
    }

    // Everything left in the mat-view map has no source match: delete it. This includes the
    // mat-view entry of an unreadable (HELP-60413) source doc, which is dropped from the index.
    for (Map.Entry<ByteBuffer, List<RawBsonDocument>> entry : matViewDocsByKey.entrySet()) {
      for (RawBsonDocument matViewDoc : entry.getValue()) {
        BsonValue id =
            DocumentMetadata.fromOriginalDocument(Optional.of(matViewDoc)).getId().orElseThrow();
        deleteIds.add(id);
        deleteDocs.add(matViewDoc);
      }
    }

    return new Classification(
        List.copyOf(insertIds),
        List.copyOf(insertDocs),
        List.copyOf(deleteIds),
        List.copyOf(deleteDocs),
        List.copyOf(matched));
  }

  // Keys an _id by its encoded BSON bytes rather than BsonValue equality. BsonDocument.equals is
  // order-insensitive (it is a Map), but mongod treats {a:1, b:2} and {b:2, a:1} as different _ids,
  // as does the sort-merge's own comparator, MqlComparator, which compares document fields
  // positionally. A mat-view doc and a field-reordered source twin must not share a key, or the
  // stale doc would never be deleted. Encoding preserves field order and pins the BSON type, so the
  // bytes give exactly mongod's _id equality.
  private static ByteBuffer keyOf(BsonValue id) {
    return ByteBuffer.wrap(ByteUtils.toByteArray(new BsonDocument("k", id)));
  }
}
