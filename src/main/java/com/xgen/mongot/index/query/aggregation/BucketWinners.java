package com.xgen.mongot.index.query.aggregation;

import com.xgen.mongot.util.Check;
import java.io.IOException;
import java.util.Optional;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TopFieldCollector;
import org.apache.lucene.search.TopFieldCollectorManager;
import org.apache.lucene.search.TopFieldDocs;

/**
 * The document each bucket of a {@code first}/{@code last} aggregation has settled on: one
 * single-hit {@link TopFieldCollector} per owning ordinal.
 *
 * <p>A queue of one holds exactly the document its bucket wants, and the collector's own
 * comparators materialize that document's sort key on the way out, so the key needed to merge
 * slices later comes from {@link TopFieldCollector#topDocs()} rather than from reading the document
 * a second time.
 *
 * <p>Direction lives entirely in the {@link Sort} handed to the constructor -- {@code first} passes
 * the query's sort, {@code last} passes its inverse, and an unsorted query passes a plain doc-order
 * sort -- so this class does not know which operator it is serving, and there is only one of it.
 */
final class BucketWinners {

  /**
   * Count every hit, which as a side effect switches early termination off.
   *
   * <p>One bucket's queue filling says nothing about its siblings, and {@link TopFieldCollector}
   * signals early termination by throwing out of {@code collect} -- which would abandon the segment
   * for every other bucket, not just this one.
   */
  private static final int NO_EARLY_TERMINATION = Integer.MAX_VALUE;

  /** These sorts never read scores, but the leaf collectors still expect a scorer to be set. */
  private static final Scorable NO_SCORE =
      new Scorable() {
        @Override
        public float score() {
          return 0f;
        }
      };

  private static final int INITIAL_BUCKETS = 8;

  private final TopFieldCollectorManager collectorManager;

  /** One collector per bucket ordinal; created when a document first lands in that bucket. */
  private final ObjectArray<TopFieldCollector> subCollectors;

  BucketWinners(Sort sort) {
    // null after: a bucket's queue is not paginated, so every matching document competes for it.
    // A non-null value would make the collector skip documents up to that sort key, which is what
    // searchAfter wants for a result page and never what a bucket's winner wants.
    this.collectorManager = new TopFieldCollectorManager(sort, 1, null, NO_EARLY_TERMINATION);
    this.subCollectors = new ObjectArray<>(INITIAL_BUCKETS);
  }

  /**
   * Binds to one segment. Within a bucket, documents must then arrive in increasing doc order, as
   * the underlying readers are forward-only.
   */
  LeafWinners forLeaf(LeafReaderContext leaf) {
    // Rebuilt per segment. A leaf collector belongs to the segment it was bound to, and the
    // framework only has one segment in flight, so the previous segment's collectors fall away
    // with this array rather than accumulating across the search.
    ObjectArray<LeafCollector> leafCollectors = new ObjectArray<>(INITIAL_BUCKETS);
    return (leafDoc, bucketOrd) -> leafCollector(leafCollectors, leaf, bucketOrd).collect(leafDoc);
  }

  /** The current segment's leaf collector for one bucket, creating it and its collector lazily. */
  private LeafCollector leafCollector(
      ObjectArray<LeafCollector> leafCollectors, LeafReaderContext leaf, int bucketOrd)
      throws IOException {
    Optional<LeafCollector> bound = leafCollectors.get(bucketOrd);
    if (bound.isPresent()) {
      return bound.get();
    }

    TopFieldCollector subCollector =
        this.subCollectors
            .get(bucketOrd)
            .orElseGet(
                () -> {
                  TopFieldCollector created = this.collectorManager.newCollector();
                  this.subCollectors.set(bucketOrd, created);
                  return created;
                });

    LeafCollector leafCollector = subCollector.getLeafCollector(leaf);
    leafCollector.setScorer(NO_SCORE);
    leafCollectors.set(bucketOrd, leafCollector);
    return leafCollector;
  }

  /**
   * The winning document of {@code bucketOrd}, or empty if no document landed in that bucket.
   *
   * <p>Call once per ordinal: reading a winner drains the queue that produced it.
   */
  Optional<FieldDoc> winner(int bucketOrd) {
    Optional<TopFieldCollector> subCollector = this.subCollectors.get(bucketOrd);
    if (subCollector.isEmpty()) {
      return Optional.empty();
    }

    TopFieldDocs topDocs = subCollector.get().topDocs();
    return topDocs.scoreDocs.length == 0
        ? Optional.empty()
        : Optional.of(Check.instanceOf(topDocs.scoreDocs[0], FieldDoc.class));
  }

  /** Per-segment view of a {@link BucketWinners}. */
  @FunctionalInterface
  interface LeafWinners {
    void collect(int leafDoc, int bucketOrd) throws IOException;
  }
}
