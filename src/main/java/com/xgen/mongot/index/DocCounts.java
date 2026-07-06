package com.xgen.mongot.index;

/**
 * Container for the various per-index document counts.
 *
 * <p>These diverge once one source (MongoDB) document maps to more than one Lucene document — i.e.
 * indexes with embedded/nested documents (embedded search fields, or vector indexes with a {@code
 * nestedRoot}). There, each source document is stored as one root/parent Lucene doc plus one child
 * Lucene doc per embedded element (e.g. per vector). For a non-embedded index all counts coincide.
 */
public class DocCounts {

  /**
   * Number of live Lucene documents — root/parent docs plus embedded/child docs — reflecting
   * deletes. For a non-embedded index this equals {@link #numMongoDbDocs}; for embedded/nested
   * indexes it is larger (it also includes the child docs).
   */
  public final long numDocs;

  /**
   * Total Lucene {@code maxDoc} summed across all index partitions. Differs from {@link #numDocs}
   * in that it also counts documents marked deleted but not yet merged away. Tracks how close the
   * whole index is to Lucene's document limit.
   */
  public final long numLuceneMaxDocs;

  /**
   * The largest {@code maxDoc} of any single index partition (the max across partitions, not the
   * sum). Tracks how close an individual partition is to Lucene's per-index document limit.
   */
  public final int maxLuceneMaxDocs;

  /**
   * Number of source MongoDB documents — this is the document count reported to users (e.g. via
   * {@code $listSearchIndexes}). For embedded/nested indexes it is the count of root/parent docs
   * and excludes embedded/child docs; for non-embedded indexes it equals {@link #numDocs}.
   */
  public final long numMongoDbDocs;

  public DocCounts(long numDocs, long numLuceneMaxDocs, int maxLuceneMaxDocs, long numMongoDbDocs) {
    this.numDocs = numDocs;
    this.numLuceneMaxDocs = numLuceneMaxDocs;
    this.maxLuceneMaxDocs = maxLuceneMaxDocs;
    this.numMongoDbDocs = numMongoDbDocs;
  }
}
