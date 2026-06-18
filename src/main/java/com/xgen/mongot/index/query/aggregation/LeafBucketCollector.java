package com.xgen.mongot.index.query.aggregation;

import java.io.IOException;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Scorable;

/**
 * Implemented lucene LuceneCollector to collect for a segment. It provides bucket oriented
 * collection.
 */
public abstract class LeafBucketCollector implements LeafCollector {
  /**
   * Bucket-aware collection method.
   *
   * <p>We model ordinal as a bucket id. It doesn't have to be tied to lucene sorted set ordinals.
   * The implementations can decide its own ordinal space as long as a unique ordinal is assigned to
   * a bucket.
   *
   * <p>owningBucketOrd is a concept we introduced here. It is decided and passed by parent
   * accumulator down to the child. Child accumulator must group results under this scope.
   *
   * <p>Some bucket accumulators know how many ordinals up front so it can use a dense storage,
   * others don't know how many buckets will use a hash-map like storage.
   */
  public abstract void collect(int doc, long owningBucketOrd) throws IOException;

  @Override
  public final void collect(int doc) throws IOException {
    // Lucene query machinery always collects from root level.
    this.collect(doc, 0);
  }

  /**
   * No-op implementation. Currently Aggregation doesn't care about scores and require revisit
   * when support score based sorting.
   */
  @Override
  public void setScorer(Scorable scorer) throws IOException {}
}
