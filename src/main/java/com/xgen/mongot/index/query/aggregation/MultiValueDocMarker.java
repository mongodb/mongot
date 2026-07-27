package com.xgen.mongot.index.query.aggregation;

/**
 * Tracks whether the current segment doc was fanned into multiple buckets by an ancestor group
 * field.
 *
 * <p>One instance is scoped to a single top-level facet branch. State is segment-local and reset
 * when the framework binds a new {@link org.apache.lucene.index.LeafReaderContext}.
 */
final class MultiValueDocMarker {
  private volatile int markedDoc = -1;

  void reset() {
    this.markedDoc = -1;
  }

  void markMultiValued(int doc) {
    this.markedDoc = doc;
  }

  boolean isMultiValued(int doc) {
    return this.markedDoc == doc;
  }
}
