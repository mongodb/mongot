package com.xgen.mongot.index;

public non-sealed interface InitializedSearchIndex extends InitializedIndex, SearchIndex {

  /**
   * Returns the IndexReader for the Index.
   *
   * @throws IllegalStateException if the Index is closed.
   */
  @Override
  SearchIndexReader getReader();
}
