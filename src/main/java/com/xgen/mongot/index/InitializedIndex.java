package com.xgen.mongot.index;

import com.xgen.mongot.index.definition.IndexDefinitionGeneration;
import com.xgen.mongot.index.version.GenerationId;
import com.xgen.mongot.util.Check;
import java.io.Closeable;

public interface InitializedIndex extends Index, Closeable {
  /**
   * Returns the IndexReader for the Index.
   *
   * @throws IllegalStateException if the Index is closed.
   */
  IndexReader getReader();

  /**
   * Returns the IndexWriter for the Index.
   *
   * @throws IllegalStateException if the Index is closed.
   */
  IndexWriter getWriter();

  /**
   * Returns an IndexMetricsUpdater to be used for updating this Index's statistics.
   *
   * @throws IllegalStateException if the Index is closed.
   */
  IndexMetricsUpdater getMetricsUpdater();

  /**
   * Returns an IndexMetrics (if present) which is generated from the Index's IndexMetricsUpdater.
   *
   * <p>Some metrics may be expensive to snapshot, and care should be taken to not call this method
   * more than is necessary (for example, multiple times in a loop for the same index).
   *
   * <p>If only the index size is needed, prefer {@link #getIndexSize()} which returns a cached
   * value and is safe to call on hot paths.
   */
  IndexMetrics getMetrics();

  /**
   * Returns the cached index size in bytes.
   *
   * <p>This method returns a pre-computed value that is updated during async metrics collection.
   *
   * @return the cached index size in bytes, or 0 if not yet computed
   */
  long getIndexSize();

  /** Clears all the index data on disk, but does not drop the index. */
  void clear(EncodedUserData dropUserData);

  /** Returns the generation id associated with the InitializedIndex. */
  GenerationId getGenerationId();

  IndexDefinitionGeneration.Type getType();

  @Override
  default InitializedSearchIndex asSearchIndex() {
    return Check.instanceOf(this, InitializedSearchIndex.class);
  }

  @Override
  default InitializedVectorIndex asVectorIndex() {
    return Check.instanceOf(this, InitializedVectorIndex.class);
  }
}
