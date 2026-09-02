package com.xgen.mongot.replication.mongodb.common;

import com.xgen.mongot.index.DocumentEvent;
import com.xgen.mongot.index.ExceededLimitsException;
import com.xgen.mongot.index.FieldExceededLimitsException;
import com.xgen.mongot.index.definition.IndexDefinition;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * An interface for indexing and commit. Implementations should be thread-safe.
 *
 * <ul>
 *   Implementation should also make sure that the following methods guarantee a serialized
 *   execution when called concurrently:
 *   <li>{@link DocumentIndexer#commit}
 *   <li>{@link DocumentIndexer#clearIndex}
 * </ul>
 */
public interface DocumentIndexer {

  void indexDocumentEvent(DocumentEvent event) throws FieldExceededLimitsException;

  /**
   * Pre-processes a batch of document events before individual indexing. Delegates to the
   * underlying writer's {@link com.xgen.mongot.index.IndexWriter#prepareBatch} to allow
   * batch-optimized resolution of auxiliary data (e.g. custom vector engine IDs).
   *
   * @param events the batch of document events
   * @return the (possibly mutated) events, ready for individual {@link #indexDocumentEvent} calls
   */
  List<DocumentEvent> prepareBatch(List<DocumentEvent> events);

  /**
   * Updates live commit user data, which will be persisted along with the indexed documents at
   * commit time. This method provides "last write wins" semantics, so clients should take into
   * account that concurrent calls might cause unexpected results.
   */
  void updateCommitUserData(IndexCommitUserData commitUserData);

  /**
   * Installs a supplier read at commit time, for writers whose resume position has to be assembled
   * from live state instead of staged ahead of a batch. The parallel collection scan uses it: its
   * partitions advance independently, so a blob staged per batch would carry one partition's
   * position and roll the others back.
   */
  void updateCommitUserDataSupplier(Supplier<IndexCommitUserData> commitUserDataSupplier);

  /** Commits all pending changes to the index. */
  void commit() throws IOException;

  /** Clear the index and update the commit user data atomically. */
  void clearIndex(IndexCommitUserData commitUserData) throws IOException;

  Optional<ExceededLimitsException> exceededLimits();

  IndexDefinition getIndexDefinition();
}
