package com.xgen.mongot.index;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Optional;

/**
 * Responsible for mutating the state of an Index, either in response to a DocumentEvent or because
 * of an administrative request such as dropping all indexed Documents.
 *
 * <p>Prior to calling close() on an IndexWriter, all other Objects using the IndexWriter must cease
 * using all its methods.
 */
public interface IndexWriter extends Closeable {

  /** Updates the index to reflect the changed document. */
  void updateIndex(DocumentEvent event) throws IOException, FieldExceededLimitsException;

  /**
   * Pre-processes a batch of document events before individual indexing. Writers that maintain
   * auxiliary mappings (e.g. custom vector engine IDs) can override this to resolve those mappings
   * in batch (single query) rather than per-document, setting the resolved data directly on each
   * {@link DocumentEvent} via its mutable fields.
   *
   * <p>The default implementation returns the events unchanged.
   *
   * @param events the batch of document events to prepare
   * @return the (possibly mutated) events, ready for individual {@link #updateIndex} calls
   * @throws IOException if there's an error reading the index during preparation
   */
  default List<DocumentEvent> prepareBatch(List<DocumentEvent> events) throws IOException {
    return events;
  }

  /**
   * Commits the index, along with the supplied user data that can later be retrieved via
   * getCommitUserData.
   */
  void commit(EncodedUserData userData) throws IOException;

  /**
   * Returns the last set of user data that was committed, or an empty map if the index has not been
   * committed yet.
   */
  EncodedUserData getCommitUserData();

  /**
   * Whether or not this index has exceeded usage limits.
   *
   * <p>There two limits:
   *
   * <ul>
   *   <li>Number of fields in a single document. See {@code FieldExceededLimitsException}.
   *   <li>Number of documents in a single index. See {@code DocsExceededLimitsException}.
   * </ul>
   */
  Optional<ExceededLimitsException> exceededLimits();

  /** Returns the number of unique fields indexed by the IndexWriter. */
  int getNumFields() throws WriterClosedException;

  /** Removes all indexed documents from the collection and commits it with the given userData. */
  void deleteAll(EncodedUserData userData) throws IOException;

  @Override
  void close() throws IOException;

  /**
   * The total number of docs in this index. Depending on implementation, the returned value may be
   * approximate.
   */
  long getNumDocs() throws WriterClosedException;

  /**
   * Cancels all running merges and blocks new merges from being scheduled.
   *
   * <p>This method will:
   *
   * <ul>
   *   <li>Block new merges from being scheduled
   *   <li>Mark all running merges as aborted
   *   <li>Wait for running merges to stop (with per-thread timeout)
   * </ul>
   *
   * <p><b>Note on pending merges:</b> Pending merges (those queued in IndexWriter but not yet
   * started) are not explicitly aborted by this method. However, new merge scheduling is blocked,
   * so no new merge threads will be started. Pending merges will be discarded when the IndexWriter
   * is closed.
   *
   * <p><b>Important Notes:</b>
   *
   * <ul>
   *   <li><b>Merges remain disabled:</b> After calling this method, merges will remain disabled. To
   *       re-enable merges, the index writer must be reconfigured or recreated.
   *   <li><b>Disk space cleanup:</b> This method does NOT immediately free up disk space consumed
   *       by partial merge files. Aborted merges may leave temporary segment files on disk that
   *       consume up to 1-2X the size of the segments being merged. To reclaim this disk space, you
   *       must call {@link #commit(EncodedUserData)} or {@link #close()} after cancelling merges.
   *       The commit or close operation will trigger cleanup of unreferenced temporary files.
   *   <li><b>Timeout behavior:</b> This method waits for each merge thread with a configurable
   *       per-thread timeout. If a merge thread does not terminate within the timeout, this method
   *       will return while that thread is still running. Callers should NOT assume all merges are
   *       definitely stopped when this method returns. Stuck merges will continue to be tracked in
   *       monitoring metrics (mergeElapsedSeconds) for visibility.
   * </ul>
   *
   * <p><b>Example usage to reclaim disk space:</b>
   *
   * <pre>{@code
   * writer.cancelMerges();
   * writer.commit(EncodedUserData.EMPTY); // Triggers cleanup of partial merge files
   * }</pre>
   *
   * @throws IOException if there is an error during merge cancellation
   */
  void cancelMerges() throws IOException;

  /**
   * Force-merges the index down to at most {@code maxNumSegments} segments.
   *
   * <p>The default implementation is a no-op for writer implementations that do not support
   * force-merging.
   *
   * @param maxNumSegments the maximum number of segments to merge down to (e.g. {@code 1})
   * @throws IOException if there is an error during the merge
   */
  default void forceMerge(int maxNumSegments) throws IOException {}
}
