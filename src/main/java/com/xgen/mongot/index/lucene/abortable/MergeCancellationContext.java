package com.xgen.mongot.index.lucene.abortable;

import org.apache.lucene.index.MergePolicy;
import org.jetbrains.annotations.Nullable;

/**
 * Thread-local context that exposes the currently running {@link MergePolicy.OneMerge} and the
 * cancel-merge feature flag to code paths that do not have direct access to a {@code OneMerge}
 * reference.
 *
 * <p>This is used to bridge the gap between Lucene's vector merge code (which receives a {@link
 * org.apache.lucene.index.MergeState} but no {@code OneMerge}) and {@link
 * AbortableRandomVectorScorerSupplier}, which needs both the {@code OneMerge} and the
 * cancellation flag to enable score-level abort detection during HNSW graph building.
 *
 * <p>The context is set by the merge scheduler at the start of a merge (on the dedicated merge
 * thread) and cleared in a {@code finally} block when the merge finishes, so the lifecycle is
 * bounded by the merge thread's call to {@code IndexWriter#merge}.
 *
 * <p><b>Threading:</b> values are confined to the thread that set them. Vector formats run with
 * {@code numMergeWorkers == 1} in production, so graph construction executes on the same merge
 * thread and observes the context. If a format ever runs with multiple workers, the context is
 * not propagated to worker threads and abort detection in those threads is simply skipped (the
 * existing IO-level checks in {@link AbortableDirectory} continue to apply).
 */
public final class MergeCancellationContext {

  private static final ThreadLocal<MergePolicy.OneMerge> CURRENT_MERGE = new ThreadLocal<>();
  private static final ThreadLocal<Boolean> CANCEL_ENABLED = ThreadLocal.withInitial(() -> false);

  private MergeCancellationContext() {}

  /**
   * Sets the current merge and cancel-merge flag for this thread. Callers must pair this with a
   * call to {@link #clear()} in a {@code finally} block.
   */
  public static void set(MergePolicy.OneMerge merge, boolean cancelMergeEnabled) {
    CURRENT_MERGE.set(merge);
    CANCEL_ENABLED.set(cancelMergeEnabled);
  }

  /** Clears the current merge and cancel-merge flag for this thread. */
  public static void clear() {
    CURRENT_MERGE.remove();
    CANCEL_ENABLED.remove();
  }

  /** Returns the merge currently running on this thread, or {@code null} if none is set. */
  public static @Nullable MergePolicy.OneMerge currentMerge() {
    return CURRENT_MERGE.get();
  }

  /** Returns whether cancel-merge is enabled for the merge running on this thread. */
  public static boolean isCancelMergeEnabled() {
    return CANCEL_ENABLED.get();
  }
}

