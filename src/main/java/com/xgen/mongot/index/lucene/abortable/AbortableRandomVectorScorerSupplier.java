package com.xgen.mongot.index.lucene.abortable;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.hnsw.CloseableRandomVectorScorerSupplier;
import org.apache.lucene.util.hnsw.RandomVectorScorer;
import org.apache.lucene.util.hnsw.RandomVectorScorerSupplier;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A wrapper for {@link CloseableRandomVectorScorerSupplier} that enables abort detection during
 * HNSW graph building.
 *
 * <p>This wrapper addresses a limitation of {@link AbortableDirectory}: during HNSW merge, the
 * graph building phase (Phase 2) is entirely in-memory with no disk writes, so the IO-level abort
 * detection in AbortableDirectory cannot detect abort requests during this phase.
 *
 * <p>This wrapper intercepts both {@link RandomVectorScorerSupplier#scorer(int)} and {@link
 * RandomVectorScorer#score(int)} calls and periodically checks if the merge has been aborted. When
 * an abort is detected, it throws a {@link MergePolicy.MergeAbortedException}, causing the merge to
 * terminate.
 *
 * <p><b>HNSW Merge Phases:</b>
 *
 * <ul>
 *   <li>Phase 1: Write vectors to disk - AbortableDirectory handles this
 *   <li>Phase 2: Build graph in memory - THIS CLASS handles abort detection here
 *   <li>Phase 3: Write graph to disk - AbortableDirectory handles this
 * </ul>
 *
 * <p><b>Design:</b>
 *
 * <ul>
 *   <li>Checks abort status on every {@code scorer(int)} call. Lucene's HNSW builder creates a new
 *       scorer at least once per inserted node (and additional short-lived scorers during diversity
 *       checks), so this guarantees per-node responsiveness even when individual scorers issue very
 *       few {@code score()} calls (e.g., small segments, low beam widths).
 *   <li>Also tracks {@code score()} calls in a shared {@link AtomicInteger} on the supplier so that
 *       the {@link #CHECK_ABORT_INTERVAL_SCORE_CALLS} threshold accumulates across all scorers
 *       created by the same supplier rather than resetting per short-lived scorer.
 *   <li>Throws {@link MergePolicy.MergeAbortedException} when abort is detected, allowing Lucene to
 *       recognize this as an intentional abort and handle it appropriately.
 *   <li>Minimal overhead: one atomic increment + comparison per {@code score()} call.
 * </ul>
 */
public class AbortableRandomVectorScorerSupplier implements CloseableRandomVectorScorerSupplier {
  private static final Logger LOG =
      LoggerFactory.getLogger(AbortableRandomVectorScorerSupplier.class);

  private final CloseableRandomVectorScorerSupplier delegate;
  private final MergePolicy.OneMerge merge;
  /**
   * Shared counter across all {@link AbortableRandomVectorScorer} instances created by this
   * supplier. HNSW graph building creates many short-lived scorers (each via {@code scorer(ord)}),
   * some of which only invoke {@code score()} a handful of times. A per-supplier counter ensures
   * the threshold is reached regardless of per-scorer call counts.
   */
  private final AtomicInteger scoreCallsSinceLastCheck = new AtomicInteger();

  /**
   * How often to check if the merge is aborted, in number of score() calls. Set to 1000 to balance
   * between responsiveness and performance overhead. During HNSW graph building, score() is called
   * millions of times, so checking every 1000 calls provides sub-millisecond abort detection with
   * negligible overhead.
   */
  static final int CHECK_ABORT_INTERVAL_SCORE_CALLS = 1000;

  /**
   * Creates a new AbortableRandomVectorScorerSupplier that wraps the given supplier.
   *
   * @param delegate the underlying scorer supplier to wrap
   * @param merge the merge operation to monitor for abortion
   */
  public AbortableRandomVectorScorerSupplier(
      CloseableRandomVectorScorerSupplier delegate, MergePolicy.OneMerge merge) {
    this.delegate = delegate;
    this.merge = merge;
  }

  /**
   * Wraps a CloseableRandomVectorScorerSupplier with abort detection if cancellation is enabled.
   *
   * <p>When {@code cancelMergeEnabled} is {@code true} but {@code merge} is {@code null} (e.g.,
   * called off the merge thread where {@link MergeCancellationContext} is not set), this method
   * logs a warning and returns the unwrapped supplier; Phase 2 will fall back to IO-level abort
   * detection only. The warning makes the silent fallback observable in logs.
   *
   * @param supplier the supplier to potentially wrap
   * @param merge the merge operation to monitor for abortion
   * @param cancelMergeEnabled whether merge cancellation is enabled
   * @return the wrapped supplier if cancellation is enabled and a merge is provided, otherwise the
   *     original supplier
   */
  public static CloseableRandomVectorScorerSupplier wrapIfEnabled(
      CloseableRandomVectorScorerSupplier supplier,
      @Nullable MergePolicy.OneMerge merge,
      boolean cancelMergeEnabled) {
    if (cancelMergeEnabled && merge != null) {
      return new AbortableRandomVectorScorerSupplier(supplier, merge);
    }
    if (cancelMergeEnabled) {
      LOG.warn(
          "Cancel-merge is enabled but no OneMerge is available on the current thread; "
              + "score-level abort detection will be skipped for this field. This indicates the "
              + "HNSW merge ran off the merge thread (e.g., numMergeWorkers > 1).");
    }
    return supplier;
  }

  @Override
  public RandomVectorScorer scorer(int ord) throws IOException {
    // Check on every scorer() call: Lucene's HNSW builder invokes this at least once per node
    // insertion (and again during diversity / connectComponents work), so this provides a
    // deterministic per-node abort signal independent of how many score() calls the returned
    // scorer issues.
    checkAborted(this.merge);
    return new AbortableRandomVectorScorer(
        this.delegate.scorer(ord), this.merge, this.scoreCallsSinceLastCheck);
  }

  /**
   * Returns a copy of this supplier wrapping {@code delegate.copy()}.
   *
   * <p>The {@link CloseableRandomVectorScorerSupplier} interface documents that "the {@link
   * #copy()} returned {@link RandomVectorScorerSupplier} is not necessarily closeable" — i.e.
   * Lucene's API does not guarantee the copy carries a resource lifecycle, and callers are not
   * expected to close it. To remain defensive in case a specific implementation does require
   * cleanup, the returned wrapper here propagates close to the underlying delegate whenever the
   * delegate happens to implement {@link Closeable} (the broader JDK interface), even if it is not
   * the Lucene-specific {@link CloseableRandomVectorScorerSupplier}.
   */
  @Override
  public RandomVectorScorerSupplier copy() throws IOException {
    RandomVectorScorerSupplier copiedSupplier = this.delegate.copy();
    if (copiedSupplier instanceof CloseableRandomVectorScorerSupplier closeableCopy) {
      return new AbortableRandomVectorScorerSupplier(closeableCopy, this.merge);
    }
    return new AbortableRandomVectorScorerSupplierAdapter(copiedSupplier, this.merge);
  }

  @Override
  public void close() throws IOException {
    this.delegate.close();
  }

  @Override
  public int totalVectorCount() {
    return this.delegate.totalVectorCount();
  }

  /**
   * Checks if the merge has been aborted and throws MergeAbortedException if so.
   *
   * <p>We throw {@link MergePolicy.MergeAbortedException} (which extends IOException) instead of a
   * generic IOException so that Lucene can recognize this as an intentional abort and handle it
   * appropriately (e.g., suppress it rather than treating it as a merge failure/tragedy).
   */
  private static void checkAborted(MergePolicy.OneMerge merge)
      throws MergePolicy.MergeAbortedException {
    if (merge.isAborted()) {
      String message =
          "Merge aborted during HNSW graph building for segments: " + merge.segments;
      LOG.info("HNSW graph building interrupted: merge aborted, segments: {}", merge.segments);
      throw new MergePolicy.MergeAbortedException(message);
    }
  }

  /**
   * A RandomVectorScorer wrapper that periodically checks if the merge has been aborted.
   *
   * <p>This wrapper increments a counter shared with its parent supplier on every {@code score()}
   * call and checks {@link MergePolicy.OneMerge#isAborted()} when the counter reaches {@link
   * #CHECK_ABORT_INTERVAL_SCORE_CALLS}. The counter is shared (rather than per-scorer) because
   * HNSW graph building creates many short-lived scorers, some of which only issue a handful of
   * {@code score()} calls; a per-scorer counter could fail to reach the threshold and miss abort
   * detection entirely.
   */
  static class AbortableRandomVectorScorer implements RandomVectorScorer {
    private final RandomVectorScorer delegate;
    private final MergePolicy.OneMerge merge;
    private final AtomicInteger scoreCallsSinceLastCheck;

    AbortableRandomVectorScorer(
        RandomVectorScorer delegate,
        MergePolicy.OneMerge merge,
        AtomicInteger scoreCallsSinceLastCheck) {
      this.delegate = delegate;
      this.merge = merge;
      this.scoreCallsSinceLastCheck = scoreCallsSinceLastCheck;
    }

    @Override
    public float score(int node) throws IOException {
      float result = this.delegate.score(node);
      int count = this.scoreCallsSinceLastCheck.incrementAndGet();
      if (count >= CHECK_ABORT_INTERVAL_SCORE_CALLS) {
        // Only one thread per threshold crossing actually subtracts the interval (CAS); racing
        // threads still call checkAborted but leave the counter alone. This preserves the
        // increments from concurrent scorers (unlike set(0)) while keeping the counter bounded
        // and non-negative (unlike an unconditional addAndGet, which can drive it negative when
        // multiple threads cross the threshold at once and thereby delay the next check).
        this.scoreCallsSinceLastCheck.compareAndSet(
            count, count - CHECK_ABORT_INTERVAL_SCORE_CALLS);
        checkAborted(this.merge);
      }
      return result;
    }

    @Override
    public int maxOrd() {
      return this.delegate.maxOrd();
    }

    @Override
    public int ordToDoc(int ord) {
      return this.delegate.ordToDoc(ord);
    }

    @Override
    public Bits getAcceptOrds(Bits acceptDocs) {
      return this.delegate.getAcceptOrds(acceptDocs);
    }
  }

  /**
   * An adapter for wrapping a {@link RandomVectorScorerSupplier} (typically returned from {@link
   * RandomVectorScorerSupplier#copy()}) that is not a {@link CloseableRandomVectorScorerSupplier}
   * with abort detection.
   *
   * <p>The adapter additionally implements {@link Closeable} and propagates {@code close()} to the
   * underlying delegate if the delegate itself happens to implement {@link Closeable}. This guards
   * against the (uncommon) case where a {@code copy()} implementation owns resources but does not
   * also implement Lucene's {@link CloseableRandomVectorScorerSupplier}: callers that perform an
   * {@code instanceof Closeable} check on the returned supplier can still release those resources.
   * Per Lucene's contract, callers are not required to close copies, so most code paths will
   * simply ignore this.
   */
  static class AbortableRandomVectorScorerSupplierAdapter
      implements RandomVectorScorerSupplier, Closeable {
    private final RandomVectorScorerSupplier delegate;
    private final MergePolicy.OneMerge merge;
    private final AtomicInteger scoreCallsSinceLastCheck = new AtomicInteger();

    AbortableRandomVectorScorerSupplierAdapter(
        RandomVectorScorerSupplier delegate, MergePolicy.OneMerge merge) {
      this.delegate = delegate;
      this.merge = merge;
    }

    @Override
    public RandomVectorScorer scorer(int ord) throws IOException {
      checkAborted(this.merge);
      return new AbortableRandomVectorScorer(
          this.delegate.scorer(ord), this.merge, this.scoreCallsSinceLastCheck);
    }

    @Override
    public RandomVectorScorerSupplier copy() throws IOException {
      return new AbortableRandomVectorScorerSupplierAdapter(this.delegate.copy(), this.merge);
    }

    @Override
    public void close() throws IOException {
      if (this.delegate instanceof Closeable closeableDelegate) {
        closeableDelegate.close();
      }
    }
  }
}

