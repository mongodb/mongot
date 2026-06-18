package com.xgen.mongot.index.lucene.abortable;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.util.hnsw.CloseableRandomVectorScorerSupplier;
import org.apache.lucene.util.hnsw.RandomVectorScorer;
import org.apache.lucene.util.hnsw.RandomVectorScorerSupplier;
import org.junit.Test;

/** Unit tests for {@link AbortableRandomVectorScorerSupplier}. */
public class AbortableRandomVectorScorerSupplierTest {

  /**
   * Test that AbortableRandomVectorScorerSupplier throws MergeAbortedException from scorer() when
   * the merge is already aborted. This guarantees per-node responsiveness in Lucene's HNSW builder,
   * which calls scorer(ord) at least once per inserted node.
   */
  @Test
  public void testAbortDetectedOnScorerCreation() throws IOException {
    MergePolicy.OneMerge mockMerge = mock(MergePolicy.OneMerge.class);
    when(mockMerge.isAborted()).thenReturn(true);

    CloseableRandomVectorScorerSupplier mockSupplier =
        mock(CloseableRandomVectorScorerSupplier.class);

    AbortableRandomVectorScorerSupplier abortableSupplier =
        new AbortableRandomVectorScorerSupplier(mockSupplier, mockMerge);

    MergePolicy.MergeAbortedException exception =
        assertThrows(MergePolicy.MergeAbortedException.class, () -> abortableSupplier.scorer(0));

    assertTrue(
        "Exception message should mention HNSW graph building",
        exception.getMessage().contains("Merge aborted during HNSW graph building"));
  }

  /**
   * Test that AbortableRandomVectorScorerSupplier throws MergeAbortedException when the merge is
   * aborted during score() calls (i.e., abort flips between scorer() and the threshold check).
   */
  @Test
  public void testAbortDuringScoring() throws IOException {
    // Merge starts non-aborted (so scorer() succeeds) and becomes aborted thereafter.
    MergePolicy.OneMerge mockMerge = mock(MergePolicy.OneMerge.class);
    when(mockMerge.isAborted()).thenReturn(false).thenReturn(true);

    RandomVectorScorer mockScorer = mock(RandomVectorScorer.class);
    when(mockScorer.score(anyInt())).thenReturn(0.5f);

    CloseableRandomVectorScorerSupplier mockSupplier =
        mock(CloseableRandomVectorScorerSupplier.class);
    when(mockSupplier.scorer(anyInt())).thenReturn(mockScorer);

    AbortableRandomVectorScorerSupplier abortableSupplier =
        new AbortableRandomVectorScorerSupplier(mockSupplier, mockMerge);

    RandomVectorScorer scorer = abortableSupplier.scorer(0);

    int callsToTriggerCheck = AbortableRandomVectorScorerSupplier.CHECK_ABORT_INTERVAL_SCORE_CALLS;

    MergePolicy.MergeAbortedException exception =
        assertThrows(
            MergePolicy.MergeAbortedException.class,
            () -> {
              for (int i = 0; i < callsToTriggerCheck; i++) {
                scorer.score(i);
              }
            });

    assertTrue(
        "Exception message should mention HNSW graph building",
        exception.getMessage().contains("Merge aborted during HNSW graph building"));
  }

  /**
   * Test that the score()-call threshold is shared across all scorers created by the same supplier.
   * This prevents the regression where many short-lived scorers (each issuing &lt; threshold
   * score() calls) could individually never trigger an abort check.
   *
   * <p>The merge's aborted state is driven by an external counter that increments only on {@code
   * score()} calls. {@code isAborted()} returns {@code true} exclusively after that counter
   * reaches the per-supplier threshold, so any abort observed by the {@code scorer()} path would
   * be impossible — the only way the exception can be thrown is via the shared score()-counter
   * check, which is precisely the behavior under test.
   */
  @Test
  public void testAbortThresholdAccumulatesAcrossShortLivedScorers() throws IOException {
    int interval = AbortableRandomVectorScorerSupplier.CHECK_ABORT_INTERVAL_SCORE_CALLS;
    AtomicInteger scoreCallCount = new AtomicInteger(0);

    MergePolicy.OneMerge mockMerge = mock(MergePolicy.OneMerge.class);
    // isAborted() returns true only once the score()-driven counter has reached the threshold.
    // Any scorer()-side abort check before that point will observe false, so the only way the
    // exception can be thrown is via the shared score()-counter check.
    when(mockMerge.isAborted()).thenAnswer(invocation -> scoreCallCount.get() >= interval);

    RandomVectorScorer mockScorer = mock(RandomVectorScorer.class);
    when(mockScorer.score(anyInt()))
        .thenAnswer(
            invocation -> {
              scoreCallCount.incrementAndGet();
              return 0.5f;
            });

    CloseableRandomVectorScorerSupplier mockSupplier =
        mock(CloseableRandomVectorScorerSupplier.class);
    when(mockSupplier.scorer(anyInt())).thenReturn(mockScorer);

    AbortableRandomVectorScorerSupplier abortableSupplier =
        new AbortableRandomVectorScorerSupplier(mockSupplier, mockMerge);

    // Each short-lived scorer issues only a few score() calls (well below the interval), yet the
    // accumulated count across them must still reach the threshold and trigger the abort check.
    int callsPerScorer = 5;
    int numScorers = (interval / callsPerScorer) + 1;

    MergePolicy.MergeAbortedException exception =
        assertThrows(
            MergePolicy.MergeAbortedException.class,
            () -> {
              for (int s = 0; s < numScorers; s++) {
                RandomVectorScorer shortLived = abortableSupplier.scorer(s);
                for (int i = 0; i < callsPerScorer; i++) {
                  shortLived.score(i);
                }
              }
            });

    assertTrue(
        "Exception message should mention HNSW graph building",
        exception.getMessage().contains("Merge aborted during HNSW graph building"));
    // Sanity: the abort must have been observed via the score()-counter path, which means at
    // least `interval` score() calls were made (i.e., short-lived scorers contributed
    // cumulatively).
    assertTrue(
        "Abort should only trip after the cumulative score() count reaches the threshold; got "
            + scoreCallCount.get(),
        scoreCallCount.get() >= interval);
  }

  /**
   * Test that AbortableRandomVectorScorerSupplier allows normal scoring when merge is not aborted.
   */
  @Test
  public void testNormalScoringWhenNotAborted() throws IOException {
    // Create a mock merge that is never aborted
    MergePolicy.OneMerge mockMerge = mock(MergePolicy.OneMerge.class);
    when(mockMerge.isAborted()).thenReturn(false);

    // Create a mock scorer that returns a constant score
    RandomVectorScorer mockScorer = mock(RandomVectorScorer.class);
    when(mockScorer.score(anyInt())).thenReturn(0.75f);

    // Create a mock supplier
    CloseableRandomVectorScorerSupplier mockSupplier =
        mock(CloseableRandomVectorScorerSupplier.class);
    when(mockSupplier.scorer(anyInt())).thenReturn(mockScorer);

    // Create the abortable supplier
    AbortableRandomVectorScorerSupplier abortableSupplier =
        new AbortableRandomVectorScorerSupplier(mockSupplier, mockMerge);

    // Get a scorer
    RandomVectorScorer scorer = abortableSupplier.scorer(0);

    // Call score() many times - should not throw
    int numCalls = AbortableRandomVectorScorerSupplier.CHECK_ABORT_INTERVAL_SCORE_CALLS * 3;
    for (int i = 0; i < numCalls; i++) {
      float score = scorer.score(i % 100);
      assertEquals(0.75f, score, 0.001f);
    }

    // Verify the delegate scorer was called the expected number of times
    verify(mockScorer, times(numCalls)).score(anyInt());
  }

  /** Test that abort check happens at the correct interval. */
  @Test
  public void testAbortCheckInterval() throws IOException {
    MergePolicy.OneMerge mockMerge = mock(MergePolicy.OneMerge.class);
    // isAborted() is invoked: (1) by scorer(0) on creation, (2) by the first threshold check after
    // `interval` score() calls, (3) by the second threshold check after another `interval` calls.
    // Only the third invocation reports aborted.
    when(mockMerge.isAborted()).thenReturn(false, false, true);

    RandomVectorScorer mockScorer = mock(RandomVectorScorer.class);
    when(mockScorer.score(anyInt())).thenReturn(0.5f);

    CloseableRandomVectorScorerSupplier mockSupplier =
        mock(CloseableRandomVectorScorerSupplier.class);
    when(mockSupplier.scorer(anyInt())).thenReturn(mockScorer);

    AbortableRandomVectorScorerSupplier abortableSupplier =
        new AbortableRandomVectorScorerSupplier(mockSupplier, mockMerge);

    RandomVectorScorer scorer = abortableSupplier.scorer(0);

    int interval = AbortableRandomVectorScorerSupplier.CHECK_ABORT_INTERVAL_SCORE_CALLS;

    // First batch of calls should succeed (threshold check returns false)
    for (int i = 0; i < interval; i++) {
      scorer.score(i);
    }

    // Second batch should fail (threshold check returns true)
    MergePolicy.MergeAbortedException exception =
        assertThrows(
            MergePolicy.MergeAbortedException.class,
            () -> {
              for (int i = 0; i < interval; i++) {
                scorer.score(i);
              }
            });

    assertTrue(
        "Exception message should mention HNSW graph building",
        exception.getMessage().contains("Merge aborted during HNSW graph building"));
  }

  /** Test that wrapIfEnabled returns the original supplier when cancellation is disabled. */
  @Test
  public void testWrapIfEnabledWhenDisabled() {
    CloseableRandomVectorScorerSupplier mockSupplier =
        mock(CloseableRandomVectorScorerSupplier.class);
    MergePolicy.OneMerge mockMerge = mock(MergePolicy.OneMerge.class);

    CloseableRandomVectorScorerSupplier result =
        AbortableRandomVectorScorerSupplier.wrapIfEnabled(mockSupplier, mockMerge, false);

    assertSame("Should return original supplier when disabled", mockSupplier, result);
  }

  /** Test that wrapIfEnabled returns wrapped supplier when cancellation is enabled. */
  @Test
  public void testWrapIfEnabledWhenEnabled() {
    CloseableRandomVectorScorerSupplier mockSupplier =
        mock(CloseableRandomVectorScorerSupplier.class);
    MergePolicy.OneMerge mockMerge = mock(MergePolicy.OneMerge.class);

    CloseableRandomVectorScorerSupplier result =
        AbortableRandomVectorScorerSupplier.wrapIfEnabled(mockSupplier, mockMerge, true);

    assertTrue(
        "Should return AbortableRandomVectorScorerSupplier when enabled",
        result instanceof AbortableRandomVectorScorerSupplier);
  }

  /** Test that wrapIfEnabled handles null merge gracefully. */
  @Test
  public void testWrapIfEnabledWithNullMerge() {
    CloseableRandomVectorScorerSupplier mockSupplier =
        mock(CloseableRandomVectorScorerSupplier.class);

    CloseableRandomVectorScorerSupplier result =
        AbortableRandomVectorScorerSupplier.wrapIfEnabled(mockSupplier, null, true);

    assertSame("Should return original supplier when merge is null", mockSupplier, result);
  }

  /** Test that close() is properly delegated. */
  @Test
  public void testCloseIsDelegated() throws IOException {
    CloseableRandomVectorScorerSupplier mockSupplier =
        mock(CloseableRandomVectorScorerSupplier.class);
    MergePolicy.OneMerge mockMerge = mock(MergePolicy.OneMerge.class);

    AbortableRandomVectorScorerSupplier abortableSupplier =
        new AbortableRandomVectorScorerSupplier(mockSupplier, mockMerge);

    abortableSupplier.close();

    verify(mockSupplier, times(1)).close();
  }

  /** Test that totalVectorCount() is properly delegated. */
  @Test
  public void testTotalVectorCountIsDelegated() {
    CloseableRandomVectorScorerSupplier mockSupplier =
        mock(CloseableRandomVectorScorerSupplier.class);
    when(mockSupplier.totalVectorCount()).thenReturn(12345);
    MergePolicy.OneMerge mockMerge = mock(MergePolicy.OneMerge.class);

    AbortableRandomVectorScorerSupplier abortableSupplier =
        new AbortableRandomVectorScorerSupplier(mockSupplier, mockMerge);

    assertEquals(12345, abortableSupplier.totalVectorCount());
  }

  /** Test that copy() returns a wrapped supplier when the delegate copy is closeable. */
  @Test
  public void testCopyReturnsWrappedSupplier() throws IOException {
    CloseableRandomVectorScorerSupplier mockSupplier =
        mock(CloseableRandomVectorScorerSupplier.class);
    CloseableRandomVectorScorerSupplier mockCopiedSupplier =
        mock(CloseableRandomVectorScorerSupplier.class);
    when(mockSupplier.copy()).thenReturn(mockCopiedSupplier);
    MergePolicy.OneMerge mockMerge = mock(MergePolicy.OneMerge.class);

    AbortableRandomVectorScorerSupplier abortableSupplier =
        new AbortableRandomVectorScorerSupplier(mockSupplier, mockMerge);

    RandomVectorScorerSupplier copiedSupplier = abortableSupplier.copy();

    assertNotNull("Copied supplier should not be null", copiedSupplier);
    assertTrue(
        "Copied supplier should be AbortableRandomVectorScorerSupplier",
        copiedSupplier instanceof AbortableRandomVectorScorerSupplier);
  }

  /**
   * Test that copy() falls back to a non-closeable adapter when the delegate's copy() returns a
   * non-closeable supplier, and that abort detection is still enforced through that adapter.
   */
  @Test
  public void testCopyFallsBackToAdapterForNonCloseableCopy() throws IOException {
    // Delegate copy() returns a plain RandomVectorScorerSupplier (not closeable)
    RandomVectorScorerSupplier nonCloseableCopy = mock(RandomVectorScorerSupplier.class);
    RandomVectorScorer mockScorer = mock(RandomVectorScorer.class);
    when(mockScorer.score(anyInt())).thenReturn(0.25f);
    when(nonCloseableCopy.scorer(anyInt())).thenReturn(mockScorer);

    CloseableRandomVectorScorerSupplier mockSupplier =
        mock(CloseableRandomVectorScorerSupplier.class);
    when(mockSupplier.copy()).thenReturn(nonCloseableCopy);

    MergePolicy.OneMerge mockMerge = mock(MergePolicy.OneMerge.class);
    // First isAborted() call (from scorer() on the adapter) returns false so we can verify
    // that the per-score() threshold check on the adapter-produced scorer still detects abort.
    when(mockMerge.isAborted()).thenReturn(false, true);

    AbortableRandomVectorScorerSupplier abortableSupplier =
        new AbortableRandomVectorScorerSupplier(mockSupplier, mockMerge);

    RandomVectorScorerSupplier copiedSupplier = abortableSupplier.copy();
    assertNotNull("Copied supplier should not be null", copiedSupplier);
    // It should NOT be the closeable wrapper since the delegate copy is non-closeable
    assertTrue(
        "Copied supplier should not be AbortableRandomVectorScorerSupplier when delegate copy is "
            + "non-closeable",
        !(copiedSupplier instanceof AbortableRandomVectorScorerSupplier));

    // Scorer produced by the adapter should still honor abort detection
    RandomVectorScorer scorer = copiedSupplier.scorer(0);
    int interval = AbortableRandomVectorScorerSupplier.CHECK_ABORT_INTERVAL_SCORE_CALLS;
    MergePolicy.MergeAbortedException exception =
        assertThrows(
            MergePolicy.MergeAbortedException.class,
            () -> {
              for (int i = 0; i < interval; i++) {
                scorer.score(i);
              }
            });
    assertTrue(
        "Exception message should mention HNSW graph building",
        exception.getMessage().contains("Merge aborted during HNSW graph building"));
  }

  /**
   * Test that when {@code delegate.copy()} returns a supplier that happens to also implement {@link
   * Closeable} (but not Lucene's {@link CloseableRandomVectorScorerSupplier}), the adapter
   * propagates {@code close()} to that delegate so its resources can still be released by callers
   * that perform an {@code instanceof Closeable} check.
   */
  @Test
  public void testAdapterPropagatesCloseToCloseableDelegate() throws IOException {
    // Mock implements both RandomVectorScorerSupplier and java.io.Closeable, but NOT Lucene's
    // CloseableRandomVectorScorerSupplier — this is the case the reviewer flagged.
    RandomVectorScorerSupplier closeableButNotLuceneCloseable =
        mock(RandomVectorScorerSupplier.class, withSettings().extraInterfaces(Closeable.class));

    CloseableRandomVectorScorerSupplier mockSupplier =
        mock(CloseableRandomVectorScorerSupplier.class);
    when(mockSupplier.copy()).thenReturn(closeableButNotLuceneCloseable);

    MergePolicy.OneMerge mockMerge = mock(MergePolicy.OneMerge.class);

    AbortableRandomVectorScorerSupplier abortableSupplier =
        new AbortableRandomVectorScorerSupplier(mockSupplier, mockMerge);

    RandomVectorScorerSupplier copiedSupplier = abortableSupplier.copy();
    assertTrue(
        "Adapter should itself be Closeable so callers can release delegate resources",
        copiedSupplier instanceof Closeable);

    ((Closeable) copiedSupplier).close();
    verify((Closeable) closeableButNotLuceneCloseable, times(1)).close();
  }

  /**
   * Test that closing the adapter when the underlying delegate is not {@link Closeable} is a no-op
   * and does not throw.
   */
  @Test
  public void testAdapterCloseIsNoOpForNonCloseableDelegate() throws IOException {
    RandomVectorScorerSupplier nonCloseable = mock(RandomVectorScorerSupplier.class);

    CloseableRandomVectorScorerSupplier mockSupplier =
        mock(CloseableRandomVectorScorerSupplier.class);
    when(mockSupplier.copy()).thenReturn(nonCloseable);

    MergePolicy.OneMerge mockMerge = mock(MergePolicy.OneMerge.class);

    AbortableRandomVectorScorerSupplier abortableSupplier =
        new AbortableRandomVectorScorerSupplier(mockSupplier, mockMerge);

    RandomVectorScorerSupplier copiedSupplier = abortableSupplier.copy();
    assertTrue(copiedSupplier instanceof Closeable);
    ((Closeable) copiedSupplier).close();
  }

  /** Test that scorer delegates maxOrd() correctly. */
  @Test
  public void testScorerMaxOrdIsDelegated() throws IOException {
    MergePolicy.OneMerge mockMerge = mock(MergePolicy.OneMerge.class);
    when(mockMerge.isAborted()).thenReturn(false);

    RandomVectorScorer mockScorer = mock(RandomVectorScorer.class);
    when(mockScorer.maxOrd()).thenReturn(999);

    CloseableRandomVectorScorerSupplier mockSupplier =
        mock(CloseableRandomVectorScorerSupplier.class);
    when(mockSupplier.scorer(anyInt())).thenReturn(mockScorer);

    AbortableRandomVectorScorerSupplier abortableSupplier =
        new AbortableRandomVectorScorerSupplier(mockSupplier, mockMerge);

    RandomVectorScorer scorer = abortableSupplier.scorer(0);

    assertEquals(999, scorer.maxOrd());
  }

  /** Test that scorer delegates ordToDoc() correctly. */
  @Test
  public void testScorerOrdToDocIsDelegated() throws IOException {
    MergePolicy.OneMerge mockMerge = mock(MergePolicy.OneMerge.class);
    when(mockMerge.isAborted()).thenReturn(false);

    RandomVectorScorer mockScorer = mock(RandomVectorScorer.class);
    when(mockScorer.ordToDoc(42)).thenReturn(100);

    CloseableRandomVectorScorerSupplier mockSupplier =
        mock(CloseableRandomVectorScorerSupplier.class);
    when(mockSupplier.scorer(anyInt())).thenReturn(mockScorer);

    AbortableRandomVectorScorerSupplier abortableSupplier =
        new AbortableRandomVectorScorerSupplier(mockSupplier, mockMerge);

    RandomVectorScorer scorer = abortableSupplier.scorer(0);

    assertEquals(100, scorer.ordToDoc(42));
  }
}

