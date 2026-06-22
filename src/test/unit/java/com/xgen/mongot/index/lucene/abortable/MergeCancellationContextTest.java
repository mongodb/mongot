package com.xgen.mongot.index.lucene.abortable;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import java.util.concurrent.atomic.AtomicReference;
import org.apache.lucene.index.MergePolicy;
import org.junit.After;
import org.junit.Test;

/** Unit tests for {@link MergeCancellationContext}. */
public class MergeCancellationContextTest {

  @After
  public void tearDown() {
    // Make sure the context is cleared even if a test fails partway through.
    MergeCancellationContext.clear();
  }

  /** Default state on a fresh thread is no merge and cancel disabled. */
  @Test
  public void testDefaultState() {
    assertNull(MergeCancellationContext.currentMerge());
    assertFalse(MergeCancellationContext.isCancelMergeEnabled());
  }

  /** set() and clear() round-trip the merge reference and flag. */
  @Test
  public void testSetAndClear() {
    MergePolicy.OneMerge merge = mock(MergePolicy.OneMerge.class);

    MergeCancellationContext.set(merge, true);
    assertSame(merge, MergeCancellationContext.currentMerge());
    assertTrue(MergeCancellationContext.isCancelMergeEnabled());

    MergeCancellationContext.clear();
    assertNull(MergeCancellationContext.currentMerge());
    assertFalse(MergeCancellationContext.isCancelMergeEnabled());
  }

  /** set() can carry the flag as false; the merge is still observable. */
  @Test
  public void testSetWithCancelDisabled() {
    MergePolicy.OneMerge merge = mock(MergePolicy.OneMerge.class);

    MergeCancellationContext.set(merge, false);
    assertSame(merge, MergeCancellationContext.currentMerge());
    assertFalse(MergeCancellationContext.isCancelMergeEnabled());
  }

  /** Values set on one thread are not visible on another thread. */
  @Test
  public void testThreadConfinement() throws InterruptedException {
    MergePolicy.OneMerge merge = mock(MergePolicy.OneMerge.class);
    MergeCancellationContext.set(merge, true);

    AtomicReference<MergePolicy.OneMerge> observedMerge = new AtomicReference<>();
    AtomicReference<Boolean> observedFlag = new AtomicReference<>();
    Thread other =
        new Thread(
            () -> {
              observedMerge.set(MergeCancellationContext.currentMerge());
              observedFlag.set(MergeCancellationContext.isCancelMergeEnabled());
            });
    other.start();
    other.join();

    assertNull(observedMerge.get());
    assertFalse(observedFlag.get());
  }
}
