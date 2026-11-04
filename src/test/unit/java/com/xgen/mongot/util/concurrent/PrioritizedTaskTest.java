package com.xgen.mongot.util.concurrent;

import static com.google.common.truth.Truth.assertThat;

import java.util.concurrent.atomic.AtomicReference;
import org.junit.Assert;
import org.junit.Test;

public class PrioritizedTaskTest {

  @Test
  public void testConstructor_rejectsNullRunnable() {
    Assert.assertThrows(NullPointerException.class, () -> new PrioritizedTask(null, true));
  }

  @Test
  public void testRun_delegatesToRunnable() {
    AtomicReference<Boolean> ran = new AtomicReference<>(false);
    PrioritizedTask task = new PrioritizedTask(() -> ran.set(true), true);

    task.run();

    assertThat(ran.get()).isTrue();
  }

  @Test
  public void testIsHighPriority_reflectsConstructorArgument() {
    assertThat(new PrioritizedTask(() -> {}, true).isHighPriority()).isTrue();
    assertThat(new PrioritizedTask(() -> {}, false).isHighPriority()).isFalse();
  }

  @Test
  public void testCreatedNanos_defaultsToCurrentNanoTime() {
    long before = System.nanoTime();
    PrioritizedTask task = new PrioritizedTask(() -> {}, true);
    long after = System.nanoTime();

    assertThat(task.createdNanos()).isAtLeast(before);
    assertThat(task.createdNanos()).isAtMost(after);
  }
}
