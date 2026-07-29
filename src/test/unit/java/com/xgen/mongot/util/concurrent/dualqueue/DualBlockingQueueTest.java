package com.xgen.mongot.util.concurrent.dualqueue;

import static com.google.common.truth.Truth.assertThat;

import com.xgen.mongot.metrics.MetricsFactory;
import com.xgen.mongot.util.concurrent.PrioritizedTask;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Assert;
import org.junit.Test;

public class DualBlockingQueueTest {

  private static final String QUEUE_NAME = "test-queue";

  private static MetricsFactory metricsFactory() {
    return new MetricsFactory(QUEUE_NAME, new SimpleMeterRegistry());
  }

  private static PrioritizedTask task(boolean isHighPriority) {
    return new PrioritizedTask(() -> {}, isHighPriority);
  }

  @Test
  public void testTake_returnsTaskFromHighPriorityQueue() throws InterruptedException {
    DualBlockingQueue queue = new DualBlockingQueue(4, metricsFactory());
    PrioritizedTask task = task(true);
    queue.offer(task);

    assertThat(queue.take()).isEqualTo(task);
  }

  @Test
  public void testTake_returnsTaskFromLowPriorityQueue() throws InterruptedException {
    DualBlockingQueue queue = new DualBlockingQueue(4, metricsFactory());
    PrioritizedTask task = task(false);
    queue.offer(task);

    assertThat(queue.take()).isEqualTo(task);
  }

  @Test
  public void testPoll_returnsNullWhenEmpty() {
    DualBlockingQueue queue = new DualBlockingQueue(4, metricsFactory());

    assertThat(queue.poll()).isNull();
  }

  @Test
  public void testPoll_returnsTaskWhenAvailable() {
    DualBlockingQueue queue = new DualBlockingQueue(4, metricsFactory());
    PrioritizedTask task = task(true);
    queue.offer(task);

    assertThat(queue.poll()).isEqualTo(task);
  }

  @Test
  public void testPollWithTimeout_returnsNullAfterTimeout() throws InterruptedException {
    DualBlockingQueue queue = new DualBlockingQueue(4, metricsFactory());

    assertThat(queue.poll(50, TimeUnit.MILLISECONDS)).isNull();
  }

  @Test
  public void testRatio_servesHighPriorityTasksBeforeLowPriority() throws InterruptedException {
    DualBlockingQueue queue = new DualBlockingQueue(2, metricsFactory());
    PrioritizedTask high1 = task(true);
    PrioritizedTask high2 = task(true);
    PrioritizedTask low1 = task(false);
    queue.offer(high1);
    queue.offer(high2);
    queue.offer(low1);

    assertThat(queue.take()).isEqualTo(high1);
    assertThat(queue.take()).isEqualTo(high2);
    assertThat(queue.take()).isEqualTo(low1);
  }

  @Test
  public void testFallback_servesLowPriorityWhenHighPriorityQueueEmpty()
      throws InterruptedException {
    DualBlockingQueue queue = new DualBlockingQueue(4, metricsFactory());
    PrioritizedTask low1 = task(false);
    queue.offer(low1);

    assertThat(queue.take()).isEqualTo(low1);
  }

  @Test
  public void testFallback_servesHighPriorityWhenLowPriorityQueueEmpty()
      throws InterruptedException {
    DualBlockingQueue queue = new DualBlockingQueue(1, metricsFactory());
    PrioritizedTask high1 = task(true);
    PrioritizedTask high2 = task(true);
    queue.offer(high1);
    queue.offer(high2);

    assertThat(queue.take()).isEqualTo(high1);
    assertThat(queue.take()).isEqualTo(high2);
  }

  @Test
  public void testSize_reflectsBothQueues() {
    DualBlockingQueue queue = new DualBlockingQueue(4, metricsFactory());
    queue.offer(task(true));
    queue.offer(task(false));
    queue.offer(task(true));

    assertThat(queue.size()).isEqualTo(3);
  }

  @Test
  public void testDrainTo_drainsAllTasks() {
    DualBlockingQueue queue = new DualBlockingQueue(4, metricsFactory());
    queue.offer(task(true));
    queue.offer(task(false));
    queue.offer(task(true));

    List<Runnable> drained = new ArrayList<>();
    int count = queue.drainTo(drained);

    assertThat(count).isEqualTo(3);
    assertThat(drained).hasSize(3);
    assertThat(queue.size()).isEqualTo(0);
  }

  @Test
  public void testDrainTo_respectsMaxElements() {
    DualBlockingQueue queue = new DualBlockingQueue(4, metricsFactory());
    queue.offer(task(true));
    queue.offer(task(true));
    queue.offer(task(true));

    List<Runnable> drained = new ArrayList<>();
    int count = queue.drainTo(drained, 2);

    assertThat(count).isEqualTo(2);
    assertThat(drained).hasSize(2);
    assertThat(queue.size()).isEqualTo(1);
  }

  @Test
  public void testPrioritizedTask_runDelegatesToRunnable() {
    AtomicReference<Boolean> ran = new AtomicReference<>(false);
    PrioritizedTask task = new PrioritizedTask(() -> ran.set(true), true);
    task.run();

    assertThat(ran.get()).isTrue();
  }

  @Test
  public void testConstructor_rejectsNonPositiveRatio() {
    Assert.assertThrows(
        IllegalArgumentException.class, () -> new DualBlockingQueue(0, metricsFactory()));
    Assert.assertThrows(
        IllegalArgumentException.class, () -> new DualBlockingQueue(-1, metricsFactory()));
  }

  @Test
  public void testConstructor_rejectsNullMetricsFactory() {
    Assert.assertThrows(NullPointerException.class, () -> new DualBlockingQueue(4, null));
  }

  @Test
  public void testOffer_rejectsNull() {
    DualBlockingQueue queue = new DualBlockingQueue(4, metricsFactory());

    Assert.assertThrows(NullPointerException.class, () -> queue.offer(null));
  }

  @Test
  public void testOffer_rejectsNonPrioritizedTask() {
    // The PrioritizedTask-only contract (TODO(CLOUDP-410283)) is enforced, not conventional: a raw
    // Runnable is rejected and nothing leaks into either queue.
    DualBlockingQueue queue = new DualBlockingQueue(4, metricsFactory());

    Assert.assertThrows(IllegalArgumentException.class, () -> queue.offer(() -> {}));
    assertThat(queue.size()).isEqualTo(0);
    assertThat(queue.peek()).isNull();
  }

  @Test
  public void testIterator_removeIsUnsupported() {
    // iterator() returns a snapshot; remove() would only mutate the copy, so it throws rather than
    // silently no-op'ing on the backing queues. Removal goes through remove(Object) instead.
    DualBlockingQueue queue = new DualBlockingQueue(4, metricsFactory());
    queue.offer(task(true));
    queue.offer(task(false));

    var it = queue.iterator();
    assertThat(it.hasNext()).isTrue();
    it.next();
    Assert.assertThrows(UnsupportedOperationException.class, it::remove);

    assertThat(queue.size()).isEqualTo(2);
  }

  @Test
  public void testTake_blocksUntilTaskAvailable() throws InterruptedException {
    DualBlockingQueue queue = new DualBlockingQueue(4, metricsFactory());
    PrioritizedTask task = task(true);
    CountDownLatch takeStarted = new CountDownLatch(1);
    AtomicReference<Runnable> taken = new AtomicReference<>();

    Thread consumer =
        new Thread(
            () -> {
              try {
                takeStarted.countDown();
                taken.set(queue.take());
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
              }
            });
    consumer.start();

    // Wait until the consumer thread has started, then confirm it is blocked (no task yet)
    takeStarted.await();
    Thread.sleep(50);
    assertThat(taken.get()).isNull();

    // Offering a task should unblock take()
    queue.offer(task);
    consumer.join(1000);
    assertThat(taken.get()).isEqualTo(task);
  }

  @Test
  public void testPollWithTimeout_wakesWhenTaskOfferedBeforeTimeout() throws InterruptedException {
    DualBlockingQueue queue = new DualBlockingQueue(4, metricsFactory());
    PrioritizedTask task = task(true);
    CountDownLatch pollStarted = new CountDownLatch(1);
    AtomicReference<Runnable> polled = new AtomicReference<>();

    Thread consumer =
        new Thread(
            () -> {
              try {
                pollStarted.countDown();
                polled.set(queue.poll(5, TimeUnit.SECONDS));
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
              }
            });
    consumer.start();

    pollStarted.await();
    queue.offer(task);
    consumer.join(2000);
    assertThat(polled.get()).isEqualTo(task);
  }

  @Test
  public void testRemove_removesFromBothQueues() {
    DualBlockingQueue queue = new DualBlockingQueue(4, metricsFactory());
    PrioritizedTask high = task(true);
    PrioritizedTask low = task(false);
    queue.offer(high);
    queue.offer(low);

    assertThat(queue.remove(high)).isTrue();
    assertThat(queue.remove(low)).isTrue();
    assertThat(queue.size()).isEqualTo(0);
    // Removing something not present returns false
    assertThat(queue.remove(task(true))).isFalse();
  }

  @Test
  public void testPeek_returnsNextTaskWithoutRemoving() {
    DualBlockingQueue queue = new DualBlockingQueue(4, metricsFactory());
    PrioritizedTask high = task(true);
    PrioritizedTask low = task(false);
    queue.offer(high);
    queue.offer(low);

    // peek() returns the next preferred task and does not remove it
    assertThat(queue.peek()).isEqualTo(high);
    assertThat(queue.size()).isEqualTo(2);
    assertThat(queue.peek()).isEqualTo(high);
  }

  @Test
  public void testPeek_returnsNullWhenEmpty() {
    DualBlockingQueue queue = new DualBlockingQueue(4, metricsFactory());

    assertThat(queue.peek()).isNull();
  }

  @Test
  public void testOfferWithTimeout_succeedsImmediately() throws InterruptedException {
    // The queue is unbounded, so the timed offer never blocks: it returns true at once and enqueues
    // the task regardless of the timeout.
    DualBlockingQueue queue = new DualBlockingQueue(4, metricsFactory());
    PrioritizedTask task = task(true);

    assertThat(queue.offer(task, 1, TimeUnit.HOURS)).isTrue();
    assertThat(queue.size()).isEqualTo(1);
    assertThat(queue.take()).isEqualTo(task);
  }

  @Test
  public void testPeek_matchesNextServedTaskWhenRatioReached() throws InterruptedException {
    // With ratio 1, after serving one preferred task the next served task is the regular one;
    // peek() must report that same task (mirroring selectNext()).
    DualBlockingQueue queue = new DualBlockingQueue(1, metricsFactory());
    PrioritizedTask high = task(true);
    PrioritizedTask low = task(false);
    queue.offer(high);
    queue.offer(low);

    assertThat(queue.take()).isEqualTo(high); // ratio reached
    assertThat(queue.peek()).isEqualTo(low); // regular is served next
    assertThat(queue.take()).isEqualTo(low);
  }

  @Test
  public void testOffer_incrementsRoutingCounters() {
    SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
    DualBlockingQueue queue =
        new DualBlockingQueue(4, new MetricsFactory(QUEUE_NAME, meterRegistry));

    queue.offer(task(true));
    queue.offer(task(false));
    queue.offer(task(true));

    assertThat(meterRegistry.get("test-queue.tasksRoutedToPreferredQueue").counter().count())
        .isEqualTo(2.0);
    assertThat(meterRegistry.get("test-queue.tasksRoutedToRegularQueue").counter().count())
        .isEqualTo(1.0);
  }

  @Test
  public void testSelectNext_incrementsServedCounters() throws InterruptedException {
    // Ratio of 2: with 2 preferred + 1 regular queued, both preferred are served (2) before the
    // ratio triggers a regular task (1).
    SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
    DualBlockingQueue queue =
        new DualBlockingQueue(2, new MetricsFactory(QUEUE_NAME, meterRegistry));
    queue.offer(task(true));
    queue.offer(task(true));
    queue.offer(task(false));

    queue.take();
    queue.take();
    queue.take();

    assertThat(meterRegistry.get("test-queue.preferredQueueTasksServed").counter().count())
        .isEqualTo(2.0);
    assertThat(meterRegistry.get("test-queue.regularQueueTasksServed").counter().count())
        .isEqualTo(1.0);
  }

  @Test
  public void testGauges_reflectPerPriorityQueueLength() {
    SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
    DualBlockingQueue queue =
        new DualBlockingQueue(4, new MetricsFactory(QUEUE_NAME, meterRegistry));

    queue.offer(task(true));
    queue.offer(task(false));
    queue.offer(task(true));

    assertThat(meterRegistry.get("test-queue.preferredQueueLength").gauge().value()).isEqualTo(2.0);
    assertThat(meterRegistry.get("test-queue.regularQueueLength").gauge().value()).isEqualTo(1.0);

    queue.poll();

    assertThat(meterRegistry.get("test-queue.preferredQueueLength").gauge().value()).isEqualTo(1.0);
    assertThat(meterRegistry.get("test-queue.regularQueueLength").gauge().value()).isEqualTo(1.0);
  }

  @Test
  public void testRatio_regularEmptyAtBoundary_resetsCounterAndServesPreferred()
      throws InterruptedException {
    // When the ratio is reached but the regular queue is empty, the
    // counter resets and a preferred task is served. Because the counter reset, a regular task
    // offered just afterwards waits behind another preferred task rather than being served next.
    DualBlockingQueue queue = new DualBlockingQueue(2, metricsFactory());
    PrioritizedTask high1 = task(true);
    PrioritizedTask high2 = task(true);
    PrioritizedTask high3 = task(true);
    PrioritizedTask high4 = task(true);
    queue.offer(high1);
    queue.offer(high2);
    queue.offer(high3);
    queue.offer(high4);

    assertThat(queue.take()).isEqualTo(high1);
    assertThat(queue.take()).isEqualTo(high2);

    assertThat(queue.take()).isEqualTo(high3);

    PrioritizedTask low = task(false);
    queue.offer(low);
    assertThat(queue.take()).isEqualTo(high4);
    assertThat(queue.take()).isEqualTo(low);
  }
}
