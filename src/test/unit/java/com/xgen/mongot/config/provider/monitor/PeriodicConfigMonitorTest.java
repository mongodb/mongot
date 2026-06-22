package com.xgen.mongot.config.provider.monitor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import com.xgen.mongot.config.updater.ConfigUpdater;
import com.xgen.mongot.config.updater.RetriableConfigUpdateException;
import com.xgen.mongot.util.Condition;
import com.xgen.mongot.util.Crash;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class PeriodicConfigMonitorTest {

  private ConfigUpdater mockUpdater;
  private PeriodicConfigMonitor monitor;

  private static final class HaltError extends Error {
    private HaltError() {
      super("test halt");
    }
  }

  @Before
  public void setUp() {
    this.mockUpdater = mock(ConfigUpdater.class);
  }

  @After
  @Crash.TestOnlyHaltHandler
  public void tearDown() {
    if (this.monitor != null) {
      this.monitor.stop();
    }
    Crash.clearHaltHandlerForTesting();
  }

  @Test
  public void testUpdateIsCalled() throws Exception {
    CountDownLatch updateCalled = new CountDownLatch(1);

    doAnswer(
            invocation -> {
              updateCalled.countDown();
              return null;
            })
        .when(this.mockUpdater)
        .update();

    this.monitor =
        PeriodicConfigMonitor.create(
            this.mockUpdater, Duration.ofMillis(100), new SimpleMeterRegistry());

    this.monitor.start();

    // Wait for at least one update to be called
    Condition.await().atMost(Duration.ofSeconds(5)).until(() -> updateCalled.getCount() == 0);

    verify(this.mockUpdater, atLeastOnce()).update();
  }

  @Test
  public void testMultipleUpdatesAreCalled() throws Exception {
    AtomicInteger callCount = new AtomicInteger(0);

    doAnswer(
            invocation -> {
              callCount.incrementAndGet();
              return null;
            })
        .when(this.mockUpdater)
        .update();

    this.monitor =
        PeriodicConfigMonitor.create(
            this.mockUpdater, Duration.ofMillis(100), new SimpleMeterRegistry());

    this.monitor.start();

    // Wait for at least 3 update cycles
    Condition.await().atMost(Duration.ofSeconds(5)).until(() -> callCount.get() >= 3);

    // Should have been called at least 3 times
    verify(this.mockUpdater, atLeast(3)).update();
  }

  @Test
  public void testStopClosesUpdater() {
    this.monitor =
        PeriodicConfigMonitor.create(
            this.mockUpdater, Duration.ofSeconds(10), new SimpleMeterRegistry());

    this.monitor.start();
    this.monitor.stop();

    verify(this.mockUpdater).close();
  }

  @Test
  @Crash.TestOnlyHaltHandler
  public void testExceptionInUpdateCausesHalt() throws Exception {
    CountDownLatch haltCalled = new CountDownLatch(1);
    AtomicInteger capturedExitCode = new AtomicInteger(-1);

    Crash.setHaltHandlerForTesting(
        exitCode -> {
          capturedExitCode.set(exitCode);
          haltCalled.countDown();
          // Simulate Runtime.halt() so the scheduler doesn't keep running.
          throw new HaltError();
        });

    doAnswer(
            invocation -> {
              throw new RuntimeException("test exception");
            })
        .when(this.mockUpdater)
        .update();

    this.monitor =
        PeriodicConfigMonitor.create(
            this.mockUpdater, Duration.ofMillis(100), new SimpleMeterRegistry());

    this.monitor.start();

    // Crash.ifThrowsExceptionOrError() catches the Exception and calls Crash.now() -> halt().
    // This is distinct from scheduleWithFixedDelay's own exception suppression, which also stops
    // rescheduling but never invokes halt().
    assertTrue(
        "halt() should be called when update() throws Exception",
        haltCalled.await(5, TimeUnit.SECONDS));
    assertEquals(1, capturedExitCode.get());
  }

  @Test
  @Crash.TestOnlyHaltHandler
  public void testErrorInUpdateCausesHalt() throws Exception {
    CountDownLatch haltCalled = new CountDownLatch(1);
    AtomicInteger capturedExitCode = new AtomicInteger(-1);

    Crash.setHaltHandlerForTesting(
        exitCode -> {
          capturedExitCode.set(exitCode);
          haltCalled.countDown();
          // Simulate Runtime.halt() so the scheduler doesn't keep running.
          throw new HaltError();
        });

    doAnswer(
            invocation -> {
              throw new Error("test error");
            })
        .when(this.mockUpdater)
        .update();

    this.monitor =
        PeriodicConfigMonitor.create(
            this.mockUpdater, Duration.ofMillis(100), new SimpleMeterRegistry());

    this.monitor.start();

    // Crash.ifThrowsExceptionOrError() catches the Error (unlike the deprecated ifThrows which
    // only catches Exception) and calls Crash.now() -> halt(). Verifying halt() is invoked proves
    // the Error reached the crash path rather than silently propagating to the scheduler.
    assertTrue(
        "halt() should be called when update() throws Error",
        haltCalled.await(5, TimeUnit.SECONDS));
    assertEquals(1, capturedExitCode.get());
  }

  @Test
  public void testRetriableExceptionDoesNotHaltAndRescheduling() throws Exception {
    // First few calls throw; later calls succeed. The monitor must keep rescheduling across
    // retriable failures rather than halting.
    AtomicInteger callCount = new AtomicInteger(0);
    doAnswer(
            invocation -> {
              if (callCount.incrementAndGet() <= 2) {
                throw new RetriableConfigUpdateException(
                    "conf call failed", new RuntimeException());
              }
              return null;
            })
        .when(this.mockUpdater)
        .update();

    this.monitor =
        PeriodicConfigMonitor.create(
            this.mockUpdater, Duration.ofMillis(10), new SimpleMeterRegistry());

    this.monitor.start();

    // After at least one success the monitor returns to normal cadence, so callCount keeps growing.
    Condition.await().atMost(Duration.ofSeconds(5)).until(() -> callCount.get() >= 5);

    verify(this.mockUpdater, atLeast(5)).update();
  }

  @Test
  public void testStopDropsPendingBackoffAttemptInsteadOfWaiting() throws Exception {
    // With a one-hour period, the first failure schedules the next attempt ~one hour out. stop()
    // must drop that pending delayed task rather than block until it would have fired, so it should
    // return in well under the backoff delay.
    doThrow(new RetriableConfigUpdateException("boom", new RuntimeException()))
        .when(this.mockUpdater)
        .update();

    this.monitor =
        PeriodicConfigMonitor.create(
            this.mockUpdater, Duration.ofHours(1), new SimpleMeterRegistry());

    this.monitor.start();
    // Let the first failure happen so a far-out backoff attempt is queued.
    Condition.await()
        .atMost(Duration.ofSeconds(5))
        .until(() -> Mockito.mockingDetails(this.mockUpdater).getInvocations().size() >= 1);

    long beforeStop = System.nanoTime();
    this.monitor.stop();
    long elapsedMs = (System.nanoTime() - beforeStop) / 1_000_000L;

    assertTrue("stop() unexpectedly took " + elapsedMs + "ms", elapsedMs < 2_000L);
    verify(this.mockUpdater).close();
  }
}
