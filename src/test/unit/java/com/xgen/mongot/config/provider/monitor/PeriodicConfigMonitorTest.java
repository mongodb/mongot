package com.xgen.mongot.config.provider.monitor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import com.xgen.mongot.config.updater.ConfigUpdater;
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
    Condition.await()
        .atMost(Duration.ofSeconds(5))
        .until(() -> updateCalled.getCount() == 0);

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
    Condition.await()
        .atMost(Duration.ofSeconds(5))
        .until(() -> callCount.get() >= 3);

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
  public void testExceptionInUpdateCausesHalt() throws InterruptedException {
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
  public void testErrorInUpdateCausesHalt() throws InterruptedException {
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
}
