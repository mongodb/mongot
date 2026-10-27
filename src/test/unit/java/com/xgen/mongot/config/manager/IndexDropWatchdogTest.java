package com.xgen.mongot.config.manager;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.Test;

public class IndexDropWatchdogTest {
  @Test
  public void close_beforeThreshold_disarmsWatchdog() throws Exception {
    var overdue = new AtomicLong();
    var notification = new CountDownLatch(1);
    var watchdog = new IndexDropWatchdog(Duration.ofMillis(100), overdue, notification::countDown);

    watchdog.close();

    assertThat(notification.await(200, TimeUnit.MILLISECONDS)).isFalse();
    assertThat(overdue.get()).isEqualTo(0);
  }

  @Test
  public void close_afterThreshold_acceptsCompletionAndClearsGauge() throws Exception {
    var overdue = new AtomicLong();
    var notification = new CountDownLatch(1);
    var calls = new AtomicInteger();
    try (var watchdog =
        new IndexDropWatchdog(
            Duration.ofMillis(10),
            overdue,
            () -> {
              calls.incrementAndGet();
              notification.countDown();
            })) {
      assertThat(notification.await(5, TimeUnit.SECONDS)).isTrue();
      assertThat(overdue.get()).isEqualTo(1);

      watchdog.close();

      assertThat(overdue.get()).isEqualTo(0);
      assertThat(calls.get()).isEqualTo(1);
    }
    assertThat(overdue.get()).isEqualTo(0);
  }

  @Test
  public void close_slowDiagnosticCallback_doesNotBlockCompletion() throws Exception {
    var overdue = new AtomicLong();
    var started = new CountDownLatch(1);
    var release = new CountDownLatch(1);
    var finished = new CountDownLatch(1);
    try (var watchdog =
            new IndexDropWatchdog(
                Duration.ofMillis(10),
                overdue,
                () -> {
                  started.countDown();
                  try {
                    release.await();
                  } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                  } finally {
                    finished.countDown();
                  }
                });
        var executor = Executors.newSingleThreadExecutor()) {
      try {
        assertThat(started.await(5, TimeUnit.SECONDS)).isTrue();
        executor.submit(watchdog::close).get(5, TimeUnit.SECONDS);
        assertThat(overdue.get()).isEqualTo(0);
      } finally {
        release.countDown();
      }
      assertThat(finished.await(5, TimeUnit.SECONDS)).isTrue();
    }
  }

  @Test
  public void constructor_nonPositiveThreshold_rejectsConfiguration() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new IndexDropWatchdog(Duration.ZERO, new AtomicLong(), () -> {}));
    assertThrows(
        IllegalArgumentException.class,
        () -> new IndexDropWatchdog(Duration.ofSeconds(-1), new AtomicLong(), () -> {}));
  }
}
