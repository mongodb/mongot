package com.xgen.mongot.embedding.providers.congestion;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.errorprone.annotations.Var;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Test;

public class DynamicSemaphoreTest {

  @Test
  public void acquire_blocksWhenNoPermitsAvailable() throws InterruptedException {
    DynamicSemaphorePolicy policy =
        new DynamicSemaphorePolicy() {
          @Override
          public int onAcquire() {
            return 1;
          }

          @Override
          public int onRelease(boolean isAck) {
            return 1;
          }

          @Override
          public int getTotalPermits() {
            return 1;
          }
        };

    DynamicSemaphore semaphore = new DynamicSemaphore(policy);

    // First acquire should succeed
    semaphore.acquire();
    assertEquals(1, semaphore.getUsedPermits());

    // Start a thread that will try to acquire (and should block)
    AtomicInteger acquireCount = new AtomicInteger(0);
    CountDownLatch startedLatch = new CountDownLatch(1);
    Thread blockedThread =
        new Thread(
            () -> {
              try {
                startedLatch.countDown();
                semaphore.acquire();
                acquireCount.incrementAndGet();
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
              }
            });
    blockedThread.start();

    // Wait for thread to start
    startedLatch.await();
    Thread.sleep(100); // Give some time for thread to attempt acquire

    // The blocked thread should not have acquired yet
    assertEquals(0, acquireCount.get());

    // Release the permit
    semaphore.release(true);

    // Wait for blocked thread to complete
    blockedThread.join(1000);
    assertEquals(1, acquireCount.get());
  }

  @Test
  public void release_signalsWaitingThreads() throws InterruptedException {
    AimdCongestionControl policy = new AimdCongestionControl();
    DynamicSemaphore semaphore = new DynamicSemaphore(policy);

    int numThreads = 5;
    CountDownLatch allStarted = new CountDownLatch(numThreads);
    CountDownLatch allCompleted = new CountDownLatch(numThreads);
    AtomicInteger maxConcurrent = new AtomicInteger(0);
    AtomicInteger currentConcurrent = new AtomicInteger(0);

    ExecutorService executor = Executors.newFixedThreadPool(numThreads);

    for (int i = 0; i < numThreads; i++) {
      executor.submit(
          () -> {
            try {
              allStarted.countDown();
              semaphore.acquire();
              int concurrent = currentConcurrent.incrementAndGet();
              maxConcurrent.updateAndGet(max -> Math.max(max, concurrent));

              Thread.sleep(10); // Simulate work

              currentConcurrent.decrementAndGet();
              semaphore.release(true);
              allCompleted.countDown();
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
            }
          });
    }

    allStarted.await();
    assertTrue(allCompleted.await(5, TimeUnit.SECONDS));

    // With AIMD starting at cwnd=1, initial concurrency should be limited
    // As window grows, more concurrent access is allowed
    assertTrue(
        "Max concurrent should be bounded by semaphore, was: " + maxConcurrent.get(),
        maxConcurrent.get() <= numThreads);

    executor.shutdown();
  }

  @Test
  public void tryAcquire_returnsNegativeOneWhenNoPermits() {
    DynamicSemaphorePolicy policy =
        new DynamicSemaphorePolicy() {
          @Override
          public int onAcquire() {
            return 1;
          }

          @Override
          public int onRelease(boolean isAck) {
            return 1;
          }

          @Override
          public int getTotalPermits() {
            return 1;
          }
        };

    DynamicSemaphore semaphore = new DynamicSemaphore(policy);

    // First tryAcquire should succeed
    @Var int result = semaphore.tryAcquire();
    assertEquals(1, result);

    // Second tryAcquire should fail (returns -1)
    result = semaphore.tryAcquire();
    assertEquals(-1, result);
  }

  @Test
  public void release_updatesPermitsBasedOnPolicy() {
    AtomicInteger permits = new AtomicInteger(2);
    DynamicSemaphorePolicy policy =
        new DynamicSemaphorePolicy() {
          @Override
          public int onAcquire() {
            return permits.get();
          }

          @Override
          public int onRelease(boolean isAck) {
            // Increase permits on ACK, decrease on congestion
            if (isAck) {
              return permits.incrementAndGet();
            } else {
              return permits.updateAndGet(p -> Math.max(1, p - 1));
            }
          }

          @Override
          public int getTotalPermits() {
            return permits.get();
          }
        };

    DynamicSemaphore semaphore = new DynamicSemaphore(policy);

    // Initial state: 2 permits
    assertEquals(2, semaphore.getTotalPermits());

    // Acquire and release with ACK
    try {
      semaphore.acquire();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    @Var DynamicSemaphore.ReleaseResult result = semaphore.release(true);

    // After ACK: permits should increase
    assertEquals(3, result.getTotalPermits());
    assertEquals(0, result.getUsedPermits());

    // Acquire and release with congestion signal
    try {
      semaphore.acquire();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    result = semaphore.release(false);

    // After congestion: permits should decrease
    assertEquals(2, result.getTotalPermits());
    assertEquals(0, result.getUsedPermits());
  }

  @Test
  public void getAvailablePermits_returnsCorrectValue() {
    AimdCongestionControl policy = AimdCongestionControl.builder().initialCwnd(3).build();
    DynamicSemaphore semaphore = new DynamicSemaphore(policy);

    assertEquals(3, semaphore.getAvailablePermits());

    try {
      semaphore.acquire();
      assertEquals(2, semaphore.getAvailablePermits());

      semaphore.acquire();
      assertEquals(1, semaphore.getAvailablePermits());

      semaphore.acquire();
      assertEquals(0, semaphore.getAvailablePermits());

      semaphore.release(true);
      assertTrue(semaphore.getAvailablePermits() > 0);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  @Test
  public void integrationWithAimd_windowGrowsOnSuccess() throws InterruptedException {
    AimdCongestionControl aimd = new AimdCongestionControl();
    DynamicSemaphore semaphore = new DynamicSemaphore(aimd);

    // Initial window should be 1
    assertEquals(1, semaphore.getTotalPermits());

    // Simulate successful requests
    for (int i = 0; i < 5; i++) {
      semaphore.acquire();
      semaphore.release(true); // ACK
    }

    // Window should have grown
    assertTrue("Window should grow after successful requests", semaphore.getTotalPermits() > 1);
  }

  @Test
  public void integrationWithAimd_windowShrinksOnCongestion() throws InterruptedException {
    // Start with larger window, disable slow start for predictable test
    AimdCongestionControl aimd =
        AimdCongestionControl.builder()
            .initialCwnd(1)
            .slowStartThreshold(0) // Disable slow start so window doesn't grow much
            .multiplicativeDecrease(0.5)
            .build();
    DynamicSemaphore semaphore = new DynamicSemaphore(aimd);

    assertEquals(1, semaphore.getTotalPermits());

    // AIMD requires sequence number >= cwnd before first decrease.
    // Simulate enough successful requests first.
    for (int i = 0; i < 10; i++) {
      semaphore.acquire();
      semaphore.release(true); // ACK
    }

    int permitsBeforeCongestion = semaphore.getTotalPermits();

    // Simulate congestion event
    semaphore.acquire();
    semaphore.release(false); // Congestion signal

    // Window should have shrunk
    assertTrue(
        "Window should shrink after congestion, before="
            + permitsBeforeCongestion
            + ", after="
            + semaphore.getTotalPermits(),
        semaphore.getTotalPermits() < permitsBeforeCongestion);
  }
}
