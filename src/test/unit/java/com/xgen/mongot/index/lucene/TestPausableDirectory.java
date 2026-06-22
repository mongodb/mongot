package com.xgen.mongot.index.lucene;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.xgen.mongot.index.lucene.merge.PausableDirectory;
import com.xgen.mongot.monitor.Gate;
import com.xgen.mongot.monitor.ToggleGate;
import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.junit.Test;

/** Unit tests for {@link PausableDirectory}. */
public class TestPausableDirectory {

  /** Test that PausableDirectory allows normal writes when gate is open (disk usage is low). */
  @Test
  public void testWriteBytes_gateOpen_writesImmediately() throws IOException {
    Gate openGate = ToggleGate.opened();
    Directory baseDir = new ByteBuffersDirectory();
    PausableDirectory pausableDir = new PausableDirectory(baseDir, openGate);

    IndexOutput output = pausableDir.createOutput("test.dat", IOContext.DEFAULT);

    long bytesToWrite = PausableDirectory.CHECK_PAUSE_INTERVAL_BYTES * 2;
    byte[] largeData = new byte[(int) bytesToWrite];
    for (int i = 0; i < largeData.length; i++) {
      largeData[i] = (byte) (i % 256);
    }

    output.writeBytes(largeData, 0, largeData.length);
    output.close();

    assertEquals(bytesToWrite, baseDir.fileLength("test.dat"));
    pausableDir.close();
  }

  /**
   * Test that PausableDirectory blocks writes when gate is closed, then resumes when gate opens.
   */
  @Test
  public void testWriteBytes_gateClosed_blocksUntilOpen() throws Exception {
    ToggleGate gate = ToggleGate.closed();
    Directory baseDir = new ByteBuffersDirectory();
    PausableDirectory pausableDir = new PausableDirectory(baseDir, gate);

    IndexOutput output = pausableDir.createOutput("test.dat", IOContext.DEFAULT);

    AtomicBoolean writeCompleted = new AtomicBoolean(false);
    AtomicReference<Exception> writeException = new AtomicReference<>();
    CountDownLatch writeStarted = new CountDownLatch(1);

    ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      executor.submit(
          () -> {
            try {
              byte[] largeData = new byte[(int) PausableDirectory.CHECK_PAUSE_INTERVAL_BYTES];
              writeStarted.countDown();
              output.writeBytes(largeData, 0, largeData.length);
              writeCompleted.set(true);
            } catch (Exception e) {
              writeException.set(e);
            }
          });

      assertTrue("Write should have started", writeStarted.await(5, TimeUnit.SECONDS));
      Thread.sleep(100);
      assertTrue("Write should be blocked when gate is closed", !writeCompleted.get());

      gate.open();

      executor.shutdown();
      assertTrue(
          "Write should complete after gate opens", executor.awaitTermination(5, TimeUnit.SECONDS));

      if (writeException.get() != null) {
        throw writeException.get();
      }
      assertTrue("Write should have completed", writeCompleted.get());
    } finally {
      gate.open();
      executor.shutdownNow();
      executor.awaitTermination(5, TimeUnit.SECONDS);
    }

    output.close();
    pausableDir.close();
  }

  /** Test that PausableDirectory throws IOException when thread is interrupted while waiting. */
  @Test
  public void testWriteBytes_interruptedWhileWaiting_throwsIoException() throws Exception {
    ToggleGate gate = ToggleGate.closed();
    Directory baseDir = new ByteBuffersDirectory();
    PausableDirectory pausableDir = new PausableDirectory(baseDir, gate);

    IndexOutput output = pausableDir.createOutput("test.dat", IOContext.DEFAULT);

    AtomicReference<Exception> caughtException = new AtomicReference<>();
    CountDownLatch writeStarted = new CountDownLatch(1);

    Thread writeThread =
        new Thread(
            () -> {
              try {
                byte[] largeData = new byte[(int) PausableDirectory.CHECK_PAUSE_INTERVAL_BYTES];
                writeStarted.countDown();
                output.writeBytes(largeData, 0, largeData.length);
              } catch (Exception e) {
                caughtException.set(e);
              }
            });
    writeThread.start();

    assertTrue("Write should have started", writeStarted.await(5, TimeUnit.SECONDS));
    Thread.sleep(100);

    writeThread.interrupt();
    writeThread.join(5000);

    assertTrue("Should have caught an exception", caughtException.get() != null);
    assertTrue("Exception should be IOException", caughtException.get() instanceof IOException);
    assertTrue(
        "Exception message should mention interruption",
        caughtException.get().getMessage().contains("Interrupted"));

    output.close();
    pausableDir.close();
  }

  /** Test that PausableDirectory only checks gate status every CHECK_PAUSE_INTERVAL_BYTES. */
  @Test
  public void testCheckInterval_onlyChecksEvery256KB() throws IOException {
    ToggleGate gate = ToggleGate.opened();
    Directory baseDir = new ByteBuffersDirectory();
    PausableDirectory pausableDir = new PausableDirectory(baseDir, gate);

    IndexOutput output = pausableDir.createOutput("test.dat", IOContext.DEFAULT);

    int smallWriteSize = (int) (PausableDirectory.CHECK_PAUSE_INTERVAL_BYTES / 2);
    byte[] smallData = new byte[smallWriteSize];
    output.writeBytes(smallData, 0, smallData.length);

    gate.close();

    int remainingBytes = (int) (PausableDirectory.CHECK_PAUSE_INTERVAL_BYTES / 4);
    byte[] moreData = new byte[remainingBytes];
    output.writeBytes(moreData, 0, moreData.length);

    assertEquals(smallWriteSize + remainingBytes, output.getFilePointer());

    output.close();
    pausableDir.close();
  }

  /** Test that PausableDirectory works with createTempOutput as well. */
  @Test
  public void testPauseDuringTempOutput() throws Exception {
    ToggleGate gate = ToggleGate.closed();
    Directory baseDir = new ByteBuffersDirectory();
    PausableDirectory pausableDir = new PausableDirectory(baseDir, gate);

    IndexOutput output = pausableDir.createTempOutput("test", ".tmp", IOContext.DEFAULT);

    AtomicBoolean writeCompleted = new AtomicBoolean(false);
    CountDownLatch writeStarted = new CountDownLatch(1);

    ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      executor.submit(
          () -> {
            try {
              byte[] largeData = new byte[(int) PausableDirectory.CHECK_PAUSE_INTERVAL_BYTES];
              writeStarted.countDown();
              output.writeBytes(largeData, 0, largeData.length);
              writeCompleted.set(true);
            } catch (Exception e) {
              // Ignore
            }
          });

      assertTrue("Write should have started", writeStarted.await(5, TimeUnit.SECONDS));
      Thread.sleep(100);
      assertTrue("Write should be blocked when gate is closed", !writeCompleted.get());

      gate.open();

      executor.shutdown();
      assertTrue(
          "Write should complete after gate opens", executor.awaitTermination(5, TimeUnit.SECONDS));
      assertTrue("Write should have completed", writeCompleted.get());
    } finally {
      gate.open();
      executor.shutdownNow();
      executor.awaitTermination(5, TimeUnit.SECONDS);
    }

    output.close();
    pausableDir.close();
  }

  /**
   * Test that different write methods (writeByte, writeInt, writeLong, etc.) all check for pause.
   */
  @Test
  public void testPauseCheckOnDifferentWriteMethods() throws IOException {
    Gate openGate = ToggleGate.opened();
    Directory baseDir = new ByteBuffersDirectory();
    PausableDirectory pausableDir = new PausableDirectory(baseDir, openGate);

    IndexOutput output = pausableDir.createOutput("test.dat", IOContext.DEFAULT);

    output.writeByte((byte) 1);
    output.writeShort((short) 2);
    output.writeInt(3);
    output.writeLong(4L);

    byte[] data = new byte[(int) PausableDirectory.CHECK_PAUSE_INTERVAL_BYTES];
    output.writeBytes(data, 0, data.length);

    output.close();

    long expectedSize =
        1 + Short.BYTES + Integer.BYTES + Long.BYTES + PausableDirectory.CHECK_PAUSE_INTERVAL_BYTES;
    assertEquals(expectedSize, baseDir.fileLength("test.dat"));

    pausableDir.close();
  }

  /** Test that PausableDirectory properly delegates getFilePointer and getChecksum. */
  @Test
  public void testDelegationMethods() throws IOException {
    Gate openGate = ToggleGate.opened();
    Directory baseDir = new ByteBuffersDirectory();
    PausableDirectory pausableDir = new PausableDirectory(baseDir, openGate);

    IndexOutput output = pausableDir.createOutput("test.dat", IOContext.DEFAULT);

    assertEquals(0, output.getFilePointer());

    byte[] data = new byte[100];
    output.writeBytes(data, 0, data.length);

    assertEquals(100, output.getFilePointer());

    long checksum = output.getChecksum();
    assertTrue("Checksum should be non-negative", checksum >= 0);

    output.close();
    pausableDir.close();
  }

  /**
   * Test that the onPause callback is not invoked when the gate is open and writes proceed without
   * blocking.
   */
  @Test
  public void testOnPauseCallback_gateOpen_notInvoked() throws IOException {
    Gate openGate = ToggleGate.opened();
    Directory baseDir = new ByteBuffersDirectory();
    AtomicInteger pauseCount = new AtomicInteger(0);
    PausableDirectory pausableDir =
        new PausableDirectory(baseDir, openGate, pauseCount::incrementAndGet);

    IndexOutput output = pausableDir.createOutput("test.dat", IOContext.DEFAULT);

    byte[] data = new byte[(int) (PausableDirectory.CHECK_PAUSE_INTERVAL_BYTES * 3)];
    output.writeBytes(data, 0, data.length);
    output.close();

    assertEquals(0, pauseCount.get());
    pausableDir.close();
  }

  /**
   * Test that the onPause callback is invoked exactly once when a write hits a closed gate and
   * resumes after the gate opens.
   */
  @Test
  public void testOnPauseCallback_gateClosed_invokedOncePerPauseEvent() throws Exception {
    ToggleGate gate = ToggleGate.closed();
    Directory baseDir = new ByteBuffersDirectory();
    AtomicInteger pauseCount = new AtomicInteger(0);
    PausableDirectory pausableDir =
        new PausableDirectory(baseDir, gate, pauseCount::incrementAndGet);

    IndexOutput output = pausableDir.createOutput("test.dat", IOContext.DEFAULT);

    AtomicReference<Exception> writeException = new AtomicReference<>();
    CountDownLatch writeStarted = new CountDownLatch(1);

    ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      executor.submit(
          () -> {
            try {
              byte[] largeData = new byte[(int) PausableDirectory.CHECK_PAUSE_INTERVAL_BYTES];
              writeStarted.countDown();
              output.writeBytes(largeData, 0, largeData.length);
            } catch (Exception e) {
              writeException.set(e);
            }
          });

      assertTrue("Write should have started", writeStarted.await(5, TimeUnit.SECONDS));
      assertTrue(
          "onPause should have fired exactly once",
          pollUntil(() -> pauseCount.get() >= 1, Duration.ofSeconds(5)));
      assertEquals("onPause should have fired exactly once", 1, pauseCount.get());

      gate.open();
      executor.shutdown();
      assertTrue(
          "Write should complete after gate opens", executor.awaitTermination(5, TimeUnit.SECONDS));

      if (writeException.get() != null) {
        throw writeException.get();
      }
      assertEquals("onPause should not fire again once gate is open", 1, pauseCount.get());
    } finally {
      gate.open();
      executor.shutdownNow();
      executor.awaitTermination(5, TimeUnit.SECONDS);
    }

    output.close();
    pausableDir.close();
  }

  /**
   * Test that two outputs from the same PausableDirectory share deduplication: when both hit a
   * closed gate during the same gate-close event, onPause fires only once, not once per output.
   */
  @Test
  public void testOnPauseCallback_multipleOutputs_singleGateClose_invokedOnce() throws Exception {
    ToggleGate gate = ToggleGate.closed();
    Directory baseDir = new ByteBuffersDirectory();
    AtomicInteger pauseCount = new AtomicInteger(0);
    PausableDirectory pausableDir =
        new PausableDirectory(baseDir, gate, pauseCount::incrementAndGet);

    IndexOutput output1 = pausableDir.createOutput("file1.dat", IOContext.DEFAULT);
    IndexOutput output2 = pausableDir.createOutput("file2.dat", IOContext.DEFAULT);

    AtomicReference<Exception> ex1 = new AtomicReference<>();
    AtomicReference<Exception> ex2 = new AtomicReference<>();
    CountDownLatch bothStarted = new CountDownLatch(2);

    ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      executor.submit(
          () -> {
            try {
              byte[] data = new byte[(int) PausableDirectory.CHECK_PAUSE_INTERVAL_BYTES];
              bothStarted.countDown();
              output1.writeBytes(data, 0, data.length);
            } catch (Exception e) {
              ex1.set(e);
            }
          });
      executor.submit(
          () -> {
            try {
              byte[] data = new byte[(int) PausableDirectory.CHECK_PAUSE_INTERVAL_BYTES];
              bothStarted.countDown();
              output2.writeBytes(data, 0, data.length);
            } catch (Exception e) {
              ex2.set(e);
            }
          });

      assertTrue("Both writes should have started", bothStarted.await(5, TimeUnit.SECONDS));
      assertTrue(
          "onPause should fire at least once after both writes hit the closed gate",
          pollUntil(() -> pauseCount.get() >= 1, Duration.ofSeconds(5)));

      assertEquals(
          "onPause should fire exactly once for one gate-close event", 1, pauseCount.get());

      gate.open();
      executor.shutdown();
      assertTrue(
          "Both writes should complete after gate opens",
          executor.awaitTermination(5, TimeUnit.SECONDS));

      if (ex1.get() != null) {
        throw ex1.get();
      }
      if (ex2.get() != null) {
        throw ex2.get();
      }

      assertEquals("onPause should not fire again after gate opens", 1, pauseCount.get());
    } finally {
      gate.open();
      executor.shutdownNow();
      executor.awaitTermination(5, TimeUnit.SECONDS);
    }

    output1.close();
    output2.close();
    pausableDir.close();
  }

  /**
   * Test that after a pause window completes (gate opens, write resumes), a subsequent gate-close
   * event fires the onPause callback again. Locks down the {@code onResume} latch-reset contract.
   */
  @Test
  public void testOnPauseCallback_resumeThenRepause_invokedTwice() throws Exception {
    ToggleGate gate = ToggleGate.closed();
    Directory baseDir = new ByteBuffersDirectory();
    AtomicInteger pauseCount = new AtomicInteger(0);
    PausableDirectory pausableDir =
        new PausableDirectory(baseDir, gate, pauseCount::incrementAndGet);

    IndexOutput output = pausableDir.createOutput("test.dat", IOContext.DEFAULT);

    AtomicReference<Exception> writeException = new AtomicReference<>();
    CountDownLatch firstWriteStarted = new CountDownLatch(1);
    CountDownLatch firstWriteDone = new CountDownLatch(1);
    // Held by the writer thread until the main thread has reclosed the gate, so the second
    // writeBytes() is guaranteed to observe a closed gate at its pause check.
    CountDownLatch gateReclosed = new CountDownLatch(1);

    ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      executor.submit(
          () -> {
            try {
              byte[] data = new byte[(int) PausableDirectory.CHECK_PAUSE_INTERVAL_BYTES];
              firstWriteStarted.countDown();
              output.writeBytes(data, 0, data.length);
              firstWriteDone.countDown();
              // Wait until the main thread has reclosed the gate before issuing the second write,
              // so the second pause check deterministically observes a closed gate.
              if (!gateReclosed.await(5, TimeUnit.SECONDS)) {
                throw new IllegalStateException("Timed out waiting for gate to be reclosed");
              }
              output.writeBytes(data, 0, data.length);
            } catch (Exception e) {
              writeException.set(e);
              firstWriteDone.countDown();
            }
          });

      assertTrue("First write should start", firstWriteStarted.await(5, TimeUnit.SECONDS));
      assertTrue(
          "onPause should fire once for first pause window",
          pollUntil(() -> pauseCount.get() >= 1, Duration.ofSeconds(5)));
      assertEquals("onPause should fire once for first pause window", 1, pauseCount.get());

      gate.open();
      assertTrue("First write should complete", firstWriteDone.await(5, TimeUnit.SECONDS));

      // Reclose the gate, then release the writer to issue the second writeBytes(). This ordering
      // guarantees the second write's pause check sees the gate as closed.
      gate.close();
      gateReclosed.countDown();
      assertTrue(
          "onPause should fire again for the second distinct pause window",
          pollUntil(() -> pauseCount.get() >= 2, Duration.ofSeconds(5)));
      assertEquals(
          "onPause should fire again for the second distinct pause window", 2, pauseCount.get());

      gate.open();
      executor.shutdown();
      assertTrue(
          "Second write should complete after gate reopens",
          executor.awaitTermination(5, TimeUnit.SECONDS));

      if (writeException.get() != null) {
        throw writeException.get();
      }
    } finally {
      gate.open();
      executor.shutdownNow();
      executor.awaitTermination(5, TimeUnit.SECONDS);
    }

    output.close();
    pausableDir.close();
  }

  /**
   * Test that if the merge thread is interrupted while blocked in {@code awaitOpen()}, the
   * resulting IOException propagates, the interrupt flag is preserved, and the pause latch is reset
   * so a future pause window (on a future output from the same directory) would still fire onPause.
   */
  @Test
  public void testOnPauseCallback_interruptedDuringPause_latchReset() throws Exception {
    ToggleGate gate = ToggleGate.closed();
    Directory baseDir = new ByteBuffersDirectory();
    AtomicInteger pauseCount = new AtomicInteger(0);
    PausableDirectory pausableDir =
        new PausableDirectory(baseDir, gate, pauseCount::incrementAndGet);

    IndexOutput output = pausableDir.createOutput("test.dat", IOContext.DEFAULT);

    AtomicReference<Exception> writeException = new AtomicReference<>();
    AtomicBoolean interruptObserved = new AtomicBoolean(false);
    CountDownLatch writeStarted = new CountDownLatch(1);

    ExecutorService executor = Executors.newSingleThreadExecutor();
    Thread[] writerThread = new Thread[1];
    try {
      executor.submit(
          () -> {
            writerThread[0] = Thread.currentThread();
            try {
              byte[] data = new byte[(int) PausableDirectory.CHECK_PAUSE_INTERVAL_BYTES];
              writeStarted.countDown();
              output.writeBytes(data, 0, data.length);
            } catch (IOException e) {
              writeException.set(e);
              interruptObserved.set(Thread.currentThread().isInterrupted());
            } catch (Exception e) {
              writeException.set(e);
            }
          });

      assertTrue("Write should start", writeStarted.await(5, TimeUnit.SECONDS));
      assertTrue(
          "onPause should fire once before interrupt",
          pollUntil(() -> pauseCount.get() >= 1, Duration.ofSeconds(5)));
      assertEquals("onPause should fire once before interrupt", 1, pauseCount.get());

      // Interrupt the writer while it is blocked in awaitOpen().
      writerThread[0].interrupt();
      executor.shutdown();
      assertTrue(
          "Writer should exit after interrupt", executor.awaitTermination(5, TimeUnit.SECONDS));

      assertTrue(
          "Writer should have thrown IOException", writeException.get() instanceof IOException);
      assertTrue(
          "Interrupt flag should be preserved on the writer thread", interruptObserved.get());

      // A new output from the same directory should still be able to fire onPause for a new
      // pause window, proving the latch was reset on the interrupt path.
      IndexOutput output2 = pausableDir.createOutput("test2.dat", IOContext.DEFAULT);
      CountDownLatch write2Started = new CountDownLatch(1);
      AtomicReference<Exception> write2Exception = new AtomicReference<>();
      ExecutorService executor2 = Executors.newSingleThreadExecutor();
      try {
        executor2.submit(
            () -> {
              try {
                byte[] data = new byte[(int) PausableDirectory.CHECK_PAUSE_INTERVAL_BYTES];
                write2Started.countDown();
                output2.writeBytes(data, 0, data.length);
              } catch (Exception e) {
                write2Exception.set(e);
              }
            });
        assertTrue("Second write should start", write2Started.await(5, TimeUnit.SECONDS));
        assertTrue(
            "onPause should fire again after interrupt reset the latch",
            pollUntil(() -> pauseCount.get() >= 2, Duration.ofSeconds(5)));
        assertEquals(
            "onPause should fire again after interrupt reset the latch", 2, pauseCount.get());
      } finally {
        gate.open();
        executor2.shutdown();
        executor2.awaitTermination(5, TimeUnit.SECONDS);
      }
      output2.close();
    } finally {
      gate.open();
      executor.shutdownNow();
      executor.awaitTermination(5, TimeUnit.SECONDS);
    }

    output.close();
    pausableDir.close();
  }

  private static boolean pollUntil(java.util.function.BooleanSupplier condition, Duration timeout)
      throws InterruptedException {
    long deadline = System.nanoTime() + timeout.toNanos();
    while (System.nanoTime() < deadline) {
      if (condition.getAsBoolean()) {
        return true;
      }
      Thread.sleep(10);
    }
    return condition.getAsBoolean();
  }
}
