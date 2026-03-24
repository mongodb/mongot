package com.xgen.mongot.server.executors;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.xgen.mongot.server.command.Command;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.bson.BsonDocument;
import org.junit.Test;

/** Unit tests for {@link BulkheadCommandExecutor}. */
public class BulkheadCommandExecutorTest {

  private final BulkheadCommandExecutor executor =
      new BulkheadCommandExecutor(new SimpleMeterRegistry());

  @Test
  public void execute_syncPolicy_runsOnCallerThread()
      throws ExecutionException, InterruptedException {
    String callerThreadName = Thread.currentThread().getName();
    String[] executionThreadName = new String[1];

    this.executor
        .execute(syncCommand(() -> executionThreadName[0] = Thread.currentThread().getName()))
        .get();

    assertThat(executionThreadName[0]).isEqualTo(callerThreadName);
  }

  @Test
  public void execute_syncPolicyWithException_wrapsInExecutionException() {
    assertThrows(
        ExecutionException.class,
        () ->
            this.executor
                .execute(syncCommand(() -> {
                  throw new UncheckedIOException(new IOException());
                }))
                .get());
  }

  @Test
  public void execute_asyncPolicy_runsOnDifferentThread()
      throws ExecutionException, InterruptedException {
    String callerThreadName = Thread.currentThread().getName();
    String[] executionThreadName = new String[1];

    this.executor
        .execute(asyncCommand(() -> executionThreadName[0] = Thread.currentThread().getName()))
        .get();

    assertThat(executionThreadName[0]).isNotEqualTo(callerThreadName);
  }

  @Test
  public void execute_asyncPolicyWithException_wrapsInExecutionException() {
    assertThrows(
        ExecutionException.class,
        () ->
            this.executor
                .execute(asyncCommand(() -> {
                  throw new UncheckedIOException(new IOException());
                }))
                .get());
  }

  @Test
  public void execute_afterClose_throwsRejectedExecutionException() {
    BulkheadCommandExecutor closedExecutor = new BulkheadCommandExecutor(new SimpleMeterRegistry());
    closedExecutor.close();

    assertThrows(
        RejectedExecutionException.class, () -> closedExecutor.execute(simpleAsyncCommand()));
  }

  @Test
  public void constructor_boundedQueueSettings_registersRejectionCounter() {
    SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
    // virtualQueueCapacity=false means bounded queue (real rejection)
    RegularBlockingRequestSettings settings =
        RegularBlockingRequestSettings.create(
            Optional.of(2.0), Optional.of(2.0), Optional.of(false));

    try (BulkheadCommandExecutor exec = new BulkheadCommandExecutor(meterRegistry, settings)) {
      Counter rejectedCounter =
          meterRegistry.find("loadShedding.rejected").tag("executor", "blocking-server-worker")
              .counter();

      assertThat(rejectedCounter).isNotNull();
    }
  }

  @Test
  public void constructor_unboundedQueueSettings_registersWouldHaveRejectedCounter() {
    SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
    // virtualQueueCapacity=true means unbounded queue with virtual capacity tracking
    RegularBlockingRequestSettings settings =
        RegularBlockingRequestSettings.create(
            Optional.of(2.0), Optional.of(4.0), Optional.of(true));

    try (BulkheadCommandExecutor exec = new BulkheadCommandExecutor(meterRegistry, settings)) {
      Counter wouldHaveRejectedCounter =
          meterRegistry.find("loadShedding.wouldHaveRejected")
              .tag("executor", "blocking-server-worker")
              .counter();

      assertThat(wouldHaveRejectedCounter).isNotNull();
    }
  }

  @Test
  public void constructor_defaultSettings_doesNotRegisterLoadSheddingCounters() {
    SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
    RegularBlockingRequestSettings settings = RegularBlockingRequestSettings.defaults();

    try (BulkheadCommandExecutor exec = new BulkheadCommandExecutor(meterRegistry, settings)) {
      Counter rejectedCounter =
          meterRegistry.find("loadShedding.rejected").tag("executor", "blocking-server-worker")
              .counter();
      Counter wouldHaveRejectedCounter =
          meterRegistry.find("loadShedding.wouldHaveRejected")
              .tag("executor", "blocking-server-worker")
              .counter();

      assertThat(rejectedCounter).isNull();
      assertThat(wouldHaveRejectedCounter).isNull();
    }
  }

  @Test
  public void constructor_unboundedQueueWithoutQueueConfig_doesNotRegisterWouldHaveRejected() {
    SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
    // virtualQueueCapacity=true but no queue config -> no wouldHaveRejected counter
    RegularBlockingRequestSettings settings =
        RegularBlockingRequestSettings.create(
            Optional.of(2.0), Optional.empty(), Optional.of(true));

    try (BulkheadCommandExecutor exec = new BulkheadCommandExecutor(meterRegistry, settings)) {
      Counter wouldHaveRejectedCounter =
          meterRegistry.find("loadShedding.wouldHaveRejected")
              .tag("executor", "blocking-server-worker")
              .counter();

      assertThat(wouldHaveRejectedCounter).isNull();
    }
  }

  @Test
  public void execute_boundedQueueAtCapacity_throwsLoadSheddingRejectedException()
      throws InterruptedException {
    SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
    // Use small multiplier so poolSize=1, queueCapacity=1 regardless of CPU count
    RegularBlockingRequestSettings settings =
        RegularBlockingRequestSettings.create(
            Optional.of(0.001), Optional.of(0.001), Optional.of(false));

    try (BulkheadCommandExecutor exec = new BulkheadCommandExecutor(meterRegistry, settings)) {
      CountDownLatch blockingLatch = new CountDownLatch(1);
      CountDownLatch taskStartedLatch = new CountDownLatch(1);

      exec.execute(blockingCommand(blockingLatch, taskStartedLatch));
      assertTrue(taskStartedLatch.await(5, TimeUnit.SECONDS));
      exec.execute(simpleAsyncCommand()); // fills queue

      // LoadSheddingRejectedException is a subclass of RejectedExecutionException
      LoadSheddingRejectedException exception =
          assertThrows(
              LoadSheddingRejectedException.class, () -> exec.execute(simpleAsyncCommand()));
      assertThat(exception.getMessage()).contains("at capacity");

      blockingLatch.countDown();
    }
  }

  @Test
  public void execute_boundedQueueAtCapacity_incrementsRejectionCounter()
      throws InterruptedException {
    SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
    // Use small multiplier so poolSize=1, queueCapacity=1 regardless of CPU count
    RegularBlockingRequestSettings settings =
        RegularBlockingRequestSettings.create(
            Optional.of(0.001), Optional.of(0.001), Optional.of(false));

    try (BulkheadCommandExecutor exec = new BulkheadCommandExecutor(meterRegistry, settings)) {
      CountDownLatch blockingLatch = new CountDownLatch(1);
      CountDownLatch taskStartedLatch = new CountDownLatch(1);

      exec.execute(blockingCommand(blockingLatch, taskStartedLatch));
      assertTrue(taskStartedLatch.await(5, TimeUnit.SECONDS));
      exec.execute(simpleAsyncCommand());

      try {
        exec.execute(simpleAsyncCommand());
      } catch (LoadSheddingRejectedException ignored) {
        // expected - LoadSheddingRejectedException is thrown for load shedding rejections
      }

      Counter rejectedCounter =
          meterRegistry.find("loadShedding.rejected").tag("executor", "blocking-server-worker")
              .counter();

      assertThat(rejectedCounter.count()).isEqualTo(1.0);

      blockingLatch.countDown();
    }
  }

  @Test
  public void execute_unboundedQueueExceedsVirtualCapacity_incrementsWouldHaveRejectedCounter()
      throws InterruptedException {
    SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
    // Use small multiplier so poolSize=1, queueCapacity=1 regardless of CPU count
    RegularBlockingRequestSettings settings =
        RegularBlockingRequestSettings.create(
            Optional.of(0.001), Optional.of(0.001), Optional.of(true));

    try (BulkheadCommandExecutor exec = new BulkheadCommandExecutor(meterRegistry, settings)) {
      CountDownLatch blockingLatch = new CountDownLatch(1);
      CountDownLatch taskStartedLatch = new CountDownLatch(1);

      exec.execute(blockingCommand(blockingLatch, taskStartedLatch));
      assertTrue(taskStartedLatch.await(5, TimeUnit.SECONDS));
      exec.execute(simpleAsyncCommand());
      Thread.sleep(50); // allow queue to populate
      exec.execute(simpleAsyncCommand()); // exceeds virtual capacity

      Counter wouldHaveRejectedCounter =
          meterRegistry.find("loadShedding.wouldHaveRejected")
              .tag("executor", "blocking-server-worker")
              .counter();

      assertThat(wouldHaveRejectedCounter.count()).isGreaterThan(0);

      blockingLatch.countDown();
    }
  }

  @Test
  public void execute_unboundedQueueExceedsVirtualCapacity_doesNotReject()
      throws InterruptedException {
    SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
    // Use small multiplier so poolSize=1, queueCapacity=1 regardless of CPU count
    RegularBlockingRequestSettings settings =
        RegularBlockingRequestSettings.create(
            Optional.of(0.001), Optional.of(0.001), Optional.of(true));

    try (BulkheadCommandExecutor exec = new BulkheadCommandExecutor(meterRegistry, settings)) {
      CountDownLatch blockingLatch = new CountDownLatch(1);
      CountDownLatch taskStartedLatch = new CountDownLatch(1);

      exec.execute(blockingCommand(blockingLatch, taskStartedLatch));
      assertTrue(taskStartedLatch.await(5, TimeUnit.SECONDS));

      // These should all succeed without throwing - unbounded queue never rejects
      exec.execute(simpleAsyncCommand());
      exec.execute(simpleAsyncCommand());
      exec.execute(simpleAsyncCommand());
      exec.execute(simpleAsyncCommand());
      exec.execute(simpleAsyncCommand());

      blockingLatch.countDown();
    }
    // Test passes if no RejectedExecutionException is thrown
  }

  private Command syncCommand(Runnable action) {
    return new Command() {
      @Override
      public String name() {
        return "sync-test";
      }

      @Override
      public BsonDocument run() {
        action.run();
        return null;
      }

      @Override
      public ExecutionPolicy getExecutionPolicy() {
        return ExecutionPolicy.SYNC;
      }
    };
  }

  private Command asyncCommand(Runnable action) {
    return new Command() {
      @Override
      public String name() {
        return "async-test";
      }

      @Override
      public BsonDocument run() {
        action.run();
        return null;
      }

      @Override
      public ExecutionPolicy getExecutionPolicy() {
        return ExecutionPolicy.ASYNC;
      }
    };
  }

  private Command simpleAsyncCommand() {
    return asyncCommand(() -> {});
  }

  private Command blockingCommand(CountDownLatch blockingLatch, CountDownLatch taskStartedLatch) {
    return new Command() {
      @Override
      public String name() {
        return "blocking";
      }

      @Override
      public BsonDocument run() {
        taskStartedLatch.countDown();
        try {
          blockingLatch.await(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
        return null;
      }

      @Override
      public ExecutionPolicy getExecutionPolicy() {
        return ExecutionPolicy.ASYNC;
      }
    };
  }

  @Test
  public void execute_nonLoadSheddableAsyncCommand_runsOnGuaranteedExecutor()
      throws ExecutionException, InterruptedException {
    String[] executionThreadName = new String[1];

    this.executor
        .execute(
            nonLoadSheddableAsyncCommand(
                () -> executionThreadName[0] = Thread.currentThread().getName()))
        .get();

    assertThat(executionThreadName[0]).startsWith("guaranteed-blocking-server-worker");
  }

  @Test
  public void execute_loadSheddableAsyncCommand_runsOnBlockingExecutor()
      throws ExecutionException, InterruptedException {
    String[] executionThreadName = new String[1];

    this.executor
        .execute(asyncCommand(() -> executionThreadName[0] = Thread.currentThread().getName()))
        .get();

    assertThat(executionThreadName[0]).startsWith("blocking-server-worker");
  }

  @Test
  public void execute_nonLoadSheddableCommandAtCapacity_doesNotReject()
      throws InterruptedException {
    SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
    // Use small multiplier so poolSize=1, queueCapacity=1 regardless of CPU count
    RegularBlockingRequestSettings settings =
        RegularBlockingRequestSettings.create(
            Optional.of(0.001), Optional.of(0.001), Optional.of(false));

    try (BulkheadCommandExecutor exec = new BulkheadCommandExecutor(meterRegistry, settings)) {
      CountDownLatch blockingLatch = new CountDownLatch(1);
      CountDownLatch taskStartedLatch = new CountDownLatch(1);

      exec.execute(blockingCommand(blockingLatch, taskStartedLatch));
      assertTrue(taskStartedLatch.await(5, TimeUnit.SECONDS));
      exec.execute(simpleAsyncCommand()); // fills queue

      // Load-sheddable command would be rejected with LoadSheddingRejectedException
      assertThrows(
          LoadSheddingRejectedException.class, () -> exec.execute(simpleAsyncCommand()));

      // Non-load-sheddable command should still succeed
      exec.execute(simpleNonLoadSheddableAsyncCommand()); // should NOT throw

      blockingLatch.countDown();
    }
    // Test passes if non-load-sheddable command is not rejected
  }

  @Test
  public void execute_nonLoadSheddableSyncCommand_runsOnCallerThread()
      throws ExecutionException, InterruptedException {
    String callerThreadName = Thread.currentThread().getName();
    String[] executionThreadName = new String[1];

    this.executor
        .execute(
            nonLoadSheddableSyncCommand(
                () -> executionThreadName[0] = Thread.currentThread().getName()))
        .get();

    // SYNC commands always run on caller thread, regardless of load shedding status
    assertThat(executionThreadName[0]).isEqualTo(callerThreadName);
  }

  private Command nonLoadSheddableAsyncCommand(Runnable action) {
    return new Command() {
      @Override
      public String name() {
        return "non-load-sheddable-async-test";
      }

      @Override
      public BsonDocument run() {
        action.run();
        return null;
      }

      @Override
      public ExecutionPolicy getExecutionPolicy() {
        return ExecutionPolicy.ASYNC;
      }

      @Override
      public boolean maybeLoadShed() {
        return false;
      }
    };
  }

  private Command simpleNonLoadSheddableAsyncCommand() {
    return nonLoadSheddableAsyncCommand(() -> {});
  }

  private Command nonLoadSheddableSyncCommand(Runnable action) {
    return new Command() {
      @Override
      public String name() {
        return "non-load-sheddable-sync-test";
      }

      @Override
      public BsonDocument run() {
        action.run();
        return null;
      }

      @Override
      public ExecutionPolicy getExecutionPolicy() {
        return ExecutionPolicy.SYNC;
      }

      @Override
      public boolean maybeLoadShed() {
        return false;
      }
    };
  }

  @Test
  public void execute_aicManagementCommand_runsOnGuaranteedExecutor()
      throws ExecutionException, InterruptedException {
    String[] executionThreadName = new String[1];

    this.executor
        .execute(
            nonLoadSheddableAsyncCommand(
                () -> executionThreadName[0] = Thread.currentThread().getName()))
        .get();

    assertThat(executionThreadName[0]).startsWith("guaranteed-blocking-server-worker");
  }

  @Test
  public void execute_aicManagementCommandAtCapacity_doesNotReject() throws InterruptedException {
    SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
    RegularBlockingRequestSettings settings =
        RegularBlockingRequestSettings.create(
            Optional.of(0.001), Optional.of(0.001), Optional.of(false));

    try (BulkheadCommandExecutor exec = new BulkheadCommandExecutor(meterRegistry, settings)) {
      CountDownLatch blockingLatch = new CountDownLatch(1);
      CountDownLatch taskStartedLatch = new CountDownLatch(1);

      exec.execute(blockingCommand(blockingLatch, taskStartedLatch));
      assertTrue(taskStartedLatch.await(5, TimeUnit.SECONDS));
      exec.execute(simpleAsyncCommand());

      assertThrows(
          LoadSheddingRejectedException.class, () -> exec.execute(simpleAsyncCommand()));

      exec.execute(simpleNonLoadSheddableAsyncCommand());

      blockingLatch.countDown();
    }
  }

  @Test
  public void execute_cancelledStream_skipsCommandAndIncrementsCounter()
      throws InterruptedException {
    SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
    try (BulkheadCommandExecutor exec = new BulkheadCommandExecutor(meterRegistry)) {
      AtomicBoolean commandRan = new AtomicBoolean(false);
      Command command = asyncCommand(() -> commandRan.set(true));

      var future = exec.execute(command, () -> true);
      ExecutionException thrown = assertThrows(ExecutionException.class, future::get);
      assertThat(thrown.getCause()).isInstanceOf(CancelledStreamSkipException.class);

      assertThat(commandRan.get()).isFalse();

      Counter skippedCounter =
          meterRegistry
              .find("loadShedding.skippedDueToCancelledStream")
              .tag("executor", "blocking-server-worker")
              .counter();
      assertThat(skippedCounter).isNotNull();
      assertThat(skippedCounter.count()).isEqualTo(1.0);
    }
  }

  @Test
  public void execute_liveStream_runsCommandNormally()
      throws ExecutionException, InterruptedException {
    SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
    try (BulkheadCommandExecutor exec = new BulkheadCommandExecutor(meterRegistry)) {
      AtomicBoolean commandRan = new AtomicBoolean(false);
      Command command = asyncCommand(() -> commandRan.set(true));

      exec.execute(command, () -> false).get();

      assertThat(commandRan.get()).isTrue();

      Counter skippedCounter =
          meterRegistry
              .find("loadShedding.skippedDueToCancelledStream")
              .tag("executor", "blocking-server-worker")
              .counter();
      assertThat(skippedCounter).isNotNull();
      assertThat(skippedCounter.count()).isEqualTo(0.0);
    }
  }

  @Test
  public void execute_streamCancelledWhileQueued_skipsOnDequeue() throws InterruptedException {
    SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
    RegularBlockingRequestSettings settings =
        RegularBlockingRequestSettings.create(
            Optional.of(0.001), Optional.of(0.001), Optional.of(true));

    try (BulkheadCommandExecutor exec = new BulkheadCommandExecutor(meterRegistry, settings)) {
      CountDownLatch blockingLatch = new CountDownLatch(1);
      CountDownLatch taskStartedLatch = new CountDownLatch(1);
      AtomicBoolean cancelled = new AtomicBoolean(false);
      AtomicBoolean secondCommandRan = new AtomicBoolean(false);

      exec.execute(blockingCommand(blockingLatch, taskStartedLatch));
      assertTrue(taskStartedLatch.await(5, TimeUnit.SECONDS));

      var future =
          exec.execute(asyncCommand(() -> secondCommandRan.set(true)), cancelled::get);

      cancelled.set(true);
      blockingLatch.countDown();

      ExecutionException thrown = assertThrows(ExecutionException.class, future::get);
      assertThat(thrown.getCause()).isInstanceOf(CancelledStreamSkipException.class);

      assertThat(secondCommandRan.get()).isFalse();

      Counter skippedCounter =
          meterRegistry
              .find("loadShedding.skippedDueToCancelledStream")
              .tag("executor", "blocking-server-worker")
              .counter();
      assertThat(skippedCounter).isNotNull();
      assertThat(skippedCounter.count()).isEqualTo(1.0);
    }
  }

  @Test
  public void execute_cancelledStreamWithGuaranteedCommand_stillRunsCommand()
      throws ExecutionException, InterruptedException {
    SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
    try (BulkheadCommandExecutor exec = new BulkheadCommandExecutor(meterRegistry)) {
      AtomicBoolean commandRan = new AtomicBoolean(false);
      Command command = nonLoadSheddableAsyncCommand(() -> commandRan.set(true));

      exec.execute(command, () -> true).get();

      assertThat(commandRan.get()).isTrue();

      Counter skippedCounter =
          meterRegistry
              .find("loadShedding.skippedDueToCancelledStream")
              .tag("executor", "blocking-server-worker")
              .counter();
      assertThat(skippedCounter.count()).isEqualTo(0.0);
    }
  }

  @Test
  public void execute_cancelledStreamWithSyncCommand_stillRunsCommand()
      throws ExecutionException, InterruptedException {
    SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
    try (BulkheadCommandExecutor exec = new BulkheadCommandExecutor(meterRegistry)) {
      AtomicBoolean commandRan = new AtomicBoolean(false);
      Command command = syncCommand(() -> commandRan.set(true));

      exec.execute(command, () -> true).get();

      assertThat(commandRan.get()).isTrue();
    }
  }

  @Test
  public void constructor_registersSkippedDueToCancelledStreamCounter() {
    SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
    try (BulkheadCommandExecutor exec = new BulkheadCommandExecutor(meterRegistry)) {
      Counter skippedCounter =
          meterRegistry
              .find("loadShedding.skippedDueToCancelledStream")
              .tag("executor", "blocking-server-worker")
              .counter();
      assertThat(skippedCounter).isNotNull();
    }
  }
}
