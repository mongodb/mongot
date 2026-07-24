package com.xgen.mongot.replication.mongodb.steadystate.changestream;

import com.xgen.mongot.metrics.MetricsFactory;
import com.xgen.mongot.util.concurrent.Executors;
import com.xgen.mongot.util.concurrent.NamedScheduledExecutorService;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.Optional;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import org.jetbrains.annotations.TestOnly;
import org.jetbrains.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link ChangeStreamTaskExecutor} that caps in-flight getMores for auto-embedding indexes.
 *
 * <p>Only submissions whose {@link EnqueueContext#indexDefinition()} is an auto-embedding index are
 * subject to the cap; other submissions (e.g. search indexes sharing this executor when priority
 * scheduling is disabled) run unthrottled. When a max in-flight cap is configured and the semaphore
 * cannot be acquired, the task is re-scheduled on the underlying executor after a short delay
 * rather than blocking the worker thread.
 */
// TODO(CLOUDP-426992): Remove AutoEmbeddingExecutor once legacy AE indexes are deprecated.
final class AutoEmbeddingExecutor implements ChangeStreamTaskExecutor {

  private static final Logger LOG = LoggerFactory.getLogger(AutoEmbeddingExecutor.class);
  private static final long RESCHEDULE_DELAY_SECONDS = 1;

  private final NamedScheduledExecutorService delegate;
  private final Optional<Integer> maxInFlightEmbeddingGetMores;
  private final Optional<Semaphore> concurrentEmbeddingGetMores;
  private final Counter rescheduledEmbeddingGetMores;

  AutoEmbeddingExecutor(
      MeterRegistry meterRegistry,
      NamedScheduledExecutorService delegate,
      Optional<Integer> maxInFlightEmbeddingGetMores) {
    this.delegate = delegate;
    this.maxInFlightEmbeddingGetMores = maxInFlightEmbeddingGetMores;
    this.concurrentEmbeddingGetMores = maxInFlightEmbeddingGetMores.map(Semaphore::new);

    MetricsFactory metricsFactory =
        new MetricsFactory("indexing.steadyStateChangeStream", meterRegistry);
    this.rescheduledEmbeddingGetMores = metricsFactory.counter("rescheduledEmbeddingGetMores");
  }

  @Override
  public boolean enqueue(Runnable task, EnqueueContext context) {
    try {
      if (this.concurrentEmbeddingGetMores.isPresent()
          && context.indexDefinition().isAutoEmbeddingIndex()) {
        this.delegate.submit(() -> runWithSemaphore(task, context));
      } else {
        this.delegate.submit(task);
      }
      return true;
    } catch (RejectedExecutionException ex) {
      // The delegate uses an unbounded queue, so rejection only happens after shutdown. Hand the
      // task back to the dispatcher via onDropped so it can release resources.
      context.onDropped().run();
      return false;
    }
  }

  @Override
  public void shutdown() {
    Executors.shutdownOrFail(this.delegate);
  }

  private void runWithSemaphore(Runnable task, EnqueueContext context) {
    Semaphore semaphore = this.concurrentEmbeddingGetMores.get();
    if (!semaphore.tryAcquire()) {
      LOG.atDebug().log(
          "Max embedding getMores already in progress: {}. "
              + "Attempting to reschedule batch in {} second(s)",
          this.maxInFlightEmbeddingGetMores.get(),
          RESCHEDULE_DELAY_SECONDS);
      try {
        this.delegate.schedule(
            () -> runWithSemaphore(task, context), RESCHEDULE_DELAY_SECONDS, TimeUnit.SECONDS);
        this.rescheduledEmbeddingGetMores.increment();
      } catch (RejectedExecutionException ex) {
        // The executor uses an unbounded queue, so this can only happen during shutdown.
        if (this.delegate.isShutdown()) {
          LOG.atInfo().log(
              "Failed to reschedule auto-embedding batch on executor service due to shutdown.");
        } else {
          LOG.atError()
              .log(
                  "Failed to reschedule auto-embedding batch despite executor being active. "
                      + "This should never happen.");
        }
        context.onDropped().run();
      }
      return;
    }
    try {
      task.run();
    } finally {
      semaphore.release();
    }
  }

  @TestOnly
  @VisibleForTesting
  Optional<Integer> getEmbeddingAvailablePermits() {
    return this.concurrentEmbeddingGetMores.map(Semaphore::availablePermits);
  }
}
