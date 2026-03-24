package com.xgen.mongot.server.executors;

import com.xgen.mongot.server.command.Command;
import com.xgen.mongot.util.Runtime;
import com.xgen.mongot.util.concurrent.Executors;
import com.xgen.mongot.util.concurrent.NamedExecutorService;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.io.Closeable;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.function.BooleanSupplier;
import org.bson.BsonDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Executes provided commands on corresponding thread pools, implementing the Bulkhead pattern. This
 * behavior allows grouping of homogeneous tasks and isolates blocking calls from response-time
 * sensitive tasks to guarantee predictable execution time under load.
 */
public class BulkheadCommandExecutor implements Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(BulkheadCommandExecutor.class);
  private static final String REGULAR_EXECUTOR_NAME = "blocking-server-worker";
  private static final String GUARANTEED_EXECUTOR_NAME = "guaranteed-blocking-server-worker";

  private final NamedExecutorService regularBlockingCommandExecutor;
  private final NamedExecutorService guaranteedBlockingCommandExecutor;
  private final OptionalInt virtualQueueCapacity;
  private final Optional<Counter> wouldHaveRejectedCounter;
  private final Counter skippedDueToCancelledStreamCounter;

  /** Holds the regular executor configuration created during construction. */
  private record RegularExecutorConfig(
      NamedExecutorService executor,
      OptionalInt virtualQueueCapacity,
      Optional<Counter> wouldHaveRejectedCounter) {}

  public BulkheadCommandExecutor(MeterRegistry meterRegistry) {
    this(meterRegistry, RegularBlockingRequestSettings.defaults());
  }

  /**
   * Creates a bulkhead executor using the provided sizing settings.
   *
   * @param meterRegistry registry for executor metrics
   * @param settings pool/queue sizing configuration
   */
  public BulkheadCommandExecutor(
      MeterRegistry meterRegistry, RegularBlockingRequestSettings settings) {
    int numCpus = Runtime.INSTANCE.getNumCpus();
    int poolSize = settings.resolvedPoolSize(numCpus);
    OptionalInt queueCapacity = settings.maybeResolvedQueueCapacity(numCpus);

    // Initialize the guaranteed command executor - always unbounded to ensure guaranteed execution
    this.guaranteedBlockingCommandExecutor =
        Executors.unboundedCachingThreadPool(GUARANTEED_EXECUTOR_NAME, meterRegistry);

    // Use switch expression to satisfy both Java's definite assignment and Error Prone's
    // exhaustiveness check (no default case needed for enum switch expressions)
    RegularExecutorConfig config =
        switch (settings.getMode()) {
          case FIXED_POOL_BOUNDED_QUEUE -> {
            int boundedQueue =
                queueCapacity.orElseThrow(
                    () ->
                        new IllegalStateException(
                            "Queue capacity required for bounded queue mode"));
            Counter rejectionCounter =
                meterRegistry.counter("loadShedding.rejected", "executor", REGULAR_EXECUTOR_NAME);
            LOG.info(
                "Regular blocking bulkhead executor configured: mode=FIXED_POOL_BOUNDED_QUEUE,"
                    + " poolSize={}, queueCapacity={}",
                poolSize,
                boundedQueue);
            yield new RegularExecutorConfig(
                Executors.fixedSizeThreadPool(
                    REGULAR_EXECUTOR_NAME,
                    poolSize,
                    boundedQueue,
                    rejectionHandler(rejectionCounter, poolSize, boundedQueue),
                    meterRegistry),
                OptionalInt.empty(),
                Optional.empty());
          }
          case FIXED_POOL_UNBOUNDED_QUEUE -> {
            // Virtual queue capacity is optional - only record wouldHaveRejected if configured
            LOG.info(
                "Regular blocking bulkhead executor configured: mode=FIXED_POOL_UNBOUNDED_QUEUE,"
                    + " poolSize={}, virtualQueueCapacity={}",
                poolSize,
                queueCapacity.isPresent() ? queueCapacity.getAsInt() : "not configured");
            yield new RegularExecutorConfig(
                Executors.fixedSizeThreadPool(REGULAR_EXECUTOR_NAME, poolSize, meterRegistry),
                queueCapacity,
                queueCapacity.isPresent()
                    ? Optional.of(
                        meterRegistry.counter(
                            "loadShedding.wouldHaveRejected", "executor", REGULAR_EXECUTOR_NAME))
                    : Optional.empty());
          }
          case UNBOUNDED_CACHING -> {
            LOG.info("Bulkhead executor configured: mode=UNBOUNDED_CACHING");
            yield new RegularExecutorConfig(
                Executors.unboundedCachingThreadPool(REGULAR_EXECUTOR_NAME, meterRegistry),
                OptionalInt.empty(),
                Optional.empty());
          }
        };

    this.regularBlockingCommandExecutor = config.executor();
    this.virtualQueueCapacity = config.virtualQueueCapacity();
    this.wouldHaveRejectedCounter = config.wouldHaveRejectedCounter();
    this.skippedDueToCancelledStreamCounter =
        meterRegistry.counter(
            "loadShedding.skippedDueToCancelledStream", "executor", REGULAR_EXECUTOR_NAME);
  }

  private RejectedExecutionHandler rejectionHandler(
      Counter rejectionCounter, int poolSize, int queueCapacity) {
    return (r, executor) -> {
      rejectionCounter.increment();
      LOG.warn(
          "Query rejected due to executor capacity limits: poolSize={}, queueCapacity={}",
          poolSize,
          queueCapacity);
      throw new LoadSheddingRejectedException(
          "Query rejected: search server is currently at capacity. Please try again later.");
    };
  }

  private void recordWouldHaveRejectedIfNeeded() {
    if (this.wouldHaveRejectedCounter.isEmpty() || this.virtualQueueCapacity.isEmpty()) {
      return;
    }

    OptionalInt activeCount = this.regularBlockingCommandExecutor.getActiveCount();
    OptionalInt maxPoolSize = this.regularBlockingCommandExecutor.getMaxPoolSize();
    OptionalInt queueSize = this.regularBlockingCommandExecutor.getQueueSize();

    if (activeCount.isPresent()
        && maxPoolSize.isPresent()
        && queueSize.isPresent()
        && activeCount.getAsInt() >= maxPoolSize.getAsInt()
        && queueSize.getAsInt() >= this.virtualQueueCapacity.getAsInt()) {
      this.wouldHaveRejectedCounter.get().increment();
    }
  }

  /**
   * Schedules given command according to its execution policy.
   *
   * <p>Equivalent to {@link #execute(Command, BooleanSupplier)} with no cancellation check.
   */
  public CompletableFuture<BsonDocument> execute(Command command) {
    return execute(command, () -> false);
  }

  /**
   * Schedules given command according to its execution policy, with an optional cancellation check.
   *
   * <p>If an exception is thrown in {@link Command#run}, this method won't throw the exception. The
   * returned object will wrap the exception as {@code cause}. Calling {@link CompletableFuture#get}
   * on the returned object will throw a {@link java.util.concurrent.ExecutionException} that wraps
   * the exception in {@link Command#run}.
   *
   * <p>`recordWouldHaveRejectedIfNeeded()` will be called if the queue is unbounded but there is a
   * virtual capacity configured. This event will be recorded as if the command was rejected.
   *
   * <p>Commands that return {@code false} from {@link Command#maybeLoadShed()} will be executed on
   * a dedicated unbounded thread pool to ensure they are never rejected due to load shedding.
   *
   * <p>For ASYNC commands on the regular pool, the {@code isCancelled} supplier is checked at
   * dequeue time (just before execution). If the supplier returns {@code true}, the command is
   * skipped and a cancellation metric is recorded.
   *
   * @param command the command to execute
   * @param isCancelled supplier that returns true if the originating stream has been
   *     cancelled/closed
   * @throws RejectedExecutionException if the executor is configured with a bounded queue and the
   *     queue is full, or if the executor has been shut down
   */
  public CompletableFuture<BsonDocument> execute(Command command, BooleanSupplier isCancelled) {
    return switch (command.getExecutionPolicy()) {
      case ASYNC -> {
        if (!command.maybeLoadShed()) {
          yield CompletableFuture.supplyAsync(command::run, this.guaranteedBlockingCommandExecutor);
        }
        recordWouldHaveRejectedIfNeeded();
        yield CompletableFuture.supplyAsync(
            () -> {
              if (isCancelled.getAsBoolean()) {
                this.skippedDueToCancelledStreamCounter.increment();
                LOG.debug(
                    "Skipping dequeued command '{}' because stream was cancelled", command.name());
                throw new CancelledStreamSkipException(
                    "Command '"
                        + command.name()
                        + "' skipped: stream was cancelled before execution");
              }
              return command.run();
            },
            this.regularBlockingCommandExecutor);
      }
      case SYNC -> {
        try {
          yield CompletableFuture.completedFuture(command.run());
        } catch (Throwable t) {
          yield CompletableFuture.failedFuture(t);
        }
      }
    };
  }

  @Override
  public void close() {
    Executors.shutdownOrFail(this.regularBlockingCommandExecutor);
    Executors.shutdownOrFail(this.guaranteedBlockingCommandExecutor);
  }
}
