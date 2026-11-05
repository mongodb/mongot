package com.xgen.mongot.replication.mongodb.common;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.flogger.FluentLogger;
import com.xgen.mongot.metrics.MetricsFactory;
import com.xgen.mongot.util.concurrent.Sleeper;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import java.time.Duration;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

/**
 * Retries a steady-state change-stream getMore that mongod shed for overload (see {@link
 * com.xgen.mongot.util.mongodb.Errors#THROTTLING_ERROR_CODES}). A shed getMore never ran, so the
 * cursor is still valid and the same call can be repeated on a short jittered backoff instead of
 * failing the batch and resuming the change stream.
 *
 * <p>Applied inside {@link ChangeStreamMongoCursorClient} around the driver cursor's getMore, so
 * the wrapping clients never see a throttled call. The aggregate that opens the cursor is not
 * retried: a shed open fails to the existing resume path, which needs no cursor. A client without
 * one does not retry; overload rejections are counted by the client either way.
 *
 * <p>A shedding host can fail thousands of getMores a minute across all indexes, so each kind of
 * log line is emitted at most once a minute across all indexes. The counters stay exact and are the
 * source of truth.
 */
public final class ChangeStreamThrottlingRetry {

  private static final FluentLogger FLOGGER = FluentLogger.forEnclosingClass();

  @VisibleForTesting public static final int MAX_THROTTLING_RETRIES = 5;

  /** Base delay for throttling retries; the actual delay is uniform over [0, base). */
  @VisibleForTesting public static final Duration THROTTLING_RETRY_BACKOFF = Duration.ofMillis(100);

  private final Sleeper sleeper;
  /* getMores retried on the same cursor after being throttled by mongod */
  private final Counter retries;
  /* getMores that exhausted MAX_THROTTLING_RETRIES and failed the batch. Distinct from retries,
  which counts attempts and so cannot distinguish "succeeded on the last retry" from "gave up". */
  private final Counter retriesExhausted;
  /* Stops retrying when the owner of the change stream shuts down, e.g. the dispatcher or index */
  private final BooleanSupplier stopCondition;

  private ChangeStreamThrottlingRetry(
      Sleeper sleeper, Counter retries, Counter retriesExhausted, BooleanSupplier stopCondition) {
    this.sleeper = sleeper;
    this.retries = retries;
    this.retriesExhausted = retriesExhausted;
    this.stopCondition = stopCondition;
  }

  /** Creates the retry for one replication type. */
  public static ChangeStreamThrottlingRetry create(
      MeterRegistry meterRegistry, CommonReplicationConfig.Type replicationType) {
    return create(meterRegistry, replicationType, Sleeper.DEFAULT);
  }

  @VisibleForTesting
  public static ChangeStreamThrottlingRetry create(
      MeterRegistry meterRegistry, CommonReplicationConfig.Type replicationType, Sleeper sleeper) {

    var metricsFactory = new MetricsFactory("indexing.steadyStateChangeStream", meterRegistry);
    Tags replicationTypeTags = Tags.of("replicationType", replicationType.name());
    return new ChangeStreamThrottlingRetry(
        sleeper,
        metricsFactory.counter("getMoreThrottlingRetries", replicationTypeTags),
        metricsFactory.counter("getMoreThrottlingRetriesExhausted", replicationTypeTags),
        () -> false);
  }

  /**
   * Returns a retry sharing this one's settings and metrics that also stops retrying once {@code
   * stopCondition} is true. Shutdown leaves an active cursor open until its getMore unwinds, so
   * retries must stop on the owner's shutdown rather than on the client closing.
   */
  public ChangeStreamThrottlingRetry withStopCondition(BooleanSupplier stopCondition) {
    return new ChangeStreamThrottlingRetry(
        this.sleeper,
        this.retries,
        this.retriesExhausted,
        () -> this.stopCondition.getAsBoolean() || stopCondition.getAsBoolean());
  }

  /**
   * Runs {@code getMore}, repeating it while mongod sheds it, up to {@link #MAX_THROTTLING_RETRIES}
   * times, until the stop condition is true. Rethrows the last failure when the call is not
   * throttling, the stop condition is true, or the budget is spent.
   */
  <T> T run(Callable<T> getMore) throws Exception {
    for (int attempt = 0; ; attempt++) {
      try {
        return getMore.call();
      } catch (Exception e) {
        boolean retryable =
            MongoExceptionUtils.isThrottling(e) && !this.stopCondition.getAsBoolean();
        if (!retryable || attempt >= MAX_THROTTLING_RETRIES) {
          // Only an exhausted budget counts: the other paths never intended to retry.
          if (retryable) {
            this.retriesExhausted.increment();
            FLOGGER.atWarning().atMostEvery(1, TimeUnit.MINUTES).withCause(e).log(
                "exhausted %d throttling retries for getMore, failing the batch", attempt);
          }
          throw e;
        }
        FLOGGER.atInfo().atMostEvery(1, TimeUnit.MINUTES).withCause(e).log(
            "getMore was throttled by mongod, retrying on the same cursor (retry %d)", attempt + 1);
        this.retries.increment();
        sleepWithFullJitter(e);
        // The owner may have shut down while we slept; don't issue another getMore.
        if (this.stopCondition.getAsBoolean()) {
          throw e;
        }
      }
    }
  }

  private void sleepWithFullJitter(Exception cause) throws Exception {
    try {
      this.sleeper.sleep(
          Duration.ofMillis(
              ThreadLocalRandom.current().nextLong(THROTTLING_RETRY_BACKOFF.toMillis())));
    } catch (InterruptedException interrupted) {
      Thread.currentThread().interrupt();
      cause.addSuppressed(interrupted);
      throw cause;
    }
  }
}
