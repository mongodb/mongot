package com.xgen.mongot.embedding.utils;

import com.mongodb.MongoBulkWriteException;
import com.mongodb.MongoCursorNotFoundException;
import com.mongodb.MongoException;
import com.mongodb.MongoNodeIsRecoveringException;
import com.mongodb.MongoNotPrimaryException;
import com.mongodb.MongoSocketException;
import com.mongodb.MongoTimeoutException;
import com.xgen.mongot.embedding.exceptions.MaterializedViewTransientException;
import com.xgen.mongot.metrics.MetricsFactory;
import com.xgen.mongot.util.mongodb.CheckedMongoException;
import com.xgen.mongot.util.mongodb.Errors;
import com.xgen.mongot.util.retry.ExponentialBackoffPolicy;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import java.time.Duration;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BooleanSupplier;
import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.RetryPolicy;
import net.jodah.failsafe.function.CheckedRunnable;
import net.jodah.failsafe.function.CheckedSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Convenience class for executing MongoDB operations with a retry policy and basic latency and
 * success/failure metrics. Ideally to be only used with idempotent operations where we know the
 * operation is retryable. Also note that the retry policy only works for top level exceptions /
 * errors - bulk operations like bulkWrite which can produce partial failures need to be handled
 * appropriately by the caller.
 */
public class MongoClientOperationExecutor {

  private static final Logger LOG = LoggerFactory.getLogger(MongoClientOperationExecutor.class);
  private final RetryPolicy<Object> retryPolicy;
  private final MetricsFactory metricsFactory;
  private final String requestLatencyMetricName;
  private final String perAttemptLatencyMetricName;
  private final String failedRequestsMetricName;
  private final String successfulRequestsMetricName;
  private final ConcurrentHashMap<String, Timer> timerCache = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<String, Timer> perAttemptTimerCache = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<String, Counter> successCounterCache = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<String, Counter> failureCounterCache = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<String, Counter> retriedAttemptsCache = new ConcurrentHashMap<>();
  private final Counter mongodDiskFullErrors;
  private final Counter mongodUserWritesBlockedErrors;
  private final Counter mongodSystemOverloadErrors;

  /**
   * Creates a new MongoClientOperationExecutor.
   *
   * @param metricsFactory the metrics factory to use for creating metrics.
   * @param resourceName the mongoDB resource name. This will be used as the prefix for all metrics.
   */
  public MongoClientOperationExecutor(MetricsFactory metricsFactory, String resourceName) {
    this(
        metricsFactory,
        resourceName,
        ExponentialBackoffPolicy.builder()
            .initialDelay(Duration.ofMillis(500))
            .backoffFactor(2)
            .maxDelay(Duration.ofMillis(5000))
            .maxRetries(5)
            .jitter(0.1)
            .build(),
        () -> false);
  }

  /**
   * Creates a new MongoClientOperationExecutor with a custom backoff policy and abort condition.
   *
   * @param shouldAbortRetry supplier that returns true to stop retries immediately (e.g., when the
   *     owning resource is closed). When true, the current exception propagates without further
   *     retry attempts.
   */
  public MongoClientOperationExecutor(
      MetricsFactory metricsFactory,
      String resourceName,
      ExponentialBackoffPolicy backoffPolicy,
      BooleanSupplier shouldAbortRetry) {
    this.metricsFactory = metricsFactory;

    String retriedAttemptsMetricName = resourceName + ".retriedAttempts";
    this.retryPolicy =
        backoffPolicy.applyParameters(
            new RetryPolicy<>()
                .handleIf(e -> !shouldAbortRetry.getAsBoolean() && isRetryable(e))
                .onRetry(
                    ex -> {
                      String errorReason = classifyError(ex.getLastFailure());
                      this.retriedAttemptsCache
                          .computeIfAbsent(
                              errorReason,
                              k ->
                                  metricsFactory.counter(
                                      retriedAttemptsMetricName, Tags.of("errorReason", k)))
                          .increment();
                    })
                .onFailure(
                    ex ->
                        LOG.warn(
                            "Operation failed after {} attempts ({} elapsed).",
                            ex.getAttemptCount(),
                            ex.getElapsedTime(),
                            ex.getFailure())));
    this.requestLatencyMetricName = resourceName + ".requestLatency";
    this.perAttemptLatencyMetricName = resourceName + ".perAttemptLatency";
    this.failedRequestsMetricName = resourceName + ".failedRequests";
    this.successfulRequestsMetricName = resourceName + ".successfulRequests";
    this.mongodDiskFullErrors = metricsFactory.counter("mongodDiskFullErrors");
    this.mongodUserWritesBlockedErrors = metricsFactory.counter("mongodUserWritesBlockedErrors");
    this.mongodSystemOverloadErrors = metricsFactory.counter("mongodSystemOverloadErrors");
  }

  /**
   * Execute a MongoDB operation with retries and operation-specific metrics. Note that the retries
   * will be performed on the caller's thread so this is best suited for short-lived operations.
   *
   * @param operationName Name of the operation (e.g., "bulkWrite", "readCheckpoint")
   * @param operation The MongoDB operation to execute
   */
  public <T> T execute(String operationName, CheckedSupplier<T> operation) throws Exception {

    var metricTags = Tags.of("operation", operationName);
    Timer timer =
        this.timerCache.computeIfAbsent(
            operationName,
            k ->
                this.metricsFactory.timer(
                    this.requestLatencyMetricName, metricTags, 0.5, 0.75, 0.9, 0.99));
    Timer perAttemptTimer =
        this.perAttemptTimerCache.computeIfAbsent(
            operationName,
            k ->
                this.metricsFactory.timer(
                    this.perAttemptLatencyMetricName, metricTags, 0.5, 0.75, 0.9, 0.99));
    Counter successCounter =
        this.successCounterCache.computeIfAbsent(
            operationName,
            k -> this.metricsFactory.counter(this.successfulRequestsMetricName, metricTags));
    Counter failureCounter =
        this.failureCounterCache.computeIfAbsent(
            operationName,
            k -> this.metricsFactory.counter(this.failedRequestsMetricName, metricTags));

    // Wrap operation to time each individual attempt (excluding backoff wait time).
    CheckedSupplier<T> timedOperation =
        () -> {
          Timer.Sample attemptSample = Timer.start();
          try {
            return operation.get();
          } finally {
            attemptSample.stop(perAttemptTimer);
          }
        };

    // The latency metric here includes retries and is recorded for both success and failure.
    Timer.Sample sample = Timer.start();
    try {
      T result = Failsafe.with(this.retryPolicy).get(timedOperation);
      successCounter.increment();
      return result;

    } catch (Exception e) {
      // Detect transient server conditions across all operation types and callsites.
      // These are temporary (disk can be freed, auto-scaling kicks in, load subsides), so wrap
      // as transient so the upper retry loop can back off and retry.
      if (isDiskFull(e)) {
        this.mongodDiskFullErrors.increment();
        throw new MaterializedViewTransientException(
            e, MaterializedViewTransientException.Reason.EXCEEDED_DISK_LIMIT);
      }
      if (isUserWritesBlocked(e)) {
        this.mongodUserWritesBlockedErrors.increment();
        throw new MaterializedViewTransientException(
            e, MaterializedViewTransientException.Reason.USER_WRITES_BLOCKED);
      }
      if (isSystemOverloaded(e)) {
        this.mongodSystemOverloadErrors.increment();
        throw new MaterializedViewTransientException(
            e, MaterializedViewTransientException.Reason.SYSTEM_OVERLOADED);
      }
      if (e instanceof MongoBulkWriteException bulkEx) {
        boolean hasDiskFull =
            bulkEx.getWriteErrors().stream()
                .anyMatch(err -> err.getCode() == Errors.EXCEEDED_DISK_LIMIT.code);
        boolean hasUserWritesBlocked =
            bulkEx.getWriteErrors().stream()
                .anyMatch(err -> err.getCode() == Errors.USER_WRITES_BLOCKED.code);
        boolean hasSystemOverload =
            bulkEx.getWriteErrors().stream()
                .anyMatch(err -> Errors.SYSTEM_OVERLOADED_ERROR_CODES.contains(err.getCode()));
        if (hasDiskFull) {
          this.mongodDiskFullErrors.increment();
        }
        if (hasUserWritesBlocked) {
          this.mongodUserWritesBlockedErrors.increment();
        }
        if (hasSystemOverload) {
          this.mongodSystemOverloadErrors.increment();
        }
        if (hasDiskFull || hasUserWritesBlocked || hasSystemOverload) {
          // Priority: disk-full > writes-blocked > system-overloaded.
          // A bulk write can contain errors from multiple categories; we
          // pick the most specific reason for the metric tag.
          MaterializedViewTransientException.Reason reason =
              hasDiskFull
                  ? MaterializedViewTransientException.Reason.EXCEEDED_DISK_LIMIT
                  : hasUserWritesBlocked
                      ? MaterializedViewTransientException.Reason.USER_WRITES_BLOCKED
                      : MaterializedViewTransientException.Reason.SYSTEM_OVERLOADED;
          throw new MaterializedViewTransientException(e, reason);
        }
      }
      failureCounter.increment();
      throw e;

    } finally {
      sample.stop(timer);
    }
  }

  public void execute(String operationName, CheckedRunnable operation) throws Exception {
    execute(
        operationName,
        () -> {
          operation.run();
          return null;
        });
  }

  private static boolean isDiskFull(Throwable e) {
    Throwable cause = unwrapCheckedMongoException(e);
    return cause instanceof MongoException me && me.getCode() == Errors.EXCEEDED_DISK_LIMIT.code;
  }

  private static boolean isUserWritesBlocked(Throwable e) {
    Throwable cause = unwrapCheckedMongoException(e);
    return cause instanceof MongoException me && me.getCode() == Errors.USER_WRITES_BLOCKED.code;
  }

  private static boolean isSystemOverloaded(Throwable e) {
    Throwable cause = unwrapCheckedMongoException(e);
    return cause instanceof MongoException me
        && Errors.SYSTEM_OVERLOADED_ERROR_CODES.contains(me.getCode());
  }

  private static String classifyError(Throwable e) {
    Throwable cause = unwrapCheckedMongoException(e);
    if (cause instanceof MongoSocketException) {
      return "socket_error";
    }
    if (cause instanceof MongoTimeoutException) {
      return "timeout";
    }
    if (cause instanceof MongoNotPrimaryException) {
      return "not_primary";
    }
    if (cause instanceof MongoNodeIsRecoveringException) {
      return "node_recovering";
    }
    if (cause instanceof MongoCursorNotFoundException) {
      return "cursor_not_found";
    }
    if (cause instanceof MongoException mongoException) {
      return "mongo_error_" + mongoException.getCode();
    }
    if (cause instanceof IllegalStateException) {
      return "client_closed";
    }
    if (cause instanceof MaterializedViewTransientException) {
      return "mv_transient";
    }
    return "unknown";
  }

  private static boolean isRetryable(Throwable e) {
    Throwable cause = unwrapCheckedMongoException(e);
    if (cause instanceof IllegalStateException) {
      // When MongoClient is closed during sync source update, it will throw IllegalStateException.
      // This is not an ideal exception type check as MongoClient throws IllegalStateException when
      // it is closed instead of MongoException.
      return true;
    }
    if (cause instanceof MaterializedViewTransientException) {
      // MaterializedViewTransientException may be thrown when sync source is missing.
      return true;
    }
    if (!(cause instanceof MongoException mongoException)) {
      return false;
    }
    if (mongoException instanceof MongoSocketException
        || mongoException instanceof MongoNotPrimaryException
        || mongoException instanceof MongoNodeIsRecoveringException
        || mongoException instanceof MongoCursorNotFoundException
        || mongoException instanceof MongoTimeoutException) {
      return true;
    }
    return Errors.RETRYABLE_ERROR_CODES.contains(mongoException.getCode());
  }

  /**
   * {@link CheckedMongoException} is a thin wrapper around {@link MongoException} used by helpers
   * like {@link com.xgen.mongot.util.mongodb.MongoDbDatabase#getCollectionInfos}. Returns the
   * underlying cause so retryability classification, error tagging and transient-condition
   * detection (disk-full / writes-blocked / overloaded) treat the wrapped exception the same as a
   * directly-thrown one. Returns the input unchanged when not a {@link CheckedMongoException} or
   * when the cause is null.
   */
  private static Throwable unwrapCheckedMongoException(Throwable e) {
    if (e instanceof CheckedMongoException && e.getCause() != null) {
      return e.getCause();
    }
    return e;
  }
}
