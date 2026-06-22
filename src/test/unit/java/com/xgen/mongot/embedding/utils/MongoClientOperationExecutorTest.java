package com.xgen.mongot.embedding.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.mongodb.MongoBulkWriteException;
import com.mongodb.MongoCommandException;
import com.mongodb.MongoSocketReadException;
import com.mongodb.MongoTimeoutException;
import com.mongodb.MongoWriteException;
import com.mongodb.ServerAddress;
import com.mongodb.WriteError;
import com.mongodb.bulk.BulkWriteError;
import com.xgen.mongot.embedding.exceptions.MaterializedViewTransientException;
import com.xgen.mongot.metrics.MetricsFactory;
import com.xgen.mongot.util.mongodb.Errors;
import com.xgen.mongot.util.retry.ExponentialBackoffPolicy;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.junit.Test;

public class MongoClientOperationExecutorTest {

  private static final String PREFIX = "test";

  private MongoClientOperationExecutor newExecutor(SimpleMeterRegistry registry) {
    return new MongoClientOperationExecutor(new MetricsFactory(PREFIX, registry), "resource");
  }

  private static MongoCommandException commandException(int code, String msg) {
    return new MongoCommandException(
        new BsonDocument("ok", new BsonInt32(0))
            .append("errmsg", new BsonString(msg))
            .append("code", new BsonInt32(code)),
        new ServerAddress());
  }

  private static MongoBulkWriteException bulkWriteException(int code, String msg) {
    BulkWriteError error = new BulkWriteError(code, msg, new BsonDocument(), 0);
    MongoBulkWriteException ex = mock(MongoBulkWriteException.class);
    when(ex.getWriteErrors()).thenReturn(List.of(error));
    return ex;
  }

  private static MongoWriteException writeException(int code, String msg) {
    return new MongoWriteException(
        new WriteError(code, msg, new BsonDocument()), new ServerAddress());
  }

  // --- ExceededDiskLimit (14031) via MongoCommandException ---

  @Test
  public void diskFull_commandException_throwsTransient() {
    SimpleMeterRegistry registry = new SimpleMeterRegistry();
    MongoClientOperationExecutor executor = newExecutor(registry);

    assertThrows(
        MaterializedViewTransientException.class,
        () ->
            executor.execute(
                "bulkWrite",
                () -> {
                  throw commandException(Errors.EXCEEDED_DISK_LIMIT.code, "disk limit exceeded");
                }));
  }

  @Test
  public void diskFull_commandException_incrementsMetric() throws Exception {
    SimpleMeterRegistry registry = new SimpleMeterRegistry();
    MongoClientOperationExecutor executor = newExecutor(registry);

    try {
      executor.execute(
          "bulkWrite",
          () -> {
            throw commandException(Errors.EXCEEDED_DISK_LIMIT.code, "disk limit exceeded");
          });
    } catch (MaterializedViewTransientException ignored) {
      // ignored
    }

    Counter counter = registry.find(PREFIX + ".mongodDiskFullErrors").counter();
    assertNotNull(counter);
    assertEquals(1, (int) counter.count());
  }

  // --- ExceededDiskLimit (14031) via MongoBulkWriteException ---

  @Test
  public void diskFull_bulkWriteException_throwsTransient() {
    SimpleMeterRegistry registry = new SimpleMeterRegistry();
    MongoClientOperationExecutor executor = newExecutor(registry);

    assertThrows(
        MaterializedViewTransientException.class,
        () ->
            executor.execute(
                "bulkWrite",
                () -> {
                  throw bulkWriteException(Errors.EXCEEDED_DISK_LIMIT.code, "disk limit exceeded");
                }));
  }

  @Test
  public void diskFull_bulkWriteException_incrementsMetric() throws Exception {
    SimpleMeterRegistry registry = new SimpleMeterRegistry();
    MongoClientOperationExecutor executor = newExecutor(registry);

    try {
      executor.execute(
          "bulkWrite",
          () -> {
            throw bulkWriteException(Errors.EXCEEDED_DISK_LIMIT.code, "disk limit exceeded");
          });
    } catch (MaterializedViewTransientException ignored) {
      // ignored
    }

    Counter counter = registry.find(PREFIX + ".mongodDiskFullErrors").counter();
    assertNotNull(counter);
    assertEquals(1, (int) counter.count());
  }

  // --- UserWritesBlocked (371) via MongoCommandException ---

  @Test
  public void userWritesBlocked_commandException_throwsTransient() {
    SimpleMeterRegistry registry = new SimpleMeterRegistry();
    MongoClientOperationExecutor executor = newExecutor(registry);

    assertThrows(
        MaterializedViewTransientException.class,
        () ->
            executor.execute(
                "deleteOne",
                () -> {
                  throw commandException(Errors.USER_WRITES_BLOCKED.code, "user writes blocked");
                }));
  }

  @Test
  public void userWritesBlocked_commandException_incrementsMetric() throws Exception {
    SimpleMeterRegistry registry = new SimpleMeterRegistry();
    MongoClientOperationExecutor executor = newExecutor(registry);

    try {
      executor.execute(
          "deleteOne",
          () -> {
            throw commandException(Errors.USER_WRITES_BLOCKED.code, "user writes blocked");
          });
    } catch (MaterializedViewTransientException ignored) {
      // ignored
    }

    Counter counter = registry.find(PREFIX + ".mongodUserWritesBlockedErrors").counter();
    assertNotNull(counter);
    assertEquals(1, (int) counter.count());
  }

  // --- UserWritesBlocked (371) via MongoBulkWriteException ---

  @Test
  public void userWritesBlocked_bulkWriteException_throwsTransient() {
    SimpleMeterRegistry registry = new SimpleMeterRegistry();
    MongoClientOperationExecutor executor = newExecutor(registry);

    assertThrows(
        MaterializedViewTransientException.class,
        () ->
            executor.execute(
                "bulkWrite",
                () -> {
                  throw bulkWriteException(Errors.USER_WRITES_BLOCKED.code, "user writes blocked");
                }));
  }

  @Test
  public void userWritesBlocked_bulkWriteException_incrementsMetric() throws Exception {
    SimpleMeterRegistry registry = new SimpleMeterRegistry();
    MongoClientOperationExecutor executor = newExecutor(registry);

    try {
      executor.execute(
          "bulkWrite",
          () -> {
            throw bulkWriteException(Errors.USER_WRITES_BLOCKED.code, "user writes blocked");
          });
    } catch (MaterializedViewTransientException ignored) {
      // ignored
    }

    Counter counter = registry.find(PREFIX + ".mongodUserWritesBlockedErrors").counter();
    assertNotNull(counter);
    assertEquals(1, (int) counter.count());
  }

  // --- SystemOverloadedError codes (433, 449, 450, 473) ---

  @Test
  public void admissionQueueOverflow_commandException_throwsTransient() {
    SimpleMeterRegistry registry = new SimpleMeterRegistry();
    MongoClientOperationExecutor executor = newExecutor(registry);

    assertThrows(
        MaterializedViewTransientException.class,
        () ->
            executor.execute(
                "bulkWrite",
                () -> {
                  throw commandException(
                      Errors.ADMISSION_QUEUE_OVERFLOW.code, "admission queue overflow");
                }));

    Counter counter = registry.find(PREFIX + ".mongodSystemOverloadErrors").counter();
    assertNotNull(counter);
    assertEquals(1, (int) counter.count());
  }

  @Test
  public void interruptedDueToOverload_commandException_throwsTransient() {
    SimpleMeterRegistry registry = new SimpleMeterRegistry();
    MongoClientOperationExecutor executor = newExecutor(registry);

    assertThrows(
        MaterializedViewTransientException.class,
        () ->
            executor.execute(
                "deleteOne",
                () -> {
                  throw commandException(
                      Errors.INTERRUPTED_DUE_TO_OVERLOAD.code, "interrupted due to overload");
                }));

    Counter counter = registry.find(PREFIX + ".mongodSystemOverloadErrors").counter();
    assertNotNull(counter);
    assertEquals(1, (int) counter.count());
  }

  @Test
  public void systemOverload_bulkWriteException_throwsTransient() {
    SimpleMeterRegistry registry = new SimpleMeterRegistry();
    MongoClientOperationExecutor executor = newExecutor(registry);

    assertThrows(
        MaterializedViewTransientException.class,
        () ->
            executor.execute(
                "bulkWrite",
                () -> {
                  throw bulkWriteException(
                      Errors.ADMISSION_QUEUE_OVERFLOW.code, "admission queue overflow");
                }));

    Counter counter = registry.find(PREFIX + ".mongodSystemOverloadErrors").counter();
    assertNotNull(counter);
    assertEquals(1, (int) counter.count());
  }

  // --- ExceededDiskLimit (14031) via MongoWriteException ---

  @Test
  public void diskFull_writeException_throwsTransient() {
    SimpleMeterRegistry registry = new SimpleMeterRegistry();
    MongoClientOperationExecutor executor = newExecutor(registry);

    assertThrows(
        MaterializedViewTransientException.class,
        () ->
            executor.execute(
                "bulkWrite",
                () -> {
                  throw writeException(Errors.EXCEEDED_DISK_LIMIT.code, "disk limit exceeded");
                }));

    Counter counter = registry.find(PREFIX + ".mongodDiskFullErrors").counter();
    assertNotNull(counter);
    assertEquals(1, (int) counter.count());
  }

  // --- UserWritesBlocked (371) via MongoWriteException ---

  @Test
  public void userWritesBlocked_writeException_throwsTransient() {
    SimpleMeterRegistry registry = new SimpleMeterRegistry();
    MongoClientOperationExecutor executor = newExecutor(registry);

    assertThrows(
        MaterializedViewTransientException.class,
        () ->
            executor.execute(
                "deleteOne",
                () -> {
                  throw writeException(Errors.USER_WRITES_BLOCKED.code, "user writes blocked");
                }));

    Counter counter = registry.find(PREFIX + ".mongodUserWritesBlockedErrors").counter();
    assertNotNull(counter);
    assertEquals(1, (int) counter.count());
  }

  // --- SystemOverloadedError codes via MongoWriteException ---

  @Test
  public void systemOverload_writeException_throwsTransient() {
    SimpleMeterRegistry registry = new SimpleMeterRegistry();
    MongoClientOperationExecutor executor = newExecutor(registry);

    assertThrows(
        MaterializedViewTransientException.class,
        () ->
            executor.execute(
                "bulkWrite",
                () -> {
                  throw writeException(
                      Errors.ADMISSION_QUEUE_OVERFLOW.code, "admission queue overflow");
                }));

    Counter counter = registry.find(PREFIX + ".mongodSystemOverloadErrors").counter();
    assertNotNull(counter);
    assertEquals(1, (int) counter.count());
  }

  // --- Other errors are not intercepted ---

  @Test
  public void otherMongoCommandException_rethrowsUnwrapped() {
    SimpleMeterRegistry registry = new SimpleMeterRegistry();
    MongoClientOperationExecutor executor = newExecutor(registry);

    assertThrows(
        MongoCommandException.class,
        () ->
            executor.execute(
                "findOne",
                () -> {
                  throw commandException(2, "bad value");
                }));
  }

  // --- Retryable errors are retried and can succeed ---

  @Test
  public void retryableError_succeedsAfterRetry() throws Exception {
    SimpleMeterRegistry registry = new SimpleMeterRegistry();
    MongoClientOperationExecutor executor = newExecutor(registry);
    AtomicInteger attempts = new AtomicInteger(0);

    String result =
        executor.execute(
            "bulkWrite",
            () -> {
              if (attempts.incrementAndGet() == 1) {
                throw new MongoTimeoutException("timeout");
              }
              return "success";
            });

    assertEquals("success", result);
    assertEquals(2, attempts.get());
  }

  // --- shouldAbortRetry stops retries when closed ---

  @Test
  public void shouldAbortRetry_stopsRetryWhenTrue() {
    SimpleMeterRegistry registry = new SimpleMeterRegistry();
    AtomicBoolean closed = new AtomicBoolean(false);
    MongoClientOperationExecutor executor =
        new MongoClientOperationExecutor(
            new MetricsFactory(PREFIX, registry),
            "resource",
            ExponentialBackoffPolicy.builder()
                .initialDelay(Duration.ofMillis(10))
                .backoffFactor(2)
                .maxDelay(Duration.ofMillis(100))
                .maxRetries(5)
                .jitter(0)
                .build(),
            closed::get);
    AtomicInteger attempts = new AtomicInteger(0);

    assertThrows(
        MongoTimeoutException.class,
        () ->
            executor.execute(
                "bulkWrite",
                () -> {
                  attempts.incrementAndGet();
                  closed.set(true);
                  throw new MongoTimeoutException("timeout");
                }));

    assertEquals(1, attempts.get());
  }

  @Test
  public void shouldAbortRetry_retriesWhenFalse() throws Exception {
    SimpleMeterRegistry registry = new SimpleMeterRegistry();
    MongoClientOperationExecutor executor =
        new MongoClientOperationExecutor(
            new MetricsFactory(PREFIX, registry),
            "resource",
            ExponentialBackoffPolicy.builder()
                .initialDelay(Duration.ofMillis(10))
                .backoffFactor(2)
                .maxDelay(Duration.ofMillis(100))
                .maxRetries(5)
                .jitter(0)
                .build(),
            () -> false);
    AtomicInteger attempts = new AtomicInteger(0);

    String result =
        executor.execute(
            "bulkWrite",
            () -> {
              if (attempts.incrementAndGet() == 1) {
                throw new MongoTimeoutException("timeout");
              }
              return "success";
            });

    assertEquals("success", result);
    assertEquals(2, attempts.get());
  }

  // --- Per-attempt retry metrics ---

  @Test
  public void retryAttempts_socketError_recordsErrorReason() throws Exception {
    SimpleMeterRegistry registry = new SimpleMeterRegistry();
    MongoClientOperationExecutor executor = newExecutor(registry);

    AtomicInteger attempts = new AtomicInteger(0);
    executor.execute(
        "bulkWrite",
        () -> {
          if (attempts.getAndIncrement() < 2) {
            throw new MongoSocketReadException("connection reset", new ServerAddress());
          }
          return "ok";
        });

    Counter retryCounter =
        registry
            .find(PREFIX + ".resource.retriedAttempts")
            .tag("errorReason", "socket_error")
            .counter();
    assertNotNull("Retry counter should exist", retryCounter);
    assertEquals(2.0, retryCounter.count(), 0.0);
  }

  @Test
  public void retryAttempts_timeout_recordsErrorReason() throws Exception {
    SimpleMeterRegistry registry = new SimpleMeterRegistry();
    MongoClientOperationExecutor executor = newExecutor(registry);

    AtomicInteger attempts = new AtomicInteger(0);
    executor.execute(
        "readCheckpoint",
        () -> {
          if (attempts.getAndIncrement() < 1) {
            throw new MongoTimeoutException("timed out");
          }
          return "ok";
        });

    Counter retryCounter =
        registry.find(PREFIX + ".resource.retriedAttempts").tag("errorReason", "timeout").counter();
    assertNotNull("Retry counter should exist", retryCounter);
    assertEquals(1.0, retryCounter.count(), 0.0);
  }

  @Test
  public void retryAttempts_mongoErrorCode_recordsCode() throws Exception {
    SimpleMeterRegistry registry = new SimpleMeterRegistry();
    MongoClientOperationExecutor executor = newExecutor(registry);

    AtomicInteger attempts = new AtomicInteger(0);
    // Error code 91 is in RETRYABLE_ERROR_CODES
    executor.execute(
        "findOne",
        () -> {
          if (attempts.getAndIncrement() < 1) {
            throw commandException(91, "shutdown in progress");
          }
          return "ok";
        });

    Counter retryCounter =
        registry
            .find(PREFIX + ".resource.retriedAttempts")
            .tag("errorReason", "mongo_error_91")
            .counter();
    assertNotNull("Retry counter should exist", retryCounter);
    assertEquals(1.0, retryCounter.count(), 0.0);
  }

  @Test
  public void retryAttempts_noRetries_noCounter() throws Exception {
    SimpleMeterRegistry registry = new SimpleMeterRegistry();
    MongoClientOperationExecutor executor = newExecutor(registry);

    executor.execute("readCheckpoint", () -> "ok");

    assertTrue(
        "No retry counters should exist when operation succeeds first try",
        registry.find(PREFIX + ".resource.retriedAttempts").counters().isEmpty());
  }

  // --- Per-attempt latency timer ---

  @Test
  public void perAttemptLatency_recordsEachAttempt() throws Exception {
    SimpleMeterRegistry registry = new SimpleMeterRegistry();
    MongoClientOperationExecutor executor = newExecutor(registry);

    AtomicInteger attempts = new AtomicInteger(0);
    executor.execute(
        "bulkWrite",
        () -> {
          if (attempts.getAndIncrement() < 2) {
            throw new MongoSocketReadException("connection reset", new ServerAddress());
          }
          return "ok";
        });

    Timer perAttemptTimer =
        registry.find(PREFIX + ".resource.perAttemptLatency").tag("operation", "bulkWrite").timer();
    assertNotNull("Per-attempt timer should exist", perAttemptTimer);
    // 3 attempts: 2 failures + 1 success
    assertEquals(3, perAttemptTimer.count());
  }

  @Test
  public void perAttemptLatency_singleAttempt_recordsOnce() throws Exception {
    SimpleMeterRegistry registry = new SimpleMeterRegistry();
    MongoClientOperationExecutor executor = newExecutor(registry);

    executor.execute("readCheckpoint", () -> "ok");

    Timer perAttemptTimer =
        registry
            .find(PREFIX + ".resource.perAttemptLatency")
            .tag("operation", "readCheckpoint")
            .timer();
    assertNotNull("Per-attempt timer should exist", perAttemptTimer);
    assertEquals(1, perAttemptTimer.count());
  }
}
