package com.xgen.mongot.replication.mongodb.common;

import static com.google.common.truth.Truth.assertThat;

import com.google.errorprone.annotations.Var;
import com.mongodb.MongoCommandException;
import com.mongodb.MongoException;
import com.mongodb.ServerAddress;
import com.xgen.mongot.util.mongodb.Errors;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.junit.Test;

public class MongoExceptionUtilsTest {

  @Test
  public void isRetryable_interruptedDueToOverload_returnsTrue() {
    MongoException exception = commandException(Errors.INTERRUPTED_DUE_TO_OVERLOAD.code);

    assertThat(MongoExceptionUtils.isRetryable(exception)).isTrue();
  }

  @Test
  public void isRetryable_ingressRequestRateLimitExceeded_returnsTrue() {
    MongoException exception = commandException(Errors.INGRESS_REQUEST_RATE_LIMIT_EXCEEDED.code);

    assertThat(MongoExceptionUtils.isRetryable(exception)).isTrue();
  }

  @Test
  public void isRetryable_admissionQueueOverflow_returnsTrue() {
    MongoException exception = commandException(Errors.ADMISSION_QUEUE_OVERFLOW.code);

    assertThat(MongoExceptionUtils.isRetryable(exception)).isTrue();
  }

  @Test
  public void isThrottling_admissionQueueOverflow_returnsTrue() {
    // 433 is thrown at ticket acquisition, before the operation runs: the cursor is untouched, so
    // it is eligible for the same-cursor retry like 462 (CLOUDP-450070). Contrast 473, which
    // interrupts a running operation and gets the cursor deleted server-side.
    assertThat(
            MongoExceptionUtils.isThrottling(
                commandException(Errors.ADMISSION_QUEUE_OVERFLOW.code)))
        .isTrue();
  }

  @Test
  public void isThrottling_ingressRequestRateLimitExceeded_returnsTrue() {
    assertThat(
            MongoExceptionUtils.isThrottling(
                commandException(Errors.INGRESS_REQUEST_RATE_LIMIT_EXCEEDED.code)))
        .isTrue();
  }

  @Test
  public void isThrottling_interruptedDueToOverload_returnsFalse() {
    // 473 arrives by killing the running operation, which kills its cursor: retrying the same
    // cursor cannot succeed, so it is retryable but not retried in place.
    assertThat(
            MongoExceptionUtils.isThrottling(
                commandException(Errors.INTERRUPTED_DUE_TO_OVERLOAD.code)))
        .isFalse();
  }

  @Test
  public void isThrottling_otherRetryableCode_returnsFalse() {
    assertThat(MongoExceptionUtils.isThrottling(commandException(Errors.CURSOR_NOT_FOUND.code)))
        .isFalse();
  }

  @Test
  public void isThrottling_throttlingCauseWrappedInSteadyStateException_returnsTrue() {
    SteadyStateException wrapped =
        SteadyStateException.wrapMongoException(
            commandException(Errors.INGRESS_REQUEST_RATE_LIMIT_EXCEEDED.code));

    assertThat(MongoExceptionUtils.isThrottling((Throwable) wrapped)).isTrue();
  }

  @Test
  public void isThrottling_nonMongoThrowable_returnsFalse() {
    assertThat(MongoExceptionUtils.isThrottling(new RuntimeException("nope"))).isFalse();
  }

  @Test
  public void isThrottling_cyclicCauseChain_terminates() {
    // Throwable.initCause rejects a self-cycle, but a longer cycle is constructible. The walk must
    // not spin forever on one.
    RuntimeException first = new RuntimeException("first");
    RuntimeException second = new RuntimeException("second", first);
    first.initCause(second);

    assertThat(MongoExceptionUtils.isThrottling(first)).isFalse();
  }

  @Test
  public void isThrottling_throttlingCauseDeeperThanCauseCap_returnsFalse() {
    // Documents the depth cap: a throttling cause buried deeper than MAX_CAUSE_DEPTH is not found.
    // Real wrapping is one or two layers, so this only bounds pathological chains.
    Throwable deeplyNested =
        nest(commandException(Errors.INGRESS_REQUEST_RATE_LIMIT_EXCEEDED.code), 20);

    assertThat(MongoExceptionUtils.isThrottling(deeplyNested)).isFalse();
  }

  @Test
  public void overloadErrorCode_overloadCodes_returnTheCode() {
    for (int code : Errors.OVERLOAD_REJECTION_ERROR_CODES) {
      assertThat(MongoExceptionUtils.overloadErrorCode(commandException(code)).getAsInt())
          .isEqualTo(code);
    }
  }

  @Test
  public void overloadErrorCode_wrappedCause_isFound() {
    SteadyStateException wrapped =
        SteadyStateException.wrapMongoException(
            commandException(Errors.INGRESS_REQUEST_RATE_LIMIT_EXCEEDED.code));

    assertThat(MongoExceptionUtils.overloadErrorCode(wrapped).getAsInt())
        .isEqualTo(Errors.INGRESS_REQUEST_RATE_LIMIT_EXCEEDED.code);
  }

  @Test
  public void overloadErrorCode_nonOverloadError_returnsEmpty() {
    assertThat(
            MongoExceptionUtils.overloadErrorCode(commandException(Errors.CURSOR_NOT_FOUND.code)))
        .isEmpty();
    assertThat(MongoExceptionUtils.overloadErrorCode(new RuntimeException("nope"))).isEmpty();
  }

  @Test
  public void overloadErrorCode_cyclicCauseChain_terminates() {
    RuntimeException first = new RuntimeException("first");
    RuntimeException second = new RuntimeException("second", first);
    first.initCause(second);

    assertThat(MongoExceptionUtils.overloadErrorCode(first)).isEmpty();
  }

  /** Wraps {@code cause} in {@code layers} nested RuntimeExceptions. */
  private static Throwable nest(Throwable cause, int layers) {
    @Var Throwable nested = cause;
    for (int i = 0; i < layers; i++) {
      nested = new RuntimeException("layer " + i, nested);
    }
    return nested;
  }

  private static MongoCommandException commandException(int code) {
    return new MongoCommandException(
        new BsonDocument("ok", new BsonInt32(0))
            .append("code", new BsonInt32(code))
            .append("errmsg", new BsonString("test")),
        new ServerAddress());
  }
}
