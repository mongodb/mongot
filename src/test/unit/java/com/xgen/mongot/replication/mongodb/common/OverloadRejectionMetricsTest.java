package com.xgen.mongot.replication.mongodb.common;

import static com.google.common.truth.Truth.assertThat;

import com.mongodb.MongoCommandException;
import com.mongodb.ServerAddress;
import com.xgen.mongot.util.mongodb.Errors;
import io.micrometer.core.instrument.search.MeterNotFoundException;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.junit.Test;

public class OverloadRejectionMetricsTest {

  @Test
  public void recordIfOverload_rateLimitExceeded_incrementsCodeAndPhase() {
    var meterRegistry = new SimpleMeterRegistry();
    var metrics =
        OverloadRejectionMetrics.create(meterRegistry, OverloadRejectionMetrics.Phase.STEADY_STATE);

    metrics.recordIfOverload(commandException(Errors.INGRESS_REQUEST_RATE_LIMIT_EXCEEDED.code));

    assertThat(count(meterRegistry, Errors.INGRESS_REQUEST_RATE_LIMIT_EXCEEDED.code, "steadyState"))
        .isEqualTo(1.0);
  }

  @Test
  public void recordIfOverload_initialSyncPhase_taggedSeparatelyFromSteadyState() {
    var meterRegistry = new SimpleMeterRegistry();
    var initialSync =
        OverloadRejectionMetrics.create(meterRegistry, OverloadRejectionMetrics.Phase.INITIAL_SYNC);
    var steadyState =
        OverloadRejectionMetrics.create(meterRegistry, OverloadRejectionMetrics.Phase.STEADY_STATE);

    initialSync.recordIfOverload(commandException(Errors.INGRESS_REQUEST_RATE_LIMIT_EXCEEDED.code));
    initialSync.recordIfOverload(commandException(Errors.INGRESS_REQUEST_RATE_LIMIT_EXCEEDED.code));
    steadyState.recordIfOverload(commandException(Errors.INGRESS_REQUEST_RATE_LIMIT_EXCEEDED.code));

    assertThat(count(meterRegistry, Errors.INGRESS_REQUEST_RATE_LIMIT_EXCEEDED.code, "initialSync"))
        .isEqualTo(2.0);
    assertThat(count(meterRegistry, Errors.INGRESS_REQUEST_RATE_LIMIT_EXCEEDED.code, "steadyState"))
        .isEqualTo(1.0);
  }

  @Test
  public void recordIfOverload_wrappedCause_isFound() {
    var meterRegistry = new SimpleMeterRegistry();
    var metrics =
        OverloadRejectionMetrics.create(meterRegistry, OverloadRejectionMetrics.Phase.INITIAL_SYNC);

    // How the error actually reaches us: the driver exception nested inside our own wrapper.
    metrics.recordIfOverload(
        new RuntimeException("wrapper", commandException(Errors.ADMISSION_QUEUE_OVERFLOW.code)));

    assertThat(count(meterRegistry, Errors.ADMISSION_QUEUE_OVERFLOW.code, "initialSync"))
        .isEqualTo(1.0);
  }

  @Test
  public void recordIfOverload_nonOverloadError_doesNotIncrementAnything() {
    var meterRegistry = new SimpleMeterRegistry();
    var metrics =
        OverloadRejectionMetrics.create(meterRegistry, OverloadRejectionMetrics.Phase.STEADY_STATE);

    metrics.recordIfOverload(commandException(Errors.CURSOR_NOT_FOUND.code));
    metrics.recordIfOverload(new RuntimeException("not a mongo error"));

    for (int code : Errors.OVERLOAD_REJECTION_ERROR_CODES) {
      assertThat(count(meterRegistry, code, "steadyState")).isEqualTo(0.0);
    }
  }

  @Test
  public void create_registersEveryCodeEagerly() {
    var meterRegistry = new SimpleMeterRegistry();
    OverloadRejectionMetrics.create(meterRegistry, OverloadRejectionMetrics.Phase.STEADY_STATE);

    // A phase that has seen no rejections must read zero rather than be absent, so a dashboard
    // does not render a healthy cluster as a gap in the series.
    for (int code : Errors.OVERLOAD_REJECTION_ERROR_CODES) {
      assertThat(count(meterRegistry, code, "steadyState")).isEqualTo(0.0);
    }
  }

  @Test
  public void create_doesNotRegisterCodesOutsideTheOverloadSet() {
    var meterRegistry = new SimpleMeterRegistry();
    OverloadRejectionMetrics.create(meterRegistry, OverloadRejectionMetrics.Phase.STEADY_STATE);

    assertThat(Errors.OVERLOAD_REJECTION_ERROR_CODES)
        .doesNotContain(Errors.RATE_LIMIT_EXCEEDED.code);
    try {
      count(meterRegistry, Errors.RATE_LIMIT_EXCEEDED.code, "steadyState");
      throw new AssertionError("expected no counter for a non-overload code");
    } catch (MeterNotFoundException expected) {
      // Only the codes we count are registered.
    }
  }

  @Test
  public void recordIfOverload_everyOverloadCode_isCounted() {
    var meterRegistry = new SimpleMeterRegistry();
    var metrics =
        OverloadRejectionMetrics.create(meterRegistry, OverloadRejectionMetrics.Phase.STEADY_STATE);

    // 462 is the ingress rate limiter; 473 and 433 come from execution control. All three are
    // counted, including the codes the same-cursor retry cannot handle, because the metric is about
    // how often mongod shed us rather than what we could do about it.
    for (int code : Errors.OVERLOAD_REJECTION_ERROR_CODES) {
      metrics.recordIfOverload(commandException(code));
      assertThat(count(meterRegistry, code, "steadyState")).isEqualTo(1.0);
    }
  }

  private static double count(SimpleMeterRegistry meterRegistry, int code, String phase) {
    return meterRegistry
        .get("replication.overloadRejections")
        .tag("code", String.valueOf(code))
        .tag("phase", phase)
        .counter()
        .count();
  }

  private static MongoCommandException commandException(int code) {
    return new MongoCommandException(
        new BsonDocument("ok", new BsonInt32(0))
            .append("code", new BsonInt32(code))
            .append("errmsg", new BsonString("test")),
        new ServerAddress());
  }
}
