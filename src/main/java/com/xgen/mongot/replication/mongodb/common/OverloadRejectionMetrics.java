package com.xgen.mongot.replication.mongodb.common;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.xgen.mongot.metrics.MetricsFactory;
import com.xgen.mongot.util.mongodb.Errors;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import java.util.OptionalInt;

/**
 * Counts replication requests rejected by mongod because it is shedding load, broken out by
 * replication phase and error code.
 *
 * <p>Exported as {@code mongot_replication_overloadRejections_total{phase,code}}.
 *
 * <p><b>Scope: the cursor data path.</b> Counted are the requests issued by the change-stream and
 * collection-scan clients. Includes the aggregate that opens a cursor (getNext). Doesn't count
 * control-plane one-off calls, e.g. {@code fsync}.
 *
 * <p>Counters are registered eagerly for every known code so that a phase reporting no rejections
 * is visibly zero rather than an absent series, which a dashboard would otherwise render as a gap.
 */
public final class OverloadRejectionMetrics {

  /** The replication phase a rejection occurred in. */
  public enum Phase {
    STEADY_STATE("steadyState"),
    INITIAL_SYNC("initialSync");

    private final String tagValue;

    Phase(String tagValue) {
      this.tagValue = tagValue;
    }

    String tagValue() {
      return this.tagValue;
    }
  }

  private static final String METRIC_NAME = "overloadRejections";
  private static final String NAMESPACE = "replication";

  private final ImmutableMap<Integer, Counter> countersByCode;

  private OverloadRejectionMetrics(ImmutableMap<Integer, Counter> countersByCode) {
    this.countersByCode = countersByCode;
  }

  public static OverloadRejectionMetrics create(MeterRegistry meterRegistry, Phase phase) {
    var metricsFactory = new MetricsFactory(NAMESPACE, meterRegistry);
    return new OverloadRejectionMetrics(
        ImmutableMap.copyOf(
            Maps.toMap(
                Errors.OVERLOAD_REJECTION_ERROR_CODES,
                code ->
                    metricsFactory.counter(
                        METRIC_NAME,
                        Tags.of("phase", phase.tagValue()).and("code", String.valueOf(code))))));
  }

  /**
   * Increments the counter for {@code throwable}'s overload error code, if it carries one. Safe to
   * call on any exception; non-overload exceptions are ignored.
   */
  public void recordIfOverload(Throwable throwable) {
    OptionalInt code = MongoExceptionUtils.overloadErrorCode(throwable);
    if (code.isEmpty()) {
      return;
    }
    // Present for every code in OVERLOAD_REJECTION_ERROR_CODES, which is what overloadErrorCode
    // matches against, so the counter is never null.
    this.countersByCode.get(code.getAsInt()).increment();
  }
}
