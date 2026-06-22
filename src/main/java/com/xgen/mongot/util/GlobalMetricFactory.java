package com.xgen.mongot.util;

import com.google.common.annotations.VisibleForTesting;
import com.google.errorprone.annotations.CompileTimeConstant;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Singleton global metric factory for metrics that should exist globally across Mongot, e.g.,
 * unreachable code counters.
 *
 * <p>This version is self-contained and does not depend on MetricsFactory.
 */
public final class GlobalMetricFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(GlobalMetricFactory.class);
  private static volatile Optional<MeterRegistry> meterRegistry = Optional.empty();

  private GlobalMetricFactory() {}

  /** Initialize the global metric factory. Should be called once at startup. */
  public static synchronized void initialize(MeterRegistry registry) {
    if (meterRegistry.isPresent()) {
      LOGGER.debug("GlobalMetricFactory already initialized.");
      return;
    }
    meterRegistry = Optional.of(registry);
    LOGGER.info("GlobalMetricFactory initialized successfully.");
  }

  /**
   * Returns an unreachable counter labeled by reason.
   *
   * @param reason the compile-time constant reason for the unreachable path
   * @param registry the initialized MeterRegistry
   * @return the counter corresponding to the given reason
   */
  private static Counter unreachableCounter(
      @CompileTimeConstant String reason, MeterRegistry registry) {
    return Counter.builder("mongot.unreachable_code_paths")
        .description("Counts unreachable code paths, labeled by reason")
        .tags(Tags.of("reason", reason))
        .register(registry);
  }

  /**
   * Increments the unreachable counter for a given reason.
   *
   * @param reason the compile-time constant reason for the unreachable path
   */
  public static void incrementUnreachable(@CompileTimeConstant String reason) {
    Optional<MeterRegistry> maybeRegistry = meterRegistry;
    if (maybeRegistry.isEmpty()) {
      LOGGER
          .atError()
          .setCause(new Throwable("stacktrace"))
          .addKeyValue("reason", reason)
          .log(
              "Invariant violated (this is a bug). "
                  + "Please file a report with the attached stacktrace");
      return;
    }

    MeterRegistry registry = maybeRegistry.get();
    unreachableCounter(reason, registry).increment();
  }

  /**
   * Increments the counter for unloggable ID types.
   *
   * @param bsonType the BSON type that could not be logged
   */
  public static void incrementUnloggableIdType(String bsonType) {
    Optional<MeterRegistry> maybeRegistry = meterRegistry;
    if (maybeRegistry.isEmpty()) {
      LOGGER
          .atWarn()
          .addKeyValue("bsonType", bsonType)
          .log("GlobalMetricFactory not initialized, cannot record unloggable ID type metric");
      return;
    }

    MeterRegistry registry = maybeRegistry.get();
    Counter.builder("mongot.unloggable_id_types")
        .description("Counts document IDs with types that cannot be logged")
        .tags(Tags.of("bsonType", bsonType))
        .register(registry)
        .increment();
  }

  @VisibleForTesting
  static void resetForTest() {
    meterRegistry = Optional.empty();
  }

  @VisibleForTesting
  static Optional<MeterRegistry> getRegistryForTest() {
    return meterRegistry;
  }
}
