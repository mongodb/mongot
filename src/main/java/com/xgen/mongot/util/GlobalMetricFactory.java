package com.xgen.mongot.util;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.flogger.FluentLogger;
import com.google.errorprone.annotations.CompileTimeConstant;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
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
  private static final FluentLogger FLOGGER = FluentLogger.forEnclosingClass();
  private static volatile Optional<MeterRegistry> meterRegistry = Optional.empty();

  // In Grafana, these translate to mongot_<metricName>_total
  static final String BLOOM_FILTER_ID_LOOKUP_SKIPPED = "bloom_filter_id_lookup_skipped";
  static final String BLOOM_FILTER_ID_LOOKUP_PROBED = "bloom_filter_id_lookup_probed";
  static final String BLOOM_FILTER_ID_LOOKUP_FALSE_POSITIVE =
      "bloom_filter_id_lookup_false_positive";
  static final String BLOOM_FILTER_SATURATED_DROPPED = "bloom_filter_saturated_dropped";

  /**
   * Eager-register only when:
   *
   * <ol>
   *   <li>the metric is queried by an alert/dashboard
   *   <li>the tag set is bounded and known at startup
   *   <li>a missing series would be misread as an outage
   * </ol>
   */
  private static final Map<String, String> EAGER_COUNTER_TO_DESCRIPTIONS =
      Map.of(
          BLOOM_FILTER_ID_LOOKUP_SKIPPED,
          "Counts _id lookups skipped because the bloom filter reported the term as absent",
          BLOOM_FILTER_ID_LOOKUP_PROBED,
          "Counts _id lookups the bloom filter admitted, requiring a delegate postings lookup",
          BLOOM_FILTER_ID_LOOKUP_FALSE_POSITIVE,
          "Counts _id bloom filter false positives (admitted but no matching delegate term)",
          BLOOM_FILTER_SATURATED_DROPPED,
          "Counts per-field bloom filters dropped at write time because the FuzzySet saturated");

  private GlobalMetricFactory() {}

  /** Initialize the global metric factory. Should be called once at startup. */
  public static synchronized void initialize(MeterRegistry registry) {
    if (meterRegistry.isPresent()) {
      LOGGER.debug("GlobalMetricFactory already initialized.");
      return;
    }
    meterRegistry = Optional.of(registry);
    registerEagerCounters(registry);
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

  /**
   * Increments the counter tracking {@code _id} bloom-filter probes that were skipped because the
   * in-memory filter reported the term as definitely absent (the fast-fail that avoids a delegate
   * postings lookup).
   */
  public static void incrementBloomFilterIdLookupSkipped() {
    incrementBloomCounter(BLOOM_FILTER_ID_LOOKUP_SKIPPED, 1);
  }

  /**
   * Increments the counter tracking {@code _id} bloom-filter probes that passed the filter and
   * therefore fell through to a delegate postings lookup.
   */
  public static void incrementBloomFilterIdLookupProbed() {
    incrementBloomCounter(BLOOM_FILTER_ID_LOOKUP_PROBED, 1);
  }

  /**
   * Increments the counter tracking {@code _id} bloom-filter false positives: the filter admitted
   * the term but the delegate postings had no matching term.
   */
  public static void incrementBloomFilterIdLookupFalsePositive() {
    incrementBloomCounter(BLOOM_FILTER_ID_LOOKUP_FALSE_POSITIVE, 1);
  }

  /**
   * Increments the counter tracking per-field bloom filters dropped at segment-write time because
   * the FuzzySet was saturated -- too full to yield a useful false-positive rate (see {@code
   * BloomFilterFactory#isSaturated}). Such blooms are omitted from the segment's .blm file.
   * Incremented per write by the number dropped; with only the {@code _id} field bloom-enabled this
   * is effectively one per segment that dropped its bloom.
   *
   * @param amount the number of saturated blooms dropped in this write
   */
  public static void incrementBloomFilterSaturatedDropped(int amount) {
    incrementBloomCounter(BLOOM_FILTER_SATURATED_DROPPED, amount);
  }

  private static void incrementBloomCounter(String name, int amount) {
    Optional<MeterRegistry> maybeRegistry = meterRegistry;
    if (maybeRegistry.isEmpty()) {
      FLOGGER.atWarning().atMostEvery(1, TimeUnit.HOURS).log(
          "GlobalMetricFactory not initialized, cannot record bloom filter metric %s", name);
      return;
    }
    getOrRegisterCounter(maybeRegistry.get(), name).increment(amount);
  }

  /**
   * Eagerly registers all bloom filter counters (initialized to 0) so they are always present in
   * the scrape output. These metrics back dashboards and alerts, so they must exist even before the
   * first event -- otherwise a cluster that has not yet exercised the bloom read path would report
   * "no data" instead of 0.
   */
  private static void registerEagerCounters(MeterRegistry registry) {
    for (String name : EAGER_COUNTER_TO_DESCRIPTIONS.keySet()) {
      getOrRegisterCounter(registry, name);
    }
  }

  private static Counter getOrRegisterCounter(MeterRegistry registry, String name) {
    return Counter.builder(name)
        .description(EAGER_COUNTER_TO_DESCRIPTIONS.get(name))
        .register(registry);
  }

  @VisibleForTesting
  public static void resetForTest() {
    meterRegistry = Optional.empty();
  }

  @VisibleForTesting
  static Optional<MeterRegistry> getRegistryForTest() {
    return meterRegistry;
  }
}
