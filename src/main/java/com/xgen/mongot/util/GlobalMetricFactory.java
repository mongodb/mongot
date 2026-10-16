package com.xgen.mongot.util;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.flogger.FluentLogger;
import com.google.errorprone.annotations.CompileTimeConstant;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.FunctionCounter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
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
  static final String NATIVE_SCALAR_VECTOR_SCORING_ENGAGED = "native_scalar_vector_scoring_engaged";
  static final String NATIVE_SCALAR_VECTOR_SCORING_QUERY_ACTIVE =
      "native_scalar_vector_scoring_query_active";
  static final String NATIVE_BYTE_VECTOR_SCORING_ENGAGED = "native_byte_vector_scoring_engaged";
  static final String NATIVE_BYTE_VECTOR_SCORING_QUERY_ACTIVE =
      "native_byte_vector_scoring_query_active";

  private static final AtomicLong NATIVE_SCALAR_VECTOR_SCORING_QUERY_ACTIVE_VALUE =
      new AtomicLong();
  private static final AtomicLong NATIVE_BYTE_VECTOR_SCORING_QUERY_ACTIVE_VALUE = new AtomicLong();

  // The bloom filter counters are incremented once per _id lookup per segment on the indexing path
  // from multiple writer threads, so the hot path increments striped LongAdders directly and
  // initialize only binds them to the registry as FunctionCounters -- no registry lookup (builder
  // allocation plus a Meter.Id hash) per increment, and no single cache line CAS to contend on.
  private static final LongAdder BLOOM_FILTER_ID_LOOKUP_SKIPPED_VALUE = new LongAdder();
  private static final LongAdder BLOOM_FILTER_ID_LOOKUP_PROBED_VALUE = new LongAdder();
  private static final LongAdder BLOOM_FILTER_ID_LOOKUP_FALSE_POSITIVE_VALUE = new LongAdder();
  private static final LongAdder BLOOM_FILTER_SATURATED_DROPPED_VALUE = new LongAdder();

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
          NATIVE_SCALAR_VECTOR_SCORING_ENGAGED,
          "Counts native scalar-quantized scorer engagements -- once per native scorer/supplier"
              + " construction call, typically once per segment touched by a merge or a"
              + " $vectorSearch query, so a single query spanning N segments can move this by N;"
              + " not once per vector scored. A series that stays at 0 means native scoring never"
              + " actually engaged on this process even if the dynamic feature flag is on",
          NATIVE_BYTE_VECTOR_SCORING_ENGAGED,
          "Counts native int8 byte scorer engagements -- once per native scorer/supplier"
              + " construction call (mmap-backed or in-memory), typically once per segment touched"
              + " by a merge, an HNSW graph build, or a $vectorSearch query, so a single query"
              + " spanning N segments can move this by N; not once per vector scored. Stays at 0 if"
              + " the default-on NATIVE_BYTE_VECTOR_SCORING flag is disabled (or never engaged)");

  private GlobalMetricFactory() {}

  /** Initialize the global metric factory. Should be called once at startup. */
  public static synchronized void initialize(MeterRegistry registry) {
    if (meterRegistry.isPresent()) {
      LOGGER.debug("GlobalMetricFactory already initialized.");
      return;
    }
    meterRegistry = Optional.of(registry);
    registerEagerCounters(registry);
    registerBloomFilterCounters(registry);
    registerNativeScalarVectorScoringQueryActiveGauge(registry);
    registerNativeByteVectorScoringQueryActiveGauge(registry);
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
    BLOOM_FILTER_ID_LOOKUP_SKIPPED_VALUE.increment();
  }

  /**
   * Increments the counter tracking {@code _id} bloom-filter probes that passed the filter and
   * therefore fell through to a delegate postings lookup.
   */
  public static void incrementBloomFilterIdLookupProbed() {
    BLOOM_FILTER_ID_LOOKUP_PROBED_VALUE.increment();
  }

  /**
   * Increments the counter tracking {@code _id} bloom-filter false positives: the filter admitted
   * the term but the delegate postings had no matching term.
   */
  public static void incrementBloomFilterIdLookupFalsePositive() {
    BLOOM_FILTER_ID_LOOKUP_FALSE_POSITIVE_VALUE.increment();
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
    BLOOM_FILTER_SATURATED_DROPPED_VALUE.add(amount);
  }

  /**
   * Increments the counter tracking native scalar-quantized scorer engagements -- once per native
   * scorer/supplier construction call that takes the native SIMD path (typically once per segment
   * touched by a merge or a query, so a single {@code $vectorSearch} spanning N segments moves this
   * by N), not once per vector scored (as opposed to falling back to Lucene's Java scorer). Call
   * only from the branch that actually takes the native path -- a series that stays at 0 confirms
   * native scoring never engaged on this process, e.g. because a shadowed {@code KnnVectorsFormat}
   * lost SPI resolution to Lucene's stock class on read, even with the dynamic feature flag on.
   */
  public static void incrementNativeScalarVectorScoringEngaged() {
    Optional<MeterRegistry> maybeRegistry = meterRegistry;
    if (maybeRegistry.isEmpty()) {
      FLOGGER.atWarning().atMostEvery(1, TimeUnit.HOURS).log(
          "GlobalMetricFactory not initialized, cannot record native scalar vector scoring metric");
      return;
    }
    getOrRegisterCounter(maybeRegistry.get(), NATIVE_SCALAR_VECTOR_SCORING_ENGAGED).increment();
  }

  /**
   * Marks that this process has served at least one {@code $vectorSearch} query using the native
   * scalar-quantized scorer, flipping the {@code native_scalar_vector_scoring_query_active} gauge
   * to 1. Call only from the query path (e.g. {@code getRandomVectorScorer}), never from
   * merge/graph-build ({@code getRandomVectorScorerSupplier}) -- a process that only ever merges
   * natively, with no live query engaging native scoring, should not count toward "customers
   * impacted".
   *
   * <p>The gauge is sticky for the life of the process (never reset back to 0 when the flag is
   * later disabled) -- it answers "was this cluster ever positively impacted", not "is native
   * scoring active on this cluster right now". {@link #NATIVE_SCALAR_VECTOR_SCORING_ENGAGED} is not
   * a substitute for a windowed, query-only signal: it also increments from merge/graph-build
   * ({@code getRandomVectorScorerSupplier}), so a cluster that only ever merges natively -- with no
   * live {@code $vectorSearch} query -- would show up in {@code
   * increase(native_scalar_vector_scoring_engaged_total[...]) > 0}. There is currently no
   * query-only windowed metric; use this gauge for "ever impacted".
   */
  public static void markNativeScalarVectorScoringQueryActive() {
    // Plain read first: a failed compareAndSet still issues a locked CAS instruction and can bounce
    // the cache line on every query, so check-then-CAS to make every call after the first flip a
    // true no-write volatile read.
    if (NATIVE_SCALAR_VECTOR_SCORING_QUERY_ACTIVE_VALUE.get() == 0) {
      NATIVE_SCALAR_VECTOR_SCORING_QUERY_ACTIVE_VALUE.compareAndSet(0, 1);
    }
  }

  private static void registerNativeScalarVectorScoringQueryActiveGauge(MeterRegistry registry) {
    Gauge.builder(
            NATIVE_SCALAR_VECTOR_SCORING_QUERY_ACTIVE,
            NATIVE_SCALAR_VECTOR_SCORING_QUERY_ACTIVE_VALUE,
            AtomicLong::get)
        .description(
            "1 once this process has served a $vectorSearch query using the native"
                + " scalar-quantized scorer at least once, 0 otherwise. Sticky for the life of the"
                + " process. Since multiple mongot processes can back one cluster, first collapse"
                + " with max by (mmsLabel_clusterId) (or org/group) to get one 0/1 status per"
                + " cluster -- count by (mmsLabel_clusterId) alone overcounts clusters with more"
                + " than one process reporting 1 -- then count/sum that result across clusters to"
                + " get the number of clusters ever positively impacted by native query scoring.")
        .register(registry);
  }

  /**
   * Increments the counter tracking native int8 byte scorer engagements -- once per native
   * scorer/supplier construction call, not once per vector scored. Call only from the branch that
   * actually takes the native path: a series that stays at 0 is how one confirms that disabling the
   * default-on {@code NATIVE_BYTE_VECTOR_SCORING} kill switch actually took effect.
   */
  public static void incrementNativeByteVectorScoringEngaged() {
    Optional<MeterRegistry> maybeRegistry = meterRegistry;
    if (maybeRegistry.isEmpty()) {
      FLOGGER.atWarning().atMostEvery(1, TimeUnit.HOURS).log(
          "GlobalMetricFactory not initialized, cannot record native byte vector scoring metric");
      return;
    }
    getOrRegisterCounter(maybeRegistry.get(), NATIVE_BYTE_VECTOR_SCORING_ENGAGED).increment();
  }

  /**
   * Marks that this process has served at least one {@code $vectorSearch} query using the native
   * int8 byte scorer, flipping the {@code native_byte_vector_scoring_query_active} gauge to 1.
   * Query path only, never merge/graph-build: merge-only engagement should not count toward
   * "customers impacted". Like {@link #markNativeScalarVectorScoringQueryActive()}, the gauge is
   * sticky for the life of the process.
   */
  public static void markNativeByteVectorScoringQueryActive() {
    // Plain read first: a failed compareAndSet still issues a locked CAS instruction and can bounce
    // the cache line on every query, so check-then-CAS to make every call after the first flip a
    // true no-write volatile read.
    if (NATIVE_BYTE_VECTOR_SCORING_QUERY_ACTIVE_VALUE.get() == 0) {
      NATIVE_BYTE_VECTOR_SCORING_QUERY_ACTIVE_VALUE.compareAndSet(0, 1);
    }
  }

  private static void registerNativeByteVectorScoringQueryActiveGauge(MeterRegistry registry) {
    Gauge.builder(
            NATIVE_BYTE_VECTOR_SCORING_QUERY_ACTIVE,
            NATIVE_BYTE_VECTOR_SCORING_QUERY_ACTIVE_VALUE,
            AtomicLong::get)
        .description(
            "1 once this process has served a $vectorSearch query using the native int8"
                + " byte scorer, 0 otherwise. Sticky for the life of the process. See"
                + " native_scalar_vector_scoring_query_active for aggregation guidance.")
        .register(registry);
  }

  /**
   * Binds the bloom filter counters to the registry as FunctionCounters (initialized to 0) so they
   * are always present in the scrape output. These metrics back dashboards and alerts, so they must
   * exist even before the first event -- otherwise a cluster that has not yet exercised the bloom
   * read path would report "no data" instead of 0.
   */
  private static void registerBloomFilterCounters(MeterRegistry registry) {
    FunctionCounter.builder(
            BLOOM_FILTER_ID_LOOKUP_SKIPPED,
            BLOOM_FILTER_ID_LOOKUP_SKIPPED_VALUE,
            LongAdder::doubleValue)
        .description(
            "Counts _id lookups skipped because the bloom filter reported the term as absent")
        .register(registry);
    FunctionCounter.builder(
            BLOOM_FILTER_ID_LOOKUP_PROBED,
            BLOOM_FILTER_ID_LOOKUP_PROBED_VALUE,
            LongAdder::doubleValue)
        .description(
            "Counts _id lookups the bloom filter admitted, requiring a delegate postings lookup")
        .register(registry);
    FunctionCounter.builder(
            BLOOM_FILTER_ID_LOOKUP_FALSE_POSITIVE,
            BLOOM_FILTER_ID_LOOKUP_FALSE_POSITIVE_VALUE,
            LongAdder::doubleValue)
        .description(
            "Counts _id bloom filter false positives (admitted but no matching delegate term)")
        .register(registry);
    FunctionCounter.builder(
            BLOOM_FILTER_SATURATED_DROPPED,
            BLOOM_FILTER_SATURATED_DROPPED_VALUE,
            LongAdder::doubleValue)
        .description(
            "Counts per-field bloom filters dropped at write time because the FuzzySet saturated")
        .register(registry);
  }

  /**
   * Eagerly registers the native scoring engaged counters (initialized to 0) so they are always
   * present in the scrape output. These metrics back dashboards and alerts, so they must exist even
   * before the first event -- otherwise a cluster that has not yet exercised the path would report
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
    BLOOM_FILTER_ID_LOOKUP_SKIPPED_VALUE.reset();
    BLOOM_FILTER_ID_LOOKUP_PROBED_VALUE.reset();
    BLOOM_FILTER_ID_LOOKUP_FALSE_POSITIVE_VALUE.reset();
    BLOOM_FILTER_SATURATED_DROPPED_VALUE.reset();
    NATIVE_SCALAR_VECTOR_SCORING_QUERY_ACTIVE_VALUE.set(0);
    NATIVE_BYTE_VECTOR_SCORING_QUERY_ACTIVE_VALUE.set(0);
  }

  @VisibleForTesting
  static Optional<MeterRegistry> getRegistryForTest() {
    return meterRegistry;
  }
}
