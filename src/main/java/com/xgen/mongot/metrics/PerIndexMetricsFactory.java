package com.xgen.mongot.metrics;

import static com.xgen.mongot.util.Check.checkArg;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.CaseFormat;
import com.xgen.mongot.index.version.GenerationId;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.Enums;
import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.LongTaskTimer;
import io.micrometer.core.instrument.Measurement;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.distribution.HistogramSnapshot;
import io.micrometer.core.instrument.search.RequiredSearch;
import java.util.Collection;
import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.ToDoubleFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A MetricsFactory that may maintain metrics per-index and per-process. For per-process exported
 * meters the corresponding per-index meter is created on a different registry so that one may be
 * prometheus exported while the other is not.
 *
 * <p>This class is required to be thread-safe.
 */
public class PerIndexMetricsFactory {

  public static double[] defaultSummaryPercentiles = {0.5, 0.75, 0.9, 0.99};

  private final MeterAndFtdcRegistry meterAndFtdcRegistry;
  private final MetricsFactory perIndexAllFactory;
  private final MetricsFactory perIndexFtdcFactory;
  private final MetricsFactory perProcessFactory;

  public PerIndexMetricsFactory(
      String namespace, MeterAndFtdcRegistry meterAndFtdcRegistry, GenerationId generationId) {
    this(
        namespace,
        meterAndFtdcRegistry,
        generationId.uniqueString(),
        generationId.indexId.toHexString());
  }

  @VisibleForTesting
  public PerIndexMetricsFactory(
      String namespace,
      MeterAndFtdcRegistry meterAndFtdcRegistry,
      String indexGenerationId,
      String indexId) {
    this(
        meterAndFtdcRegistry,
        new MetricsFactory(
            namespace,
            meterAndFtdcRegistry.meterRegistry(),
            Tags.of("generationId logString", indexGenerationId).and("indexId logString", indexId)),
        new MetricsFactory(
            namespace,
            meterAndFtdcRegistry.ftdcRegistry(),
            Tags.of("generationId logString", indexGenerationId).and("indexId logString", indexId)),
        new MetricsFactory(namespace, meterAndFtdcRegistry.meterRegistry()));
  }

  private PerIndexMetricsFactory(
      MeterAndFtdcRegistry meterAndFtdcRegistry,
      MetricsFactory perIndexAllFactory,
      MetricsFactory perIndexFtdcFactory,
      MetricsFactory perProcessFactory) {
    this.meterAndFtdcRegistry = meterAndFtdcRegistry;
    this.perIndexAllFactory = perIndexAllFactory;
    this.perIndexFtdcFactory = perIndexFtdcFactory;
    this.perProcessFactory = perProcessFactory;
  }

  private static class PerProcessAndIndexCounter implements Counter {
    private final Counter perIndex;
    private final Counter perProcess;

    PerProcessAndIndexCounter(Counter perIndex, Counter perProcess) {
      this.perIndex = perIndex;
      this.perProcess = perProcess;
    }

    @Override
    public Id getId() {
      return this.perIndex.getId();
    }

    @Override
    public void increment(double v) {
      this.perIndex.increment(v);
      this.perProcess.increment(v);
    }

    @Override
    public double count() {
      return this.perIndex.count();
    }
  }

  private static class PerProcessAndIndexDistributionSummary implements DistributionSummary {
    private final DistributionSummary perIndex;
    private final DistributionSummary perProcess;

    PerProcessAndIndexDistributionSummary(
        DistributionSummary perIndex, DistributionSummary perProcess) {
      this.perIndex = perIndex;
      this.perProcess = perProcess;
    }

    @Override
    public Id getId() {
      return this.perIndex.getId();
    }

    @Override
    public void record(double v) {
      this.perIndex.record(v);
      this.perProcess.record(v);
    }

    @Override
    public long count() {
      return this.perIndex.count();
    }

    @Override
    public double max() {
      return this.perIndex.max();
    }

    @Override
    public double mean() {
      return this.perIndex.mean();
    }

    @Override
    public Iterable<Measurement> measure() {
      return this.perIndex.measure();
    }

    @Override
    public double totalAmount() {
      return this.perIndex.totalAmount();
    }

    @Override
    public HistogramSnapshot takeSnapshot() {
      return this.perIndex.takeSnapshot();
    }
  }

  private static class PerProcessAndIndexTimer implements Timer {
    private final Clock clock;
    private final Timer perIndex;
    private final Timer perProcess;

    PerProcessAndIndexTimer(Clock clock, Timer perIndex, Timer perProcess) {
      this.clock = clock;
      this.perIndex = perIndex;
      this.perProcess = perProcess;
    }

    @Override
    public Id getId() {
      return this.perIndex.getId();
    }

    @Override
    public void record(long amount, TimeUnit unit) {
      this.perIndex.record(amount, unit);
      this.perProcess.record(amount, unit);
    }

    @Override
    public <T> T record(Supplier<T> f) {
      Sample sample = Timer.start(this.clock);
      try {
        return f.get();
      } finally {
        sample.stop(this);
      }
    }

    @Override
    public void record(Runnable f) {
      Sample sample = Timer.start(this.clock);
      try {
        f.run();
      } finally {
        sample.stop(this);
      }
    }

    @Override
    public <T> T recordCallable(Callable<T> f) throws Exception {
      Sample sample = Timer.start(this.clock);
      try {
        return f.call();
      } finally {
        sample.stop(this);
      }
    }

    @Override
    public long count() {
      return this.perIndex.count();
    }

    @Override
    public double totalTime(TimeUnit unit) {
      return this.perIndex.totalTime(unit);
    }

    @Override
    public double max(TimeUnit unit) {
      return this.perIndex.max(unit);
    }

    @Override
    public TimeUnit baseTimeUnit() {
      return this.perIndex.baseTimeUnit();
    }

    @Override
    public HistogramSnapshot takeSnapshot() {
      return this.perIndex.takeSnapshot();
    }
  }

  /**
   * Export a counter with name. If this MetricsFactory is configured to produce a timeseries per
   * index this counter will be back by two underlying counters:
   *
   * <ol>
   *   <li>a counter without the index tag which will be exported to all outputs.
   *   <li>a counter with the index tag that will be omitted from prometheus exports.
   * </ol>
   *
   * <p>Prometheus exports are limited in an attempt to control the number of timeseries we export
   * as this affects prometheus scalability. If it is important to see this counter per-index on a
   * prometheus console, use perIndexCounter instead.
   *
   * @param name the name of the metric
   * @return a {@link Counter} that can be incremented.
   */
  public Counter counter(String name) {
    return counter(name, Tags.empty());
  }

  /**
   * Export a counter with name. If this MetricsFactory is configured to produce a timeseries per
   * index this counter will be back by two underlying counters:
   *
   * <ol>
   *   <li>a counter without the index tag which will be exported to all outputs.
   *   <li>a counter with the index tag that will be omitted from prometheus exports.
   * </ol>
   *
   * <p>Prometheus exports are limited in an attempt to control the number of timeseries we export
   * as this affects prometheus scalability. If it is important to see this counter per-index on a
   * prometheus console, use perIndexCounter instead.
   *
   * @param name the name of the metric
   * @param meterTags additional tags to add to the exported counters.
   * @return a {@link Counter} that can be incremented.
   */
  public Counter counter(String name, Tags meterTags) {
    return new PerProcessAndIndexCounter(
        this.perIndexFtdcFactory.counter(name, meterTags),
        this.perProcessFactory.counter(name, meterTags));
  }

  /**
   * Export a counter with name that exports a timeseries specifically for each index. NB: this will
   * cause a very large fanout. Before using this method consider if you need to see this value in a
   * live prometheus console or if it is OK to examine the values in FTDC.
   *
   * @param name the name of the metric to export.
   * @return a {@link Counter} that can be incremented.
   */
  public Counter perIndexCounter(String name) {
    return perIndexCounter(name, Tags.empty());
  }

  /**
   * Export a counter with name that exports a timeseries specifically for each index. NB: this will
   * cause a very large fanout. Before using this method consider if you need to see this value in a
   * live prometheus console or if it is OK to examine the values in FTDC.
   *
   * @param name the name of the metric to export.
   * @param meterTags additional tags to add to the exported counter.
   * @return a {@link Counter} that can be incremented.
   */
  public Counter perIndexCounter(String name, Tags meterTags) {
    return this.perIndexAllFactory.counter(name, meterTags);
  }

  public AtomicLong perIndexNumGauge(String name) {
    return perIndexNumGauge(name, Tags.empty());
  }

  public AtomicLong perIndexNumGauge(String name, Tags meterTags) {
    return perIndexObjectValueGauge(name, new AtomicLong(), Number::doubleValue, meterTags);
  }

  public <T> T perIndexObjectValueGauge(String name, T stateObject, ToDoubleFunction<T> valueFunc) {
    return perIndexObjectValueGauge(name, stateObject, valueFunc, Tags.empty());
  }

  public <T> T perIndexObjectValueGauge(
      String name, T stateObject, ToDoubleFunction<T> valueFunc, Tags meterTags) {
    return this.perIndexAllFactory.objectValueGauge(name, stateObject, valueFunc, meterTags);
  }

  public <T extends Collection<?>> T perIndexCollectionSizeGauge(String name, T collection) {
    return perIndexCollectionSizeGauge(name, collection, Tags.empty());
  }

  public <T extends Collection<?>> T perIndexCollectionSizeGauge(
      String name, T collection, Tags meterTags) {
    return perIndexObjectValueGauge(name, collection, Collection::size, meterTags);
  }

  /**
   * Export distribution summary under name: counter, max, sum, and percentiles 50, 75, 90, and 99.
   *
   * <p>NB: to limit prometheus metric growth this method will record both per-index and per-process
   * distribution summaries, but only the latter will be exported.
   *
   * @param name name of the variable to export
   * @return a {@link DistributionSummary} that can be used to record samples.
   */
  public DistributionSummary summary(String name) {
    return summary(name, defaultSummaryPercentiles);
  }

  /**
   * Export distribution summary under name: counter, max, sum, and the specified percentiles
   *
   * <p>NB: to limit prometheus metric growth this method will record both per-index and per-process
   * distribution summaries, but only the latter will be exported.
   *
   * @param name name of the variable to export
   * @param percentiles a list of percentile values to export.
   * @return a {@link DistributionSummary} that can be used to record samples.
   */
  public DistributionSummary summary(String name, double... percentiles) {
    return summary(name, Tags.empty(), percentiles);
  }

  /**
   * Export distribution summary under name: counter, max, sum, and the specified percentiles
   *
   * <p>NB: to limit prometheus metric growth this method will record both per-index and per-process
   * distribution summaries, but only the latter will be exported.
   *
   * @param name name of the variable to export
   * @param meterTags additional tags to add to the exported histogram variables.
   * @param percentiles a list of percentile values to export.
   * @return a {@link DistributionSummary} that can be used to record samples.
   */
  public DistributionSummary summary(String name, Tags meterTags, double... percentiles) {
    return new PerProcessAndIndexDistributionSummary(
        this.perIndexFtdcFactory.summary(name, meterTags, percentiles),
        this.perProcessFactory.summary(name, meterTags, percentiles));
  }

  /**
   * Export distribution summary with histogram buckets defined by {@code buckets}.
   *
   * <p>The exported summary supports PromQL queries such as {@code histogram_quantile()} to compute
   * accurate percentiles aggregated across all instances.
   *
   * <p>NB: to limit prometheus metric growth this method will record both per-index and per-process
   * distribution summaries, but only the latter will be exported.
   *
   * @param name name of the variable to export
   * @param buckets upper-bound cutoffs for histogram buckets (strictly increasing, non-empty).
   * @return a {@link DistributionSummary} that can be used to record samples.
   * @throws IllegalArgumentException if {@code buckets} is empty.
   */
  public DistributionSummary histogram(String name, double... buckets) {
    return histogram(name, Tags.empty(), buckets);
  }

  /**
   * Export distribution summary with histogram buckets defined by {@code buckets}.
   *
   * <p>The exported summary supports PromQL queries such as {@code histogram_quantile()} to compute
   * accurate percentiles aggregated across all instances.
   *
   * <p>NB: to limit prometheus metric growth this method will record both per-index and per-process
   * distribution summaries, but only the latter will be exported.
   *
   * @param name name of the variable to export
   * @param meterTags additional tags to add to the exported histogram variables.
   * @param buckets upper-bound cutoffs for histogram buckets (strictly increasing, non-empty).
   * @return a {@link DistributionSummary} that can be used to record samples.
   * @throws IllegalArgumentException if {@code buckets} is empty.
   */
  public DistributionSummary histogram(String name, Tags meterTags, double... buckets) {
    checkArg(buckets.length > 0, "histogram requires at least one explicit bucket boundary");
    return new PerProcessAndIndexDistributionSummary(
        this.perIndexFtdcFactory.histogram(name, meterTags, buckets),
        this.perProcessFactory.histogram(name, meterTags, buckets));
  }

  /**
   * Used to track the duration of short-running tasks. "Short-running" is subjective but the
   * assumption is that it should be under a minute.
   */
  public Timer perIndexTimer(String name) {
    return perIndexTimer(name, Tags.empty());
  }

  /**
   * Used to track the duration of short-running tasks. "Short-running" is subjective but the
   * assumption is that it should be under a minute.
   */
  public Timer perIndexTimer(String name, Tags meterTags) {
    return this.perIndexAllFactory.timer(
        name,
        meterTags.and(
            Tags.of(
                "timeUnit", Enums.convertNameTo(CaseFormat.LOWER_CAMEL, TimeUnit.MILLISECONDS))),
        0.5,
        0.75,
        0.9,
        0.99);
  }

  /**
   * Used to track the duration of short-running tasks. "Short-running" is subjective but the
   * assumption is that it should be under a minute.
   */
  public Timer perIndexTimer(String name, Tags meterTags, double... percentiles) {
    return this.perIndexAllFactory.timer(name, meterTags, percentiles);
  }

  /**
   * Used to track the duration of short-running tasks. "Short-running" is subjective but the
   * assumption is that it should be under a minute.
   *
   * <p>NB: to limit prometheus metric growth this method will record both per-index and per-process
   * distribution summaries, but only the latter will be exported.
   */
  public Timer timer(String name) {
    return timer(
        name,
        Tags.of("timeUnit", Enums.convertNameTo(CaseFormat.LOWER_CAMEL, TimeUnit.MILLISECONDS)),
        0.5,
        0.75,
        0.9,
        0.99);
  }

  /**
   * Used to track the duration of short-running tasks. "Short-running" is subjective but the
   * assumption is that it should be under a minute.
   *
   * <p>NB: to limit prometheus metric growth this method will record both per-index and per-process
   * distribution summaries, but only the latter will be exported.
   */
  public Timer timer(String name, Tags meterTags, double... percentiles) {
    return new PerProcessAndIndexTimer(
        this.meterAndFtdcRegistry.meterRegistry().config().clock(),
        this.perIndexFtdcFactory.timer(name, meterTags, percentiles),
        this.perProcessFactory.timer(name, meterTags, percentiles));
  }

  /** Used to track the duration of long-running tasks (over a minute). */
  public LongTaskTimer perIndexLongTaskTimer(String name, double... percentiles) {
    return perIndexLongTaskTimer(name, Tags.empty(), percentiles);
  }

  public LongTaskTimer perIndexLongTaskTimer(String name, Tags meterTags, double... percentiles) {
    return this.perIndexAllFactory.longTaskTimer(name, meterTags, percentiles);
  }

  public PerIndexMetricsFactory childMetricsFactory(String childNamespace) {
    return new PerIndexMetricsFactory(
        this.meterAndFtdcRegistry,
        this.perIndexAllFactory.childMetricsFactory(childNamespace),
        this.perIndexFtdcFactory.childMetricsFactory(childNamespace),
        this.perProcessFactory.childMetricsFactory(childNamespace));
  }

  /** Closes all meters created by this MetricsFactory. */
  public void close() {
    this.perIndexAllFactory.close();
    this.perIndexFtdcFactory.close();
    // NB: do not close the per-process factory; these may be referenced by other factories.
  }

  @VisibleForTesting
  public RequiredSearch get(String name) {
    return this.get(name, Tags.empty());
  }

  @VisibleForTesting
  public RequiredSearch get(String name, Tags meterTags) {
    return this.meterAndFtdcRegistry
        .meterRegistry()
        .get(this.perProcessFactory.metricName(name))
        .tags(meterTags);
  }

  String metricName(String name) {
    // NB: Names are identical across all underlying factories.
    return this.perProcessFactory.metricName(name);
  }

  /**
   * Maps Enum constant to micrometer counter.
   *
   * <p>NB: this will cause a counter to be exported to prometheus per (index,enum,tags) tuple,
   * which may be very expensive. Consider using getEnumCounterMap() instead and obtaining per index
   * data from FTDC instead.
   */
  public <T extends Enum<T>> Map<T, Counter> getPerIndexEnumCounterMap(
      Class<T> enumClass, Tags meterTags) {
    checkArg(enumClass.isEnum(), "provided class is not Enum");
    return Stream.of(enumClass.getEnumConstants())
        .collect(
            Collectors.toMap(
                type -> type,
                type ->
                    perIndexCounter(Enums.convertNameTo(CaseFormat.LOWER_CAMEL, type), meterTags),
                (oldCounter, newCounter) ->
                    Check.unreachable("Duplicate enum key should not occur"),
                () -> new EnumMap<>(enumClass)));
  }

  /**
   * Maps Enum constant to micrometer counter.
   *
   * <p>NB: to limit prometheus metric growth this method will record both per-index and per-process
   * distribution summaries, but only the latter will be exported.
   */
  public <T extends Enum<T>> Map<T, Counter> getEnumCounterMap(Class<T> enumClass) {
    return getEnumCounterMap(enumClass, this::counter);
  }

  public <T extends Enum<T>> Map<T, Counter> getEnumCounterMap(
      Class<T> enumClass, Function<String, Counter> nameToCounter) {
    checkArg(enumClass.isEnum(), "provided class is not Enum");
    return Stream.of(enumClass.getEnumConstants())
        .collect(
            Collectors.toMap(
                type -> type,
                type -> nameToCounter.apply(Enums.convertNameTo(CaseFormat.LOWER_CAMEL, type)),
                (oldCounter, newCounter) ->
                    Check.unreachable("Duplicate enum key should not occur"),
                () -> new EnumMap<>(enumClass)));
  }
}
