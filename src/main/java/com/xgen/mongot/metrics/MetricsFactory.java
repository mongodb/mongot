package com.xgen.mongot.metrics;

import static com.google.common.collect.Maps.toImmutableEnumMap;
import static com.xgen.mongot.util.Check.checkArg;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.CaseFormat;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.Enums;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.LongTaskTimer;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.TimeGauge;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import io.micrometer.core.instrument.distribution.HistogramSnapshot;
import io.micrometer.core.instrument.noop.NoopCounter;
import io.micrometer.core.instrument.noop.NoopTimer;
import io.micrometer.core.instrument.search.RequiredSearch;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import java.util.function.ToDoubleFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Wraps the Meter instantiation APIs of the backing MeterRegistry by prepending the Meter names
 * with a namespace and applying optional class level tags when creating Meters.
 *
 * <p>This class is required to be thread-safe.
 */
public class MetricsFactory {

  private static final double[] DEFAULT_TIMER_PERCENTILES = {0.5, 0.75, 0.9, 0.99};
  private static final Logger LOG = LoggerFactory.getLogger(MetricsFactory.class);

  private static final String TIME_UNIT_TAG_KEY = "timeUnit";

  /**
   * An exhaustive mapping from all TimeUnit values to an immutable tag with the key {@link
   * #TIME_UNIT_TAG_KEY}. This cache prevents the need to allocate new strings for each metric.
   */
  private static final ImmutableMap<TimeUnit, Tags> TIME_UNIT_TO_TAG =
      Arrays.stream(TimeUnit.values())
          .collect(
              toImmutableEnumMap(
                  unit -> unit,
                  unit ->
                      Tags.of(
                          TIME_UNIT_TAG_KEY, Enums.convertNameTo(CaseFormat.LOWER_CAMEL, unit))));

  /**
   * A {@link Counter} that is never registered with a MeterRegistry and never exported. Use in
   * place of an optional or null counter.
   */
  public static final Counter NOOP_COUNTER =
      new NoopCounter(new Meter.Id("noop_counter", Tags.empty(), null, null, Meter.Type.COUNTER));

  /**
   * A {@link Timer} that is never registered with a MeterRegistry and never exported. Use in place
   * of an optional or null timer.
   */
  public static final Timer NOOP_TIMER =
      new NoopTimer(new Meter.Id("noop_timer", Tags.empty(), null, null, Meter.Type.TIMER));

  private final String namespace;
  private final MeterRegistry registry;
  private final Tags factoryTags;
  private final Set<Meter> registeredMeters;

  // When true, close() removes a meter from the registry only if the registry still holds the exact
  // meter instance this factory registered. This guards against tearing down a meter that a sibling
  // factory re-registered under the same Meter.Id. Auto-embedding matview generations share a
  // Meter.Id across attempts (MaterializedViewGenerationId.uniqueString() omits the attempt
  // number), so a dropped generation's close() would otherwise remove the live successor's
  // leaderStatus gauge. Opt-in so behavior is unchanged for every non-matview factory.
  private final boolean identitySafeClose;

  // Registry that counter and timer values are read from; see readFrom(Counter).
  private final MeterRegistry readRegistry;

  public MetricsFactory(String namespace, MeterRegistry registry) {
    this(namespace, registry, Tags.empty());
  }

  public MetricsFactory(String namespace, MeterRegistry registry, Tag... factoryTags) {
    this(namespace, registry, Tags.of(factoryTags));
  }

  public MetricsFactory(String namespace, MeterRegistry registry, Tags factoryTags) {
    this(namespace, registry, factoryTags, false);
  }

  public MetricsFactory(
      String namespace, MeterRegistry registry, Tags factoryTags, boolean identitySafeClose) {
    this(namespace, registry, factoryTags, identitySafeClose, registry);
  }

  /**
   * Creates a factory whose counters and timers record to {@code registry} but read their values
   * from {@code readRegistry} when it is a child of {@code registry}. Reading a composite meter
   * returns an arbitrary child, which is a no-op for any child whose meter filter denies the meter.
   */
  public MetricsFactory(
      String namespace,
      MeterRegistry registry,
      Tags factoryTags,
      boolean identitySafeClose,
      MeterRegistry readRegistry) {
    this.namespace = namespace;
    this.registry = registry;
    this.factoryTags = factoryTags;
    this.registeredMeters = ConcurrentHashMap.newKeySet();
    this.identitySafeClose = identitySafeClose;
    this.readRegistry = readRegistry;
  }

  /**
   * Returns Tags containing a single {@link Tag} to specify the units of a {@link Timer}.
   *
   * <p>Timers must have a tag that specifies their unit, or one will be added by default. As best
   * practice, all new Timers should use {@link TimeUnit#SECONDS}.
   */
  public static Tags getTimeUnitTag(TimeUnit unit) {
    return TIME_UNIT_TO_TAG.get(unit);
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
    var counter = this.registry.counter(metricName(name), this.factoryTags.and(meterTags));
    this.registeredMeters.add(counter);
    return readFrom(counter);
  }

  /**
   * Exports a gauge with the provided name and no tags. We internally create an AtomicLong, which
   * is tracked by the gauge and returned.
   *
   * <p>Note: creating a new gauge with the same name and tags will remove the previous gauge,
   * potentially leaving an "orphaned" state object that does not update any gauges.
   */
  public AtomicLong numGauge(String name) {
    return numGauge(name, Tags.empty());
  }

  /**
   * Exports a gauge with the provided name and tags. We internally create an AtomicLong, which is
   * tracked by the gauge and returned.
   *
   * <p>Note: creating a new gauge with the same name and tags will remove the previous gauge,
   * potentially leaving an "orphaned" state object that does not update any gauges.
   */
  public AtomicLong numGauge(String name, Tags meterTags) {
    return objectValueGauge(name, new AtomicLong(), Number::doubleValue, meterTags);
  }

  /**
   * Exports a gauge with the provided name and no tags, with the value of valueFunc(stateObject).
   *
   * <p>Note: creating a new gauge with the same name and tags will remove the previous gauge,
   * potentially leaving an "orphaned" state object that does not update any gauges.
   */
  public <T> T objectValueGauge(String name, T stateObject, ToDoubleFunction<T> valueFunc) {
    return objectValueGauge(name, stateObject, valueFunc, Tags.empty());
  }

  /**
   * Exports a gauge with the provided name and tags, with the value of valueFunc(stateObject).
   *
   * <p>Note: creating a new gauge with the same name and tags will remove the previous gauge,
   * potentially leaving an "orphaned" state object that does not update any gauges.
   */
  public synchronized <T> T objectValueGauge(
      String name, T stateObject, ToDoubleFunction<T> valueFunc, Tags meterTags) {
    var fullName = metricName(name);
    var fullTags = this.factoryTags.and(meterTags);

    // We remove the old gauge first, as we prefer to have the more recently created gauge work over
    // the older one. We must assume that we will not create an identical gauge while the old one is
    // still in use
    Meter.Id gaugeId = new Meter.Id(fullName, fullTags, null, null, Meter.Type.GAUGE);
    Optional<Meter> previousGauge = Optional.ofNullable(this.registry.remove(gaugeId));
    previousGauge.ifPresent(this.registeredMeters::remove);

    Gauge objectValueGauge =
        Gauge.builder(fullName, stateObject, valueFunc)
            // maintain a strong reference so that the stateObject does not get garbage collected
            .strongReference(true)
            .tags(fullTags)
            .register(this.registry);
    this.registeredMeters.add(objectValueGauge);
    if (previousGauge.isPresent() && fullName.endsWith(".leaderStatus")) {
      LOG.atInfo()
          .addKeyValue("name", fullName)
          .addKeyValue("tags", fullTags)
          .addKeyValue("newStateObjectIdentity", System.identityHashCode(stateObject))
          .log("Re-registered gauge with same id");
    }
    return stateObject;
  }

  /**
   * Re-registers a gauge for {@code stateObject} under {@code name} + {@code meterTags} only if no
   * gauge is currently registered for that id. Unlike {@link #objectValueGauge}, this never
   * removes-then-re-adds an already-present gauge, so it is safe to call on a hot path (e.g. a
   * heartbeat) to self-heal a gauge that was evicted from the registry — for instance one torn down
   * by a sibling generation sharing the same Meter.Id.
   *
   * @return true if a re-registration occurred, false if the gauge was already present.
   */
  public synchronized <T> boolean ensureObjectValueGaugeRegistered(
      String name, T stateObject, ToDoubleFunction<T> valueFunc, Tags meterTags) {
    var fullName = metricName(name);
    var fullTags = this.factoryTags.and(meterTags);
    if (this.registry.find(fullName).tags(fullTags).gauge() != null) {
      return false;
    }
    objectValueGauge(name, stateObject, valueFunc, meterTags);
    return true;
  }

  /**
   * Exports a time gauge with the provided name and no tags, with the value of valueFunc
   * (stateObject).
   *
   * <p>Note: creating a new gauge with the same name and tags will remove the previous gauge,
   * potentially leaving an "orphaned" state object that does not update any gauges.
   */
  public synchronized <T> T timeGauge(String name, T stateObject, ToDoubleFunction<T> valueFunc) {
    return timeGauge(name, stateObject, valueFunc, Tags.empty());
  }

  /**
   * Exports a time gauge with the provided name and tags, with the value of valueFunc(stateObject).
   *
   * <p>Note: creating a new gauge with the same name and tags will remove the previous gauge,
   * potentially leaving an "orphaned" state object that does not update any gauges.
   */
  public synchronized <T> T timeGauge(
      String name, T stateObject, ToDoubleFunction<T> valueFunc, Tags meterTags) {
    var fullName = metricName(name);
    var fullTags = this.factoryTags.and(meterTags);

    // We remove the old gauge first, as we prefer to have the more recently created gauge work over
    // the older one. We must assume that we will not create an identical gauge while the old one is
    // still in use
    Meter.Id timeGaugeId = new Meter.Id(fullName, fullTags, null, null, Meter.Type.GAUGE);
    Optional<Meter> previousGauge = Optional.ofNullable(this.registry.remove(timeGaugeId));
    previousGauge.ifPresent(this.registeredMeters::remove);

    TimeGauge timeGauge =
        TimeGauge.builder(fullName, stateObject, TimeUnit.MILLISECONDS, valueFunc)
            // maintain a strong reference so that the stateObject does not get garbage collected
            .strongReference(true)
            .tags(fullTags)
            .register(this.registry);
    this.registeredMeters.add(timeGauge);
    return stateObject;
  }

  /**
   * Exports a gauge with the provided name and no tags, with the value of the collection's size.
   *
   * <p>Note: creating a new gauge with the same name and tags will remove the previous gauge,
   * potentially leaving an "orphaned" state object that does not update any gauges.
   */
  public <T extends Collection<?>> T collectionSizeGauge(String name, T collection) {
    return collectionSizeGauge(name, collection, Tags.empty());
  }

  /**
   * Exports a gauge with the provided name and tags, with the value of the collection's size.
   *
   * <p>Note: creating a new gauge with the same name and tags will remove the previous gauge,
   * potentially leaving an "orphaned" state object that does not update any gauges.
   */
  public <T extends Collection<?>> T collectionSizeGauge(
      String name, T collection, Tags meterTags) {
    return objectValueGauge(name, collection, Collection::size, meterTags);
  }

  /**
   * Export distribution summary under name: counter, max, sum, and percentiles 50, 75, 90, and 99.
   *
   * <p>These percentiles computed cannot be aggregated to produce fleet-wide percentiles in
   * Prometheus. If you need fleet-wide aggregation of percentiles across all instances, use {@link
   * #histogram(String, double...)} with explicit buckets instead.
   *
   * @param name name of the variable to export
   * @return a {@link DistributionSummary} that can be used to record samples.
   */
  public DistributionSummary summary(String name) {
    return summary(name, 0.5, 0.75, 0.9, 0.99);
  }

  /**
   * Export distribution summary under name: counter, max, sum, and the specified percentiles
   *
   * <p>These percentiles computed cannot be aggregated to produce fleet-wide percentiles in
   * Prometheus. If you need fleet-wide aggregation of percentiles across all instances, use {@link
   * #histogram(String, double...)} with explicit buckets instead.
   *
   * @param name name of the variable to export
   * @param percentiles a list of percentile values to export.
   * @return a {@link DistributionSummary} that can be used to record samples.
   */
  public DistributionSummary summary(String name, double... percentiles) {
    return summary(name, Tags.empty(), percentiles);
  }

  /**
   * Export distribution summary under name: counter, max, sum, and the specified percentiles.
   *
   * <p>These percentiles computed cannot be aggregated to produce fleet-wide percentiles in
   * Prometheus. If you need fleet-wide aggregation of percentiles across all instances, use {@link
   * #histogram(String, double...)} with explicit buckets instead.
   *
   * @param name name of the variable to export
   * @param meterTags additional tags to add to the exported histogram variables.
   * @param percentiles a list of percentile values to export.
   * @return a {@link DistributionSummary} that can be used to record samples.
   */
  public DistributionSummary summary(String name, Tags meterTags, double... percentiles) {
    var summary =
        DistributionSummary.builder(metricName(name))
            .tags(this.factoryTags.and(meterTags))
            .publishPercentiles(percentiles)
            .register(this.registry);
    this.registeredMeters.add(summary);
    return summary;
  }

  /**
   * Export distribution summary with histogram buckets defined by {@code buckets}.
   *
   * <p>The exported summary supports PromQL queries such as {@code histogram_quantile()} to compute
   * accurate percentiles aggregated across all instances.
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
   * @param name name of the variable to export
   * @param meterTags additional tags to add to the exported histogram variables.
   * @param buckets upper-bound cutoffs for histogram buckets (strictly increasing, non-empty).
   * @return a {@link DistributionSummary} that can be used to record samples.
   * @throws IllegalArgumentException if {@code buckets} is empty.
   */
  public DistributionSummary histogram(String name, Tags meterTags, double... buckets) {
    checkArg(buckets.length > 0, "histogram requires at least one explicit bucket boundary");
    var histogram =
        DistributionSummary.builder(metricName(name))
            .tags(this.factoryTags.and(meterTags))
            .serviceLevelObjectives(buckets)
            .register(this.registry);
    this.registeredMeters.add(histogram);
    return histogram;
  }

  /**
   * Used to track the duration of short-running tasks. "Short-running" is subjective but the
   * assumption is that it should be under a minute.
   *
   * <p>Registers a timer with a {@code timeUnit=seconds} tag and the default percentile set ({@code
   * 0.5}, {@code 0.75}, {@code 0.9}, {@code 0.99}). Calling this method again with the same {@code
   * name} returns the existing {@link Timer} from the backing {@link MeterRegistry} rather than
   * creating a duplicate meter.
   *
   * @param name the name of the metric (without the factory namespace prefix)
   * @return a {@link Timer} that can record task durations
   */
  public Timer timerSec(String name) {
    return timer(name, TimeUnit.SECONDS);
  }

  /**
   * Used to track the duration of short-running tasks. "Short-running" is subjective but the
   * assumption is that it should be under a minute.
   *
   * <p>Registers a timer tagged with {@code timeUnit} set from {@code timeUnit} and the default
   * percentile set ({@code 0.5}, {@code 0.75}, {@code 0.9}, {@code 0.99}). Calling this method
   * again with the same {@code name} and {@code timeUnit} returns the existing {@link Timer} from
   * the backing {@link MeterRegistry} rather than creating a duplicate meter.
   *
   * @param name the name of the metric (without the factory namespace prefix)
   * @param timeUnit the unit used to record timer samples; exported as a {@code timeUnit} tag on
   *     the meter
   * @return a {@link Timer} that can record task durations
   */
  public Timer timer(String name, TimeUnit timeUnit) {
    return timer(name, getTimeUnitTag(timeUnit), DEFAULT_TIMER_PERCENTILES);
  }

  /**
   * Used to track the duration of short-running tasks. "Short-running" is subjective but the
   * assumption is that it should be under a minute.
   *
   * <p>Registers a timer tagged with {@code timeUnit} set from {@code timeUnit}, {@code meterTags},
   * and the default percentile set ({@code 0.5}, {@code 0.75}, {@code 0.9}, {@code 0.99}). If
   * {@code meterTags} already contains a {@code timeUnit} tag, it is not duplicated. Calling this
   * method again with the same {@code name}, {@code meterTags}, and {@code timeUnit} returns the
   * existing {@link Timer} from the backing {@link MeterRegistry} rather than creating a duplicate
   * meter.
   *
   * @param name the name of the metric (without the factory namespace prefix)
   * @param meterTags additional tags to add to the exported timer
   * @param timeUnit the unit used to record timer samples; exported as a {@code timeUnit} tag on
   *     the meter unless already present in {@code meterTags}
   * @return a {@link Timer} that can record task durations
   */
  public Timer timer(String name, Tags meterTags, TimeUnit timeUnit) {
    return timer(name, meterTags.and(getTimeUnitTag(timeUnit)), DEFAULT_TIMER_PERCENTILES);
  }

  /**
   * Used to track the duration of short-running tasks. "Short-running" is subjective but the
   * assumption is that it should be under a minute.
   */
  public Timer timer(String name, Tags meterTags, double... percentiles) {
    boolean hasTimeUnitTag =
        meterTags.stream().anyMatch(tag -> tag.getKey().equals(TIME_UNIT_TAG_KEY));
    // Prometheus recommends seconds as the base unit. Unit conversions should be handled on
    // display-side (see https://prometheus.io/docs/practices/naming/#base-units).
    Timer timer =
        Timer.builder(metricName(name))
            .tags(
                hasTimeUnitTag
                    ? this.factoryTags.and(meterTags)
                    : this.factoryTags.and(meterTags.and(getTimeUnitTag(TimeUnit.SECONDS))))
            .publishPercentiles(percentiles)
            .register(this.registry);
    this.registeredMeters.add(timer);
    return readFrom(timer);
  }

  /** Used to track the duration of long-running tasks (over a minute). */
  public LongTaskTimer longTaskTimer(String name, double... percentiles) {
    return longTaskTimer(name, Tags.empty(), percentiles);
  }

  public LongTaskTimer longTaskTimer(String name, Tags meterTags, double... percentiles) {
    LongTaskTimer.Builder builder =
        LongTaskTimer.builder(metricName(name)).tags(this.factoryTags.and(meterTags));
    // Percentiles must be either null or have length > 0 (see https://tinyurl.com/2p86tekm).
    LongTaskTimer longTaskTimer =
        percentiles.length > 0
            ? builder.publishPercentiles(percentiles).register(this.registry)
            : builder.register(this.registry);
    this.registeredMeters.add(longTaskTimer);
    return longTaskTimer;
  }

  public String metricName(String name) {
    return String.format("%s.%s", this.namespace, name);
  }

  public MetricsFactory childMetricsFactory(String childNamespace) {
    return new MetricsFactory(
        String.format("%s.%s", this.namespace, childNamespace),
        this.registry,
        this.factoryTags,
        this.identitySafeClose,
        this.readRegistry);
  }

  /**
   * Like {@link #childMetricsFactory} but with an independent namespace and tags (not inherited).
   */
  public MetricsFactory siblingMetricsFactory(String namespace, Tags factoryTags) {
    return new MetricsFactory(namespace, this.registry, factoryTags);
  }

  private boolean shouldRedirectReads() {
    return this.registry instanceof CompositeMeterRegistry composite
        && composite.getRegistries().contains(this.readRegistry);
  }

  private Counter readFrom(Counter counter) {
    if (!shouldRedirectReads()) {
      return counter;
    }
    Meter.Id id = counter.getId();
    return new ChildReadCounter(counter, this.readRegistry.counter(id.getName(), id.getTags()));
  }

  private Timer readFrom(Timer timer) {
    if (!shouldRedirectReads()) {
      return timer;
    }
    Meter.Id id = timer.getId();
    return new ChildReadTimer(
        timer, Timer.builder(id.getName()).tags(id.getTags()).register(this.readRegistry));
  }

  /** Records to the composite counter; reads the same meter in one of its children. */
  private record ChildReadCounter(Counter recorder, Counter reader) implements Counter {
    @Override
    public Id getId() {
      return this.recorder.getId();
    }

    @Override
    public void increment(double amount) {
      this.recorder.increment(amount);
    }

    @Override
    public double count() {
      return this.reader.count();
    }
  }

  /** Records to the composite timer; reads the same meter in one of its children. */
  private record ChildReadTimer(Timer recorder, Timer reader) implements Timer {
    @Override
    public Id getId() {
      return this.recorder.getId();
    }

    @Override
    public void record(long amount, TimeUnit unit) {
      this.recorder.record(amount, unit);
    }

    @Override
    public <T> T record(Supplier<T> f) {
      return this.recorder.record(f);
    }

    @Override
    public void record(Runnable f) {
      this.recorder.record(f);
    }

    @Override
    public <T> T recordCallable(Callable<T> f) throws Exception {
      return this.recorder.recordCallable(f);
    }

    @Override
    public long count() {
      return this.reader.count();
    }

    @Override
    public double totalTime(TimeUnit unit) {
      return this.reader.totalTime(unit);
    }

    @Override
    public double max(TimeUnit unit) {
      return this.reader.max(unit);
    }

    @Override
    public TimeUnit baseTimeUnit() {
      return this.reader.baseTimeUnit();
    }

    @Override
    public HistogramSnapshot takeSnapshot() {
      return this.reader.takeSnapshot();
    }
  }

  /** Closes all meters created by this MetricsFactory. */
  public synchronized void close() {
    if (this.identitySafeClose) {
      // Remove only meters the registry still maps to the exact instance we registered.
      Set<Meter> liveMeters = Collections.newSetFromMap(new IdentityHashMap<>());
      liveMeters.addAll(this.registry.getMeters());
      this.registeredMeters.stream().filter(liveMeters::contains).forEach(this.registry::remove);
    } else {
      this.registeredMeters.forEach(this.registry::remove);
    }
    this.registeredMeters.clear();
  }

  /**
   * Removes every meter this factory registered under {@code name} + {@code meterTags}, so
   * per-entity meters can be retired when their entity goes away instead of accumulating for the
   * process lifetime. Unlike {@link #close()} this leaves the factory usable.
   *
   * @param name the metric name, without namespace
   * @param meterTags the tags the meters were registered with
   */
  public synchronized void removeMeters(String name, Tags meterTags) {
    var fullName = metricName(name);
    var fullTags = this.factoryTags.and(meterTags);
    // Search matches on a subset of tags, so this still finds the series when the registry adds
    // tags of its own (e.g. common tags from config().commonTags). An exact-equality match would
    // miss those and leave the meter registered forever.
    this.registry.find(fullName).tags(fullTags).meters().stream()
        .toList()
        .forEach(
            meter -> {
              this.registry.remove(meter);
              this.registeredMeters.remove(meter);
            });
  }

  /** The tags applied to every meter created by this factory. */
  @VisibleForTesting
  public Tags factoryTags() {
    return this.factoryTags;
  }

  @VisibleForTesting
  public RequiredSearch get(String name) {
    return this.get(name, Tags.empty());
  }

  @VisibleForTesting
  public RequiredSearch get(String name, Tags meterTags) {
    return this.registry.get(metricName(name)).tags(this.factoryTags.and(meterTags));
  }

  /** Returns all meters registered by this MetricsFactory. */
  @VisibleForTesting
  public Set<Meter> getAllRegisteredMeters() {
    return ImmutableSet.copyOf(this.registeredMeters);
  }

  /** Maps Enum constant to micrometer counter. */
  public <T extends Enum<T>> Map<T, Counter> getEnumCounterMap(Class<T> enumClass) {
    checkArg(enumClass.isEnum(), "provided class is not Enum");
    return Stream.of(enumClass.getEnumConstants())
        .collect(
            Collectors.toMap(
                type -> type,
                type -> counter(Enums.convertNameTo(CaseFormat.LOWER_CAMEL, type)),
                (oldCounter, newCounter) ->
                    Check.unreachable("Duplicate enum key should not occur"),
                () -> new EnumMap<>(enumClass)));
  }
}
