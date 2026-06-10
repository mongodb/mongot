package com.xgen.mongot.metrics.ftdc;

import static com.google.common.collect.ImmutableSortedMap.toImmutableSortedMap;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.flogger.FluentLogger;
import com.google.errorprone.annotations.CompileTimeConstant;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import com.xgen.mongot.metrics.ThreadPoolResourceMetrics;
import com.xgen.mongot.metrics.micrometer.SerializableDistributionSummary;
import com.xgen.mongot.metrics.micrometer.SerializableTimer;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.VerboseRunnable;
import com.xgen.mongot.util.concurrent.Executors;
import com.xgen.mongot.util.concurrent.NamedScheduledExecutorService;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.FunctionCounter;
import io.micrometer.core.instrument.FunctionTimer;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.LongTaskTimer;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.TimeGauge;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.concurrent.TimeUnit;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Implements periodic reporting to FTDC. * */
public class FtdcScheduledReporter {
  static final String FTDC_MONGOT_METER_PREFIX = "mongot";
  static final String FTDC_MONGOT_REPORTING_TIMER_NAME = "ftdc-reporting-time";
  static final Tags FTDC_MONGOT_REPORTING_TIMER_TAGS = Tags.of("timeUnit", "milliseconds");
  private static final Logger LOG = LoggerFactory.getLogger(FtdcScheduledReporter.class);
  private static final FluentLogger FLOGGER = FluentLogger.forEnclosingClass();
  private final Reporter reporter;
  private final NamedScheduledExecutorService executor;

  /**
   * Default maximum number of meters allowed before FTDC reporting is skipped when the meter limit
   * is enabled.
   *
   * <p>This is an approximation of the number of meters that will be reported for a typical
   * deployment with 100 indexes.
   */
  public static final int DEFAULT_MAX_METER_COUNT = 40000;

  /**
   * Creates a new FtdcScheduledReporter with a meter count limit.
   *
   * @param reportingMeterRegistry the registry to use for reporting metrics to FTDC. This should be
   *     the ftdcRegistry to ensure only FTDC-specific metrics are reported.
   * @param combinedMeterRegistry the registry to use for executor metrics. This could be a
   *     CompositeMeterRegistry so that executor metrics are exported to all sinks (e.g., Prometheus
   *     and FTDC) if this is enabled as a feature flag. Otherwise, it can be a SimpleMeterRegistry
   *     to keep executor metrics only in FTDC.
   * @param ftdc the FTDC instance to write metrics to
   * @param useCombinedRegistryForExecutorMetrics whether to use the combined meter registry for
   *     executor metrics
   * @param maxMeterCount the maximum number of meters before FTDC reporting is skipped. Use {@link
   *     Integer#MAX_VALUE} to disable the limit.
   * @return the newly created FtdcScheduledReporter
   */
  public static FtdcScheduledReporter create(
      MeterRegistry reportingMeterRegistry,
      CompositeMeterRegistry combinedMeterRegistry,
      Ftdc ftdc,
      boolean useCombinedRegistryForExecutorMetrics,
      int maxMeterCount) {
    Check.argIsPositive(maxMeterCount, "maxMeterCount");
    MeterRegistry executorRegistry =
        useCombinedRegistryForExecutorMetrics ? combinedMeterRegistry : reportingMeterRegistry;
    return new FtdcScheduledReporter(executorRegistry, reportingMeterRegistry, ftdc, maxMeterCount);
  }

  /**
   * Creates a new FtdcScheduledReporter.
   *
   * @param executorRegistry the registry to use for executor metrics. This could be a
   *     CompositeMeterRegistry so that executor metrics are exported to all sinks (e.g., Prometheus
   *     and FTDC) if this is enabled as a feature flag. Otherwise, it can be a SimpleMeterRegistry
   *     to keep executor metrics only in FTDC.
   * @param reportingRegistry the registry to use for reporting metrics to FTDC. This should be the
   *     ftdcRegistry to ensure only FTDC-specific metrics are reported.
   * @param ftdc the FTDC instance to write metrics to
   * @param maxMeterCount the maximum number of meters before FTDC reporting is skipped
   */
  private FtdcScheduledReporter(
      MeterRegistry executorRegistry,
      MeterRegistry reportingRegistry,
      Ftdc ftdc,
      int maxMeterCount) {
    this.executor = Executors.singleThreadScheduledExecutor("ftdc-reporter", executorRegistry);
    ThreadPoolResourceMetrics.create("ftdc").register(this.executor, executorRegistry);
    this.reporter = new Reporter(executorRegistry, reportingRegistry, ftdc, maxMeterCount);
  }

  /**
   * Returns an FTDC failure counter labeled by context.
   *
   * @param context the compile-time constant context for the FTDC failure
   * @param registry the initialized MeterRegistry
   * @return the counter corresponding to the given context
   */
  private static Counter ftdcFailureCounter(
      @CompileTimeConstant String context, MeterRegistry registry) {
    return registry.counter("mongot.ftdc_executor_failure", Tags.of("context", context));
  }

  /** Start reporting ftdc metrics at a fixed delay. */
  public void start(long delay, TimeUnit unit) {
    LOG.atInfo()
        .addKeyValue("interval", delay)
        .addKeyValue("timeUnit", unit.name().toLowerCase())
        .log("Starting periodic ftdc reporting");
    this.executor.scheduleWithFixedDelay(this.reporter, delay, delay, unit);
  }

  /** Stops periodic scheduling. */
  public void stop() {
    LOG.info("Shutting down ftdc reporting");
    // Stop reporting, waiting for a current report to finish if one is running.
    Executors.shutdownOrFail(this.executor);
  }

  static class Reporter implements VerboseRunnable {
    @GuardedBy("this")
    private final Ftdc ftdc;

    private final MeterRegistry meterRegistry;
    private final MeterRegistry executorRegistry;
    private final int maxMeterCount;

    /** Timer to export metric of how long reporting takes. */
    private final Timer reportingTimer;

    /**
     * Meter names have to be recomputed on every poll (once per second) and are fairly expensive
     * (prefix + metric_name + ",".join(tags)). This map caches the meter name with weak keys so
     * that they are free to be garbage collected whenever a metric is removed.
     */
    private final WeakHashMap<Meter.Id, String> meterNameCache = new WeakHashMap<>();

    @VisibleForTesting
    Reporter(MeterRegistry executorRegistry, MeterRegistry reportingRegistry, Ftdc ftdc) {
      this(executorRegistry, reportingRegistry, ftdc, Integer.MAX_VALUE);
    }

    @VisibleForTesting
    Reporter(
        MeterRegistry executorRegistry,
        MeterRegistry reportingRegistry,
        Ftdc ftdc,
        int maxMeterCount) {
      this.executorRegistry = executorRegistry;
      this.meterRegistry = reportingRegistry;
      this.ftdc = ftdc;
      this.maxMeterCount = maxMeterCount;
      this.reportingTimer =
          Timer.builder(FTDC_MONGOT_REPORTING_TIMER_NAME)
              .tags(FTDC_MONGOT_REPORTING_TIMER_TAGS)
              .publishPercentiles(0.5, 0.75, 0.9, 0.99)
              .register(reportingRegistry);
    }

    @Override
    public void verboseRun() {
      this.reportingTimer.record(this::report);
    }

    @Override
    public Logger getLogger() {
      return LOG;
    }

    /** Report the current values of all metrics in the registry. */
    synchronized void report() {
      try {
        List<Meter> meters = this.meterRegistry.getMeters();
        int meterCount = meters.size();

        if (meterCount > this.maxMeterCount) {
          FLOGGER.atWarning().atMostEvery(1, TimeUnit.HOURS).log(
              "Skipping FTDC reporting: meter count (%d) exceeds limit (%d). "
                  + "This typically happens when there are too many indexes.",
              meterCount, this.maxMeterCount);
          return;
        }

        // we sort by meter names so metrics in FTDC will be displayed in alphabetical order and for
        // the BSON document to be a consistent format across all FTDC samples
        ImmutableSortedMap<String, Meter> sortedMeterMap =
            meters.stream()
                .collect(
                    toImmutableSortedMap(
                        Comparator.naturalOrder(), this::createMeterName, meter -> meter));

        long epochTime = System.currentTimeMillis();
        // t2 likes the first metric to be the time of the sample
        BsonDocument doc = new BsonDocument("start", new BsonInt64(epochTime));

        sortedMeterMap.entrySet().forEach(entry -> addToDoc(doc, entry));

        try {
          this.ftdc.addSample(doc, epochTime);
        } catch (IOException e) {
          // We catch an IOException here because this could just be a transient error and we do not
          // want to fail the ScheduledReporter entirely
          LOG.error("error adding ftdc sample", e);
          ftdcFailureCounter("ioExceptionDuringAddSample", this.executorRegistry).increment();
        }
      } catch (Exception e) {
        // Catch any exceptions during metric collection or document creation to prevent them from
        // interrupting the entire reporting cycle. Log the error but allow the reporter to
        // continue.
        LOG.atError().setCause(e).log("Error during metric reporting, skipping this report cycle");

        ftdcFailureCounter("exceptionDuringReport", this.executorRegistry).increment();
      }
    }

    private void addToDoc(BsonDocument doc, Map.Entry<String, Meter> entry) {
      // If a gauge throws an exception, it will not fail the entire report, but instead cause an
      // Undefined Behavior in the actual meter value being recorded. For example, a double type
      // value could result in a NaN value in the metrics report.
      try {
        String meterName = entry.getKey();
        Meter meter = entry.getValue();

        MeterDocumentBuilder meterDocumentBuilder = new MeterDocumentBuilder(doc, meterName);
        meterDocumentBuilder.matchMeter(meter);
      } catch (Exception e) {
        LOG.atError()
            .setCause(e)
            .addKeyValue("meterName", entry.getKey())
            .log("Error processing meter, skipping this meter");

        ftdcFailureCounter("exceptionDuringAddToDoc", this.executorRegistry).increment();
      }
    }

    String createMeterName(Meter meter) {
      return this.meterNameCache.computeIfAbsent(
          meter.getId(),
          id -> {
            String meterName = id.getName();
            Iterable<Tag> tags = id.getTagsAsIterable();
            Tags sortedTags = Tags.of(tags);
            return FTDC_MONGOT_METER_PREFIX + "," + meterName + "." + sortedTags;
          });
    }

    /** Populates the BsonDocument with data from a single meter. */
    private static class MeterDocumentBuilder {
      // T2 will truncate doubles to integers, we will multiply the value of following metrics by
      // 1000 when reporting to FTDC.
      private static final Set<String> GAUGE_METRICS_TO_SCALE =
          Set.of("system.load.average.1m", "system.cpu.usage", "process.cpu.usage");
      private static final Set<String> TIMER_METRICS_TO_SCALE =
          Set.of(
              "index.stats.indexing.indexingBatchDurations",
              "indexingWorkScheduler.indexingBatchSchedulingDurations",
              "index.stats.replication.steadyState.decodingBatchDurations",
              "decodingWorkScheduler.decodingBatchSchedulingDurations",
              "indexing.steadyStateChangeStream.batchesInProgressTotalDurations",
              "indexing.steadyStateChangeStream.preprocessingBatchDurations",
              "indexing.steadyStateChangeStream.getMoreDurations",
              "indexing.steadyStateChangeStream.getMoresSchedulingDurations",
              "initialSyncManager.changeStreamTime",
              "initialSyncManager.changeStreamPreprocessingBatchDurations",
              "indexing.initialSyncChangeStream.getMoreDurations",
              "initialSyncManager.collectionScanTime",
              "initialSyncManager.collectionScanPreprocessingBatchDurations",
              "indexing.initialSyncCollectionScan.getMoreDurations");

      private final BsonDocument doc;
      private final String meterName;

      private MeterDocumentBuilder(BsonDocument doc, String meterName) {
        this.doc = doc;
        this.meterName = meterName;
      }

      private void matchMeter(Meter meter) {
        meter.use(
            this::visitGauge,
            this::visitCounter,
            this::visitTimer,
            this::visitSummary,
            this::visitLongTaskTimer,
            this::visitTimeGauge,
            this::visitFunctionCounter,
            this::visitFunctionTimer,
            this::visitMeter);
      }

      private void visitGauge(Gauge gauge) {
        if (GAUGE_METRICS_TO_SCALE.contains(gauge.getId().getName())) {
          this.doc.append(this.meterName + "‰", new BsonDouble(gauge.value() * 1000));
        } else {
          this.doc.append(this.meterName, new BsonDouble(gauge.value()));
        }
      }

      private void visitCounter(Counter counter) {
        this.doc.append(this.meterName, new BsonDouble(counter.count()));
      }

      private void visitTimer(Timer timer) {
        putDocumentMetersFlat(
            SerializableTimer.create(timer).toBson(), this.meterName, timer.getId().getName());
      }

      private void visitSummary(DistributionSummary summary) {
        putDocumentMetersFlat(
            SerializableDistributionSummary.create(summary).toBson(),
            this.meterName,
            summary.getId().getName());
      }

      // fullMeterName is prepended with "mongot," and includes tags, ie. "[tag(timeUnit=seconds)]".
      // meterName is used to determine which metrics to scale.
      private void putDocumentMetersFlat(BsonDocument doc, String fullMeterName, String meterName) {
        for (var entry : doc.entrySet()) {
          String key = fullMeterName + "." + entry.getKey();
          if (entry.getValue() instanceof BsonDocument childDoc) {
            putDocumentMetersFlat(childDoc, key, meterName);
          } else {
            if (TIMER_METRICS_TO_SCALE.contains(meterName) && entry.getValue().isDouble()) {
              this.doc.put(
                  key + "Scaled", new BsonDouble(entry.getValue().asDouble().getValue() * 1000));
            } else {
              this.doc.put(key, entry.getValue());
            }
          }
        }
      }

      private void visitLongTaskTimer(LongTaskTimer timer) {
        this.doc.append(this.meterName, new BsonInt32(timer.activeTasks()));
      }

      private void visitTimeGauge(TimeGauge gauge) {
        this.doc.append(this.meterName, new BsonDouble(gauge.value()));
      }

      private void visitFunctionCounter(FunctionCounter counter) {
        this.doc.append(this.meterName, new BsonDouble(counter.count()));
      }

      private void visitFunctionTimer(FunctionTimer timer) {
        this.doc.append(this.meterName, new BsonDouble(timer.count()));
      }

      private void visitMeter(Meter meter) {
        LOG.error("Unexpected meter type: {}", meter.getId().getType());
        Check.unreachable("Unexpected meter type");
      }
    }
  }
}
