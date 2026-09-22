package com.xgen.mongot.metrics;

import com.google.common.annotations.VisibleForTesting;
import com.google.errorprone.annotations.Var;
import com.xgen.mongot.metrics.micrometer.SerializableDistributionSummary;
import com.xgen.mongot.metrics.micrometer.SerializableTimer;
import com.xgen.mongot.util.Check;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.FunctionCounter;
import io.micrometer.core.instrument.FunctionTimer;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.ImmutableTag;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ServerStatusDataExtractor {
  private static final Logger logger = LoggerFactory.getLogger(ServerStatusDataExtractor.class);

  @SuppressWarnings("ImmutableEnumChecker")
  public enum Scope {
    JVM("jvm"),
    LUCENE("lucene"),
    REPLICATION("replication"),
    MMS("mms");

    private final ImmutableTag tag;

    Scope(String tagValue) {
      this.tag = new ImmutableTag("Scope", tagValue);
    }

    public Tag getTag() {
      return this.tag;
    }
  }

  private final MeterRegistry meterRegistry;

  public ServerStatusDataExtractor(MeterRegistry meterRegistry) {
    this.meterRegistry = meterRegistry;
  }

  public JvmMeterData createJvmMeterData() {
    return JvmMeterData.create(this.meterRegistry);
  }

  public LuceneMeterData createLuceneMeterData() {
    return LuceneMeterData.create(
        MeterExtractorFactory.getFactory(this.meterRegistry, Scope.LUCENE));
  }

  public ReplicationMeterData createReplicationMeterData() {
    return ReplicationMeterData.create(
        MeterExtractorFactory.getFactory(this.meterRegistry, Scope.REPLICATION));
  }

  public MmsMeterData createMmsMeterData() {
    return MmsMeterData.create(MeterExtractorFactory.getFactory(this.meterRegistry, Scope.MMS));
  }

  public ProcessMeterData createProcessMeterData() {
    return ProcessMeterData.create(this.meterRegistry);
  }

  public LoadSheddingMeterData createLoadSheddingMeterData() {
    return LoadSheddingMeterData.create(this.meterRegistry);
  }

  public static class JvmMeterData {
    static final String MAX_MEMORY_KEY = "jvm.memory.max";
    static final String USED_MEMORY_KEY = "jvm.memory.used";
    static final String GC_PAUSE_KEY = "jvm.gc.pause";
    static final String PROCESS_UPTIME = "process.uptime";

    static final Tag HEAP_TAGS = Tag.of("area", "heap");

    public final double totalHeapMemory;
    public final double usedHeapMemory;
    public final SerializableTimer jvmGcPause;

    public final double processUptime;

    @VisibleForTesting
    public JvmMeterData(
        double totalHeapMemory,
        double usedHeapMemory,
        SerializableTimer jvmGcPause,
        double processUptime) {
      this.totalHeapMemory = totalHeapMemory;
      this.usedHeapMemory = usedHeapMemory;
      this.jvmGcPause = jvmGcPause;
      this.processUptime = processUptime;
    }

    private static JvmMeterData create(MeterRegistry meterRegistry) {
      Map<String, Set<Meter>> jvmMeters = groupByScope(meterRegistry);

      Optional<Set<Meter>> jvmMaxMemoryMeters = Optional.ofNullable(jvmMeters.get(MAX_MEMORY_KEY));
      double heapMaxMemory =
          getHeapMetersTotal(Check.isPresent(jvmMaxMemoryMeters, "jvmMaxMemoryMeters"));

      Optional<Set<Meter>> jvmUsedMemoryMeters =
          Optional.ofNullable(jvmMeters.get(USED_MEMORY_KEY));
      double heapUsedMemory =
          getHeapMetersTotal(Check.isPresent(jvmUsedMemoryMeters, "jvmUsedMemoryMeters"));

      Optional<Set<Meter>> jvmGcPauseMeters = Optional.ofNullable(jvmMeters.get(GC_PAUSE_KEY));
      SerializableTimer gcPauseMeter = getGcPause(jvmGcPauseMeters);

      Optional<Set<Meter>> jvmProcessUptime = Optional.ofNullable(jvmMeters.get(PROCESS_UPTIME));
      double processUptime =
          getProcessUptime(Check.isPresent(jvmProcessUptime, "jvmProcessUptime"));

      return new JvmMeterData(heapMaxMemory, heapUsedMemory, gcPauseMeter, processUptime);
    }

    private static Map<String, Set<Meter>> groupByScope(MeterRegistry meterRegistry) {
      return meterRegistry.getMeters().stream()
          .filter(meter -> meter.getId().getTags().contains(Scope.JVM.getTag()))
          .collect(
              Collectors.toMap(
                  meter -> meter.getId().getName(),
                  Set::of,
                  (prevMeters, newMeter) ->
                      Stream.concat(prevMeters.stream(), newMeter.stream())
                          .collect(Collectors.toSet())));
    }

    private static double getHeapMetersTotal(Set<Meter> meters) {
      return meters.stream()
          // this tag is used in JVM meters that track heap metrics
          .filter(meter -> meter.getId().getTags().contains(HEAP_TAGS))
          .mapToDouble(ServerStatusDataExtractor::getMeterCount)
          .sum();
    }

    private static SerializableTimer getGcPause(Optional<Set<Meter>> meters) {
      if (meters.isEmpty()) {
        return new SerializableTimer(TimeUnit.SECONDS, 0L, 0.0, 0.0, 0.0, Optional.empty());
      }

      Meter gcPauseTimer =
          meters.get().stream()
              .filter(meter -> meter.getId().getName().equals(GC_PAUSE_KEY))
              .findFirst()
              .get();
      return SerializableTimer.create(gcPauseTimer);
    }

    private static double getProcessUptime(Set<Meter> meters) {
      Meter processUptimeMeter =
          meters.stream()
              .filter(meter -> meter.getId().getName().equals(PROCESS_UPTIME))
              .findFirst()
              .get();
      return getMeterCount(processUptimeMeter);
    }
  }

  public static class LuceneMeterData {
    public static final String NUM_MERGES_KEY = "numMerges";
    public static final String NUM_SEGMENTS_MERGED_KEY = "numSegmentsMerged";
    public static final String MERGE_SIZE_KEY = "mergeSize";
    public static final String MERGE_RESULT_SIZE_KEY = "mergeResultSize";
    public static final String MERGED_DOCS_KEY = "mergedDocs";
    public static final String SEGMENT_MERGE_TIME_KEY = "mergeTime";
    public static final String MERGE_CANCELLATION_TIME_KEY = "mergeCancellationTime";
    public static final String NUM_MERGES_ABORTED_KEY = "numMergesAborted";
    public static final String MERGE_PAUSE_EVENTS_KEY = "numMergePauseEvents";

    public final double numMerges;
    public final double numSegmentsMerged;
    public final SerializableDistributionSummary mergeSize;
    public final SerializableDistributionSummary mergeResultSize;
    public final SerializableDistributionSummary mergedDocs;
    public final SerializableTimer segmentMerge;
    public final SerializableTimer mergeCancellationTime;
    public final double numMergesAborted;
    public final double numMergePauseEvents;

    @VisibleForTesting
    public LuceneMeterData(
        double numMerges,
        double numSegmentsMerged,
        SerializableDistributionSummary mergeSize,
        SerializableDistributionSummary mergeResultSize,
        SerializableDistributionSummary mergedDocs,
        SerializableTimer segmentMerge,
        SerializableTimer mergeCancellationTime,
        double numMergesAborted,
        double numMergePauseEvents) {
      this.numMerges = numMerges;
      this.numSegmentsMerged = numSegmentsMerged;
      this.mergeSize = mergeSize;
      this.mergeResultSize = mergeResultSize;
      this.mergedDocs = mergedDocs;
      this.segmentMerge = segmentMerge;
      this.mergeCancellationTime = mergeCancellationTime;
      this.numMergesAborted = numMergesAborted;
      this.numMergePauseEvents = numMergePauseEvents;
    }

    private static LuceneMeterData create(MeterExtractorFactory meterExtractorFactory) {
      var numMerges = getMeterCount(meterExtractorFactory.create(NUM_MERGES_KEY).getSingleMeter());
      var numSegmentsMerged =
          getMeterCount(meterExtractorFactory.create(NUM_SEGMENTS_MERGED_KEY).getSingleMeter());
      var mergeSize =
          SerializableDistributionSummary.create(
              meterExtractorFactory.create(MERGE_SIZE_KEY).getSingleMeter());
      var mergeResultSize =
          SerializableDistributionSummary.create(
              meterExtractorFactory.create(MERGE_RESULT_SIZE_KEY).getSingleMeter());
      var mergedDocs =
          SerializableDistributionSummary.create(
              meterExtractorFactory.create(MERGED_DOCS_KEY).getSingleMeter());
      var segmentMergeTimer =
          SerializableTimer.create(
              meterExtractorFactory.create(SEGMENT_MERGE_TIME_KEY).getSingleMeter());
      var mergeCancellationTimer =
          SerializableTimer.create(
              meterExtractorFactory.create(MERGE_CANCELLATION_TIME_KEY).getSingleMeter());
      var numMergesAborted =
          getMeterCount(meterExtractorFactory.create(NUM_MERGES_ABORTED_KEY).getSingleMeter());
      var numMergePauseEvents =
          getMeterCount(meterExtractorFactory.create(MERGE_PAUSE_EVENTS_KEY).getSingleMeter());

      return new LuceneMeterData(
          numMerges,
          numSegmentsMerged,
          mergeSize,
          mergeResultSize,
          mergedDocs,
          segmentMergeTimer,
          mergeCancellationTimer,
          numMergesAborted,
          numMergePauseEvents);
    }
  }

  public static class ReplicationMeterData {
    public static final String STAGED_INDEXES = "stagedIndexes";
    public static final String LIVE_INDEXES = "indexesInCatalog";
    public static final String PHASING_OUT_INDEXES = "indexesPhasingOut";

    public final double stagedIndexes;
    public final double liveIndexes;
    public final double phasingOutIndexes;
    public final IndexingMeterData indexingMeterData;
    public final InitialSyncMeterData initialSyncMeterData;
    public final ChangeStreamMeterData changeStreamMeterData;
    public final MongodbClientMeterData mongodbClientMeterData;

    @VisibleForTesting
    public ReplicationMeterData(
        double stagedIndexes,
        double liveIndexes,
        double phasingOutIndexes,
        IndexingMeterData indexingMeterData,
        InitialSyncMeterData initialSyncMeterData,
        ChangeStreamMeterData changeStreamMeterData,
        MongodbClientMeterData mongodbClientMeterData) {
      this.stagedIndexes = stagedIndexes;
      this.liveIndexes = liveIndexes;
      this.phasingOutIndexes = phasingOutIndexes;
      this.indexingMeterData = indexingMeterData;
      this.initialSyncMeterData = initialSyncMeterData;
      this.changeStreamMeterData = changeStreamMeterData;
      this.mongodbClientMeterData = mongodbClientMeterData;
    }

    private static ReplicationMeterData create(MeterExtractorFactory meterExtractorFactory) {
      return new ReplicationMeterData(
          getMetersCount(meterExtractorFactory.create(STAGED_INDEXES).getAllMeters()),
          getMetersCount(meterExtractorFactory.create(LIVE_INDEXES).getAllMeters()),
          getMetersCount(meterExtractorFactory.create(PHASING_OUT_INDEXES).getAllMeters()),
          IndexingMeterData.create(meterExtractorFactory),
          InitialSyncMeterData.create(meterExtractorFactory),
          ChangeStreamMeterData.create(meterExtractorFactory),
          MongodbClientMeterData.create(meterExtractorFactory));
    }

    public static class IndexingMeterData {
      public static final String INDEXING_BATCH_DISTRIBUTION = "indexingBatchDistribution";
      public static final String INDEXING_BATCH_DURATIONS = "indexingBatchDurations";
      public static final String INDEXING_BATCH_SCHEDULING_DURATIONS =
          "indexingBatchSchedulingDurations";

      public final SerializableDistributionSummary indexingBatchDistribution;
      public final SerializableTimer indexingBatchDurations;
      public final SerializableTimer indexingBatchSchedulingDurations;

      @VisibleForTesting
      public IndexingMeterData(
          SerializableDistributionSummary indexingBatchDistribution,
          SerializableTimer indexingBatchDurations,
          SerializableTimer indexingBatchSchedulingDurations) {
        this.indexingBatchDistribution = indexingBatchDistribution;
        this.indexingBatchDurations = indexingBatchDurations;
        this.indexingBatchSchedulingDurations = indexingBatchSchedulingDurations;
      }

      static IndexingMeterData create(MeterExtractorFactory meterExtractorFactory) {
        return new IndexingMeterData(
            SerializableDistributionSummary.create(
                meterExtractorFactory.create(INDEXING_BATCH_DISTRIBUTION).getOptionalSingleMeter()),
            SerializableTimer.create(
                meterExtractorFactory.create(INDEXING_BATCH_DURATIONS).getOptionalSingleMeter()),
            SerializableTimer.create(
                meterExtractorFactory
                    .create(INDEXING_BATCH_SCHEDULING_DURATIONS)
                    .getOptionalSingleMeter()));
      }
    }

    public static class InitialSyncMeterData {
      public static final String WITNESSED_UPDATES = "witnessedInitialSyncUpdates";
      public static final String APPLICABLE_UPDATES = "applicableInitialSyncUpdates";

      public final double witnessedInitialSyncUpdates;
      public final double applicableInitialSyncUpdates;

      @VisibleForTesting
      public InitialSyncMeterData(double witnessedUpdates, double applicableUpdates) {
        this.witnessedInitialSyncUpdates = witnessedUpdates;
        this.applicableInitialSyncUpdates = applicableUpdates;
      }

      // TODO(CLOUDP-362251): Support auto-embedding indexes as materialized view manager register
      // meters with different namespace
      static InitialSyncMeterData create(MeterExtractorFactory meterExtractorFactory) {
        return new InitialSyncMeterData(
            getMeterCount(
                meterExtractorFactory
                    .create(WITNESSED_UPDATES, "initialSyncManager")
                    .getOptionalSingleMeter()),
            getMeterCount(
                meterExtractorFactory
                    .create(APPLICABLE_UPDATES, "initialSyncManager")
                    .getOptionalSingleMeter()));
      }
    }

    // TODO(CLOUDP-362251): Support auto-embedding indexes as materialized view manager register
    // meters with different namespace
    public static class ChangeStreamMeterData {
      public static final String WITNESSED_UPDATES = "witnessedChangeStreamUpdates";
      public static final String APPLICABLE_UPDATES = "applicableChangeStreamUpdates";

      public final double witnessedChangeStreamUpdates;
      public final double applicableChangeStreamUpdates;

      @VisibleForTesting
      public ChangeStreamMeterData(
          double witnessedChangeStreamUpdates, double applicableChangeStreamUpdates) {
        this.witnessedChangeStreamUpdates = witnessedChangeStreamUpdates;
        this.applicableChangeStreamUpdates = applicableChangeStreamUpdates;
      }

      static ChangeStreamMeterData create(MeterExtractorFactory meterExtractorFactory) {
        return new ChangeStreamMeterData(
            getMeterCount(
                meterExtractorFactory
                    .create(WITNESSED_UPDATES, "indexing.steadyStateChangeStream")
                    .getOptionalSingleMeter()),
            getMeterCount(
                meterExtractorFactory
                    .create(APPLICABLE_UPDATES, "indexing.steadyStateChangeStream")
                    .getOptionalSingleMeter()));
      }
    }

    public static class MongodbClientMeterData {
      public static final String FAILED_SESSION_REFRESHES = "failedSessionRefreshes";
      public static final String SESSION_REFRESH_DURATIONS = "sessionRefreshDurations";
      public static final String SUCCESSFUL_DYNAMIC_LINKING = "successfulOpenSSLDynamicLinking";
      public static final String FAILED_DYNAMIC_LINKING = "failedOpenSSLDynamicLinking";

      public final double failedSessionRefreshes;
      public final SerializableTimer sessionRefreshDurations;
      public final double successfulDynamicLinking;
      public final double failedDynamicLinking;

      @VisibleForTesting
      public MongodbClientMeterData(
          double failedSessionRefreshes,
          SerializableTimer sessionRefreshDurations,
          double successfulDynamicLinking,
          double failedDynamicLinking) {
        this.failedSessionRefreshes = failedSessionRefreshes;
        this.sessionRefreshDurations = sessionRefreshDurations;
        this.successfulDynamicLinking = successfulDynamicLinking;
        this.failedDynamicLinking = failedDynamicLinking;
      }

      // TODO(CLOUDP-362251): Support auto-embedding indexes as materialized view manager register
      // meters with different namespace.
      static MongodbClientMeterData create(MeterExtractorFactory meterExtractorFactory) {
        return new MongodbClientMeterData(
            getMeterCount(
                meterExtractorFactory
                    .create(FAILED_SESSION_REFRESHES, "replication.sessionRefresher")
                    .getOptionalSingleMeter()),
            SerializableTimer.create(
                meterExtractorFactory
                    .create(SESSION_REFRESH_DURATIONS, "replication.sessionRefresher")
                    .getOptionalSingleMeter()),
            getMeterCount(
                meterExtractorFactory
                    .create(SUCCESSFUL_DYNAMIC_LINKING, "mongoClientBuilder")
                    .getOptionalSingleMeter()),
            getMeterCount(
                meterExtractorFactory
                    .create(FAILED_DYNAMIC_LINKING, "mongoClientBuilder")
                    .getOptionalSingleMeter()));
      }
    }
  }

  public static class MmsMeterData {
    public static final String CONF_CALL_DURATIONS_KEY = "confCallDurations";
    public static final String SUCCESSFUL_CONF_CALLS_KEY = "successfulConfCalls";
    public static final String FAILED_CONF_CALLS_KEY = "failedConfCalls";

    public final SerializableTimer confCallDurations;
    public final double successfulConfCalls;
    public final double failedConfCalls;

    @VisibleForTesting
    public MmsMeterData(
        SerializableTimer confCallDurations, double successfulConfCalls, double failedConfCalls) {
      this.confCallDurations = confCallDurations;
      this.successfulConfCalls = successfulConfCalls;
      this.failedConfCalls = failedConfCalls;
    }

    public static MmsMeterData create(MeterExtractorFactory meterExtractorFactory) {
      var confCallDurations =
          SerializableTimer.create(
              meterExtractorFactory.create(CONF_CALL_DURATIONS_KEY).getOptionalSingleMeter());
      var successfulConfCalls =
          getMeterCount(
              meterExtractorFactory.create(SUCCESSFUL_CONF_CALLS_KEY).getOptionalSingleMeter());
      var failedConfCalls =
          getMeterCount(
              meterExtractorFactory.create(FAILED_CONF_CALLS_KEY).getOptionalSingleMeter());

      return new MmsMeterData(confCallDurations, successfulConfCalls, failedConfCalls);
    }
  }

  public static class ProcessMeterData {
    public static final String MAJOR_PAGE_FAULTS = "system.process.majorPageFaults";
    public static final String MINOR_PAGE_FAULTS = "system.process.minorPageFaults";

    public final double majorPageFaults;
    public final double minorPageFaults;

    @VisibleForTesting
    public ProcessMeterData(double majorPageFaults, double minorPageFaults) {
      this.majorPageFaults = majorPageFaults;
      this.minorPageFaults = minorPageFaults;
    }

    private static double getMeterValue(MeterRegistry meterRegistry, String meterName) {
      Meter meter = meterRegistry.find(meterName).meter();
      // If mongot is run with system instrumentation off in mms config (or locally) then process
      // metrics will not be available so we zero-fill.
      return meter != null ? getMeterCount(meter) : 0;
    }

    public static ProcessMeterData create(MeterRegistry meterRegistry) {
      return new ProcessMeterData(
          getMeterValue(meterRegistry, MAJOR_PAGE_FAULTS),
          getMeterValue(meterRegistry, MINOR_PAGE_FAULTS));
    }
  }

  public static class LoadSheddingMeterData {
    // Admission-control variant 1 (FIXED_POOL_UNBOUNDED_QUEUE): query is still admitted but the
    // virtual queue capacity was exceeded.
    public static final String WOULD_HAVE_REJECTED = "loadShedding.wouldHaveRejected";
    // Admission-control variant 2 (FIXED_POOL_BOUNDED_QUEUE): query is rejected outright.
    // A given executor only ever registers one of the two counters, so at most one is non-zero.
    public static final String REJECTED = "loadShedding.rejected";
    // search-envoy overload-retry signal (CLOUDP-438063): a mongot load-shedding rejection that
    // envoy may retry on another mongot. Prometheus-only (not in serverStatus); only registered
    // while the OVERLOAD_RETRY_SIGNAL feature flag is on.
    public static final String ENVOY_RETRY = "loadShedding.envoyRetry";
    public static final String ENVOY_RETRY_ATTEMPT_TAG = "attempt";
    public static final String ENVOY_RETRY_OUTCOME_TAG = "outcome";
    // outcome=rejected: mongot rejected this attempt and envoy may retry on another host.
    public static final String ENVOY_RETRY_REJECTED = "rejected";
    // outcome=exhausted: envoy's retry budget is spent and the error is surfaced to the driver.
    public static final String ENVOY_RETRY_EXHAUSTED = "exhausted";
    // Total commands received by mongot regardless of outcome (success, rejection, error).
    // Denominator for load-shedding / envoy-retry rates. Prometheus-only (not in serverStatus).
    // Tagged retry=true when x-envoy-attempt-count > 1 so first attempts and envoy retries
    // are separable without subtracting rejection counters.
    public static final String TOTAL_REQUESTS = "command.totalRequests";
    public static final String TOTAL_REQUESTS_RETRY_TAG = "retry";
    public static final String TOTAL_REQUESTS_RETRY_TRUE = "true";
    public static final String TOTAL_REQUESTS_RETRY_FALSE = "false";

    public final double wouldHaveRejectedTotal;
    public final double rejectedTotal;

    @VisibleForTesting
    public LoadSheddingMeterData(double wouldHaveRejectedTotal, double rejectedTotal) {
      this.wouldHaveRejectedTotal = wouldHaveRejectedTotal;
      this.rejectedTotal = rejectedTotal;
    }

    private static double getMeterValue(MeterRegistry meterRegistry, String meterName) {
      Meter meter = meterRegistry.find(meterName).meter();
      return meter != null ? getMeterCount(meter) : 0;
    }

    public static LoadSheddingMeterData create(MeterRegistry meterRegistry) {
      return new LoadSheddingMeterData(
          getMeterValue(meterRegistry, WOULD_HAVE_REJECTED),
          getMeterValue(meterRegistry, REJECTED));
    }
  }

  private static double getMetersCount(Set<Meter> meters) {
    return meters.stream().mapToDouble(ServerStatusDataExtractor::getMeterCount).sum();
  }

  private static double getMeterCount(Optional<Meter> meter) {
    return meter.map(ServerStatusDataExtractor::getMeterCount).orElse(0.0);
  }

  private static double getMeterCount(Meter meter) {
    return meter.match(
        Gauge::value,
        Counter::count,
        timer -> (double) timer.count(),
        distributionSummary -> (double) distributionSummary.count(),
        longTaskTimer -> (double) longTaskTimer.activeTasks(),
        Gauge::value,
        FunctionCounter::count,
        FunctionTimer::count,
        unused -> {
          logger.error("Unexpected meter type: {}", meter.getClass().getName());
          return Check.unreachable("Unexpected meter type");
        });
  }

  private static class MeterExtractorFactory {
    private final Map<String, Set<Meter>> nameToMeters;

    private MeterExtractorFactory(Map<String, Set<Meter>> nameToMeters) {
      this.nameToMeters = nameToMeters;
    }

    static MeterExtractorFactory getFactory(MeterRegistry meterRegistry, Scope scope) {
      return new MeterExtractorFactory(groupByScope(meterRegistry, scope));
    }

    /** Returns a MeterExtractor for meters with the given meter name from different namespaces. */
    MeterExtractor create(String meterName) {
      return createInternal(meterName, Optional.empty());
    }

    /**
     * Returns a MeterExtractor for meters with the given meter name and namespaces prefix filter.
     */
    MeterExtractor create(String meterName, String namespacePrefixFilter) {
      return createInternal(meterName, Optional.of(namespacePrefixFilter));
    }

    private MeterExtractor createInternal(
        String meterName, Optional<String> namespacePrefixFilter) {
      @Var Set<Meter> meters = this.nameToMeters.getOrDefault(meterName, Set.of());
      if (namespacePrefixFilter.isPresent()) {
        meters =
            meters.stream()
                .filter(meter -> meter.getId().getName().startsWith(namespacePrefixFilter.get()))
                .collect(Collectors.toSet());
      }
      return new MeterExtractor(meters);
    }

    /**
     * Some meters are registered twice with the same name but different tags. We group such meters
     * into a set.
     */
    private static Map<String, Set<Meter>> groupByScope(MeterRegistry meterRegistry, Scope scope) {
      return meterRegistry.getMeters().stream()
          .filter(meter -> meter.getId().getTags().contains(scope.getTag()))
          .collect(
              Collectors.toMap(
                  meter -> getMeterName(meter.getId().getName()),
                  Set::of,
                  (prevMeters, newMeter) ->
                      Stream.concat(prevMeters.stream(), newMeter.stream())
                          .collect(Collectors.toSet())));
    }

    private static String getMeterName(String nameWithClassPath) {
      return Arrays.asList(StringUtils.split(nameWithClassPath, '.')).getLast();
    }
  }

  private static class MeterExtractor {
    private final Set<Meter> meters;

    private MeterExtractor(Set<Meter> meters) {
      this.meters = meters;
    }

    /**
     * Returns the single meter if it exists, or empty if no meters are present. Throws exception if
     * the same meter is registered multiple times.
     */
    public Optional<Meter> getOptionalSingleMeter() {
      if (this.meters.isEmpty()) {
        return Optional.empty();
      }
      Check.checkState(
          this.meters.size() == 1,
          "Either 0 or 1 meter must be present, but found %s meters, including: %s",
          this.meters.size(),
          this.meters.stream().map(meter -> meter.getId().getName()).toList());

      return this.meters.stream().findFirst();
    }

    /**
     * Returns the single meter if it exists. Throws exception if no meters are present or if the
     * same meter is registered multiple times.
     */
    public Meter getSingleMeter() {
      Check.checkState(
          this.meters.size() == 1,
          "exactly 1 meter must be present, but found %s meters, including: %s",
          this.meters.size(),
          this.meters.stream().map(meter -> meter.getId().getName()).toList());

      return this.meters.stream().findFirst().get();
    }

    /** Returns all meters by this meter name. */
    public Set<Meter> getAllMeters() {
      return this.meters;
    }
  }
}
