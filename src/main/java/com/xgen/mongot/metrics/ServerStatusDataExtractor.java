package com.xgen.mongot.metrics;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.CaseFormat;
import com.xgen.mongot.metrics.micrometer.SerializableDistributionSummary;
import com.xgen.mongot.metrics.micrometer.SerializableTimer;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.Enums;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.FunctionCounter;
import io.micrometer.core.instrument.FunctionTimer;
import io.micrometer.core.instrument.Gauge;
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

  public enum Scope {
    JVM,
    LUCENE,
    REPLICATION,
    MMS;

    public Tag getTag() {
      return Tag.of("Scope", Enums.convertNameTo(CaseFormat.LOWER_CAMEL, this));
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
        BaseMeterExtractorBuilder.create(this.meterRegistry, Scope.LUCENE));
  }

  public ReplicationMeterData createReplicationMeterData() {
    return ReplicationMeterData.create(
        BaseMeterExtractorBuilder.create(this.meterRegistry, Scope.REPLICATION));
  }

  public MmsMeterData createMmsMeterData() {
    return MmsMeterData.create(BaseMeterExtractorBuilder.create(this.meterRegistry, Scope.MMS));
  }

  public ProcessMeterData createProcessMeterData() {
    return ProcessMeterData.create(this.meterRegistry);
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

    public final double numMerges;
    public final double numSegmentsMerged;
    public final SerializableDistributionSummary mergeSize;
    public final SerializableDistributionSummary mergeResultSize;
    public final SerializableDistributionSummary mergedDocs;
    public final SerializableTimer segmentMerge;

    @VisibleForTesting
    public LuceneMeterData(
        double numMerges,
        double numSegmentsMerged,
        SerializableDistributionSummary mergeSize,
        SerializableDistributionSummary mergeResultSize,
        SerializableDistributionSummary mergedDocs,
        SerializableTimer segmentMerge) {
      this.numMerges = numMerges;
      this.numSegmentsMerged = numSegmentsMerged;
      this.mergeSize = mergeSize;
      this.mergeResultSize = mergeResultSize;
      this.mergedDocs = mergedDocs;
      this.segmentMerge = segmentMerge;
    }

    private static LuceneMeterData create(BaseMeterExtractorBuilder baseMeterExtractorBuilder) {
      var numMerges =
          getMeterCount(
              baseMeterExtractorBuilder.builder().meterName(NUM_MERGES_KEY).getSingleMeter());
      var numSegmentsMerged =
          getMeterCount(
              baseMeterExtractorBuilder
                  .builder()
                  .meterName(NUM_SEGMENTS_MERGED_KEY)
                  .getSingleMeter());
      var mergeSize =
          SerializableDistributionSummary.create(
              baseMeterExtractorBuilder.builder().meterName(MERGE_SIZE_KEY).getSingleMeter());
      var mergeResultSize =
          SerializableDistributionSummary.create(
              baseMeterExtractorBuilder
                  .builder()
                  .meterName(MERGE_RESULT_SIZE_KEY)
                  .getSingleMeter());
      var mergedDocs =
          SerializableDistributionSummary.create(
              baseMeterExtractorBuilder.builder().meterName(MERGED_DOCS_KEY).getSingleMeter());
      var segmentMergeTimer =
          SerializableTimer.create(
              baseMeterExtractorBuilder
                  .builder()
                  .meterName(SEGMENT_MERGE_TIME_KEY)
                  .getSingleMeter());

      return new LuceneMeterData(
          numMerges, numSegmentsMerged, mergeSize, mergeResultSize, mergedDocs, segmentMergeTimer);
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

    private static ReplicationMeterData create(
        BaseMeterExtractorBuilder baseMeterExtractorBuilder) {
      return new ReplicationMeterData(
          getMetersCount(
              baseMeterExtractorBuilder.builder().meterName(STAGED_INDEXES).getMultipleMeters()),
          getMetersCount(
              baseMeterExtractorBuilder.builder().meterName(LIVE_INDEXES).getMultipleMeters()),
          getMetersCount(
              baseMeterExtractorBuilder
                  .builder()
                  .meterName(PHASING_OUT_INDEXES)
                  .getMultipleMeters()),
          IndexingMeterData.create(baseMeterExtractorBuilder),
          InitialSyncMeterData.create(baseMeterExtractorBuilder),
          ChangeStreamMeterData.create(baseMeterExtractorBuilder),
          MongodbClientMeterData.create(baseMeterExtractorBuilder));
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

      static IndexingMeterData create(BaseMeterExtractorBuilder baseMeterExtractorBuilder) {
        return new IndexingMeterData(
            SerializableDistributionSummary.create(
                baseMeterExtractorBuilder
                    .builder()
                    .meterName(INDEXING_BATCH_DISTRIBUTION)
                    .getOptionalSingleMeters()),
            SerializableTimer.create(
                baseMeterExtractorBuilder
                    .builder()
                    .meterName(INDEXING_BATCH_DURATIONS)
                    .getOptionalSingleMeters()),
            SerializableTimer.create(
                baseMeterExtractorBuilder
                    .builder()
                    .meterName(INDEXING_BATCH_SCHEDULING_DURATIONS)
                    .getOptionalSingleMeters()));
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

      static InitialSyncMeterData create(BaseMeterExtractorBuilder baseMeterExtractorBuilder) {
        return new InitialSyncMeterData(
            getMeterCount(
                baseMeterExtractorBuilder
                    .builder()
                    .meterName(WITNESSED_UPDATES)
                    .getOptionalSingleMeters()),
            getMeterCount(
                baseMeterExtractorBuilder
                    .builder()
                    .meterName(APPLICABLE_UPDATES)
                    .getOptionalSingleMeters()));
      }
    }

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

      static ChangeStreamMeterData create(BaseMeterExtractorBuilder baseMeterExtractorBuilder) {
        return new ChangeStreamMeterData(
            getMeterCount(
                baseMeterExtractorBuilder
                    .builder()
                    .meterName(WITNESSED_UPDATES)
                    .getOptionalSingleMeters()),
            getMeterCount(
                baseMeterExtractorBuilder
                    .builder()
                    .meterName(APPLICABLE_UPDATES)
                    .getOptionalSingleMeters()));
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

      static MongodbClientMeterData create(BaseMeterExtractorBuilder baseMeterExtractorBuilder) {
        return new MongodbClientMeterData(
            getMeterCount(
                baseMeterExtractorBuilder
                    .builder()
                    .meterName(FAILED_SESSION_REFRESHES)
                    .getOptionalSingleMeters()),
            SerializableTimer.create(
                baseMeterExtractorBuilder
                    .builder()
                    .meterName(SESSION_REFRESH_DURATIONS)
                    .getOptionalSingleMeters()),
            getMeterCount(
                baseMeterExtractorBuilder
                    .builder()
                    .meterName(SUCCESSFUL_DYNAMIC_LINKING)
                    .getOptionalSingleMeters()),
            getMeterCount(
                baseMeterExtractorBuilder
                    .builder()
                    .meterName(FAILED_DYNAMIC_LINKING)
                    .getOptionalSingleMeters()));
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

    public static MmsMeterData create(BaseMeterExtractorBuilder baseMeterExtractorBuilder) {
      var confCallDurations =
          SerializableTimer.create(
              baseMeterExtractorBuilder
                  .builder()
                  .meterName(CONF_CALL_DURATIONS_KEY)
                  .getOptionalSingleMeters());
      var successfulConfCalls =
          getMeterCount(
              baseMeterExtractorBuilder
                  .builder()
                  .meterName(SUCCESSFUL_CONF_CALLS_KEY)
                  .getOptionalSingleMeters());
      var failedConfCalls =
          getMeterCount(
              baseMeterExtractorBuilder
                  .builder()
                  .meterName(FAILED_CONF_CALLS_KEY)
                  .getOptionalSingleMeters());

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

  private static class BaseMeterExtractorBuilder {
    private final Map<String, Set<Meter>> nameToMeters;

    private BaseMeterExtractorBuilder(Map<String, Set<Meter>> nameToMeters) {
      this.nameToMeters = nameToMeters;
    }

    static BaseMeterExtractorBuilder create(MeterRegistry meterRegistry, Scope scope) {
      return new BaseMeterExtractorBuilder(groupByScope(meterRegistry, scope));
    }

    MeterExtractorBuilder builder() {
      return new MeterExtractorBuilder(this);
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

  private static class MeterExtractorBuilder {
    private final BaseMeterExtractorBuilder baseMeterExtractorBuilder;

    private Optional<String> meterName = Optional.empty();

    private MeterExtractorBuilder(BaseMeterExtractorBuilder baseMeterExtractorBuilder) {
      this.baseMeterExtractorBuilder = baseMeterExtractorBuilder;
    }

    public MeterExtractorBuilder meterName(String meterName) {
      this.meterName = Optional.of(meterName);
      return this;
    }

    public Set<Meter> getMultipleMeters() {
      String name = Check.isPresent(this.meterName, "meterName");

      Optional<Set<Meter>> meters =
          Optional.ofNullable(this.baseMeterExtractorBuilder.nameToMeters.get(name));
      return Check.isPresent(meters, "meters");
    }

    public Optional<Meter> getOptionalSingleMeters() {
      String name = Check.isPresent(this.meterName, "meterName");

      Optional<Set<Meter>> meters =
          Optional.ofNullable(this.baseMeterExtractorBuilder.nameToMeters.get(name));

      if (meters.isEmpty()) {
        return Optional.empty();
      }
      Check.checkState(
          meters.get().size() == 1,
          "exactly 1 meter must be present, but found %s, including: %s",
          meters.get().size(),
          meters.get().stream().map(meter -> meter.getId().getName()).toList());

      return meters.get().stream().findFirst();
    }

    public Meter getSingleMeter() {
      Set<Meter> meters = getMultipleMeters();
      Check.checkState(
          meters.size() == 1,
          "exactly 1 meter must be present, but found %s, including: %s",
          meters.size(),
          meters.stream().map(meter -> meter.getId().getName()).toList());

      return meters.stream().findFirst().get();
    }
  }
}
