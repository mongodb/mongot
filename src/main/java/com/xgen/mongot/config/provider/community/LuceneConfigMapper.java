package com.xgen.mongot.config.provider.community;

import com.xgen.mongot.config.util.HysteresisConfig;
import com.xgen.mongot.index.lucene.config.LuceneConfig;
import com.xgen.mongot.util.Bytes;
import com.xgen.mongot.util.Runtime;
import java.time.Duration;
import java.util.Optional;

/**
 * Maps the Community {@code indexing} and {@code querying} config blocks onto the runtime {@link
 * LuceneConfig}.
 */
public final class LuceneConfigMapper {
  private static final int DOCS_LIMIT = LuceneConfig.MAX_DOCS_LIMIT;
  // floorSegmentMB is only consumed by the vector merge policy.
  // Regular search indexes uses Lucene's default floor segment size.
  // Used as the default when querying.lucene.floorSegmentMB is unset.
  private static final double DEFAULT_FLOOR_SEGMENT_MB = 64.0;
  private static final Optional<Boolean> ENABLE_CONCURRENT_SEARCH = Optional.of(true);
  private static final double MAX_VECTOR_INPUT_TO_MERGE_BUDGET_RATIO = 0.5;
  private static final double MAX_COMPOUND_DATA_TO_MAX_VECTOR_INPUT_RATIO = 0.1;

  private LuceneConfigMapper() {}

  /**
   * Builds a {@link LuceneConfig} from the parsed community config.
   *
   * @param config the community configuration
   * @param runtime runtime used to get numCpus and JVM heap size
   * @param mergePolicyDiskUtilizationConfig disk-utilization thresholds gating merges, if any
   */
  public static LuceneConfig toLuceneConfig(
      CommunityConfig config,
      Runtime runtime,
      Optional<HysteresisConfig> mergePolicyDiskUtilizationConfig) {
    var indexingLucene =
        config
            .advancedConfigs()
            .flatMap(AdvancedConfigs::indexingConfig)
            .flatMap(CommunityIndexingConfig::luceneConfig);

    var refreshInterval =
        indexingLucene
            .flatMap(l -> l.refreshConfig())
            .flatMap(r -> r.intervalMs())
            .map(Duration::ofMillis);

    var concurrentScheduler =
        indexingLucene
            .flatMap(l -> l.mergeSchedulerConfig())
            .flatMap(m -> m.concurrentSchedulerConfig());

    var numMaxMergeThreads =
        concurrentScheduler
            .flatMap(c -> c.maxThreadCount())
            .or(() -> Optional.of(runtime.getNumCpus()));

    var configuredMergeBudgetMb =
        indexingLucene
            .flatMap(l -> l.mergePolicyConfig())
            .flatMap(m -> m.tieredMergePolicyConfig())
            .flatMap(t -> t.vectorMergePolicyConfig())
            .flatMap(v -> v.mergeBudgetMb());
    var vectorMergePolicyConfig = getVectorMergePolicy(runtime, configuredMergeBudgetMb);

    var fieldLimit = indexingLucene.flatMap(l -> l.fieldLimit());

    var queryingLucene =
        config
            .advancedConfigs()
            .flatMap(AdvancedConfigs::queryingConfig)
            .flatMap(CommunityQueryingConfig::luceneConfig);

    var maxClauseLimit = queryingLucene.flatMap(q -> q.maxClauseLimit());

    var floorSegmentMB =
        queryingLucene
            .flatMap(CommunityQueryingConfig.LuceneConfig::floorSegmentMB)
            .or(() -> Optional.of(DEFAULT_FLOOR_SEGMENT_MB));

    return LuceneConfig.create(
        config.storageConfig().dataPath(),
        refreshInterval,
        Optional.empty(),
        numMaxMergeThreads,
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        fieldLimit,
        Optional.of(DOCS_LIMIT),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        maxClauseLimit,
        ENABLE_CONCURRENT_SEARCH,
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.of(vectorMergePolicyConfig),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        floorSegmentMB,
        mergePolicyDiskUtilizationConfig,
        Optional.empty(),
        Optional.empty());
  }

  /**
   * Resolves the runtime {@link LuceneConfig.VectorMergePolicyConfig} from the JVM heap size and
   * optionally configured merge budget.
   *
   * @param runtime runtime used to determine the instance memory and JVM max heap size
   * @param configuredMergeBudgetMb operator-set {@code mergeBudgetMb}, if any
   */
  static LuceneConfig.VectorMergePolicyConfig getVectorMergePolicy(
      Runtime runtime, Optional<Integer> configuredMergeBudgetMb) {
    Bytes instanceMemorySize = Bytes.ofBytes(runtime.getTotalMemoryBytes());
    int mergeBudgetMb =
        configuredMergeBudgetMb.orElseGet(() -> (int) (0.10 * instanceMemorySize.toMebi()));
    int maxVectorInputMb = (int) Math.ceil(MAX_VECTOR_INPUT_TO_MERGE_BUDGET_RATIO * mergeBudgetMb);
    int maxCompoundDataMb =
        (int)
            Math.ceil(
                MAX_COMPOUND_DATA_TO_MAX_VECTOR_INPUT_RATIO
                    * MAX_VECTOR_INPUT_TO_MERGE_BUDGET_RATIO
                    * mergeBudgetMb);

    long heapMb = runtime.getMaxHeapSize().toMebi();
    int segmentHeapBudgetMb = (int) Math.ceil(0.2 * heapMb);
    int globalHeapBudgetMb = (int) Math.ceil(0.8 * heapMb);
    return new LuceneConfig.VectorMergePolicyConfig(
        maxCompoundDataMb,
        maxVectorInputMb,
        mergeBudgetMb,
        segmentHeapBudgetMb,
        globalHeapBudgetMb);
  }
}
