package com.xgen.mongot.index.lucene.config;

import static com.xgen.mongot.util.Check.checkArg;

import com.google.common.annotations.VisibleForTesting;
import com.xgen.mongot.config.util.HysteresisConfig;
import com.xgen.mongot.util.Bytes;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.Runtime;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Optional;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriter.DocStats;
import org.apache.lucene.util.Version;
import org.bson.BsonDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * LuceneConfig
 *
 * @param dataPath The path under which we should store data (such as indexes).
 * @param refreshInterval How long we should wait before refreshing the index to make updates
 *     available for querying.
 * @param refreshExecutorThreads The number of threads to run in the Executor in charge of
 *     refreshing indexes.
 * @param numMaxMergeThreads The maximum number of MergeThreads that are allowed to be running at
 *     any given time.
 * @param ramBufferSizeMb The amount of RAM that may be used for buffering added documents and
 *     deletions before they are flushed to the Directory.
 * @param nrtCacheEnabled Explicitly enables the NRT cache, which is needed to reduce the amount of
 *     small segments written to disk by keeping them in RAM. This results in more efficient I/O
 *     utilization on workloads with frequent IndexReader reopenings but low indexing rate.
 * @param nrtTotalCacheSizeMb The total size of the NRT cache.
 * @param nrtMaxMergeSizeMb The maximum expected size of the newly merged segment which should be
 *     cached (unless the total cache size would exceed {@link #nrtTotalCacheSizeMb()}).
 * @param numMaxMerges When a thread doing some operation that requires a merge (indexing documents,
 *     refreshing a near real time reader, committing, etc) invokes the ConcurrentMergeScheduler,
 *     this setting defines the number of outstanding MergeThreads that can be either running or
 *     pending. If there are > numMaxMerges MergeThreads outstanding, the thread needing to merge
 *     will block. For example, if numMaxMergeThreads is 8 and numMaxMerges is 10, then 10 different
 *     calls to the MergeScheduler will not block but instead create a MergeThread. Of these 10
 *     MergeThreads, 8 may be running at a time. If 10 MergeThreads are still outstanding, an 11th
 *     call to the MergeScheduler will block until there is capacity to create a new MergeThread.
 * @param maxMergedSegmentSize The maximum number of bytes that a segment should be allowed to be.
 *     The MergePolicy will not suggest merges that would result in a segment larger than this size.
 * @param fieldLimit A hard limit on the number of fields to be indexed (as counted by Lucene's
 *     IndexWriter), used by LuceneIndexWriter::exceededLimits(). This takes into account metadata
 *     fields, different data types for the same BSON field, and possibly deleted fields.
 * @param docsLimit A soft limit of the number of documents to be indexed (as counted by Lucene's
 *     IndexWriter), used by com.xgen.mongot.index.lucene.LuceneIndexWriter::throwIfTooManyDocs.
 *     This is an internal limit. It should be less than Lucene hard limit that is equal to ~2B. It
 *     should include pending and deleted documents. See {@link DocStats#maxDoc}
 * @param maxSynonymMappingsPerIndex Maximum number of synonym mappings allowed on an index.
 *     Intended to be set on shared tier clusters.
 * @param maxDocumentsPerSynonymCollection Maximum number of documents allowed in a single synonyms
 *     collection. Intended to be set on shared tier clusters.
 * @param disableMaxClauseLimit Deprecated. Use {@link #maxClauseLimit} instead. When true, disables
 *     the clause limit entirely (Integer.MAX_VALUE). Takes precedence over maxClauseLimit.
 * @param deletesPctAllowed The maximum percentage of deleted documents allowed in the index before
 *     triggering merges to reclaim space.
 * @param forceMergeDeletesPctAllowed Percentage threshold of deleted documents that triggers
 *     segment merging during forceMergeDeletes operations. Only segments with a deletion percentage
 *     above this threshold will be considered for merging to reclaim space from deleted documents.
 * @param floorSegmentMB Segments smaller than this size are merged more aggressively. This helps
 *     prevent frequent flushing of tiny segments to create a long tail of small segments in the
 *     index.
 */
public record LuceneConfig(
    Path dataPath,
    Duration refreshInterval,
    int refreshExecutorThreads,
    int numMaxMergeThreads,
    int numMaxMerges,
    double ramBufferSizeMb,
    boolean nrtCacheEnabled,
    double nrtTotalCacheSizeMb,
    double nrtMaxMergeSizeMb,
    Bytes maxMergedSegmentSize,
    Optional<Integer> fieldLimit,
    Optional<Integer> docsLimit,
    Optional<Integer> maxSynonymMappingsPerIndex,
    Optional<Integer> maxDocumentsPerSynonymCollection,
    @Deprecated boolean disableMaxClauseLimit,
    Optional<Integer> maxClauseLimit,
    boolean enableConcurrentSearch,
    int concurrentSearchExecutorThreads,
    int concurrentSearchExecutorQueueSize,
    int concurrentVectorRescoringExecutorThreads,
    int concurrentVectorRescoringExecutorQueueSize,
    Optional<VectorMergePolicyConfig> vectorMergePolicyConfig,
    boolean enableTextOperatorNewSynonymsSyntax,
    Optional<Integer> tokenFacetingCardinalityLimit,
    Optional<Double> deletesPctAllowed,
    Optional<Double> forceMergeDeletesPctAllowed,
    Optional<Double> floorSegmentMB,
    Optional<HysteresisConfig> mergePolicyDiskUtilizationConfig,
    Optional<Long> cancelMergePerThreadTimeoutMs,
    Optional<Long> cancelAllMergesPerThreadTimeoutMs)
    implements DocumentEncodable {
  private static class Fields {
    private static final Field.Required<Integer> REFRESH_INTERVAL =
        Field.builder("refreshInterval").intField().required();

    private static final Field.Required<Integer> NUM_MAX_MERGE_THREADS =
        Field.builder("numMaxMergeThreads").intField().mustBePositive().required();

    private static final Field.Required<Integer> NUM_MAX_MERGES =
        Field.builder("numMaxMerges").intField().mustBePositive().required();

    private static final Field.Required<Long> MAX_MERGED_SEGMENT_SIZE =
        Field.builder("maxMergedSegmentSize").longField().mustBePositive().required();

    private static final Field.Required<String> DATA_PATH =
        Field.builder("dataPath").stringField().required();

    private static final Field.Required<Integer> REFRESH_EXEC_THREADS =
        Field.builder("refreshExecutorThreads").intField().required();

    private static final Field.Optional<Integer> FIELD_LIMIT =
        Field.builder("fieldLimit").intField().optional().noDefault();

    private static final Field.Optional<Integer> DOCS_LIMIT =
        Field.builder("docsLimit").intField().optional().noDefault();

    private static final Field.Required<Boolean> DISABLE_MAX_CLAUSE_LIMIT =
        Field.builder("disableMaxClauseLimit").booleanField().required();

    private static final Field.Optional<Integer> MAX_CLAUSE_LIMIT =
        Field.builder("maxClauseLimit").intField().optional().noDefault();

    private static final Field.Required<Boolean> ENABLE_CONCURRENT_SEARCH =
        Field.builder("enableConcurrentSearch").booleanField().required();

    private static final Field.Required<Integer> CONCURRENT_SEARCH_EXECUTOR_THREADS =
        Field.builder("concurrentSearchExecutorThreads").intField().required();

    private static final Field.Required<Integer> CONCURRENT_SEARCH_EXECUTOR_QUEUE_SIZE =
        Field.builder("concurrentSearchExecutorQueueSize").intField().required();

    private static final Field.Required<Integer> CONCURRENT_VECTOR_RESCORING_EXECUTOR_THREADS =
        Field.builder("concurrentVectorRescoringExecutorThreads").intField().required();

    private static final Field.Required<Integer> CONCURRENT_VECTOR_RESCORING_EXECUTOR_QUEUE_SIZE =
        Field.builder("concurrentVectorRescoringExecutorQueueSize").intField().required();

    private static final Field.Optional<VectorMergePolicyConfig> VECTOR_MERGE_POLICY_CONFIG =
        Field.builder("vectorMergePolicyConfig")
            .classField(VectorMergePolicyConfig::fromBson)
            .disallowUnknownFields()
            .optional()
            .noDefault();

    private static final Field.Required<Boolean> ENABLE_TEXT_OPERATOR_NEW_SYNONYMS_SYNTAX =
        Field.builder("enableTextOperatorNewSynonymsSyntax").booleanField().required();

    private static final Field.Optional<Double> DELETES_PCT_ALLOWED =
        Field.builder("deletesPctAllowed").doubleField().optional().noDefault();

    private static final Field.Optional<Double> FORCE_MERGE_DELETES_PCT_ALLOWED =
        Field.builder("forceMergeDeletesPctAllowed").doubleField().optional().noDefault();

    private static final Field.Optional<Double> FLOOR_SEGMENT_MB =
        Field.builder("floorSegmentMB").doubleField().optional().noDefault();

    private static final Field.Optional<HysteresisConfig> MERGE_POLICY_DISK_UTILIZATION_CONFIG =
        Field.builder("mergePolicyDiskUtilizationConfig")
            .classField(HysteresisConfig::fromBson)
            .disallowUnknownFields()
            .optional()
            .noDefault();

    private static final Field.Optional<Long> CANCEL_MERGE_PER_THREAD_TIMEOUT_MS =
        Field.builder("cancelMergePerThreadTimeoutMs")
            .longField()
            .mustBePositive()
            .optional()
            .noDefault();

    private static final Field.Optional<Long> CANCEL_ALL_MERGES_PER_THREAD_TIMEOUT_MS =
        Field.builder("cancelAllMergesPerThreadTimeoutMs")
            .longField()
            .mustBePositive()
            .optional()
            .noDefault();
  }

  private static final Logger LOG = LoggerFactory.getLogger(LuceneConfig.class);

  private static final Duration DEFAULT_REFRESH_INTERVAL = Duration.ofSeconds(1);

  /**
   * The default factor used to calculate the size of the concurrent search queue based on the size
   * of the thread pool.
   */
  private static final float DEFAULT_CONCURRENT_SEARCH_QUEUE_SIZE_FACTOR = 1.5f;

  /** Max allowed term length in worst-case scenario when each character takes 4 bytes. */
  public static final int MAX_TERM_CHAR_LENGTH = IndexWriter.MAX_TERM_LENGTH / 4;

  public static final int MIN_FIELD_LIMIT = 3;

  /** Min value for docsLimit parameter */
  public static final int MIN_DOCS_LIMIT = 1;

  /** Max allowed number of documents per index */
  public static final int MAX_DOCS_LIMIT = 2_100_000_000;

  public LuceneConfig {
    LOG.info("using Lucene Version {}.", Version.LATEST);
  }

  /** Creates a new LuceneConfig, deriving defaults for any options that are not supplied. */
  public static LuceneConfig create(
      Path dataPath,
      Optional<Duration> optionalRefreshInterval,
      Optional<Integer> optionalRefreshExecutorThreads,
      Optional<Integer> optionalNumMaxMergeThreads,
      Optional<Integer> optionalNumMaxMerges,
      Optional<Double> optionalRamBufferSizeMb,
      Optional<Boolean> optionalNrtCacheEnabled,
      Optional<Double> optionalNrtTotalCacheSizeMb,
      Optional<Double> optionalNrtMaxMergeSizeMb,
      Optional<Bytes> optionalMaxMergedSegmentSize,
      Optional<Integer> fieldLimit,
      Optional<Integer> docsLimit,
      Optional<Integer> optionalMaxSynonymMappingsPerIndex,
      Optional<Integer> optionalMaxDocumentsPerSynonymCollection,
      Optional<Boolean> disableMaxClauseLimit,
      Optional<Integer> maxClauseLimit,
      Optional<Boolean> enableConcurrentSearch,
      Optional<Integer> concurrentSearchExecutorThreads,
      Optional<Integer> concurrentSearchExecutorQueueSize,
      Optional<Integer> concurrentVectorRescoringExecutorThreads,
      Optional<Integer> concurrentVectorRescoringExecutorQueueSize,
      Optional<VectorMergePolicyConfig> vectorMergePolicy,
      Optional<Boolean> optionalEnableTextOperatorNewSynonymsSyntax,
      Optional<Integer> tokenFacetingCardinalityLimit,
      Optional<Double> deletesPctAllowed,
      Optional<Double> forceMergeDeletesPctAllowed,
      Optional<Double> floorSegmentMB,
      Optional<HysteresisConfig> mergePolicyDiskUtilizationConfig,
      Optional<Long> cancelMergePerThreadTimeoutMs,
      Optional<Long> cancelAllMergesPerThreadTimeoutMs) {
    return create(
        Runtime.INSTANCE,
        dataPath,
        optionalRefreshInterval,
        optionalRefreshExecutorThreads,
        optionalNumMaxMergeThreads,
        optionalNumMaxMerges,
        optionalRamBufferSizeMb,
        optionalNrtCacheEnabled,
        optionalNrtTotalCacheSizeMb,
        optionalNrtMaxMergeSizeMb,
        optionalMaxMergedSegmentSize,
        fieldLimit,
        docsLimit,
        optionalMaxSynonymMappingsPerIndex,
        optionalMaxDocumentsPerSynonymCollection,
        disableMaxClauseLimit,
        maxClauseLimit,
        enableConcurrentSearch,
        concurrentSearchExecutorThreads,
        concurrentSearchExecutorQueueSize,
        concurrentVectorRescoringExecutorThreads,
        concurrentVectorRescoringExecutorQueueSize,
        vectorMergePolicy,
        optionalEnableTextOperatorNewSynonymsSyntax,
        tokenFacetingCardinalityLimit,
        deletesPctAllowed,
        forceMergeDeletesPctAllowed,
        floorSegmentMB,
        mergePolicyDiskUtilizationConfig,
        cancelMergePerThreadTimeoutMs,
        cancelAllMergesPerThreadTimeoutMs);
  }

  @VisibleForTesting
  static LuceneConfig create(
      Runtime runtime,
      Path dataPath,
      Optional<Duration> optionalRefreshInterval,
      Optional<Integer> optionalRefreshExecutorThreads,
      Optional<Integer> optionalNumMaxMergeThreads,
      Optional<Integer> optionalNumMaxMerges,
      Optional<Double> optionalRamBufferSizeMb,
      Optional<Boolean> optionalNrtCacheEnabled,
      Optional<Double> optionalNrtTotalCacheSizeMb,
      Optional<Double> optionalNrtMaxMergeSizeMb,
      Optional<Bytes> optionalMaxMergedSegmentSize,
      Optional<Integer> fieldLimit,
      Optional<Integer> docsLimit,
      Optional<Integer> optionalMaxSynonymMappingsPerIndex,
      Optional<Integer> optionalMaxDocumentsPerSynonymCollection,
      Optional<Boolean> optionalDisableMaxClauseLimit,
      Optional<Integer> maxClauseLimit,
      Optional<Boolean> optionalEnableConcurrentSearch,
      Optional<Integer> optionalConcurrentSearchExecutorThreads,
      Optional<Integer> optionalConcurrentSearchExecutorQueueSize,
      Optional<Integer> optionalConcurrentVectorRescoringExecutorThreads,
      Optional<Integer> optionalConcurrentVectorRescoringExecutorQueueSize,
      Optional<VectorMergePolicyConfig> vectorMergePolicy,
      Optional<Boolean> optionalEnableTextOperatorNewSynonymsSyntax,
      Optional<Integer> tokenFacetingCardinalityLimit,
      Optional<Double> deletesPctAllowed,
      Optional<Double> forceMergeDeletesPctAllowed,
      Optional<Double> floorSegmentMB,
      Optional<HysteresisConfig> mergePolicyDiskUtilizationConfig,
      Optional<Long> cancelMergePerThreadTimeoutMs,
      Optional<Long> cancelAllMergesPerThreadTimeoutMs) {

    Duration refreshInterval = optionalRefreshInterval.orElse(DEFAULT_REFRESH_INTERVAL);
    checkArg(
        !refreshInterval.isNegative() && !refreshInterval.isZero(),
        "refresh interval must be positive (is %s)",
        refreshInterval);

    int refreshExecutorThreads = getRefreshExecutorThreads(runtime, optionalRefreshExecutorThreads);
    Check.argIsPositive(refreshExecutorThreads, "refreshExecutorThreads");

    int numMaxMergeThreads =
        getNumMaxMergeThreads(runtime, optionalNumMaxMergeThreads, optionalNumMaxMerges);
    Check.argIsPositive(numMaxMergeThreads, "numMaxMergeThreads");

    int numMaxMerge = getNumMaxMerges(runtime, optionalNumMaxMerges, numMaxMergeThreads);
    Check.argIsPositive(numMaxMerge, "numMaxMerge");

    double ramBufferSizeMb = getRamBufferSizeMb(runtime, optionalRamBufferSizeMb);
    Check.argIsPositive(ramBufferSizeMb, "ramBufferSizeMb");

    boolean nrtCacheEnabled = getNrtCacheEnabled(runtime, optionalNrtCacheEnabled);

    double nrtTotalCacheSizeMb = getNrtTotalCacheSizeMb(runtime, optionalNrtTotalCacheSizeMb);
    Check.argNotNegative(nrtTotalCacheSizeMb, "nrtTotalCacheSizeMb");

    double nrtMaxMergeSizeMb = getNrtMaxMergeSizeMb(nrtTotalCacheSizeMb, optionalNrtMaxMergeSizeMb);
    Check.argNotNegative(nrtMaxMergeSizeMb, "nrtMaxMergeSizeMb");

    checkArg(
        numMaxMerge >= numMaxMergeThreads,
        "numMaxMerge (%s) must be >= numMaxMergeThreads (%s)",
        numMaxMerge,
        numMaxMergeThreads);

    Bytes maxMergedSegmentSize = getMaxMergedSegmentSize(runtime, optionalMaxMergedSegmentSize);

    validateFieldLimit(fieldLimit);

    validateDocsLimit(docsLimit);

    validateDeletesPctAllowed(deletesPctAllowed);

    validateForceMergeDeletesPctAllowed(forceMergeDeletesPctAllowed);

    validateFloorSegmentMB(floorSegmentMB);

    validateMaxClauseLimit(maxClauseLimit);

    boolean disableMaxClauseLimit = optionalDisableMaxClauseLimit.orElse(false);
    boolean enableConcurrentSearch = optionalEnableConcurrentSearch.orElse(false);
    int concurrentSearchExecutorThreads =
        getConcurrentSearchExecutorThreads(runtime, optionalConcurrentSearchExecutorThreads);
    int concurrentSearchExecutorQueueSize =
        optionalConcurrentSearchExecutorQueueSize.orElse(
            (int) (concurrentSearchExecutorThreads * DEFAULT_CONCURRENT_SEARCH_QUEUE_SIZE_FACTOR));
    int concurrentVectorRescoringExecutorThreads =
        getConcurrentVectorRescoringExecutorThreads(
            runtime, optionalConcurrentVectorRescoringExecutorThreads);
    int concurrentVectorRescoringExecutorQueueSize =
        optionalConcurrentVectorRescoringExecutorQueueSize.orElse(
            (int)
                (concurrentVectorRescoringExecutorThreads
                    * DEFAULT_CONCURRENT_SEARCH_QUEUE_SIZE_FACTOR));
    boolean enableTextOperatorNewSynonymsSyntax =
        optionalEnableTextOperatorNewSynonymsSyntax.orElse(false);

    return new LuceneConfig(
        dataPath,
        refreshInterval,
        refreshExecutorThreads,
        numMaxMergeThreads,
        numMaxMerge,
        ramBufferSizeMb,
        nrtCacheEnabled,
        nrtTotalCacheSizeMb,
        nrtMaxMergeSizeMb,
        maxMergedSegmentSize,
        fieldLimit,
        docsLimit,
        optionalMaxSynonymMappingsPerIndex,
        optionalMaxDocumentsPerSynonymCollection,
        disableMaxClauseLimit,
        maxClauseLimit,
        enableConcurrentSearch,
        concurrentSearchExecutorThreads,
        concurrentSearchExecutorQueueSize,
        concurrentVectorRescoringExecutorThreads,
        concurrentVectorRescoringExecutorQueueSize,
        vectorMergePolicy,
        enableTextOperatorNewSynonymsSyntax,
        tokenFacetingCardinalityLimit,
        deletesPctAllowed,
        forceMergeDeletesPctAllowed,
        floorSegmentMB,
        mergePolicyDiskUtilizationConfig,
        cancelMergePerThreadTimeoutMs,
        cancelAllMergesPerThreadTimeoutMs);
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder()
        .field(Fields.REFRESH_INTERVAL, Math.toIntExact(this.refreshInterval.toMillis()))
        .field(Fields.NUM_MAX_MERGE_THREADS, this.numMaxMergeThreads)
        .field(Fields.NUM_MAX_MERGES, this.numMaxMerges)
        .field(Fields.MAX_MERGED_SEGMENT_SIZE, this.maxMergedSegmentSize.toBytes())
        .field(Fields.DATA_PATH, this.dataPath.toString())
        .field(Fields.REFRESH_EXEC_THREADS, this.refreshExecutorThreads)
        .field(Fields.FIELD_LIMIT, this.fieldLimit)
        .field(Fields.DOCS_LIMIT, this.docsLimit)
        .field(Fields.DISABLE_MAX_CLAUSE_LIMIT, this.disableMaxClauseLimit)
        .field(Fields.MAX_CLAUSE_LIMIT, this.maxClauseLimit)
        .field(Fields.ENABLE_CONCURRENT_SEARCH, this.enableConcurrentSearch)
        .field(Fields.CONCURRENT_SEARCH_EXECUTOR_THREADS, this.concurrentSearchExecutorThreads)
        .field(Fields.CONCURRENT_SEARCH_EXECUTOR_QUEUE_SIZE, this.concurrentSearchExecutorQueueSize)
        .field(
            Fields.CONCURRENT_VECTOR_RESCORING_EXECUTOR_THREADS,
            this.concurrentVectorRescoringExecutorThreads)
        .field(
            Fields.CONCURRENT_VECTOR_RESCORING_EXECUTOR_QUEUE_SIZE,
            this.concurrentVectorRescoringExecutorQueueSize)
        .field(Fields.VECTOR_MERGE_POLICY_CONFIG, this.vectorMergePolicyConfig)
        .field(
            Fields.ENABLE_TEXT_OPERATOR_NEW_SYNONYMS_SYNTAX,
            this.enableTextOperatorNewSynonymsSyntax)
        .field(Fields.DELETES_PCT_ALLOWED, this.deletesPctAllowed)
        .field(Fields.FORCE_MERGE_DELETES_PCT_ALLOWED, this.forceMergeDeletesPctAllowed)
        .field(Fields.FLOOR_SEGMENT_MB, this.floorSegmentMB)
        .field(Fields.MERGE_POLICY_DISK_UTILIZATION_CONFIG, this.mergePolicyDiskUtilizationConfig)
        .field(Fields.CANCEL_MERGE_PER_THREAD_TIMEOUT_MS, this.cancelMergePerThreadTimeoutMs)
        .field(
            Fields.CANCEL_ALL_MERGES_PER_THREAD_TIMEOUT_MS, this.cancelAllMergesPerThreadTimeoutMs)
        .build();
  }

  private static int getRefreshExecutorThreads(
      Runtime runtime, Optional<Integer> optionalRefreshExecutorThreads) {
    return optionalRefreshExecutorThreads.orElseGet(
        // Limit the default to a maximum of 10 threads.
        () -> {
          int refreshExecutorThreads =
              Math.max(1, Math.min(10, Math.floorDiv(runtime.getNumCpus(), 2)));
          LOG.atInfo()
              .addKeyValue("defaultRefreshExecutorThreads", refreshExecutorThreads)
              .log("refreshExecutorThreads not configured, setting to default value.");
          return refreshExecutorThreads;
        });
  }

  private static int getNumMaxMergeThreads(
      Runtime runtime,
      Optional<Integer> optionalNumMaxMergeThreads,
      Optional<Integer> optionalNumMaxMerges) {
    return optionalNumMaxMergeThreads.orElseGet(
        () -> {
          // By default ConcurrentMergeScheduler uses max(1, min(4, numCpus/2)). Since we only have
          // one shared ConcurrentMergeScheduler for all of the indexes, we lift the maximum of 4 so
          // that we can scale better to higher core machines.
          int maxGivenCpus = Math.max(1, Math.floorDiv(runtime.getNumCpus(), 2));

          // If we've set numMaxMerges, we can't have more threads than numMaxMerges, so default to
          // the smaller of the two.
          int numMaxMergeThreads =
              optionalNumMaxMerges
                  .map(numMaxMerges -> Math.min(numMaxMerges, maxGivenCpus))
                  .orElse(maxGivenCpus);

          LOG.atInfo()
              .addKeyValue("defaultNumMaxMergeThreads", numMaxMergeThreads)
              .log("numMaxMergeThreads not configured, setting to default");
          return numMaxMergeThreads;
        });
  }

  private static int getNumMaxMerges(
      Runtime runtime, Optional<Integer> optionalNumMaxMerges, int numMaxMergeThreads) {
    return optionalNumMaxMerges.orElseGet(
        () -> {
          // Allow a merge in memory for every 512mb of heap we have.
          int maxGivenMemory =
              (int) (runtime.getMaxHeapSize().toBytes() / Bytes.ofMebi(512).toBytes());

          // If there is a lot more memory than mergeThreads, we could end up piling up way too many
          // merges, so only allow up to maxMergeThreads + 5 merges.
          int maxGivenThreads = numMaxMergeThreads + 5;

          int max = Math.min(maxGivenMemory, maxGivenThreads);

          // We must have at least as many numMaxMerges as numMaxThreads.
          int numMaxMerges = Math.max(max, numMaxMergeThreads);

          LOG.atInfo()
              .addKeyValue("defaultNumMaxMerges", numMaxMerges)
              .log("defaultNumMaxMerges not configured, setting to default");
          return numMaxMerges;
        });
  }

  private static int getConcurrentSearchExecutorThreads(
      Runtime runtime, Optional<Integer> optionalConcurrentSearchExecutorThreads) {
    return optionalConcurrentSearchExecutorThreads.orElseGet(
        () -> {
          LOG.atInfo()
              .addKeyValue("defaultConcurrentSearchExecutorThreads", runtime.getNumCpus())
              .log("concurrentSearchExecutorThreads not configured, setting to default.");
          return runtime.getNumCpus();
        });
  }

  private static int getConcurrentVectorRescoringExecutorThreads(
      Runtime runtime, Optional<Integer> optionalConcurrentVectorRescoringExecutorThreads) {
    return optionalConcurrentVectorRescoringExecutorThreads.orElseGet(
        () -> {
          LOG.atInfo()
              .addKeyValue("defaultConcurrentVectorRescoringExecutorThreads", runtime.getNumCpus())
              .log("concurrentVectorRescoringExecutorThreads not configured, setting to default.");
          return runtime.getNumCpus();
        });
  }

  private static double getRamBufferSizeMb(
      Runtime runtime, Optional<Double> optionalRamBufferSizeMb) {
    return optionalRamBufferSizeMb.orElseGet(
        () -> {
          // this exponential function provides the desired buffer size based on the
          // available heap size for all Atlas tiers
          double ramBufferSizeMb = Math.floor(Math.pow(runtime.getMaxHeapSize().toMebi(), 0.49));
          LOG.atInfo()
              .addKeyValue("defaultRamBufferSizeMb", ramBufferSizeMb)
              .log("ramBufferSizeMb not configured, setting to default.");
          return ramBufferSizeMb;
        });
  }

  private static boolean getNrtCacheEnabled(
      Runtime runtime, Optional<Boolean> optionalNrtCacheEnabled) {
    return optionalNrtCacheEnabled.orElseGet(
        () -> {
          boolean nrtCacheEnabled = runtime.getMaxHeapSize().toGibi() >= 4;
          LOG.atInfo()
              .addKeyValue("defaultNrtCacheEnabled", nrtCacheEnabled)
              .log("nrtCacheEnabled not configured, setting to default.");
          return nrtCacheEnabled;
        });
  }

  private static double getNrtTotalCacheSizeMb(
      Runtime runtime, Optional<Double> optionalNrtTotalCacheSizeMb) {
    return optionalNrtTotalCacheSizeMb.orElseGet(
        () -> {
          double nrtTotalCacheSizeMb =
              Math.floor(Math.pow(runtime.getMaxHeapSize().toMebi(), 0.45));
          LOG.atInfo()
              .addKeyValue("defaultNrtTotalCacheSizeMb", nrtTotalCacheSizeMb)
              .log("nrtTotalCacheSizeMb not configured, setting to default.");
          return nrtTotalCacheSizeMb;
        });
  }

  private static double getNrtMaxMergeSizeMb(
      double nrtTotalCacheSizeMb, Optional<Double> optionalNrtMaxMergeSizeMb) {
    return optionalNrtMaxMergeSizeMb.orElseGet(
        () -> {
          double nrtMaxMergeSizeMb = Math.min(16, nrtTotalCacheSizeMb / 10);
          LOG.atInfo()
              .addKeyValue("defaultNrtMaxMergeSizeMb", nrtMaxMergeSizeMb)
              .log("nrtMaxMergeSizeMb not configured, setting to default.");
          return nrtMaxMergeSizeMb;
        });
  }

  private static Bytes getMaxMergedSegmentSize(
      Runtime runtime, Optional<Bytes> optionalMaxMergedSegmentSize) {
    return optionalMaxMergedSegmentSize.orElseGet(
        () -> {
          // In testing, mongots on different sized boxes all seemed to be able to handle merging a
          // segment with a target size of the configured heap. mongot can probably handle larger
          // segments as well, but to account for multiple concurrent merges, we'll use this
          // possibly conservative number for now.
          // Lucene's default is 5 GiB, so for now don't configure the maxMergedSegmentSize to be
          // larger than that.
          Bytes max = ObjectUtils.min(runtime.getMaxHeapSize(), Bytes.ofGibi(5));
          LOG.atInfo()
              .addKeyValue("defaultMaxMergedSegmentSize", max)
              .log("maxMergedSegmentSize not configured, setting to default.");
          return max;
        });
  }

  private static void validateFieldLimit(Optional<Integer> fieldLimit) {
    fieldLimit.ifPresent(
        limit -> {
          checkArg(
              limit >= MIN_FIELD_LIMIT, "fieldLimit (%s) must be >= %s", limit, MIN_FIELD_LIMIT);
          LOG.atInfo().addKeyValue("fieldLimit", limit).log("fieldLimit set");
        });
  }

  private static void validateDocsLimit(Optional<Integer> docsLimit) {
    docsLimit.ifPresent(
        limit -> {
          checkArg(limit <= MAX_DOCS_LIMIT, "docsLimit (%s) must be <= %s", limit, MAX_DOCS_LIMIT);
          Check.argIsPositive(limit, "docsLimit");
          LOG.atInfo().addKeyValue("docsLimit", limit).log("docsLimit set");
        });
  }

  private static void validateDeletesPctAllowed(Optional<Double> deletesPctAllowed) {
    deletesPctAllowed.ifPresent(
        pct -> {
          checkArg(
              pct >= 5.0 && pct <= 50.0,
              "deletesPctAllowed (%s) must be between 5.0 and 50.0",
              pct);
          LOG.atInfo().addKeyValue("deletesPctAllowed", pct).log("deletesPctAllowed set");
        });
  }

  private static void validateForceMergeDeletesPctAllowed(
      Optional<Double> forceMergeDeletesPctAllowed) {
    forceMergeDeletesPctAllowed.ifPresent(
        pct -> {
          checkArg(
              pct >= 0.0 && pct <= 100.0,
              "forceMergeDeletesPctAllowed (%s) must be between 0.0 and 100.0",
              pct);
          LOG.atInfo()
              .addKeyValue("forceMergeDeletesPctAllowed", pct)
              .log("forceMergeDeletesPctAllowed set");
        });
  }

  private static void validateFloorSegmentMB(Optional<Double> floorSegmentMB) {
    floorSegmentMB.ifPresent(
        value -> {
          Check.argIsPositive(value, "floorSegmentMB");
          LOG.atInfo().addKeyValue("floorSegmentMB", value).log("floorSegmentMB set");
        });
  }

  private static void validateMaxClauseLimit(Optional<Integer> maxClauseLimit) {
    maxClauseLimit.ifPresent(
        limit -> {
          Check.argIsPositive(limit, "maxClauseLimit");
          LOG.atInfo().addKeyValue("maxClauseLimit", limit).log("maxClauseLimit set");
        });
  }

  public record VectorMergePolicyConfig(
      int maxCompoundDataMb,
      int maxVectorInputMb,
      int mergeBudgetMb,
      int segmentHeapBudgetMb,
      int globalHeapBudgetMb)
      implements DocumentEncodable {
    static class Fields {
      public static final Field.Required<Integer> MAX_COMPOUND_DATA_MB =
          Field.builder("maxCompoundDataMb").intField().mustBePositive().required();
      public static final Field.Required<Integer> MAX_VECTOR_INPUT_MB =
          Field.builder("maxVectorInputMb").intField().mustBePositive().required();
      public static final Field.Required<Integer> MERGE_BUDGET_MB =
          Field.builder("mergeBudgetMb").intField().mustBePositive().required();
      public static final Field.WithDefault<Integer> SEGMENT_HEAP_BUDGET_MB =
          Field.builder("segmentHeapBudgetMb")
              .intField()
              .mustBePositive()
              .optional()
              .withDefault((int) Math.ceil(Runtime.INSTANCE.getMaxHeapSize().toMebi() * 0.2));
      public static final Field.WithDefault<Integer> GLOBAL_HEAP_BUDGET_MB =
          Field.builder("globalHeapBudgetMb")
              .intField()
              .mustBePositive()
              .optional()
              .withDefault((int) Math.ceil(Runtime.INSTANCE.getMaxHeapSize().toMebi() * 0.8));
    }

    public static VectorMergePolicyConfig fromBson(DocumentParser parser)
        throws BsonParseException {
      return new VectorMergePolicyConfig(
          parser.getField(Fields.MAX_COMPOUND_DATA_MB).unwrap(),
          parser.getField(Fields.MAX_VECTOR_INPUT_MB).unwrap(),
          parser.getField(Fields.MERGE_BUDGET_MB).unwrap(),
          parser.getField(Fields.SEGMENT_HEAP_BUDGET_MB).unwrap(),
          parser.getField(Fields.GLOBAL_HEAP_BUDGET_MB).unwrap());
    }

    @Override
    public BsonDocument toBson() {
      return BsonDocumentBuilder.builder()
          .field(Fields.MAX_COMPOUND_DATA_MB, this.maxCompoundDataMb)
          .field(Fields.MAX_VECTOR_INPUT_MB, this.maxVectorInputMb)
          .field(Fields.MERGE_BUDGET_MB, this.mergeBudgetMb)
          .field(Fields.SEGMENT_HEAP_BUDGET_MB, this.segmentHeapBudgetMb)
          .field(Fields.GLOBAL_HEAP_BUDGET_MB, this.globalHeapBudgetMb)
          .build();
    }
  }
}
