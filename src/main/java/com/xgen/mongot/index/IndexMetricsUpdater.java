package com.xgen.mongot.index;

import static com.xgen.mongot.index.IndexTypeData.INDEX_TYPE_TAG_NAME;
import static com.xgen.mongot.index.IndexTypeData.getIndexTypeTag;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.flogger.FluentLogger;
import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.definition.SearchIndexCapabilities;
import com.xgen.mongot.index.definition.VectorIndexCapabilities;
import com.xgen.mongot.index.query.CollectorQuery;
import com.xgen.mongot.index.query.InvalidQueryException;
import com.xgen.mongot.index.query.Query;
import com.xgen.mongot.index.query.collectors.Collector;
import com.xgen.mongot.index.query.collectors.DrillSidewaysInfoBuilder.DrillSidewaysInfo.QueryOptimizationStatus;
import com.xgen.mongot.index.query.collectors.FacetCollector;
import com.xgen.mongot.index.query.operators.Operator;
import com.xgen.mongot.index.query.operators.TextOperator;
import com.xgen.mongot.index.query.operators.VectorSearchCriteria;
import com.xgen.mongot.index.query.operators.mql.MqlFilterOperator;
import com.xgen.mongot.index.query.scores.Score;
import com.xgen.mongot.index.query.sort.NullEmptySortPosition;
import com.xgen.mongot.metrics.PerIndexMetricsFactory;
import com.xgen.mongot.metrics.ServerStatusDataExtractor;
import com.xgen.mongot.util.Bytes;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import java.io.Closeable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;
import org.apache.lucene.search.TotalHits;
import org.jetbrains.annotations.TestOnly;

/**
 * An IndexMetricsUpdater registers index-related metrics with a {@link PerIndexMetricsFactory} and
 * provides objects, mainly Micrometer instruments, that can be used by consuming code to update
 * their values.
 */
public class IndexMetricsUpdater implements Closeable {
  public static final String NAMESPACE = "index.stats";
  private static final double[] NUM_CANDIDATES_BUCKETS = {100, 500, 1000, 2000, 5000, 10_000};
  private static final double[] LIMIT_BUCKETS = {10, 50, 100, 200, 500, 1000};
  private static final double[] VISITED_NODES_BUCKETS = {
    100, 500, 1000, 2000, 5000, 10_000, 50_000
  };

  /**
   * Upper bounds for {@code totalFacetBucketsPerQuery} (string facet bucket demand per query).
   *
   * <p>Kept coarse (vs finer histograms) to limit Prometheus series: tail-focused bands around the
   * 10k bucket-limit context; use {@code sum}/{@code count} on export for mean demand.
   */
  private static final double[] TOTAL_FACET_BUCKETS_BUCKETS = {1000, 10_000, 50_000};

  private final IndexDefinition indexDefinition;
  private final PerIndexMetricsFactory metricsFactory;
  private final IndexingMetricsUpdater indexingMetricsUpdater;
  private final QueryingMetricsUpdater queryingMetricsUpdater;
  private final ReplicationMetricsUpdater replicationMetricsUpdater;
  private final IndexMetricValuesSupplier indexMetricValuesSupplier;

  private static final String INDEX_FEATURE_VERSION_NAME = "indexFeatureVersion";

  /** Reports the actual method KNN search selected at query time. */
  public enum KnnSearchMode {
    APPROXIMATE,
    EXACT,
    FALLBACK_TO_EXACT,
    FULL_SCAN,
  }

  /**
   * IndexMetricsUpdater takes ownership over queryingMetricsUpdater, indexMetricValuesSupplier,
   * replicationMetricsUpdater and metricsFactory.
   */
  private IndexMetricsUpdater(
      IndexDefinition indexDefinition,
      IndexingMetricsUpdater indexingMetricsUpdater,
      QueryingMetricsUpdater queryingMetricsUpdater,
      ReplicationMetricsUpdater replicationMetricsUpdater,
      IndexMetricValuesSupplier indexMetricValuesSupplier,
      PerIndexMetricsFactory metricsFactory) {
    this.indexDefinition = indexDefinition;
    this.metricsFactory = metricsFactory;

    this.indexingMetricsUpdater = indexingMetricsUpdater;
    this.queryingMetricsUpdater = queryingMetricsUpdater;
    this.replicationMetricsUpdater = replicationMetricsUpdater;
    this.indexMetricValuesSupplier = indexMetricValuesSupplier;

    this.metricsFactory
        .perIndexNumGauge(INDEX_FEATURE_VERSION_NAME)
        .set(this.indexDefinition.getParsedIndexFeatureVersion());
  }

  @VisibleForTesting
  public IndexMetricsUpdater(
      IndexDefinition indexDefinition,
      IndexMetricValuesSupplier indexMetricValuesSupplier,
      PerIndexMetricsFactory metricsFactory) {
    this(
        indexDefinition,
        new IndexingMetricsUpdater(
            metricsFactory.childMetricsFactory(IndexingMetricsUpdater.NAMESPACE),
            indexDefinition.getType(),
            getIndexTypeTag(indexDefinition),
            indexDefinition.getParsedIndexFeatureVersion(),
            true),
        new QueryingMetricsUpdater(
            metricsFactory.childMetricsFactory(QueryingMetricsUpdater.NAMESPACE),
            getIndexTypeTag(indexDefinition),
            indexDefinition.getParsedIndexFeatureVersion(),
            true),
        new ReplicationMetricsUpdater(
            metricsFactory.childMetricsFactory(ReplicationMetricsUpdater.NAMESPACE),
            indexDefinition),
        indexMetricValuesSupplier,
        metricsFactory);
  }

  public IndexDefinition getIndexDefinition() {
    return this.indexDefinition;
  }

  public PerIndexMetricsFactory getMetricsFactory() {
    return this.metricsFactory;
  }

  public IndexingMetricsUpdater getIndexingMetricsUpdater() {
    return this.indexingMetricsUpdater;
  }

  public QueryingMetricsUpdater getQueryingMetricsUpdater() {
    return this.queryingMetricsUpdater;
  }

  public ReplicationMetricsUpdater getReplicationMetricsUpdater() {
    return this.replicationMetricsUpdater;
  }

  @TestOnly
  public IndexMetricValuesSupplier getIndexMetricValuesSupplier() {
    return this.indexMetricValuesSupplier;
  }

  /**
   * Returns the cached index size in bytes.
   *
   * <p>This method delegates to {@link IndexMetricValuesSupplier#getCachedIndexSize()}, which
   * returns a pre-computed value updated during async metrics collection. This is safe to call on
   * hot paths like query execution, as it never triggers expensive operations like directory walks.
   *
   * @return the cached index size in bytes, or 0 if not yet computed
   */
  public long getIndexSize() {
    return this.indexMetricValuesSupplier.getCachedIndexSize();
  }

  /**
   * Gets a snapshot of the current value of all the index metrics tracked by this {@code
   * IndexMetricsUpdater}.
   *
   * <p>Some metrics may be expensive to snapshot, and care should be taken to not call this method
   * more than is necessary (for example, multiple times in a loop for the same index).
   *
   * @return a record of metrics and values for this index
   */
  public IndexMetrics getMetrics() {
    IndexDefinition indexDefinition = getIndexDefinition();
    return IndexMetrics.create(
        indexDefinition,
        this.indexMetricValuesSupplier.getIndexStatus(),
        getIndexingMetricsUpdater()
            .getMetrics(this.indexMetricValuesSupplier,
                computeNumNestedVectorFields(indexDefinition)),
        getQueryingMetricsUpdater().getMetrics());
  }

  private int computeNumNestedVectorFields(IndexDefinition indexDefinition) {
    if (indexDefinition.getType() != IndexDefinition.Type.VECTOR_SEARCH) {
      return 0;
    }
    var vectorDef = indexDefinition.asVectorDefinition();
    if (vectorDef.getNestedRoot().isEmpty()) {
      return 0;
    }
    var nestedRoot = vectorDef.getNestedRoot().get();
    return (int) vectorDef.getFields().stream()
        .filter(field -> field.isVectorField() && field.getPath().isChildOf(nestedRoot))
        .count();
  }

  /** De-registers all metrics for the index. */
  @Override
  public void close() {
    this.metricsFactory.close();
    this.indexingMetricsUpdater.close();
    this.queryingMetricsUpdater.close();
    this.replicationMetricsUpdater.close();
    this.indexMetricValuesSupplier.close();
  }

  public static class IndexingMetricsUpdater implements Closeable {
    public static final String NAMESPACE = "indexing";

    static final String TOTAL_BYTES_PROCESSED_COUNTER_NAME = "totalBytesProcessed";
    static final String OPTIME_GAUGE_NAME = "replicationOpTime";
    static final String MAX_POSSIBLE_OPTIME_NAME = "maxPossibleReplicationOpTime";
    static final String REPLICATION_LAG_MILLISECONDS_NAME = "replicationLagMs";
    static final String COMMIT_TIMER_NAME = "commitDurations";
    static final String INITIAL_SYNC_EXCEPTION_COUNTER_NAME = "initialSyncExceptions";
    static final String CONSECUTIVE_INITIAL_SYNC_RESYNC_EXCEPTIONS_NAME =
        "consecutiveInitialSyncResyncExceptions";
    static final String STEADY_STATE_EXCEPTION_COUNTER_NAME = "steadyStateExceptions";
    static final String SORTABLE_STRING_TRUNCATED = "sortableStringTruncated";
    static final String INVALID_GEOMETRY_FIELD = "invalidGeometryField";
    static final String VECTOR_FIELDS_INDEXED = "vectorFieldsIndexed";
    static final String INDEX_TYPE_TAG_NAME = "indexType";
    static final String LARGE_CHANGE_STREAM_EVENTS = "largeChangeStreamEvents";
    static final String BLOOM_FILTER_ID_POSTING_CREATED = "bloomFilterIdPostingCreated";
    static final String LUCENE_99_ID_POSTING_CREATED = "lucene99IdPostingCreated";

    @VisibleForTesting
    public static final List<Bytes> CHANGE_STREAM_EVENT_SIZE_THRESHOLDS =
        List.of(Bytes.ofMebi(10), Bytes.ofMebi(12), Bytes.ofMebi(14), Bytes.ofMebi(15));

    private final PerIndexMetricsFactory metricsFactory;
    private final Map<DocumentEvent.EventType, Counter> documentEventTypeCounterMap;
    private final Counter totalBytesProcessedCounter;
    private final Timer commitTimer;
    private final Counter initialSyncExceptionCounter;
    private final AtomicLong consecutiveInitialSyncResyncExceptions;
    private final Counter steadyStateExceptionCounter;
    private final Counter sortableStringTruncated;
    private final Counter invalidGeometryField;
    private final Counter vectorFieldsIndexed;
    private final ReplicationOpTimeInfo replicationOpTimeInfo;
    private final Timer batchIndexingTimer;
    private final Map<Bytes, Counter> changeStreamEventSizeCounters;
    private final Counter bloomFilterIdPostingCreatedCounter;
    private final Counter lucene99IdPostingCreatedCounter;

    @VisibleForTesting
    public IndexingMetricsUpdater(
        PerIndexMetricsFactory metricsFactory, IndexDefinition.Type indexType) {
      this(
          metricsFactory,
          indexType,
          indexType == IndexDefinition.Type.SEARCH
              ? IndexTypeData.IndexTypeTag.TAG_SEARCH
              : IndexTypeData.IndexTypeTag.TAG_VECTOR_SEARCH,
          indexType == IndexDefinition.Type.SEARCH
              ? SearchIndexCapabilities.CURRENT_FEATURE_VERSION
              : VectorIndexCapabilities.CURRENT_FEATURE_VERSION,
          true);
    }

    /** Constructs IndexingMetricsUpdater from metrics factory and indexType. */
    public IndexingMetricsUpdater(
        PerIndexMetricsFactory metricsFactory,
        IndexDefinition.Type indexType,
        IndexTypeData.IndexTypeTag indexTypeTag,
        int indexFeatureVersion,
        boolean isIndexFeatureVersionFourEnabled) {
      Tags indexFeatureVersionTag =
          isIndexFeatureVersionFourEnabled
              ? Tags.of(INDEX_FEATURE_VERSION_NAME, String.valueOf(indexFeatureVersion))
              : Tags.empty();
      this.metricsFactory = metricsFactory;
      this.totalBytesProcessedCounter =
          metricsFactory.perIndexCounter(TOTAL_BYTES_PROCESSED_COUNTER_NAME);
      this.commitTimer = metricsFactory.perIndexTimer(COMMIT_TIMER_NAME);
      this.documentEventTypeCounterMap =
          metricsFactory.getPerIndexEnumCounterMap(
              DocumentEvent.EventType.class,
              Tags.of(INDEX_TYPE_TAG_NAME, indexType.name().toLowerCase()));
      this.initialSyncExceptionCounter =
          metricsFactory.perIndexCounter(
              INITIAL_SYNC_EXCEPTION_COUNTER_NAME, indexFeatureVersionTag);
      this.consecutiveInitialSyncResyncExceptions =
          metricsFactory.perIndexNumGauge(CONSECUTIVE_INITIAL_SYNC_RESYNC_EXCEPTIONS_NAME);
      this.steadyStateExceptionCounter =
          metricsFactory.perIndexCounter(
              STEADY_STATE_EXCEPTION_COUNTER_NAME, indexFeatureVersionTag);
      this.sortableStringTruncated = metricsFactory.counter(SORTABLE_STRING_TRUNCATED);
      this.invalidGeometryField = metricsFactory.counter(INVALID_GEOMETRY_FIELD);
      this.vectorFieldsIndexed = metricsFactory.counter(VECTOR_FIELDS_INDEXED);
      this.bloomFilterIdPostingCreatedCounter =
          metricsFactory.counter(BLOOM_FILTER_ID_POSTING_CREATED);
      this.lucene99IdPostingCreatedCounter = metricsFactory.counter(LUCENE_99_ID_POSTING_CREATED);

      this.replicationOpTimeInfo = new ReplicationOpTimeInfo();
      this.metricsFactory.perIndexObjectValueGauge(
          OPTIME_GAUGE_NAME,
          this.replicationOpTimeInfo,
          opTimeInfo ->
              opTimeInfo
                  .snapshot()
                  .map(ReplicationOpTimeInfo.Snapshot::replicationOpTime)
                  .map(Long::doubleValue)
                  .orElse(Double.NaN));

      this.metricsFactory.perIndexObjectValueGauge(
          MAX_POSSIBLE_OPTIME_NAME,
          this.replicationOpTimeInfo,
          opTimeInfo ->
              opTimeInfo
                  .snapshot()
                  .map(ReplicationOpTimeInfo.Snapshot::maxPossibleReplicationOpTime)
                  .map(Long::doubleValue)
                  .orElse(Double.NaN));

      // this gauge reports ms but since BsonTimestamp uses seconds, it is limited to second
      // granularity
      this.metricsFactory.perIndexObjectValueGauge(
          REPLICATION_LAG_MILLISECONDS_NAME,
          this.replicationOpTimeInfo,
          opTimeInfo ->
              opTimeInfo
                  .snapshot()
                  .map(ReplicationOpTimeInfo.Snapshot::replicationLagMs)
                  .map(Long::doubleValue)
                  .orElse(Double.NaN),
          Tags.of(INDEX_TYPE_TAG_NAME, indexTypeTag.tagValue));

      this.batchIndexingTimer =
          this.metricsFactory.timer(
              ServerStatusDataExtractor.ReplicationMeterData.IndexingMeterData
                  .INDEXING_BATCH_DURATIONS,
              Tags.of("indexType", indexType.name().toLowerCase()));

      this.changeStreamEventSizeCounters = new HashMap<>();
      for (Bytes threshold : CHANGE_STREAM_EVENT_SIZE_THRESHOLDS) {
        this.changeStreamEventSizeCounters.put(
            threshold,
            this.metricsFactory.counter(
                LARGE_CHANGE_STREAM_EVENTS, Tags.of("threshold", threshold.toMebi() + "MiB")));
      }
    }

    public PerIndexMetricsFactory getMetricsFactory() {
      return this.metricsFactory;
    }

    public Counter getDocumentEventTypeCounter(DocumentEvent.EventType type) {
      return this.documentEventTypeCounterMap.get(type);
    }

    public Counter getInitialSyncExceptionCounter() {
      return this.initialSyncExceptionCounter;
    }

    public AtomicLong getConsecutiveInitialSyncResyncExceptions() {
      return this.consecutiveInitialSyncResyncExceptions;
    }

    public Counter getSteadyStateExceptionCounter() {
      return this.steadyStateExceptionCounter;
    }

    public Counter getSortableStringTruncatedCounter() {
      return this.sortableStringTruncated;
    }

    public Counter getInvalidGeometryFieldCounter() {
      return this.invalidGeometryField;
    }

    public Counter getVectorFieldsIndexed() {
      return this.vectorFieldsIndexed;
    }

    public Counter getBloomFilterIdPostingCreatedCounter() {
      return this.bloomFilterIdPostingCreatedCounter;
    }

    public Counter getLucene99IdPostingCreatedCounter() {
      return this.lucene99IdPostingCreatedCounter;
    }
    
    public Timer getCommitTimer() {
      return this.commitTimer;
    }

    public Counter getTotalBytesProcessedCounter() {
      return this.totalBytesProcessedCounter;
    }

    public Timer getBatchIndexingTimer() {
      return this.batchIndexingTimer;
    }

    public ReplicationOpTimeInfo getReplicationOpTimeInfo() {
      return this.replicationOpTimeInfo;
    }

    public void recordDocumentSizeBytes(int size) {
      CHANGE_STREAM_EVENT_SIZE_THRESHOLDS.stream()
          .filter(threshold -> size >= threshold.toBytes())
          .map(this.changeStreamEventSizeCounters::get)
          .forEach(Counter::increment);
    }

    @VisibleForTesting
    IndexMetrics.IndexingMetrics getMetrics(
        IndexMetricValuesSupplier indexMetricValuesSupplier, int numNestedVectorFields) {
      DocCounts docCounts = indexMetricValuesSupplier.getDocCounts();
      return IndexMetrics.IndexingMetrics.create(
          this.documentEventTypeCounterMap,
          getInitialSyncExceptionCounter(),
          getSteadyStateExceptionCounter(),
          getReplicationOpTimeInfo().snapshot(),
          getTotalBytesProcessedCounter(),
          indexMetricValuesSupplier.getCachedIndexSize(),
          // deprecated vectorFieldSize is set to 0
          0L,
          indexMetricValuesSupplier.getNumFields(),
          indexMetricValuesSupplier.getNumFieldsPerDatatype(),
          docCounts.numDocs,
          docCounts.numLuceneMaxDocs,
          docCounts.maxLuceneMaxDocs,
          docCounts.numMongoDbDocs,
          getBatchIndexingTimer(),
          indexMetricValuesSupplier.getRequiredMemoryForVectorData(),
          numNestedVectorFields);
    }

    @Override
    public void close() {
      this.metricsFactory.close();
    }
  }

  public static class QueryingMetricsUpdater implements Closeable {
    public static final String NAMESPACE = "query";
    private static final FluentLogger flogger = FluentLogger.forEnclosingClass();
    private final PerIndexMetricsFactory metricsFactory;
    private final Counter totalQueryCounter;

    // Sum of invalidQueryCounter and internallyFailedQueryCounter.
    private final Counter failedQueryCounter;

    // A malformed user queries that cannot be parsed or does not conform to the query syntax.
    private final Counter invalidQueryCounter;

    // A query that is valid, but mongot internally hit an error when processing/executing this
    // query.
    private final Counter internallyFailedQueryCounter;
    // A query returns an NPE. This counter belongs to the counter above, but provides more direct
    // signal that mongot has a bug.
    private final Counter npeQueryCounter;

    private final Counter failedExplainQueryAggregate;

    private final Counter lenientFailureCounter;

    /** The overall number of times mongot was called for search. */
    private final Counter searchGetMoreCommandCounter;

    /** The overall number of times mongot was called for $vectorSearch over vector indexes. */
    private final Counter vectorCommandCounter;

    /** The overall number of times mongot was called for $vectorSearch over search indexes. */
    private final Counter vectorSearchQueriesOverSearchIndexes;

    /** The number of times mongot receives an extractable limit query from mongod. */
    private final Counter extractableLimitQueryCounter;

    /** Counters for full scan experiment stats. These should be removed by 6/1/2026. */
    private final Counter fallBackHeuristicSuccessCounter;

    private final Counter fallBackHeuristicFailureCounter;

    /**
     * The number of times mongot oversubscription to an extracted limit hint was not enough, and a
     * second batch is required.
     */
    private final Counter extractableLimitQuerySecondBatchCounter;

    /**
     * Number of document hits in all queries. For queries that early terminate, this number can be
     * a lower bound. Total hits normally corresponds to the number of times Lucene's
     * {Collector.collect} was called, and can be much higher than the amount of documents returned.
     */
    private final Counter totalHitsLowerBoundCount;

    /**
     * The ratio of (orphaned or deleted docs/docs returned) when mongot oversubscription is not
     * enough to compensate for them.
     */
    private final DistributionSummary orphanedDeletedDocsRatio;

    /** The number of times mongot was called in scope of a single cursor. */
    private final DistributionSummary searchGetMoreCommandPerQuery;

    /* Measures overall time spent on search and post-processing (stored fields / doc values
    retrieval, highlighting). */
    private final Timer searchResultBatchLatencyTimer;
    private final Timer vectorResultLatencyTimer;

    /** Measures time spent on lucene search/searchAfter to collect topDocs. */
    private final Timer luceneTopDocsSearchLatencyTimer;

    /** Measures time spent on lucene vectorSearch to collect initialSearch topDocs. */
    private final Timer vectorSearchInitialTopDocsLatencyTimer;

    /** Measures time spent on lucene vectorSearch to collect getMore topDocs. */
    private final Timer vectorSearchGetMoreTopDocsLatencyTimer;

    /** Number of failures caused by binary quantized vector rescoring. */
    private final Counter vectorRescoringFailureCounter;

    private final QueryFeaturesMetricsUpdater queryFeaturesMetricsUpdater;
    private final DistributionSummary batchDocumentCount;
    private final DistributionSummary batchDataSize;

    /** Number of times a batch with ties is encountered in paginated queries. */
    private final Counter batchWithTiesCounter;

    private final Counter noProgressBatchCounter;
    private final Timer tokenFacetsStateRefreshLatencyTimer;
    private final Timer stringFacetsStateRefreshLatencyTimer;

    private final DistributionSummary numCandidatesUnquantized;
    private final DistributionSummary numCandidatesScalarQuantized;
    private final DistributionSummary numCandidatesBinaryQuantized;
    private final DistributionSummary limitPerQuery;

    /** Distribution of total requested string facet buckets per facet query. */
    private final DistributionSummary totalFacetBucketsPerQuery;

    /** Number of times a phantom LuceneIndexSearcherReference is not closed. */
    private final Counter phantomSearcherCleanupCounter;

    /** Number of queries where the query sort can benefit from index sort optimization. */
    private final Counter benefitFromIndexSortCounter;

    /**
     * Tracks dynamic HNSW search method selected per segment. Exported as
     * mongot_index_stats_query_knnSearchMode_total
     */
    private final Map<KnnSearchMode, Counter> knnSearchModeCounter;

    /** Counters for visited nodes in KNN queries, indexed by filter and mode. */
    private final Counter vectorSearchVisitedNodesUnfilteredApproximateCounter;

    private final Counter vectorSearchVisitedNodesUnfilteredExactCounter;
    private final Counter vectorSearchVisitedNodesFilteredApproximateCounter;
    private final Counter vectorSearchVisitedNodesFilteredExactCounter;

    /** Histograms for visited nodes per segment in KNN queries, indexed by filter and mode. */
    private final DistributionSummary vectorSearchVisitedNodesPerSegmentUnfilteredApproximate;

    private final DistributionSummary vectorSearchVisitedNodesPerSegmentFilteredApproximate;
    private final DistributionSummary vectorSearchVisitedNodesPerSegmentUnfilteredExact;
    private final DistributionSummary vectorSearchVisitedNodesPerSegmentFilteredExact;

    @VisibleForTesting
    public QueryingMetricsUpdater(PerIndexMetricsFactory metricsFactory) {
      this(
          metricsFactory,
          IndexTypeData.IndexTypeTag.TAG_SEARCH,
          SearchIndexCapabilities.CURRENT_FEATURE_VERSION,
          true);
    }

    /** An updater for query time metrics. */
    public QueryingMetricsUpdater(
        PerIndexMetricsFactory metricsFactory,
        IndexTypeData.IndexTypeTag indexTypeTag,
        int indexFeatureVersion,
        boolean isIndexFeatureVersionFourEnabled) {
      // Search indices don't need per-index vector metrics, and likewise when roles are reversed.
      boolean isSearchIndex = indexTypeTag == IndexTypeData.IndexTypeTag.TAG_SEARCH;
      boolean isVectorIndex =
          indexTypeTag == IndexTypeData.IndexTypeTag.TAG_VECTOR_SEARCH
              || indexTypeTag == IndexTypeData.IndexTypeTag.TAG_VECTOR_SEARCH_AUTO_EMBEDDING;
      Tags indexFeatureVersionTag =
          isIndexFeatureVersionFourEnabled
              ? Tags.of(INDEX_FEATURE_VERSION_NAME, String.valueOf(indexFeatureVersion))
              : Tags.empty();
      this.metricsFactory = metricsFactory;
      this.totalQueryCounter =
          metricsFactory.perIndexCounter("totalQueries", indexFeatureVersionTag);
      this.failedQueryCounter =
          metricsFactory.perIndexCounter("failedQueries", indexFeatureVersionTag);
      this.invalidQueryCounter = metricsFactory.counter("invalidQueries");
      this.internallyFailedQueryCounter =
          metricsFactory.counter("internallyFailedQueries", indexFeatureVersionTag);
      this.failedExplainQueryAggregate = metricsFactory.counter("failedExplainQueryAggregate");
      this.npeQueryCounter = metricsFactory.counter("npeQueries");
      this.lenientFailureCounter = metricsFactory.counter("lenientFailures");
      this.searchGetMoreCommandCounter =
          isSearchIndex
              ? metricsFactory.perIndexCounter("getMoreCommandCalls")
              : metricsFactory.counter("getMoreCommandCalls");
      this.vectorCommandCounter =
          isVectorIndex
              ? metricsFactory.perIndexCounter(
                  "vectorCommandCalls", Tags.of(INDEX_TYPE_TAG_NAME, indexTypeTag.tagValue))
              : metricsFactory.counter("vectorCommandCalls");
      this.extractableLimitQueryCounter = metricsFactory.counter("extractableLimitQueries");
      this.extractableLimitQuerySecondBatchCounter =
          metricsFactory.counter("extractableLimitSecondBatchQueries");
      this.totalHitsLowerBoundCount = metricsFactory.counter("totalHitsCount");
      this.searchGetMoreCommandPerQuery = metricsFactory.summary("getMoreCommandCallsPerQuery");
      this.searchResultBatchLatencyTimer =
          isSearchIndex
              ? metricsFactory.perIndexTimer("searchResultBatchLatencies", indexFeatureVersionTag)
              : metricsFactory.timer("searchResultBatchLatencies");
      this.vectorResultLatencyTimer =
          isVectorIndex
              ? metricsFactory.perIndexTimer(
                  "vectorResultLatencies",
                  Tags.of(INDEX_TYPE_TAG_NAME, indexTypeTag.tagValue).and(indexFeatureVersionTag))
              : metricsFactory.timer("vectorResultLatencies");
      this.vectorSearchQueriesOverSearchIndexes =
          metricsFactory.perIndexCounter("vectorSearchQueriesOverSearchIndexes");
      this.luceneTopDocsSearchLatencyTimer = metricsFactory.timer("luceneTopDocsSearchLatencies");

      this.vectorSearchInitialTopDocsLatencyTimer =
          metricsFactory.timer("vectorSearchInitialTopDocsLatencyTimer");
      this.vectorSearchGetMoreTopDocsLatencyTimer =
          metricsFactory.timer("vectorSearchGetMoreTopDocsLatencyTimer");
      this.queryFeaturesMetricsUpdater = new QueryFeaturesMetricsUpdater(metricsFactory);

      this.orphanedDeletedDocsRatio = metricsFactory.summary("orphanedDeletedDocsRatio");

      this.batchDocumentCount = metricsFactory.summary("batchDocumentCount");
      this.batchDataSize = metricsFactory.summary("batchDataSize");
      this.vectorRescoringFailureCounter = metricsFactory.counter("vectorRescoringFailureCount");
      this.batchWithTiesCounter = metricsFactory.counter("batchWithTies");
      this.noProgressBatchCounter = metricsFactory.counter("noProgressBatches");
      this.tokenFacetsStateRefreshLatencyTimer =
          isSearchIndex
              ? metricsFactory.perIndexTimer("tokenFacetsStateRefreshLatency")
              : metricsFactory.timer("tokenFacetsStateRefreshLatency");
      this.stringFacetsStateRefreshLatencyTimer =
          isSearchIndex
              ? metricsFactory.perIndexTimer("stringFacetsStateRefreshLatency")
              : metricsFactory.timer("stringFacetsStateRefreshLatency");
      this.numCandidatesUnquantized =
          metricsFactory.histogram(
              "numCandidatesPerQuery",
              Tags.of("quantization", "unquantized"),
              NUM_CANDIDATES_BUCKETS);
      this.numCandidatesScalarQuantized =
          metricsFactory.histogram(
              "numCandidatesPerQuery",
              Tags.of("quantization", "scalarQuantized"),
              NUM_CANDIDATES_BUCKETS);
      this.numCandidatesBinaryQuantized =
          metricsFactory.histogram(
              "numCandidatesPerQuery",
              Tags.of("quantization", "binaryQuantized"),
              NUM_CANDIDATES_BUCKETS);
      this.knnSearchModeCounter =
          metricsFactory.getEnumCounterMap(
              KnnSearchMode.class,
              (String s) -> metricsFactory.counter("knnSearchMode", Tags.of("mode", s)));
      this.limitPerQuery = metricsFactory.histogram("limitPerQuery", LIMIT_BUCKETS);
      this.totalFacetBucketsPerQuery =
          metricsFactory.histogram(
              "totalFacetBucketsPerQuery", Tags.empty(), TOTAL_FACET_BUCKETS_BUCKETS);
      this.phantomSearcherCleanupCounter = metricsFactory.counter("phantomSearcherCleanupCount");
      this.benefitFromIndexSortCounter = metricsFactory.counter("benefitFromIndexSortCount");
      this.vectorSearchVisitedNodesUnfilteredApproximateCounter =
          metricsFactory.counter(
              "vectorSearchVisitedNodes", Tags.of("filter", "false", "mode", "approximate"));
      this.vectorSearchVisitedNodesUnfilteredExactCounter =
          metricsFactory.counter(
              "vectorSearchVisitedNodes", Tags.of("filter", "false", "mode", "exact"));
      this.vectorSearchVisitedNodesFilteredApproximateCounter =
          metricsFactory.counter(
              "vectorSearchVisitedNodes", Tags.of("filter", "true", "mode", "approximate"));
      this.vectorSearchVisitedNodesFilteredExactCounter =
          metricsFactory.counter(
              "vectorSearchVisitedNodes", Tags.of("filter", "true", "mode", "exact"));
      this.vectorSearchVisitedNodesPerSegmentUnfilteredApproximate =
          metricsFactory.histogram(
              "vectorSearchVisitedNodesPerSegment",
              Tags.of("filter", "false", "mode", "approximate"),
              VISITED_NODES_BUCKETS);
      this.vectorSearchVisitedNodesPerSegmentFilteredApproximate =
          metricsFactory.histogram(
              "vectorSearchVisitedNodesPerSegment",
              Tags.of("filter", "true", "mode", "approximate"),
              VISITED_NODES_BUCKETS);
      this.vectorSearchVisitedNodesPerSegmentUnfilteredExact =
          metricsFactory.histogram(
              "vectorSearchVisitedNodesPerSegment",
              Tags.of("filter", "false", "mode", "exact"),
              VISITED_NODES_BUCKETS);
      this.vectorSearchVisitedNodesPerSegmentFilteredExact =
          metricsFactory.histogram(
              "vectorSearchVisitedNodesPerSegment",
              Tags.of("filter", "true", "mode", "exact"),
              VISITED_NODES_BUCKETS);
      this.fallBackHeuristicSuccessCounter =
          metricsFactory.counter("fallBackHeuristicSuccessCounter");
      this.fallBackHeuristicFailureCounter =
          metricsFactory.counter("fallBackHeuristicFailureCounter");
    }

    public Counter getTotalQueryCounter() {
      return this.totalQueryCounter;
    }

    public Counter getFailedQueryCounter() {
      return this.failedQueryCounter;
    }

    public Counter getFailedExplainQueryAggregate() {
      return this.failedExplainQueryAggregate;
    }

    public Counter getInvalidQueryCounter() {
      return this.invalidQueryCounter;
    }

    public Counter getInternallyFailedQueryCounter() {
      return this.internallyFailedQueryCounter;
    }

    /**
     * Increments the search mode that we dynamically selected during query execution on a
     * per-segment basis.
     */
    public void incrementKnnSearchMode(KnnSearchMode type) {
      this.knnSearchModeCounter.get(type).increment();
    }

    /** Records the number of visited nodes for a vector search query. */
    public void recordVectorSearchVisitedNodes(
        long visitedNodes, boolean hasFilter, KnnSearchMode mode) {
      Counter counter =
          switch (mode) {
            case APPROXIMATE ->
                hasFilter
                    ? this.vectorSearchVisitedNodesFilteredApproximateCounter
                    : this.vectorSearchVisitedNodesUnfilteredApproximateCounter;
            case EXACT, FALLBACK_TO_EXACT, FULL_SCAN ->
                hasFilter
                    ? this.vectorSearchVisitedNodesFilteredExactCounter
                    : this.vectorSearchVisitedNodesUnfilteredExactCounter;
          };
      counter.increment(visitedNodes);
    }

    /**
     * Records the number of visited nodes per segment for vector search queries.
     *
     * @param visitedNodes the number of nodes visited during the search
     * @param hasFilter whether the query has a filter
     * @param mode the search mode (APPROXIMATE or FALLBACK_TO_EXACT)
     */
    public void recordVectorSearchVisitedNodesPerSegment(
        long visitedNodes, boolean hasFilter, KnnSearchMode mode) {
      DistributionSummary histogram;
      if (mode == KnnSearchMode.APPROXIMATE) {
        histogram =
            hasFilter
                ? this.vectorSearchVisitedNodesPerSegmentFilteredApproximate
                : this.vectorSearchVisitedNodesPerSegmentUnfilteredApproximate;
      } else {
        histogram =
            hasFilter
                ? this.vectorSearchVisitedNodesPerSegmentFilteredExact
                : this.vectorSearchVisitedNodesPerSegmentUnfilteredExact;
      }
      histogram.record(visitedNodes);
    }

    /**
     * Records total requested string facet buckets for facet queries when {@code enabled} returns
     * true and the query has positive string bucket demand (numeric-only facet queries are
     * skipped). Called from the search execution path after a successful query (e.g. {@link
     * MeteredSearchIndexReader}), consistent with other query-time distribution metrics.
     *
     * <p>{@code enabled} is consulted only for {@link CollectorQuery} with a {@link FacetCollector}
     * so dynamic flag evaluation stays off the hot path for non-facet queries.
     */
    public void recordTotalStringFacetBucketsIfApplicable(Query query, BooleanSupplier enabled) {
      if (!(query instanceof CollectorQuery collectorQuery)
          || !(collectorQuery.collector() instanceof FacetCollector facetCollector)) {
        return;
      }
      if (!enabled.getAsBoolean()) {
        return;
      }
      int totalStringBuckets = facetCollector.getTotalRequestedStringFacetBuckets();
      if (totalStringBuckets > 0) {
        this.totalFacetBucketsPerQuery.record(totalStringBuckets);
      }
    }

    @VisibleForTesting
    public Counter getNpeQueryCounter() {
      return this.npeQueryCounter;
    }

    // Increments the total failedQueryCounter. And depending on the exception is from malformed
    // user query, or the internal query processing error, increases corresponding counters.
    public void handleQueryException(Exception e, String queryDebugString) {
      this.failedQueryCounter.increment();
      if (e instanceof InvalidQueryException) {
        flogger.atWarning().atMostEvery(1, TimeUnit.HOURS).withCause(e).log(
            "Hit an invalid query: %s", queryDebugString);
        this.invalidQueryCounter.increment();
      } else {
        // Mongot internal processing error should be rare, log with a low limit. We still need a
        // limit because the warning log level may cause a flush().
        flogger.atWarning().atMostEvery(1, TimeUnit.MINUTES).withCause(e).log(
            "Internal processing error for query: %s", queryDebugString);

        if (e instanceof NullPointerException) {
          this.npeQueryCounter.increment();
        }
        this.internallyFailedQueryCounter.increment();
      }
    }

    public Counter getLenientFailureCounter() {
      return this.lenientFailureCounter;
    }

    public Counter getSearchAndGetMoreCommandCounter() {
      return this.searchGetMoreCommandCounter;
    }

    public Counter getVectorCommandCounter() {
      return this.vectorCommandCounter;
    }

    public Counter getExtractableLimitQueryCounter() {
      return this.extractableLimitQueryCounter;
    }

    public Counter getExtractableLimitQuerySecondBatchCounter() {
      return this.extractableLimitQuerySecondBatchCounter;
    }

    public Counter getTotalHitsLowerBoundCount() {
      return this.totalHitsLowerBoundCount;
    }

    public DistributionSummary getSearchAndGetMoreCommandPerQuery() {
      return this.searchGetMoreCommandPerQuery;
    }

    public DistributionSummary getOrphanedDeletedDocsRatio() {
      return this.orphanedDeletedDocsRatio;
    }

    public Timer getSearchResultBatchLatencyTimer() {
      return this.searchResultBatchLatencyTimer;
    }

    public Timer getVectorResultLatencyTimer() {
      return this.vectorResultLatencyTimer;
    }

    public Counter getVectorSearchQueriesOverSearchIndexes() {
      return this.vectorSearchQueriesOverSearchIndexes;
    }

    public Timer getLuceneTopDocsSearchLatencyTimer() {
      return this.luceneTopDocsSearchLatencyTimer;
    }

    public Timer getVectorSearchInitialTopDocsLatencyTimer() {
      return this.vectorSearchInitialTopDocsLatencyTimer;
    }

    public Timer getVectorSearchGetMoreTopDocsLatencyTimer() {
      return this.vectorSearchGetMoreTopDocsLatencyTimer;
    }

    public DistributionSummary getBatchDocumentCount() {
      return this.batchDocumentCount;
    }

    public DistributionSummary getBatchDataSize() {
      return this.batchDataSize;
    }

    public Counter getVectorRescoringFailureCounter() {
      return this.vectorRescoringFailureCounter;
    }

    public Counter getBatchWithTiesCounter() {
      return this.batchWithTiesCounter;
    }

    public Counter getNoProgressBatchCounter() {
      return this.noProgressBatchCounter;
    }

    public Timer getTokenFacetsStateRefreshLatencyTimer() {
      return this.tokenFacetsStateRefreshLatencyTimer;
    }

    public Timer getStringFacetsStateRefreshLatencyTimer() {
      return this.stringFacetsStateRefreshLatencyTimer;
    }

    public DistributionSummary getNumCandidatesUnquantized() {
      return this.numCandidatesUnquantized;
    }

    public DistributionSummary getNumCandidatesScalarQuantized() {
      return this.numCandidatesScalarQuantized;
    }

    public DistributionSummary getNumCandidatesBinaryQuantized() {
      return this.numCandidatesBinaryQuantized;
    }

    public DistributionSummary getLimitPerQuery() {
      return this.limitPerQuery;
    }

    public Counter getPhantomSearcherCleanupCounter() {
      return this.phantomSearcherCleanupCounter;
    }

    public Counter getBenefitFromIndexSortCounter() {
      return this.benefitFromIndexSortCounter;
    }

    public void recordDynamicFeatureFlagLatencyTimer(long durationNs) {
      for (Tags tags : DynamicFeatureFlagsMetricsRecorder.getTagsPerFlag()) {
        this.metricsFactory
            .timer("dynamicFeatureFlagLatencies", tags)
            .record(durationNs, TimeUnit.NANOSECONDS);
      }
    }

    @VisibleForTesting
    IndexMetrics.QueryingMetrics getMetrics() {
      return IndexMetrics.QueryingMetrics.create(
          this.totalQueryCounter,
          this.failedQueryCounter,
          this.lenientFailureCounter,
          this.searchGetMoreCommandCounter,
          this.searchResultBatchLatencyTimer,
          this.tokenFacetsStateRefreshLatencyTimer,
          this.stringFacetsStateRefreshLatencyTimer,
          this.queryFeaturesMetricsUpdater.getMetrics());
    }

    public QueryFeaturesMetricsUpdater getQueryFeaturesMetricsUpdater() {
      return this.queryFeaturesMetricsUpdater;
    }

    @Override
    public void close() {
      this.metricsFactory.close();
      this.queryFeaturesMetricsUpdater.close();
    }

    /**
     * Given that we predicted that we should use full scan but instead chose HNSW search, record
     * whether full scan would have been better in hindsight.
     */
    public void recordFallbackHeuristicResult(TotalHits.Relation relation) {
      switch (relation) {
        case EQUAL_TO -> this.fallBackHeuristicFailureCounter.increment();
        case GREATER_THAN_OR_EQUAL_TO -> this.fallBackHeuristicSuccessCounter.increment();
      }
    }

    /**
     * Per-index metrics for query shapes. All counters created in this class can be queried via
     * mongot_index_stats_query_feature_total with a filter on the 'name' tag.
     */
    public static class QueryFeaturesMetricsUpdater implements Closeable {
      static final String METRIC_NAME = "feature";
      static final String NAME_TAG = "name";

      static final String SORT_NO_DATA_NAME_TAG_PREFIX = "sort_noData_";
      static final String KNN_BETA_FILTER_TAG_PREFIX = "knn_beta_filter_";

      /** Tag prefix for lexical pre-filter operators. */
      static final String SEARCH_VECTOR_SEARCH_FILTER_TAG_PREFIX = "vectorSearch_filter_";

      /** Tag prefix for MQL pre-filter operators. */
      private static final String VECTOR_SEARCH_FILTER_TAG_PREFIX = "vectorSearch_operator_";

      // redundant legacy synonyms counter, keeping to avoid breaking changes to counters
      private static final String SYNONYMS_COUNTER_NAME = "synonyms";
      private static final String TEXT_SYNONYMS_COUNTER_NAME = "textSynonyms";
      private static final String PHRASE_SYNONYMS_COUNTER_NAME = "phraseSynonyms";
      private static final String TEXT_DEPRECATED_SYNONYMS_COUNTER_NAME =
          "textSynonymsWithoutMatchCriteria";
      private static final String FUZZY_COUNTER_NAME = "fuzzy";
      private static final String WILDCARD_PATHS_COUNTER_NAME = "wildcardPaths";
      private static final String HIGHLIGHTING_COUNTER_NAME = "highlighting";
      private static final String CONCURRENT_COUNTER_NAME = "concurrent";
      private static final String SORT_COUNTER_NAME = "sort";
      private static final String TRACKING_COUNTER_NAME = "tracking";
      private static final String SEQUENCE_TOKEN_COUNTER_NAME = "sequenceToken";
      private static final String SCORE_DETAILS_COUNTER_NAME = "scoreDetails";
      private static final String REQUIRE_SEQUENCE_TOKENS_COUNTER_NAME = "requireSequenceTokens";
      private static final String RETURN_STORED_SOURCE_COUNTER_NAME = "returnStoredSource";
      private static final String RETURN_SCOPE_COUNTER_NAME = "returnScope";
      private static final String VECTOR_SEARCH_FILTER_COUNTER_NAME = "vectorSearch_filter";
      private static final String EXPLAIN_COUNTER_NAME = "explain";
      private static final String FACET_DRILL_SIDEWAYS_COUNTER_NAME =
          "query_collector_facet_drill_sideways";
      private static final String FACET_DRILL_SIDEWAYS_TYPE_TAG = "type";

      private final PerIndexMetricsFactory metricsFactory;
      private final Map<Collector.Type, Counter> collectorTypeCounterMap;
      private final Map<Operator.Type, Counter> operatorTypeCounterMap;
      private final Map<Score.Type, Counter> scoreTypeCounterMap;
      private final Map<TextOperator.MatchCriteria, Counter> textMatchCriteriaCounterMap;
      private final Map<NullEmptySortPosition, Counter> noDataSortPositionCounterMap;
      private final Map<VectorSearchCriteria.Type, Counter> vectorSearchQueryTypeCounterMap;
      private final Map<Operator.Type, Counter> knnBetaFilterOperatorTypeCounterMap;

      /** Counter for each lexical operator used as a pre-filter in $search.vectorSearch queries */
      private final Map<Operator.Type, Counter> searchVectorSearchFilterOperatorTypeCounterMap;

      /** Counter for each MQL operator used as a pre-filter in $vectorSearch queries */
      private final Map<MqlFilterOperator.Category, Counter> vectorSearchFilterOperatorCounterMap;

      private final Counter synonymsCounter;
      private final Counter textSynonymsCounter;
      private final Counter phraseSynonymsCounter;
      private final Counter textDeprecatedSynonymsCounter;
      private final Counter fuzzyCounter;
      private final Counter wildcardPathsCounter;
      private final Counter highlightingCounter;
      private final Counter concurrentCounter;
      private final Counter scoreDetailsCounter;
      // incremented when pagination token is present in the request
      private final Counter sequenceTokenCounter;
      // incremented when projecting of searchSequenceToken is requested
      private final Counter requireSequenceTokensCounter;
      private final Counter sortCounter;
      private final Counter trackingCounter;
      private final Counter returnStoredSourceCounter;
      private final Counter returnScopeCounter;
      private final Counter explainCounter;
      private final Map<QueryOptimizationStatus, Counter> facetDrillSidewaysCounterMap;

      /** Counts the number vector search queries with a pre-filter. */
      private final Counter vectorSearchFilterCounter;

      @SuppressWarnings("checkstyle:MissingJavadocMethod")
      public QueryFeaturesMetricsUpdater(PerIndexMetricsFactory metricsFactory) {
        this.metricsFactory = metricsFactory;
        this.synonymsCounter = createCounter(metricsFactory, SYNONYMS_COUNTER_NAME);
        this.textSynonymsCounter = createCounter(metricsFactory, TEXT_SYNONYMS_COUNTER_NAME);
        this.phraseSynonymsCounter = createCounter(metricsFactory, PHRASE_SYNONYMS_COUNTER_NAME);
        this.textDeprecatedSynonymsCounter =
            createCounter(metricsFactory, TEXT_DEPRECATED_SYNONYMS_COUNTER_NAME);
        this.fuzzyCounter = createCounter(metricsFactory, FUZZY_COUNTER_NAME);
        this.wildcardPathsCounter = createCounter(metricsFactory, WILDCARD_PATHS_COUNTER_NAME);
        this.highlightingCounter = createCounter(metricsFactory, HIGHLIGHTING_COUNTER_NAME);
        this.concurrentCounter = createCounter(metricsFactory, CONCURRENT_COUNTER_NAME);
        this.sortCounter = createCounter(metricsFactory, SORT_COUNTER_NAME);
        this.trackingCounter = createCounter(metricsFactory, TRACKING_COUNTER_NAME);
        this.sequenceTokenCounter = createCounter(metricsFactory, SEQUENCE_TOKEN_COUNTER_NAME);
        this.scoreDetailsCounter = createCounter(metricsFactory, SCORE_DETAILS_COUNTER_NAME);
        this.requireSequenceTokensCounter =
            createCounter(metricsFactory, REQUIRE_SEQUENCE_TOKENS_COUNTER_NAME);
        this.returnStoredSourceCounter =
            createCounter(metricsFactory, RETURN_STORED_SOURCE_COUNTER_NAME);
        this.returnScopeCounter = createCounter(metricsFactory, RETURN_SCOPE_COUNTER_NAME);
        this.explainCounter = createCounter(metricsFactory, EXPLAIN_COUNTER_NAME);
        this.facetDrillSidewaysCounterMap =
            Map.of(
                QueryOptimizationStatus.OPTIMIZABLE,
                metricsFactory.counter(
                    METRIC_NAME,
                    Tags.of(NAME_TAG, FACET_DRILL_SIDEWAYS_COUNTER_NAME)
                        .and(
                            FACET_DRILL_SIDEWAYS_TYPE_TAG,
                            QueryOptimizationStatus.OPTIMIZABLE.name().toLowerCase())),
                QueryOptimizationStatus.GENERIC,
                metricsFactory.counter(
                    METRIC_NAME,
                    Tags.of(NAME_TAG, FACET_DRILL_SIDEWAYS_COUNTER_NAME)
                        .and(
                            FACET_DRILL_SIDEWAYS_TYPE_TAG,
                            QueryOptimizationStatus.GENERIC.name().toLowerCase())));
        this.collectorTypeCounterMap = createEnumCounterMap(metricsFactory, Collector.Type.class);
        this.operatorTypeCounterMap = createEnumCounterMap(metricsFactory, Operator.Type.class);
        this.scoreTypeCounterMap = createEnumCounterMap(metricsFactory, Score.Type.class);
        this.textMatchCriteriaCounterMap =
            createEnumCounterMap(metricsFactory, TextOperator.MatchCriteria.class);
        this.noDataSortPositionCounterMap =
            createEnumCounterMap(
                metricsFactory, NullEmptySortPosition.class, SORT_NO_DATA_NAME_TAG_PREFIX);
        this.vectorSearchQueryTypeCounterMap =
            createEnumCounterMap(metricsFactory, VectorSearchCriteria.Type.class);
        this.knnBetaFilterOperatorTypeCounterMap =
            createEnumCounterMap(metricsFactory, Operator.Type.class, KNN_BETA_FILTER_TAG_PREFIX);
        this.searchVectorSearchFilterOperatorTypeCounterMap =
            createEnumCounterMap(
                metricsFactory, Operator.Type.class, SEARCH_VECTOR_SEARCH_FILTER_TAG_PREFIX);
        this.vectorSearchFilterOperatorCounterMap =
            createEnumCounterMap(
                metricsFactory, MqlFilterOperator.Category.class, VECTOR_SEARCH_FILTER_TAG_PREFIX);
        this.vectorSearchFilterCounter =
            createCounter(metricsFactory, VECTOR_SEARCH_FILTER_COUNTER_NAME);
      }

      private static Counter createCounter(PerIndexMetricsFactory metricsFactory, String tagValue) {
        return metricsFactory.counter(METRIC_NAME, Tags.of(NAME_TAG, tagValue));
      }

      private static <T extends Enum<T>> Map<T, Counter> createEnumCounterMap(
          PerIndexMetricsFactory metricsFactory, Class<T> enumClass) {
        return createEnumCounterMap(metricsFactory, enumClass, "");
      }

      private static <T extends Enum<T>> Map<T, Counter> createEnumCounterMap(
          PerIndexMetricsFactory metricsFactory, Class<T> enumClass, String featureNameTagPrefix) {
        return metricsFactory.getEnumCounterMap(
            enumClass,
            (featureName) ->
                metricsFactory.counter(
                    METRIC_NAME,
                    Tags.of(NAME_TAG, String.format("%s%s", featureNameTagPrefix, featureName))));
      }

      public Counter getSynonymsCounter() {
        return this.synonymsCounter;
      }

      public Counter getTextSynonymsCounter() {
        return this.textSynonymsCounter;
      }

      public Counter getPhraseSynonymsCounter() {
        return this.phraseSynonymsCounter;
      }

      public Counter getTextDeprecatedSynonymsCounter() {
        return this.textDeprecatedSynonymsCounter;
      }

      public Counter getFuzzyCounter() {
        return this.fuzzyCounter;
      }

      public Counter getWildcardPathsCounter() {
        return this.wildcardPathsCounter;
      }

      public Counter getHighlightingCounter() {
        return this.highlightingCounter;
      }

      public Counter getReturnScopeCounter() {
        return this.returnScopeCounter;
      }

      public Counter getConcurrentCounter() {
        return this.concurrentCounter;
      }

      public Counter getSortCounter() {
        return this.sortCounter;
      }

      public Counter getTrackingCounter() {
        return this.trackingCounter;
      }

      public Counter getCollectorTypeCounter(Collector.Type collectorType) {
        return this.collectorTypeCounterMap.get(collectorType);
      }

      public Counter getFacetDrillSidewaysCounter(QueryOptimizationStatus status) {
        return this.facetDrillSidewaysCounterMap.get(status);
      }

      public Counter getOperatorTypeCounter(Operator.Type operatorType) {
        return this.operatorTypeCounterMap.get(operatorType);
      }

      public Counter getScoreTypeCounter(Score.Type scoreType) {
        return this.scoreTypeCounterMap.get(scoreType);
      }

      public Counter getTextMatchCriteriaCounter(TextOperator.MatchCriteria matchCriteria) {
        return this.textMatchCriteriaCounterMap.get(matchCriteria);
      }

      public Counter getNoDataSortPositionCounter(NullEmptySortPosition nullEmptySortPosition) {
        return this.noDataSortPositionCounterMap.get(nullEmptySortPosition);
      }

      public Counter getSequenceTokenCounter() {
        return this.sequenceTokenCounter;
      }

      public Counter getScoreDetailsCounter() {
        return this.scoreDetailsCounter;
      }

      public Counter getExplainCounter() {
        return this.explainCounter;
      }

      public Counter getRequireSequenceTokensCounter() {
        return this.requireSequenceTokensCounter;
      }

      public Counter getReturnStoredSourceCounter() {
        return this.returnStoredSourceCounter;
      }

      /** Counter for $vectorSearch and $search.vectorSearch queries with at least one filter */
      public Counter getFilteredVectorSearchCounter() {
        return this.vectorSearchFilterCounter;
      }

      public Counter getVectorSearchQueryTypeCounter(
          VectorSearchCriteria.Type vectorSearchQueryType) {
        return this.vectorSearchQueryTypeCounterMap.get(vectorSearchQueryType);
      }

      public Counter getKnnBetaFilterOperatorTypeCounterMap(Operator.Type operatorType) {
        return this.knnBetaFilterOperatorTypeCounterMap.get(operatorType);
      }

      /**
       * Return a per-operator counter for $search.vectorSearch pre-filters.
       *
       * <p>This is similar to {@link #getOperatorTypeCounter(Operator.Type)}), but we count lexical
       * pre-filters separately from other $search operators.
       */
      public Counter getSearchVectorSearchFilterOperatorTypeCounterMap(Operator.Type operatorType) {
        return this.searchVectorSearchFilterOperatorTypeCounterMap.get(operatorType);
      }

      public Counter getVectorSearchFilterOperatorTypeCounterMap(
          MqlFilterOperator.Category operatorCategory) {
        return this.vectorSearchFilterOperatorCounterMap.get(operatorCategory);
      }

      @VisibleForTesting
      IndexMetrics.QueryingMetrics.QueryFeaturesMetrics getMetrics() {
        return IndexMetrics.QueryingMetrics.QueryFeaturesMetrics.create(
            this.collectorTypeCounterMap,
            this.operatorTypeCounterMap,
            this.scoreTypeCounterMap,
            this.textMatchCriteriaCounterMap,
            this.noDataSortPositionCounterMap,
            this.vectorSearchQueryTypeCounterMap,
            this.knnBetaFilterOperatorTypeCounterMap,
            this.searchVectorSearchFilterOperatorTypeCounterMap,
            this.synonymsCounter,
            this.textSynonymsCounter,
            this.textDeprecatedSynonymsCounter,
            this.phraseSynonymsCounter,
            this.fuzzyCounter,
            this.wildcardPathsCounter,
            this.highlightingCounter,
            this.concurrentCounter,
            this.returnStoredSourceCounter,
            this.sequenceTokenCounter,
            this.scoreDetailsCounter,
            this.explainCounter,
            this.sortCounter,
            this.trackingCounter,
            this.requireSequenceTokensCounter,
            this.returnScopeCounter,
            this.facetDrillSidewaysCounterMap.get(QueryOptimizationStatus.OPTIMIZABLE).count(),
            this.facetDrillSidewaysCounterMap.get(QueryOptimizationStatus.GENERIC).count());
      }

      @Override
      public void close() {
        this.metricsFactory.close();
      }
    }
  }

  /** An Updater for replication metrics. */
  public static class ReplicationMetricsUpdater implements Closeable {
    public static final String NAMESPACE = "replication";

    private final InitialSyncMetrics initialSyncMetrics;
    private final SteadyStateMetrics steadyStateMetrics;

    public ReplicationMetricsUpdater(
        PerIndexMetricsFactory metricsFactory, IndexDefinition indexDefinition) {
      var tags = extractTagsFromIndexDefinition(indexDefinition);
      this.initialSyncMetrics =
          new InitialSyncMetrics(
              metricsFactory.childMetricsFactory(InitialSyncMetrics.NAMESPACE), tags);
      this.steadyStateMetrics =
          new SteadyStateMetrics(
              metricsFactory.childMetricsFactory(SteadyStateMetrics.NAMESPACE), tags);
    }

    public InitialSyncMetrics getInitialSyncMetrics() {
      return this.initialSyncMetrics;
    }

    public SteadyStateMetrics getSteadyStateMetrics() {
      return this.steadyStateMetrics;
    }

    @Override
    public void close() {
      this.initialSyncMetrics.close();
      this.steadyStateMetrics.close();
    }

    // --- Inner Classes --- ///

    public static class InitialSyncMetrics implements Closeable {
      public static final String NAMESPACE = "initialSync";
      static final String COLL_SCAN_BATCH_TOTAL_APPLICABLE_DOCUMENTS =
          "collScan_batchTotalApplicableDocuments";
      static final String COLL_SCAN_BATCH_TOTAL_APPLICABLE_BYTES =
          "collScan_batchTotalApplicableBytes";
      static final String COLL_SCAN_BATCH_GET_MORE_TIMER = "collScan_getMoreDurations";
      static final String CHANGE_STREAM_BATCH_TOTAL_APPLICABLE_DOCUMENTS =
          "changeStream_batchTotalApplicableDocuments";
      static final String CHANGE_STREAM_BATCH_TOTAL_APPLICABLE_BYTES =
          "changeStream_batchTotalApplicableBytes";
      static final String CHANGE_STREAM_BATCH_GET_MORE_TIMER = "changeStream_getMoreDurations";
      static final String TOTAL_APPLICABLE_BYTES = "totalApplicableBytes";

      private final PerIndexMetricsFactory metricsFactory;
      private final Tags tags;
      private final DistributionSummary collScanBatchTotalApplicableDocuments;
      private final DistributionSummary collScanBatchTotalApplicableBytes;
      private final DistributionSummary changeStreamBatchTotalApplicableDocuments;
      private final DistributionSummary changeStreamBatchTotalApplicableBytes;
      private final Timer collScanBatchGetMoreTimer;
      private final Timer changeStreamBatchGetMoreTimer;
      private final Counter totalApplicableBytes;

      InitialSyncMetrics(PerIndexMetricsFactory metricsFactory, Tags tags) {
        this.metricsFactory = metricsFactory;
        this.tags = tags;
        this.collScanBatchTotalApplicableDocuments =
            this.metricsFactory.summary(
                COLL_SCAN_BATCH_TOTAL_APPLICABLE_DOCUMENTS,
                tags,
                PerIndexMetricsFactory.defaultSummaryPercentiles);

        this.collScanBatchTotalApplicableBytes =
            this.metricsFactory.summary(
                COLL_SCAN_BATCH_TOTAL_APPLICABLE_BYTES,
                tags,
                PerIndexMetricsFactory.defaultSummaryPercentiles);

        this.changeStreamBatchTotalApplicableDocuments =
            this.metricsFactory.summary(
                CHANGE_STREAM_BATCH_TOTAL_APPLICABLE_DOCUMENTS,
                tags,
                PerIndexMetricsFactory.defaultSummaryPercentiles);

        this.changeStreamBatchTotalApplicableBytes =
            this.metricsFactory.summary(
                CHANGE_STREAM_BATCH_TOTAL_APPLICABLE_BYTES,
                tags,
                PerIndexMetricsFactory.defaultSummaryPercentiles);

        this.collScanBatchGetMoreTimer =
            this.metricsFactory.timer(COLL_SCAN_BATCH_GET_MORE_TIMER, tags);

        this.changeStreamBatchGetMoreTimer =
            this.metricsFactory.timer(CHANGE_STREAM_BATCH_GET_MORE_TIMER, tags);

        this.totalApplicableBytes = this.metricsFactory.counter(TOTAL_APPLICABLE_BYTES, tags);
      }

      public DistributionSummary getCollectionScanBatchTotalApplicableDocuments() {
        return this.collScanBatchTotalApplicableDocuments;
      }

      public DistributionSummary getCollectionScanBatchTotalApplicableBytes() {
        return this.collScanBatchTotalApplicableBytes;
      }

      public Timer getCollectionScanBatchGetMoreTimer() {
        return this.collScanBatchGetMoreTimer;
      }

      public DistributionSummary getChangeStreamBatchTotalApplicableDocuments() {
        return this.changeStreamBatchTotalApplicableDocuments;
      }

      public DistributionSummary getChangeStreamBatchTotalApplicableBytes() {
        return this.changeStreamBatchTotalApplicableBytes;
      }

      public Timer getChangeStreamBatchGetMoreTimer() {
        return this.changeStreamBatchGetMoreTimer;
      }

      public Counter getTotalApplicableBytes() {
        return this.totalApplicableBytes;
      }

      public void reportIdKeyFieldType(String idFieldTypeName) {
        reportIdKeyFieldTypeGauge(this.metricsFactory, this, idFieldTypeName, this.tags);
      }

      @Override
      public void close() {
        this.metricsFactory.close();
      }
    }

    public static class SteadyStateMetrics implements Closeable {
      public static final String NAMESPACE = "steadyState";
      static final String BATCH_TOTAL_APPLICABLE_DOCUMENTS = "batchTotalApplicableDocuments";
      static final String BATCH_TOTAL_APPLICABLE_BYTES = "batchTotalApplicableBytes";
      static final String BATCH_GET_MORE_TIMER = "getMoreDurations";
      static final String BATCH_DECODING_TIMER = "decodingBatchDurations";

      private final PerIndexMetricsFactory metricsFactory;
      private final Tags tags;
      private final DistributionSummary batchTotalApplicableDocuments;
      private final DistributionSummary batchTotalApplicableBytes;
      private final Timer batchGetMoreTimer;
      private final Timer batchDecodingTimer;

      SteadyStateMetrics(PerIndexMetricsFactory metricsFactory, Tags tags) {
        this.metricsFactory = metricsFactory;
        this.tags = tags;
        this.batchTotalApplicableDocuments =
            this.metricsFactory.summary(
                BATCH_TOTAL_APPLICABLE_DOCUMENTS,
                tags,
                PerIndexMetricsFactory.defaultSummaryPercentiles);

        this.batchTotalApplicableBytes =
            this.metricsFactory.summary(
                BATCH_TOTAL_APPLICABLE_BYTES,
                tags,
                PerIndexMetricsFactory.defaultSummaryPercentiles);

        this.batchGetMoreTimer = this.metricsFactory.timer(BATCH_GET_MORE_TIMER, tags);
        this.batchDecodingTimer = this.metricsFactory.timer(BATCH_DECODING_TIMER, tags);
      }

      public DistributionSummary getBatchTotalApplicableDocuments() {
        return this.batchTotalApplicableDocuments;
      }

      public DistributionSummary getBatchTotalApplicableBytes() {
        return this.batchTotalApplicableBytes;
      }

      public Timer getBatchGetMoreTimer() {
        return this.batchGetMoreTimer;
      }

      public Timer getBatchDecodingTimer() {
        return this.batchDecodingTimer;
      }

      public void reportIdKeyFieldType(String idFieldTypeName) {
        reportIdKeyFieldTypeGauge(this.metricsFactory, this, idFieldTypeName, this.tags);
      }

      @Override
      public void close() {
        this.metricsFactory.close();
      }
    }

    /**
     * Records the BSON type name of the collection's {@code _id} field as an info gauge fixed at
     * 1.0, with the type encoded in the {@code bsonType} tag. Safe to call more than once —
     * Micrometer returns the same gauge on repeated calls with identical tags.
     */
    private static void reportIdKeyFieldTypeGauge(
        PerIndexMetricsFactory metricsFactory, Object owner, String idFieldTypeName, Tags tags) {
      metricsFactory.perIndexObjectValueGauge(
          "idFieldType", owner, ignored -> 1.0, tags.and("bsonType", idFieldTypeName));
    }

    private static Tags extractTagsFromIndexDefinition(IndexDefinition indexDefinition) {
      return Tags.of(INDEX_TYPE_TAG_NAME, getIndexTypeTag(indexDefinition).tagValue);
    }
  }

  public static class Builder {
    private final IndexDefinition indexDefinition;
    private final PerIndexMetricsFactory metricsFactory;
    private final IndexingMetricsUpdater indexingMetricsUpdater;
    private final QueryingMetricsUpdater queryingMetricsUpdater;
    private final ReplicationMetricsUpdater replicationMetricsUpdater;

    public Builder(
        IndexDefinition indexDefinition,
        PerIndexMetricsFactory metricsFactory,
        boolean isIndexFeatureVersionFourEnabled) {
      this.indexDefinition = indexDefinition;
      this.metricsFactory = metricsFactory;
      this.indexingMetricsUpdater =
          new IndexingMetricsUpdater(
              metricsFactory.childMetricsFactory(IndexingMetricsUpdater.NAMESPACE),
              indexDefinition.getType(),
              getIndexTypeTag(indexDefinition),
              indexDefinition.getParsedIndexFeatureVersion(),
              isIndexFeatureVersionFourEnabled);
      this.queryingMetricsUpdater =
          new QueryingMetricsUpdater(
              metricsFactory.childMetricsFactory(QueryingMetricsUpdater.NAMESPACE),
              getIndexTypeTag(indexDefinition),
              indexDefinition.getParsedIndexFeatureVersion(),
              isIndexFeatureVersionFourEnabled);
      this.replicationMetricsUpdater =
          new ReplicationMetricsUpdater(
              metricsFactory.childMetricsFactory(ReplicationMetricsUpdater.NAMESPACE),
              indexDefinition);
    }

    public IndexingMetricsUpdater getIndexingMetricsUpdater() {
      return this.indexingMetricsUpdater;
    }

    public QueryingMetricsUpdater getQueryingMetricsUpdater() {
      return this.queryingMetricsUpdater;
    }

    public ReplicationMetricsUpdater getReplicationMetricsUpdater() {
      return this.replicationMetricsUpdater;
    }

    public IndexMetricsUpdater build(IndexMetricValuesSupplier indexMetricValuesSupplier) {
      return new IndexMetricsUpdater(
          this.indexDefinition,
          this.indexingMetricsUpdater,
          this.queryingMetricsUpdater,
          this.replicationMetricsUpdater,
          indexMetricValuesSupplier,
          this.metricsFactory);
    }
  }
}
