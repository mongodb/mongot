package com.xgen.mongot.index.lucene;

import com.google.common.annotations.VisibleForTesting;
import com.google.errorprone.annotations.Var;
import com.xgen.mongot.cursor.batch.BatchCursorOptions;
import com.xgen.mongot.cursor.batch.BatchSizeStrategy;
import com.xgen.mongot.cursor.batch.QueryCursorOptions;
import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlagRegistry;
import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlags;
import com.xgen.mongot.index.BatchProducer;
import com.xgen.mongot.index.CountMergingBatchProducer;
import com.xgen.mongot.index.CountMetaBatchProducer;
import com.xgen.mongot.index.MetaResults;
import com.xgen.mongot.index.MeteredBatchProducer;
import com.xgen.mongot.index.ReaderClosedException;
import com.xgen.mongot.index.SearchIndexReader;
import com.xgen.mongot.index.lucene.explain.tracing.Explain;
import com.xgen.mongot.index.lucene.facet.TokenFacetsStateCache;
import com.xgen.mongot.index.query.CollectorQuery;
import com.xgen.mongot.index.query.InvalidQueryException;
import com.xgen.mongot.index.query.OperatorQuery;
import com.xgen.mongot.index.query.Query;
import com.xgen.mongot.index.query.QueryOptimizationFlags;
import com.xgen.mongot.index.query.SearchQuery;
import com.xgen.mongot.index.query.VectorSearchQuery;
import com.xgen.mongot.util.Bytes;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.concurrent.NamedExecutorService;
import com.xgen.mongot.util.regex.RegexInterruptedException;
import com.xgen.mongot.util.regex.RegexLimitExceededException;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.IntStream;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.search.TaskExecutor;
import org.bson.BsonArray;

/**
 * Takes in multiple LuceneSearchIndexReaders, and dispatch the query properly to each individual
 * reader. This class is thread-safe, but concurrently calling query() and close() on it may cause
 * query() to fail, without crashing mongot or other damaging effects, since some of the underlying
 * readers may have been closed.
 */
public class MultiLuceneSearchIndexReader implements SearchIndexReader {
  private final List<LuceneSearchIndexReader> readers;
  private final boolean shouldCollectMultiPartitionEmptyBatchProducer;
  private final Optional<TaskExecutor> taskExecutor;

  MultiLuceneSearchIndexReader(
      List<LuceneSearchIndexReader> readers,
      DynamicFeatureFlagRegistry dynamicFeatureFlagRegistry,
      Optional<NamedExecutorService> concurrentSearchExecutor) {
    var dynamicFeatureFlag = DynamicFeatureFlags.COLLECT_MULTI_PARTITION_EMPTY_SEARCH_PRODUCER;
    this.shouldCollectMultiPartitionEmptyBatchProducer =
        dynamicFeatureFlagRegistry.evaluateClusterInvariant(
            dynamicFeatureFlag.getName(), dynamicFeatureFlag.getFallback());
    Check.checkState(
        readers.size() >= 2,
        "There must be >= 2 underlying readers to construct MultiLuceneSearchIndexReader.");
    this.readers = readers;
    this.taskExecutor = concurrentSearchExecutor.map(TaskExecutor::new);
  }

  @Override
  public SearchProducerAndMetaResults query(
      Query query,
      QueryCursorOptions queryCursorOptions,
      BatchSizeStrategy batchSizeStrategy,
      QueryOptimizationFlags queryOptimizationFlags)
      throws IOException, InvalidQueryException, InterruptedException {
    return switch (query) {
      case OperatorQuery operatorQuery ->
          vectorOrOperatorQuery(
              query, queryCursorOptions, batchSizeStrategy, queryOptimizationFlags);
      case VectorSearchQuery vectorSearchQuery ->
          vectorOrOperatorQuery(
              query, queryCursorOptions, batchSizeStrategy, QueryOptimizationFlags.DEFAULT_OPTIONS);
      case CollectorQuery collectorQuery ->
          collectorQuery(
              collectorQuery, queryCursorOptions, batchSizeStrategy, queryOptimizationFlags);
    };
  }

  private SearchProducerAndMetaResults vectorOrOperatorQuery(
      Query query,
      QueryCursorOptions queryCursorOptions,
      BatchSizeStrategy batchSizeStrategy,
      QueryOptimizationFlags queryOptimizationFlags)
      throws IOException, InvalidQueryException, InterruptedException {
    Check.checkState(
        query instanceof VectorSearchQuery || query instanceof OperatorQuery,
        "query here must be a vector or operator query");
    List<LuceneSearchBatchProducer> searchBatchProducers =
        java.util.Collections.synchronizedList(new ArrayList<>(this.readers.size()));
    List<BatchProducer> extraBatchProducersToClose =
        java.util.Collections.synchronizedList(new ArrayList<>(this.readers.size()));
    List<MetaResults> metaResultsList =
        java.util.Collections.synchronizedList(new ArrayList<>(this.readers.size()));

    executeOnAllPartitions(
        query,
        i ->
            executeVectorOrOperatorQueryOnPartition(
                i,
                query,
                queryCursorOptions,
                batchSizeStrategy,
                queryOptimizationFlags,
                searchBatchProducers,
                extraBatchProducersToClose,
                metaResultsList));
    var searchMergingBatchProducer =
        searchBatchProducers.isEmpty()
            ? new EmptySearchBatchProducer()
            : new SearchMergingBatchProducer(searchBatchProducers);
    BatchProducer searchBatchProducer =
        this.shouldCollectMultiPartitionEmptyBatchProducer
            ? wrapWithAdditionalClosers(searchMergingBatchProducer, extraBatchProducersToClose)
            : searchMergingBatchProducer;
    // Vector search query or operator query doesn't have facet at all.
    return new SearchProducerAndMetaResults(
        searchBatchProducer, MetaResults.mergeCountResult(metaResultsList));
  }

  private SearchProducerAndMetaResults collectorQuery(
      CollectorQuery query,
      QueryCursorOptions queryCursorOptions,
      BatchSizeStrategy batchSizeStrategy,
      QueryOptimizationFlags queryOptimizationFlags)
      throws IOException, InvalidQueryException, InterruptedException {
    // If the query is a collector search query with string facet, we have to issue an
    // intermediateQuery() to get _all_ instead of top string facet buckets.
    SearchProducerAndMetaProducer result =
        intermediateQuery(query, queryCursorOptions, batchSizeStrategy, queryOptimizationFlags);
    MetaResults metaResults =
        switch (result.metaBatchProducer) {
          case FacetMergingBatchProducer facetMergingBatchProducer ->
              facetMergingBatchProducer.getMetaResultsAndClose(query.count().type());
          case AggregationIntermediateBatchProducer aggregationBatchProducer ->
              aggregationBatchProducer.getMetaResultsAndClose(query.count().type());
          default ->
              throw new IllegalStateException(
                  "metaProducer from intermediateQuery() must be a FacetMergingBatchProducer or"
                      + " an AggregationIntermediateBatchProducer.");
        };
    return new SearchProducerAndMetaResults(result.searchBatchProducer, metaResults);
  }

  @Override
  public SearchProducerAndMetaProducer intermediateQuery(
      SearchQuery query,
      QueryCursorOptions queryCursorOptions,
      BatchSizeStrategy batchSizeStrategy,
      QueryOptimizationFlags queryOptimizationFlags)
      throws IOException, InvalidQueryException, InterruptedException {
    List<LuceneSearchBatchProducer> searchBatchProducers =
        java.util.Collections.synchronizedList(new ArrayList<>(this.readers.size()));
    List<BatchProducer> extraBatchProducersToClose =
        java.util.Collections.synchronizedList(new ArrayList<>(this.readers.size()));
    List<LuceneFacetCollectorMetaBatchProducer> facetBatchProducers =
        java.util.Collections.synchronizedList(new ArrayList<>(this.readers.size()));
    List<CountMetaBatchProducer> countBatchProducers =
        java.util.Collections.synchronizedList(new ArrayList<>(this.readers.size()));
    List<AggregationIntermediateBatchProducer> aggregationBatchProducers =
        java.util.Collections.synchronizedList(new ArrayList<>(this.readers.size()));

    executeOnAllPartitions(
        query,
        i ->
            executeIntermediateQueryOnPartition(
                i,
                query,
                queryCursorOptions,
                batchSizeStrategy,
                queryOptimizationFlags,
                searchBatchProducers,
                extraBatchProducersToClose,
                facetBatchProducers,
                countBatchProducers,
                aggregationBatchProducers));
    var searchMergingBatchProducer =
        searchBatchProducers.isEmpty()
            ? new EmptySearchBatchProducer()
            : new SearchMergingBatchProducer(searchBatchProducers);
    BatchProducer searchBatchProducer =
        this.shouldCollectMultiPartitionEmptyBatchProducer
            ? wrapWithAdditionalClosers(searchMergingBatchProducer, extraBatchProducersToClose)
            : searchMergingBatchProducer;
    BatchProducer mergedMetaProducer;
    if (!facetBatchProducers.isEmpty()) {
      mergedMetaProducer = FacetMergingBatchProducer.create(facetBatchProducers);
    } else if (!aggregationBatchProducers.isEmpty()) {
      // Sequential fold after the concurrent per-partition fan-out above has joined: the merge
      // (like AggregationResultBuilder) is not thread-safe, so partitions must not fold directly.
      mergedMetaProducer =
          AggregationIntermediateBatchProducer.merge(
              aggregationBatchProducers, this.readers.get(0).getSearchAggregationMetricsReporter());
    } else if (!countBatchProducers.isEmpty()) {
      mergedMetaProducer = new CountMergingBatchProducer(countBatchProducers);
    } else {
      return Check.unreachable(
          "Either facetBatchProducers, aggregationBatchProducers or countBatchProducers must be"
              + " non-empty");
    }
    return new SearchProducerAndMetaProducer(searchBatchProducer, mergedMetaProducer);
  }

  @Override
  public long getNumEmbeddedRootDocuments() throws IOException, ReaderClosedException {
    @Var long sum = 0;
    for (var reader : this.readers) {
      sum += reader.getNumEmbeddedRootDocuments();
    }
    return sum;
  }

  @Override
  public List<FieldInfos> getFieldInfos() throws IOException, ReaderClosedException {
    List<FieldInfos> fieldInfos = new ArrayList<>();
    for (var reader : this.readers) {
      fieldInfos.addAll(reader.getFieldInfos());
    }

    return fieldInfos;
  }

  @Override
  public void refresh() throws IOException, ReaderClosedException {
    for (var reader : this.readers) {
      reader.refresh();
    }
  }

  @Override
  public void open() {
    for (var reader : this.readers) {
      reader.open();
    }
  }

  @Override
  public void close() {
    for (var reader : this.readers) {
      reader.close();
    }
  }

  @VisibleForTesting
  int numUnderlyingReaders() {
    return this.readers.size();
  }

  /**
   * Returns the maximum cardinality across all cached stringFacet fields across all underlying
   * index partitions.
   *
   * @return the maximum cardinality across all partitions
   */
  public int getMaxStringFacetCardinality() {
    return this.readers.stream()
        .mapToInt(LuceneSearchIndexReader::getMaxStringFacetCardinality)
        .max()
        .orElse(0);
  }

  /**
   * Summed across partitions: each partition reader holds its own token-facets cache and is
   * acquired once.
   */
  @Override
  public TokenFacetsStateCache.Snapshot getTokenFacetsStateCacheSnapshot() {
    return this.readers.stream()
        .map(LuceneSearchIndexReader::getTokenFacetsStateCacheSnapshot)
        .reduce(TokenFacetsStateCache.Snapshot.EMPTY, TokenFacetsStateCache.Snapshot::plus);
  }

  /**
   * The merge path retains only {@link LuceneSearchBatchProducer} instances, so any other producer
   * types (e.g., {@link EmptySearchBatchProducer}) must be tracked separately to ensure they are
   * properly closed.
   */
  private void collectSearchBatchProducer(
      BatchProducer producer,
      List<LuceneSearchBatchProducer> searchBatchProducers,
      List<BatchProducer> extraBatchProducersToClose) {
    Objects.requireNonNull(producer, "producer");
    BatchProducer unwrapped =
        producer instanceof MeteredBatchProducer metered ? metered.unwrap() : producer;
    if (unwrapped instanceof LuceneSearchBatchProducer luceneSearchBatchProducer) {
      searchBatchProducers.add(luceneSearchBatchProducer);
    } else {
      if (this.shouldCollectMultiPartitionEmptyBatchProducer) {
        extraBatchProducersToClose.add(producer);
      }
    }
  }

  // Wrap batch producer in ClosingBatchProducer, which records descendants of the BatchProducers
  // of itself so that all its descendants will be closed when itself is closed.
  private static BatchProducer wrapWithAdditionalClosers(
      BatchProducer primary, List<BatchProducer> extraBatchProducersToClose) {
    if (extraBatchProducersToClose.isEmpty()) {
      return primary;
    }
    return new ClosingBatchProducer(primary, extraBatchProducersToClose);
  }

  private static final class ClosingBatchProducer implements BatchProducer {
    private final BatchProducer primary;
    private final List<BatchProducer> extraBatchProducersToClose;

    private ClosingBatchProducer(
        BatchProducer primary, List<BatchProducer> extraBatchProducersToClose) {
      this.primary = primary;
      this.extraBatchProducersToClose = List.copyOf(extraBatchProducersToClose);
    }

    @Override
    public void execute(Bytes sizeLimit, BatchCursorOptions queryCursorOptions) throws IOException {
      this.primary.execute(sizeLimit, queryCursorOptions);
    }

    @Override
    public BsonArray getNextBatch(Bytes sizeLimit) throws IOException {
      return this.primary.getNextBatch(sizeLimit);
    }

    @Override
    public boolean isExhausted() {
      return this.primary.isExhausted();
    }

    @Override
    public void close() throws IOException {
      @Var IOException first = null;
      try {
        this.primary.close();
      } catch (IOException e) {
        first = e;
      }
      for (var producer : this.extraBatchProducersToClose) {
        try {
          producer.close();
        } catch (IOException e) {
          if (first == null) {
            first = e;
          } else {
            first.addSuppressed(e);
          }
        }
      }
      if (first != null) {
        throw first;
      }
    }
  }

  @FunctionalInterface
  private interface PartitionExecutor {
    void execute(int partitionIndex)
        throws IOException, InvalidQueryException, InterruptedException;
  }

  private void executeOnAllPartitions(Query query, PartitionExecutor partitionExecutor)
      throws IOException, InvalidQueryException, InterruptedException {
    if (query.concurrent() && this.taskExecutor.isPresent()) {
      Context parentContext = Context.current();
      Queue<Exception> exceptions = new ConcurrentLinkedQueue<>();
      List<Callable<Void>> tasks =
          IntStream.range(0, this.readers.size())
              .<Callable<Void>>mapToObj(
                  i ->
                      () -> {
                        try (Scope ignored = parentContext.makeCurrent()) {
                          partitionExecutor.execute(i);
                        } catch (Exception e) {
                          exceptions.add(e);
                        }
                        return null;
                      })
              .toList();
      this.taskExecutor.get().invokeAll(tasks);
      if (!exceptions.isEmpty()) {
        Exception first = exceptions.poll();
        while (!exceptions.isEmpty()) {
          first.addSuppressed(exceptions.poll());
        }
        rethrowException(first);
      }
    } else {
      for (int i = 0; i < this.readers.size(); i++) {
        partitionExecutor.execute(i);
      }
    }
  }

  private void executeVectorOrOperatorQueryOnPartition(
      int i,
      Query query,
      QueryCursorOptions queryCursorOptions,
      BatchSizeStrategy batchSizeStrategy,
      QueryOptimizationFlags queryOptimizationFlags,
      List<LuceneSearchBatchProducer> searchBatchProducers,
      List<BatchProducer> extraBatchProducersToClose,
      List<MetaResults> metaResultsList)
      throws IOException, InvalidQueryException, InterruptedException {
    try (var indexPartitionResourceManager = Explain.maybeEnterIndexPartitionQueryContext(i)) {
      var result =
          this.readers
              .get(i)
              .query(query, queryCursorOptions, batchSizeStrategy, queryOptimizationFlags);

      collectSearchBatchProducer(
          result.searchBatchProducer, searchBatchProducers, extraBatchProducersToClose);

      metaResultsList.add(result.metaResults);
    }
  }

  private void executeIntermediateQueryOnPartition(
      int i,
      SearchQuery query,
      QueryCursorOptions queryCursorOptions,
      BatchSizeStrategy batchSizeStrategy,
      QueryOptimizationFlags queryOptimizationFlags,
      List<LuceneSearchBatchProducer> searchBatchProducers,
      List<BatchProducer> extraBatchProducersToClose,
      List<LuceneFacetCollectorMetaBatchProducer> facetBatchProducers,
      List<CountMetaBatchProducer> countBatchProducers,
      List<AggregationIntermediateBatchProducer> aggregationBatchProducers)
      throws IOException, InvalidQueryException, InterruptedException {
    try (var indexPartitionResourceManager = Explain.maybeEnterIndexPartitionQueryContext(i)) {
      var reader = this.readers.get(i);

      var result =
          reader.intermediateQuery(
              query, queryCursorOptions, batchSizeStrategy, queryOptimizationFlags);
      collectSearchBatchProducer(
          result.searchBatchProducer, searchBatchProducers, extraBatchProducersToClose);
      if (result.metaBatchProducer
          instanceof LuceneFacetCollectorMetaBatchProducer facetMetaBatchProducer) {
        facetBatchProducers.add(facetMetaBatchProducer);
      } else if (result.metaBatchProducer
          instanceof CountMetaBatchProducer countMetaBatchProducer) {
        countBatchProducers.add(countMetaBatchProducer);
      } else if (result.metaBatchProducer
          instanceof AggregationIntermediateBatchProducer aggregationBatchProducer) {
        aggregationBatchProducers.add(aggregationBatchProducer);
      } else {
        throw new IllegalStateException(
            String.format(
                "Cannot handle result.metaBatchProducer's type %s",
                result.metaBatchProducer.getClass()));
      }
    }
  }

  private static void rethrowException(Exception e)
      throws IOException, InvalidQueryException, InterruptedException {
    switch (e) {
      case IOException ioException -> throw ioException;
      case InvalidQueryException invalidQueryException -> throw invalidQueryException;
      case InterruptedException interruptedException -> throw interruptedException;
      // Command handlers distinguish analyzer limits and cancellation from internal failures.
      case RegexLimitExceededException regexLimitExceededException ->
          throw regexLimitExceededException;
      case RegexInterruptedException regexInterruptedException -> throw regexInterruptedException;
      default -> throw new RuntimeException(e);
    }
  }
}
