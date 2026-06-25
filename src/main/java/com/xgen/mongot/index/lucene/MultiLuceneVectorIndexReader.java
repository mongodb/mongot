package com.xgen.mongot.index.lucene;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.xgen.mongot.cursor.batch.BatchSizeStrategy;
import com.xgen.mongot.cursor.batch.QueryCursorOptions;
import com.xgen.mongot.index.IndexMetricsUpdater;
import com.xgen.mongot.index.MetaResults;
import com.xgen.mongot.index.ReaderClosedException;
import com.xgen.mongot.index.VectorIndexReader;
import com.xgen.mongot.index.VectorSearchResult;
import com.xgen.mongot.index.lucene.explain.tracing.Explain;
import com.xgen.mongot.index.query.DeadlineExceededException;
import com.xgen.mongot.index.query.InvalidQueryException;
import com.xgen.mongot.index.query.MaterializedVectorSearchQuery;
import com.xgen.mongot.index.query.QueryExecutionContext;
import com.xgen.mongot.index.query.QueryOptimizationFlags;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.concurrent.NamedExecutorService;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.IntStream;
import org.apache.lucene.search.TaskExecutor;
import org.bson.BsonArray;

/**
 * Takes in multiple LuceneVectorIndexReaders, sends the query to each individual reader and
 * aggregates the search results. This class is thread-safe, but concurrently calling query() and
 * close() on it may cause query() to fail, without crashing mongot or other damaging effects, since
 * some of the underlying readers may have been closed.
 */
public class MultiLuceneVectorIndexReader implements VectorIndexReader {
  private final List<LuceneVectorIndexReader> readers;
  private final IndexMetricsUpdater.QueryingMetricsUpdater metricsUpdater;
  private final Optional<TaskExecutor> taskExecutor;

  MultiLuceneVectorIndexReader(
      List<LuceneVectorIndexReader> readers,
      IndexMetricsUpdater.QueryingMetricsUpdater metricsUpdater,
      Optional<NamedExecutorService> concurrentSearchExecutor) {
    Check.checkState(
        readers.size() >= 2,
        "There must be >= 2 underlying readers to construct MultiLuceneVectorIndexReader.");
    this.readers = readers;
    this.metricsUpdater = metricsUpdater;
    this.taskExecutor = concurrentSearchExecutor.map(TaskExecutor::new);
  }

  // If this is slow, we can explore querying each underlying LuceneVectorIndexReader with a
  // lowered limit.
  @Override
  public BsonArray query(MaterializedVectorSearchQuery query, QueryExecutionContext context)
      throws ReaderClosedException, IOException, InvalidQueryException {
    List<Iterator<VectorSearchResult>> vectorSearchResultIterators =
        java.util.Collections.synchronizedList(new ArrayList<>(this.readers.size()));

    if (query.concurrent() && this.taskExecutor.isPresent()) {
      // Each partition reader also enforces the deadline on its own IndexSearcher and will throw if
      // it elapses mid-query, this check avoids submitting any partition work once it has passed.
      DeadlineQueryTimeout.throwIfExceeded(
          DeadlineQueryTimeout.fromDeadline(context.deadlineTimestampMs()));
      Context parentContext = Context.current();
      Queue<Exception> exceptions = new ConcurrentLinkedQueue<>();
      List<Callable<Void>> tasks =
          IntStream.range(0, this.readers.size())
              .<Callable<Void>>mapToObj(
                  i ->
                      () -> {
                        try (Scope ignored = parentContext.makeCurrent()) {
                          executeVectorQueryOnPartition(
                              i, query, context, vectorSearchResultIterators);
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
      // Each partition reader sets the timeout on its IndexSearcher internally; this check throws
      // before starting a new partition query once the deadline has passed, so we surface a timeout
      // to the user instead of silently returning a partial merge of the partitions queried so far.
      var timeout = DeadlineQueryTimeout.fromDeadline(context.deadlineTimestampMs());
      for (int i = 0; i < this.readers.size(); i++) {
        DeadlineQueryTimeout.throwIfExceeded(timeout);
        executeVectorQueryOnPartition(i, query, context, vectorSearchResultIterators);
      }
    }

    List<VectorSearchResult> mergeVectorSearchResult =
        mergeVectorSearchResults(vectorSearchResultIterators, query.limit());

    return LuceneVectorIndexReader.getBsonArray(mergeVectorSearchResult, this.metricsUpdater);
  }

  @Override
  public VectorProducerAndMetaResults query(
      MaterializedVectorSearchQuery materializedVectorSearchQuery,
      QueryExecutionContext context,
      QueryCursorOptions queryCursorOptions,
      BatchSizeStrategy batchSizeStrategy,
      QueryOptimizationFlags queryOptimizationFlags)
      throws InvalidQueryException, IOException, InterruptedException, ReaderClosedException {
    List<Iterator<VectorSearchResult>> vectorSearchResultIterators =
        new ArrayList<>(this.readers.size());
    // Each partition reader sets the timeout on its IndexSearcher internally; this check throws
    // before starting a new partition query once the deadline has passed, so we surface a timeout
    // to the user instead of silently returning a partial merge of the partitions queried so far.
    var timeout = DeadlineQueryTimeout.fromDeadline(context.deadlineTimestampMs());
    for (int i = 0; i < this.readers.size(); i++) {
      DeadlineQueryTimeout.throwIfExceeded(timeout);
      try (var indexPartitionResourceManager = Explain.maybeEnterIndexPartitionQueryContext(i)) {
        var reader = this.readers.get(i);
        vectorSearchResultIterators.add(
            reader.queryResults(materializedVectorSearchQuery, context).iterator());
      }
    }

    List<VectorSearchResult> mergeVectorSearchResult =
        mergeVectorSearchResults(
            vectorSearchResultIterators, materializedVectorSearchQuery.limit());

    return new VectorProducerAndMetaResults(
        new LuceneVectorSearchBatchProducer(
            mergeVectorSearchResult, this.metricsUpdater, batchSizeStrategy),
        MetaResults.EMPTY);
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

  private List<VectorSearchResult> mergeVectorSearchResults(
      List<Iterator<VectorSearchResult>> vectorSearchResultIterators, int limit) {
    var merged =
        Iterators.mergeSorted(
            vectorSearchResultIterators, VectorSearchResult.VECTOR_SEARCH_RESULT_COMPARATOR);
    return Lists.newArrayList(Iterators.limit(merged, limit));
  }

  private void executeVectorQueryOnPartition(
      int i,
      MaterializedVectorSearchQuery query,
      QueryExecutionContext context,
      List<Iterator<VectorSearchResult>> vectorSearchResultIterators)
      throws ReaderClosedException, IOException, InvalidQueryException {
    try (var indexPartitionResourceManager = Explain.maybeEnterIndexPartitionQueryContext(i)) {
      var reader = this.readers.get(i);
      vectorSearchResultIterators.add(reader.queryResults(query, context).iterator());
    }
  }

  private static void rethrowException(Exception e)
      throws ReaderClosedException, IOException, InvalidQueryException {
    switch (e) {
      case IOException ioException -> throw ioException;
      case ReaderClosedException readerClosedException -> throw readerClosedException;
      case InvalidQueryException invalidQueryException -> throw invalidQueryException;
      // Preserve the user-facing timeout type/message instead of wrapping it in a RuntimeException.
      case DeadlineExceededException deadlineExceededException -> throw deadlineExceededException;
      default -> throw new RuntimeException(e);
    }
  }
}
