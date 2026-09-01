package com.xgen.mongot.index;

import com.google.common.annotations.VisibleForTesting;
import com.xgen.mongot.cursor.batch.BatchSizeStrategy;
import com.xgen.mongot.cursor.batch.QueryCursorOptions;
import com.xgen.mongot.index.query.InvalidQueryException;
import com.xgen.mongot.index.query.MaterializedVectorSearchQuery;
import com.xgen.mongot.index.query.QueryExecutionContext;
import com.xgen.mongot.index.query.QueryOptimizationFlags;
import java.io.IOException;
import org.bson.BsonArray;

// TODO(CLOUDP-210212): vector search: expand this to record more detailed query stats
public class MeteredVectorIndexReader implements VectorIndexReader {

  private final VectorIndexReader indexReader;
  private final IndexMetricsUpdater.QueryingMetricsUpdater metricsUpdater;
  private final QueryMetricsRecorder queryMetricsRecorder;

  public MeteredVectorIndexReader(
      VectorIndexReader indexReader, IndexMetricsUpdater.QueryingMetricsUpdater metricsUpdater) {
    this.indexReader = indexReader;
    this.metricsUpdater = metricsUpdater;
    this.queryMetricsRecorder =
        new QueryMetricsRecorder(
            metricsUpdater.getQueryFeaturesMetricsUpdater(),
            metricsUpdater.getSearchAggregationMetricsReporter());
  }

  @Override
  public BsonArray query(
      MaterializedVectorSearchQuery materializedQuery, QueryExecutionContext context)
      throws ReaderClosedException, IOException, InvalidQueryException {
    this.metricsUpdater.getTotalQueryCounter().increment();
    try {
      BsonArray result = this.indexReader.query(materializedQuery, context);
      this.queryMetricsRecorder.record(materializedQuery.vectorSearchQuery());
      return result;
    } catch (Exception e) {
      this.metricsUpdater.handleQueryException(
          e,
          "from VectorIndexReader.query, shape="
              + QueryShape.of(materializedQuery.vectorSearchQuery()));
      throw e;
    }
  }

  @Override
  public VectorProducerAndMetaResults query(
      MaterializedVectorSearchQuery query,
      QueryExecutionContext context,
      QueryCursorOptions queryCursorOptions,
      BatchSizeStrategy batchSizeStrategy,
      QueryOptimizationFlags queryOptimizationFlags)
      throws InvalidQueryException, IOException, InterruptedException, ReaderClosedException {
    this.metricsUpdater.getTotalQueryCounter().increment();
    try {
      VectorProducerAndMetaResults result =
          this.indexReader.query(
              query, context, queryCursorOptions, batchSizeStrategy, queryOptimizationFlags);
      this.queryMetricsRecorder.record(query.vectorSearchQuery());
      return result;
    } catch (Exception e) {
      this.metricsUpdater.handleQueryException(
          e, "from VectorIndexReader.query, shape=" + QueryShape.of(query.vectorSearchQuery()));
      throw e;
    }
  }

  @Override
  public void refresh() throws IOException, ReaderClosedException {
    this.indexReader.refresh();
  }

  @Override
  public void open() {
    this.indexReader.open();
  }

  @Override
  public void close() {
    this.indexReader.close();
  }

  @VisibleForTesting
  public VectorIndexReader unwrap() {
    return this.indexReader;
  }

  @Override
  public long getNumEmbeddedRootDocuments() throws IOException, ReaderClosedException {
    return this.indexReader.getNumEmbeddedRootDocuments();
  }
}
