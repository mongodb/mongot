package com.xgen.mongot.index;

import com.google.common.annotations.VisibleForTesting;
import com.xgen.mongot.cursor.batch.BatchSizeStrategy;
import com.xgen.mongot.cursor.batch.QueryCursorOptions;
import com.xgen.mongot.index.query.InvalidQueryException;
import com.xgen.mongot.index.query.Query;
import com.xgen.mongot.index.query.QueryOptimizationFlags;
import com.xgen.mongot.index.query.SearchQuery;
import java.io.IOException;
import java.util.List;
import java.util.function.BooleanSupplier;
import org.apache.lucene.index.FieldInfos;

public class MeteredSearchIndexReader implements SearchIndexReader {
  private final SearchIndexReader indexReader;
  private final IndexMetricsUpdater.QueryingMetricsUpdater queryingMetricsUpdater;
  private final QueryMetricsRecorder queryMetricsRecorder;
  private final BooleanSupplier recordTotalFacetBucketsMetric;

  public MeteredSearchIndexReader(
      SearchIndexReader indexReader,
      IndexMetricsUpdater.QueryingMetricsUpdater queryingMetricsUpdater,
      BooleanSupplier recordTotalFacetBucketsMetric) {
    this.indexReader = indexReader;
    this.queryingMetricsUpdater = queryingMetricsUpdater;
    this.recordTotalFacetBucketsMetric = recordTotalFacetBucketsMetric;
    this.queryMetricsRecorder =
        new QueryMetricsRecorder(queryingMetricsUpdater.getQueryFeaturesMetricsUpdater());
  }

  @Override
  public SearchProducerAndMetaResults query(
      Query query,
      QueryCursorOptions queryCursorOptions,
      BatchSizeStrategy batchSizeStrategy,
      QueryOptimizationFlags queryOptimizationFlags)
      throws IOException, InvalidQueryException, InterruptedException {
    this.queryingMetricsUpdater.getTotalQueryCounter().increment();
    try {
      SearchProducerAndMetaResults result =
          this.indexReader.query(
              query, queryCursorOptions, batchSizeStrategy, queryOptimizationFlags);
      this.queryingMetricsUpdater.recordTotalFacetBucketsIfApplicable(
          query, this.recordTotalFacetBucketsMetric);
      this.queryMetricsRecorder.record(query, queryCursorOptions);
      return result;
    } catch (Exception e) {
      this.queryingMetricsUpdater.handleQueryException(e, query.toBson().toString());
      throw e;
    }
  }

  @Override
  public SearchProducerAndMetaProducer intermediateQuery(
      SearchQuery query,
      QueryCursorOptions queryCursorOptions,
      BatchSizeStrategy batchSizeStrategy,
      QueryOptimizationFlags queryOptimizationFlags)
      throws IOException, InvalidQueryException, InterruptedException {
    this.queryingMetricsUpdater.getTotalQueryCounter().increment();
    try {
      SearchProducerAndMetaProducer result =
          this.indexReader.intermediateQuery(
              query, queryCursorOptions, batchSizeStrategy, queryOptimizationFlags);
      this.queryingMetricsUpdater.recordTotalFacetBucketsIfApplicable(
          query, this.recordTotalFacetBucketsMetric);
      this.queryMetricsRecorder.record(query, queryCursorOptions);
      return result;
    } catch (Exception e) {
      this.queryingMetricsUpdater.handleQueryException(e, query.toBson().toString());
      throw e;
    }
  }

  @Override
  public long getNumEmbeddedRootDocuments() throws IOException, ReaderClosedException {
    return this.indexReader.getNumEmbeddedRootDocuments();
  }

  @Override
  public List<FieldInfos> getFieldInfos() throws IOException, ReaderClosedException {
    return this.indexReader.getFieldInfos();
  }

  @Override
  public void refresh() throws IOException, ReaderClosedException {
    this.indexReader.refresh();
  }

  @Override
  public void close() {
    this.indexReader.close();
  }

  @Override
  public void open() {
    this.indexReader.open();
  }

  @VisibleForTesting
  public SearchIndexReader unwrap() {
    return this.indexReader;
  }
}
