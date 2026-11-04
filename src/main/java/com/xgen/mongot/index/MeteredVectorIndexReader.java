package com.xgen.mongot.index;

import com.google.common.annotations.VisibleForTesting;
import com.xgen.mongot.cursor.batch.BatchSizeStrategy;
import com.xgen.mongot.cursor.batch.QueryCursorOptions;
import com.xgen.mongot.index.definition.FieldDefinitionResolver;
import com.xgen.mongot.index.definition.quantization.VectorQuantization;
import com.xgen.mongot.index.query.InvalidQueryException;
import com.xgen.mongot.index.query.MaterializedVectorSearchQuery;
import com.xgen.mongot.index.query.QueryExecutionContext;
import com.xgen.mongot.index.query.QueryOptimizationFlags;
import com.xgen.mongot.index.query.operators.ApproximateVectorSearchCriteria;
import com.xgen.mongot.util.FieldPath;
import java.io.IOException;
import java.util.Optional;
import org.bson.BsonArray;

// TODO(CLOUDP-210212): vector search: expand this to record more detailed query stats
public class MeteredVectorIndexReader implements VectorIndexReader {

  private final VectorIndexReader indexReader;
  private final IndexMetricsUpdater.QueryingMetricsUpdater metricsUpdater;
  private final QueryMetricsRecorder queryMetricsRecorder;
  private final Optional<FieldDefinitionResolver> fieldDefinitionResolver;

  public MeteredVectorIndexReader(
      VectorIndexReader indexReader, IndexMetricsUpdater.QueryingMetricsUpdater metricsUpdater) {
    this(indexReader, metricsUpdater, Optional.empty());
  }

  /**
   * Creates a reader that records the effective auto numCandidates per query.
   *
   * @param fieldDefinitionResolver resolves the queried field's quantization so the query-level
   *     auto-numCandidates histogram records the effective value actually used. Engines that apply
   *     the binary auto ratio (Lucene) pass a resolver; engines that do not (vectorlite) pass
   *     {@link Optional#empty()} and record the default-ratio value.
   */
  public MeteredVectorIndexReader(
      VectorIndexReader indexReader,
      IndexMetricsUpdater.QueryingMetricsUpdater metricsUpdater,
      Optional<FieldDefinitionResolver> fieldDefinitionResolver) {
    this.indexReader = indexReader;
    this.metricsUpdater = metricsUpdater;
    this.fieldDefinitionResolver = fieldDefinitionResolver;
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
      recordAutoNumCandidatesMetrics(materializedQuery);
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
      recordAutoNumCandidatesMetrics(query);
      return result;
    } catch (Exception e) {
      this.metricsUpdater.handleQueryException(
          e, "from VectorIndexReader.query, shape=" + QueryShape.of(query.vectorSearchQuery()));
      throw e;
    }
  }

  /**
   * Records auto-numCandidates metrics once per user query. The underlying query factory runs once
   * per index partition (see {@code MultiLuceneVectorIndexReader}), so recording there would scale
   * the counter and histogram with partition count instead of user queries. This wrapper is the
   * single query-level metering point, so recording here keeps both metrics query-level. Called
   * only after a successful query, so queries rejected during resolution (e.g. limit above {@code
   * MAX_NUM_CANDIDATES}) are not counted.
   */
  private void recordAutoNumCandidatesMetrics(MaterializedVectorSearchQuery materializedQuery) {
    if (materializedQuery.vectorSearchQuery().criteria()
            instanceof ApproximateVectorSearchCriteria approximate
        && approximate.isAutoNumCandidates()) {
      this.metricsUpdater.getAutoNumCandidatesCounter().increment();
      boolean binaryQuantized =
          ApproximateVectorSearchCriteria.isBinaryQuantizedSearch(
              resolveQuantization(approximate.path()), materializedQuery.vector());
      this.metricsUpdater
          .getAutoNumCandidatesValues()
          .record(approximate.resolveNumCandidates(binaryQuantized));
    }
  }

  /**
   * Resolves the queried field's quantization, or {@link VectorQuantization#NONE} when no resolver
   * was supplied (engines that do not apply the binary auto ratio).
   */
  private VectorQuantization resolveQuantization(FieldPath path) {
    return this.fieldDefinitionResolver
        .map(resolver -> resolver.resolveQuantization(path))
        .orElse(VectorQuantization.NONE);
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
