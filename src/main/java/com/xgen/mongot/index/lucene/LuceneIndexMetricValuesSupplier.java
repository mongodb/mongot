package com.xgen.mongot.index.lucene;

import com.google.common.annotations.VisibleForTesting;
import com.xgen.mongot.index.IndexMetricValuesSupplier;
import com.xgen.mongot.index.IndexReader;
import com.xgen.mongot.index.WriterClosedException;
import com.xgen.mongot.index.status.IndexStatus;
import com.xgen.mongot.index.status.IndexStatus.StatusCode;
import com.xgen.mongot.metrics.CachedGauge;
import com.xgen.mongot.metrics.PerIndexMetricsFactory;
import com.xgen.mongot.util.FunctionalUtils;
import io.micrometer.core.instrument.Tags;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import java.util.stream.Stream;

/**
 * An abstract class that implements the {@link IndexMetricValuesSupplier} interface, providing
 * common logic for supplying metrics for a Lucene-based index.
 */
@VisibleForTesting
public abstract class LuceneIndexMetricValuesSupplier implements IndexMetricValuesSupplier {
  @VisibleForTesting static final String INDEX_PARTITIONS_NAMESPACE = "indexPartitions";

  private final Supplier<IndexStatus> indexStatusSupplier;
  private final IndexBackingStrategy indexBackingStrategy;
  private final IndexReader indexReader;
  private final LuceneIndexWriter indexWriter;
  private final List<PerIndexMetricsFactory> metricsFactories;

  /**
   * Cached index size in bytes, updated during async metrics collection. This allows the query hot
   * path to read the index size without triggering expensive directory walks.
   */
  private final AtomicLong cachedIndexSize = new AtomicLong(0);

  /** Create LuceneIndexMetricValuesSupplier */
  public LuceneIndexMetricValuesSupplier(
      Supplier<IndexStatus> indexStatusSupplier,
      IndexBackingStrategy indexBackingStrategy,
      IndexReader indexReader,
      LuceneIndexWriter luceneIndexWriter,
      PerIndexMetricsFactory metricsFactory,
      int indexFeatureVersion,
      boolean isIndexFeatureVersionFourEnabled) {
    this.indexStatusSupplier = indexStatusSupplier;
    this.indexBackingStrategy = indexBackingStrategy;
    this.indexReader = indexReader;
    this.indexWriter = luceneIndexWriter;
    this.metricsFactories = new ArrayList<>();
    Tags indexFeatureVersionTag =
        isIndexFeatureVersionFourEnabled
            ? Tags.of("indexFeatureVersion", String.valueOf(indexFeatureVersion))
            : Tags.empty();

    // Note these gauges are probed asynchronously and not atomically.
    // As we don't create our own metric namespace, it's not our responsibility to de-register them.
    // When the gauge computes index size, also cache it in the AtomicLong so that
    // getCachedIndexSize() can return the value without triggering expensive computation.
    // This allows the query hot path to read index size in O(1) time.
    metricsFactory.perIndexObjectValueGauge(
        MetricNames.INDEX_SIZE_BYTES,
        this,
        CachedGauge.of(
            supplier -> {
              long size = supplier.computeIndexSize();
              supplier.cachedIndexSize.set(size);
              return size;
            },
            Duration.ofMinutes(1)),
        getNumPartitionTag().and(indexFeatureVersionTag));
    metricsFactory.perIndexObjectValueGauge(
        MetricNames.REQUIRED_MEMORY,
        this,
        LuceneIndexMetricValuesSupplier::getRequiredMemoryForVectorData,
        getNumPartitionTag().and(indexFeatureVersionTag));
    metricsFactory.perIndexObjectValueGauge(
        MetricNames.LARGEST_INDEX_FILE_SIZE_BYTES,
        this,
        CachedGauge.of(
            LuceneIndexMetricValuesSupplier::getLargestIndexFileSize, Duration.ofMinutes(1)),
        getNumPartitionTag().and(indexFeatureVersionTag));
    metricsFactory.perIndexObjectValueGauge(
        MetricNames.NUMBER_OF_FILES_IN_INDEX,
        this,
        CachedGauge.of(LuceneIndexMetricValuesSupplier::getNumFilesInIndex, Duration.ofMinutes(1)),
        getNumPartitionTag().and(indexFeatureVersionTag));
    metricsFactory.perIndexObjectValueGauge(
        MetricNames.NUM_LUCENE_FIELDS,
        this,
        LuceneIndexMetricValuesSupplier::getNumFields,
        getNumPartitionTag().and(indexFeatureVersionTag));
    metricsFactory.perIndexObjectValueGauge(
        MetricNames.NUM_LUCENE_DOCS,
        this,
        LuceneIndexMetricValuesSupplier::getNumDocs,
        getNumPartitionTag().and(indexFeatureVersionTag));
    metricsFactory.perIndexObjectValueGauge(
        MetricNames.NUM_LUCENE_MAX_DOCS,
        this,
        LuceneIndexMetricValuesSupplier::getNumLuceneMaxDocs,
        getNumPartitionTag().and(indexFeatureVersionTag));
    Stream.of(IndexStatus.StatusCode.values())
        .forEach(
            statusCode ->
                metricsFactory.perIndexObjectValueGauge(
                    MetricNames.INDEX_STATUS_CODE,
                    this,
                    (supplier) -> supplier.getIndexCountInState(statusCode),
                    Tags.of("status", statusCode.name()).and(indexFeatureVersionTag)));

    if (this.indexWriter instanceof MultiLuceneIndexWriter multiLuceneIndexWriter) {
      // If there are multiple index-partitions, export following metrics for per index-partition.
      PerIndexMetricsFactory indexPartitionMetricsFactory =
          metricsFactory.childMetricsFactory(INDEX_PARTITIONS_NAMESPACE);
      this.metricsFactories.add(indexPartitionMetricsFactory);
      List<SingleLuceneIndexWriter> indexPartitionWriters =
          multiLuceneIndexWriter.getSingleLuceneIndexWriters();
      for (int i = 0; i < indexPartitionWriters.size(); ++i) {
        int indexPartitionId = i;
        LuceneIndexWriter indexPartitionWriter = indexPartitionWriters.get(indexPartitionId);
        indexPartitionMetricsFactory.perIndexObjectValueGauge(
            MetricNames.INDEX_SIZE_BYTES,
            this.indexBackingStrategy,
            strategy -> strategy.getIndexSizeForIndexPartition(indexPartitionId),
            getIndexPartitionTags(indexPartitionId).and(indexFeatureVersionTag));
        indexPartitionMetricsFactory.perIndexObjectValueGauge(
            MetricNames.NUM_LUCENE_FIELDS,
            indexPartitionWriter,
            indexWriter ->
                FunctionalUtils.getOrDefaultIfThrows(
                    indexWriter::getNumFields, WriterClosedException.class, 0),
            getIndexPartitionTags(indexPartitionId).and(indexFeatureVersionTag));
        indexPartitionMetricsFactory.perIndexObjectValueGauge(
            MetricNames.NUM_LUCENE_DOCS,
            indexPartitionWriter,
            indexWriter ->
                FunctionalUtils.getOrDefaultIfThrows(
                    indexWriter::getNumDocs, WriterClosedException.class, 0L),
            getIndexPartitionTags(indexPartitionId).and(indexFeatureVersionTag));
        indexPartitionMetricsFactory.perIndexObjectValueGauge(
            MetricNames.NUM_LUCENE_MAX_DOCS,
            indexPartitionWriter,
            indexWriter ->
                FunctionalUtils.getOrDefaultIfThrows(
                    indexWriter::getNumLuceneMaxDocs, WriterClosedException.class, 0L),
            getIndexPartitionTags(indexPartitionId).and(indexFeatureVersionTag));
        Stream.of(IndexStatus.StatusCode.values())
            .forEach(
                statusCode ->
                    indexPartitionMetricsFactory.perIndexObjectValueGauge(
                        MetricNames.INDEX_STATUS_CODE,
                        this,
                        (supplier) -> supplier.getIndexCountInState(statusCode),
                        getIndexPartitionTags(indexPartitionId)
                            .and(
                                Tags.of("status", statusCode.name()).and(indexFeatureVersionTag))));
        indexPartitionMetricsFactory.perIndexObjectValueGauge(
            MetricNames.REQUIRED_MEMORY,
            indexPartitionWriter,
            indexWriter ->
                FunctionalUtils.getOrDefaultIfThrows(
                    this.indexReader::getRequiredMemoryForVectorData, Exception.class, 0L),
            getIndexPartitionTags(indexPartitionId).and(indexFeatureVersionTag));
      }
    }
  }

  @Override
  public long computeIndexSize() {
    return this.indexBackingStrategy.getIndexSize();
  }

  @Override
  public long getCachedIndexSize() {
    return this.cachedIndexSize.get();
  }

  @Override
  public long getLargestIndexFileSize() {
    return this.indexBackingStrategy.getLargestIndexFileSize();
  }

  @Override
  public long getNumFilesInIndex() {
    return this.indexBackingStrategy.getNumFilesInIndex();
  }

  @Override
  public int getNumFields() {
    return FunctionalUtils.getOrDefaultIfThrows(
        this.indexWriter::getNumFields, WriterClosedException.class, 0);
  }

  @Override
  public IndexStatus getIndexStatus() {
    return this.indexStatusSupplier.get();
  }

  @Override
  public void close() {
    this.metricsFactories.forEach(PerIndexMetricsFactory::close);
  }

  @Override
  public long getRequiredMemoryForVectorData() {
    return FunctionalUtils.getOrDefaultIfThrows(
        this.indexReader::getRequiredMemoryForVectorData, Exception.class, 0L);
  }

  protected int getIndexCountInState(StatusCode state) {
    return (this.getIndexStatus().getStatusCode() == state) ? 1 : 0;
  }

  protected long getNumDocs() {
    return FunctionalUtils.getOrDefaultIfThrows(
        this.indexWriter::getNumDocs, WriterClosedException.class, 0L);
  }

  protected long getNumLuceneMaxDocs() {
    return FunctionalUtils.getOrDefaultIfThrows(
        this.indexWriter::getNumLuceneMaxDocs, WriterClosedException.class, 0L);
  }

  protected int getMaxLuceneMaxDocs() {
    return FunctionalUtils.getOrDefaultIfThrows(
        this.indexWriter::getMaxLuceneMaxDocs, WriterClosedException.class, 0);
  }

  private Tags getNumPartitionTag() {
    return Tags.of("numPartitions", String.valueOf(this.indexWriter.getNumWriters()));
  }

  @VisibleForTesting
  static Tags getIndexPartitionTags(int indexPartitionId) {
    return Tags.of("indexPartition", Integer.toHexString(indexPartitionId));
  }
}
