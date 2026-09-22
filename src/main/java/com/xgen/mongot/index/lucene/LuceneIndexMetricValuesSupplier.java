package com.xgen.mongot.index.lucene;

import com.google.common.annotations.VisibleForTesting;
import com.xgen.mongot.index.IndexMetricValuesSupplier;
import com.xgen.mongot.index.IndexTypeData;
import com.xgen.mongot.index.WriterClosedException;
import com.xgen.mongot.index.lucene.backing.IndexBackingStrategy;
import com.xgen.mongot.index.lucene.writer.LuceneIndexWriter;
import com.xgen.mongot.index.lucene.writer.MultiLuceneIndexWriter;
import com.xgen.mongot.index.lucene.writer.SingleLuceneIndexWriter;
import com.xgen.mongot.index.status.IndexStatus;
import com.xgen.mongot.index.status.IndexStatus.StatusCode;
import com.xgen.mongot.metrics.CachedGauge;
import com.xgen.mongot.metrics.PerIndexMetricsFactory;
import com.xgen.mongot.util.FunctionalUtils;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import java.io.IOException;
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
  private final LuceneIndexWriter indexWriter;
  private final List<PerIndexMetricsFactory> metricsFactories;

  /**
   * Cached index size in bytes, updated during async metrics collection. This allows the query hot
   * path to read the index size without triggering expensive directory walks.
   */
  private final AtomicLong cachedIndexSize;

  /**
   * Private constructor - does not register gauges. Subclasses should use static factory methods to
   * construct instances and register gauges after construction is complete.
   */
  protected LuceneIndexMetricValuesSupplier(
      Supplier<IndexStatus> indexStatusSupplier,
      IndexBackingStrategy indexBackingStrategy,
      LuceneIndexWriter luceneIndexWriter) {
    this.indexStatusSupplier = indexStatusSupplier;
    this.indexBackingStrategy = indexBackingStrategy;
    this.indexWriter = luceneIndexWriter;
    this.metricsFactories = new ArrayList<>();
    this.cachedIndexSize = new AtomicLong(indexBackingStrategy.getDiskStats().totalFileByteSize());
  }

  /**
   * Registers common gauges for all Lucene index types. Should be called by subclass static factory
   * methods after construction is complete to avoid this-escape.
   */
  protected final void registerCommonGauges(
      PerIndexMetricsFactory metricsFactory,
      int indexFeatureVersion,
      boolean isIndexFeatureVersionFourEnabled,
      IndexTypeData.IndexTypeTag indexTypeTag) {
    Tags indexFeatureVersionTag =
        isIndexFeatureVersionFourEnabled
            ? Tags.of("indexFeatureVersion", String.valueOf(indexFeatureVersion))
            : Tags.empty();
    Tags indexTypeTags = Tags.of(IndexTypeData.INDEX_TYPE_TAG_NAME, indexTypeTag.tagValue);
    Tags ifvAndNumPartition = indexFeatureVersionTag.and(getNumPartitionTag());

    // Note these gauges are probed asynchronously and not atomically.
    // As we don't create our own metric namespace, it's not our responsibility to de-register them.
    // When the gauge computes index size, also cache it in the AtomicLong so that
    // getCachedIndexSize() can return the value without triggering expensive computation.
    // This allows the query hot path to read index size in O(1) time.
    metricsFactory.perIndexObjectValueGauge(
        MetricNames.INDEX_SIZE_BYTES,
        this.indexBackingStrategy,
        strategy -> {
          long size = strategy.getDiskStats().totalFileByteSize();
          this.cachedIndexSize.set(size);
          return size;
        },
        ifvAndNumPartition);
    metricsFactory.perIndexObjectValueGauge(
        MetricNames.REQUIRED_MEMORY,
        this,
        CachedGauge.of(
            LuceneIndexMetricValuesSupplier::getRequiredMemoryForVectorData, Duration.ofMinutes(1)),
        ifvAndNumPartition);
    metricsFactory.perIndexObjectValueGauge(
        MetricNames.LARGEST_INDEX_FILE_SIZE_BYTES,
        this.indexBackingStrategy,
        strategy -> strategy.getDiskStats().largestFileByteSize(),
        ifvAndNumPartition);
    metricsFactory.perIndexObjectValueGauge(
        MetricNames.NUMBER_OF_FILES_IN_INDEX,
        this.indexBackingStrategy,
        strategy -> strategy.getDiskStats().numFiles(),
        ifvAndNumPartition);
    metricsFactory.perIndexObjectValueGauge(
        MetricNames.NUM_LUCENE_FIELDS,
        this,
        CachedGauge.of(LuceneIndexMetricValuesSupplier::getNumFields, Duration.ofMinutes(1)),
        ifvAndNumPartition);
    metricsFactory.perIndexObjectValueGauge(
        MetricNames.NUM_LUCENE_DOCS,
        this,
        LuceneIndexMetricValuesSupplier::getNumDocs,
        ifvAndNumPartition);
    metricsFactory.perIndexObjectValueGauge(
        MetricNames.NUM_LUCENE_MAX_DOCS,
        this,
        LuceneIndexMetricValuesSupplier::getNumLuceneMaxDocs,
        ifvAndNumPartition);
    Stream.of(IndexStatus.StatusCode.values())
        .forEach(
            statusCode ->
                metricsFactory.perIndexObjectValueGauge(
                    MetricNames.INDEX_STATUS_CODE,
                    this,
                    (supplier) -> supplier.getIndexCountInState(statusCode),
                    Tags.of("status", statusCode.name())
                        .and(indexFeatureVersionTag)
                        .and(indexTypeTags)));

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
        Tag partitionId = getIndexPartitionTags(indexPartitionId);
        Tags ifvAndPartitionIdTags = indexFeatureVersionTag.and(partitionId);

        indexPartitionMetricsFactory.perIndexObjectValueGauge(
            MetricNames.INDEX_SIZE_BYTES,
            this.indexBackingStrategy,
            strategy -> strategy.getIndexSizeForIndexPartition(indexPartitionId),
            ifvAndPartitionIdTags);
        indexPartitionMetricsFactory.perIndexObjectValueGauge(
            MetricNames.NUM_LUCENE_FIELDS,
            indexPartitionWriter,
            indexWriter ->
                FunctionalUtils.getOrDefaultIfThrows(
                    indexWriter::getNumFields, WriterClosedException.class, 0),
            ifvAndPartitionIdTags);
        indexPartitionMetricsFactory.perIndexObjectValueGauge(
            MetricNames.NUM_LUCENE_DOCS,
            indexPartitionWriter,
            indexWriter ->
                FunctionalUtils.getOrDefaultIfThrows(
                    indexWriter::getNumDocs, WriterClosedException.class, 0L),
            ifvAndPartitionIdTags);
        indexPartitionMetricsFactory.perIndexObjectValueGauge(
            MetricNames.NUM_LUCENE_MAX_DOCS,
            indexPartitionWriter,
            indexWriter ->
                FunctionalUtils.getOrDefaultIfThrows(
                    indexWriter::getNumLuceneMaxDocs, WriterClosedException.class, 0L),
            ifvAndPartitionIdTags);
        Stream.of(IndexStatus.StatusCode.values())
            .forEach(
                statusCode ->
                    indexPartitionMetricsFactory.perIndexObjectValueGauge(
                        MetricNames.INDEX_STATUS_CODE,
                        this,
                        (supplier) -> supplier.getIndexCountInState(statusCode),
                        ifvAndPartitionIdTags.and(indexTypeTags).and("status", statusCode.name())));
      }
    }
  }

  @Override
  public long getCachedIndexSize() {
    return this.cachedIndexSize.get();
  }

  @Override
  public long refreshAndGetIndexSize() throws IOException {
    long size = this.indexBackingStrategy.refreshDiskStats().totalFileByteSize();
    this.cachedIndexSize.set(size);
    return size;
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
    return this.indexBackingStrategy.getDiskStats().requiredMemoryBytes();
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

  private Tag getNumPartitionTag() {
    return Tag.of("numPartitions", String.valueOf(this.indexWriter.getNumWriters()));
  }

  @VisibleForTesting
  static Tag getIndexPartitionTags(int indexPartitionId) {
    return Tag.of("indexPartition", Integer.toHexString(indexPartitionId));
  }
}
