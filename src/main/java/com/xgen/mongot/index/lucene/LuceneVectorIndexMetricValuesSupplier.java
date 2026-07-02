package com.xgen.mongot.index.lucene;

import com.google.common.annotations.VisibleForTesting;
import com.xgen.mongot.index.DocCounts;
import com.xgen.mongot.index.VectorIndexReader;
import com.xgen.mongot.index.lucene.backing.IndexBackingStrategy;
import com.xgen.mongot.index.lucene.field.FieldName;
import com.xgen.mongot.index.lucene.writer.LuceneIndexWriter;
import com.xgen.mongot.index.status.IndexStatus;
import com.xgen.mongot.metrics.CachedGauge;
import com.xgen.mongot.metrics.PerIndexMetricsFactory;
import com.xgen.mongot.util.FunctionalUtils;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.function.Supplier;

@VisibleForTesting
public final class LuceneVectorIndexMetricValuesSupplier extends LuceneIndexMetricValuesSupplier {

  private final VectorIndexReader indexReader;
  private final boolean hasNestedVectorFields;

  /** Private constructor - does not register gauges. Use {@link #create} to construct instances. */
  private LuceneVectorIndexMetricValuesSupplier(
      Supplier<IndexStatus> indexStatusSupplier,
      IndexBackingStrategy indexBackingStrategy,
      LuceneIndexWriter luceneIndexWriter,
      VectorIndexReader indexReader,
      boolean hasNestedVectorFields) {
    super(indexStatusSupplier, indexBackingStrategy, luceneIndexWriter);
    this.indexReader = indexReader;
    this.hasNestedVectorFields = hasNestedVectorFields;
  }

  /** Static factory method that constructs the supplier and registers all gauges. */
  public static LuceneVectorIndexMetricValuesSupplier create(
      Supplier<IndexStatus> indexStatusSupplier,
      IndexBackingStrategy indexBackingStrategy,
      LuceneIndexWriter luceneIndexWriter,
      VectorIndexReader indexReader,
      boolean hasNestedVectorFields,
      PerIndexMetricsFactory metricsFactory,
      int indexFeatureVersion,
      boolean isIndexFeatureVersionFourEnabled) {
    LuceneVectorIndexMetricValuesSupplier supplier =
        new LuceneVectorIndexMetricValuesSupplier(
            indexStatusSupplier,
            indexBackingStrategy,
            luceneIndexWriter,
            indexReader,
            hasNestedVectorFields);

    // Register common gauges after construction is complete
    supplier.registerCommonGauges(
        metricsFactory, indexFeatureVersion, isIndexFeatureVersionFourEnabled);

    // Number of live vectors in the index, exported to Prometheus/Grafana. Cached because the
    // gauge is probed asynchronously and the nested-index path acquires a Lucene searcher.
    metricsFactory.perIndexObjectValueGauge(
        MetricNames.NUM_VECTORS,
        supplier,
        CachedGauge.of(
            LuceneVectorIndexMetricValuesSupplier::getVectorCount, Duration.ofMinutes(1)));

    return supplier;
  }

  @Override
  public Map<FieldName.TypeField, Double> getNumFieldsPerDatatype() {
    return Collections.emptyMap();
  }

  @Override
  public DocCounts getDocCounts() {
    var numDocs = getNumDocs();
    return new DocCounts(numDocs, getNumLuceneMaxDocs(), getMaxLuceneMaxDocs(), numDocs);
  }

  /**
   * Returns the current number of live vectors in this index. For non-nested indexes each Lucene
   * doc is a vector, so {@code numDocs} is exact and no searcher lookup is performed. For nested
   * indexes, Lucene also stores one root doc per MongoDB document (marked with {@code
   * EMBEDDED_ROOT}); subtracting those gives the true vector count.
   */
  private long getVectorCount() {
    long numDocs = getNumDocs();
    // Flat vector indexes have no embedded root docs to subtract, so skip the searcher lookup
    // entirely. This is the common case.
    if (!this.hasNestedVectorFields) {
      return numDocs;
    }
    // Guard against a transient root-doc overestimate (e.g. stale stats) yielding a negative count.
    return Math.max(0L, numDocs - getNumEmbeddedRootDocs());
  }

  private long getNumEmbeddedRootDocs() {
    return FunctionalUtils.getOrDefaultIfThrows(
        this.indexReader::getNumEmbeddedRootDocuments, Exception.class, 0L);
  }
}
