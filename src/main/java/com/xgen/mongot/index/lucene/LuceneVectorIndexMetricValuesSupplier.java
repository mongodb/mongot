package com.xgen.mongot.index.lucene;

import com.google.common.annotations.VisibleForTesting;
import com.xgen.mongot.index.DocCounts;
import com.xgen.mongot.index.VectorIndexReader;
import com.xgen.mongot.index.lucene.field.FieldName;
import com.xgen.mongot.index.status.IndexStatus;
import com.xgen.mongot.metrics.PerIndexMetricsFactory;
import java.util.Collections;
import java.util.Map;
import java.util.function.Supplier;

@VisibleForTesting
public final class LuceneVectorIndexMetricValuesSupplier extends LuceneIndexMetricValuesSupplier {

  /** Private constructor - does not register gauges. Use {@link #create} to construct instances. */
  private LuceneVectorIndexMetricValuesSupplier(
      Supplier<IndexStatus> indexStatusSupplier,
      IndexBackingStrategy indexBackingStrategy,
      VectorIndexReader indexReader,
      LuceneIndexWriter luceneIndexWriter) {
    super(indexStatusSupplier, indexBackingStrategy, indexReader, luceneIndexWriter);
  }

  /** Static factory method that constructs the supplier and registers all gauges. */
  public static LuceneVectorIndexMetricValuesSupplier create(
      Supplier<IndexStatus> indexStatusSupplier,
      IndexBackingStrategy indexBackingStrategy,
      VectorIndexReader indexReader,
      LuceneIndexWriter luceneIndexWriter,
      PerIndexMetricsFactory metricsFactory,
      int indexFeatureVersion,
      boolean isIndexFeatureVersionFourEnabled) {
    LuceneVectorIndexMetricValuesSupplier supplier =
        new LuceneVectorIndexMetricValuesSupplier(
            indexStatusSupplier, indexBackingStrategy, indexReader, luceneIndexWriter);

    // Register common gauges after construction is complete
    supplier.registerCommonGauges(
        metricsFactory, indexFeatureVersion, isIndexFeatureVersionFourEnabled);

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
}
