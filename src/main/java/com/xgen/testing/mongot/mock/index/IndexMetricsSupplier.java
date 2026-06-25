package com.xgen.testing.mongot.mock.index;

import com.xgen.mongot.index.DocCounts;
import com.xgen.mongot.index.IndexMetricValuesSupplier;
import com.xgen.mongot.index.lucene.field.FieldName;
import com.xgen.mongot.index.status.IndexStatus;
import java.util.Map;
import org.mockito.Mockito;

public class IndexMetricsSupplier {

  /** Creates a real IndexMetricValuesSupplier that reports no files in the index. */
  public static IndexMetricValuesSupplier ofEmptyIndex(IndexStatus status) {
    return new IndexMetricValuesSupplier() {

      @Override
      public long getCachedIndexSize() {
        return 0;
      }

      @Override
      public int getNumFields() {
        return 0;
      }

      @Override
      public Map<FieldName.TypeField, Double> getNumFieldsPerDatatype() {
        return Map.of();
      }

      @Override
      public DocCounts getDocCounts() {
        return new DocCounts(0, 0, 0, 0L);
      }

      @Override
      public IndexStatus getIndexStatus() {
        return status;
      }

      @Override
      public void close() {}
    };
  }

  public static IndexMetricValuesSupplier mockEmptyIndexMetricsSupplier() {
    var indexMetricsSupplier = Mockito.mock(IndexMetricValuesSupplier.class);
    Mockito.doReturn(0L).when(indexMetricsSupplier).getCachedIndexSize();
    Mockito.doReturn(new DocCounts(0, 0, 0, 0L)).when(indexMetricsSupplier).getDocCounts();
    Mockito.doReturn(0).when(indexMetricsSupplier).getNumFields();
    Mockito.doAnswer((ignored) -> IndexStatus.notStarted())
        .when(indexMetricsSupplier)
        .getIndexStatus();
    return indexMetricsSupplier;
  }
}
