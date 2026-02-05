package com.xgen.testing.mongot.mock.index;

import com.xgen.mongot.index.DocCounts;
import com.xgen.mongot.index.IndexMetricValuesSupplier;
import com.xgen.mongot.index.status.IndexStatus;
import org.mockito.Mockito;

public class IndexMetricsSupplier {

  public static IndexMetricValuesSupplier mockEmptyIndexMetricsSupplier() {
    var indexMetricsSupplier = Mockito.mock(IndexMetricValuesSupplier.class);
    Mockito.doReturn(0L).when(indexMetricsSupplier).computeIndexSize();
    Mockito.doReturn(0L).when(indexMetricsSupplier).getCachedIndexSize();
    Mockito.doReturn(new DocCounts(0, 0, 0, 0L)).when(indexMetricsSupplier).getDocCounts();
    Mockito.doReturn(0).when(indexMetricsSupplier).getNumFields();
    Mockito.doAnswer((ignored) -> IndexStatus.notStarted())
        .when(indexMetricsSupplier)
        .getIndexStatus();
    return indexMetricsSupplier;
  }
}
