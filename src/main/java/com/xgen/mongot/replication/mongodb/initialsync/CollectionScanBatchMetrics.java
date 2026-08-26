package com.xgen.mongot.replication.mongodb.initialsync;

import com.xgen.mongot.index.DocumentEvent;
import java.util.List;

/**
 * Per-batch metrics recorded by every collection scanner once a batch is handed to the indexing
 * scheduler, so that single-cursor and parallel scans report the same counters.
 */
final class CollectionScanBatchMetrics {

  private CollectionScanBatchMetrics() {}

  static void record(InitialSyncContext context, List<DocumentEvent> batch) {
    if (batch.isEmpty()) {
      return;
    }

    context
        .getInitialSyncMetricsUpdater()
        .getCollectionScanBatchTotalApplicableDocuments()
        .record(batch.size());

    long byteTotal =
        batch.stream()
            .flatMap(event -> event.getDocument().stream())
            .map(document -> document.getByteBuffer().remaining())
            .reduce(0L, Long::sum, Long::sum);

    context
        .getInitialSyncMetricsUpdater()
        .getCollectionScanBatchTotalApplicableBytes()
        .record(byteTotal);

    context.getInitialSyncMetricsUpdater().getTotalApplicableBytes().increment(byteTotal);
  }
}
