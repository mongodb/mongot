package com.xgen.mongot.replication.mongodb.steadystate.changestream;

import com.xgen.mongot.replication.mongodb.common.ChangeStreamBatch;

/** Treats an index as high priority if the byte size of its previous batch met a threshold */
final class BatchDataSizePriorityStrategy implements ChangeStreamPriorityStrategy {

  // Half the 16 MiB BSON batch cap: a previous batch this large means the index has substantial
  // pending change data, so its next getMore is prioritized.
  static final int DEFAULT_HIGH_PRIORITY_BATCH_SIZE_BYTES = 8 * 1024 * 1024;

  private final int highPriorityBatchSizeBytes;

  BatchDataSizePriorityStrategy(int highPriorityBatchSizeBytes) {
    if (highPriorityBatchSizeBytes < 0) {
      throw new IllegalArgumentException(
          "highPriorityBatchSizeBytes must be non-negative, got: " + highPriorityBatchSizeBytes);
    }
    this.highPriorityBatchSizeBytes = highPriorityBatchSizeBytes;
  }

  @Override
  public boolean isHighPriority(ChangeStreamBatch batch) {
    int batchDataSizeBytes =
        batch.getRawEvents().stream().mapToInt(doc -> doc.getByteBuffer().remaining()).sum();
    return batchDataSizeBytes >= this.highPriorityBatchSizeBytes;
  }
}
