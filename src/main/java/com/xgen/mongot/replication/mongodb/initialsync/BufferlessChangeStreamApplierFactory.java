package com.xgen.mongot.replication.mongodb.initialsync;

import com.xgen.mongot.replication.mongodb.common.InitialSyncException;
import java.util.Optional;
import org.bson.BsonTimestamp;

interface BufferlessChangeStreamApplierFactory {

  /**
   * Creates a BufferlessChangeStreamApplier using the optime to start the change stream.
   *
   * <p>If this is a fresh initial sync, i.e., not resuming, the change stream will start at
   * highWaterMark + 1 since the collection scan already captured everything at highWaterMark. If
   * false, we're resuming from a crash and must use highWaterMark (inclusive) to avoid missing
   * events as two events can share the same optime.
   *
   * <p>A present {@code parallelResumeState} puts the applier on the parallel path, where it
   * advances that state's high water mark once each batch is indexed instead of staging a resume
   * blob per batch.
   */
  BufferlessChangeStreamApplier create(
      BsonTimestamp highWaterMark,
      boolean isFreshStart,
      Optional<ParallelCollectionScanResumeState> parallelResumeState)
      throws InitialSyncException;
}
