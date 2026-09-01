package com.xgen.mongot.replication.mongodb.initialsync;

import java.util.function.BooleanSupplier;
import org.bson.BsonTimestamp;

/**
 * Builds the scanner for one partition of one scan window. The dispatcher supplies the window's
 * freshness floor and its abort flag.
 */
interface ParallelCollectionScannerFactory {

  ParallelCollectionScanner create(
      BsonTimestamp highWaterMark,
      ParallelCollectionScanPartitionState partitionState,
      BooleanSupplier abortRequested);
}
