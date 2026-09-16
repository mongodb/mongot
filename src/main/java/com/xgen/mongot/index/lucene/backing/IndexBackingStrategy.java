package com.xgen.mongot.index.lucene.backing;

import com.google.common.collect.ImmutableList;
import com.xgen.mongot.index.status.IndexStatus;
import java.io.Closeable;
import java.io.IOException;
import java.util.function.Supplier;
import org.apache.lucene.search.ReferenceManager;

/** Strategy for managing index-level resources: refresh, storage metrics, and cleanup. */
public interface IndexBackingStrategy {

  Closeable createIndexRefresher(
      Supplier<IndexStatus> statusRef, ImmutableList<ReferenceManager<?>> searcherManagers);

  void releaseResources() throws IOException;

  /**
   * Gets or computes the latest disk stats for a specific index.
   *
   * <p>This includes all files in the index irrespective of whether they are part of any active
   * IndexReader.
   *
   * <p><b>WARNING: </b>This is a blocking operation that may be expensive for large indexes. This
   * method may return immediately if recent cached data is available, or it may block until metrics
   * are computed. No guarantees are made with respect to the freshness of this data. This method
   * should NOT be called on latency-sensitive path without additional caching layers.
   */
  DiskStats getDiskStats();

  /** Returns current disk stats, forcing a directory walk when this strategy caches them. */
  default DiskStats refreshDiskStats() throws IOException {
    return getDiskStats();
  }

  /**
   * Gets or computes the size in bytes of the specified index partition, or zero if the requested
   * partition does not exist.
   *
   * <p>This includes all files in the partition irrespective of whether they are part of any active
   * IndexReader.
   *
   * <p><b>WARNING: </b>This is a blocking operation that may be expensive for large indexes. This
   * method may return immediately if recent cached data is available, or it may block until metrics
   * are computed. No guarantees are made with respect to the freshness of this data. This method
   * should NOT be called on latency-sensitive path without additional caching layers.
   */
  long getIndexSizeForIndexPartition(int indexPartitionId);

  /** Clear metadata associated with the index (this does not release all the index resources). */
  void clearMetadata() throws IOException;
}
