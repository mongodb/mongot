package com.xgen.mongot.index.blobstore;

import com.xgen.mongot.blobstore.BlobstoreException;
import java.util.Optional;

/** Uploads and downloads index snapshots from blobstore. */
public interface IndexBlobstoreSnapshotter {
  /**
   * Upload the index.
   *
   * @throws BlobstoreException to propagate all exceptions.
   */
  void uploadIndex() throws BlobstoreException;

  /**
   * Download the index.
   *
   * @throws BlobstoreException to propagate all exceptions.
   */
  void downloadIndex() throws BlobstoreException;

  /** Returns true if index needs to be downloaded. */
  boolean shouldDownloadIndex();

  /**
   * Returns index snapshot status if available.
   *
   * <p>This metadata can potentially lag behind real-time snapshot progress,
   * as implementations can cache this data.
   */
  Optional<IndexSnapshotStatus> getIndexSnapshotStatus();
}
