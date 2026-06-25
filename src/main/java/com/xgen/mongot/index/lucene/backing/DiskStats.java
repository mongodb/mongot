package com.xgen.mongot.index.lucene.backing;

/**
 * Value class for file metrics computed over a specific directory (recursive). These metrics are
 * not tied to any specific IndexReader, so they include temporary files and leaked segments. Since
 * computing these metrics is IO heavy, we always compute them together.
 *
 * @param totalFileByteSize sum of the sizes of all files in the directory.
 * @param largestFileByteSize size of the single largest file in the directory.
 * @param numFiles total number of files in the directory.
 * @param requiredMemoryBytes estimated heap (in bytes) to serve the index's vector data, summed
 *     from on-disk vector files: {@code .vex} (HNSW graph) and {@code .veq} (quantized vectors)
 *     always count, plus {@code .vec} (raw float vectors) only for non-quantized fields. Vector
 *     data inside {@code .cfs} compound files is estimated by assuming the same vector-data
 *     proportion as the non-{@code .cfs} files ({@code cfsBytes * vectorDataBytes / nonCfsBytes});
 *     an all-compound index returns 0.
 */
public record DiskStats(
    long totalFileByteSize, long largestFileByteSize, long numFiles, long requiredMemoryBytes) {
  public static final DiskStats EMPTY = new DiskStats(0, 0, 0, 0);
}
