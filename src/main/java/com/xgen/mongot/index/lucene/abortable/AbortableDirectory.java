package com.xgen.mongot.index.lucene.abortable;

import java.io.IOException;
import java.util.Set;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Directory wrapper that enables IO-level interruption of merge operations.
 *
 * <p>This wrapper intercepts all IO operations and periodically checks if the associated merge has
 * been aborted via {@link MergePolicy.OneMerge#isAborted()}. When an abort is detected, it throws a
 * {@link MergePolicy.MergeAbortedException}, causing the merge to terminate immediately rather than
 * waiting for the next checkpoint.
 *
 * <p>This is particularly important for long-running operations like HNSW graph building, which may
 * not have frequent checkpoints and could otherwise take hours to abort.
 *
 * <p><b>Design:</b>
 *
 * <ul>
 *   <li>Wraps IndexOutput to check abort status periodically during writes
 *   <li>Wraps IndexInput for vector files (.vex, .vec) to check abort status during reads
 *   <li>Checks every {@link #CHECK_ABORT_INTERVAL_BYTES} bytes read/written (default: 256 KB)
 *   <li>Throws {@link MergePolicy.MergeAbortedException} when abort is detected, allowing Lucene to
 *       recognize this as an intentional abort and handle it appropriately
 * </ul>
 */
public class AbortableDirectory extends FilterDirectory {
  private static final Logger LOG = LoggerFactory.getLogger(AbortableDirectory.class);

  private final MergePolicy.OneMerge merge;

  /**
   * How often to check if the merge is aborted, in bytes read/written. Set to 256 KB to balance
   * between responsiveness and performance overhead. This was reduced from 1 MB to improve abort
   * responsiveness for large HNSW merges.
   */
  public static final long CHECK_ABORT_INTERVAL_BYTES = 256 * 1024; // 256 KB

  /**
   * File extensions for vector files that should have abort detection on reads. These files are
   * read during HNSW merge operations when building the graph from source segment vectors.
   *
   * <ul>
   *   <li>.vex - Vector index (HNSW graph) files
   *   <li>.vec - Flat vector data files (raw float[] or byte[] vectors)
   *   <li>.veq - Quantized flat vector data files (scalar and binary quantized vectors)
   *   <li>.vemq - Binary quantized vector metadata files
   * </ul>
   */
  private static final Set<String> VECTOR_FILE_EXTENSIONS = Set.of("vex", "vec", "veq", "vemq");

  /**
   * Creates a new AbortableDirectory that wraps the given directory.
   *
   * @param in the underlying directory to wrap
   * @param merge the merge operation to monitor for abortion
   */
  public AbortableDirectory(Directory in, MergePolicy.OneMerge merge) {
    super(in);
    this.merge = merge;
  }

  @Override
  public IndexOutput createOutput(String name, IOContext context) throws IOException {
    return new AbortableIndexOutput(super.createOutput(name, context), this.merge);
  }

  @Override
  public IndexOutput createTempOutput(String prefix, String suffix, IOContext context)
      throws IOException {
    return new AbortableIndexOutput(super.createTempOutput(prefix, suffix, context), this.merge);
  }

  @Override
  public IndexInput openInput(String name, IOContext context) throws IOException {
    IndexInput delegate = super.openInput(name, context);
    if (isVectorFile(name)) {
      return new AbortableIndexInput(delegate, this.merge);
    }
    return delegate;
  }

  /**
   * Checks if the given filename is a vector file that should have abort detection on reads.
   *
   * @param name the filename to check
   * @return true if the file is a vector file (.vex or .vec)
   */
  private static boolean isVectorFile(String name) {
    int dotIndex = name.lastIndexOf('.');
    if (dotIndex == -1) {
      return false;
    }
    String extension = name.substring(dotIndex + 1);
    return VECTOR_FILE_EXTENSIONS.contains(extension);
  }

  /**
   * An IndexOutput wrapper that periodically checks if the merge has been aborted.
   *
   * <p>This wrapper tracks the number of bytes written and checks {@link
   * MergePolicy.OneMerge#isAborted()} every {@link #CHECK_ABORT_INTERVAL_BYTES} bytes. If the merge
   * is aborted, it throws an IOException with a clear message.
   */
  static class AbortableIndexOutput extends IndexOutput {
    private final IndexOutput delegate;
    private final MergePolicy.OneMerge merge;
    private long bytesWrittenSinceLastCheck = 0;

    AbortableIndexOutput(IndexOutput delegate, MergePolicy.OneMerge merge) {
      super(
          "AbortableIndexOutput(" + delegate.toString() + ")",
          delegate.getName()); // Use delegate's name
      this.delegate = delegate;
      this.merge = merge;
    }

    /**
     * Checks if the merge has been aborted and throws MergeAbortedException if so.
     *
     * <p>We throw {@link MergePolicy.MergeAbortedException} (which extends IOException) instead of
     * a generic IOException so that Lucene can recognize this as an intentional abort and handle it
     * appropriately (e.g., suppress it rather than treating it as a merge failure/tragedy).
     *
     * @throws MergePolicy.MergeAbortedException if the merge has been aborted
     */
    private void checkAborted() throws MergePolicy.MergeAbortedException {
      if (this.merge.isAborted()) {
        String message =
            "Merge aborted via IO-level interruption for segments: " + this.merge.segments;
        LOG.info(
            "IO-level interruption detected: merge aborted after {} bytes written since last "
                + "check to {}, segments: {}",
            this.bytesWrittenSinceLastCheck,
            this.delegate.getName(),
            this.merge.segments);
        throw new MergePolicy.MergeAbortedException(message);
      }
    }

    /**
     * Records bytes written and checks for abortion if threshold is reached.
     *
     * @param numBytes number of bytes written in this operation
     * @throws IOException if the merge has been aborted
     */
    private void recordBytesAndCheckAbort(long numBytes) throws IOException {
      this.bytesWrittenSinceLastCheck += numBytes;
      if (this.bytesWrittenSinceLastCheck >= CHECK_ABORT_INTERVAL_BYTES) {
        checkAborted();
        // Use modulo to preserve remainder, ensuring consistent check intervals
        // even when a single large write exceeds the threshold
        this.bytesWrittenSinceLastCheck %= CHECK_ABORT_INTERVAL_BYTES;
      }
    }

    @Override
    public void writeByte(byte b) throws IOException {
      this.delegate.writeByte(b);
      recordBytesAndCheckAbort(1);
    }

    @Override
    public void writeBytes(byte[] b, int offset, int length) throws IOException {
      this.delegate.writeBytes(b, offset, length);
      recordBytesAndCheckAbort(length);
    }

    @Override
    public void writeShort(short i) throws IOException {
      this.delegate.writeShort(i);
      recordBytesAndCheckAbort(Short.BYTES);
    }

    @Override
    public void writeInt(int i) throws IOException {
      this.delegate.writeInt(i);
      recordBytesAndCheckAbort(Integer.BYTES);
    }

    @Override
    public void writeLong(long i) throws IOException {
      this.delegate.writeLong(i);
      recordBytesAndCheckAbort(Long.BYTES);
    }

    @Override
    public long getFilePointer() {
      return this.delegate.getFilePointer();
    }

    @Override
    public long getChecksum() throws IOException {
      return this.delegate.getChecksum();
    }

    @Override
    public void close() throws IOException {
      this.delegate.close();
    }
  }

  /**
   * An IndexInput wrapper that periodically checks if the merge has been aborted.
   *
   * <p>This wrapper tracks the number of bytes read and checks {@link
   * MergePolicy.OneMerge#isAborted()} every {@link #CHECK_ABORT_INTERVAL_BYTES} bytes. If the merge
   * is aborted, it throws a {@link MergePolicy.MergeAbortedException} with a clear message.
   *
   * <p>This is particularly important for float[] and byte[] vector merges that use Lucene's
   * built-in vector formats (Lucene99HnswVectorsFormat), which read vector data from source
   * segments during HNSW graph building. Without this wrapper, these merges would not be
   * cancellable at the IO level.
   */
  static class AbortableIndexInput extends IndexInput {
    private final IndexInput delegate;
    private final MergePolicy.OneMerge merge;
    private long bytesReadSinceLastCheck = 0;

    AbortableIndexInput(IndexInput delegate, MergePolicy.OneMerge merge) {
      super("AbortableIndexInput(" + delegate.toString() + ")");
      this.delegate = delegate;
      this.merge = merge;
    }

    /**
     * Private constructor for clone/slice operations that preserves the merge reference.
     *
     * @param resourceDescription the resource description for this input
     * @param delegate the underlying IndexInput
     * @param merge the merge operation to monitor
     */
    private AbortableIndexInput(
        String resourceDescription, IndexInput delegate, MergePolicy.OneMerge merge) {
      super(resourceDescription);
      this.delegate = delegate;
      this.merge = merge;
    }

    /**
     * Checks if the merge has been aborted and throws MergeAbortedException if so.
     *
     * @throws MergePolicy.MergeAbortedException if the merge has been aborted
     */
    private void checkAborted() throws MergePolicy.MergeAbortedException {
      if (this.merge.isAborted()) {
        String message =
            "Merge aborted via IO-level interruption (read) for segments: " + this.merge.segments;
        LOG.info(
            "IO-level interruption detected: merge aborted after {} bytes read since last "
                + "check from {}, segments: {}",
            this.bytesReadSinceLastCheck,
            this.delegate.toString(),
            this.merge.segments);
        throw new MergePolicy.MergeAbortedException(message);
      }
    }

    /**
     * Records bytes read and checks for abortion if threshold is reached.
     *
     * @param numBytes number of bytes read in this operation
     * @throws IOException if the merge has been aborted
     */
    private void recordBytesAndCheckAbort(long numBytes) throws IOException {
      this.bytesReadSinceLastCheck += numBytes;
      if (this.bytesReadSinceLastCheck >= CHECK_ABORT_INTERVAL_BYTES) {
        checkAborted();
        this.bytesReadSinceLastCheck %= CHECK_ABORT_INTERVAL_BYTES;
      }
    }

    @Override
    public byte readByte() throws IOException {
      byte result = this.delegate.readByte();
      recordBytesAndCheckAbort(1);
      return result;
    }

    @Override
    public void readBytes(byte[] b, int offset, int len) throws IOException {
      this.delegate.readBytes(b, offset, len);
      recordBytesAndCheckAbort(len);
    }

    @Override
    public void readBytes(byte[] b, int offset, int len, boolean useBuffer) throws IOException {
      this.delegate.readBytes(b, offset, len, useBuffer);
      recordBytesAndCheckAbort(len);
    }

    @Override
    public void close() throws IOException {
      this.delegate.close();
    }

    @Override
    public long getFilePointer() {
      return this.delegate.getFilePointer();
    }

    @Override
    public void seek(long pos) throws IOException {
      this.delegate.seek(pos);
    }

    @Override
    public long length() {
      return this.delegate.length();
    }

    @Override
    public IndexInput clone() {
      return new AbortableIndexInput(
          "AbortableIndexInput(" + this.delegate.clone().toString() + ")",
          this.delegate.clone(),
          this.merge);
    }

    @Override
    public IndexInput slice(String sliceDescription, long offset, long length) throws IOException {
      IndexInput slicedDelegate = this.delegate.slice(sliceDescription, offset, length);
      return new AbortableIndexInput(
          "AbortableIndexInput(" + slicedDelegate.toString() + ")", slicedDelegate, this.merge);
    }
  }
}
