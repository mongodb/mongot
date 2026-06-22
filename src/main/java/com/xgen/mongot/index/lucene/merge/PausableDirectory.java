package com.xgen.mongot.index.lucene.merge;

import com.google.common.annotations.VisibleForTesting;
import com.xgen.mongot.monitor.Gate;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Directory wrapper that pauses (blocks) write operations when disk usage is high.
 *
 * <p>Unlike {@link com.xgen.mongot.index.lucene.abortable.AbortableDirectory} which throws an
 * exception to abort merges, this directory blocks on the gate's {@link Gate#awaitOpen()} method,
 * allowing the merge to resume from where it paused when disk usage drops.
 *
 * <p>This is designed for disk-full scenarios where we want to:
 *
 * <ul>
 *   <li>Pause ongoing merges without losing progress
 *   <li>Automatically resume when disk usage drops
 *   <li>Avoid partial/temporary files from aborted merges
 * </ul>
 *
 * <p><b>Design:</b>
 *
 * <ul>
 *   <li>Wraps IndexOutput to check gate status periodically during writes
 *   <li>Checks every {@link #CHECK_PAUSE_INTERVAL_BYTES} bytes written (default: 256 KB)
 *   <li>Blocks on {@link Gate#awaitOpen()} when gate is closed, resuming when it opens
 *   <li>Read operations (IndexInput) are not wrapped as they don't consume disk space
 * </ul>
 */
public class PausableDirectory extends FilterDirectory {
  private static final Logger LOG = LoggerFactory.getLogger(PausableDirectory.class);

  private final Gate mergeGate;
  private final Runnable guardedOnPause;
  private final Runnable onResume;

  /**
   * How often to check if merges should be paused, in bytes written. Set to 256 KB to balance
   * between responsiveness and performance overhead. This matches the interval used by {@link
   * com.xgen.mongot.index.lucene.abortable.AbortableDirectory}.
   */
  public static final long CHECK_PAUSE_INTERVAL_BYTES = 256 * 1024; // 256 KB

  /**
   * Creates a new PausableDirectory that wraps the given directory, with a no-op pause callback.
   * Production code should use the 3-arg constructor to receive pause notifications.
   *
   * @param in the underlying directory to wrap
   * @param mergeGate the gate that controls whether merges should be paused
   */
  @VisibleForTesting
  public PausableDirectory(Directory in, Gate mergeGate) {
    this(in, mergeGate, () -> {});
  }

  /**
   * Creates a new PausableDirectory that wraps the given directory and invokes {@code onPause} at
   * most once per pause window observed by this directory instance.
   *
   * <p>Semantics: {@code onPause} is fired the first time any {@link PausableIndexOutput} created
   * by this directory observes a closed gate. Subsequent writes (on the same or other outputs from
   * this directory) that observe the gate still closed do not re-fire the callback. Once the gate
   * is observed open again (via {@link Gate#awaitOpen()} returning normally, or its return via the
   * interrupted/IOException path), the latch is reset, so a later distinct close-window will fire
   * {@code onPause} again.
   *
   * <p>Because a new PausableDirectory is created per merge in {@code
   * InstrumentedConcurrentMergeScheduler#wrapForMerge}, this effectively counts "this merge
   * experienced at least one pause window" each time a pause window starts within a single merge.
   * Multiple concurrent merges paused by the same global gate-close event will each fire their own
   * {@code onPause}.
   *
   * @param in the underlying directory to wrap
   * @param mergeGate the gate that controls whether merges should be paused
   * @param onPause callback invoked at most once per pause window observed by this directory
   */
  public PausableDirectory(Directory in, Gate mergeGate, Runnable onPause) {
    super(in);
    this.mergeGate = mergeGate;
    AtomicBoolean paused = new AtomicBoolean(false);
    this.guardedOnPause =
        () -> {
          if (paused.compareAndSet(false, true)) {
            onPause.run();
          }
        };
    this.onResume = () -> paused.set(false);
  }

  @Override
  public IndexOutput createOutput(String name, IOContext context) throws IOException {
    return new PausableIndexOutput(
        super.createOutput(name, context), this.mergeGate, this.guardedOnPause, this.onResume);
  }

  @Override
  public IndexOutput createTempOutput(String prefix, String suffix, IOContext context)
      throws IOException {
    return new PausableIndexOutput(
        super.createTempOutput(prefix, suffix, context),
        this.mergeGate,
        this.guardedOnPause,
        this.onResume);
  }

  /**
   * An IndexOutput wrapper that periodically checks if merges should be paused.
   *
   * <p>This wrapper tracks the number of bytes written and checks {@link Gate#isClosed()} every
   * {@link #CHECK_PAUSE_INTERVAL_BYTES} bytes. If the gate is closed (disk usage is high), it
   * blocks on {@link Gate#awaitOpen()} until disk usage drops.
   */
  static class PausableIndexOutput extends IndexOutput {
    private final IndexOutput delegate;
    private final Gate mergeGate;
    private final Runnable onPause;
    private final Runnable onResume;
    private long bytesWrittenSinceLastCheck = 0;

    PausableIndexOutput(IndexOutput delegate, Gate mergeGate, Runnable onPause, Runnable onResume) {
      super("PausableIndexOutput(" + delegate.toString() + ")", delegate.getName());
      this.delegate = delegate;
      this.mergeGate = mergeGate;
      this.onPause = onPause;
      this.onResume = onResume;
    }

    /**
     * Checks if disk usage is high and blocks until it drops.
     *
     * <p>This method is called periodically during write operations. If the gate is closed
     * (indicating high disk usage), it blocks on {@link Gate#awaitOpen()} until the gate opens
     * (disk usage drops below the threshold).
     *
     * @throws IOException if the thread is interrupted while waiting
     */
    private void maybeWaitForDiskSpace() throws IOException {
      if (this.mergeGate.isClosed()) {
        this.onPause.run();
        LOG.info(
            "Merge paused due to high disk usage, waiting for disk space... "
                + "(file: {}, bytes written: {})",
            this.delegate.getName(),
            this.delegate.getFilePointer());
        try {
          this.mergeGate.awaitOpen();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new IOException("Interrupted while waiting for disk space", e);
        } finally {
          // Reset the pause latch on every exit path (normal resume or interrupt) so the next
          // distinct pause window can fire onPause again.
          this.onResume.run();
        }
        LOG.info(
            "Disk usage dropped, resuming merge (file: {}, bytes written: {})",
            this.delegate.getName(),
            this.delegate.getFilePointer());
      }
    }

    /**
     * Records bytes written and checks for pause if threshold is reached.
     *
     * <p>Uses modulo to preserve the remainder when crossing the threshold, ensuring consistent
     * pause-check intervals even when large writes push the counter well past the threshold.
     *
     * @param numBytes number of bytes written in this operation
     * @throws IOException if the thread is interrupted while waiting for disk space
     */
    private void recordBytesAndMaybeWait(long numBytes) throws IOException {
      this.bytesWrittenSinceLastCheck += numBytes;
      if (this.bytesWrittenSinceLastCheck >= CHECK_PAUSE_INTERVAL_BYTES) {
        maybeWaitForDiskSpace();
        // Use modulo to preserve remainder, ensuring consistent check intervals
        // even when a single large write exceeds the threshold
        this.bytesWrittenSinceLastCheck %= CHECK_PAUSE_INTERVAL_BYTES;
      }
    }

    @Override
    public void writeByte(byte b) throws IOException {
      this.delegate.writeByte(b);
      recordBytesAndMaybeWait(1);
    }

    @Override
    public void writeBytes(byte[] b, int offset, int length) throws IOException {
      this.delegate.writeBytes(b, offset, length);
      recordBytesAndMaybeWait(length);
    }

    @Override
    public void writeShort(short i) throws IOException {
      this.delegate.writeShort(i);
      recordBytesAndMaybeWait(Short.BYTES);
    }

    @Override
    public void writeInt(int i) throws IOException {
      this.delegate.writeInt(i);
      recordBytesAndMaybeWait(Integer.BYTES);
    }

    @Override
    public void writeLong(long i) throws IOException {
      this.delegate.writeLong(i);
      recordBytesAndMaybeWait(Long.BYTES);
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
}
