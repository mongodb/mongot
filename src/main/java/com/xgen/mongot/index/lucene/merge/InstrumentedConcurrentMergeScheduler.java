package com.xgen.mongot.index.lucene.merge;

import com.google.common.base.Stopwatch;
import com.google.errorprone.annotations.Var;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import com.sun.management.ThreadMXBean;
import com.xgen.mongot.index.lucene.abortable.AbortableDirectory;
import com.xgen.mongot.index.version.GenerationId;
import com.xgen.mongot.metrics.MetricsFactory;
import com.xgen.mongot.metrics.ServerStatusDataExtractor;
import com.xgen.mongot.metrics.ServerStatusDataExtractor.LuceneMeterData;
import com.xgen.mongot.metrics.ThreadPoolResourceMetrics;
import com.xgen.mongot.metrics.Timed;
import com.xgen.mongot.monitor.Gate;
import com.xgen.mongot.monitor.ToggleGate;
import com.xgen.mongot.util.Bytes;
import com.xgen.mongot.util.concurrent.LiveThreadIdsRegistry;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;
import org.apache.lucene.index.ConcurrentMergeScheduler;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.MergeScheduler;
import org.apache.lucene.index.MergeTrigger;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.InfoStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * InstrumentedConcurrentMergeScheduler tracks and exports prometheus metrics for merges and allows
 * for the same MergeScheduler to be shared by multiple lucene indices.
 *
 * <p>This scheduler is shared by all indices but should not be passed directly to IndexWriter.
 */
public class InstrumentedConcurrentMergeScheduler extends ConcurrentMergeScheduler {
  /**
   * IndexPartitionIdentifier is a wrapper of GenerationId and optional indexPartitionId. It is used
   * to identify an index-partition in all indexes.
   *
   * <p>When there is only one index-partition, indexPartitionId will be empty.
   */
  static class IndexPartitionIdentifier {
    private final GenerationId generationId;
    private final Optional<Integer> indexPartitionId;
    private final String indexType;

    IndexPartitionIdentifier(
        GenerationId generationId, Optional<Integer> indexPartitionId, String indexType) {
      this.generationId = generationId;
      this.indexPartitionId = indexPartitionId;
      this.indexType = indexType;
    }

    GenerationId getGenerationId() {
      return this.generationId;
    }

    Optional<Integer> getIndexPartitionId() {
      return this.indexPartitionId;
    }

    String getIndexType() {
      return this.indexType;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      IndexPartitionIdentifier that = (IndexPartitionIdentifier) o;
      return Objects.equals(this.generationId, that.generationId)
          && Objects.equals(this.indexPartitionId, that.indexPartitionId)
          && Objects.equals(this.indexType, that.indexType);
    }

    @Override
    public int hashCode() {
      return Objects.hash(this.generationId, this.indexPartitionId, this.indexType);
    }

    @Override
    public String toString() {
      return "IndexPartitionIdentifier(generationIdLogString="
          + this.generationId.uniqueString()
          + ", indexPartition="
          + this.indexPartitionId
          + ", indexType="
          + this.indexType
          + ")";
    }
  }

  // A filter pattern
  static class TaggedMergeSource implements MergeScheduler.MergeSource {
    private final MergeScheduler.MergeSource in;
    private final IndexPartitionIdentifier indexPartitionIdentifier;
    private final boolean cancelMergeEnabled;

    TaggedMergeSource(
        MergeScheduler.MergeSource in,
        IndexPartitionIdentifier indexPartitionIdentifier,
        boolean cancelMergeEnabled) {
      this.in = in;
      this.indexPartitionIdentifier = indexPartitionIdentifier;
      this.cancelMergeEnabled = cancelMergeEnabled;
    }

    public IndexPartitionIdentifier getIndexPartitionIdentifier() {
      return this.indexPartitionIdentifier;
    }

    public boolean isCancelMergeEnabled() {
      return this.cancelMergeEnabled;
    }

    @Override
    public MergePolicy.OneMerge getNextMerge() {
      return this.in.getNextMerge();
    }

    @Override
    public void onMergeFinished(MergePolicy.OneMerge merge) {
      this.in.onMergeFinished(merge);
    }

    @Override
    public boolean hasPendingMerges() {
      return this.in.hasPendingMerges();
    }

    @Override
    public void merge(MergePolicy.OneMerge merge) throws IOException {
      this.in.merge(merge);
    }
  }

  /**
   * A thin-wrapped scheduler passed to each index: e.g., one index has one instance of this class.
   * But under-the-hood, it dispatches all operations to the global
   * InstrumentedConcurrentMergeScheduler, except for the close(), that only waits for the
   * corresponding index's ongoing merges to finish.
   */
  public static class PerIndexPartitionMergeScheduler extends MergeScheduler {
    public InstrumentedConcurrentMergeScheduler getIn() {
      return this.in;
    }

    private final InstrumentedConcurrentMergeScheduler in;
    private final IndexPartitionIdentifier indexPartitionIdentifier;
    private final boolean cancelMergeEnabled;

    /** Creates a merge scheduler for a specific index partition. */
    public PerIndexPartitionMergeScheduler(
        InstrumentedConcurrentMergeScheduler in,
        IndexPartitionIdentifier indexPartitionIdentifier,
        boolean cancelMergeEnabled) {
      this.in = in;
      this.indexPartitionIdentifier = indexPartitionIdentifier;
      this.cancelMergeEnabled = cancelMergeEnabled;
    }

    @Override
    public void merge(MergeSource mergeSource, MergeTrigger trigger) throws IOException {
      // Fast-path check if merges have been cancelled for this index or globally.
      // Note: This check is not atomic with the actual merge scheduling, so a concurrent
      // cancelMerges() call could still allow a merge to slip through. The authoritative
      // check happens in getMergeThread() which is synchronized with cancelMerges().
      if (this.in.isMergeCancelled(this.indexPartitionIdentifier)) {
        LOG.debug(
            "Rejecting new merge request for cancelled index: {}, trigger: {}",
            this.indexPartitionIdentifier,
            trigger);
        return;
      }
      var taggedMergeSource =
          new TaggedMergeSource(
              mergeSource, this.indexPartitionIdentifier, this.cancelMergeEnabled);
      this.in.merge(taggedMergeSource, trigger);
    }

    @Override
    public Directory wrapForMerge(MergePolicy.OneMerge merge, Directory in) {
      // Delegate to the parent scheduler which handles all wrapping:
      // 1. PausableDirectory for disk-based pause/resume
      // 2. AbortableDirectory for IO-level interruption (if cancelMergeEnabled)
      return this.in.wrapForMerge(merge, in, this.cancelMergeEnabled);
    }

    @Override
    public void close() throws IOException {
      this.in.close(this.indexPartitionIdentifier);
    }

    /**
     * Aborts all running merges for this index partition, then waits for them to complete.
     *
     * <p>This method will:
     *
     * <ol>
     *   <li>Block new merges from being scheduled for this index
     *   <li>Mark all running merges for this index as aborted via {@link
     *       MergePolicy.OneMerge#setAborted()}
     *   <li>Wait for running merge threads to detect the abort signal and stop
     * </ol>
     *
     * <p><b>Note on pending merges:</b> Pending merges (those queued in IndexWriter but not yet
     * started) are not explicitly aborted by this method. However, new merge scheduling is blocked,
     * so no new merge threads will be started. Pending merges will be discarded when the
     * IndexWriter is closed.
     *
     * <p>Unlike {@link #close()}, this method actively aborts merges rather than just waiting for
     * them to finish naturally.
     *
     * @return true if all merge threads terminated within the timeout, false if some threads are
     *     still running after the timeout
     * @throws IOException if there is an error during merge cancellation
     */
    public boolean cancelMerges() throws IOException {
      return this.in.cancelMerges(this.indexPartitionIdentifier);
    }

    /**
     * Returns whether merges are currently paused due to high disk usage.
     *
     * @return true if merges are paused, false otherwise
     * @see InstrumentedConcurrentMergeScheduler#isMergePaused()
     */
    public boolean isMergePaused() {
      return this.in.isMergePaused();
    }

    // We created this method because we cannot easily override the initialize() method
    // in ConcurrentMergeScheduler. We don't need the initDynamicDefaults() part in the
    // initialize() method, and only need the setInfoStream().
    public void setInfoStream(InfoStream infoStream) {
      this.in.setInfoStream(infoStream);
    }
  }

  private static final Logger LOG =
      LoggerFactory.getLogger(InstrumentedConcurrentMergeScheduler.class);

  /**
   * Default timeout in milliseconds for waiting on each merge thread to terminate during
   * cancellation of merges for a single index. If a merge thread does not terminate within
   * this timeout, a warning is logged and cancellation continues without waiting further.
   */
  public static final long DEFAULT_CANCEL_MERGE_PER_THREAD_TIMEOUT_MS = 100_000; // 100 seconds

  /**
   * Default timeout in milliseconds for waiting on each merge thread to terminate during
   * global cancellation of all merges (e.g., during shutdown). If a merge thread does not
   * terminate within this timeout, a warning is logged and cancellation continues.
   */
  public static final long DEFAULT_CANCEL_ALL_MERGES_PER_THREAD_TIMEOUT_MS = 50_000; // 50 seconds

  private static record SegmentSize(String name, long size) {}

  private final MetricsFactory metricsFactory;
  private final Counter numMerges;
  private final AtomicLong runningMerges;
  // It tells the number of current merging documents, which also includes deleted documents.
  private final AtomicLong mergingDocs;
  private final Counter numSegmentsMerged;
  private final DistributionSummary mergeSize;
  private final DistributionSummary mergeResultSize;
  private final DistributionSummary mergedDocs;
  private final Timer mergeTime;
  private final Timer mergeCancellationTime;
  private final Counter numMergesAborted;
  // If our GenerationId is the only reference back to a particular index it is fine to GC.
  private final WeakHashMap<GenerationId, MergeStopwatch> mergeElapsedStopwatches;

  // Configurable timeout for cancelMerges() - per merge thread for a single index
  private final long cancelMergePerThreadTimeoutMs;
  // Configurable timeout for cancelAllMerges() - per merge thread during global shutdown
  private final long cancelAllMergesPerThreadTimeoutMs;

  // Tracks indices that have had their merges cancelled - new merges for these indices will be
  // rejected. Access must be synchronized on 'this'.
  @GuardedBy("this")
  private final Set<IndexPartitionIdentifier> cancelledIndices = new HashSet<>();

  // Global flag to prevent new merges from being scheduled during shutdown.
  // Once set to true, no new merges will be started for any index.
  private final AtomicBoolean allMergesCancelled = new AtomicBoolean(false);

  // Gate for pausing merges when disk usage is high. Default is always open (no blocking).
  private Gate mergeGate = ToggleGate.opened();

  // Per-merge attribution. Empty when MERGE_ATTRIBUTION_METRICS is disabled or per-thread
  // tracking is unavailable on this JVM.
  private final Optional<MergeAttribution> mergeAttribution;

  /**
   * Creates a per index-partition merge scheduler where input 'idx' tags the merge threads that
   * belong to a particular index-partition. The output PerIndexMergeScheduler wraps the running
   * instance of InstrumentedConcurrentMergeScheduler, and only one of its type exists per
   * index-partition.
   */
  public PerIndexPartitionMergeScheduler createForIndexPartition(
      GenerationId generationId,
      int indexPartitionId,
      int numIndexes,
      boolean cancelMergeEnabled,
      String indexType) {
    Optional<Integer> optionalIndexPartitionId =
        numIndexes > 1 ? Optional.of(indexPartitionId) : Optional.empty();
    return new PerIndexPartitionMergeScheduler(
        this,
        new IndexPartitionIdentifier(generationId, optionalIndexPartitionId, indexType),
        cancelMergeEnabled);
  }

  /**
   * Sets the gate used to pause merges when disk usage is high.
   *
   * <p>When the gate is closed (disk usage is high), ongoing merge operations will block on
   * {@link Gate#awaitOpen()} during I/O operations. When the gate opens (disk usage drops),
   * the blocked merges will automatically resume from where they paused.
   *
   * @param mergeGate the gate that controls whether merges should be paused
   */
  public void setMergeGate(Gate mergeGate) {
    this.mergeGate = mergeGate;
  }

  /**
   * Returns whether merges are currently paused due to high disk usage.
   *
   * <p>When paused, ongoing merge I/O operations block in {@link PausableDirectory} until disk
   * usage drops and the gate reopens. New merges are also rejected by {@link
   * DiskUtilizationAwareMergePolicy}.
   *
   * @return true if merges are paused (gate is closed), false otherwise
   */
  public boolean isMergePaused() {
    return this.mergeGate.isClosed();
  }

  /**
   * Creates a new InstrumentedConcurrentMergeScheduler with default timeout values and
   * attribution metrics disabled.
   *
   * @param meterRegistry the meter registry for metrics
   */
  public InstrumentedConcurrentMergeScheduler(MeterRegistry meterRegistry) {
    this(
        meterRegistry,
        DEFAULT_CANCEL_MERGE_PER_THREAD_TIMEOUT_MS,
        DEFAULT_CANCEL_ALL_MERGES_PER_THREAD_TIMEOUT_MS,
        false);
  }

  /**
   * Creates a new InstrumentedConcurrentMergeScheduler with the specified timeout values and
   * attribution metrics disabled.
   */
  public InstrumentedConcurrentMergeScheduler(
      MeterRegistry meterRegistry,
      long cancelMergePerThreadTimeoutMs,
      long cancelAllMergesPerThreadTimeoutMs) {
    this(meterRegistry, cancelMergePerThreadTimeoutMs, cancelAllMergesPerThreadTimeoutMs, false);
  }

  /**
   * Creates a new InstrumentedConcurrentMergeScheduler with the specified timeout values.
   *
   * @param meterRegistry the meter registry for metrics
   * @param cancelMergePerThreadTimeoutMs timeout in milliseconds for waiting on each merge thread
   *     during cancellation of merges for a single index (used by cancelMerges()). Must be
   *     positive.
   * @param cancelAllMergesPerThreadTimeoutMs timeout in milliseconds for waiting on each merge
   *     thread during global cancellation of all merges (used by cancelAllMerges()). Must be
   *     positive.
   * @param enableAttributionMetrics if true, register per-merge ThreadMXBean memory and cpu time
   *     attribution metrics.
   * @throws IllegalArgumentException if either timeout is not positive
   */
  public InstrumentedConcurrentMergeScheduler(
      MeterRegistry meterRegistry,
      long cancelMergePerThreadTimeoutMs,
      long cancelAllMergesPerThreadTimeoutMs,
      boolean enableAttributionMetrics) {
    super();

    if (cancelMergePerThreadTimeoutMs <= 0) {
      throw new IllegalArgumentException(
          "cancelMergePerThreadTimeoutMs must be positive, got: " + cancelMergePerThreadTimeoutMs);
    }
    if (cancelAllMergesPerThreadTimeoutMs <= 0) {
      throw new IllegalArgumentException(
          "cancelAllMergesPerThreadTimeoutMs must be positive, got: "
              + cancelAllMergesPerThreadTimeoutMs);
    }
    this.cancelMergePerThreadTimeoutMs = cancelMergePerThreadTimeoutMs;
    this.cancelAllMergesPerThreadTimeoutMs = cancelAllMergesPerThreadTimeoutMs;

    this.metricsFactory = new MetricsFactory("mergeScheduler", meterRegistry);
    var luceneTag = ServerStatusDataExtractor.Scope.LUCENE.getTag();
    this.numMerges =
        this.metricsFactory.counter(LuceneMeterData.NUM_MERGES_KEY, Tags.of(luceneTag));
    this.runningMerges = this.metricsFactory.numGauge("currentlyRunningMerges", Tags.of(luceneTag));
    this.mergingDocs = this.metricsFactory.numGauge("currentlyMergingDocs", Tags.of(luceneTag));
    this.numSegmentsMerged =
        this.metricsFactory.counter(LuceneMeterData.NUM_SEGMENTS_MERGED_KEY, Tags.of(luceneTag));
    this.mergedDocs =
        this.metricsFactory.summary(LuceneMeterData.MERGED_DOCS_KEY, Tags.of(luceneTag));
    this.mergeTime =
        this.metricsFactory.timer(LuceneMeterData.SEGMENT_MERGE_TIME_KEY, Tags.of(luceneTag));
    this.mergeSize =
        this.metricsFactory.summary(
            LuceneMeterData.MERGE_SIZE_KEY, Tags.of(luceneTag), 0.5, 0.75, 0.9, 0.99);
    this.mergeResultSize =
        this.metricsFactory.summary(
            LuceneMeterData.MERGE_RESULT_SIZE_KEY, Tags.of(luceneTag), 0.5, 0.75, 0.9, 0.99);
    this.mergeCancellationTime =
        this.metricsFactory.timer(LuceneMeterData.MERGE_CANCELLATION_TIME_KEY, Tags.of(luceneTag));
    this.numMergesAborted =
        this.metricsFactory.counter(
            LuceneMeterData.NUM_MERGES_ABORTED_KEY, Tags.of(luceneTag));
    this.mergeElapsedStopwatches = new WeakHashMap<>();

    this.mergeAttribution =
        enableAttributionMetrics
            ? MergeAttribution.create(this.metricsFactory, meterRegistry)
            : Optional.empty();
  }

  // Same functionality as calling initialize() in MergeScheduler, used by PerIndexMergeScheduler to
  // initialize the wrapped per index InstrumentedConcurrentMergeScheduler
  public void setInfoStream(InfoStream infoStream) {
    this.infoStream = infoStream;
  }

  /**
   * Checks if merges have been cancelled for the specified index partition or globally.
   *
   * @param indexPartitionIdentifier the identifier of the index partition to check
   * @return true if merges have been cancelled for this index or globally, false otherwise
   */
  boolean isMergeCancelled(IndexPartitionIdentifier indexPartitionIdentifier) {
    // Check global cancellation first (fast path using atomic)
    if (this.allMergesCancelled.get()) {
      return true;
    }
    // Check per-index cancellation
    synchronized (this) {
      return this.cancelledIndices.contains(indexPartitionIdentifier);
    }
  }

  /**
   * Closes all merge threads associated with the given index partition.
   *
   * <p>The implementation is copied from sync() in ConcurrentMergeScheduler in Lucene code, with
   * only one additional check:
   * ((InstrumentedMergeThread) t).getIndexPartitionIdentifier().equals(indexPartitionIdentifier).
   */
  public void close(IndexPartitionIdentifier indexPartitionIdentifier) {
    @Var boolean interrupted = false;
    try {
      while (true) {
        @Var MergeThread toSync = null;
        synchronized (this) {
          for (MergeThread t : this.mergeThreads) {
            // In case a merge thread is calling us, don't try to sync on
            // itself, since that will never finish!
            if (t.isAlive()
                && t != Thread.currentThread()
                // Only wait for merge threads for the current index to finish
                && ((InstrumentedMergeThread) t)
                    .getIndexPartitionIdentifier()
                    .equals(indexPartitionIdentifier)) {
              toSync = t;
              break;
            }
          }
        }
        if (toSync != null) {
          try {
            toSync.join();
          } catch (InterruptedException ie) {
            // ignore this Exception, we will retry until all threads are dead
            interrupted = true;
          }
        } else {
          break;
        }
      }
    } finally {
      // Clean up the cancelled index entry to prevent unbounded growth of the set
      // during repeated index close/delete cycles
      synchronized (this) {
        this.cancelledIndices.remove(indexPartitionIdentifier);
      }
      // finally, restore interrupt status:
      if (interrupted) {
        Thread.currentThread().interrupt();
      }
    }
  }

  /** Result of a merge cancellation operation. */
  private record CancelMergesResult(
      int numMergesAborted,
      List<InstrumentedMergeThread> terminatedThreads,
      List<InstrumentedMergeThread> timedOutThreads,
      boolean interrupted) {

    boolean allMergesTerminated() {
      return this.timedOutThreads.isEmpty();
    }
  }

  /**
   * Core implementation for cancelling merges that match the given predicate.
   *
   * <p>This method:
   *
   * <ol>
   *   <li>Marks all running merges matching the predicate as aborted
   *   <li>Collects threads to wait for
   *   <li>Waits for threads to terminate with the specified timeout
   * </ol>
   *
   * @param threadPredicate predicate to filter which merge threads to cancel
   * @param perThreadTimeoutMs timeout in milliseconds to wait for each thread
   * @return result containing counts and lists of terminated/timed-out threads
   */
  private CancelMergesResult cancelMergesMatching(
      Predicate<InstrumentedMergeThread> threadPredicate, long perThreadTimeoutMs) {
    @Var int numMergesAborted = 0;
    @Var boolean interrupted = false;

    // Step 1: Mark running merges as aborted and collect threads to wait for.
    // Safety: The cast to InstrumentedMergeThread is safe because getMergeThread() is
    // overridden to always return InstrumentedMergeThread instances.
    List<MergeThread> threadsToWait = new ArrayList<>();
    synchronized (this) {
      for (MergeThread t : this.mergeThreads) {
        if (t.isAlive()) {
          InstrumentedMergeThread imt = (InstrumentedMergeThread) t;
          if (threadPredicate.test(imt)) {
            imt.merge.setAborted();
            // Interrupt the thread to wake it up if it's blocked (e.g., in
            // PausableDirectory.awaitOpen() waiting for disk space). Without this,
            // a merge thread paused on a closed gate would hang indefinitely since
            // setAborted() only sets a flag but doesn't unblock waiting threads.
            t.interrupt();
            numMergesAborted++;
            LOG.info(
                "Marked merge as aborted and interrupted for index: {}, segments: {}",
                imt.getIndexPartitionIdentifier(),
                imt.merge.segments);
            // Collect thread for waiting, but skip if it's the current thread to avoid deadlock
            if (t != Thread.currentThread()) {
              threadsToWait.add(t);
            }
          }
        }
      }
    }

    // Step 2: Wait for all threads to complete.
    // The merge threads will detect the abort signal and stop at the next checkpoint.
    // Threads blocked on Gate.awaitOpen() (in PausableDirectory) will be woken up by
    // the interrupt sent in Step 1.
    // Note: joins are sequential, so worst-case total wait time is
    // threadsToWait.size() * perThreadTimeoutMs. However, threads that
    // terminate quickly will return from join() immediately, and all threads are
    // processing the abort signal concurrently.
    List<InstrumentedMergeThread> terminatedThreads = new ArrayList<>();
    List<InstrumentedMergeThread> timedOutThreads = new ArrayList<>();
    for (MergeThread t : threadsToWait) {
      try {
        t.join(perThreadTimeoutMs);
      } catch (InterruptedException ie) {
        // Record that we were interrupted, but still check if the thread is alive below
        interrupted = true;
      }
      // Always check if thread is still alive after join (whether it completed, timed out,
      // or was interrupted). This ensures we don't incorrectly treat still-running threads
      // as terminated, which would cause stopwatches to be cleared prematurely.
      InstrumentedMergeThread imt = (InstrumentedMergeThread) t;
      if (t.isAlive()) {
        timedOutThreads.add(imt);
        LOG.warn(
            "Merge thread {} did not terminate within {} ms timeout for index: {}, "
                + "continuing without waiting further",
            t.getName(),
            perThreadTimeoutMs,
            imt.getIndexPartitionIdentifier());
      } else {
        terminatedThreads.add(imt);
      }
    }

    return new CancelMergesResult(
        numMergesAborted, terminatedThreads, timedOutThreads, interrupted);
  }

  /**
   * Aborts all running merges for the specified index partition, then waits for them to complete.
   *
   * <p>This method will:
   *
   * <ol>
   *   <li>Block new merges from being scheduled for this index
   *   <li>Mark all running merges for this index as aborted via {@link
   *       MergePolicy.OneMerge#setAborted()}
   *   <li>Wait for running merge threads to detect the abort signal and stop
   * </ol>
   *
   * <p><b>Note on pending merges:</b> Pending merges (those queued in IndexWriter but not yet
   * started) are not explicitly aborted by this method. However, new merge scheduling is blocked,
   * so no new merge threads will be started. Pending merges will be discarded when the IndexWriter
   * is closed.
   *
   * <p>This is similar to {@link ConcurrentMergeScheduler#close()} but only affects merges for the
   * specified index partition, allowing other indices to continue merging.
   *
   * @param indexPartitionIdentifier the identifier of the index partition whose merges should be
   *     cancelled
   * @return true if all merge threads terminated within the timeout, false if some threads are
   *     still running after the timeout
   */
  public boolean cancelMerges(IndexPartitionIdentifier indexPartitionIdentifier) {
    Stopwatch cancellationStopwatch = Stopwatch.createStarted();

    LOG.info("Starting merge cancellation for index: {}", indexPartitionIdentifier);

    // Block new merges from being scheduled for this index
    synchronized (this) {
      this.cancelledIndices.add(indexPartitionIdentifier);
    }
    LOG.info("Blocked new merge scheduling for index: {}", indexPartitionIdentifier);

    // Cancel merges matching this index partition
    CancelMergesResult result =
        cancelMergesMatching(
            imt -> imt.getIndexPartitionIdentifier().equals(indexPartitionIdentifier),
            this.cancelMergePerThreadTimeoutMs);

    try {
      // Step 3: Only clear stopwatch entries for merges whose threads have actually terminated.
      // If some threads timed out, their entries remain in the stopwatch so that
      // mergeElapsedSeconds continues to reflect the stuck merge, making it visible in monitoring.
      // Note: The stopwatch is keyed by GenerationId (shared across partitions), so we must only
      // remove entries for the specific merges that were cancelled for this partition, not clear
      // the entire stopwatch which would wipe entries for other partitions of the same index.
      MergeStopwatch stopwatch = getMergeStopwatch(indexPartitionIdentifier);
      synchronized (stopwatch) {
        for (InstrumentedMergeThread imt : result.terminatedThreads()) {
          stopwatch.runningMerges.remove(imt.merge);
        }
      }
      if (result.allMergesTerminated()) {
        LOG.info(
            "Cleared {} merge stopwatch entries for index: {}",
            result.terminatedThreads().size(),
            indexPartitionIdentifier);
      } else {
        LOG.warn(
            "Merge cancellation timed out for index: {}, {} merge thread(s) still running. "
                + "mergeElapsedSeconds will continue to reflect stuck merges.",
            indexPartitionIdentifier,
            result.timedOutThreads().size());
      }
    } finally {
      cancellationStopwatch.stop();
      long elapsedMs = cancellationStopwatch.elapsed(TimeUnit.MILLISECONDS);

      // Record metrics
      this.mergeCancellationTime.record(elapsedMs, TimeUnit.MILLISECONDS);
      this.numMergesAborted.increment(result.numMergesAborted());

      LOG.info(
          "Completed merge cancellation for index: {}, aborted {} merge(s), elapsed time: {} ms",
          indexPartitionIdentifier,
          result.numMergesAborted(),
          elapsedMs);

      // finally, restore interrupt status:
      if (result.interrupted()) {
        Thread.currentThread().interrupt();
      }
    }
    return result.allMergesTerminated();
  }

  /**
   * Aborts all running merges across ALL indices, then waits for them to complete.
   *
   * <p>This method is intended for global shutdown scenarios where all merge activity should be
   * terminated immediately. Unlike {@link #cancelMerges(IndexPartitionIdentifier)}, this method
   * affects merges from all indices on the node.
   *
   * <p>This method will:
   *
   * <ol>
   *   <li>Block new merges from being scheduled for all indices
   *   <li>Mark all running merges across all indices as aborted via {@link
   *       MergePolicy.OneMerge#setAborted()}
   *   <li>Wait for all merge threads to detect the abort signal and stop
   * </ol>
   *
   * <p>This should be called during mongot shutdown to ensure all merge activity is terminated
   * before the process exits.
   */
  public void cancelAllMerges() {
    Stopwatch cancellationStopwatch = Stopwatch.createStarted();

    LOG.info("Starting global merge cancellation for all indices");

    // Block new merges globally
    this.allMergesCancelled.set(true);
    LOG.info("Blocked new merge scheduling globally for all indices");

    // Cancel all merges (predicate always returns true)
    CancelMergesResult result =
        cancelMergesMatching(imt -> true, this.cancelAllMergesPerThreadTimeoutMs);

    try {
      // Step 3: Only clear stopwatch entries for merges whose threads have actually terminated.
      // If some threads timed out, their entries remain in the stopwatch so that
      // mergeElapsedSeconds continues to reflect the stuck merge, making it visible in monitoring.
      if (result.allMergesTerminated()) {
        // All threads terminated - safe to clear all stopwatches
        synchronized (this.mergeElapsedStopwatches) {
          for (MergeStopwatch stopwatch : this.mergeElapsedStopwatches.values()) {
            synchronized (stopwatch) {
              stopwatch.runningMerges.clear();
            }
          }
        }
        LOG.info("Cleared all merge stopwatches for all indices");
      } else {
        // Some threads are still alive - don't clear their stopwatch entries
        // The still-running merges will remain in the stopwatch for monitoring
        LOG.warn(
            "Global merge cancellation timed out, {} merge thread(s) still running. "
                + "mergeElapsedSeconds will continue to reflect stuck merges.",
            result.timedOutThreads().size());
      }
    } finally {
      cancellationStopwatch.stop();
      long elapsedMs = cancellationStopwatch.elapsed(TimeUnit.MILLISECONDS);

      // Record metrics
      this.mergeCancellationTime.record(elapsedMs, TimeUnit.MILLISECONDS);
      this.numMergesAborted.increment(result.numMergesAborted());

      LOG.info(
          "Completed global merge cancellation, aborted {} merge(s), elapsed time: {} ms",
          result.numMergesAborted(),
          elapsedMs);

      // finally, restore interrupt status:
      if (result.interrupted()) {
        Thread.currentThread().interrupt();
      }
    }
  }

  /**
   * Wraps the directory for merge operations when cancel merge is enabled.
   *
   * <p>When {@code cancelMergeEnabled} is true, the directory is wrapped with:
   * <ol>
   *   <li>PausableDirectory - blocks writes when disk usage is high, resumes when it drops</li>
   *   <li>AbortableDirectory - throws IOException when merge is aborted</li>
   * </ol>
   *
   * <p>When {@code cancelMergeEnabled} is false, the directory is returned unwrapped.
   *
   * @param merge the merge operation
   * @param in the directory to wrap
   * @param cancelMergeEnabled whether to wrap with PausableDirectory and AbortableDirectory
   * @return the wrapped directory, or the original directory if cancel merge is disabled
   */
  public Directory wrapForMerge(
      MergePolicy.OneMerge merge, Directory in, boolean cancelMergeEnabled) {
    if (!cancelMergeEnabled) {
      return in;
    }
    // Wrap with PausableDirectory for disk-based pause/resume support
    // This allows merges to block when disk usage is high and resume when it drops
    @Var Directory wrapped = new PausableDirectory(in, this.mergeGate);

    // Then wrap with AbortableDirectory to enable IO-level interruption
    // This allows merges to be aborted quickly even during long-running operations
    // like HNSW graph building, which may not have frequent checkpoints
    wrapped = new AbortableDirectory(wrapped, merge);
    return wrapped;
  }

  @Override
  public Directory wrapForMerge(MergePolicy.OneMerge merge, Directory in) {
    // Default behavior: no wrapping (feature flag disabled by default)
    // The PerIndexPartitionMergeScheduler will call wrapForMerge with the cancelMergeEnabled flag
    return in;
  }

  @Override
  protected synchronized ConcurrentMergeScheduler.MergeThread getMergeThread(
      MergeSource mergeSource, MergePolicy.OneMerge merge) {
    String threadNamePrefix = "Lucene Merge Thread #" + this.mergeThreadCount++;
    var taggedMergeSource = (TaggedMergeSource) mergeSource;
    IndexPartitionIdentifier indexPartitionIdentifier =
        taggedMergeSource.getIndexPartitionIdentifier();

    // Check if merges have been cancelled for this index. This check is synchronized (via this
    // method's synchronized modifier) with cancelMerges() which also synchronizes on 'this'.
    // This prevents the race condition where a merge request slips through after cancelMerges()
    // is called but before the merge thread is created.
    if (this.cancelledIndices.contains(indexPartitionIdentifier) || this.allMergesCancelled.get()) {
      // Mark the merge as aborted immediately so it will be skipped when the thread runs
      merge.setAborted();
      LOG.debug(
          "Marking merge as aborted during thread creation for cancelled index: {}, segments: {}",
          indexPartitionIdentifier,
          merge.segments);
    }

    ConcurrentMergeScheduler.MergeThread thread =
        new InstrumentedMergeThread(
            mergeSource, merge, indexPartitionIdentifier);
    thread.setDaemon(true);
    thread.setName(threadNamePrefix + " " + indexPartitionIdentifier.toString());
    return thread;
  }

  // XXX consider mapping by the merge itself and taking the max.

  /** A Stopwatch wrapper that can be used as an object for a gauge export. */
  private static class MergeStopwatch {
    private final Map<MergePolicy.OneMerge, Stopwatch> runningMerges = new HashMap<>();

    public synchronized double elapsedSeconds() {
      return this.runningMerges.values().stream()
          .mapToLong(s -> s.elapsed(TimeUnit.SECONDS))
          .max()
          .orElse(0L);
    }

    public synchronized void startOneMerge(MergePolicy.OneMerge merge) {
      this.runningMerges.computeIfAbsent(merge, unused -> Stopwatch.createStarted());
    }

    public synchronized void stopOneMerge(MergePolicy.OneMerge merge) {
      this.runningMerges.remove(merge);
    }
  }

  private MergeStopwatch getMergeStopwatch(IndexPartitionIdentifier indexPartitionIdentifier) {
    synchronized (this.mergeElapsedStopwatches) {
      // Find the stopwatch for the current index, or create a new stopwatch and a gauge for export
      // if not present.
      var mergeStopwatch =
          this.mergeElapsedStopwatches.computeIfAbsent(
              indexPartitionIdentifier.getGenerationId(),
              genId -> {
                var stopwatch = new MergeStopwatch();
                this.metricsFactory.objectValueGauge(
                    "mergeElapsedSeconds",
                    stopwatch,
                    MergeStopwatch::elapsedSeconds,
                    Tags.of("generationId logString", genId.uniqueString()));
                return stopwatch;
              });
      return mergeStopwatch;
    }
  }

  private class InstrumentedMergeThread extends MergeThread {

    private final MergePolicy.OneMerge merge;
    private final IndexPartitionIdentifier indexPartitionIdentifier;
    private static final Logger LOG = LoggerFactory.getLogger(InstrumentedMergeThread.class);

    public IndexPartitionIdentifier getIndexPartitionIdentifier() {
      return this.indexPartitionIdentifier;
    }

    private InstrumentedMergeThread(
        MergeSource mergeSource,
        MergePolicy.OneMerge merge,
        IndexPartitionIdentifier indexPartitionIdentifier) {
      super(mergeSource, merge);
      this.merge = merge;
      this.indexPartitionIdentifier = indexPartitionIdentifier;
    }

    @Override
    @SuppressWarnings("DoNotCall") // allow calling Thread.run()
    public void run() {
      // Log input segments before merge
      List<SegmentSize> inputSegments = new ArrayList<>();
      for (SegmentCommitInfo info : this.merge.segments) {
        @Var long size;
        try {
          size = info.sizeInBytes();
        } catch (IOException e) {
          size = -1L; // Use -1 if unable to read size
        }
        inputSegments.add(new SegmentSize(info.info.name, size));
      }
      LOG.atDebug()
          .addKeyValue("indexId", this.indexPartitionIdentifier.getGenerationId().indexId)
          .addKeyValue("inputSegments", inputSegments)
          .log("[Merge Start]");
      InstrumentedConcurrentMergeScheduler.this.numMerges.increment();
      InstrumentedConcurrentMergeScheduler.this.numSegmentsMerged.increment(
          this.merge.segments.size());
      InstrumentedConcurrentMergeScheduler.this.mergeSize.record(this.merge.totalBytesSize());
      InstrumentedConcurrentMergeScheduler.this.mergedDocs.record(this.merge.totalNumDocs());

      InstrumentedConcurrentMergeScheduler.this.runningMerges.incrementAndGet();
      InstrumentedConcurrentMergeScheduler.this.mergingDocs.addAndGet(this.merge.totalNumDocs());
      MergeStopwatch stopwatch =
          InstrumentedConcurrentMergeScheduler.this.getMergeStopwatch(
              this.indexPartitionIdentifier);
      stopwatch.startOneMerge(this.merge);

      // Per-merge ThreadMXBean attribution. The snapshot must happen before any merge work so
      // the metric captures the entire merge.
      Optional<MergeAttribution> attribution =
          InstrumentedConcurrentMergeScheduler.this.mergeAttribution;
      long threadId = Thread.currentThread().threadId();
      long allocBefore = attribution.map(a -> a.snapshotAllocated(threadId)).orElse(0L);
      long cpuBefore = attribution.map(a -> a.snapshotCpu(threadId)).orElse(0L);
      attribution.ifPresent(a -> a.markStarted(threadId));
      String indexType = this.indexPartitionIdentifier.getIndexType();

      try {
        Timed.runnable(InstrumentedConcurrentMergeScheduler.this.mergeTime, super::run);
      } catch (Exception e) {
        LOG.atWarn()
            .addKeyValue("indexId", this.indexPartitionIdentifier.getGenerationId().indexId)
            .setCause(e)
            .log("Exception during merge");
      } finally {
        stopwatch.stopOneMerge(this.merge);
        // Decrease the counters no matter run() throws error or not to guarantee the correctness.
        InstrumentedConcurrentMergeScheduler.this.runningMerges.decrementAndGet();
        InstrumentedConcurrentMergeScheduler.this.mergingDocs.addAndGet(-this.merge.totalNumDocs());
        attribution.ifPresent(
            a -> a.recordEnd(threadId, allocBefore, cpuBefore, indexType));
      }

      try {
        var commitInfo = this.merge.getMergeInfo();
        if (commitInfo != null) {
          InstrumentedConcurrentMergeScheduler.this.mergeResultSize.record(
              commitInfo.sizeInBytes());
          // Log the details of the output segment after the merge
          LOG.atDebug()
              .addKeyValue("indexId", this.indexPartitionIdentifier.getGenerationId().indexId)
              .addKeyValue("outputSegmentName", commitInfo.info.name)
              .addKeyValue("outputSegmentSize", commitInfo.sizeInBytes())
              .addKeyValue("inputSegments", inputSegments)
              .log("[Merge End]");
        }
      } catch (Exception e) {
        // ok to ignore as this is expected only in case of IO errors or merge abort
      }
    }
  }

  /**
   * Per-merge ThreadMXBean attribution. Tracks live merge thread ids and lazily registers
   * per-merge {@code merge.allocatedBytes} / {@code merge.cpuTimeNanos} histograms tagged by
   * {@code indexType} on first sample. Created only when {@code MERGE_ATTRIBUTION_METRICS} is
   * enabled.
   */
  static final class MergeAttribution {

    private static final String MERGE_SUBSYSTEM = "merge";
    private static final String MERGE_POOL_NAME = "lucene-merge";
    // Matches IndexTypeData.INDEX_TYPE_TAG_NAME
    private static final String INDEX_TYPE_TAG = "indexType";

    /**
     * Bucket upper bounds for per-merge heap allocation, in bytes.
     */
    private static final double[] ALLOCATED_BYTES_BUCKETS = {
      Bytes.ofMebi(16).toBytes(),
      Bytes.ofMebi(64).toBytes(),
      Bytes.ofMebi(256).toBytes(),
      Bytes.ofGibi(1).toBytes(),
      Bytes.ofGibi(4).toBytes(),
      Bytes.ofGibi(16).toBytes(),
      Bytes.ofGibi(64).toBytes()
    };

    /** Bucket upper bounds for per-merge CPU time, in nanoseconds.*/
    private static final double[] CPU_TIME_NANOS_BUCKETS = {
      Duration.ofMillis(100).toNanos(),
      Duration.ofSeconds(1).toNanos(),
      Duration.ofSeconds(10).toNanos(),
      Duration.ofMinutes(1).toNanos(),
      Duration.ofMinutes(5).toNanos(),
      Duration.ofMinutes(15).toNanos(),
      Duration.ofMinutes(30).toNanos()
    };

    private final ThreadMXBean threadMxBean;
    private final LiveThreadIdsRegistry liveMergeThreadIds;
    private final MetricsFactory metricsFactory;
    private final ConcurrentHashMap<String, DistributionSummary> allocatedBytesByIndexType =
        new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, DistributionSummary> cpuTimeNanosByIndexType =
        new ConcurrentHashMap<>();

    private MergeAttribution(
        ThreadMXBean threadMxBean,
        LiveThreadIdsRegistry liveMergeThreadIds,
        MetricsFactory metricsFactory) {
      this.threadMxBean = threadMxBean;
      this.liveMergeThreadIds = liveMergeThreadIds;
      this.metricsFactory = metricsFactory;
    }

    static Optional<MergeAttribution> create(
        MetricsFactory metricsFactory, MeterRegistry meterRegistry) {
      if (!(ManagementFactory.getThreadMXBean() instanceof ThreadMXBean bean)) {
        LOG.warn("Per-thread tracking unavailable on this JVM, merge attribution disabled.");
        return Optional.empty();
      }
      if (!bean.isThreadAllocatedMemorySupported() || !bean.isThreadCpuTimeSupported()) {
        LOG.warn(
            "Per-thread allocation or CPU tracking unsupported, merge attribution disabled.");
        return Optional.empty();
      }
      try {
        bean.setThreadAllocatedMemoryEnabled(true);
        bean.setThreadCpuTimeEnabled(true);
      } catch (UnsupportedOperationException | SecurityException e) {
        LOG.warn("Per-thread tracking refused enabling, merge attribution disabled.");
        return Optional.empty();
      }

      LiveThreadIdsRegistry liveThreadIds = new LiveThreadIdsRegistry();

      // Aggregate live-merge FunctionCounters.
      ThreadPoolResourceMetrics.create(MERGE_SUBSYSTEM)
          .register(MERGE_POOL_NAME, liveThreadIds, meterRegistry);

      return Optional.of(new MergeAttribution(bean, liveThreadIds, metricsFactory));
    }

    long snapshotAllocated(long threadId) {
      return this.threadMxBean.getThreadAllocatedBytes(threadId);
    }

    long snapshotCpu(long threadId) {
      return this.threadMxBean.getThreadCpuTime(threadId);
    }

    void markStarted(long threadId) {
      this.liveMergeThreadIds.register(threadId);
    }

    void recordEnd(long threadId, long allocBefore, long cpuBefore, String indexType) {
      long allocDelta = this.threadMxBean.getThreadAllocatedBytes(threadId) - allocBefore;
      long cpuDelta = this.threadMxBean.getThreadCpuTime(threadId) - cpuBefore;
      // Negative delta only happens if the thread died between start and end (after = -1).
      // Skip recording in that case to avoid corrupting DistributionSummary statistics.
      if (allocDelta >= 0) {
        allocatedBytesHistogramFor(indexType).record(allocDelta);
      }
      if (cpuDelta >= 0) {
        cpuTimeNanosHistogramFor(indexType).record(cpuDelta);
      }
      // The threads are re-used across merges, so we do not need to remove from liveMergeThreadIds.
      // ThreadPoolResourceMetrics will prune the thread ids if the thread dies for any reason.
    }

    private DistributionSummary allocatedBytesHistogramFor(String indexType) {
      return this.allocatedBytesByIndexType.computeIfAbsent(
          indexType,
          t ->
              this.metricsFactory.histogram(
                  "merge.allocatedBytes",
                  Tags.of(ServerStatusDataExtractor.Scope.LUCENE.getTag())
                      .and(INDEX_TYPE_TAG, t),
                  ALLOCATED_BYTES_BUCKETS));
    }

    private DistributionSummary cpuTimeNanosHistogramFor(String indexType) {
      return this.cpuTimeNanosByIndexType.computeIfAbsent(
          indexType,
          t ->
              this.metricsFactory.histogram(
                  "merge.cpuTimeNanos",
                  Tags.of(ServerStatusDataExtractor.Scope.LUCENE.getTag())
                      .and(INDEX_TYPE_TAG, t),
                  CPU_TIME_NANOS_BUCKETS));
    }
  }
}
