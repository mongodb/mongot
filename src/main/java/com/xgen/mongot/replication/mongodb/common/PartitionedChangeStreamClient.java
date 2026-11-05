package com.xgen.mongot.replication.mongodb.common;

import static com.xgen.mongot.util.Check.checkArg;
import static com.xgen.mongot.util.Check.checkState;

import com.google.common.annotations.VisibleForTesting;
import com.xgen.mongot.metrics.MetricsFactory;
import com.xgen.mongot.util.mongodb.BatchMongoClient;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.bson.BsonDocument;
import org.bson.BsonTimestamp;
import org.bson.RawBsonDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * PartitionedChangeStreamClient reads one collection's change stream as N separate partitions and
 * exposes them through the same getNext() interface as an ordinary change stream. Each call returns
 * one batch from whichever partition has finished fetching. The batch is tagged with that partition
 * and ordinary batches carry the minimum safe checkpoint across all partitions. The final lifecycle
 * batch retains its actual server token. Each reader's checkpoint starts at the shared restart
 * token and advances on each non-lifecycle batch. Partitions are scheduled independently, up to the
 * shared executor's capacity. Readers more than 60 seconds ahead of the slowest unfinished reader
 * stop submitting new fetches until it catches up. Already submitted fetches may overshoot this
 * gap.
 *
 * <p>Each document belongs to exactly one reader. Batches retain that reader's order, while readers
 * advance independently. Reader creation must apply the partition filter and reassemble split
 * events before delivery.
 *
 * <p>This client does not track confirmed indexing progress. Each reader stops at its first
 * lifecycle batch and retains that batch while other readers catch up. Once all readers stop, their
 * remaining documents are returned with the minimum safe checkpoint before a single copy of the
 * lifecycle events is returned with its actual token.
 *
 * <p>Retries must be handled by the low-level change stream clients. If getNext() throws, the
 * caller must close this client and resume from the last confirmed checkpoint. Retrying this client
 * may skip an undelivered rename batch. Tokens used for pacing or checkpoint selection must contain
 * a decodable timestamp; a malformed token fails the read instead of producing an unsafe
 * checkpoint.
 */
public class PartitionedChangeStreamClient<E extends Exception>
    implements ChangeStreamMongoClient<E> {
  // In the future, we may want to make this configurable and dynamic.
  private static final Duration DEFAULT_MAX_OPTIME_GAP = Duration.ofSeconds(60);

  private static final Logger LOG = LoggerFactory.getLogger(PartitionedChangeStreamClient.class);

  /** Creates partition readers with caller-selected resumption and split-event reassembly. */
  @FunctionalInterface
  public interface ReaderFactory<E extends Exception> {
    /**
     * Creates a reader using the supplied Mongo client. The caller chooses how to resume; {@code
     * start} is the saved token's timestamp. The pipeline must apply the partition filter and emit
     * complete, reassembled events.
     */
    ChangeStreamMongoClient<E> create(
        BatchMongoClient mongoClient, ChangeStreamPartition partition, BsonTimestamp start)
        throws E;
  }

  private final List<Reader<E>> readers;
  private final long maxOptimeGapSeconds;

  /** Completed readers in completion order, or an empty entry to wake getNext() on closure. */
  private final BlockingQueue<Optional<Reader<E>>> completedReaders;

  /** Shared across clients and owned by the ChangeStreamManager. */
  private final Executor executor;

  private final WrapIfThrows<E> exceptionWrapper;

  /**
   * Whether close() has started. Atomic so shutdown does not need the monitor held by getNext().
   * True does not imply that reader cleanup has finished.
   */
  private final AtomicBoolean closed;

  private final Metrics metrics;

  /**
   * How long getNext waits for a completion before re-running the pass. Periodic wakes keep the
   * pacing metrics sampling while every reader is stalled or paused, and would otherwise produce no
   * completions.
   */
  @VisibleForTesting Duration getNextPollInterval = Duration.ofSeconds(1);

  /** Next reader whose retained batch will be returned after all readers stop. */
  private int nextFinalReader;

  /**
   * Creates partition readers through the supplied factory and seeds their safe checkpoint and
   * pacing at the saved token. The factory owns cursor resumption.
   *
   * <p>Takes ownership of the created readers. The supplied Mongo client is shared and remains open
   * when this client closes. Closing a reader aborts any active fetch.
   *
   * <p>Uses the supplied executor without taking ownership. It must be separate from the executor
   * calling {@link #getNext()}, which waits for a reader to finish. Each reader has at most one
   * outstanding fetch or buffered batch, retained including between {@link #getNext()} calls. A
   * fetch combines split rename and invalidate batches before completing. Readers may wait for a
   * worker when the shared pool is busy.
   *
   * <p>If initialization fails, closes any readers already created before propagating the failure.
   * All {@code resume} overloads delegate here.
   */
  public static <E extends Exception> PartitionedChangeStreamClient<E> resume(
      int partitionCount,
      BsonDocument resumeToken,
      BatchMongoClient mongoClient,
      ReaderFactory<E> readerFactory,
      Executor executor,
      WrapIfThrows<E> exceptionWrapper,
      MetricsFactory metricsFactory)
      throws E {
    return exceptionWrapper.wrapIfThrows(
        () -> {
          var clients = new ArrayList<ChangeStreamMongoClient<E>>();
          try {
            checkArg(partitionCount > 0, "partitionCount must be positive");
            BsonTimestamp start = ResumeTokenUtils.opTimeFromResumeToken(resumeToken);
            BsonDocument restartToken = resumeToken.clone();
            for (int id = 0; id < partitionCount; id++) {
              clients.add(
                  readerFactory.create(
                      mongoClient, new ChangeStreamPartition(id, partitionCount), start));
            }
            return new PartitionedChangeStreamClient<>(
                clients,
                executor,
                exceptionWrapper,
                restartToken,
                start,
                DEFAULT_MAX_OPTIME_GAP,
                metricsFactory);
          } catch (Exception | Error failure) {
            for (var client : clients) {
              try {
                client.close();
              } catch (RuntimeException closeFailure) {
                if (closeFailure != failure) {
                  failure.addSuppressed(closeFailure);
                }
              }
            }
            throw failure;
          }
        });
  }

  /** Test-only overload; production callers must pass a MetricsFactory so metrics are scraped. */
  public static <E extends Exception> PartitionedChangeStreamClient<E> resume(
      int partitionCount,
      BsonDocument resumeToken,
      BatchMongoClient mongoClient,
      ReaderFactory<E> readerFactory,
      Executor executor,
      WrapIfThrows<E> exceptionWrapper)
      throws E {
    return resume(
        partitionCount,
        resumeToken,
        mongoClient,
        readerFactory,
        executor,
        exceptionWrapper,
        new MetricsFactory("changestream", io.micrometer.core.instrument.Metrics.globalRegistry));
  }

  /**
   * Takes ownership of preconfigured readers and borrows their executor. Tests can control fetch
   * scheduling; production callers use {@link #resume}.
   */
  @VisibleForTesting
  PartitionedChangeStreamClient(
      List<ChangeStreamMongoClient<E>> clients,
      Executor executor,
      WrapIfThrows<E> exceptionWrapper,
      BsonDocument resumeToken,
      BsonTimestamp start,
      Duration maxOptimeGap,
      MetricsFactory metricsFactory) {
    this.metrics = new Metrics(metricsFactory);
    checkArg(
        !maxOptimeGap.isNegative()
            && maxOptimeGap.equals(Duration.ofSeconds(maxOptimeGap.toSeconds())),
        "maxOptimeGap must be a nonnegative whole-second duration");
    checkArg(!clients.isEmpty(), "clients must contain at least one reader");
    var readers = new ArrayList<Reader<E>>();
    for (int id = 0; id < clients.size(); id++) {
      readers.add(
          new Reader<>(
              clients.get(id),
              new ChangeStreamPartition(id, clients.size()),
              resumeToken,
              start,
              () -> this.metrics.pauses.increment()));
    }
    this.readers = readers;
    this.maxOptimeGapSeconds = maxOptimeGap.toSeconds();
    this.completedReaders = new LinkedBlockingQueue<>();
    this.executor = executor;
    this.exceptionWrapper = exceptionWrapper;
    this.closed = new AtomicBoolean(false);
    this.nextFinalReader = 0;
  }

  @VisibleForTesting
  PartitionedChangeStreamClient(
      List<ChangeStreamMongoClient<E>> clients,
      Executor executor,
      WrapIfThrows<E> exceptionWrapper,
      BsonDocument resumeToken,
      BsonTimestamp start,
      Duration maxOptimeGap) {
    this(
        clients,
        executor,
        exceptionWrapper,
        resumeToken,
        start,
        maxOptimeGap,
        new MetricsFactory("changestream", io.micrometer.core.instrument.Metrics.globalRegistry));
  }

  /*
   * Each getNext() call follows these steps:
   *
   * 1. Check closure. If all readers finished, drain their retained documents in partition order,
   *    with lifecycle events only in the final batch.
   * 2. Call submitFetchForReaders(). Each reader checks its lifecycle state, pending fetch, and
   *    optime gap from the slowest unfinished reader. A completed, unread result still occupies
   *    the pending slot. Pacing is recalculated on every submission; pacingPaused is
   *    metrics-only.
   * 3. Take the next completed reader, waiting if necessary. A shutdown marker wakes this wait
   *    and causes the call to throw.
   * 4. Take the fetched batch, freeing its reader's slot, or propagate the fetch failure so the
   *    caller can close this client and resume from its checkpoint.
   * 5. If the reader reached a lifecycle boundary, retain its entire batch and repeat these steps
   *    for the remaining readers. Otherwise, return the batch with the minimum safe checkpoint and
   *    its original partition metadata. Retained lifecycle batches are not returned until every
   *    reader reaches the boundary. Each reader combines split rename and invalidate batches within
   *    its fetch.
   * 6. After the terminal batch is returned, further calls fail without submitting more fetches.
   * 7. OTHER does not itself finish a reader. If there is no lifecycle suffix, return the batch
   *    without waiting for other readers to observe the event or catch up. The index manager
   *    requests a resync, so ordering across readers is unnecessary.
   *    For example, [INSERT, OTHER, UPDATE, DELETE] is returned unchanged. As with a single
   *    reader, decoding fails on OTHER and requests a resync before indexing the batch.
   *
   * Example: A has a ready batch, B is idle, and C is still fetching. Submit B's fetch before
   * returning A's batch. A can fetch again on the next call. Completed document batches remain
   * deliverable regardless of pacing eligibility. close() wakes a waiting call and closes readers.
   */
  @Override
  public synchronized ChangeStreamBatch getNext() throws E {
    checkState(!this.closed.get(), "Change stream client is closed");
    return this.exceptionWrapper.wrapIfThrows(
        () -> {
          // One call returns one batch. If a reader reaches a lifecycle event before the others,
          // save its batch, stop that reader, and keep waiting for results from the other readers.
          // For example, consider there are three readers for an index: A, B and C. In this case,
          // we will retain A's and B's lifecycle batches, then return C's ordinary batch.
          // If C also reaches the lifecycle boundary, all readers are finished and the next loop
          // iteration returns a retained batch through handleAllReadersFinished().
          // take() blocks when no result is ready, so this loop does not spin while waiting.
          while (true) {
            checkState(!this.closed.get(), "Change stream client is closed");
            // Wait for every reader to finish its lifecycle batch. Return all preceding
            // documents before delivering lifecycle events once.
            if (allReadersFinished()) {
              return handleAllReadersFinished();
            }

            // Submit a fetch for every reader that is not finished.
            submitFetchForReaders();

            var completed =
                this.completedReaders.poll(
                    this.getNextPollInterval.toMillis(), TimeUnit.MILLISECONDS);
            if (completed == null) {
              // No reader completed. Loop, so the pacing gap keeps being sampled while every
              // reader is stalled or paused.
              continue;
            }
            Reader<E> reader =
                completed.orElseThrow(
                    () -> new IllegalStateException("Change stream client is closed"));
            checkState(!this.closed.get(), "Change stream client is closed");
            // Taking the fetched batch frees the pending slot; a lifecycle batch stops this reader.
            ChangeStreamBatch batch = reader.takeFetchedBatch();

            checkState(!this.closed.get(), "Change stream client is closed");
            if (reader.isFinished()) {
              continue;
            }
            return withCheckpoint(batch, safeCheckpoint());
          }
        });
  }

  /**
   * Lifecycle events stop a reader until the other readers catch up. OTHER does not itself stop a
   * reader and must reach the index manager to trigger resync.
   */
  private static int findLifecycleStart(ChangeStreamBatch batch) {
    return ChangeStreamDocumentUtils.indexOfLifecycleEvent(batch.getRawEvents());
  }

  /** Checks whether every reader has reached the lifecycle boundary. */
  private boolean allReadersFinished() {
    return this.readers.stream().allMatch(Reader::isFinished);
  }

  /** Drains retained documents before returning the shared lifecycle events once. */
  private ChangeStreamBatch handleAllReadersFinished() {
    checkState(
        this.nextFinalReader < this.readers.size(), "All lifecycle batches have been returned");
    // For example, suppose the retained batches are:
    //   Reader A: [A1, rename, invalidate]
    //   Reader B: [B1, rename, invalidate]
    //   Reader C: [C1, rename, invalidate]
    // Successive getNext() calls return these batches in partition order:
    //   Call 1: [A1]
    //   Call 2: [B1]
    //   Call 3: [C1, rename, invalidate]
    //
    // Return lifecycle events only with the last reader's documents. ChangeStreamIndexManager uses
    // the partition tracker's confirmed low-water mark when committing this mixed final batch.
    for (int readerIndex = this.nextFinalReader;
        readerIndex < this.readers.size() - 1;
        readerIndex++) {
      var reader = this.readers.get(readerIndex);
      var result = reader.lifecycleResult.orElseThrow();
      var batch = result.batch();
      this.nextFinalReader = readerIndex + 1;
      if (result.lifecycleStart() == 0) {
        continue;
      }
      var documents = batch.getRawEvents().subList(0, result.lifecycleStart());
      return withCheckpoint(batchWithEvents(batch, documents), safeCheckpoint());
    }

    var reader = this.readers.getLast();
    checkState(!this.closed.get(), "Change stream client is closed");
    this.nextFinalReader++;
    return reader.lifecycleResult.orElseThrow().batch();
  }

  // Each reader records the latest non-lifecycle position it has passed.
  // Ordinary batches expose the minimum of those positions so no partition is skipped on restart.
  //
  // Checkpoint safety rules:
  // 1. Each reader starts with the shared restart token.
  // 2. Each non-lifecycle batch replaces that reader's safe token with its post-batch token.
  //    Empty batches count because their post-batch token proves reader progress.
  // 3. Ordinary and document-only batches use the minimum safe token across all readers.
  // 4. The final lifecycle batch uses its actual token.
  // 5. Every batch retains its originating partition for acknowledgment tracking.
  private BsonDocument safeCheckpoint() {
    return this.readers.stream()
        .map(Reader::resumeTokenOfNonLifecycleBatch)
        .min(ResumeTokenUtils.BY_STREAM_POSITION)
        .orElseThrow()
        .clone();
  }

  // Replace only the outgoing checkpoint. It will always preserve the batch contents and
  // originating partition.
  private static ChangeStreamBatch withCheckpoint(
      ChangeStreamBatch batch, BsonDocument checkpoint) {
    return new ChangeStreamBatch(
        batch.getRawEvents(), checkpoint, batch.getCommandOperationTime(), batch.getPartition());
  }

  private static ChangeStreamBatch batchWithEvents(
      ChangeStreamBatch batch, List<RawBsonDocument> events) {
    return new ChangeStreamBatch(
        events,
        batch.getPostBatchResumeToken(),
        batch.getCommandOperationTime(),
        batch.getPartition());
  }

  // Helper function to check if the batch ends with a rename event.
  private static boolean endsWithRename(ChangeStreamBatch batch) {
    var events = batch.getRawEvents();
    return !events.isEmpty()
        && events.getLast().getString("operationType").getValue().equals("rename");
  }

  /** Lets each reader decide whether to fetch and queues it when the fetch completes. */
  private void submitFetchForReaders() {
    var slowestReaderOptime = slowestUnfinishedReaderOptime();
    this.readers.stream()
        .filter(reader -> !reader.isFinished())
        .mapToLong(
            reader ->
                Integer.toUnsignedLong(reader.optime.getTime())
                    - Integer.toUnsignedLong(slowestReaderOptime.getTime()))
        .max()
        .ifPresent(this.metrics.readerDifference::record);
    for (Reader<E> reader : this.readers) {
      try {
        reader.submitFetch(
            this.executor,
            () -> this.completedReaders.add(Optional.of(reader)),
            slowestReaderOptime,
            this.maxOptimeGapSeconds);
      } catch (RejectedExecutionException failure) {
        if (this.closed.get()) {
          throw new IllegalStateException("Change stream client is closed", failure);
        }
        throw failure;
      }
    }
  }

  private BsonTimestamp slowestUnfinishedReaderOptime() {
    return this.readers.stream()
        .filter(reader -> !reader.isFinished())
        .map(Reader::optime)
        .min(Comparator.naturalOrder())
        .orElseThrow();
  }

  /**
   * Closes this client's change stream readers, leaving the shared Mongo client open. Closing a
   * reader may abort an active fetch. This method does not wait for active reads, and reader
   * cleanup failures are logged. Repeated calls have no effect. The shared executor remains open.
   */
  @Override
  public void close() {
    if (this.closed.getAndSet(true)) {
      return;
    }
    this.completedReaders.clear();
    this.completedReaders.add(Optional.empty());
    this.readers.forEach(Reader::close);
  }

  /** Records the lifecycle suffix start, preserving it when a split rename is merged. */
  private record FetchResult(ChangeStreamBatch batch, int lifecycleStart) {
    boolean hasLifecycleEvent() {
      return this.lifecycleStart < this.batch.getRawEvents().size();
    }
  }

  /** Metrics shared by all partitioned indexes on this node; none are tagged by index. */
  private static final class Metrics {
    final Counter pauses;
    final DistributionSummary readerDifference;

    Metrics(MetricsFactory factory) {
      this.pauses = factory.counter("partitionReaderPauses");
      this.readerDifference = factory.summary("partitionReaderDifferenceSeconds");
    }
  }

  /** Owns one change stream client's fetch state and buffered result. */
  private static class Reader<E extends Exception> {
    private final ChangeStreamMongoClient<E> client;
    private final ChangeStreamPartition partition;

    private boolean closeRequested;

    /** Monotonic progress from consumed batches' resume tokens, including empty batches. */
    private BsonTimestamp optime;

    /** Shared restart token, replaced by the latest non-lifecycle batch token. */
    private BsonDocument resumeTokenOfNonLifecycleBatch;

    /**
     * Present while a fetch is queued, running, or completed and awaiting consumption. Accessed
     * only by the serialized calls to the outer client's getNext().
     */
    private Optional<FutureTask<FetchResult>> pendingFetch;

    private boolean pacingPaused;
    private final Runnable onPauseStart;

    /**
     * Retains the boundary batch and its first lifecycle position. Presence marks this reader
     * finished. Split rename and invalidate batches have already been combined by fetch(). Accessed
     * only by the outer client's getNext().
     */
    private Optional<FetchResult> lifecycleResult;

    /** Creates a reader with no outstanding fetch. */
    Reader(
        ChangeStreamMongoClient<E> client,
        ChangeStreamPartition partition,
        BsonDocument resumeToken,
        BsonTimestamp start,
        Runnable onPauseStart) {
      this.client = client;
      this.partition = partition;
      this.closeRequested = false;
      this.optime = start;
      this.resumeTokenOfNonLifecycleBatch = resumeToken;
      this.pendingFetch = Optional.empty();
      this.lifecycleResult = Optional.empty();
      this.onPauseStart = onPauseStart;
    }

    /** The reader has completed its lifecycle batch and will not fetch again. */
    boolean isFinished() {
      return this.lifecycleResult.isPresent();
    }

    BsonTimestamp optime() {
      return this.optime;
    }

    BsonDocument resumeTokenOfNonLifecycleBatch() {
      return this.resumeTokenOfNonLifecycleBatch;
    }

    private boolean shouldPauseForPacing(
        BsonTimestamp slowestReaderOptime, long maxOptimeGapSeconds) {
      // Pacing uses an approximate gap in whole seconds. The increment orders events within a
      // second; it does not measure elapsed time.
      // Seconds are unsigned 32-bit in BsonTimestamp; widen before subtracting.
      long gapSeconds =
          Integer.toUnsignedLong(this.optime.getTime())
              - Integer.toUnsignedLong(slowestReaderOptime.getTime());
      // The slowest unfinished reader has a zero gap and stays eligible without a resume signal.
      return gapSeconds > maxOptimeGapSeconds;
    }

    /**
     * Starts one fetch if eligible and the previous result has been consumed. Completion notifies
     * the caller; the result remains pending until taken. A rejected submission leaves this reader
     * idle.
     */
    void submitFetch(
        Executor executor,
        Runnable onCompletion,
        BsonTimestamp slowestReaderOptime,
        long maxOptimeGapSeconds) {
      // There are three conditions that prevent a new fetch from being submitted:
      // 1. The reader is finished because it received a lifecycle event
      // 2. The reader has a pending fetch
      // 3. The reader should pause for pacing
      boolean pauseForPacing =
          !isFinished() && shouldPauseForPacing(slowestReaderOptime, maxOptimeGapSeconds);
      if (pauseForPacing && !this.pacingPaused) {
        this.onPauseStart.run();
      }
      this.pacingPaused = pauseForPacing;
      if (isFinished() || this.pendingFetch.isPresent() || pauseForPacing) {
        return;
      }
      var future =
          new FutureTask<>(this::fetch) {
            @Override
            protected void done() {
              onCompletion.run();
            }
          };
      this.pendingFetch = Optional.of(future);
      try {
        executor.execute(future);
      } catch (RuntimeException | Error rejection) {
        this.pendingFetch = Optional.empty();
        throw rejection;
      }
    }

    /*
     * Takes the fetched batch, retaining lifecycle batches to prevent further fetches.
     *
     * Retries belong in the low-level change stream client. We unwrap and propagate fetch
     * failures; the caller must close this client and resume from its confirmed checkpoint.
     *
     * 1. Free the pending slot for a future fetch.
     * 2. Get the fetched batch and attach this reader's partition.
     * 3. Retain a lifecycle batch, or update this reader's safe non-lifecycle checkpoint.
     * 4. Propagate the failure so the caller can resume from its checkpoint.
     */
    ChangeStreamBatch takeFetchedBatch() throws Exception {
      // 1. Free the pending slot for a future fetch.
      FutureTask<FetchResult> completedFetch = this.pendingFetch.orElseThrow();
      this.pendingFetch = Optional.empty();
      try {
        // 2. Get the fetched batch and attach this reader's partition.
        var result = completedFetch.get();
        var fetched = result.batch();
        // Pacing uses the raw reader position rather than the shared outgoing checkpoint.
        this.optime = ResumeTokenUtils.opTimeFromResumeToken(fetched.getPostBatchResumeToken());
        var batch =
            new ChangeStreamBatch(
                fetched.getRawEvents(),
                fetched.getPostBatchResumeToken(),
                fetched.getCommandOperationTime(),
                Optional.of(this.partition));
        // 3. Retain a lifecycle batch, or advance this reader's safe checkpoint.
        if (result.hasLifecycleEvent()) {
          this.lifecycleResult = Optional.of(new FetchResult(batch, result.lifecycleStart()));
        } else {
          this.resumeTokenOfNonLifecycleBatch = fetched.getPostBatchResumeToken();
        }
        return batch;
      } catch (ExecutionException failure) {
        // 4. Propagate the failure so the caller can resume from its checkpoint.
        if (failure.getCause() instanceof Exception cause) {
          throw cause;
        }
        throw (Error) failure.getCause();
      }
    }

    // Reads a batch from one reader and completes any split rename sequence
    private FetchResult fetch() throws E {
      var batch = fetchNextBatch();
      int lifecycleStart = findLifecycleStart(batch);
      if (!endsWithRename(batch)) {
        return new FetchResult(batch, lifecycleStart);
      }

      // Combine rename with the next nonempty response. A failure fails the whole fetch, so
      // replication must recreate the client from its confirmed checkpoint to replay this batch.
      //
      // This loop is necessary because a single fetch can perform several underlying reads.
      //
      // Read 1 = [A1, rename]
      // Read 2 = []
      // Read 3 = [invalidate]
      // Completed result = [A1, rename, invalidate]
      while (true) {
        var next = fetchNextBatch();
        if (next.getRawEvents().isEmpty()) {
          continue;
        }
        // Let the index manager classify unexpected events following rename.
        var events = new ArrayList<>(batch.getRawEvents());
        events.addAll(next.getRawEvents());
        return new FetchResult(
            new ChangeStreamBatch(
                events, next.getPostBatchResumeToken(), next.getCommandOperationTime()),
            lifecycleStart);
      }
    }

    /** Starts no read after closure; an active read may be aborted by the client's close method. */
    private ChangeStreamBatch fetchNextBatch() throws E {
      synchronized (this) {
        checkState(!this.closeRequested, "Change stream reader is closed");
      }
      return this.client.getNext();
    }

    /** Stops new reads and closes the client, aborting an active fetch if one exists. */
    void close() {
      synchronized (this) {
        if (this.closeRequested) {
          return;
        }
        this.closeRequested = true;
      }
      closeClient();
    }

    private void closeClient() {
      try {
        this.client.close();
      } catch (RuntimeException failure) {
        LOG.warn("Failed to close partition reader", failure);
      }
    }
  }
}
