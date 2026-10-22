package com.xgen.mongot.replication.mongodb.common;

import static com.xgen.mongot.util.Check.checkArg;
import static com.xgen.mongot.util.Check.checkState;

import com.google.common.annotations.VisibleForTesting;
import com.xgen.mongot.util.mongodb.BatchMongoClient;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.bson.BsonDocument;
import org.bson.BsonTimestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * PartitionedChangeStreamClient reads one collection's change stream as N separate partitions and
 * exposes them through the same getNext() interface as an ordinary change stream. Each call returns
 * one batch from whichever partition has finished fetching. The batch is tagged with that partition
 * and carries that partition's own resume token. Partitions are scheduled independently, up to the
 * shared executor's capacity.
 *
 * <p>Each document belongs to exactly one reader. Batches retain that reader's order, while readers
 * advance independently. Reader creation must apply the partition filter and reassemble split
 * events before delivery.
 *
 * <p>Every returned batch carries its reader's real post-batch resume token, so confirmed indexing
 * progress can advance the checkpoint. This client itself does not track confirmed indexing
 * progress. Collection lifecycle events are unsupported and cause {@link #getNext()} to fail.
 */
public class PartitionedChangeStreamClient<E extends Exception>
    implements ChangeStreamMongoClient<E> {
  private static final Logger LOG = LoggerFactory.getLogger(PartitionedChangeStreamClient.class);
  private static final Set<String> SUPPORTED_OPERATION_TYPES =
      Set.of("insert", "update", "replace", "delete");

  /** Creates partition readers with timestamp-based restart and split-event reassembly. */
  @FunctionalInterface
  public interface ReaderFactory<E extends Exception> {
    /**
     * Creates a reader using the supplied Mongo client. Its pipeline must apply the partition
     * filter and start at {@code start}, inclusively. The reader must emit complete, reassembled
     * events.
     */
    ChangeStreamMongoClient<E> create(
        BatchMongoClient mongoClient, ChangeStreamPartition partition, BsonTimestamp start)
        throws E;
  }

  private final List<Reader<E>> readers;

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

  /**
   * Creates all partition readers from the saved token's timestamp. Using the exact token with
   * {@code startAfter} can fail when another partition's filter excludes its event. Timestamp
   * replay also supports tokens identifying split-event fragments.
   *
   * <p>Takes ownership of the created readers. The supplied Mongo client is shared and remains open
   * when this client closes. Closing a reader aborts any active fetch.
   *
   * <p>Uses the supplied executor without taking ownership. It must be separate from the executor
   * calling {@link #getNext()}, which waits for a reader to finish. At most one fetch or buffered
   * batch per reader is retained, including between {@link #getNext()} calls. Readers may wait for
   * a worker when the shared pool is busy.
   *
   * <p>If initialization fails, closes any readers already created before propagating the failure.
   */
  public static <E extends Exception> PartitionedChangeStreamClient<E> resume(
      int partitionCount,
      BsonDocument resumeToken,
      BatchMongoClient mongoClient,
      ReaderFactory<E> readerFactory,
      Executor executor,
      WrapIfThrows<E> exceptionWrapper)
      throws E {
    return exceptionWrapper.wrapIfThrows(
        () -> {
          var clients = new ArrayList<ChangeStreamMongoClient<E>>();
          try {
            checkArg(partitionCount > 0, "partitionCount must be positive");
            BsonTimestamp start = ResumeTokenUtils.opTimeFromResumeToken(resumeToken);
            for (int id = 0; id < partitionCount; id++) {
              clients.add(
                  readerFactory.create(
                      mongoClient, new ChangeStreamPartition(id, partitionCount), start));
            }
            return new PartitionedChangeStreamClient<>(clients, executor, exceptionWrapper);
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

  /**
   * Takes ownership of preconfigured readers and borrows their executor. Tests can control fetch
   * scheduling; production callers use {@link #resume}.
   */
  @VisibleForTesting
  PartitionedChangeStreamClient(
      List<ChangeStreamMongoClient<E>> clients,
      Executor executor,
      WrapIfThrows<E> exceptionWrapper) {
    checkArg(!clients.isEmpty(), "clients must contain at least one reader");
    var readers = new ArrayList<Reader<E>>();
    for (int id = 0; id < clients.size(); id++) {
      readers.add(new Reader<>(clients.get(id), new ChangeStreamPartition(id, clients.size())));
    }
    this.readers = readers;
    this.completedReaders = new LinkedBlockingQueue<>();
    this.executor = executor;
    this.exceptionWrapper = exceptionWrapper;
    this.closed = new AtomicBoolean(false);
  }

  /*
   * Each getNext() call follows these steps:
   *
   * 1. Check closure and whether all readers finished. Lifecycle completion is a placeholder.
   * 2. Call submitFetchForReaders(). Each reader checks its lifecycle and pacing eligibility
   *    hooks and pending fetch. Lifecycle and optime pacing are not implemented yet.
   *    A completed, unread result still occupies the pending slot.
   * 3. Take the next completed reader, waiting if necessary. A shutdown marker wakes this wait
   *    and causes the call to throw.
   * 4. Take the result, freeing its reader's slot, or propagate the fetch failure.
   * 5. Reject unsupported lifecycle events and return the batch with its original partition
   *    metadata. Other readers' results remain available for later calls.
   *
   * Example: A has a ready batch, B is idle, and C is still fetching. Submit B's fetch before
   * returning A's batch. A can fetch again on the next call. Completed results remain deliverable
   * regardless of future fetch eligibility. close() wakes a waiting call and closes the readers.
   */
  @Override
  public synchronized ChangeStreamBatch getNext() throws E {
    checkState(!this.closed.get(), "Change stream client is closed");
    return this.exceptionWrapper.wrapIfThrows(
        () -> {
          if (allReadersFinished()) {
            return handleAllReadersFinished();
          }
          submitFetchForReaders();

          Reader<E> reader =
              this.completedReaders
                  .take()
                  .orElseThrow(() -> new IllegalStateException("Change stream client is closed"));
          checkState(!this.closed.get(), "Change stream client is closed");
          // Taking the result frees the slot, including on failure. Refill happens on a later call.
          ChangeStreamBatch batch = reader.takeFetchResult();

          checkState(!this.closed.get(), "Change stream client is closed");
          checkState(
              hasSupportedFinalOperation(batch),
              "Partitioned change stream lifecycle coordination is not implemented");
          return new ChangeStreamBatch(
              batch.getRawEvents(),
              batch.getPostBatchResumeToken(),
              batch.getCommandOperationTime(),
              Optional.of(reader.partition));
        });
  }

  /**
   * Returns whether the final event is supported. An invalidating lifecycle event is always last,
   * either by itself or as the invalidate event following another lifecycle event.
   */
  private static boolean hasSupportedFinalOperation(ChangeStreamBatch batch) {
    var events = batch.getRawEvents();
    return events.isEmpty() || isSupportedOperation(events.getLast());
  }

  private static boolean isSupportedOperation(BsonDocument event) {
    return SUPPORTED_OPERATION_TYPES.contains(event.getString("operationType").getValue());
  }

  /** Checks whether every reader has reached the lifecycle boundary. */
  private boolean allReadersFinished() {
    return this.readers.stream().allMatch(Reader::isFinished);
  }

  /** Placeholder for coordinating the lifecycle event after every reader has finished. */
  private ChangeStreamBatch handleAllReadersFinished() {
    // Lifecycle coordination will supply the batch to return here.
    throw new IllegalStateException(
        "Partitioned change stream lifecycle coordination is not implemented");
  }

  /** Lets each reader decide whether to fetch and queues it when the fetch completes. */
  private void submitFetchForReaders() {
    for (Reader<E> reader : this.readers) {
      try {
        reader.submitFetch(this.executor, () -> this.completedReaders.add(Optional.of(reader)));
      } catch (RejectedExecutionException failure) {
        if (this.closed.get()) {
          throw new IllegalStateException("Change stream client is closed", failure);
        }
        throw failure;
      }
    }
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

  /** Owns one change stream client's fetch state and buffered result. */
  private static class Reader<E extends Exception> {
    private final ChangeStreamMongoClient<E> client;
    private final ChangeStreamPartition partition;

    private boolean closeRequested;

    /**
     * Present while a fetch is queued, running, or completed and awaiting consumption. Accessed
     * only by the serialized calls to the outer client's getNext().
     */
    private Optional<FutureTask<ChangeStreamBatch>> pendingFetch;

    /** Creates a reader with no outstanding fetch. */
    Reader(ChangeStreamMongoClient<E> client, ChangeStreamPartition partition) {
      this.client = client;
      this.partition = partition;
      this.closeRequested = false;
      this.pendingFetch = Optional.empty();
    }

    /** Placeholder for reader-owned state indicating that a lifecycle boundary was reached. */
    boolean isFinished() {
      // Lifecycle detection will record completion here; until then this reader stays unfinished.
      return false;
    }

    /** Placeholder for deriving pacing eligibility from optime progress on each submission. */
    private boolean shouldPauseForPacing() {
      // The optime policy will go here. It must leave the slowest unfinished reader eligible
      // so completions can advance progress without an external resume signal.
      return false;
    }

    /**
     * Starts one fetch if eligible and the previous result has been consumed. Completion notifies
     * the caller; the result remains pending until taken. A rejected submission leaves this reader
     * idle.
     */
    void submitFetch(Executor executor, Runnable onCompletion) {
      // There are three conditions that prevent a new fetch from being submitted:
      // 1. The reader is finished because it received a lifecycle event
      // 2. The reader has a pending fetch
      // 3. The reader should pause for pacing
      if (isFinished() || this.pendingFetch.isPresent() || shouldPauseForPacing()) {
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

    /** Takes the fetch result or propagates its failure, allowing another fetch next time. */
    ChangeStreamBatch takeFetchResult() throws Exception {
      FutureTask<ChangeStreamBatch> completedFetch = this.pendingFetch.orElseThrow();
      this.pendingFetch = Optional.empty();
      try {
        return completedFetch.get();
      } catch (ExecutionException failure) {
        if (failure.getCause() instanceof Exception cause) {
          throw cause;
        }
        throw (Error) failure.getCause();
      }
    }

    /** Starts no read after closure; an active read may be aborted by the client's close method. */
    private ChangeStreamBatch fetch() throws E {
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
