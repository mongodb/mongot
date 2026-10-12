package com.xgen.mongot.replication.mongodb.common;

import static com.xgen.mongot.util.Check.checkArg;
import static com.xgen.mongot.util.Check.checkState;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.xgen.mongot.util.mongodb.BatchMongoClient;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.bson.BsonDocument;
import org.bson.BsonTimestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reads partitioned change streams concurrently through the {@link ChangeStreamMongoClient}
 * interface.
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

  /** Completed readers, or an empty entry to wake getNext() when shutdown starts. */
  private final BlockingQueue<Optional<Reader<E>>> completedReaders;

  /** Dedicated to this client, with one worker available for every partition. */
  private final ExecutorService executor;

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
   * <p>Creates a dedicated executor with one worker per partition so long-polling readers cannot
   * prevent other partitions from starting their reads. At most one fetch or buffered batch per
   * reader is retained, including between {@link #getNext()} calls. This client owns the executor.
   *
   * <p>If initialization fails, closes any readers already created before propagating the failure.
   */
  public static <E extends Exception> PartitionedChangeStreamClient<E> resume(
      int partitionCount,
      BsonDocument resumeToken,
      BatchMongoClient mongoClient,
      ReaderFactory<E> readerFactory,
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
            return new PartitionedChangeStreamClient<>(
                clients,
                Executors.newFixedThreadPool(
                    partitionCount,
                    new ThreadFactoryBuilder()
                        .setNameFormat("partitioned-change-stream-reader-%d")
                        .build()),
                exceptionWrapper);
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
   * Takes ownership of preconfigured readers and their executor. Tests can supply an executor to
   * control fetch scheduling; production callers use {@link #resume} to size the pool.
   */
  @VisibleForTesting
  PartitionedChangeStreamClient(
      List<ChangeStreamMongoClient<E>> clients,
      ExecutorService executor,
      WrapIfThrows<E> exceptionWrapper) {
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

  /**
   * Returns the next completed reader batch independently of other readers' progress. Calls to this
   * method are serialized. {@link #close()} wakes a waiting call and closes in-flight reader
   * clients.
   *
   * <p>A failed read preserves pending reads from other readers of the same index for subsequent
   * calls. {@link #close()} requests cleanup of outstanding reads.
   */
  @Override
  public synchronized ChangeStreamBatch getNext() throws E {
    checkState(!this.closed.get(), "Change stream client is closed");
    return this.exceptionWrapper.wrapIfThrows(
        () -> {
          submitFetchesForIdleReaders();
          Reader<E> reader =
              this.completedReaders
                  .take()
                  .orElseThrow(() -> new IllegalStateException("Change stream client is closed"));
          checkState(!this.closed.get(), "Change stream client is closed");
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

  /** Asks each reader to fetch if idle and queues it when the fetch completes. */
  private void submitFetchesForIdleReaders() {
    for (Reader<E> reader : this.readers) {
      try {
        reader.submitFetchIfIdle(
            this.executor, () -> this.completedReaders.add(Optional.of(reader)));
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
   * cleanup failures are logged. Repeated calls have no effect. The executor shuts down without
   * interrupting or waiting for its workers.
   */
  @Override
  public void close() {
    if (this.closed.getAndSet(true)) {
      return;
    }
    this.completedReaders.clear();
    this.completedReaders.add(Optional.empty());
    this.executor.shutdown();
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

    /**
     * Starts one fetch if the previous result has been consumed. Completion notifies the caller;
     * the result remains pending until taken. A rejected submission leaves this reader idle.
     */
    void submitFetchIfIdle(Executor executor, Runnable onCompletion) {
      if (this.pendingFetch.isPresent()) {
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
