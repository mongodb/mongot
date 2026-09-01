package com.xgen.mongot.replication.mongodb.initialsync;

import static com.xgen.mongot.util.Check.checkState;

import com.xgen.mongot.logging.DefaultKeyValueLogger;
import com.xgen.mongot.replication.mongodb.common.InitialSyncException;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.FutureUtils;
import java.io.Closeable;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.bson.BsonTimestamp;

/**
 * Fans one scan window out to a scanner per unfinished partition, holds them all to a single
 * deadline, and stops them as a group.
 */
class ParallelCollectionScanDispatcher implements Closeable {

  private record CollectionScanWorker(
      ParallelCollectionScanPartitionState partitionState,
      CompletableFuture<ParallelCollectionScanner.Result> future) {}

  private final DefaultKeyValueLogger logger;
  private final Clock clock;
  private final InitialSyncContext context;
  private final Duration collectionScanTime;
  private final ParallelCollectionScannerFactory scannerFactory;
  private final CollectionScanTaskExecutor taskExecutor;

  private volatile boolean shutdownRequested;
  private volatile boolean closed;

  /**
   * The window's cause of failure, set once: setting it is what aborts the other workers, and it is
   * what {@link #dispatch} throws after joining them. Later failures are fallout from the abort
   * this one starts. Holds a future rather than a throwable so a fan-out throw can report a
   * synthetic one through the same path.
   */
  private final AtomicReference<CompletableFuture<ParallelCollectionScanner.Result>> firstFailure =
      new AtomicReference<>();

  /** Makes the cancel one-shot: shutdown and a worker failure can race from different threads. */
  private final AtomicBoolean cancelIssued = new AtomicBoolean();

  /** The cancel's future, held here because it is issued off-thread but awaited by dispatch. */
  private volatile Optional<CompletableFuture<Void>> cancellation = Optional.empty();

  ParallelCollectionScanDispatcher(
      Clock clock,
      InitialSyncContext context,
      Duration collectionScanTime,
      ParallelCollectionScannerFactory scannerFactory,
      CollectionScanTaskExecutor taskExecutor) {
    HashMap<String, Object> defaultKeyValues = new HashMap<>();
    defaultKeyValues.put("indexId", context.getIndexId());
    defaultKeyValues.put("generationId", context.getGenerationId());
    this.logger =
        DefaultKeyValueLogger.getLogger(ParallelCollectionScanDispatcher.class, defaultKeyValues);

    this.clock = clock;
    this.context = context;
    this.collectionScanTime = collectionScanTime;
    this.scannerFactory = scannerFactory;
    this.taskExecutor = taskExecutor;
  }

  /**
   * Scans every unfinished partition until each consumes its range or the window expires, and
   * returns whether the collection still has range left.
   */
  boolean dispatch(
      BsonTimestamp highWaterMark, List<ParallelCollectionScanPartitionState> partitions)
      throws InitialSyncException {

    checkState(!this.closed, "dispatching a scan window after the dispatcher was closed");

    // One deadline for every worker: a per-worker duration would stretch the window to whichever
    // thread started last.
    Instant deadline = Instant.now(this.clock).plus(this.collectionScanTime);

    List<ParallelCollectionScanPartitionState> unfinished =
        partitions.stream().filter(partitionState -> !partitionState.isDone()).toList();

    this.logger
        .atInfo()
        .addKeyValue("numPartitions", unfinished.size())
        .log("Starting a scan window.");

    List<CollectionScanWorker> workers = new ArrayList<>();
    try {
      for (ParallelCollectionScanPartitionState partitionState : unfinished) {
        workers.add(launch(highWaterMark, partitionState, deadline));
      }
    } catch (RuntimeException e) {
      // A throw part-way through the fan-out reaches neither the whenComplete abort nor
      // signalShutdown, so the workers already launched would scan on unattended. Recording it as
      // the window's failure stops them, and the wait below joins them before this window reports.
      this.firstFailure.compareAndSet(null, CompletableFuture.failedFuture(e));
      cancelGenerationOnce(e);
    }

    // In order:
    //   1. join the workers: each awaits its own indexing before returning, so afterwards no
    //      scanner is still reading and none of their batches are still being indexed,
    //   2. record how far each partition got, now that its worker has settled,
    //   3. join the batch that was in flight when the cancel was issued. This comes after the
    //      workers because a cancel does not stop later submissions, so joining it first would
    //      leave a running worker free to enqueue more afterwards.
    awaitAll(workers);
    recordCycleEnd(workers);
    awaitCancellation();

    long documentsIndexed = documentsIndexed(workers);

    // Shutdown outranks a worker failure: the cancel it issued is what failed the workers.
    if (this.shutdownRequested) {
      this.logger
          .atInfo()
          .addKeyValue("numDocumentsIndexed", documentsIndexed)
          .log("Scan window shut down.");
      throw InitialSyncException.createShutDown();
    }

    CompletableFuture<ParallelCollectionScanner.Result> failed = this.firstFailure.get();
    if (failed != null) {
      this.logger
          .atInfo()
          .addKeyValue("numDocumentsIndexed", documentsIndexed)
          .log("Scan window ended with exception.");
      InitialSyncException.getOrWrapThrowable(failed, InitialSyncException.Phase.COLLECTION_SCAN);
      return Check.unreachable("the recorded failure must throw");
    }

    boolean continueSync = partitions.stream().anyMatch(partitionState -> !partitionState.isDone());

    this.logger
        .atInfo()
        .addKeyValue("numDocumentsIndexed", documentsIndexed)
        .addKeyValue("continueSync", continueSync)
        .log("Finished a scan window.");

    return continueSync;
  }

  /**
   * Asks the running window to stop. Callable from another thread, and used by {@link #dispatch} on
   * its own thread when that thread is interrupted.
   */
  void signalShutdown() {
    this.logger.info("Signalling ParallelCollectionScanDispatcher shutdown.");
    this.shutdownRequested = true;
    cancelGenerationOnce(InitialSyncException.createShutDown());
  }

  @Override
  public void close() {
    this.closed = true;
    this.taskExecutor.shutdown();
  }

  private boolean abortRequested() {
    return this.shutdownRequested || this.firstFailure.get() != null;
  }

  /**
   * What this window landed, from the workers that settled with a result. A worker that threw is
   * left out because only a {@link ParallelCollectionScanner.Result} carries a count, so a failed
   * window reports a floor; the worker that threw logs how far it got on its way out.
   */
  private long documentsIndexed(List<CollectionScanWorker> workers) {
    return workers.stream()
        .filter(worker -> !worker.future().isCompletedExceptionally())
        .mapToLong(worker -> worker.future().join().documentsIndexed())
        .sum();
  }

  private CollectionScanWorker launch(
      BsonTimestamp highWaterMark,
      ParallelCollectionScanPartitionState partitionState,
      Instant deadline) {
    ParallelCollectionScanner scanner =
        this.scannerFactory.create(highWaterMark, partitionState, this::abortRequested);

    CompletableFuture<ParallelCollectionScanner.Result> scanFuture =
        this.taskExecutor.submit(() -> scanner.scanUntil(deadline));

    return new CollectionScanWorker(
        partitionState,
        scanFuture.whenComplete(
            (result, throwable) -> {
              // The first worker to fail is the one that cancels the generation, so the reason
              // recorded is the cause rather than fallout from the abort it triggers.
              if (throwable != null && this.firstFailure.compareAndSet(null, scanFuture)) {
                cancelGenerationOnce(throwable);
              }
            }));
  }

  /** Cancels this generation's queued work once, for whichever of shutdown or failure is first. */
  private void cancelGenerationOnce(Throwable reason) {
    if (this.cancelIssued.compareAndSet(false, true)) {
      this.cancellation = Optional.of(this.context.cancel(reason));
    }
  }

  /**
   * Waits for every worker to settle, so that no scanner is still running when this window reports.
   * Returning early would leave one holding a cursor open and indexing into the generation.
   */
  private void awaitAll(List<CollectionScanWorker> workers) {
    CompletableFuture<Void> allSettled =
        CompletableFuture.allOf(
            workers.stream()
                .map(worker -> worker.future().handle((result, throwable) -> null))
                .toArray(CompletableFuture[]::new));

    try {
      allSettled.get();
    } catch (InterruptedException e) {
      // Tell the workers to stop, then join them as usual rather than abandoning them. This
      // thread's interrupt status is left clear because dispatch reports SHUT_DOWN instead.
      signalShutdown();
      allSettled.join();
    } catch (ExecutionException e) {
      // Unreachable: the handled futures above cannot fail, so the wait cannot fail either. Only
      // get's checked exception makes this branch necessary.
      throw new AssertionError("unreachable: worker failures are mapped away before the wait", e);
    }
  }

  /** Records whether each partition consumed its range, now that its worker has settled. */
  private void recordCycleEnd(List<CollectionScanWorker> workers) {
    for (CollectionScanWorker worker : workers) {
      boolean rangeConsumed =
          !worker.future().isCompletedExceptionally()
              && worker.future().join().stopReason()
                  == ParallelCollectionScanner.StopReason.RANGE_CONSUMED;
      worker.partitionState().onCycleEnd(rangeConsumed);
    }
  }

  /**
   * Waits for the batch that was in flight when the cancel was issued. Failures here are logged
   * rather than thrown so they cannot replace the worker failure this cancel was issued for.
   */
  private void awaitCancellation() {
    if (this.cancellation.isEmpty()) {
      return;
    }

    CompletableFuture<Void> cancelFuture = this.cancellation.get();
    try {
      cancelFuture.get();
      return;
    } catch (InterruptedException e) {
      // Means shutdown here just as it does in awaitAll, which outranks the failure this cancel was
      // issued for. The drain still has to finish, so fall through and wait it out.
      signalShutdown();
    } catch (ExecutionException e) {
      // Fall through as well: the wait below settles immediately and reports this failure.
    }

    FutureUtils.getAndSwallow(
        cancelFuture, error -> this.logger.error("Failure during indexing cancellation", error));
  }
}
