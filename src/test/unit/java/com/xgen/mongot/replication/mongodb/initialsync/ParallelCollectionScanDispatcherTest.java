package com.xgen.mongot.replication.mongodb.initialsync;

import static com.xgen.testing.mongot.mock.index.SearchIndex.IGNORE_METRICS;
import static com.xgen.testing.mongot.mock.index.SearchIndex.MOCK_INDEX_DEFINITION_GENERATION;
import static com.xgen.testing.mongot.mock.replication.mongodb.common.DocumentIndexer.mockDocumentIndexer;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.xgen.mongot.monitor.ToggleGate;
import com.xgen.mongot.replication.mongodb.common.DocumentIndexer;
import com.xgen.mongot.replication.mongodb.common.IndexingWorkScheduler;
import com.xgen.mongot.replication.mongodb.common.InitialSyncException;
import com.xgen.mongot.util.FutureUtils;
import com.xgen.mongot.util.functionalinterfaces.CheckedSupplier;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BooleanSupplier;
import org.bson.BsonTimestamp;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

public class ParallelCollectionScanDispatcherTest {

  private static final BsonTimestamp HIGH_WATER_MARK = new BsonTimestamp(10, 1);
  private static final BsonTimestamp LATER_HIGH_WATER_MARK = new BsonTimestamp(20, 1);
  private static final Duration SCAN_TIME = Duration.ofMinutes(5);
  private static final Clock FIXED_CLOCK = Clock.fixed(Instant.EPOCH, ZoneOffset.UTC);

  /** Long enough that a stuck worker fails the test rather than hanging the suite. */
  private static final Duration ABORT_TIMEOUT = Duration.ofSeconds(10);

  private final List<ParallelCollectionScanDispatcher> dispatchers = new ArrayList<>();

  @After
  public void closeDispatchers() {
    this.dispatchers.forEach(ParallelCollectionScanDispatcher::close);
  }

  /** The scanner's side of the dispatcher contract: the abort flag it is handed. */
  private static final class Worker {
    private final BooleanSupplier abortRequested;
    private final Runnable onAbortObserved;

    private Worker(BooleanSupplier abortRequested, Runnable onAbortObserved) {
      this.abortRequested = abortRequested;
      this.onAbortObserved = onAbortObserved;
    }

    /** Blocks like a worker mid-batch would, until the dispatcher asks every worker to stop. */
    void awaitAbort() {
      Instant limit = Instant.now().plus(ABORT_TIMEOUT);
      while (!this.abortRequested.getAsBoolean()) {
        if (Instant.now().isAfter(limit)) {
          throw new AssertionError("the dispatcher never asked this worker to stop");
        }
        LockSupport.parkNanos(Duration.ofMillis(1).toNanos());
      }
      this.onAbortObserved.run();
    }
  }

  /** What a partition's scanner does when the dispatcher runs it. */
  private interface Behaviour {
    ParallelCollectionScanner.Result run(Worker worker) throws InitialSyncException;
  }

  private static Behaviour consumesItsRange(long documentsIndexed) {
    return worker ->
        new ParallelCollectionScanner.Result(
            ParallelCollectionScanner.StopReason.RANGE_CONSUMED, documentsIndexed);
  }

  private static Behaviour runsOutOfWindow(long documentsIndexed) {
    return worker ->
        new ParallelCollectionScanner.Result(
            ParallelCollectionScanner.StopReason.WINDOW_DEADLINE, documentsIndexed);
  }

  private static Behaviour fails(InitialSyncException failure) {
    return worker -> {
      throw failure;
    };
  }

  private static Behaviour stopsWhenAborted(long documentsIndexed) {
    return worker -> {
      worker.awaitAbort();
      return new ParallelCollectionScanner.Result(
          ParallelCollectionScanner.StopReason.ABORTED, documentsIndexed);
    };
  }

  /** A sibling whose own failure is a consequence of the abort, not the cause of it. */
  private static Behaviour failsWhenAborted(InitialSyncException failure) {
    return worker -> {
      worker.awaitAbort();
      throw failure;
    };
  }

  /**
   * Stands in for the production pool: real threads, because the dispatcher's whole job is running
   * partitions at once, but with the shutdown recorded rather than a pool to reclaim.
   */
  private static final class FakeTaskExecutor implements CollectionScanTaskExecutor {

    private final ExecutorService pool = Executors.newCachedThreadPool();
    private boolean shutdown;
    private OptionalInt rejectAfter = OptionalInt.empty();
    private int submitted;

    /** Makes the pool refuse work part-way through a fan-out, as a released one would. */
    void rejectSubmitsAfter(int accepted) {
      this.rejectAfter = OptionalInt.of(accepted);
    }

    @Override
    public CompletableFuture<ParallelCollectionScanner.Result> submit(
        CheckedSupplier<ParallelCollectionScanner.Result, InitialSyncException> scan) {
      if (this.rejectAfter.isPresent() && this.submitted >= this.rejectAfter.getAsInt()) {
        throw new RejectedExecutionException("mocked rejection");
      }
      this.submitted++;
      return FutureUtils.checkedSupplyAsync(scan, this.pool, InitialSyncException.class);
    }

    @Override
    public void shutdown() {
      this.shutdown = true;
      this.pool.shutdown();
    }
  }

  private static ParallelCollectionScanPartitionState partition(int partitionId, boolean isDone) {
    return new ParallelCollectionScanPartitionState(
        partitionId, Optional.empty(), Optional.empty(), Optional.empty(), isDone);
  }

  private Fixture fixture(ParallelCollectionScanPartitionState... partitions) {
    Fixture fixture = new Fixture(List.of(partitions));
    this.dispatchers.add(fixture.dispatcher);
    return fixture;
  }

  private static final class Fixture {

    private final IndexingWorkScheduler indexingWorkScheduler;
    private final List<ParallelCollectionScanPartitionState> partitions;
    private final Map<Integer, Behaviour> behaviours = new HashMap<>();
    private final List<Integer> scannedPartitionIds = new ArrayList<>();
    private final Map<Integer, Instant> deadlines = new HashMap<>();
    private final List<BsonTimestamp> highWaterMarks = new ArrayList<>();
    private final Set<Integer> observedAbort = new HashSet<>();
    private final FakeTaskExecutor taskExecutor = new FakeTaskExecutor();
    private final ParallelCollectionScanDispatcher dispatcher;

    private Fixture(List<ParallelCollectionScanPartitionState> partitions) {
      this.partitions = partitions;

      DocumentIndexer documentIndexer = mockDocumentIndexer();
      this.indexingWorkScheduler = mock(IndexingWorkScheduler.class);
      when(this.indexingWorkScheduler.cancel(any(), any(), any()))
          .thenReturn(CompletableFuture.completedFuture(null));

      InitialSyncContext context =
          InitialSyncContext.create(
              MOCK_INDEX_DEFINITION_GENERATION,
              this.indexingWorkScheduler,
              documentIndexer,
              IGNORE_METRICS,
              Optional.empty(),
              false,
              true,
              true,
              partitions.size(),
              ToggleGate.opened());

      this.dispatcher =
          new ParallelCollectionScanDispatcher(
              FIXED_CLOCK, context, SCAN_TIME, this::scanner, this.taskExecutor);
    }

    Fixture where(int partitionId, Behaviour behaviour) {
      this.behaviours.put(partitionId, behaviour);
      return this;
    }

    /**
     * Holds the cancelled work's drain open, so a window parks in the drain until the returned
     * future is completed.
     */
    CompletableFuture<Void> whereCancellationBlocks() {
      CompletableFuture<Void> drain = new CompletableFuture<>();
      doReturn(drain).when(this.indexingWorkScheduler).cancel(any(), any(), any());
      return drain;
    }

    /** Makes draining the cancelled indexing work fail, as a scheduler in trouble would. */
    Fixture whereCancellationFails() {
      doReturn(CompletableFuture.failedFuture(new IllegalStateException("mocked cancel failure")))
          .when(this.indexingWorkScheduler)
          .cancel(any(), any(), any());
      return this;
    }

    boolean dispatch(BsonTimestamp highWaterMark) throws InitialSyncException {
      return this.dispatcher.dispatch(highWaterMark, this.partitions);
    }

    /**
     * Builds the stand-in scanner for a partition, recording that the dispatcher chose to scan it,
     * what it held the scanner to, and whether the scanner ever saw the abort flag set.
     */
    private ParallelCollectionScanner scanner(
        BsonTimestamp highWaterMark,
        ParallelCollectionScanPartitionState partitionState,
        BooleanSupplier abortRequested) {
      int partitionId = partitionState.getPartitionId();
      record(
          () -> {
            this.scannedPartitionIds.add(partitionId);
            this.highWaterMarks.add(highWaterMark);
          });

      Worker worker =
          new Worker(abortRequested, () -> record(() -> this.observedAbort.add(partitionId)));

      ParallelCollectionScanner scanner = mock(ParallelCollectionScanner.class);
      try {
        when(scanner.scanUntil(any()))
            .thenAnswer(
                invocation -> {
                  record(() -> this.deadlines.put(partitionId, invocation.getArgument(0)));
                  return this.behaviours.get(partitionId).run(worker);
                });
      } catch (InitialSyncException e) {
        throw new AssertionError("stubbing does not run the scanner", e);
      }
      return scanner;
    }

    /** Workers run on their own threads, so everything they report is written under one lock. */
    private synchronized void record(Runnable update) {
      update.run();
    }

    /**
     * How many workers have made it into their scan, recorded as each one is handed its deadline.
     */
    synchronized int workersInTheirScan() {
      return this.deadlines.size();
    }

    ParallelCollectionScanPartitionState partition(int partitionId) {
      return this.partitions.get(partitionId);
    }
  }

  @Test
  public void dispatchReportsTheCollectionConsumedOnceEveryPartitionIsDone() throws Exception {
    Fixture fixture =
        fixture(partition(0, false), partition(1, false), partition(2, false))
            .where(0, consumesItsRange(1))
            .where(1, consumesItsRange(2))
            .where(2, consumesItsRange(3));

    boolean continueSync = fixture.dispatch(HIGH_WATER_MARK);

    Assert.assertFalse(continueSync);
    Assert.assertTrue(fixture.partition(0).isDone());
    Assert.assertTrue(fixture.partition(1).isDone());
    Assert.assertTrue(fixture.partition(2).isDone());
    // Cancelling tears down the generation's queued work, so a window that went fine must not.
    verify(fixture.indexingWorkScheduler, never()).cancel(any(), any(), any());
  }

  /** One partition still holding range keeps the whole sync going, however many others finished. */
  @Test
  public void dispatchReportsMoreWorkWhenAPartitionRunsOutOfWindow() throws Exception {
    Fixture fixture =
        fixture(partition(0, false), partition(1, false))
            .where(0, consumesItsRange(1))
            .where(1, runsOutOfWindow(2));

    boolean continueSync = fixture.dispatch(HIGH_WATER_MARK);

    Assert.assertTrue(continueSync);
    Assert.assertTrue(fixture.partition(0).isDone());
    Assert.assertFalse(fixture.partition(1).isDone());
  }

  /**
   * A finished partition has nothing left to scan, and clearing its flag would put it back in every
   * window from here on.
   */
  @Test
  public void dispatchLeavesPartitionsThatAlreadyConsumedTheirRangeAlone() throws Exception {
    Fixture fixture =
        fixture(partition(0, true), partition(1, false)).where(1, consumesItsRange(1));

    boolean continueSync = fixture.dispatch(HIGH_WATER_MARK);

    Assert.assertFalse(continueSync);
    Assert.assertEquals(List.of(1), fixture.scannedPartitionIds);
    Assert.assertTrue(fixture.partition(0).isDone());
  }

  /** Workers start staggered, so a per-worker duration would stretch the window. */
  @Test
  public void dispatchHoldsEveryWorkerToOneDeadline() throws Exception {
    Fixture fixture =
        fixture(partition(0, false), partition(1, false))
            .where(0, consumesItsRange(1))
            .where(1, consumesItsRange(1));

    fixture.dispatch(HIGH_WATER_MARK);

    Instant expected = Instant.EPOCH.plus(SCAN_TIME);
    Assert.assertEquals(Map.of(0, expected, 1, expected), fixture.deadlines);
  }

  /**
   * The freshness floor advances every change-stream apply window, so a dispatcher that fixed it at
   * construction would keep reading the whole collection at the first window's cluster time.
   */
  @Test
  public void dispatchGivesEachWindowsScannersThatWindowsFreshnessFloor() throws Exception {
    Fixture fixture = fixture(partition(0, false)).where(0, runsOutOfWindow(1));

    fixture.dispatch(HIGH_WATER_MARK);
    fixture.dispatch(LATER_HIGH_WATER_MARK);

    Assert.assertEquals(List.of(HIGH_WATER_MARK, LATER_HIGH_WATER_MARK), fixture.highWaterMarks);
  }

  @Test
  public void dispatchScansNothingWhenEveryPartitionIsAlreadyDone() throws Exception {
    Fixture fixture = fixture(partition(0, true), partition(1, true));

    boolean continueSync = fixture.dispatch(HIGH_WATER_MARK);

    Assert.assertFalse(continueSync);
    Assert.assertEquals(List.of(), fixture.scannedPartitionIds);
  }

  /**
   * The executor outlives a window, so only the manager closing the dispatcher releases what it
   * holds.
   */
  @Test
  public void closeReleasesTheTaskExecutor() {
    Fixture fixture = fixture(partition(0, false));

    fixture.dispatcher.close();

    Assert.assertTrue(fixture.taskExecutor.shutdown);
  }

  /** A closed dispatcher has no threads left, so every partition would fail to launch. */
  @Test
  public void dispatchAfterCloseRefusesTheWindow() {
    Fixture fixture =
        fixture(partition(0, false), partition(1, false))
            .where(0, consumesItsRange(1))
            .where(1, consumesItsRange(1));
    fixture.dispatcher.close();

    Assert.assertThrows(IllegalStateException.class, () -> fixture.dispatch(HIGH_WATER_MARK));

    Assert.assertEquals(List.of(), fixture.scannedPartitionIds);
  }

  /**
   * A launch that throws leaves nothing telling the workers that did start to stop, so the window
   * has to treat it as its own failure: abort them, cancel once, join them, then report the throw.
   */
  @Test
  public void dispatchStopsAndJoinsTheWorkersItLaunchedWhenALaterLaunchFails() {
    Fixture fixture =
        fixture(partition(0, false), partition(1, false)).where(0, stopsWhenAborted(1));
    fixture.taskExecutor.rejectSubmitsAfter(1);

    Assert.assertThrows(RejectedExecutionException.class, () -> fixture.dispatch(HIGH_WATER_MARK));

    // Recorded by the worker itself as it stopped, so seeing it means it was told and then joined.
    Assert.assertEquals(Set.of(0), fixture.observedAbort);
    verify(fixture.indexingWorkScheduler, times(1)).cancel(any(), eq(Optional.empty()), any());
  }

  /**
   * A failing worker cannot cancel its own queued batches: cancel is generation-wide, so it would
   * tear down its siblings. The dispatcher asks them to stop and issues the one cancel.
   */
  @Test
  public void dispatchStopsSiblingsAndCancelsTheGenerationOnceWhenAWorkerFails() {
    InitialSyncException failure = InitialSyncException.createRequiresResync("mocked failure");
    Fixture fixture =
        fixture(partition(0, false), partition(1, false))
            .where(0, fails(failure))
            .where(1, stopsWhenAborted(2));

    InitialSyncException thrown =
        Assert.assertThrows(InitialSyncException.class, () -> fixture.dispatch(HIGH_WATER_MARK));

    Assert.assertSame(failure, thrown);
    Assert.assertEquals(Set.of(1), fixture.observedAbort);
    // Cancelling once, for the generation rather than for an attempt, is what spares the siblings.
    verify(fixture.indexingWorkScheduler, times(1)).cancel(any(), eq(Optional.empty()), any());
    Assert.assertFalse(fixture.partition(0).isDone());
    Assert.assertFalse(fixture.partition(1).isDone());
  }

  /**
   * Draining the cancelled work can fail too. The window still reports the worker failure that the
   * cancel was issued for, rather than the second failure that came out of handling the first.
   */
  @Test
  public void dispatchStillReportsTheWorkerFailureWhenTheCancellationFails() {
    InitialSyncException failure = InitialSyncException.createRequiresResync("mocked failure");
    Fixture fixture =
        fixture(partition(0, false), partition(1, false))
            .where(0, fails(failure))
            .where(1, stopsWhenAborted(1))
            .whereCancellationFails();

    InitialSyncException thrown =
        Assert.assertThrows(InitialSyncException.class, () -> fixture.dispatch(HIGH_WATER_MARK));

    Assert.assertSame(failure, thrown);
    Assert.assertEquals(Set.of(1), fixture.observedAbort);
  }

  /** Whichever worker failed first is the reason the run failed; the rest are fallout. */
  @Test
  public void dispatchKeepsTheFirstFailureAsTheCause() {
    InitialSyncException cause = InitialSyncException.createRequiresResync("the first failure");
    InitialSyncException fallout = InitialSyncException.createFailed(new IllegalStateException());
    Fixture fixture =
        fixture(partition(0, false), partition(1, false))
            .where(0, fails(cause))
            .where(1, failsWhenAborted(fallout));

    InitialSyncException thrown =
        Assert.assertThrows(InitialSyncException.class, () -> fixture.dispatch(HIGH_WATER_MARK));

    Assert.assertSame(cause, thrown);
    Assert.assertEquals(Set.of(1), fixture.observedAbort);
  }

  /**
   * A worker failing must not end the window while a sibling still scans: reporting then would
   * leave it holding a cursor and indexing into a generation the manager has already failed.
   */
  @Test
  public void dispatchWaitsForEveryWorkerBeforeItThrows() {
    Fixture fixture =
        fixture(partition(0, false), partition(1, false))
            .where(0, fails(InitialSyncException.createRequiresResync("mocked failure")))
            .where(1, stopsWhenAborted(0));

    Assert.assertThrows(InitialSyncException.class, () -> fixture.dispatch(HIGH_WATER_MARK));

    // Recorded by the worker itself as it finished, so seeing it means the dispatcher joined it.
    Assert.assertEquals(Set.of(1), fixture.observedAbort);
    Assert.assertEquals(
        Map.of(0, Instant.EPOCH.plus(SCAN_TIME), 1, Instant.EPOCH.plus(SCAN_TIME)),
        fixture.deadlines);
  }

  /**
   * Workers stop cleanly on shutdown, but the run has not finished the collection, so it must
   * report shutdown rather than a completed window.
   */
  @Test
  public void dispatchThrowsShutDownWhenShutdownStoppedTheWorkers() {
    Fixture fixture =
        fixture(partition(0, false), partition(1, false))
            .where(0, stopsWhenAborted(1))
            .where(1, stopsWhenAborted(2));
    fixture.dispatcher.signalShutdown();

    InitialSyncException thrown =
        Assert.assertThrows(InitialSyncException.class, () -> fixture.dispatch(HIGH_WATER_MARK));

    Assert.assertTrue(thrown.isShutdown());
    Assert.assertEquals(Set.of(0, 1), fixture.observedAbort);
    verify(fixture.indexingWorkScheduler, times(1)).cancel(any(), eq(Optional.empty()), any());
    Assert.assertFalse(fixture.partition(0).isDone());
    Assert.assertFalse(fixture.partition(1).isDone());
  }

  /**
   * Shutdown cancels the generation, which fails whatever was queued, so a worker failure during
   * shutdown is expected. Reporting it instead of shutdown would send the index down the retry path
   * rather than the shutdown path.
   */
  @Test
  public void dispatchReportsShutDownRatherThanAFailureItCaused() {
    InitialSyncException fallout = InitialSyncException.createFailed(new IllegalStateException());
    Fixture fixture =
        fixture(partition(0, false), partition(1, false))
            .where(0, failsWhenAborted(fallout))
            .where(1, stopsWhenAborted(1));
    fixture.dispatcher.signalShutdown();

    InitialSyncException thrown =
        Assert.assertThrows(InitialSyncException.class, () -> fixture.dispatch(HIGH_WATER_MARK));

    Assert.assertTrue(thrown.isShutdown());
    Assert.assertEquals(Set.of(0, 1), fixture.observedAbort);
    // Two reasons to cancel arrive here, shutdown and the failure it caused, but the generation is
    // cancelled once: a second cancel would tear down work the next attempt has already queued.
    verify(fixture.indexingWorkScheduler, times(1)).cancel(any(), eq(Optional.empty()), any());
  }

  /**
   * An interrupt on dispatch's own thread has to reach the workers, since only the abort flag stops
   * them. Waiting with {@code join} would absorb it and let the window run to its deadline.
   */
  @Test
  public void dispatchStopsTheWorkersWhenItsOwnThreadIsInterrupted() throws Exception {
    Fixture fixture =
        fixture(partition(0, false), partition(1, false))
            .where(0, stopsWhenAborted(1))
            .where(1, stopsWhenAborted(2));

    InitialSyncException thrown = interruptDispatchOnceTheWorkersAreScanning(fixture, 2);

    Assert.assertTrue(thrown.isShutdown());
    Assert.assertEquals(Set.of(0, 1), fixture.observedAbort);
    Assert.assertFalse(fixture.partition(0).isDone());
    Assert.assertFalse(fixture.partition(1).isDone());
  }

  /**
   * An interrupt while the cancel drains means what one during the scan means: the process is going
   * away. Swallowing it here would report the worker failure instead, sending the index down the
   * retry path rather than the shutdown path.
   */
  @Test
  public void dispatchReportsShutDownWhenInterruptedWhileTheCancellationDrains() throws Exception {
    Fixture fixture =
        fixture(partition(0, false), partition(1, false))
            .where(0, fails(InitialSyncException.createRequiresResync("mocked failure")))
            .where(1, consumesItsRange(1));
    CompletableFuture<Void> drain = fixture.whereCancellationBlocks();

    DispatchOnItsOwnThread dispatch = startDispatch(fixture);
    // Recorded between the wait on the workers and the drain, so seeing it means the interrupt can
    // only land in the drain, and the drain is still open so the wait there has to observe it.
    awaitPartitionRecordedDone(fixture, 1);
    dispatch.thread().interrupt();
    awaitInterruptConsumed(dispatch.thread());
    drain.complete(null);

    InitialSyncException thrown = awaitDispatchReturning(dispatch);

    Assert.assertTrue(
        "dispatch reported the worker failure rather than shutdown", thrown.isShutdown());
  }

  /** A worker blocked awaiting its in-flight batch is only released by cancelling queued work. */
  @Test
  public void signalShutdownCancelsQueuedWorkImmediately() {
    Fixture fixture = fixture(partition(0, false));

    fixture.dispatcher.signalShutdown();

    verify(fixture.indexingWorkScheduler, times(1)).cancel(any(), eq(Optional.empty()), any());
  }

  /**
   * Runs a window on its own thread and interrupts it once every worker is inside its scan, so the
   * interrupt lands while dispatch is waiting on them rather than still launching them.
   */
  private static InitialSyncException interruptDispatchOnceTheWorkersAreScanning(
      Fixture fixture, int workers) throws Exception {
    DispatchOnItsOwnThread dispatch = startDispatch(fixture);

    awaitWorkersScanning(fixture, workers);
    dispatch.thread().interrupt();

    return awaitDispatchReturning(dispatch);
  }

  /** A window the test can interrupt, and what it ended up reporting. */
  private record DispatchOnItsOwnThread(Thread thread, AtomicReference<Throwable> thrown) {}

  private static DispatchOnItsOwnThread startDispatch(Fixture fixture) {
    AtomicReference<Throwable> thrown = new AtomicReference<>();
    Thread thread =
        new Thread(
            () -> {
              try {
                fixture.dispatch(HIGH_WATER_MARK);
              } catch (Throwable t) {
                thrown.set(t);
              }
            });

    thread.start();
    return new DispatchOnItsOwnThread(thread, thrown);
  }

  private static InitialSyncException awaitDispatchReturning(DispatchOnItsOwnThread dispatch)
      throws InterruptedException {
    // Longer than a worker's own patience, so a swallowed interrupt reports what dispatch did
    // rather than timing out here.
    dispatch.thread().join(ABORT_TIMEOUT.multipliedBy(3).toMillis());

    Assert.assertFalse("dispatch never returned after the interrupt", dispatch.thread().isAlive());
    Throwable failure = dispatch.thrown().get();
    Assert.assertTrue(
        "dispatch should have thrown InitialSyncException, got: " + failure,
        failure instanceof InitialSyncException);
    return (InitialSyncException) failure;
  }

  private static void awaitWorkersScanning(Fixture fixture, int workers) {
    awaitOrFail(
        () -> fixture.workersInTheirScan() >= workers, "the workers never started scanning");
  }

  private static void awaitPartitionRecordedDone(Fixture fixture, int partitionId) {
    awaitOrFail(
        () -> fixture.partition(partitionId).isDone(),
        "the window never recorded the partition as done");
  }

  /** Whichever wait threw cleared the flag, so its clearing is dispatch observing the interrupt. */
  private static void awaitInterruptConsumed(Thread thread) {
    awaitOrFail(() -> !thread.isInterrupted(), "dispatch never observed the interrupt");
  }

  private static void awaitOrFail(BooleanSupplier condition, String message) {
    Instant limit = Instant.now().plus(ABORT_TIMEOUT);
    while (!condition.getAsBoolean()) {
      if (Instant.now().isAfter(limit)) {
        throw new AssertionError(message);
      }
      LockSupport.parkNanos(Duration.ofMillis(1).toNanos());
    }
  }
}
