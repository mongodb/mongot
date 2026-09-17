package com.xgen.mongot.replication.mongodb.initialsync;

import static com.google.common.truth.Truth.assertThat;
import static com.xgen.testing.mongot.mock.index.SearchIndex.IGNORE_METRICS;
import static com.xgen.testing.mongot.mock.index.SearchIndex.MOCK_INDEX_DEFINITION_GENERATION;
import static com.xgen.testing.mongot.mock.replication.mongodb.common.DocumentIndexer.mockDocumentIndexer;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.xgen.mongot.index.version.IndexFormatVersion;
import com.xgen.mongot.metrics.MetricsFactory;
import com.xgen.mongot.monitor.ToggleGate;
import com.xgen.mongot.replication.mongodb.common.BufferlessParallelNaturalOrderInitialSyncResumeInfo;
import com.xgen.mongot.replication.mongodb.common.CollectionScanMongoClient;
import com.xgen.mongot.replication.mongodb.common.DocumentIndexer;
import com.xgen.mongot.replication.mongodb.common.IndexCommitUserData;
import com.xgen.mongot.replication.mongodb.common.IndexingWorkScheduler;
import com.xgen.mongot.replication.mongodb.common.InitialSyncException;
import com.xgen.mongot.util.mongodb.CollectionScanAggregateCommand;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Supplier;
import org.bson.BsonTimestamp;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentCaptor;

@RunWith(JUnit4.class)
public class ParallelCollectionScanSessionTest {

  private static final BsonTimestamp HIGH_WATER_MARK = new BsonTimestamp(10, 1);
  private static final BsonTimestamp LATER_HIGH_WATER_MARK = new BsonTimestamp(20, 1);
  private static final UUID INITIAL_SYNC_ID =
      UUID.fromString("390a604a-4979-4d10-9f4f-3d2552224aee");
  private static final Duration SCAN_TIME = Duration.ofMinutes(5);
  private static final Clock FIXED_CLOCK = Clock.fixed(Instant.EPOCH, ZoneOffset.UTC);
  private static final String AFTER_CLUSTER_TIME = "afterClusterTime";

  private final DocumentIndexer indexer = mockDocumentIndexer();

  private final ParallelCollectionScanDispatcher dispatcher =
      mock(ParallelCollectionScanDispatcher.class);

  /**
   * Installing the supplier is what makes a periodic commit persist the whole session's position. A
   * blob captured at install time would freeze the position the session started at.
   */
  @Test
  public void createInstallsASupplierThatReadsTheSessionsLivePosition() {
    ParallelCollectionScanResumeState resumeState = resumeState(HIGH_WATER_MARK);

    try (ParallelCollectionScanSession ignored =
        ParallelCollectionScanSession.create(
            FIXED_CLOCK,
            context(),
            mock(InitialSyncMongoClient.class),
            resumeState,
            SCAN_TIME,
            metricsFactory(),
            new SimpleMeterRegistry())) {

      ArgumentCaptor<Supplier<IndexCommitUserData>> installed = ArgumentCaptor.captor();
      verify(this.indexer).updateCommitUserDataSupplier(installed.capture());

      resumeState.getPartitions().getFirst().advanceResumeRecordId(42L);
      resumeState.advanceHighWaterMark(LATER_HIGH_WATER_MARK);
      BufferlessParallelNaturalOrderInitialSyncResumeInfo persisted =
          (BufferlessParallelNaturalOrderInitialSyncResumeInfo)
              installed.getValue().get().getInitialSyncResumeInfo().orElseThrow();

      assertThat(persisted.getResumeOperationTime()).isEqualTo(LATER_HIGH_WATER_MARK);
      assertThat(persisted.getPartitions().getFirst().getResumeRecordId()).hasValue(42L);
    }
  }

  /**
   * Each window has to read at the floor the applier left it, not the one the session started at,
   * or a window would scan a snapshot older than the events already indexed.
   */
  @Test
  public void scanWindowHoldsEachWindowToTheSessionsCurrentFreshnessFloor() throws Exception {
    ParallelCollectionScanResumeState resumeState = resumeState(HIGH_WATER_MARK);
    ParallelCollectionScanSession session = session(resumeState);
    when(this.dispatcher.dispatch(any(), any())).thenReturn(true);

    session.scanWindow();
    resumeState.advanceHighWaterMark(LATER_HIGH_WATER_MARK);
    session.scanWindow();

    ArgumentCaptor<BsonTimestamp> floors = ArgumentCaptor.captor();
    verify(this.dispatcher, times(2)).dispatch(floors.capture(), eq(resumeState.getPartitions()));
    assertThat(floors.getAllValues())
        .containsExactly(HIGH_WATER_MARK, LATER_HIGH_WATER_MARK)
        .inOrder();
  }

  @Test
  public void scanWindowReportsWhetherAnyRangeIsLeft() throws Exception {
    ParallelCollectionScanSession session = session(resumeState(HIGH_WATER_MARK));
    when(this.dispatcher.dispatch(any(), any())).thenReturn(true, false);

    assertThat(session.scanWindow()).isTrue();
    assertThat(session.scanWindow()).isFalse();
  }

  @Test
  public void signalShutdownAsksTheRunningWindowToStop() {
    session(resumeState(HIGH_WATER_MARK)).signalShutdown();

    verify(this.dispatcher).signalShutdown();
  }

  /** Nothing else reclaims the partition pool, so closing the session has to release it. */
  @Test
  public void closeReleasesTheWindowMachinery() {
    session(resumeState(HIGH_WATER_MARK)).close();

    verify(this.dispatcher).close();
  }

  /**
   * A scanner takes its {@code afterClusterTime} from the context it was built with, so handing
   * every window the session's own context would pin all of them to the floor the sync opened at.
   */
  @Test
  public void scannerFactoryOpensCursorsAtTheWindowsFreshnessFloor() throws Exception {
    CollectionScanMongoClient<InitialSyncException> cursor = mock(CollectionScanMongoClient.class);
    when(cursor.hasNext()).thenReturn(false);

    ArgumentCaptor<CollectionScanAggregateCommand> command = ArgumentCaptor.captor();
    InitialSyncMongoClient mongoClient = mock(InitialSyncMongoClient.class);
    when(mongoClient.getCollectionAggregateCommandMongoClient(
            command.capture(), any(), any(), any()))
        .thenReturn(cursor);

    ParallelCollectionScanResumeState resumeState = resumeState(HIGH_WATER_MARK);
    ParallelCollectionScanner scanner =
        ParallelCollectionScanSession.scannerFactory(
                FIXED_CLOCK, context(), mongoClient, resumeState, metricsFactory())
            .create(LATER_HIGH_WATER_MARK, resumeState.getPartitions().getFirst(), () -> false);

    scanner.scanUntil(Instant.EPOCH.plus(SCAN_TIME));

    assertThat(command.getValue().getReadConcernDocument().orElseThrow().get(AFTER_CLUSTER_TIME))
        .isEqualTo(LATER_HIGH_WATER_MARK);
  }

  private ParallelCollectionScanSession session(ParallelCollectionScanResumeState resumeState) {
    return new ParallelCollectionScanSession(context(), resumeState, this.dispatcher);
  }

  private InitialSyncContext context() {
    return InitialSyncContext.create(
        MOCK_INDEX_DEFINITION_GENERATION,
        mock(IndexingWorkScheduler.class),
        this.indexer,
        IGNORE_METRICS,
        Optional.empty(),
        false,
        true,
        true,
        2,
        ToggleGate.opened());
  }

  private static MetricsFactory metricsFactory() {
    return new MetricsFactory("test", new SimpleMeterRegistry());
  }

  private static ParallelCollectionScanResumeState resumeState(BsonTimestamp highWaterMark) {
    return new ParallelCollectionScanResumeState(
        IndexFormatVersion.CURRENT.versionNumber,
        INITIAL_SYNC_ID,
        Optional.of("shard0-0:27017"),
        highWaterMark,
        List.of(
            new ParallelCollectionScanPartitionState(
                0, Optional.empty(), Optional.of(100L), Optional.empty(), false),
            new ParallelCollectionScanPartitionState(
                1, Optional.of(100L), Optional.empty(), Optional.empty(), false)));
  }
}
