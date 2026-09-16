package com.xgen.mongot.replication.mongodb.initialsync;

import static com.google.common.truth.Truth.assertThat;
import static com.xgen.testing.mongot.mock.index.SearchIndex.IGNORE_METRICS;
import static com.xgen.testing.mongot.mock.index.SearchIndex.MOCK_INDEX_DEFINITION_GENERATION;
import static com.xgen.testing.mongot.mock.replication.mongodb.common.DocumentIndexer.mockDocumentIndexer;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.mongodb.MongoException;
import com.xgen.mongot.metrics.MetricsFactory;
import com.xgen.mongot.monitor.ToggleGate;
import com.xgen.mongot.replication.mongodb.common.CollectionStatsMongoClient;
import com.xgen.mongot.replication.mongodb.common.IndexingWorkScheduler;
import com.xgen.mongot.replication.mongodb.common.PartitionPlanningMongoClient;
import com.xgen.mongot.replication.mongodb.initialsync.InitialSyncPartitionPlan.SequentialFallback;
import com.xgen.mongot.replication.mongodb.initialsync.InitialSyncPartitionPlan.ShortCircuitReason;
import com.xgen.mongot.util.mongodb.CollectionStatsResponse;
import com.xgen.mongot.util.mongodb.serialization.MongoDbCollectionInfo;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Clock;
import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import org.bson.BsonTimestamp;
import org.junit.Test;

public class InitialSyncPartitionPlannerTest {

  private static final BsonTimestamp HIGH_WATER_MARK = new BsonTimestamp(10, 1);
  private static final UUID INITIAL_SYNC_ID = UUID.randomUUID();
  private static final int PARTITION_COUNT = 4;

  private static final MongoDbCollectionInfo.Collection ORDINARY_COLLECTION =
      new MongoDbCollectionInfo.Collection(
          "collection",
          new MongoDbCollectionInfo.Collection.Info(UUID.randomUUID()),
          new MongoDbCollectionInfo.Collection.Options(false));

  private static final MongoDbCollectionInfo.Collection CLUSTERED_COLLECTION =
      new MongoDbCollectionInfo.Collection(
          "collection",
          new MongoDbCollectionInfo.Collection.Info(UUID.randomUUID()),
          new MongoDbCollectionInfo.Collection.Options(true));

  private static final class Fixture {
    private final CollectionStatsMongoClient statsClient = mock(CollectionStatsMongoClient.class);

    private final PartitionPlanningMongoClient planningClient =
        mock(PartitionPlanningMongoClient.class);

    private final InitialSyncPartitionPlanner planner;

    private Fixture(Clock clock) {
      this(clock, Duration.ofSeconds(30));
    }

    private Fixture(Clock clock, Duration stageBudget) {
      InitialSyncContext context =
          InitialSyncContext.create(
                  MOCK_INDEX_DEFINITION_GENERATION,
                  mock(IndexingWorkScheduler.class),
                  mockDocumentIndexer(),
                  IGNORE_METRICS,
                  Optional.empty(),
                  false,
                  true,
                  true,
                  PARTITION_COUNT,
                  ToggleGate.opened())
              .withProgress(HIGH_WATER_MARK);

      this.planner =
          new InitialSyncPartitionPlanner(
              clock,
              context,
              this.statsClient,
              this.planningClient,
              new MetricsFactory("test", new SimpleMeterRegistry()),
              stageBudget);
    }

    /** A source that is large enough to partition and reports an incarnation. */
    static Fixture healthy() {
      Fixture fixture = new Fixture(Clock.systemUTC());
      fixture.withDocumentCount(InitialSyncPartitionPlanner.MIN_DOCUMENT_COUNT);
      when(fixture.planningClient.captureInitialSyncId(any(), any()))
          .thenReturn(Optional.of(INITIAL_SYNC_ID));
      return fixture;
    }

    Fixture withDocumentCount(long count) {
      when(this.statsClient.getStats(any())).thenReturn(new CollectionStatsResponse(count, 100));
      return this;
    }
  }

  private static ShortCircuitReason reasonFrom(InitialSyncPartitionPlan plan) {
    assertThat(plan).isInstanceOf(SequentialFallback.class);
    return ((SequentialFallback) plan).reason();
  }

  @Test
  public void plan_clusteredCollection_fallsBackToSequential() {
    Fixture fixture = Fixture.healthy();

    InitialSyncPartitionPlan plan = fixture.planner.plan(CLUSTERED_COLLECTION);

    assertThat(reasonFrom(plan)).isEqualTo(ShortCircuitReason.CLUSTERED_COLLECTION);
  }

  @Test
  public void plan_clusteredCollection_issuesNoSourceReads() {
    // The clustered bit rides on the listCollections namespace resolution already issued, so
    // declining on it must not cost a round trip.
    Fixture fixture = Fixture.healthy();

    fixture.planner.plan(CLUSTERED_COLLECTION);

    verify(fixture.statsClient, never()).getStats(any());
    verify(fixture.planningClient, never()).captureInitialSyncId(any(), any());
  }

  @Test
  public void plan_documentCountBelowMinimum_fallsBackToSequential() {
    Fixture fixture = Fixture.healthy();
    fixture.withDocumentCount(InitialSyncPartitionPlanner.MIN_DOCUMENT_COUNT - 1);

    InitialSyncPartitionPlan plan = fixture.planner.plan(ORDINARY_COLLECTION);

    assertThat(reasonFrom(plan)).isEqualTo(ShortCircuitReason.TOO_FEW_DOCUMENTS);
  }

  @Test
  public void plan_emptyCollection_fallsBackToSequential() {
    Fixture fixture = Fixture.healthy();
    fixture.withDocumentCount(0);

    InitialSyncPartitionPlan plan = fixture.planner.plan(ORDINARY_COLLECTION);

    assertThat(reasonFrom(plan)).isEqualTo(ShortCircuitReason.TOO_FEW_DOCUMENTS);
  }

  @Test
  public void plan_documentCountBelowMinimum_doesNotCaptureInitialSyncId() {
    Fixture fixture = Fixture.healthy();
    fixture.withDocumentCount(InitialSyncPartitionPlanner.MIN_DOCUMENT_COUNT - 1);

    fixture.planner.plan(ORDINARY_COLLECTION);

    verify(fixture.planningClient, never()).captureInitialSyncId(any(), any());
  }

  @Test
  public void plan_documentCountAtMinimum_capturesInitialSyncId() {
    // The floor is inclusive: a collection exactly at it is still worth partitioning.
    Fixture fixture = Fixture.healthy();
    fixture.withDocumentCount(InitialSyncPartitionPlanner.MIN_DOCUMENT_COUNT);

    fixture.planner.plan(ORDINARY_COLLECTION);

    verify(fixture.planningClient).captureInitialSyncId(any(), eq(HIGH_WATER_MARK));
  }

  @Test
  public void plan_noInitialSyncId_fallsBackToSequential() {
    Fixture fixture = Fixture.healthy();
    when(fixture.planningClient.captureInitialSyncId(any(), any())).thenReturn(Optional.empty());

    InitialSyncPartitionPlan plan = fixture.planner.plan(ORDINARY_COLLECTION);

    assertThat(reasonFrom(plan)).isEqualTo(ShortCircuitReason.NO_INITIAL_SYNC_ID);
  }

  @Test
  public void plan_sizeEstimateThrows_fallsBackToSequential() {
    // Planning is an optimisation, so a failed read may never surface as an initial sync failure.
    Fixture fixture = Fixture.healthy();
    when(fixture.statsClient.getStats(any())).thenThrow(new MongoException("collStats failed"));

    InitialSyncPartitionPlan plan = fixture.planner.plan(ORDINARY_COLLECTION);

    assertThat(reasonFrom(plan)).isEqualTo(ShortCircuitReason.PLANNING_FAILED);
  }

  @Test
  public void plan_initialSyncIdCaptureThrows_fallsBackToSequential() {
    Fixture fixture = Fixture.healthy();
    when(fixture.planningClient.captureInitialSyncId(any(), any()))
        .thenThrow(new MongoException("find failed"));

    InitialSyncPartitionPlan plan = fixture.planner.plan(ORDINARY_COLLECTION);

    assertThat(reasonFrom(plan)).isEqualTo(ShortCircuitReason.PLANNING_FAILED);
  }

  @Test
  public void plan_readBlocksPastStageBudget_fallsBackToSequential() throws Exception {
    // A read that never returns, which is what the budget exists for: the planning thread stays
    // blocked and the caller has to give up on it rather than wait.
    CountDownLatch released = new CountDownLatch(1);
    Fixture fixture = new Fixture(Clock.systemUTC(), Duration.ofMillis(200));
    when(fixture.statsClient.getStats(any()))
        .thenAnswer(
            invocation -> {
              released.await();
              return new CollectionStatsResponse(InitialSyncPartitionPlanner.MIN_DOCUMENT_COUNT, 1);
            });

    try {
      InitialSyncPartitionPlan plan = fixture.planner.plan(ORDINARY_COLLECTION);

      assertThat(reasonFrom(plan)).isEqualTo(ShortCircuitReason.PLANNING_BUDGET_EXCEEDED);
    } finally {
      // Let the abandoned worker finish so it does not outlive the test.
      released.countDown();
    }
  }

  @Test
  public void plan_readCompletesWithinStageBudget_doesNotReportBudgetExceeded() {
    Fixture fixture = new Fixture(Clock.systemUTC(), Duration.ofSeconds(30));
    fixture.withDocumentCount(InitialSyncPartitionPlanner.MIN_DOCUMENT_COUNT);
    when(fixture.planningClient.captureInitialSyncId(any(), any()))
        .thenReturn(Optional.of(INITIAL_SYNC_ID));

    InitialSyncPartitionPlan plan = fixture.planner.plan(ORDINARY_COLLECTION);

    assertThat(reasonFrom(plan)).isNotEqualTo(ShortCircuitReason.PLANNING_BUDGET_EXCEEDED);
  }
}
