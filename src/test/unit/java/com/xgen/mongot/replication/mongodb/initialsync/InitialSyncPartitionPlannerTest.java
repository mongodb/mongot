package com.xgen.mongot.replication.mongodb.initialsync;

import static com.google.common.truth.Truth.assertThat;
import static com.xgen.testing.mongot.mock.index.SearchIndex.IGNORE_METRICS;
import static com.xgen.testing.mongot.mock.index.SearchIndex.MOCK_INDEX_DEFINITION_GENERATION;
import static com.xgen.testing.mongot.mock.replication.mongodb.common.DocumentIndexer.mockDocumentIndexer;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.mongodb.MongoException;
import com.xgen.mongot.metrics.MetricsFactory;
import com.xgen.mongot.monitor.ToggleGate;
import com.xgen.mongot.replication.mongodb.common.CollectionSamplingMongoClient;
import com.xgen.mongot.replication.mongodb.common.CollectionStatsMongoClient;
import com.xgen.mongot.replication.mongodb.common.IndexingWorkScheduler;
import com.xgen.mongot.replication.mongodb.common.PartitionPlanningMongoClient;
import com.xgen.mongot.replication.mongodb.initialsync.InitialSyncPartitionPlan.PartitionRange;
import com.xgen.mongot.replication.mongodb.initialsync.InitialSyncPartitionPlan.PartitionedResult;
import com.xgen.mongot.replication.mongodb.initialsync.InitialSyncPartitionPlan.SequentialFallback;
import com.xgen.mongot.replication.mongodb.initialsync.InitialSyncPartitionPlan.ShortCircuitReason;
import com.xgen.mongot.util.mongodb.CollectionStatsResponse;
import com.xgen.mongot.util.mongodb.serialization.MongoDbCollectionInfo;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.stream.LongStream;
import org.bson.BsonTimestamp;
import org.bson.RawBsonDocument;
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

    private final CollectionSamplingMongoClient samplingClient =
        mock(CollectionSamplingMongoClient.class);

    private final InitialSyncPartitionPlanner planner;

    private Fixture(Clock clock, Duration stageBudget) {
      this(clock, stageBudget, PARTITION_COUNT);
    }

    private Fixture(Clock clock, Duration stageBudget, int partitionCount) {
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
                  partitionCount,
                  ToggleGate.opened())
              .withProgress(HIGH_WATER_MARK);

      this.planner =
          new InitialSyncPartitionPlanner(
              clock,
              context,
              this.statsClient,
              this.planningClient,
              this.samplingClient,
              new MetricsFactory("test", new SimpleMeterRegistry()),
              stageBudget);
    }

    /** A source that is large enough to partition and reports an incarnation. */
    static Fixture healthy() {
      return healthy(PARTITION_COUNT);
    }

    /** A healthy source configured for the given number of partitions. */
    static Fixture healthy(int partitionCount) {
      Fixture fixture = new Fixture(Clock.systemUTC(), Duration.ofSeconds(30), partitionCount);
      fixture.withDocumentCount(InitialSyncPartitionPlanner.MIN_DOCUMENT_COUNT);
      when(fixture.planningClient.captureInitialSyncId(any(), any()))
          .thenReturn(Optional.of(INITIAL_SYNC_ID));
      fixture.withSampleSize(InitialSyncPartitionPlanner.SAMPLE_SIZE);
      fixture.withResolvedRecordIds(InitialSyncPartitionPlanner.SAMPLE_SIZE);
      return fixture;
    }

    /** Stubs {@code $sample} to return the given number of {@code _id}s. */
    Fixture withSampleSize(int size) {
      List<RawBsonDocument> sample = new ArrayList<>();
      for (int i = 0; i < size; i++) {
        sample.add(RawBsonDocument.parse(String.format("{\"_id\": %d}", i)));
      }
      when(this.samplingClient.getSamples(any(), any(), anyInt(), any(), any())).thenReturn(sample);
      return this;
    }

    /** Stubs consecutive resolves to return the given counts, one per call. */
    Fixture withResolvedRecordIdsPerAttempt(int first, int second) {
      when(this.planningClient.resolveRecordIds(any(), any(), any()))
          .thenReturn(recordIds(first), recordIds(second));
      return this;
    }

    /** Stubs the resolve to return the given number of recordIds. */
    Fixture withResolvedRecordIds(int count) {
      when(this.planningClient.resolveRecordIds(any(), any(), any())).thenReturn(recordIds(count));
      return this;
    }

    private static List<Long> recordIds(int count) {
      List<Long> recordIds = new ArrayList<>(count);
      for (int i = 0; i < count; i++) {
        recordIds.add((long) i);
      }
      return recordIds;
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

  private static ImmutableList<PartitionRange> partitionsFrom(InitialSyncPartitionPlan plan) {
    assertThat(plan).isInstanceOf(PartitionedResult.class);
    return ((PartitionedResult) plan).partitions();
  }

  @Test
  public void plan_healthyCollection_returnsOnePartitionPerRequestedCount() {
    Fixture fixture = Fixture.healthy();

    ImmutableList<PartitionRange> partitions =
        partitionsFrom(fixture.planner.plan(ORDINARY_COLLECTION));

    // Contiguity is asserted by PartitionedResult itself, so only the open ends are checked here.
    assertThat(partitions).hasSize(PARTITION_COUNT);
    assertThat(partitions.get(0).lowerRecordId()).isEmpty();
    assertThat(partitions.get(PARTITION_COUNT - 1).upperRecordId()).isEmpty();
  }

  @Test
  public void plan_healthyCollection_carriesTheCapturedInitialSyncId() {
    Fixture fixture = Fixture.healthy();

    InitialSyncPartitionPlan plan = fixture.planner.plan(ORDINARY_COLLECTION);

    assertThat(plan).isInstanceOf(PartitionedResult.class);
    assertThat(((PartitionedResult) plan).initialSyncId()).isEqualTo(INITIAL_SYNC_ID);
  }

  @Test
  public void plan_partitionCountBelowMinimum_clampsUp() {
    Fixture fixture = Fixture.healthy(1);

    assertThat(partitionsFrom(fixture.planner.plan(ORDINARY_COLLECTION)))
        .hasSize(InitialSyncPartitionPlanner.MIN_PARTITION_COUNT);
  }

  @Test
  public void plan_partitionCountAboveMaximum_clampsDown() {
    Fixture fixture = Fixture.healthy(500);

    assertThat(partitionsFrom(fixture.planner.plan(ORDINARY_COLLECTION)))
        .hasSize(InitialSyncPartitionPlanner.MAX_PARTITION_COUNT);
  }

  @Test
  public void plan_resolveThrows_fallsBackToSequential() {
    Fixture fixture = Fixture.healthy();
    when(fixture.planningClient.resolveRecordIds(any(), any(), any()))
        .thenThrow(new MongoException("resolve failed"));

    InitialSyncPartitionPlan plan = fixture.planner.plan(ORDINARY_COLLECTION);

    assertThat(reasonFrom(plan)).isEqualTo(ShortCircuitReason.PLANNING_FAILED);
  }

  @Test
  public void partitionsFrom_cutsAtEvenQuantiles() {
    // 1000 recordIds into four ranges meet at the 250th, 500th and 750th of them.
    List<Long> sorted = LongStream.range(0, 1000).boxed().toList();

    ImmutableList<PartitionRange> partitions =
        InitialSyncPartitionPlanner.partitionsFrom(sorted, 4);

    assertThat(partitions.stream().map(PartitionRange::upperRecordId).toList())
        .containsExactly(Optional.of(250L), Optional.of(500L), Optional.of(750L), Optional.empty())
        .inOrder();
    assertThat(partitions.stream().map(PartitionRange::lowerRecordId).toList())
        .containsExactly(Optional.empty(), Optional.of(250L), Optional.of(500L), Optional.of(750L))
        .inOrder();
  }

  @Test
  public void partitionsFrom_cutsThatRepeat_throws() {
    // Unreachable with real recordIds, which are unique, but the consumer alerts on a repeat so
    // the planner must not emit one.
    List<Long> sorted = Collections.nCopies(1000, 7L);

    assertThrows(
        IllegalStateException.class, () -> InitialSyncPartitionPlanner.partitionsFrom(sorted, 4));
  }

  @Test
  public void plan_tooFewResolved_redrawsUpToTheAttemptBudget() {
    // Documents deleted between the sample and the resolve are worth redrawing for, but a
    // collection that never resolves enough stops at the budget rather than retrying forever.
    Fixture fixture = Fixture.healthy();
    fixture.withResolvedRecordIds(InitialSyncPartitionPlanner.MIN_RESOLVED_SAMPLES - 1);

    fixture.planner.plan(ORDINARY_COLLECTION);

    verify(fixture.samplingClient, times(InitialSyncPartitionPlanner.MAX_SAMPLE_ATTEMPTS))
        .getSamples(any(), any(), anyInt(), any(), any());
  }

  @Test
  public void plan_redrawResolvesEnough_partitionsWithoutSpendingTheBudget() {
    // The redraw exists for a draw that lands better than the last, so a good one stops the loop.
    Fixture fixture = Fixture.healthy();
    fixture.withResolvedRecordIdsPerAttempt(
        InitialSyncPartitionPlanner.MIN_RESOLVED_SAMPLES - 1,
        InitialSyncPartitionPlanner.SAMPLE_SIZE);

    InitialSyncPartitionPlan plan = fixture.planner.plan(ORDINARY_COLLECTION);

    assertThat(plan).isInstanceOf(PartitionedResult.class);
    verify(fixture.samplingClient, times(2)).getSamples(any(), any(), anyInt(), any(), any());
  }

  @Test
  public void plan_neverEnoughResolved_fallsBackToSequential() {
    Fixture fixture = Fixture.healthy();
    fixture.withResolvedRecordIds(InitialSyncPartitionPlanner.MIN_RESOLVED_SAMPLES - 1);

    InitialSyncPartitionPlan plan = fixture.planner.plan(ORDINARY_COLLECTION);

    assertThat(reasonFrom(plan)).isEqualTo(ShortCircuitReason.TOO_FEW_DOCUMENTS);
  }

  @Test
  public void plan_sampleThrows_fallsBackToSequential() {
    Fixture fixture = Fixture.healthy();
    when(fixture.samplingClient.getSamples(any(), any(), anyInt(), any(), any()))
        .thenThrow(new MongoException("$sample failed"));

    InitialSyncPartitionPlan plan = fixture.planner.plan(ORDINARY_COLLECTION);

    assertThat(reasonFrom(plan)).isEqualTo(ShortCircuitReason.PLANNING_FAILED);
  }

  @Test
  public void plan_sampleComesBackShort_fallsBackToSequential() {
    // $sample errors rather than under-delivering, so a short batch means the collection shrank
    // since the size estimate.
    Fixture fixture = Fixture.healthy();
    fixture.withSampleSize(InitialSyncPartitionPlanner.SAMPLE_SIZE - 1);

    InitialSyncPartitionPlan plan = fixture.planner.plan(ORDINARY_COLLECTION);

    assertThat(reasonFrom(plan)).isEqualTo(ShortCircuitReason.TOO_FEW_DOCUMENTS);
  }

  @Test
  public void plan_collectionBelowMinimumSize_doesNotSample() {
    Fixture fixture = Fixture.healthy();
    fixture.withDocumentCount(InitialSyncPartitionPlanner.MIN_DOCUMENT_COUNT - 1);

    fixture.planner.plan(ORDINARY_COLLECTION);

    verify(fixture.samplingClient, never()).getSamples(any(), any(), anyInt(), any(), any());
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
