package com.xgen.mongot.replication.mongodb.common;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.google.common.base.CaseFormat;
import com.xgen.mongot.index.IndexMetricsUpdater.ReplicationMetricsUpdater;
import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.version.Generation;
import com.xgen.mongot.index.version.GenerationId;
import com.xgen.mongot.metrics.MetricsFactory;
import com.xgen.mongot.metrics.ServerStatusDataExtractor;
import com.xgen.mongot.util.BsonUtils;
import com.xgen.mongot.util.Enums;
import com.xgen.mongot.util.concurrent.Executors;
import com.xgen.testing.mongot.mock.index.SearchIndex;
import io.micrometer.core.instrument.FunctionCounter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.IntStream;
import org.bson.BsonDocument;
import org.bson.RawBsonDocument;
import org.bson.types.ObjectId;
import org.junit.Assert;
import org.junit.Test;

public class DecodingWorkSchedulerTest {

  private static final double NO_EPSILON = 0.0;

  private static final ReplicationMetricsUpdater IGNORE_METRICS =
      SearchIndex.mockReplicationMetricsUpdater(IndexDefinition.Type.SEARCH);

  @Test
  public void testSingleBatch() throws Exception {
    DocumentBatchDecoder decoder = mock(DocumentBatchDecoder.class);
    DecodingWorkScheduler scheduler = DecodingWorkScheduler.create(2, new SimpleMeterRegistry());

    List<RawBsonDocument> batch = List.of();
    CompletableFuture<Void> decodingFuture =
        scheduler.schedule(
            new GenerationId(new ObjectId(), Generation.CURRENT),
            Optional.of(new ObjectId()),
            batch,
            SchedulerQueue.Priority.STEADY_STATE_CHANGE_STREAM,
            decoder,
            IGNORE_METRICS);

    decodingFuture.get(5, TimeUnit.SECONDS);

    verify(decoder).decode(eq(batch));
  }

  @Test
  public void testExceptionDuringBatch() {
    DecodingWorkScheduler scheduler = DecodingWorkScheduler.create(2, new SimpleMeterRegistry());

    List<RawBsonDocument> batch = List.of();
    CompletableFuture<Void> decodingFuture =
        scheduler.schedule(
            new GenerationId(new ObjectId(), Generation.CURRENT),
            Optional.of(new ObjectId()),
            batch,
            SchedulerQueue.Priority.STEADY_STATE_CHANGE_STREAM,
            events -> {
              throw new RuntimeException();
            },
            IGNORE_METRICS);

    Assert.assertThrows(ExecutionException.class, () -> decodingFuture.get(5, TimeUnit.SECONDS));
  }

  @Test
  public void testScheduledBatchAfterExceptionNotExecuted() {
    DocumentBatchDecoder decoder = mock(DocumentBatchDecoder.class);
    DecodingWorkScheduler scheduler = DecodingWorkScheduler.create(2, new SimpleMeterRegistry());

    GenerationId generationId = new GenerationId(new ObjectId(), Generation.CURRENT);
    ObjectId attemptId = new ObjectId();
    CompletableFuture<Void> decodingFuture1 =
        scheduler.schedule(
            generationId,
            Optional.of(attemptId),
            List.of(),
            SchedulerQueue.Priority.STEADY_STATE_CHANGE_STREAM,
            events -> {
              throw new RuntimeException();
            },
            IGNORE_METRICS);

    CompletableFuture<Void> decodingFuture2 =
        scheduler.schedule(
            generationId,
            Optional.of(attemptId),
            List.of(),
            SchedulerQueue.Priority.STEADY_STATE_CHANGE_STREAM,
            decoder,
            IGNORE_METRICS);

    Assert.assertThrows(ExecutionException.class, () -> decodingFuture1.get(5, TimeUnit.SECONDS));
    Assert.assertThrows(TimeoutException.class, () -> decodingFuture2.get(5, TimeUnit.SECONDS));
    verify(decoder, never()).decode(anyList());
  }

  @Test
  public void testMultipleBatchesSameGenerationInOrder() throws Exception {
    CountDownLatch finishDecoding = new CountDownLatch(1);

    DocumentBatchDecoder decoder =
        spy(
            new DocumentBatchDecoder() {
              @Override
              public void decode(List<RawBsonDocument> events) {
                warpWithRuntimeException(
                    () -> {
                      finishDecoding.await();
                      return null;
                    });
              }
            });

    DecodingWorkScheduler scheduler = DecodingWorkScheduler.create(2, new SimpleMeterRegistry());

    GenerationId generationId = new GenerationId(new ObjectId(), Generation.CURRENT);
    ObjectId attemptId = new ObjectId();

    List<RawBsonDocument> firstBatch = List.of();
    CompletableFuture<Void> decodingFuture1 =
        scheduler.schedule(
            generationId,
            Optional.of(attemptId),
            firstBatch,
            SchedulerQueue.Priority.STEADY_STATE_CHANGE_STREAM,
            decoder,
            IGNORE_METRICS);

    List<RawBsonDocument> secondBatch = List.of();
    CompletableFuture<Void> decodingFuture2 =
        scheduler.schedule(
            generationId,
            Optional.of(attemptId),
            secondBatch,
            SchedulerQueue.Priority.STEADY_STATE_CHANGE_STREAM,
            decoder,
            IGNORE_METRICS);

    // first batch should decode immediately. second batch should wait for the first batch to
    // since they are both for the same index
    verify(decoder, timeout(500).times(1)).decode(eq(firstBatch));

    finishDecoding.countDown();

    // after the first batch is completed, decoding for the second batch can begin
    decodingFuture1.get(5, TimeUnit.SECONDS);
    decodingFuture2.get(5, TimeUnit.SECONDS);
    verify(decoder, timeout(500).times(2)).decode(eq(secondBatch));
  }

  @Test
  public void testMaxConcurrencyBatches() throws Exception {
    Semaphore finishDecoding = new Semaphore(0);

    DocumentBatchDecoder decoder =
        spy(
            new DocumentBatchDecoder() {
              @Override
              public void decode(List<RawBsonDocument> events) {
                warpWithRuntimeException(
                    () -> {
                      finishDecoding.acquire();
                      return null;
                    });
              }
            });

    DecodingWorkScheduler scheduler = DecodingWorkScheduler.create(2, new SimpleMeterRegistry());

    List<RawBsonDocument> firstBatch = List.of();
    CompletableFuture<Void> decodingFuture1 =
        scheduler.schedule(
            new GenerationId(new ObjectId(), Generation.CURRENT),
            Optional.of(new ObjectId()),
            firstBatch,
            SchedulerQueue.Priority.STEADY_STATE_CHANGE_STREAM,
            decoder,
            IGNORE_METRICS);

    List<RawBsonDocument> secondBatch = List.of();
    CompletableFuture<Void> decodingFuture2 =
        scheduler.schedule(
            new GenerationId(new ObjectId(), Generation.CURRENT),
            Optional.of(new ObjectId()),
            secondBatch,
            SchedulerQueue.Priority.STEADY_STATE_CHANGE_STREAM,
            decoder,
            IGNORE_METRICS);

    List<RawBsonDocument> thirdBatch = List.of();
    CompletableFuture<Void> decodingFuture3 =
        scheduler.schedule(
            new GenerationId(new ObjectId(), Generation.CURRENT),
            Optional.of(new ObjectId()),
            thirdBatch,
            SchedulerQueue.Priority.STEADY_STATE_CHANGE_STREAM,
            decoder,
            IGNORE_METRICS);

    // first and second batches should decode immediately. third batch should wait as max allowance
    // reached
    verify(decoder, timeout(500).times(2)).decode(anyList());

    Assert.assertThrows(TimeoutException.class, () -> decodingFuture3.get(5, TimeUnit.SECONDS));

    // finish one of the first batches
    finishDecoding.release(1);
    verify(decoder, timeout(500).times(3)).decode(anyList());

    finishDecoding.release(2);
    decodingFuture1.get(5, TimeUnit.SECONDS);
    decodingFuture2.get(5, TimeUnit.SECONDS);
    decodingFuture3.get(5, TimeUnit.SECONDS);
  }

  @Test
  public void testConcurrentBatchesAtATime() throws Exception {
    Semaphore finishDecoding = new Semaphore(0);

    DocumentBatchDecoder decoder =
        spy(
            new DocumentBatchDecoder() {
              @Override
              public void decode(List<RawBsonDocument> events) {
                warpWithRuntimeException(
                    () -> {
                      finishDecoding.acquire();
                      return null;
                    });
              }
            });

    DecodingWorkScheduler scheduler = DecodingWorkScheduler.create(2, new SimpleMeterRegistry());

    GenerationId firstGenId = new GenerationId(new ObjectId(), Generation.CURRENT);
    ObjectId firstAttemptId = new ObjectId();
    GenerationId secondGenId = new GenerationId(new ObjectId(), Generation.CURRENT);
    ObjectId secondAttemptId = new ObjectId();

    List<RawBsonDocument> firstBatch = List.of();

    CompletableFuture<Void> decodingFuture1 =
        scheduler.schedule(
            firstGenId,
            Optional.of(firstAttemptId),
            firstBatch,
            SchedulerQueue.Priority.STEADY_STATE_CHANGE_STREAM,
            decoder,
            IGNORE_METRICS);

    List<RawBsonDocument> secondBatch = List.of();
    CompletableFuture<Void> decodingFuture2 =
        scheduler.schedule(
            firstGenId,
            Optional.of(firstAttemptId),
            secondBatch,
            SchedulerQueue.Priority.STEADY_STATE_CHANGE_STREAM,
            decoder,
            IGNORE_METRICS);

    List<RawBsonDocument> thirdBatch = List.of();
    CompletableFuture<Void> decodingFuture3 =
        scheduler.schedule(
            secondGenId,
            Optional.of(secondAttemptId),
            thirdBatch,
            SchedulerQueue.Priority.STEADY_STATE_CHANGE_STREAM,
            decoder,
            IGNORE_METRICS);

    // two batches can decode at the same time, as they are for different indexes
    verify(decoder, timeout(500).times(2)).decode(anyList());

    // release the first two batches, and verify the third is scheduled
    finishDecoding.release(2);
    verify(decoder, timeout(500).times(3)).decode(anyList());

    finishDecoding.release(1);
    decodingFuture1.get(5, TimeUnit.SECONDS);
    decodingFuture2.get(5, TimeUnit.SECONDS);
    decodingFuture3.get(5, TimeUnit.SECONDS);
  }

  @Test
  public void testCancelBatchInExecutor() throws Exception {
    CyclicBarrier startedBarrier = new CyclicBarrier(2);
    CyclicBarrier doneBarrier = new CyclicBarrier(2);

    DocumentBatchDecoder decoder =
        spy(
            new DocumentBatchDecoder() {
              @Override
              public void decode(List<RawBsonDocument> events) {
                warpWithRuntimeException(
                    () -> {
                      startedBarrier.await();
                      doneBarrier.await();
                      return null;
                    });
              }
            });

    DecodingWorkScheduler scheduler = DecodingWorkScheduler.create(2, new SimpleMeterRegistry());

    GenerationId generationId = new GenerationId(new ObjectId(), Generation.CURRENT);
    ObjectId attemptId = new ObjectId();

    CompletableFuture<Void> decodingFuture =
        scheduler.schedule(
            generationId,
            Optional.of(new ObjectId()),
            List.of(),
            SchedulerQueue.Priority.STEADY_STATE_CHANGE_STREAM,
            decoder,
            IGNORE_METRICS);

    startedBarrier.await(5, TimeUnit.SECONDS);

    CompletableFuture<Void> cancelFuture =
        scheduler.cancel(generationId, Optional.of(attemptId), new Exception("error"));

    // cancel future should not complete until the in-flight batch is completed
    Assert.assertThrows(TimeoutException.class, () -> cancelFuture.get(500, TimeUnit.MILLISECONDS));

    // allow the in-flight batch to complete
    doneBarrier.await(5, TimeUnit.SECONDS);

    // the cancel future should now be able to complete
    cancelFuture.get(500, TimeUnit.MILLISECONDS);

    decodingFuture.get(5, TimeUnit.SECONDS);
  }

  @Test
  public void testCancelBatchWaiting() throws Exception {
    Semaphore finishDecoding = new Semaphore(0);

    DocumentBatchDecoder decoder =
        spy(
            new DocumentBatchDecoder() {
              @Override
              public void decode(List<RawBsonDocument> events) {
                warpWithRuntimeException(
                    () -> {
                      finishDecoding.acquire();
                      return null;
                    });
              }
            });

    DecodingWorkScheduler scheduler = DecodingWorkScheduler.create(2, new SimpleMeterRegistry());

    GenerationId generationId = new GenerationId(new ObjectId(), Generation.CURRENT);
    ObjectId attemptId = new ObjectId();

    CompletableFuture<Void> decodingFuture1 =
        scheduler.schedule(
            generationId,
            Optional.of(attemptId),
            List.of(),
            SchedulerQueue.Priority.STEADY_STATE_CHANGE_STREAM,
            decoder,
            IGNORE_METRICS);

    CompletableFuture<Void> decodingFuture2 =
        scheduler.schedule(
            generationId,
            Optional.of(attemptId),
            List.of(),
            SchedulerQueue.Priority.STEADY_STATE_CHANGE_STREAM,
            decoder,
            IGNORE_METRICS);

    CompletableFuture<Void> decodingFuture3 =
        scheduler.schedule(
            generationId,
            Optional.of(attemptId),
            List.of(),
            SchedulerQueue.Priority.STEADY_STATE_CHANGE_STREAM,
            decoder,
            IGNORE_METRICS);

    // first decoding should start
    verify(decoder, timeout(500).times(1)).decode(anyList());

    CompletableFuture<Void> cancelFuture =
        scheduler.cancel(generationId, Optional.of(attemptId), new Exception("error"));

    Assert.assertTrue(decodingFuture2.isCompletedExceptionally());
    Assert.assertTrue(decodingFuture3.isCompletedExceptionally());
    Assert.assertFalse(cancelFuture.isDone());

    finishDecoding.release();
    cancelFuture.get(5, TimeUnit.SECONDS);
    Assert.assertTrue(decodingFuture1.isDone());
    Assert.assertFalse(decodingFuture1.isCompletedExceptionally());

    // verify that batches submitted with the same attemptId after the cancel cannot be processed
    CompletableFuture<Void> decodingFuture4 =
        scheduler.schedule(
            generationId,
            Optional.of(attemptId),
            List.of(),
            SchedulerQueue.Priority.STEADY_STATE_CHANGE_STREAM,
            events -> {},
            IGNORE_METRICS);
    Assert.assertTrue(decodingFuture4.isCompletedExceptionally());

    // verify that batches submitted with a different attemptId after the cancel are still processed
    CompletableFuture<Void> decodingFuture5 =
        scheduler.schedule(
            generationId,
            Optional.of(new ObjectId()),
            List.of(),
            SchedulerQueue.Priority.STEADY_STATE_CHANGE_STREAM,
            events -> {},
            IGNORE_METRICS);
    decodingFuture5.get(5, TimeUnit.SECONDS);
  }

  @Test
  public void testCancelOnlyOne() throws Exception {
    Semaphore finishDecoding = new Semaphore(0);

    DocumentBatchDecoder decoder =
        spy(
            new DocumentBatchDecoder() {
              @Override
              public void decode(List<RawBsonDocument> events) {
                warpWithRuntimeException(
                    () -> {
                      finishDecoding.acquire();
                      return null;
                    });
              }
            });

    DecodingWorkScheduler scheduler = DecodingWorkScheduler.create(2, new SimpleMeterRegistry());

    GenerationId generationId1 = new GenerationId(new ObjectId(), Generation.CURRENT);
    GenerationId generationId2 = new GenerationId(new ObjectId(), Generation.CURRENT);
    ObjectId attemptId1 = new ObjectId();
    ObjectId attemptId2 = new ObjectId();

    CompletableFuture<Void> decodingFuture1 =
        scheduler.schedule(
            generationId1,
            Optional.of(attemptId1),
            List.of(),
            SchedulerQueue.Priority.STEADY_STATE_CHANGE_STREAM,
            decoder,
            IGNORE_METRICS);

    CompletableFuture<Void> decodingFuture2 =
        scheduler.schedule(
            generationId1,
            Optional.of(attemptId1),
            List.of(),
            SchedulerQueue.Priority.STEADY_STATE_CHANGE_STREAM,
            decoder,
            IGNORE_METRICS);

    CompletableFuture<Void> decodingFuture3 =
        scheduler.schedule(
            generationId2,
            Optional.of(attemptId2),
            List.of(),
            SchedulerQueue.Priority.STEADY_STATE_CHANGE_STREAM,
            decoder,
            IGNORE_METRICS);

    verify(decoder, timeout(500).times(2)).decode(anyList());

    CompletableFuture<Void> cancelFuture =
        scheduler.cancel(generationId1, Optional.of(attemptId1), new Exception("error"));

    finishDecoding.release(2);
    decodingFuture1.get(5, TimeUnit.SECONDS);
    decodingFuture3.get(5, TimeUnit.SECONDS);

    Assert.assertTrue(decodingFuture2.isCompletedExceptionally());
    Assert.assertTrue(cancelFuture.isDone());

    verify(decoder, times(2)).decode(anyList());
  }

  @Test
  public void testMetricSanity() throws Exception {
    MeterRegistry meterRegistry = new SimpleMeterRegistry();

    DecodingWorkScheduler scheduler =
        new DecodingWorkScheduler(
            2,
            CommonReplicationConfig.Type.DEFAULT,
            Executors.fixedSizeThreadPool("decoding", 2, meterRegistry),
            new MetricsFactory("decodingWorkScheduler", meterRegistry));

    List<RawBsonDocument> documents = IntStream.range(0, 5)
        .mapToObj(i -> BsonUtils.documentToRaw(new BsonDocument()))
        .toList();

    GenerationId genId = new GenerationId(new ObjectId(), Generation.CURRENT);
    ObjectId attemptId = new ObjectId();

    scheduler.schedule(
        genId,
        Optional.of(attemptId),
        documents,
        SchedulerQueue.Priority.STEADY_STATE_CHANGE_STREAM,
        events -> {},
        IGNORE_METRICS);

    CompletableFuture<Void> decodingFuture1 =
        scheduler.schedule(
            new GenerationId(new ObjectId(), Generation.CURRENT),
            Optional.of(new ObjectId()),
            documents,
            SchedulerQueue.Priority.STEADY_STATE_CHANGE_STREAM,
            events -> {},
            IGNORE_METRICS);

    CompletableFuture<Void> decodingFuture2 =
        scheduler.schedule(
            new GenerationId(new ObjectId(), Generation.CURRENT),
            Optional.of(new ObjectId()),
            documents,
            SchedulerQueue.Priority.STEADY_STATE_CHANGE_STREAM,
            events -> {},
            IGNORE_METRICS);

    // 3 batches are scheduled and enqueued
    assertEquals(3.0,
        meterRegistry.find("decodingWorkScheduler" + ".queuedBatchesTotal").gauge().value(),
        NO_EPSILON);
    // 3 * 5 events are scheduled and enqueued
    assertEquals(15.0,
        meterRegistry.find("decodingWorkScheduler" + ".queuedEventsTotal").gauge().value(),
        NO_EPSILON);

    CompletableFuture<Void> cancelFuture =
        scheduler.cancel(genId, Optional.of(attemptId), new Exception("error"));
    cancelFuture.get(500, TimeUnit.MILLISECONDS);

    // the first batch is cancelled
    assertEquals(2.0,
        meterRegistry.find("decodingWorkScheduler" + ".queuedBatchesTotal").gauge().value(),
        NO_EPSILON);
    assertEquals(10.0,
        meterRegistry.find("decodingWorkScheduler" + ".queuedEventsTotal").gauge().value(),
        NO_EPSILON);

    scheduler.start();
    decodingFuture1.get(5, TimeUnit.SECONDS);
    decodingFuture2.get(5, TimeUnit.SECONDS);

    var defaultTag =
        Tags.of("timeUnit", Enums.convertNameTo(CaseFormat.LOWER_CAMEL, TimeUnit.SECONDS));

    Assert.assertEquals(
        3, meterRegistry.counter("decodingWorkScheduler" + ".enqueueCalls").count(), NO_EPSILON);
    Assert.assertEquals(
        2, meterRegistry.counter("decodingWorkScheduler" + ".dequeueCalls").count(), NO_EPSILON);
    Assert.assertEquals(
        2,
        meterRegistry
            .timer(
                "decodingWorkScheduler.decodingBatchDurations",
                Tags.of(ServerStatusDataExtractor.Scope.REPLICATION.getTag()).and(defaultTag))
            .count(),
        0.0);

    // all batches and events are executed.
    assertEquals(0.0,
        meterRegistry.find("decodingWorkScheduler" + ".queuedBatchesTotal").gauge().value(),
        NO_EPSILON);
    assertEquals(0.0,
        meterRegistry.find("decodingWorkScheduler" + ".queuedEventsTotal").gauge().value(),
        NO_EPSILON);
  }

  @Test
  public void create_autoEmbeddingType_tagsCounterUnderAutoEmbeddingSubsystem() {
    SimpleMeterRegistry registry = new SimpleMeterRegistry();
    DecodingWorkScheduler.create(2, CommonReplicationConfig.Type.AUTO_EMBEDDING, registry, true);

    assertThat(
            registry
                .find("executor.thread.allocatedBytes")
                .tag("subsystem", "autoembedding")
                .tag("name", "autoEmbedding.decoding")
                .functionCounter())
        .isNotNull();
    // Replication subsystem should NOT carry the auto-embedding pool.
    assertThat(
            registry
                .find("executor.thread.allocatedBytes")
                .tag("subsystem", "replication")
                .functionCounter())
        .isNull();
  }

  @Test
  public void create_lifecycleAttributionDisabled_doesNotRegisterResourceMetrics() {
    SimpleMeterRegistry registry = new SimpleMeterRegistry();
    DecodingWorkScheduler.create(2, CommonReplicationConfig.Type.DEFAULT, registry, false);

    assertThat(
            registry
                .find("executor.thread.allocatedBytes")
                .tag("subsystem", "replication")
                .functionCounter())
        .isNull();
  }

  @Test
  public void create_lifecycleAttributionEnabled_decodingCounterIncreasesUnderLoad()
      throws Exception {
    SimpleMeterRegistry registry = new SimpleMeterRegistry();
    DecodingWorkScheduler scheduler =
        DecodingWorkScheduler.create(2, CommonReplicationConfig.Type.DEFAULT, registry, true);

    FunctionCounter allocated =
        registry
            .find("executor.thread.allocatedBytes")
            .tag("subsystem", "replication")
            .tag("name", "decoding")
            .functionCounter();
    assertThat(allocated).isNotNull();

    // Warm a worker thread so getThreadAllocatedBytes has a non-zero baseline.
    scheduler
        .schedule(
            new GenerationId(new ObjectId(), Generation.CURRENT),
            Optional.of(new ObjectId()),
            List.of(),
            SchedulerQueue.Priority.STEADY_STATE_CHANGE_STREAM,
            events -> {},
            IGNORE_METRICS)
        .get(5, TimeUnit.SECONDS);
    double before = allocated.count();

    // Allocate ~1 MB on a decoding-pool thread.
    scheduler
        .schedule(
            new GenerationId(new ObjectId(), Generation.CURRENT),
            Optional.of(new ObjectId()),
            List.of(),
            SchedulerQueue.Priority.STEADY_STATE_CHANGE_STREAM,
            events -> {
              byte[] dummy = new byte[1024 * 1024];
              dummy[0] = 1;
            },
            IGNORE_METRICS)
        .get(5, TimeUnit.SECONDS);
    double after = allocated.count();

    // JVMs without per-thread allocation tracking report 0 for both reads; skip rather than flake.
    if (before != 0.0 || after != 0.0) {
      assertThat(after).isGreaterThan(before);
    }
  }

  private void warpWithRuntimeException(Callable<Void> callable) {
    try {
      callable.call();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
