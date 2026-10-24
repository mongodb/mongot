package com.xgen.mongot.replication.mongodb.common;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.mongodb.MongoException;
import com.xgen.mongot.util.mongodb.BatchMongoClient;
import com.xgen.mongot.util.mongodb.Errors;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.BsonTimestamp;
import org.bson.RawBsonDocument;
import org.bson.codecs.BsonDocumentCodec;
import org.junit.After;
import org.junit.Test;

public class PartitionedChangeStreamClientTest {
  private static final ChangeStreamPartition FIRST = new ChangeStreamPartition(0, 2);

  private final BatchMongoClient mongoClient = mock(BatchMongoClient.class);
  private final ExecutorService fetchExecutor = mock(ExecutorService.class);
  private final ExecutorService caller = Executors.newSingleThreadExecutor();
  private final ExecutorService closer = Executors.newSingleThreadExecutor();
  private final ExecutorService executor = Executors.newFixedThreadPool(2);
  private final BlockingQueue<Runnable> fetches = new LinkedBlockingQueue<>();

  @After
  public void tearDown() throws Exception {
    this.caller.shutdownNow();
    this.closer.shutdownNow();
    this.executor.shutdownNow();
    assertThat(this.caller.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
    assertThat(this.closer.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
    assertThat(this.executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
  }

  @Test
  public void constructor_noReaders_throws() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new PartitionedChangeStreamClient<>(
                List.of(), this.fetchExecutor, SteadyStateException::wrapIfThrows));
  }

  @Test
  public void getNext_stalledReader_doesNotBlockFasterReaderOrAdvanceCheckpoint() throws Exception {
    var fast = reader(batch(10, event(9)), batch(20, event(19)));
    var slow = reader(batch(2, event(2)));
    try (var client = client(List.of(fast, slow), this.fetches::add)) {
      var initial = this.caller.submit(client::getNext);
      var fastFetch = nextFetch();
      var slowFetch = nextFetch();
      try {
        fastFetch.run();
        var first = initial.get(10, TimeUnit.SECONDS);

        assertThat(first.getRawEvents()).containsExactly(event(9));
        assertThat(first.getPostBatchResumeToken()).isEqualTo(token(10));

        var subsequent = this.caller.submit(client::getNext);
        nextFetch().run();
        var second = subsequent.get(10, TimeUnit.SECONDS);

        assertThat(second.getRawEvents()).containsExactly(event(19));
        assertThat(second.getPostBatchResumeToken()).isEqualTo(token(20));
        verify(fast, times(2)).getNext();
        verify(slow, times(0)).getNext();
      } finally {
        // Complete the queued fetch before leaving the test.
        slowFetch.run();
      }
    }
  }

  @Test
  public void getNext_readyBatchAndStalledReader_refillsIdleReaderBeforeReturning()
      throws Exception {
    var first = reader(batch(10), batch(40));
    var second = reader(batch(20));
    var slow = reader(batch(30));
    try (var client = client(List.of(first, second, slow), this.fetches::add)) {
      var initial = this.caller.submit(client::getNext);
      var firstFetch = nextFetch();
      var secondFetch = nextFetch();
      var slowFetch = nextFetch();
      try {
        firstFetch.run();
        assertThat(initial.get(10, TimeUnit.SECONDS).getPostBatchResumeToken())
            .isEqualTo(token(10));
        secondFetch.run();

        var readyBatch = client.getNext();

        assertThat(readyBatch.getPostBatchResumeToken()).isEqualTo(token(20));
        // The first reader's next fetch is already submitted while the caller handles this batch.
        assertThat(this.fetches).hasSize(1);
        nextFetch().run();
        verify(first, times(2)).getNext();
        verify(second).getNext();
        verify(slow, never()).getNext();
      } finally {
        slowFetch.run();
      }
    }
  }

  @Test
  public void getNext_multipleReadyReaders_refillsWithoutReorderingOrOverwritingBatches()
      throws Exception {
    var first = reader(batch(10), batch(40));
    var second = reader(batch(20));
    var third = reader(batch(30));
    try (var client = client(List.of(first, second, third), Runnable::run)) {
      assertThat(client.getNext().getPostBatchResumeToken()).isEqualTo(token(10));

      assertThat(client.getNext().getPostBatchResumeToken()).isEqualTo(token(20));

      verify(first, times(2)).getNext();
      verify(second).getNext();
      verify(third).getNext();
      assertThat(client.getNext().getPostBatchResumeToken()).isEqualTo(token(30));
      assertThat(client.getNext().getPostBatchResumeToken()).isEqualTo(token(40));
      verify(first, times(2)).getNext();
    }
  }

  @Test
  public void getNext_completedBatches_tagsBatchWithOriginatingPartition() throws Exception {
    var first = reader(batch(20, event(19)));
    var second = reader(batch(10, event(9)));
    try (var client = client(first, second)) {
      var initial = client.getNext();
      var next = client.getNext();

      assertThat(initial.getPartition()).hasValue(new ChangeStreamPartition(0, 2));
      assertThat(next.getPartition()).hasValue(new ChangeStreamPartition(1, 2));
    }
  }

  @Test
  public void getNext_completedBatches_carriesReadersRealPostBatchToken() throws Exception {
    // Distinct reader positions: batches must carry real reader tokens for the checkpoint to move.
    try (var client = client(reader(batch(20, event(19))), reader(batch(10, event(9))))) {
      var initial = client.getNext();
      var next = client.getNext();

      assertThat(initial.getPostBatchResumeToken()).isEqualTo(token(20));
      assertThat(next.getPostBatchResumeToken()).isEqualTo(token(10));
    }
  }

  @Test
  public void getNext_emptyBatch_carriesReadersPostBatchToken() throws Exception {
    try (var client = client(reader(batch(20)), reader(batch(10)))) {
      var first = client.getNext();
      var second = client.getNext();

      // Empty batches still carry the reader's real token — progress must never move backward.
      assertThat(first.getRawEvents()).isEmpty();
      assertThat(first.getPostBatchResumeToken()).isEqualTo(token(20));
      assertThat(second.getRawEvents()).isEmpty();
      assertThat(second.getPostBatchResumeToken()).isEqualTo(token(10));
    }
  }

  @Test
  public void getNext_realThenEmptyBatch_emptyBatchCarriesItsOwnToken() throws Exception {
    // The empty batch must not fall back to an earlier token — confirmed positions would move
    // backward.
    try (var client =
        client(reader(batch(20, event(19)), batch(10)), reader(batch(30, event(29))))) {
      var batches = List.of(client.getNext(), client.getNext(), client.getNext());

      // The empty batch must carry its own token, not the starting token.
      var emptyBatch =
          batches.stream().filter(b -> b.getRawEvents().isEmpty()).findFirst().orElseThrow();
      assertThat(emptyBatch.getPostBatchResumeToken()).isEqualTo(token(10));
    }
  }

  @Test
  public void getNext_multipleReaders_fetchesInParallel() throws Exception {
    var started = new CountDownLatch(2);
    var first = reader(batch(10, event(1)));
    var second = reader(batch(10, event(2)));
    when(first.getNext()).thenAnswer(unused -> awaitBothReaders(started, batch(10, event(1))));
    when(second.getNext()).thenAnswer(unused -> awaitBothReaders(started, batch(10, event(2))));
    try (var client = client(List.of(first, second), this.executor)) {
      var result = client.getNext();

      assertThat(result.getRawEvents()).hasSize(1);
      assertThat(result.getRawEvents().getFirst()).isAnyOf(event(1), event(2));
    }
    verify(this.fetchExecutor, never()).shutdown();
    verify(this.fetchExecutor, times(0)).shutdownNow();
  }

  @Test
  public void getNext_failedReader_propagatesFailureWithoutReturningBufferedBatches()
      throws Exception {
    var first = reader(batch(10, event(1)));
    var failure =
        SteadyStateException.wrapMongoException(
            new MongoException(Errors.ADMISSION_QUEUE_OVERFLOW.code, "getMore rejected"));
    when(first.getNext()).thenThrow(failure);
    var second = reader(batch(20, event(19)));
    try (var client = client(first, second)) {
      assertThat(assertThrows(SteadyStateException.class, client::getNext))
          .isSameInstanceAs(failure);
      verify(first).getNext();
      verify(second).getNext();
    }
    verify(first).close();
    verify(second).close();
  }

  @Test
  public void getNext_executorRejectsPartialSubmission_propagatesRejection() throws Exception {
    var first = reader(batch(10, event(1)));
    var second = reader(batch(20, event(19)));
    var submitted = new AtomicInteger();
    var rejection = new RejectedExecutionException();
    try (var client =
        client(
            List.of(first, second),
            task -> {
              if (submitted.getAndIncrement() == 1) {
                throw rejection;
              }
              task.run();
            })) {
      assertThat(assertThrows(RejectedExecutionException.class, client::getNext))
          .isSameInstanceAs(rejection);
      verify(first).getNext();
      verify(second, never()).getNext();
    }
    verify(first).close();
    verify(second).close();
  }

  @Test
  public void getNext_refillRejected_propagatesRejection() throws Exception {
    var first = reader(batch(10));
    var second = reader(batch(20));
    var rejection = new RejectedExecutionException();
    try (var client = client(first, second)) {
      assertThat(client.getNext().getPostBatchResumeToken()).isEqualTo(token(10));
      doThrow(rejection).when(this.fetchExecutor).execute(any(Runnable.class));

      assertThat(assertThrows(RejectedExecutionException.class, client::getNext))
          .isSameInstanceAs(rejection);
      verify(first).getNext();
      verify(second).getNext();
    }
    verify(first).close();
    verify(second).close();
  }

  @Test
  public void close_completedBatch_discardsBufferedResult() throws Exception {
    var first = reader(batch(10));
    var second = reader(batch(20));
    var client = client(first, second);
    try {
      assertThat(client.getNext().getPartition()).hasValue(FIRST);

      client.close();

      assertThat(assertThrows(IllegalStateException.class, client::getNext))
          .hasMessageThat()
          .isEqualTo("Change stream client is closed");
      verify(first).getNext();
      verify(second).getNext();
    } finally {
      client.close();
    }
  }

  @Test
  public void getNext_shutdownDuringPartialSubmission_reportsClosedClient() throws Exception {
    var first = reader(batch(10));
    var second = reader(batch(20));
    var rejection = new RejectedExecutionException("Executor is shut down");
    var client = client(List.of(first, second), this.fetches::add);
    doAnswer(
            invocation -> {
              this.fetches.add(invocation.getArgument(0));
              return null;
            })
        .doAnswer(
            unused -> {
              this.closer.submit(client::close).get(10, TimeUnit.SECONDS);
              throw rejection;
            })
        .when(this.fetchExecutor)
        .execute(any(Runnable.class));
    try {
      var failure = assertThrows(IllegalStateException.class, client::getNext);
      nextFetch().run();

      assertThat(failure).hasMessageThat().isEqualTo("Change stream client is closed");
      assertThat(failure).hasCauseThat().isSameInstanceAs(rejection);
      verify(first, never()).getNext();
      verify(second, never()).getNext();
      verify(first).close();
      verify(second).close();
      verify(this.fetchExecutor, never()).shutdown();
    } finally {
      client.close();
    }
  }

  @Test
  public void close_allFetchesQueued_wakesCallerWithoutStartingReads() throws Exception {
    var first = reader(batch(10));
    var second = reader(batch(20));
    var client = client(List.of(first, second), this.fetches::add);
    try {
      var next =
          this.caller.submit(() -> assertThrows(IllegalStateException.class, client::getNext));
      var firstFetch = nextFetch();
      var secondFetch = nextFetch();

      client.close();
      firstFetch.run();
      secondFetch.run();

      assertThat(next.get(10, TimeUnit.SECONDS))
          .hasMessageThat()
          .isEqualTo("Change stream client is closed");
      verify(first, never()).getNext();
      verify(second, never()).getNext();
    } finally {
      client.close();
    }
    verify(first).close();
    verify(second).close();
  }

  @Test
  public void close_queuedFetch_closesReaderWithoutStartingFetch() throws Exception {
    var first = reader(batch(10));
    var second = reader(batch(20));
    var client = client(List.of(first, second), this.fetches::add);
    var next = this.caller.submit(client::getNext);
    nextFetch().run();
    var pending = nextFetch();
    next.get(10, TimeUnit.SECONDS);

    client.close();
    pending.run();

    verify(second, times(0)).getNext();
    verify(second).close();
    client.close();
    verify(first).close();
    assertThrows(IllegalStateException.class, client::getNext);
  }

  @Test
  public void close_getNextWaiting_wakesCallerAndClosesReadersWithoutWaiting() throws Exception {
    var started = new CountDownLatch(2);
    var releaseFetch = new CountDownLatch(1);
    var unblocked = new CountDownLatch(2);
    var readersClosed = new CountDownLatch(2);
    var first = blockedReader(started, releaseFetch, unblocked, readersClosed);
    var second = blockedReader(started, releaseFetch, unblocked, readersClosed);
    var client = client(List.of(first, second), this.executor);
    try {
      var next =
          this.caller.submit(
              () -> {
                var failure = assertThrows(IllegalStateException.class, client::getNext);
                assertThat(Thread.currentThread().isInterrupted()).isFalse();
                return failure;
              });
      assertThat(started.await(10, TimeUnit.SECONDS)).isTrue();

      var closed = this.closer.submit(client::close);
      closed.get(10, TimeUnit.SECONDS);

      assertThat(next.get(10, TimeUnit.SECONDS))
          .hasMessageThat()
          .isEqualTo("Change stream client is closed");
      assertThat(unblocked.getCount()).isEqualTo(2);
      verify(first).close();
      verify(second).close();
      assertThat(readersClosed.await(10, TimeUnit.SECONDS)).isTrue();
      verify(this.fetchExecutor, never()).shutdown();
      verify(this.fetchExecutor, times(0)).shutdownNow();
    } finally {
      releaseFetch.countDown();
      client.close();
      assertThat(readersClosed.await(10, TimeUnit.SECONDS)).isTrue();
      assertThat(unblocked.await(10, TimeUnit.SECONDS)).isTrue();
    }
    assertThat(unblocked.getCount()).isEqualTo(0);
  }

  @Test
  public void close_fetchRemainsBlocked_returnsAndClosesCursorImmediately() throws Exception {
    var started = new CountDownLatch(1);
    var releaseFetch = new CountDownLatch(1);
    var unblocked = new CountDownLatch(1);
    var readerClosed = new CountDownLatch(1);
    var reader = blockedReader(started, releaseFetch, unblocked, readerClosed);
    var client = client(List.of(reader), this.executor);
    try {
      var next =
          this.caller.submit(() -> assertThrows(IllegalStateException.class, client::getNext));
      assertThat(started.await(10, TimeUnit.SECONDS)).isTrue();

      var closing = this.closer.submit(client::close);
      closing.get(10, TimeUnit.SECONDS);

      assertThat(next.get(10, TimeUnit.SECONDS))
          .hasMessageThat()
          .isEqualTo("Change stream client is closed");
      verify(reader).close();
      assertThat(readerClosed.await(10, TimeUnit.SECONDS)).isTrue();
      verify(this.fetchExecutor, never()).shutdown();
      verify(this.fetchExecutor, times(0)).shutdownNow();
      client.close();

      releaseFetch.countDown();
      assertThat(readerClosed.await(10, TimeUnit.SECONDS)).isTrue();
      assertThat(unblocked.await(10, TimeUnit.SECONDS)).isTrue();
      assertThat(unblocked.getCount()).isEqualTo(0);
      verify(reader).close();
    } finally {
      releaseFetch.countDown();
      client.close();
      assertThat(readerClosed.await(10, TimeUnit.SECONDS)).isTrue();
      assertThat(unblocked.await(10, TimeUnit.SECONDS)).isTrue();
    }
  }

  @Test
  public void close_oneReaderStillFetching_closesAllReaders() throws Exception {
    var started = new CountDownLatch(1);
    var releaseFetch = new CountDownLatch(1);
    var unblocked = new CountDownLatch(1);
    var readerClosed = new CountDownLatch(1);
    var slow = blockedReader(started, releaseFetch, unblocked, readerClosed);
    var fast = reader(batch(10));
    var client = client(List.of(slow, fast), this.executor);
    try {
      var next = this.caller.submit(client::getNext);
      assertThat(started.await(10, TimeUnit.SECONDS)).isTrue();
      next.get(10, TimeUnit.SECONDS);

      var closing = this.closer.submit(client::close);
      closing.get(10, TimeUnit.SECONDS);

      verify(fast).close();
      verify(slow).close();
      assertThat(readerClosed.await(10, TimeUnit.SECONDS)).isTrue();
    } finally {
      releaseFetch.countDown();
      client.close();
      assertThat(readerClosed.await(10, TimeUnit.SECONDS)).isTrue();
      assertThat(unblocked.await(10, TimeUnit.SECONDS)).isTrue();
    }
    assertThat(unblocked.getCount()).isEqualTo(0);
  }

  @Test
  public void close_readerThrows_stillClosesOtherReaders() throws Exception {
    var first = reader(batch(10));
    var second = reader(batch(20));
    var failure = new RuntimeException("close failed");
    doThrow(failure).when(first).close();
    var client = client(first, second);

    client.close();

    verify(second).close();
    client.close();
    verify(first).close();
  }

  @Test
  public void close_sharedMongoClientAndExecutor_leavesOtherClientUsable() throws Exception {
    var first = reader(batch(10));
    var second = reader(batch(20, event(19)));
    var firstClient =
        PartitionedChangeStreamClient.resume(
            1,
            token(0),
            this.mongoClient,
            (mongoClient, partition, start) -> first,
            this.executor,
            SteadyStateException::wrapIfThrows);
    try (var secondClient =
        PartitionedChangeStreamClient.resume(
            1,
            token(0),
            this.mongoClient,
            (mongoClient, partition, start) -> second,
            this.executor,
            SteadyStateException::wrapIfThrows)) {
      this.caller.submit(firstClient::getNext).get(10, TimeUnit.SECONDS);
      firstClient.close();
      var result = this.caller.submit(secondClient::getNext).get(10, TimeUnit.SECONDS);

      assertThat(result.getRawEvents()).containsExactly(event(19));
      verify(first).close();
      verify(second, never()).close();
      verify(this.mongoClient, never()).close();
      assertThat(this.executor.isShutdown()).isFalse();
    } finally {
      firstClient.close();
    }
    verify(second).close();
    verify(this.mongoClient, never()).close();
  }

  @Test
  public void getNext_poolSaturated_queuesReaderUntilWorkerAvailable() throws Exception {
    var started = new CountDownLatch(2);
    var releaseFetch = new CountDownLatch(1);
    var unblocked = new CountDownLatch(2);
    var readersClosed = new CountDownLatch(2);
    var first = blockedReader(started, releaseFetch, unblocked, readersClosed);
    var second = blockedReader(started, releaseFetch, unblocked, readersClosed);
    var third = reader(batch(130, event(127)));
    var thirdStarted = new CountDownLatch(1);
    when(third.getNext())
        .thenAnswer(
            unused -> {
              thirdStarted.countDown();
              return batch(130, event(127));
            });
    var readers = List.of(first, second, third);
    var client =
        PartitionedChangeStreamClient.resume(
            3,
            token(128),
            this.mongoClient,
            (mongoClient, partition, start) -> readers.get(partition.partitionId()),
            this.executor,
            SteadyStateException::wrapIfThrows);
    try {
      var next = this.caller.submit(client::getNext);
      assertThat(started.await(10, TimeUnit.SECONDS)).isTrue();
      verify(third, never()).getNext();

      releaseFetch.countDown();

      next.get(10, TimeUnit.SECONDS);
      assertThat(thirdStarted.await(10, TimeUnit.SECONDS)).isTrue();
      verify(third).getNext();
    } finally {
      client.close();
      releaseFetch.countDown();
      assertThat(readersClosed.await(10, TimeUnit.SECONDS)).isTrue();
      assertThat(unblocked.await(10, TimeUnit.SECONDS)).isTrue();
    }
    assertThat(unblocked.getCount()).isEqualTo(0);
  }

  @Test
  public void resume_eventToken_suppliesEveryPartitionWithSameTimestamp() throws Exception {
    var partitions = new ArrayList<ChangeStreamPartition>();
    var timestamps = new ArrayList<BsonTimestamp>();
    var mongoClients = new ArrayList<BatchMongoClient>();
    var readers = List.of(reader(batch(128, event(127))), reader(batch(128, event(128))));
    try (var client =
        PartitionedChangeStreamClient.resume(
            2,
            token(128),
            this.mongoClient,
            (mongoClient, partition, start) -> {
              mongoClients.add(mongoClient);
              partitions.add(partition);
              timestamps.add(start);
              return readers.get(partition.partitionId());
            },
            this.executor,
            SteadyStateException::wrapIfThrows)) {
      assertThat(partitions)
          .containsExactly(new ChangeStreamPartition(0, 2), new ChangeStreamPartition(1, 2))
          .inOrder();
      assertThat(timestamps).containsExactly(new BsonTimestamp(1, 1), new BsonTimestamp(1, 1));
      assertThat(mongoClients).containsExactly(this.mongoClient, this.mongoClient);
    }
    verify(this.mongoClient, never()).close();
  }

  @Test
  public void resume_readerCreationFails_closesOnlyPreviouslyCreatedReaders() throws Exception {
    var first = reader(batch(10));
    var failure = SteadyStateException.createTransient(new RuntimeException("open failed"));
    var created = new AtomicInteger();

    var thrown =
        assertThrows(
            SteadyStateException.class,
            () ->
                PartitionedChangeStreamClient.resume(
                    2,
                    token(0),
                    this.mongoClient,
                    (mongoClient, partition, start) -> {
                      if (created.getAndIncrement() == 0) {
                        return first;
                      }
                      throw failure;
                    },
                    this.executor,
                    SteadyStateException::wrapIfThrows));

    assertThat(thrown).isSameInstanceAs(failure);
    verify(first).close();
    verify(this.mongoClient, never()).close();
  }

  @Test
  public void resume_invalidPartitionCount_leavesSharedMongoClientOpen() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            PartitionedChangeStreamClient.resume(
                0,
                token(0),
                this.mongoClient,
                (mongoClient, partition, start) -> {
                  throw new AssertionError("Reader creation must not start");
                },
                this.executor,
                SteadyStateException::wrapIfThrows));

    verify(this.mongoClient, never()).close();
  }

  @Test
  public void getNext_lifecycleBatch_stopsReaderWhileOtherReadersCatchUp() throws Exception {
    var boundary = batch(50, event(49, "rename"), event(50, "invalidate"));
    // Keep the document preceding the lifecycle event in this reader's retained batch too.
    var first = reader(batch(50, event(10), event(49, "rename"), event(50, "invalidate")));
    var second = reader(batch(20, event(19)), boundary);
    var third = reader(batch(30, event(29)), batch(40), boundary);
    try (var client = client(List.of(first, second, third), Runnable::run)) {
      var secondBatch = client.getNext();
      var thirdBatch = client.getNext();
      var emptyBatch = client.getNext();

      assertThat(secondBatch.getRawEvents()).containsExactly(event(19));
      assertThat(secondBatch.getPostBatchResumeToken()).isEqualTo(token(20));
      assertThat(secondBatch.getPartition()).hasValue(new ChangeStreamPartition(1, 3));
      assertThat(thirdBatch.getRawEvents()).containsExactly(event(29));
      assertThat(thirdBatch.getPartition()).hasValue(new ChangeStreamPartition(2, 3));
      assertThat(emptyBatch.getRawEvents()).isEmpty();
      assertThat(emptyBatch.getPostBatchResumeToken()).isEqualTo(token(40));
      verify(first).getNext();
      verify(second, times(2)).getNext();
      verify(third, times(2)).getNext();

      var retainedDocuments = client.getNext();
      var lifecycle = client.getNext();

      assertThat(retainedDocuments.getRawEvents()).containsExactly(event(10));
      assertThat(retainedDocuments.getPostBatchResumeToken()).isEqualTo(token(10));
      assertThat(retainedDocuments.getPartition()).hasValue(new ChangeStreamPartition(0, 3));
      assertThat(lifecycle.getRawEvents())
          .containsExactly(event(49, "rename"), event(50, "invalidate"))
          .inOrder();
      assertThat(lifecycle.getPostBatchResumeToken()).isEqualTo(token(50));
      assertThat(lifecycle.getPartition()).hasValue(new ChangeStreamPartition(2, 3));
      assertThat(assertThrows(IllegalStateException.class, client::getNext))
          .hasMessageThat()
          .isEqualTo("All lifecycle batches have been returned");
      assertThrows(IllegalStateException.class, client::getNext);
      verify(first).getNext();
      verify(second, times(2)).getNext();
      verify(third, times(3)).getNext();
    }
  }

  @Test
  public void getNext_finalBatches_returnsEveryDocumentBeforeSingleLifecycleTail()
      throws Exception {
    var first =
        reader(
            batch(
                50,
                event(10),
                event(11, "update"),
                event(12, "replace"),
                event(13, "delete"),
                event(49, "drop"),
                event(50, "invalidate")));
    var second = reader(batch(49, event(20), event(21), event(49, "drop")));
    var third = reader(batch(50, event(30), event(49, "drop"), event(50, "invalidate")));
    try (var client = client(List.of(first, second, third), Runnable::run)) {
      var firstBatch = client.getNext();
      var secondBatch = client.getNext();
      var finalBatch = client.getNext();

      assertThat(firstBatch.getRawEvents())
          .containsExactly(
              event(10), event(11, "update"), event(12, "replace"), event(13, "delete"))
          .inOrder();
      assertThat(firstBatch.getPostBatchResumeToken()).isEqualTo(token(13));
      assertThat(firstBatch.getPartition()).hasValue(new ChangeStreamPartition(0, 3));
      assertThat(firstBatch.getCommandOperationTime()).hasValue(new BsonTimestamp(1, 1));
      assertThat(secondBatch.getRawEvents()).containsExactly(event(20), event(21)).inOrder();
      assertThat(secondBatch.getPostBatchResumeToken()).isEqualTo(token(21));
      assertThat(secondBatch.getPartition()).hasValue(new ChangeStreamPartition(1, 3));
      assertThat(finalBatch.getRawEvents())
          .containsExactly(event(30), event(49, "drop"), event(50, "invalidate"))
          .inOrder();
      assertThat(finalBatch.getPostBatchResumeToken()).isEqualTo(token(50));
      assertThat(finalBatch.getPartition()).hasValue(new ChangeStreamPartition(2, 3));
      assertThrows(IllegalStateException.class, client::getNext);
      verify(first).getNext();
      verify(second).getNext();
      verify(third).getNext();
    }
  }

  @Test
  public void getNext_longLifecycleTail_removesEveryDuplicateLifecycleEvent() throws Exception {
    var first =
        reader(
            batch(
                50,
                event(10),
                event(48, "drop"),
                event(49, "dropDatabase"),
                event(50, "invalidate")));
    var second =
        reader(batch(50, event(48, "drop"), event(49, "dropDatabase"), event(50, "invalidate")));
    var third =
        reader(
            batch(
                50,
                event(30),
                event(48, "drop"),
                event(49, "dropDatabase"),
                event(50, "invalidate")));
    try (var client = client(List.of(first, second, third), Runnable::run)) {
      var documents = client.getNext();
      var lifecycle = client.getNext();

      assertThat(documents.getRawEvents()).containsExactly(event(10));
      assertThat(documents.getPostBatchResumeToken()).isEqualTo(token(10));
      assertThat(documents.getPartition()).hasValue(new ChangeStreamPartition(0, 3));
      assertThat(lifecycle.getRawEvents())
          .containsExactly(
              event(30), event(48, "drop"), event(49, "dropDatabase"), event(50, "invalidate"))
          .inOrder();
      assertThat(lifecycle.getPartition()).hasValue(new ChangeStreamPartition(2, 3));
      assertThrows(IllegalStateException.class, client::getNext);
      verify(first).getNext();
      verify(second).getNext();
      verify(third).getNext();
    }
  }

  @Test
  public void getNext_slowReader_holdsFinalBatchesUntilEveryReaderFinishes() throws Exception {
    var first = reader(batch(50, event(10), event(50, "drop")));
    var second = reader(batch(50, event(20), event(50, "drop")));
    var third = reader(batch(50, event(30), event(50, "drop")));
    try (var client = client(List.of(first, second, third), this.fetches::add)) {
      var waiting = this.caller.submit(client::getNext);
      nextFetch().run();
      nextFetch().run();
      var slowFetch = nextFetch();

      assertThat(waiting.isDone()).isFalse();
      slowFetch.run();
      assertThat(waiting.get(10, TimeUnit.SECONDS).getRawEvents()).containsExactly(event(10));
      assertThat(client.getNext().getRawEvents()).containsExactly(event(20));
      assertThat(client.getNext().getRawEvents())
          .containsExactly(event(30), event(50, "drop"))
          .inOrder();
      assertThat(this.fetches).isEmpty();
      verify(first).getNext();
      verify(second).getNext();
      verify(third).getNext();
    }
  }

  @Test
  public void getNext_splitRename_fetchesInvalidateForEveryReader() throws Exception {
    var first = reader(batch(49, event(49, "rename")), batch(50, event(50, "invalidate")));
    var second =
        reader(batch(49, event(20), event(49, "rename")), batch(50, event(50, "invalidate")));
    try (var client = client(first, second)) {
      var lifecycle = client.getNext();

      assertThat(lifecycle.getRawEvents())
          .containsExactly(event(20), event(49, "rename"), event(50, "invalidate"))
          .inOrder();
      assertThat(lifecycle.getPostBatchResumeToken()).isEqualTo(token(50));
      assertThat(lifecycle.getPartition()).hasValue(new ChangeStreamPartition(1, 2));
      assertThrows(IllegalStateException.class, client::getNext);
      verify(first, times(2)).getNext();
      verify(second, times(2)).getNext();
    }
  }

  @Test
  public void getNext_splitRenameWithEmptyBatch_keepsOtherReaderStopped() throws Exception {
    var first = reader(batch(51, event(10), event(49, "rename"), event(51, "invalidate")));
    var second =
        reader(
            batch(49, event(20), event(49, "rename")),
            batch(50),
            new ChangeStreamBatch(
                List.of(event(51, "invalidate")), token(51), Optional.of(new BsonTimestamp(2, 1))));
    try (var client = client(first, second)) {
      var documents = client.getNext();
      var lifecycle = client.getNext();

      assertThat(documents.getRawEvents()).containsExactly(event(10));
      assertThat(documents.getPostBatchResumeToken()).isEqualTo(token(10));
      assertThat(lifecycle.getRawEvents())
          .containsExactly(event(20), event(49, "rename"), event(51, "invalidate"))
          .inOrder();
      assertThat(lifecycle.getPostBatchResumeToken()).isEqualTo(token(51));
      assertThat(lifecycle.getCommandOperationTime()).hasValue(new BsonTimestamp(2, 1));
      assertThat(lifecycle.getPartition()).hasValue(new ChangeStreamPartition(1, 2));
      assertThrows(IllegalStateException.class, client::getNext);
      verify(first).getNext();
      verify(second, times(3)).getNext();
    }
  }

  @Test
  public void getNext_splitRenameOnFirstReader_fetchesInvalidateBeforeStopping() throws Exception {
    var first =
        reader(batch(49, event(10), event(49, "rename")), batch(50, event(50, "invalidate")));
    var second = reader(batch(50, event(20), event(49, "rename"), event(50, "invalidate")));
    try (var client = client(first, second)) {
      var documents = client.getNext();
      var lifecycle = client.getNext();

      assertThat(documents.getRawEvents()).containsExactly(event(10));
      assertThat(documents.getPostBatchResumeToken()).isEqualTo(token(10));
      assertThat(documents.getPartition()).hasValue(FIRST);
      assertThat(lifecycle.getRawEvents())
          .containsExactly(event(20), event(49, "rename"), event(50, "invalidate"))
          .inOrder();
      assertThat(lifecycle.getPostBatchResumeToken()).isEqualTo(token(50));
      assertThat(lifecycle.getPartition()).hasValue(new ChangeStreamPartition(1, 2));
      assertThrows(IllegalStateException.class, client::getNext);
      verify(first, times(2)).getNext();
      verify(second).getNext();
    }
  }

  @Test
  public void getNext_splitRenameFetchFails_propagatesFailureWithoutReturningPartialBatch()
      throws Exception {
    var first = reader(batch(50, event(10), event(49, "rename"), event(50, "invalidate")));
    var second = reader(batch(49, event(20), event(49, "rename")));
    var failure =
        SteadyStateException.wrapMongoException(
            new MongoException(Errors.ADMISSION_QUEUE_OVERFLOW.code, "getMore rejected"));
    when(second.getNext())
        .thenReturn(batch(49, event(20), event(49, "rename")))
        .thenThrow(failure)
        .thenReturn(batch(50, event(50, "invalidate")));
    try (var client = client(first, second)) {
      assertThat(assertThrows(SteadyStateException.class, client::getNext))
          .isSameInstanceAs(failure);

      verify(first).getNext();
      verify(second, times(2)).getNext();
    }
  }

  @Test
  public void getNext_splitRenameEmptyResponseThenFailure_propagatesFailure() throws Exception {
    var first = reader(batch(49, event(10), event(49, "rename")));
    var failure =
        SteadyStateException.wrapMongoException(
            new MongoException(Errors.ADMISSION_QUEUE_OVERFLOW.code, "getMore rejected"));
    when(first.getNext())
        .thenReturn(batch(49, event(10), event(49, "rename")))
        .thenReturn(batch(49))
        .thenThrow(failure)
        .thenReturn(batch(50, event(50, "invalidate")));
    try (var client = client(List.of(first), Runnable::run)) {
      assertThat(assertThrows(SteadyStateException.class, client::getNext))
          .isSameInstanceAs(failure);
      verify(first, times(3)).getNext();
    }
  }

  @Test
  public void getNext_splitRenameUnexpectedEvent_returnsCombinedBatch() throws Exception {
    var finalReader = reader(batch(49, event(49, "rename")), batch(50, event(50)));
    try (var client = client(List.of(finalReader), Runnable::run)) {
      var result = client.getNext();

      assertThat(result.getRawEvents()).containsExactly(event(49, "rename"), event(50)).inOrder();
      assertThat(result.getPostBatchResumeToken()).isEqualTo(token(50));
      assertThat(result.getPartition()).hasValue(new ChangeStreamPartition(0, 1));
      assertThrows(IllegalStateException.class, client::getNext);
      verify(finalReader, times(2)).getNext();
    }
  }

  @Test
  public void getNext_splitRenameUnexpectedEvent_waitsForAllReaders() throws Exception {
    var first =
        reader(
            batch(49, event(10), event(11, "update"), event(49, "rename")), batch(50, event(50)));
    var second = reader(batch(49, event(20), event(49, "rename")), batch(50, event(50)));
    try (var client = client(List.of(first, second), this.fetches::add)) {
      var waiting = this.caller.submit(client::getNext);
      nextFetch().run();
      var slowFetch = nextFetch();

      assertThat(waiting.isDone()).isFalse();
      slowFetch.run();
      var documents = waiting.get(10, TimeUnit.SECONDS);
      assertThat(documents.getRawEvents())
          .containsExactly(event(10), event(11, "update"))
          .inOrder();
      assertThat(documents.getPostBatchResumeToken()).isEqualTo(token(11));
      assertThat(client.getNext().getRawEvents())
          .containsExactly(event(20), event(49, "rename"), event(50))
          .inOrder();
      assertThrows(IllegalStateException.class, client::getNext);
      verify(first, times(2)).getNext();
      verify(second, times(2)).getNext();
    }
  }

  @Test
  public void getNext_splitRename_fetchesInvalidateWhileOtherReadersCatchUp() throws Exception {
    var first =
        reader(batch(49, event(10), event(49, "rename")), batch(50, event(50, "invalidate")));
    var last =
        reader(
            batch(20, event(20)),
            batch(49, event(49, "rename")),
            batch(50, event(50, "invalidate")));
    try (var client = client(List.of(first, last), this.fetches::add)) {
      var initial = this.caller.submit(client::getNext);
      nextFetch().run();
      nextFetch().run();
      assertThat(initial.get(10, TimeUnit.SECONDS).getRawEvents()).containsExactly(event(20));
      verify(first, times(2)).getNext();
      verify(last).getNext();

      var documents = this.caller.submit(client::getNext);
      nextFetch().run();
      assertThat(documents.get(10, TimeUnit.SECONDS).getRawEvents()).containsExactly(event(10));
      assertThat(client.getNext().getRawEvents())
          .containsExactly(event(49, "rename"), event(50, "invalidate"))
          .inOrder();
      assertThat(this.fetches).isEmpty();
      verify(first, times(2)).getNext();
      verify(last, times(3)).getNext();
    }
  }

  @Test
  public void getNext_waitingForInvalidate_returnsOtherReadersDocuments() throws Exception {
    var first = reader(batch(49, event(10), event(49, "rename")));
    var second =
        reader(batch(20, event(20)), batch(50, event(49, "rename"), event(50, "invalidate")));
    var fetchingInvalidate = new CountDownLatch(1);
    var releaseInvalidate = new CountDownLatch(1);
    when(first.getNext())
        .thenReturn(batch(49, event(10), event(49, "rename")))
        .thenAnswer(
            unused -> {
              fetchingInvalidate.countDown();
              assertThat(releaseInvalidate.await(10, TimeUnit.SECONDS)).isTrue();
              return batch(50, event(50, "invalidate"));
            });
    try (var client = client(List.of(first, second), this.executor)) {
      try {
        var normal = this.caller.submit(client::getNext);
        assertThat(fetchingInvalidate.await(10, TimeUnit.SECONDS)).isTrue();

        assertThat(normal.get(10, TimeUnit.SECONDS).getRawEvents()).containsExactly(event(20));
        var lifecycle = this.caller.submit(client::getNext);
        assertThat(lifecycle.isDone()).isFalse();
        releaseInvalidate.countDown();
        assertThat(lifecycle.get(10, TimeUnit.SECONDS).getRawEvents()).containsExactly(event(10));
        assertThat(client.getNext().getRawEvents())
            .containsExactly(event(49, "rename"), event(50, "invalidate"))
            .inOrder();
        verify(first, times(2)).getNext();
        verify(second, times(2)).getNext();
      } finally {
        releaseInvalidate.countDown();
      }
    }
  }

  @Test
  public void getNext_drop_stopsBeforeFetchingInvalidate() throws Exception {
    var first = reader(batch(10, event(10, "drop")), batch(11, event(11, "invalidate")));
    try (var client = client(List.of(first), Runnable::run)) {
      var result = client.getNext();

      assertThat(result.getRawEvents()).containsExactly(event(10, "drop"));
      assertThat(result.getPostBatchResumeToken()).isEqualTo(token(10));
      assertThat(result.getPartition()).hasValue(new ChangeStreamPartition(0, 1));
      assertThrows(IllegalStateException.class, client::getNext);

      verify(first).getNext();
    }
  }

  @Test
  public void getNext_dropDatabase_stopsReader() throws Exception {
    var first = reader(batch(10, event(10, "dropDatabase")));
    try (var client = client(List.of(first), Runnable::run)) {
      assertThat(client.getNext().getRawEvents()).containsExactly(event(10, "dropDatabase"));
      assertThrows(IllegalStateException.class, client::getNext);

      verify(first).getNext();
    }
  }

  @Test
  public void getNext_standaloneInvalidate_stopsReader() throws Exception {
    var first = reader(batch(10, event(10, "invalidate")));
    try (var client = client(List.of(first), Runnable::run)) {
      assertThat(client.getNext().getRawEvents()).containsExactly(event(10, "invalidate"));
      assertThrows(IllegalStateException.class, client::getNext);

      verify(first).getNext();
    }
  }

  @Test
  public void getNext_otherReaderFails_propagatesFailure() throws Exception {
    var first = reader(batch(50, event(49, "rename"), event(50, "invalidate")));
    var second = reader(batch(20, event(19)));
    var failure = SteadyStateException.createTransient(new RuntimeException("getMore rejected"));
    when(second.getNext()).thenThrow(failure);
    try (var client = client(first, second)) {
      assertThat(assertThrows(SteadyStateException.class, client::getNext))
          .isSameInstanceAs(failure);
      verify(first).getNext();
      verify(second).getNext();
    }
  }

  @Test
  public void close_lifecycleReaderAndStalledReader_wakesCaller() throws Exception {
    var first = reader(batch(50, event(49, "rename"), event(50, "invalidate")));
    var second = reader(batch(20), batch(30));
    try (var client = client(List.of(first, second), this.fetches::add)) {
      var initial = this.caller.submit(client::getNext);
      nextFetch().run();
      nextFetch().run();
      assertThat(initial.get(10, TimeUnit.SECONDS).getPostBatchResumeToken()).isEqualTo(token(20));
      var waiting =
          this.caller.submit(() -> assertThrows(IllegalStateException.class, client::getNext));
      var stalledFetch = nextFetch();

      client.close();

      assertThat(waiting.get(10, TimeUnit.SECONDS))
          .hasMessageThat()
          .isEqualTo("Change stream client is closed");
      stalledFetch.run();
      verify(first).getNext();
      verify(second).getNext();
      assertThat(this.fetches).isEmpty();
    }
  }

  @Test
  public void close_waitingForInvalidate_wakesCaller() throws Exception {
    var first = reader(batch(50, event(49, "rename"), event(50, "invalidate")));
    var second = reader(batch(49, event(49, "rename")));
    var fetchingInvalidate = new CountDownLatch(1);
    var readerClosed = new CountDownLatch(1);
    when(second.getNext())
        .thenReturn(batch(49, event(49, "rename")))
        .thenAnswer(
            unused -> {
              fetchingInvalidate.countDown();
              assertThat(readerClosed.await(10, TimeUnit.SECONDS)).isTrue();
              return batch(50);
            });
    doAnswer(
            unused -> {
              readerClosed.countDown();
              return null;
            })
        .when(second)
        .close();
    try (var client = client(List.of(first, second), this.executor)) {
      var waiting =
          this.caller.submit(() -> assertThrows(IllegalStateException.class, client::getNext));
      assertThat(fetchingInvalidate.await(10, TimeUnit.SECONDS)).isTrue();

      client.close();

      assertThat(waiting.get(10, TimeUnit.SECONDS))
          .hasMessageThat()
          .isEqualTo("Change stream client is closed");
      verify(first).close();
      verify(second, times(2)).getNext();
    }
  }

  private PartitionedChangeStreamClient<SteadyStateException> client(
      ChangeStreamMongoClient<SteadyStateException> first,
      ChangeStreamMongoClient<SteadyStateException> second)
      throws Exception {
    return client(List.of(first, second), Runnable::run);
  }

  private PartitionedChangeStreamClient<SteadyStateException> client(
      List<ChangeStreamMongoClient<SteadyStateException>> readers, Executor executor)
      throws Exception {
    doAnswer(
            invocation -> {
              executor.execute(invocation.getArgument(0));
              return null;
            })
        .when(this.fetchExecutor)
        .execute(any(Runnable.class));
    return new PartitionedChangeStreamClient<>(
        readers, this.fetchExecutor, SteadyStateException::wrapIfThrows);
  }

  private Runnable nextFetch() throws Exception {
    Runnable task = this.fetches.poll(10, TimeUnit.SECONDS);
    assertThat(task).isNotNull();
    return task;
  }

  @SuppressWarnings("unchecked")
  private static ChangeStreamMongoClient<SteadyStateException> reader(
      ChangeStreamBatch first, ChangeStreamBatch... rest) throws SteadyStateException {
    ChangeStreamMongoClient<SteadyStateException> client = mock(ChangeStreamMongoClient.class);
    when(client.getNext()).thenReturn(first, rest);
    return client;
  }

  private static ChangeStreamBatch awaitBothReaders(CountDownLatch started, ChangeStreamBatch batch)
      throws Exception {
    started.countDown();
    assertThat(started.await(10, TimeUnit.SECONDS)).isTrue();
    return batch;
  }

  /** Holds a read until released and verifies that shutdown does not interrupt its worker. */
  private static ChangeStreamMongoClient<SteadyStateException> blockedReader(
      CountDownLatch started,
      CountDownLatch releaseFetch,
      CountDownLatch unblocked,
      CountDownLatch readerClosed)
      throws SteadyStateException {
    var reader = reader(batch(10));
    when(reader.getNext())
        .thenAnswer(
            unused -> {
              started.countDown();
              assertThat(releaseFetch.await(10, TimeUnit.SECONDS)).isTrue();
              assertThat(Thread.currentThread().isInterrupted()).isFalse();
              unblocked.countDown();
              return batch(10);
            });
    doAnswer(
            unused -> {
              readerClosed.countDown();
              return null;
            })
        .when(reader)
        .close();
    return reader;
  }

  /**
   * Creates a synthetic resume token with a known timestamp and a distinguishable suffix. The
   * prefix consists of the timestamp marker (82), seconds (00000001), and increment (00000001), so
   * every token decodes to BsonTimestamp(1, 1). The position is appended as hexadecimal only to
   * make tokens distinct; it does not represent a real event number or change stream position.
   *
   * <p>These values support timestamp extraction and token equality checks. They are not complete
   * MongoDB-generated resume tokens and must not be used to test server resumability.
   */
  private static BsonDocument token(int position) {
    return new BsonDocument("_data", new BsonString("820000000100000001%04X".formatted(position)));
  }

  private static RawBsonDocument event(int position) {
    return event(position, "insert");
  }

  private static RawBsonDocument event(int position, String operation) {
    return new RawBsonDocument(
        new BsonDocument("_id", token(position))
            .append("operationType", new BsonString(operation))
            .append("documentKey", new BsonDocument("_id", new BsonInt32(position))),
        new BsonDocumentCodec());
  }

  private static ChangeStreamBatch batch(int position, RawBsonDocument... events) {
    return new ChangeStreamBatch(
        List.of(events), token(position), Optional.of(new BsonTimestamp(1, 1)));
  }
}
