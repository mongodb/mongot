package com.xgen.mongot.replication.mongodb.common;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.xgen.testing.mongot.replication.mongodb.ChangeStreamUtils;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.BsonTimestamp;
import org.junit.Test;

public class PartitionedChangeStreamProgressTrackerTest {
  private static final ChangeStreamPartition FIRST = new ChangeStreamPartition(0, 2);
  private static final ChangeStreamPartition SECOND = new ChangeStreamPartition(1, 2);

  @Test
  public void getLowWaterMark_pendingBatches_usesSlowestConfirmedPosition() {
    var tracker =
        new PartitionedChangeStreamProgressTracker(Map.of(FIRST, token(10), SECOND, token(20)));
    var first = tracker.registerBatch(FIRST, token(30));

    tracker.registerBatch(SECOND, token(40)).run();

    assertThat(tracker.getLowWaterMark()).isEqualTo(token(10));
    first.run();
    assertThat(tracker.getLowWaterMark()).isEqualTo(token(30));
  }

  @Test
  public void getLowWaterMark_sameTimestamp_comparesFullTokenIgnoringCase() {
    var earlier = token("8200000001000000000a");
    var later = token("8200000001000000000B");
    var tracker = new PartitionedChangeStreamProgressTracker(Map.of(FIRST, later, SECOND, earlier));

    assertThat(tracker.getLowWaterMark()).isEqualTo(earlier);
  }

  @Test
  public void getLowWaterMark_outOfOrderAcknowledgments_doesNotSkipGap() {
    var tracker =
        new PartitionedChangeStreamProgressTracker(Map.of(FIRST, token(10), SECOND, token(40)));
    var first = tracker.registerBatch(FIRST, token(20));
    var second = tracker.registerBatch(FIRST, token(30));

    second.run();

    assertThat(tracker.getLowWaterMark()).isEqualTo(token(10));
    first.run();
    assertThat(tracker.getLowWaterMark()).isEqualTo(token(30));
  }

  @Test
  public void getLowWaterMark_capturedToken_isDetachedFromLaterProgressAndCallerMutation() {
    var tracker =
        new PartitionedChangeStreamProgressTracker(Map.of(FIRST, token(10), SECOND, token(40)));
    var captured = tracker.getLowWaterMark();

    tracker.registerBatch(FIRST, token(20)).run();

    assertThat(captured).isEqualTo(token(10));
    assertThat(tracker.getLowWaterMark()).isEqualTo(token(20));
    tracker.getLowWaterMark().put("_data", token(99).get("_data"));
    assertThat(tracker.getLowWaterMark()).isEqualTo(token(20));
  }

  @Test
  public void getLowWaterMark_singlePartition_returnsConfirmedToken() {
    var only = new ChangeStreamPartition(0, 1);
    var tracker = new PartitionedChangeStreamProgressTracker(Map.of(only, token(10)));

    tracker.registerBatch(only, token(20)).run();

    assertThat(tracker.getLowWaterMark()).isEqualTo(token(20));
  }

  @Test
  public void getLowWaterMark_earlierReplayToken_doesNotAssumeMonotonicProgress() {
    var tracker =
        new PartitionedChangeStreamProgressTracker(Map.of(FIRST, token(20), SECOND, token(20)));

    tracker.registerBatch(FIRST, token(10)).run();

    assertThat(tracker.getLowWaterMark()).isEqualTo(token(10));
  }

  @Test
  public void getLowWaterMark_malformedConfirmedToken_rejectsComparison() {
    var tracker =
        new PartitionedChangeStreamProgressTracker(
            Map.of(FIRST, token(10), SECOND, token("not-hex")));

    assertThrows(IllegalArgumentException.class, tracker::getLowWaterMark);
  }

  @Test
  public void snapshot_unconfirmedBatches_retainsAllInitialPositions() {
    var tracker = tracker();

    tracker.registerBatch(FIRST, token("first-batch"));
    tracker.registerBatch(SECOND, token("second-batch"));

    assertThat(tracker.snapshot())
        .containsExactly(FIRST, token("first-start"), SECOND, token("second-start"));
  }

  @Test
  public void acknowledge_otherPartitionPending_advancesIndependently() {
    var tracker = tracker();
    var first = tracker.registerBatch(FIRST, token("first-batch"));
    tracker.registerBatch(SECOND, token("second-batch"));

    first.run();

    assertThat(tracker.snapshot())
        .containsExactly(FIRST, token("first-batch"), SECOND, token("second-start"));
  }

  @Test
  public void acknowledge_outOfOrder_advancesOnlyThroughContiguousPrefix() {
    var tracker = tracker();
    var first = tracker.registerBatch(FIRST, token("batch-1"));
    var second = tracker.registerBatch(FIRST, token("batch-2"));
    var third = tracker.registerBatch(FIRST, token("batch-3"));

    third.run();
    assertThat(tracker.snapshot().get(FIRST)).isEqualTo(token("first-start"));

    first.run();
    assertThat(tracker.snapshot().get(FIRST)).isEqualTo(token("batch-1"));

    second.run();
    assertThat(tracker.snapshot().get(FIRST)).isEqualTo(token("batch-3"));
  }

  @Test
  public void acknowledge_duplicateCallback_doesNotRegressOrSkipPendingBatch() {
    var tracker = tracker();
    var first = tracker.registerBatch(FIRST, token("batch-1"));
    first.run();
    tracker.registerBatch(FIRST, token("batch-2"));
    var third = tracker.registerBatch(FIRST, token("batch-3"));
    third.run();

    first.run();
    third.run();

    assertThat(tracker.snapshot().get(FIRST)).isEqualTo(token("batch-1"));
  }

  @Test
  public void acknowledge_sameTokenInTwoBatches_doesNotConflateTheirCompletions() {
    var tracker = tracker();
    var first = tracker.registerBatch(FIRST, token("shared"));
    var second = tracker.registerBatch(FIRST, token("shared"));
    var third = tracker.registerBatch(FIRST, token("later"));
    first.run();
    third.run();

    first.run();
    assertThat(tracker.snapshot().get(FIRST)).isEqualTo(token("shared"));

    second.run();
    assertThat(tracker.snapshot().get(FIRST)).isEqualTo(token("later"));
  }

  @Test
  public void acknowledge_failedIndexing_leavesGapDespiteLaterSuccess() {
    var tracker = tracker();
    var indexing = new CompletableFuture<Void>();
    var confirmation = indexing.thenRun(tracker.registerBatch(FIRST, token("failed")));
    var later = tracker.registerBatch(FIRST, token("later"));

    indexing.completeExceptionally(new IllegalStateException("indexing failed"));
    later.run();

    assertThrows(CompletionException.class, confirmation::join);
    assertThat(tracker.snapshot().get(FIRST)).isEqualTo(token("first-start"));
  }

  @Test
  public void acknowledge_cancelledIndexing_leavesGapDespiteLaterSuccess() {
    var tracker = tracker();
    var indexing = new CompletableFuture<Void>();
    var confirmation = indexing.thenRun(tracker.registerBatch(FIRST, token("cancelled")));
    var later = tracker.registerBatch(FIRST, token("later"));

    indexing.cancel(false);
    later.run();

    assertThrows(CompletionException.class, confirmation::join);
    assertThat(tracker.snapshot().get(FIRST)).isEqualTo(token("first-start"));
  }

  @Test
  public void acknowledge_emptyBatch_waitsForEarlierIndexing() {
    var tracker = tracker();
    var indexing = new CompletableFuture<Void>();
    var confirmation = indexing.thenRun(tracker.registerBatch(FIRST, token("documents")));
    CompletableFuture.completedFuture(null)
        .thenRun(tracker.registerBatch(FIRST, token("empty-post-batch-token")))
        .join();
    assertThat(tracker.snapshot().get(FIRST)).isEqualTo(token("first-start"));

    indexing.complete(null);
    confirmation.join();

    assertThat(tracker.snapshot().get(FIRST)).isEqualTo(token("empty-post-batch-token"));
  }

  @Test
  public void acknowledge_oldAttemptCallback_doesNotChangeNewAttempt() {
    var previous = tracker();
    var oldConfirmation = previous.registerBatch(FIRST, token("old-attempt"));
    var current = new PartitionedChangeStreamProgressTracker(previous.snapshot());
    var currentConfirmation = current.registerBatch(FIRST, token("new-attempt"));

    oldConfirmation.run();
    assertThat(current.snapshot().get(FIRST)).isEqualTo(token("first-start"));

    currentConfirmation.run();
    oldConfirmation.run();
    assertThat(current.snapshot().get(FIRST)).isEqualTo(token("new-attempt"));
  }

  @Test
  public void snapshot_laterConfirmation_doesNotChangeCapturedCommitPositions() {
    var tracker = tracker();
    var first = tracker.registerBatch(FIRST, token("first-batch"));
    first.run();
    var snapshot = tracker.snapshot();

    tracker.registerBatch(FIRST, token("later-batch")).run();

    assertThat(snapshot)
        .containsExactly(FIRST, token("first-batch"), SECOND, token("second-start"));
    assertThat(tracker.snapshot().get(FIRST)).isEqualTo(token("later-batch"));
    assertThrows(UnsupportedOperationException.class, snapshot::clear);
  }

  @Test
  public void snapshot_mutatedInputAndOutputTokens_doesNotChangeTrackedPositions() {
    var seed = token("seed");
    var initialPositions = new HashMap<>(Map.of(FIRST, seed, SECOND, seed));
    var tracker = new PartitionedChangeStreamProgressTracker(initialPositions);
    seed.put("_data", new BsonString("mutated-seed"));
    initialPositions.clear();
    assertThat(tracker.snapshot()).containsExactly(FIRST, token("seed"), SECOND, token("seed"));

    var batchToken = token("batch");
    var confirmation = tracker.registerBatch(FIRST, batchToken);
    batchToken.put("_data", new BsonString("mutated-batch"));
    confirmation.run();
    tracker.snapshot().get(FIRST).put("_data", new BsonString("mutated-snapshot"));

    assertThat(tracker.snapshot()).containsExactly(FIRST, token("batch"), SECOND, token("seed"));
  }

  @Test
  public void acknowledge_concurrentCallbacksAndSnapshot_preservesPrefix() throws Exception {
    var tracker = tracker();
    var first = tracker.registerBatch(FIRST, token("batch-1"));
    var second = tracker.registerBatch(FIRST, token("batch-2"));
    var third = tracker.registerBatch(FIRST, token("batch-3"));
    var start = new CountDownLatch(1);
    try (var executor = Executors.newFixedThreadPool(3)) {
      var secondCompletion =
          executor.submit(
              () -> {
                start.await();
                second.run();
                return null;
              });
      var thirdCompletion =
          executor.submit(
              () -> {
                start.await();
                third.run();
                return null;
              });
      var snapshot =
          executor.submit(
              () -> {
                start.await();
                return tracker.snapshot();
              });

      start.countDown();
      secondCompletion.get(10, TimeUnit.SECONDS);
      thirdCompletion.get(10, TimeUnit.SECONDS);
      assertThat(snapshot.get(10, TimeUnit.SECONDS).get(FIRST)).isEqualTo(token("first-start"));
      first.run();

      assertThat(tracker.snapshot().get(FIRST)).isEqualTo(token("batch-3"));
    }
  }

  @Test
  public void constructor_emptyOrIncompleteLayout_rejectsMissingPartitions() {
    assertThrows(
        IllegalArgumentException.class, () -> new PartitionedChangeStreamProgressTracker(Map.of()));
    assertThrows(
        IllegalArgumentException.class,
        () -> new PartitionedChangeStreamProgressTracker(Map.of(FIRST, token("seed"))));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new PartitionedChangeStreamProgressTracker(
                Map.of(FIRST, token("seed"), new ChangeStreamPartition(1, 3), token("seed"))));
  }

  @Test
  public void registerBatch_differentPartitionLayout_rejectsBatch() {
    var tracker = tracker();

    assertThrows(
        IllegalArgumentException.class,
        () -> tracker.registerBatch(new ChangeStreamPartition(0, 3), token("batch")));

    assertThat(tracker.snapshot())
        .containsExactly(FIRST, token("first-start"), SECOND, token("second-start"));
  }

  private static PartitionedChangeStreamProgressTracker tracker() {
    return new PartitionedChangeStreamProgressTracker(
        Map.of(FIRST, token("first-start"), SECOND, token("second-start")));
  }

  private static BsonDocument token(String position) {
    return new BsonDocument("_data", new BsonString(position));
  }

  private static BsonDocument token(int seconds) {
    return ChangeStreamUtils.resumeToken(new BsonTimestamp(seconds, 0));
  }
}
