package com.xgen.mongot.replication.mongodb.initialsync;

import static com.google.common.truth.Truth.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.bson.types.ObjectId;
import org.junit.Test;

public class InitialSyncPauseStateTest {
  @Test
  public void isPaused_initialState_returnsFalse() {
    var state = new InitialSyncPauseState();

    assertThat(state.isPaused(new ObjectId())).isFalse();
  }

  @Test
  public void updatePausedIndexIds_replacesAndClearsState() {
    var state = new InitialSyncPauseState();
    var first = new ObjectId();
    var second = new ObjectId();

    state.updatePausedIndexIds(List.of(first, first));

    assertThat(state.isPaused(first)).isTrue();
    assertThat(state.isPaused(second)).isFalse();

    state.updatePausedIndexIds(List.of(second));

    assertThat(state.isPaused(first)).isFalse();
    assertThat(state.isPaused(second)).isTrue();

    state.updatePausedIndexIds(List.of());

    assertThat(state.isPaused(second)).isFalse();
  }

  @Test
  public void updatePausedIndexIds_copiesInput() {
    var state = new InitialSyncPauseState();
    var indexId = new ObjectId();
    var ids = new ArrayList<>(List.of(indexId));

    state.updatePausedIndexIds(ids);
    ids.clear();

    assertThat(state.isPaused(indexId)).isTrue();
  }

  @Test
  public void isPaused_whileUpdating_stableIdAlwaysPaused() throws Exception {
    var state = new InitialSyncPauseState();
    var stable = new ObjectId();
    var other = new ObjectId();
    state.updatePausedIndexIds(List.of(stable));
    var writerStarted = new CountDownLatch(1);
    var readerChecked = new CountDownLatch(1);
    var executor = Executors.newSingleThreadExecutor();
    try {
      var writer =
          executor.submit(
              () -> {
                state.updatePausedIndexIds(List.of(stable, other));
                writerStarted.countDown();
                if (!readerChecked.await(5, TimeUnit.SECONDS)) {
                  throw new AssertionError("Reader did not check the pause state");
                }
                for (int i = 0; i < 100_000; i++) {
                  state.updatePausedIndexIds(i % 2 == 0 ? List.of(stable, other) : List.of(stable));
                }
                return null;
              });

      assertThat(writerStarted.await(5, TimeUnit.SECONDS)).isTrue();
      assertThat(state.isPaused(stable)).isTrue();
      readerChecked.countDown();
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
      while (!writer.isDone() && System.nanoTime() < deadline) {
        assertThat(state.isPaused(stable)).isTrue();
      }
      writer.get(5, TimeUnit.SECONDS);
      assertThat(state.isPaused(stable)).isTrue();
    } finally {
      readerChecked.countDown();
      executor.shutdownNow();
      assertThat(executor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
    }
  }
}
