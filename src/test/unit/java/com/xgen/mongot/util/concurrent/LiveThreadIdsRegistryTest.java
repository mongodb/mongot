package com.xgen.mongot.util.concurrent;

import static com.google.common.truth.Truth.assertThat;

import com.google.errorprone.annotations.Var;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Test;

public class LiveThreadIdsRegistryTest {

  @Test
  public void register_addsId() {
    LiveThreadIdsRegistry r = new LiveThreadIdsRegistry();
    r.register(42L);

    assertThat(r.contains(42L)).isTrue();
    assertThat(r.size()).isEqualTo(1);
  }

  @Test
  public void register_isIdempotent() {
    LiveThreadIdsRegistry r = new LiveThreadIdsRegistry();
    r.register(42L);
    r.register(42L);
    r.register(42L);

    assertThat(r.contains(42L)).isTrue();
    assertThat(r.size()).isEqualTo(1);
  }

  @Test
  public void remove_removesId() {
    LiveThreadIdsRegistry r = new LiveThreadIdsRegistry();
    r.register(42L);
    r.register(99L);
    r.remove(42L);

    assertThat(r.contains(42L)).isFalse();
    assertThat(r.contains(99L)).isTrue();
  }

  @Test
  public void remove_isIdempotent() {
    LiveThreadIdsRegistry r = new LiveThreadIdsRegistry();
    r.register(42L);
    r.remove(42L);
    r.remove(42L);
    r.remove(99L); // never registered

    assertThat(r.size()).isEqualTo(0);
  }

  @Test
  public void stream_reflectsCurrentRegistry() {
    LiveThreadIdsRegistry r = new LiveThreadIdsRegistry();
    r.register(42L);
    r.register(99L);

    assertThat(r.stream().toList()).containsExactly(42L, 99L);

    r.remove(42L);
    assertThat(r.stream().toList()).containsExactly(99L);
  }

  @Test
  public void stream_emptyWhenNothingRegistered() {
    LiveThreadIdsRegistry r = new LiveThreadIdsRegistry();

    assertThat(r.stream().count()).isEqualTo(0);
    assertThat(r.size()).isEqualTo(0);
  }

  @Test
  public void stream_safeToIterateConcurrentlyWithMutation() throws Exception {
    LiveThreadIdsRegistry r = new LiveThreadIdsRegistry();
    for (long i = 0; i < 200; i++) {
      r.register(i);
    }

    var stop = new AtomicBoolean(false);
    var failure = new AtomicReference<Throwable>();

    Thread mutator =
        new Thread(
            () -> {
              try {
                @Var long extra = 10_000L;
                while (!stop.get()) {
                  r.register(extra);
                  r.remove(extra - 1);
                  extra++;
                }
              } catch (Throwable t) {
                failure.compareAndSet(null, t);
              }
            });
    mutator.setDaemon(true);
    mutator.start();

    // Iterate the stream many times while the mutator is churning.
    for (int i = 0; i < 1000; i++) {
      // forEach exercises the underlying weakly-consistent iterator without copying.
      r.stream().forEach(id -> {});
    }

    stop.set(true);
    mutator.join(TimeUnit.MINUTES.toMillis(1));

    assertThat(failure.get()).isNull();
  }

  @Test
  public void concurrentRegisterAndRemove_isSafe() throws Exception {
    LiveThreadIdsRegistry r = new LiveThreadIdsRegistry();
    int workers = 8;
    var done = new CountDownLatch(workers);

    // Mirrors production usage: each thread registers/removes its own id concurrently with
    // others. After all workers join, registry must be empty and no operation may have thrown.
    for (int i = 0; i < workers; i++) {
      long id = i;
      Thread worker =
          new Thread(
              () -> {
                r.register(id);
                r.remove(id);
                done.countDown();
              });
      worker.setDaemon(true);
      worker.start();
    }

    done.await(1, TimeUnit.MINUTES);
    assertThat(r.size()).isEqualTo(0);
  }
}
