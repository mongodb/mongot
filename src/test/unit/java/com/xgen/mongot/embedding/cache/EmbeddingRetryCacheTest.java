package com.xgen.mongot.embedding.cache;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.google.common.testing.FakeTicker;
import com.xgen.mongot.embedding.cache.EmbeddingCacheKey.InputType;
import com.xgen.mongot.embedding.cache.EmbeddingRetryCache.ReleaseTrigger;
import com.xgen.mongot.embedding.utils.AutoEmbeddingDocumentUtils;
import com.xgen.mongot.index.definition.quantization.VectorAutoEmbedQuantization;
import com.xgen.mongot.util.bson.FloatVector;
import com.xgen.mongot.util.bson.Vector;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.junit.Before;
import org.junit.Test;

public class EmbeddingRetryCacheTest {

  private SimpleMeterRegistry registry;
  private FakeTicker ticker;
  private DefaultEmbeddingRetryCache cache;

  @Before
  public void setUp() {
    this.registry = new SimpleMeterRegistry();
    this.ticker = new FakeTicker();
    this.cache = new DefaultEmbeddingRetryCache(this.registry, this.ticker::read);
  }

  @Test
  public void getAllServesCachedEntriesWithoutInvokingLoader() {
    var key = key("text-1");
    var loader = new RecordingLoader();

    Map<EmbeddingCacheKey, Vector> first = this.cache.getAll(List.of(key), loader).join();
    Map<EmbeddingCacheKey, Vector> second = this.cache.getAll(List.of(key), loader).join();

    assertThat(first.keySet()).containsExactly(key);
    assertThat(second.get(key)).isSameInstanceAs(first.get(key));
    assertThat(loader.invocationCount()).isEqualTo(1);
    assertThat(counter("hits")).isEqualTo(1.0);
    assertThat(counter("misses")).isEqualTo(1.0);
  }

  @Test
  public void getAllInvokesLoaderWithOnlyTheMissingKeys() {
    var cached = key("text-1");
    var missing = key("text-2");
    this.cache.getAll(List.of(cached), new RecordingLoader()).join();

    var loader = new RecordingLoader();
    Map<EmbeddingCacheKey, Vector> result =
        this.cache.getAll(List.of(cached, missing), loader).join();

    assertThat(result.keySet()).containsExactly(cached, missing);
    assertThat(loader.allLoadedKeys()).containsExactly(missing);
    assertThat(counter("hits")).isEqualTo(1.0);
    assertThat(counter("misses")).isEqualTo(2.0);
  }

  @Test
  public void concurrentLookupsCoalesceOnInFlightLoads() {
    var shared = key("text-1");
    var extra = key("text-2");
    var sharedLoad = new CompletableFuture<Map<EmbeddingCacheKey, Vector>>();
    var sharedLoaderInvocations = new AtomicInteger();

    var first =
        this.cache.getAll(
            List.of(shared),
            keys -> {
              sharedLoaderInvocations.incrementAndGet();
              return sharedLoad;
            });
    var secondLoader = new RecordingLoader();
    var second = this.cache.getAll(List.of(shared, extra), secondLoader);

    // The overlap joined the in-flight load instead of loading again.
    assertThat(secondLoader.allLoadedKeys()).containsExactly(extra);
    assertThat(second.isDone()).isFalse();

    var sharedVector = vector(1.0f);
    sharedLoad.complete(Map.of(shared, sharedVector));

    assertThat(first.join().get(shared)).isSameInstanceAs(sharedVector);
    assertThat(second.join().get(shared)).isSameInstanceAs(sharedVector);
    assertThat(second.join().keySet()).containsExactly(shared, extra);
    assertThat(sharedLoaderInvocations.get()).isEqualTo(1);
    // The joined in-flight key counts as a hit: its embed call was avoided.
    assertThat(counter("hits")).isEqualTo(1.0);
    assertThat(counter("misses")).isEqualTo(2.0);
  }

  @Test
  public void failedLoadFailsSharersAndIsNotRetained() {
    var key = key("text-1");
    var failingLoad = new CompletableFuture<Map<EmbeddingCacheKey, Vector>>();

    var first = this.cache.getAll(List.of(key), keys -> failingLoad);
    var second = this.cache.getAll(List.of(key), EmbeddingRetryCacheTest::rejectLoad);
    failingLoad.completeExceptionally(new RuntimeException("embedding provider unavailable"));

    assertThrows(CompletionException.class, first::join);
    assertThrows(CompletionException.class, second::join);

    // The failure is not cached: the next lookup loads again and succeeds.
    var loader = new RecordingLoader();
    assertThat(this.cache.getAll(List.of(key), loader).join().keySet()).containsExactly(key);
    assertThat(loader.invocationCount()).isEqualTo(1);
  }

  @Test
  public void keysOmittedByTheLoaderAreNotCachedAndDoNotFailTheLookup() {
    var loadable = key("text-1");
    var failed = key("text-2");

    // The loader omits keys whose texts failed to embed (per-text VectorOrError errors).
    Map<EmbeddingCacheKey, Vector> result =
        this.cache
            .getAll(
                List.of(loadable, failed),
                keys -> CompletableFuture.completedFuture(Map.of(loadable, vector(1.0f))))
            .join();
    assertThat(result.keySet()).containsExactly(loadable);

    var loader = new RecordingLoader();
    this.cache.getAll(List.of(loadable, failed), loader).join();
    assertThat(loader.allLoadedKeys()).containsExactly(failed);
  }

  @Test
  public void releaseClaimsEvictsOnlyKeysNoOtherGenerationClaims() {
    var exclusive = key("text-1");
    var shared = key("text-2");
    this.cache.getAll(List.of(exclusive, shared), new RecordingLoader()).join();
    this.cache.claim("generation-a", List.of(exclusive, shared));
    this.cache.claim("generation-b", List.of(shared));
    assertThat(gauge("claimedGenerations")).isEqualTo(2.0);

    this.cache.releaseClaims("generation-a", ReleaseTrigger.COMMIT);

    var loader = new RecordingLoader();
    this.cache.getAll(List.of(exclusive, shared), loader).join();
    assertThat(loader.allLoadedKeys()).containsExactly(exclusive);
    assertThat(counter("releasedEntries", "commit")).isEqualTo(1.0);
    assertThat(gauge("claimedGenerations")).isEqualTo(1.0);

    this.cache.releaseClaims("generation-b", ReleaseTrigger.DROP);

    var reloader = new RecordingLoader();
    this.cache.getAll(List.of(shared), reloader).join();
    assertThat(reloader.allLoadedKeys()).containsExactly(shared);
    assertThat(counter("releasedEntries", "drop")).isEqualTo(1.0);
  }

  @Test
  public void keyScopedReleaseEvictsOnlyTheGivenKeys() {
    var committed = key("text-1");
    var stillPending = key("text-2");
    this.cache.getAll(List.of(committed, stillPending), new RecordingLoader()).join();
    this.cache.claim("generation-a", List.of(committed, stillPending));

    // A batch commits only the keys it wrote; the other key stays cached for its own retry.
    this.cache.releaseClaims("generation-a", List.of(committed), ReleaseTrigger.COMMIT);

    var loader = new RecordingLoader();
    this.cache.getAll(List.of(committed, stillPending), loader).join();
    assertThat(loader.allLoadedKeys()).containsExactly(committed);
    assertThat(counter("releasedEntries", "commit")).isEqualTo(1.0);
  }

  @Test
  public void keyScopedReleaseOfKeysTheGenerationDoesNotHoldDoesNotEvict() {
    // Models the empty change-stream batch: it claims nothing, so its commit must not disturb the
    // claims of a different, still-in-flight batch under the same generation.
    var inFlight = key("text-1");
    var neverClaimed = key("text-2");
    this.cache.getAll(List.of(inFlight), new RecordingLoader()).join();
    this.cache.claim("generation-a", List.of(inFlight));

    this.cache.releaseClaims("generation-a", List.of(), ReleaseTrigger.COMMIT);
    this.cache.releaseClaims("generation-a", List.of(neverClaimed), ReleaseTrigger.COMMIT);

    var loader = new RecordingLoader();
    this.cache.getAll(List.of(inFlight), loader).join();
    assertThat(loader.invocationCount()).isEqualTo(0);
    assertThat(counter("releasedEntries", "commit")).isEqualTo(0.0);
  }

  @Test
  public void releaseDuringInFlightLoadEvictsTheEntryWhenItLandsUnclaimed() {
    var key = key("text-1");
    var inFlightLoad = new CompletableFuture<Map<EmbeddingCacheKey, Vector>>();

    var lookup = this.cache.getAll(List.of(key), keys -> inFlightLoad);
    this.cache.claim("generation-a", List.of(key));
    // A terminal event fires while the embedding is still loading: the entry is not yet
    // completed, so it cannot be evicted synchronously.
    this.cache.releaseClaims("generation-a", ReleaseTrigger.DROP);
    assertThat(counter("releasedEntries", "drop")).isEqualTo(0.0);

    // The load lands after the release; the now-unclaimed vector must not linger until its TTL.
    inFlightLoad.complete(Map.of(key, vector(1.0f)));
    lookup.join();
    assertThat(counter("releasedEntries", "drop")).isEqualTo(1.0);

    var loader = new RecordingLoader();
    this.cache.getAll(List.of(key), loader).join();
    assertThat(loader.allLoadedKeys()).containsExactly(key);
  }

  @Test
  public void entryReclaimedBeforeAnInFlightLoadLandsSurvivesAnEarlierRelease() {
    var key = key("text-1");
    var inFlightLoad = new CompletableFuture<Map<EmbeddingCacheKey, Vector>>();

    var lookup = this.cache.getAll(List.of(key), keys -> inFlightLoad);
    this.cache.claim("generation-a", List.of(key));
    this.cache.releaseClaims("generation-a", ReleaseTrigger.DROP);
    // A second generation claims the key before the load completes.
    this.cache.claim("generation-b", List.of(key));

    inFlightLoad.complete(Map.of(key, vector(1.0f)));
    lookup.join();

    // generation-b still claims it, so the completion hook must keep it cached.
    assertThat(counter("releasedEntries", "drop")).isEqualTo(0.0);
    var loader = new RecordingLoader();
    this.cache.getAll(List.of(key), loader).join();
    assertThat(loader.invocationCount()).isEqualTo(0);
  }

  @Test
  public void releaseClaimsForUnknownGenerationIsNoOp() {
    this.cache.getAll(List.of(key("text-1")), new RecordingLoader()).join();

    this.cache.releaseClaims("unknown-generation", ReleaseTrigger.COMMIT);

    var loader = new RecordingLoader();
    this.cache.getAll(List.of(key("text-1")), loader).join();
    assertThat(loader.invocationCount()).isEqualTo(0);
    assertThat(counter("releasedEntries", "commit")).isEqualTo(0.0);
  }

  @Test
  public void entriesExpireOnlyWhenIdlePastTtl() {
    var key = key("text-1");
    this.cache.getAll(List.of(key), new RecordingLoader()).join();

    // Accesses within the TTL keep the entry alive well past the TTL itself, mirroring a live
    // retry cycle re-accessing its entries every backoff round.
    for (int i = 0; i < 4; i++) {
      this.ticker.advance(Duration.ofMinutes(20));
      this.cache.getAll(List.of(key), EmbeddingRetryCacheTest::rejectLoad).join();
    }
    assertThat(counter("expirations")).isEqualTo(0.0);

    this.ticker.advance(Duration.ofMinutes(31));
    this.cache.cleanUp();

    assertThat(counter("expirations")).isEqualTo(1.0);
    var loader = new RecordingLoader();
    this.cache.getAll(List.of(key), loader).join();
    assertThat(loader.allLoadedKeys()).containsExactly(key);
  }

  @Test
  public void staleClaimSetsExpireAndStopPinningEntries() {
    var key = key("text-1");
    this.cache.getAll(List.of(key), new RecordingLoader()).join();
    this.cache.claim("stale-generation", List.of(key));

    // Another generation keeps the entry and its own claim alive while the stale generation
    // never claims again; 80 minutes of silence expires the stale claim set (60-minute TTL).
    for (int i = 0; i < 4; i++) {
      this.ticker.advance(Duration.ofMinutes(20));
      this.cache.claim("live-generation", List.of(key));
      this.cache.getAll(List.of(key), EmbeddingRetryCacheTest::rejectLoad).join();
    }
    this.cache.cleanUp();

    this.cache.releaseClaims("live-generation", ReleaseTrigger.DROP);

    // Were the stale claim still present, it would have kept the entry alive.
    var loader = new RecordingLoader();
    this.cache.getAll(List.of(key), loader).join();
    assertThat(loader.allLoadedKeys()).containsExactly(key);
  }

  @Test
  public void gaugesTrackEntryAndByteAccounting() {
    var floatKey = key("float-vector");
    var byteKey = key("byte-vector");
    var bitKey = key("bit-vector");
    var vectorsByKey =
        Map.of(
            floatKey, vector(1.0f), // 4 floats = 16 bytes
            byteKey, Vector.fromBytes(new byte[] {1, 2, 3}), // 3 bytes
            bitKey, Vector.fromBits(new byte[] {0x0f, 0x01})); // 2 bytes

    this.cache
        .getAll(
            vectorsByKey.keySet(),
            keys ->
                CompletableFuture.completedFuture(
                    keys.stream().collect(Collectors.toMap(key -> key, vectorsByKey::get))))
        .join();

    assertThat(gauge("entries")).isEqualTo(3.0);
    assertThat(gauge("bytes")).isEqualTo(21.0);

    this.cache.claim("generation", List.of(floatKey));
    this.cache.releaseClaims("generation", ReleaseTrigger.COMMIT);

    assertThat(gauge("entries")).isEqualTo(2.0);
    assertThat(gauge("bytes")).isEqualTo(5.0);
  }

  @Test
  public void keysDifferAcrossEveryDimension() {
    var base =
        new EmbeddingCacheKey(
            "model", 256, VectorAutoEmbedQuantization.FLOAT, InputType.DOCUMENT, "hash");

    assertThat(base)
        .isEqualTo(
            new EmbeddingCacheKey(
                "model", 256, VectorAutoEmbedQuantization.FLOAT, InputType.DOCUMENT, "hash"));
    assertThat(base)
        .isNotEqualTo(
            new EmbeddingCacheKey(
                "other", 256, VectorAutoEmbedQuantization.FLOAT, InputType.DOCUMENT, "hash"));
    assertThat(base)
        .isNotEqualTo(
            new EmbeddingCacheKey(
                "model", 512, VectorAutoEmbedQuantization.FLOAT, InputType.DOCUMENT, "hash"));
    assertThat(base)
        .isNotEqualTo(
            new EmbeddingCacheKey(
                "model", 256, VectorAutoEmbedQuantization.SCALAR, InputType.DOCUMENT, "hash"));
    assertThat(base)
        .isNotEqualTo(
            new EmbeddingCacheKey(
                "model", 256, VectorAutoEmbedQuantization.FLOAT, InputType.QUERY, "hash"));
    assertThat(base)
        .isNotEqualTo(
            new EmbeddingCacheKey(
                "model", 256, VectorAutoEmbedQuantization.FLOAT, InputType.DOCUMENT, "other"));
  }

  @Test
  public void forDocumentHashesTheSourceText() {
    var key =
        EmbeddingCacheKey.forDocument("model", 256, VectorAutoEmbedQuantization.FLOAT, "a text");

    assertThat(key.textHash()).isEqualTo(AutoEmbeddingDocumentUtils.computeTextHash("a text"));
    assertThat(key.inputType()).isEqualTo(InputType.DOCUMENT);
  }

  @Test
  public void noOpInstanceAlwaysInvokesTheLoader() {
    var noOp = EmbeddingRetryCache.noOp();
    var key = key("text-1");
    var loader = new RecordingLoader();

    assertThat(noOp.getAll(List.of(key), loader).join().keySet()).containsExactly(key);
    assertThat(noOp.getAll(List.of(key), loader).join().keySet()).containsExactly(key);
    assertThat(loader.invocationCount()).isEqualTo(2);

    // Empty keys short-circuit without invoking the loader, matching DefaultEmbeddingRetryCache.
    assertThat(noOp.getAll(List.of(), loader).join()).isEmpty();
    assertThat(loader.invocationCount()).isEqualTo(2);

    // Claim bookkeeping is a no-op but must be callable unconditionally.
    noOp.claim("generation", List.of(key));
    noOp.releaseClaims("generation", ReleaseTrigger.COMMIT);
  }

  @Test
  public void concurrentLoadClaimAndReleaseSmokeTest() throws Exception {
    int threads = 8;
    int rounds = 100;
    List<EmbeddingCacheKey> keys = new ArrayList<>();
    for (int i = 0; i < 16; i++) {
      keys.add(key("text-" + i));
    }

    ExecutorService executor = Executors.newFixedThreadPool(threads);
    try {
      var start = new CountDownLatch(1);
      List<Future<?>> workers = new ArrayList<>();
      for (int thread = 0; thread < threads; thread++) {
        int offset = thread;
        String generation = "generation-" + thread;
        workers.add(
            executor.submit(
                () -> {
                  start.await();
                  for (int round = 0; round < rounds; round++) {
                    List<EmbeddingCacheKey> subset = new ArrayList<>();
                    for (int i = 0; i < 4; i++) {
                      subset.add(keys.get((offset + round + i * 5) % keys.size()));
                    }
                    Map<EmbeddingCacheKey, Vector> result =
                        this.cache.getAll(subset, new RecordingLoader()).join();
                    // Every requested key resolves: a concurrent eviction can only cause a
                    // reload, never a missing or corrupted result.
                    assertThat(result.keySet()).containsExactlyElementsIn(subset);
                    this.cache.claim(generation, subset);
                    if (round % 10 == 9) {
                      this.cache.releaseClaims(generation, ReleaseTrigger.COMMIT);
                    }
                  }
                  return null;
                }));
      }
      start.countDown();
      for (Future<?> worker : workers) {
        worker.get(30, TimeUnit.SECONDS);
      }
    } finally {
      executor.shutdownNow();
    }
  }

  private static EmbeddingCacheKey key(String textHash) {
    return new EmbeddingCacheKey(
        "voyage-3-large", 4, VectorAutoEmbedQuantization.FLOAT, InputType.DOCUMENT, textHash);
  }

  private static Vector vector(float value) {
    return Vector.fromFloats(
        new float[] {value, value, value, value}, FloatVector.OriginalType.NATIVE);
  }

  /** Loader that fails the test if the cache invokes it. */
  private static CompletableFuture<Map<EmbeddingCacheKey, Vector>> rejectLoad(
      Set<EmbeddingCacheKey> keys) {
    throw new AssertionError("loader must not be invoked for " + keys);
  }

  private double counter(String name) {
    return this.registry.get("embeddingRetryCache." + name).counter().count();
  }

  private double counter(String name, String trigger) {
    return this.registry
        .get("embeddingRetryCache." + name)
        .tag("trigger", trigger)
        .counter()
        .count();
  }

  private double gauge(String name) {
    return this.registry.get("embeddingRetryCache." + name).gauge().value();
  }

  /** Loader that serves a vector per requested key and records each invocation's key set. */
  private static final class RecordingLoader
      implements Function<
          Set<EmbeddingCacheKey>, CompletableFuture<Map<EmbeddingCacheKey, Vector>>> {

    private final Queue<Set<EmbeddingCacheKey>> invocations = new ConcurrentLinkedQueue<>();

    @Override
    public CompletableFuture<Map<EmbeddingCacheKey, Vector>> apply(Set<EmbeddingCacheKey> keys) {
      this.invocations.add(keys);
      Map<EmbeddingCacheKey, Vector> loaded = new HashMap<>();
      keys.forEach(key -> loaded.put(key, vector(1.0f)));
      return CompletableFuture.completedFuture(loaded);
    }

    int invocationCount() {
      return this.invocations.size();
    }

    Set<EmbeddingCacheKey> allLoadedKeys() {
      return this.invocations.stream().flatMap(Set::stream).collect(Collectors.toSet());
    }
  }
}
