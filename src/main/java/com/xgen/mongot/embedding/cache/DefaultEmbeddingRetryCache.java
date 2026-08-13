package com.xgen.mongot.embedding.cache;

import com.github.benmanes.caffeine.cache.AsyncCache;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.Ticker;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.CaseFormat;
import com.xgen.mongot.metrics.MetricsFactory;
import com.xgen.mongot.util.Enums;
import com.xgen.mongot.util.bson.Vector;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import java.time.Duration;
import java.util.Collection;
import java.util.EnumMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import org.jetbrains.annotations.Nullable;

/**
 * In-memory {@link EmbeddingRetryCache}.
 *
 * <p><b>Insertion.</b> {@link #getAll} loads each missing vector once and stores it; concurrent
 * lookups for the same key share that single load rather than each calling the loader. The caller
 * then {@linkplain #claim claims} every key it looked up for its index generation. A claim ties a
 * key to the index generation that used it so the cache knows the key is still needed.
 *
 * <p><b>Eviction.</b> An entry is removed once no generation holds its key. A per-key count tracks
 * how many generations claim each key. A successful materialized-view commit releases only the keys
 * <em>that batch</em> committed (see {@link #releaseClaims(String, Collection, ReleaseTrigger)}) —
 * so the steady stream of empty change-stream batches, which embed and claim nothing, release
 * nothing and never disturb an in-flight batch's claims. An index drop releases the whole
 * generation (see {@link #releaseClaims(String, ReleaseTrigger)}). When a release drops a key's
 * count to zero the entry is removed — or, if its vector is still loading, removed when the load
 * lands (the count is re-checked, so a key re-claimed meanwhile survives).
 *
 * <p><b>TTLs.</b> Entries expire after {@value #ENTRY_TTL_MINUTES} minutes idle, and a generation's
 * claims expire after {@value #CLAIMS_TTL_MINUTES} minutes; both are long enough to fire only when
 * a generation is abandoned without a lifecycle event, never during an active retry.
 *
 * <p>The entry TTL is {@code expireAfterAccess}, and {@link #claim}/{@link #releaseClaims} do not
 * touch an entry's access time — so a claimed entry's liveness during retries relies on each retry
 * cycle re-calling {@link #getAll} (a read, which resets the idle timer) more often than {@value
 * #ENTRY_TTL_MINUTES} minutes. Retry/backoff cycles are seconds-to-minutes apart, so this holds in
 * practice; a gap longer than the entry TTL means the generation has effectively stalled, and the
 * claims TTL is the backstop that then reconciles its reference counts. We deliberately do not pin
 * entries to their claims: the extra machinery is not worth it for that abandonment edge case.
 */
final class DefaultEmbeddingRetryCache implements EmbeddingRetryCache {

  private static final long ENTRY_TTL_MINUTES = 30;
  private static final long CLAIMS_TTL_MINUTES = ENTRY_TTL_MINUTES * 2;

  private final AsyncCache<EmbeddingCacheKey, Vector> entries;
  private final Cache<EmbeddingCacheKey, Vector> entriesSync;
  private final Cache<String, Set<EmbeddingCacheKey>> claims;
  private final ConcurrentMap<EmbeddingCacheKey, Integer> claimCounts;
  private final Counter hits;
  private final Counter misses;
  private final Counter expirations;
  private final Map<ReleaseTrigger, Counter> releasedEntries;
  private final AtomicLong entriesGauge;
  private final AtomicLong bytesGauge;

  static DefaultEmbeddingRetryCache create(MeterRegistry registry) {
    return new DefaultEmbeddingRetryCache(registry, Ticker.systemTicker());
  }

  @VisibleForTesting
  DefaultEmbeddingRetryCache(MeterRegistry registry, Ticker ticker) {
    var metricsFactory = new MetricsFactory("embeddingRetryCache", registry);
    this.hits = metricsFactory.counter("hits");
    this.misses = metricsFactory.counter("misses");
    this.expirations = metricsFactory.counter("expirations");
    this.releasedEntries = new EnumMap<>(ReleaseTrigger.class);
    for (ReleaseTrigger trigger : ReleaseTrigger.values()) {
      this.releasedEntries.put(
          trigger,
          metricsFactory.counter(
              "releasedEntries",
              Tags.of("trigger", Enums.convertNameTo(CaseFormat.LOWER_CAMEL, trigger))));
    }
    this.entriesGauge = metricsFactory.numGauge("entries");
    this.bytesGauge = metricsFactory.numGauge("bytes");
    this.claimCounts = new ConcurrentHashMap<>();
    // The inline executor runs removal listeners and maintenance on the calling thread; both are
    // a few atomic updates, and it keeps the accounting synchronous with the removal.
    this.entries =
        Caffeine.newBuilder()
            .ticker(ticker)
            .expireAfterAccess(Duration.ofMinutes(ENTRY_TTL_MINUTES))
            .executor(Runnable::run)
            .removalListener(this::onEntryRemoved)
            .buildAsync();
    this.entriesSync = this.entries.synchronous();
    this.claims =
        Caffeine.newBuilder()
            .ticker(ticker)
            .expireAfterAccess(Duration.ofMinutes(CLAIMS_TTL_MINUTES))
            .executor(Runnable::run)
            .removalListener(this::onClaimSetRemoved)
            .build();
    metricsFactory.objectValueGauge("claimedGenerations", this.claims, Cache::estimatedSize);
    metricsFactory.objectValueGauge("claimedKeys", this.claimCounts, ConcurrentMap::size);
  }

  @Override
  public CompletableFuture<Map<EmbeddingCacheKey, Vector>> getAll(
      Collection<EmbeddingCacheKey> keys,
      Function<Set<EmbeddingCacheKey>, CompletableFuture<Map<EmbeddingCacheKey, Vector>>> loader) {
    Set<EmbeddingCacheKey> distinctKeys = Set.copyOf(keys);
    AtomicInteger missCount = new AtomicInteger();
    // Caffeine runs the mapping function inline with exactly the keys that are neither cached nor
    // already loading, so missCount is set before the counter updates below. Keys that join an
    // in-flight load count as hits: the embedding call was avoided.
    CompletableFuture<Map<EmbeddingCacheKey, Vector>> result =
        this.entries.getAll(
            distinctKeys,
            (missingKeys, executor) -> {
              missCount.set(missingKeys.size());
              return loader.apply(Set.copyOf(missingKeys)).thenApply(this::recordLoaded);
            });
    this.misses.increment(missCount.get());
    this.hits.increment(distinctKeys.size() - missCount.get());
    return result;
  }

  @Override
  public void claim(String generationKey, Collection<EmbeddingCacheKey> keys) {
    if (keys.isEmpty()) {
      return;
    }
    Set<EmbeddingCacheKey> claimSet =
        this.claims.get(generationKey, generation -> ConcurrentHashMap.newKeySet());
    for (EmbeddingCacheKey key : keys) {
      // Count one reference per generation. Re-claiming a key the generation already holds is a
      // no-op, so each increment is balanced by exactly one release or expiry.
      if (claimSet.add(key)) {
        this.claimCounts.merge(key, 1, Integer::sum);
      }
    }
  }

  @Override
  public void releaseClaims(
      String generationKey, Collection<EmbeddingCacheKey> keys, ReleaseTrigger trigger) {
    if (keys.isEmpty()) {
      return;
    }
    Set<EmbeddingCacheKey> claimSet = this.claims.getIfPresent(generationKey);
    if (claimSet == null) {
      return;
    }
    ConcurrentMap<EmbeddingCacheKey, CompletableFuture<Vector>> entryFutures = this.entries.asMap();
    for (EmbeddingCacheKey key : keys) {
      // Only release keys this generation actually holds; ignore the rest (e.g. an empty batch's
      // empty set, or keys an earlier commit of this generation already released).
      if (claimSet.remove(key)) {
        releaseHeldKey(key, trigger, entryFutures);
      }
    }
  }

  @Override
  public void releaseClaims(String generationKey, ReleaseTrigger trigger) {
    Set<EmbeddingCacheKey> released = this.claims.asMap().remove(generationKey);
    if (released == null) {
      return;
    }
    ConcurrentMap<EmbeddingCacheKey, CompletableFuture<Vector>> entryFutures = this.entries.asMap();
    for (EmbeddingCacheKey key : released) {
      releaseHeldKey(key, trigger, entryFutures);
    }
  }

  @VisibleForTesting
  void cleanUp() {
    this.entriesSync.cleanUp();
    this.claims.cleanUp();
  }

  /**
   * Releases one key that the generation held: drops its reference count and, if no generation
   * holds it anymore, evicts the entry. Evicts on the load's completion — immediately if the vector
   * is already cached, or once an in-flight load lands. {@link #evictIfUnclaimed} re-checks the
   * count so a key re-claimed in the meantime survives; thenRun is skipped if the load failed,
   * which Caffeine already discards.
   */
  private void releaseHeldKey(
      EmbeddingCacheKey key,
      ReleaseTrigger trigger,
      ConcurrentMap<EmbeddingCacheKey, CompletableFuture<Vector>> entryFutures) {
    if (stillClaimed(key)) {
      return;
    }
    CompletableFuture<Vector> entryFuture = entryFutures.get(key);
    if (entryFuture != null) {
      entryFuture.thenRun(() -> evictIfUnclaimed(key, trigger));
    }
  }

  /** Drops one generation's reference to a key; returns whether another generation holds it. */
  private boolean stillClaimed(EmbeddingCacheKey key) {
    return this.claimCounts.compute(
            key, (k, count) -> (count == null || count <= 1) ? null : count - 1)
        != null;
  }

  private void evictIfUnclaimed(EmbeddingCacheKey key, ReleaseTrigger trigger) {
    // Decide and remove under the per-key claimCounts bin lock that compute holds for its whole
    // body. This serializes against claim()'s claimCounts.merge, so a concurrent claim cannot slip
    // between the "still unclaimed" check and the removal and lose its just-claimed entry. We never
    // store a mapping — the lambda returns count unchanged (null stays absent, non-null stays).
    this.claimCounts.compute(
        key,
        (k, count) -> {
          if (count == null && this.entriesSync.asMap().remove(key) != null) {
            this.releasedEntries.get(trigger).increment();
          }
          return count;
        });
  }

  private void onClaimSetRemoved(
      @Nullable String generationKey, @Nullable Set<EmbeddingCacheKey> keys, RemovalCause cause) {
    // releaseClaims handles its own removals; here we only reclaim the refcounts of a claim set
    // that leaked (its generation never reached a terminal event) so its keys become evictable.
    // The entry TTL, not this path, is what then frees them.
    if (cause == RemovalCause.EXPIRED && keys != null) {
      keys.forEach(this::stillClaimed);
    }
  }

  private Map<EmbeddingCacheKey, Vector> recordLoaded(Map<EmbeddingCacheKey, Vector> loaded) {
    for (Vector vector : loaded.values()) {
      this.entriesGauge.incrementAndGet();
      this.bytesGauge.addAndGet(byteSize(vector));
    }
    return loaded;
  }

  private void onEntryRemoved(
      @Nullable EmbeddingCacheKey key, @Nullable Vector value, RemovalCause cause) {
    // A null value means the load never completed successfully, so it was never accounted.
    if (value != null) {
      this.entriesGauge.decrementAndGet();
      this.bytesGauge.addAndGet(-byteSize(value));
    }
    if (cause == RemovalCause.EXPIRED) {
      this.expirations.increment();
    }
  }

  private static long byteSize(Vector vector) {
    return switch (vector.getVectorType()) {
      case FLOAT -> (long) vector.asFloatVector().getFloatVector().length * Float.BYTES;
      // f16 and byte/bit vectors are all backed by their raw byte buffer.
      case FLOAT16, BYTE, BIT -> vector.getBytes().length;
    };
  }
}
