package com.xgen.mongot.embedding.cache;

import com.xgen.mongot.util.bson.Vector;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * Process-wide cache of generated embedding vectors, keyed by content hash, that survives batch
 * failures so retried materialized view writes do not re-embed texts a failed attempt already
 * embedded.
 *
 * <p>Callers {@link #claim} the keys a batch touches and {@link #releaseClaims} them on a terminal
 * event (commit or drop); an entry is evicted once no generation claims it, with a TTL backstop for
 * missed events. The cache imposes no size bound of its own — only texts admitted by the upstream
 * embedding budget become entries.
 */
public interface EmbeddingRetryCache {

  /**
   * Returns the vectors for {@code keys}, invoking {@code loader} at most once with the subset of
   * keys that are neither cached nor currently loading. Concurrent calls for overlapping keys join
   * the in-flight loads, so each key is embedded at most once at a time.
   *
   * <p>The loader may omit keys from its result map (e.g. per-text embedding errors); omitted keys
   * are excluded from the returned map and nothing is cached for them. A failed loader future fails
   * the returned future of every caller sharing the load, and the failure is not cached: the next
   * lookup loads again.
   *
   * <p>The loader must be non-blocking: it is invoked inline on the calling thread and must return
   * its {@link CompletableFuture} promptly, scheduling the embedding work asynchronously rather
   * than performing it before returning (the indexing-path loader wraps an async {@code embed}
   * call, so this holds by construction). In-flight coalescing does not depend on this — the shared
   * future is installed before the loader runs — but a loader that blocks would stall the calling
   * thread.
   */
  CompletableFuture<Map<EmbeddingCacheKey, Vector>> getAll(
      Collection<EmbeddingCacheKey> keys,
      Function<Set<EmbeddingCacheKey>, CompletableFuture<Map<EmbeddingCacheKey, Vector>>> loader);

  /** Records that the generation depends on {@code keys} staying cached. */
  void claim(String generationKey, Collection<EmbeddingCacheKey> keys);

  /**
   * Releases the generation's claim on exactly {@code keys} (the keys a committed batch embedded),
   * evicting those that no other generation still claims. Call when a batch's write commits to the
   * materialized view. Keys the generation does not hold are ignored, so an empty batch (which
   * embedded nothing) releases nothing and leaves other batches' in-flight claims intact.
   */
  void releaseClaims(
      String generationKey, Collection<EmbeddingCacheKey> keys, ReleaseTrigger trigger);

  /**
   * Releases all of the generation's claims and evicts the entries that no other generation still
   * claims. Call when the generation's index is dropped.
   */
  void releaseClaims(String generationKey, ReleaseTrigger trigger);

  /** The terminal event that triggered a {@link #releaseClaims} call, used to tag metrics. */
  enum ReleaseTrigger {
    COMMIT,
    DROP
  }

  /** Creates the caching implementation, registering its metrics with {@code registry}. */
  static EmbeddingRetryCache create(MeterRegistry registry) {
    return DefaultEmbeddingRetryCache.create(registry);
  }

  /**
   * Returns an instance that always invokes the loader and never retains anything, so call sites
   * that must not cache (e.g. the legacy non-materialized-view indexing strategy, which has no
   * per-batch commit to drive the claim lifecycle) can keep unconditional wiring.
   */
  static EmbeddingRetryCache noOp() {
    return NoOpEmbeddingRetryCache.INSTANCE;
  }
}
