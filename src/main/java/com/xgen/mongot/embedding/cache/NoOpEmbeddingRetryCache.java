package com.xgen.mongot.embedding.cache;

import com.xgen.mongot.util.bson.Vector;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/** Pass-through implementation that never caches; see {@link EmbeddingRetryCache#noOp()}. */
final class NoOpEmbeddingRetryCache implements EmbeddingRetryCache {

  static final NoOpEmbeddingRetryCache INSTANCE = new NoOpEmbeddingRetryCache();

  private NoOpEmbeddingRetryCache() {}

  @Override
  public CompletableFuture<Map<EmbeddingCacheKey, Vector>> getAll(
      Collection<EmbeddingCacheKey> keys,
      Function<Set<EmbeddingCacheKey>, CompletableFuture<Map<EmbeddingCacheKey, Vector>>> loader) {
    // Match DefaultEmbeddingRetryCache, whose Caffeine getAll skips the loader for an empty key
    // set, so callers see identical behavior and the loader is never invoked with no keys.
    if (keys.isEmpty()) {
      return CompletableFuture.completedFuture(Map.of());
    }
    return loader.apply(Set.copyOf(keys));
  }

  @Override
  public void claim(String generationKey, Collection<EmbeddingCacheKey> keys) {}

  @Override
  public void releaseClaims(
      String generationKey, Collection<EmbeddingCacheKey> keys, ReleaseTrigger trigger) {}

  @Override
  public void releaseClaims(String generationKey, ReleaseTrigger trigger) {}
}
