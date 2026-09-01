package com.xgen.mongot.index.lucene.facet;

import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Expiry;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.Ticker;
import com.google.common.annotations.VisibleForTesting;
import com.xgen.mongot.util.Check;
import java.io.IOException;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletionException;
import org.apache.lucene.facet.sortedset.SortedSetDocValuesReaderState;
import org.apache.lucene.index.IndexReader;

/**
 * This class is a cache for ordinal maps on `token` fields. Ordinal maps are used in facet queries,
 * and we calculate ordinal maps upon the initial facet query on a field, then cache the ordinal
 * maps.
 */
public class TokenFacetsStateCache {
  private static final Duration DEFAULT_FACET_CACHE_EXPIRY = Duration.ofDays(1);
  private final Ticker ticker;
  private final Expiry<String, Optional<TokenSsdvFacetState>> expiry;
  private final LoadingCache<String, Optional<TokenSsdvFacetState>> cache;
  private final Optional<Integer> cardinalityLimit;
  private final CacheCounts inheritedCounts;
  private final boolean recordStats;

  private TokenFacetsStateCache(
      LoadingCache<String, Optional<TokenSsdvFacetState>> cache,
      Expiry<String, Optional<TokenSsdvFacetState>> expiry,
      Optional<Integer> cardinalityLimit,
      Ticker ticker,
      CacheCounts inheritedCounts,
      boolean recordStats) {
    this.ticker = ticker;
    this.cache = cache;
    this.expiry = expiry;
    this.cardinalityLimit = cardinalityLimit;
    this.inheritedCounts = inheritedCounts;
    this.recordStats = recordStats;
  }

  /**
   * Point-in-time view of cache size and Caffeine counters. Taken while the searcher is held so the
   * live cache is not retained after release.
   */
  public record Snapshot(long heapBytes, long entries, long hits, long misses, long evictions) {
    public static final Snapshot EMPTY = new Snapshot(0L, 0L, 0L, 0L, 0L);

    public Snapshot plus(Snapshot other) {
      return new Snapshot(
          this.heapBytes + other.heapBytes,
          this.entries + other.entries,
          this.hits + other.hits,
          this.misses + other.misses,
          this.evictions + other.evictions);
    }
  }

  /**
   * Totals accumulated by this cache's predecessors, so the exported counts survive the rebuild
   * that every reader refresh performs. See {@link #cloneWithNewIndexReader}.
   */
  private record CacheCounts(long hits, long misses, long evictions) {
    private static final CacheCounts ZERO = new CacheCounts(0, 0, 0);
  }

  /**
   * Default method of creating this cache. Given a reader, use system ticker and default entry
   * expiration time.
   *
   * @param reader Lucene Index Reader
   * @return instance of TokenFacetsStateCache, tied to the IndexReader
   */
  public static TokenFacetsStateCache create(
      IndexReader reader, Optional<Integer> cardinalityLimit) {
    return create(reader, cardinalityLimit, false);
  }

  public static TokenFacetsStateCache create(
      IndexReader reader, Optional<Integer> cardinalityLimit, boolean recordStats) {
    return create(
        reader, DEFAULT_FACET_CACHE_EXPIRY, cardinalityLimit, Ticker.systemTicker(), recordStats);
  }

  @VisibleForTesting
  static TokenFacetsStateCache create(
      IndexReader reader,
      Duration expireDuration,
      Optional<Integer> cardinalityLimit,
      Ticker ticker) {
    return create(reader, expireDuration, cardinalityLimit, ticker, false);
  }

  private static TokenFacetsStateCache create(
      IndexReader reader,
      Duration expireDuration,
      Optional<Integer> cardinalityLimit,
      Ticker ticker,
      boolean recordStats) {
    var expiry = new CustomExpiry(expireDuration);
    return new TokenFacetsStateCache(
        buildCache(reader, expiry, ticker, cardinalityLimit, recordStats),
        expiry,
        cardinalityLimit,
        ticker,
        CacheCounts.ZERO,
        recordStats);
  }

  private static LoadingCache<String, Optional<TokenSsdvFacetState>> buildCache(
      IndexReader reader,
      Expiry<String, Optional<TokenSsdvFacetState>> expiry,
      Ticker ticker,
      Optional<Integer> cardinalityLimit,
      boolean recordStats) {
    var builder = Caffeine.newBuilder().softValues().ticker(ticker).expireAfter(expiry);
    if (recordStats) {
      builder.recordStats();
    }
    return builder.build(new FacetsCacheLoader(reader, cardinalityLimit));
  }

  private record FacetsCacheLoader(IndexReader indexReader, Optional<Integer> cardinalityLimit)
      implements CacheLoader<String, Optional<TokenSsdvFacetState>> {

    /**
     * Loads the corresponding TokenSsdvFacetState for a lucene field into the cache
     *
     * @param luceneFieldName the lucene field name for which an ordinal map will be created and
     *     cached
     * @return TokenSsdvFacetState
     * @throws IOException if accessing DocValues fails in an unexpected way
     * @throws TokenFacetsCardinalityLimitExceededException if the cardinality of the field is above
     *     the limit
     */
    @Override
    public Optional<TokenSsdvFacetState> load(String luceneFieldName)
        throws IOException, TokenFacetsCardinalityLimitExceededException {
      return TokenSsdvFacetState.create(this.indexReader, luceneFieldName, this.cardinalityLimit);
    }
  }

  /**
   * Grabs an entry from the cache if it is loaded, otherwise creates and loads the entry. Cache
   * contains soft references, which could have been garbage-collected. Re-compute the ordinal map
   * in that case. Existing references will have their `lastQueriedTime` updated.
   *
   * @param luceneFieldName the lucene path to the field requested for faceting
   * @return {@link SortedSetDocValuesReaderState} ordinal map entry. Should not be Optional.empty()
   *     since we re-build the ordinal map if evicted, but programmatically possible so we return an
   *     Optional.
   * @throws TokenFacetsCardinalityLimitExceededException if cardinality limit is exceeded
   * @throws RuntimeException or Error if the {@link CacheLoader} does so, in which case the mapping
   *     is left unestablished.
   */
  public Optional<TokenSsdvFacetState> get(String luceneFieldName)
      throws TokenFacetsCardinalityLimitExceededException {
    try {
      return this.cache.get(luceneFieldName);
    } catch (CompletionException e) {
      if (e.getCause()
          instanceof
          TokenFacetsCardinalityLimitExceededException cardinalityLimitExceededException) {
        throw cardinalityLimitExceededException;
      }
      // other exception types are not expected since
      // TokenFacetsCardinalityLimitExceededException is the only CheckedException thrown in `load`
      throw e;
    }
  }

  @VisibleForTesting
  public Optional<Duration> getTimeBeforeExpiring(String luceneFieldName) {
    return this.cache
        .policy()
        .expireVariably()
        .flatMap(policy -> policy.getExpiresAfter(luceneFieldName));
  }

  /**
   * Clones the cache for a new reader. Evicts any expired entries from the old map, and uses the
   * new index reader but the field names from the old map to rebuild the cache (Note: does not use
   * the old ordinal maps since those are tied to the old index reader).
   *
   * @param newReader new IndexReader
   * @return {@link TokenFacetsStateCache} refreshed cache tied to new index reader
   */
  public TokenFacetsStateCache cloneWithNewIndexReader(IndexReader newReader) throws IOException {
    return cloneWithNewIndexReader(newReader, this.recordStats);
  }

  /**
   * Clones the cache for a new reader, recording Caffeine stats only when {@code recordStats} is
   * true so a metrics kill switch takes effect on the next refresh. Called from {@code
   * LuceneIndexSearcher} on reader refresh; not private because that class lives in another
   * package.
   */
  public TokenFacetsStateCache cloneWithNewIndexReader(IndexReader newReader, boolean recordStats)
      throws IOException {
    var newCache =
        buildCache(newReader, this.expiry, this.ticker, this.cardinalityLimit, recordStats);

    for (Map.Entry<String, Optional<TokenSsdvFacetState>> entry : this.cache.asMap().entrySet()) {
      String key = entry.getKey();
      Optional<TokenSsdvFacetState> value = entry.getValue();
      // Each cache is created with a variable expiration policy so this should always be present.
      var expirePolicy = Check.isPresent(this.cache.policy().expireVariably(), "expirePolicy");
      Optional<Duration> expiresAfter = expirePolicy.getExpiresAfter(key);
      if (expiresAfter.isEmpty()) {
        // will return empty if entry is expired
        continue;
      }
      // should not be possible to be negative but just to be safe
      if (expiresAfter.get().isPositive()) {
        var newCacheExpirePolicy =
            Check.isPresent(newCache.policy().expireVariably(), "expirePolicy");
        if (value.isPresent()) {
          try {
            newCacheExpirePolicy.put(
                key,
                TokenSsdvFacetState.create(
                    newReader, value.get().luceneFieldName, this.cardinalityLimit),
                expiresAfter.get());
          } catch (TokenFacetsCardinalityLimitExceededException exception) {
            // If cardinality limit for a field is exceeded when
            // refreshing cache, we can simply exclude it from the new cache.
          }
        }
      }
    }
    // Carry this cache's totals into its successor. Caffeine's own counters are per instance and
    // the cache is replaced on every reader refresh, so without this the exported counts would
    // reset to zero several times an hour and read as a cache that never hits anything.
    return new TokenFacetsStateCache(
        newCache, this.expiry, this.cardinalityLimit, this.ticker, cumulativeCounts(), recordStats);
  }

  /** Heap, entry count, and cumulative hit/miss/eviction counters in one pass. */
  public Snapshot snapshot() {
    CacheCounts counts = cumulativeCounts();
    return new Snapshot(
        this.heapBytes(), this.entryCount(), counts.hits(), counts.misses(), counts.evictions());
  }

  @VisibleForTesting
  Map<String, Optional<TokenSsdvFacetState>> asMap() {
    return this.cache.asMap();
  }

  /** How many fields this cache is currently holding state for. */
  public long entryCount() {
    return this.cache.estimatedSize();
  }

  /**
   * Cache hits over the life of this cache and every cache it succeeded.
   *
   * <p>Monotonic across reader refreshes, which is what makes it meaningful as a rate. It does
   * reset if an index is closed and reopened, since that starts a fresh lineage rather than cloning
   * -- the same thing a process restart does to any counter.
   */
  public long hitCount() {
    return cumulativeCounts().hits();
  }

  /** Cache misses; see {@link #hitCount} for the lineage semantics. */
  public long missCount() {
    return cumulativeCounts().misses();
  }

  /**
   * Entries evicted; see {@link #hitCount} for the lineage semantics. With {@code softValues} this
   * includes entries the collector reclaimed, which is the signal that the cache is churning under
   * heap pressure rather than quietly accumulating.
   */
  public long evictionCount() {
    return cumulativeCounts().evictions();
  }

  private CacheCounts cumulativeCounts() {
    var stats = this.cache.stats();
    return new CacheCounts(
        this.inheritedCounts.hits() + stats.hitCount(),
        this.inheritedCounts.misses() + stats.missCount(),
        this.inheritedCounts.evictions() + stats.evictionCount());
  }

  /**
   * Heap held by the cached ordinal maps right now. The cache holds soft values, so entries the
   * collector has already reclaimed have dropped out of {@code asMap()} and are not counted — which
   * is the wanted semantics for a heap gauge.
   */
  public long heapBytes() {
    return this.cache.asMap().values().stream()
        .flatMap(Optional::stream)
        .mapToLong(TokenSsdvFacetState::ramBytesUsed)
        .sum();
  }

  // Need a variable expiration class to enable modifying
  private static class CustomExpiry implements Expiry<String, Optional<TokenSsdvFacetState>> {
    final long expiry;

    private CustomExpiry(Duration expiry) {
      this.expiry = expiry.toNanos();
    }

    @Override
    public long expireAfterCreate(
        String key, Optional<TokenSsdvFacetState> value, long currentTime) {
      return this.expiry;
    }

    @Override
    public long expireAfterUpdate(
        String key, Optional<TokenSsdvFacetState> value, long currentTime, long currentDuration) {
      return this.expiry;
    }

    @Override
    public long expireAfterRead(
        String key, Optional<TokenSsdvFacetState> value, long currentTime, long currentDuration) {
      return this.expiry;
    }
  }
}
