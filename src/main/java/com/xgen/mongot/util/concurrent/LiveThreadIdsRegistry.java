package com.xgen.mongot.util.concurrent;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

/**
 * Tracks the IDs of live threads owned by a single source, for {@link
 * com.xgen.mongot.metrics.ThreadPoolResourceMetrics} consumption.
 *
 * <p>Thread-safe: the per-pool metric registration calls {@link #stream()} at scrape time to
 * read the current set of live ids.
 */
public final class LiveThreadIdsRegistry {

  private final Set<Long> threadIds = ConcurrentHashMap.newKeySet();

  /** Adds a thread id to the live set. Idempotent. */
  public void register(long threadId) {
    this.threadIds.add(threadId);
  }

  /** Removes a thread id from the live set. Idempotent. */
  public void remove(long threadId) {
    this.threadIds.remove(threadId);
  }

  /**
   * Returns a stream of currently-live thread ids. The stream is not a defensive copy;
   * callers that need to retire a thread id should call {@link #remove(long)}.
   */
  public Stream<Long> stream() {
    return this.threadIds.stream();
  }

  /** Whether the given thread id is currently registered. Intended for tests and diagnostics. */
  public boolean contains(long threadId) {
    return this.threadIds.contains(threadId);
  }

  /** Number of live thread ids currently tracked. Intended for tests and diagnostics. */
  public int size() {
    return this.threadIds.size();
  }
}
