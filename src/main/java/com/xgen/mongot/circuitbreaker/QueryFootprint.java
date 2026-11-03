package com.xgen.mongot.circuitbreaker;

import java.util.concurrent.atomic.AtomicLong;

/**
 * What one in-flight query has reserved: bytes it reported and has not yet given back.
 *
 * <p>Reservations are refused and released through the registry, so the per query count and the
 * node-wide total can never disagree. This class only holds the arithmetic.
 */
public final class QueryFootprint {

  /**
   * Assigned by the registry when the query starts; unique per process, unrelated to cursor IDs.
   */
  private final long queryId;

  /** Bytes this query has reserved and not yet released. */
  private final AtomicLong reservedBytes = new AtomicLong();

  public QueryFootprint(long queryId) {
    this.queryId = queryId;
  }

  public long queryId() {
    return this.queryId;
  }

  /** The bytes this query holds. Never negative, since a stale release could make it so. */
  public long reservedBytes() {
    return Math.max(0L, this.reservedBytes.get());
  }

  /**
   * Whether this query stays under the minimum query size {@code minQueryBytes} after reserving
   * {@code bytes}. A query under that size is never refused or cancelled, whatever its type.
   *
   * <p>One minimum applies to every query type
   */
  public boolean isSmallAfter(long bytes, long minQueryBytes) {
    return reservedBytes() + bytes < minQueryBytes;
  }

  void reserve(long bytes) {
    this.reservedBytes.addAndGet(bytes);
  }

  void release(long bytes) {
    this.reservedBytes.addAndGet(-bytes);
  }
}
