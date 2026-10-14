package com.xgen.mongot.index.query.aggregation;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Per-partition running total of accumulator heap charged during one search aggregation.
 *
 * <p>Callers charge bytes they can measure: HPPC {@code ramBytesAllocated} deltas on growable
 * arrays, and the copied terms-key payloads. The total is a floor, not a JVM heap sample. One
 * instance is shared across derived {@link AggregationContext}s so every search slice of a
 * partition search contributes.
 */
public final class AggregationHeapAccountant {

  private static final AggregationHeapAccountant DISABLED = new AggregationHeapAccountant(false);

  private final AtomicLong accountedBytes = new AtomicLong();
  private final boolean recording;

  public AggregationHeapAccountant() {
    this(true);
  }

  private AggregationHeapAccountant(boolean recording) {
    this.recording = recording;
  }

  /** Shared no-op for tests and callers that do not record the process histogram. */
  public static AggregationHeapAccountant disabled() {
    return DISABLED;
  }

  public void add(long deltaBytes) {
    if (this.recording) {
      this.accountedBytes.addAndGet(deltaBytes);
    }
  }

  public long currentBytes() {
    return this.accountedBytes.get();
  }
}
