package com.xgen.mongot.index.lucene.query.sort.comparator;

import com.google.errorprone.annotations.concurrent.GuardedBy;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Timer;
import java.io.IOException;
import java.time.Duration;
import java.util.Map;
import java.util.WeakHashMap;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.join.BitSetProducer;
import org.apache.lucene.util.BitSet;

/**
 * Wraps the child {@link BitSetProducer} of an embeddedDoc-scoped sort to record what the scope
 * costs: how long each segment's child bitset takes to materialize, and how many matching children
 * it holds.
 *
 * <p>The delegate must be a fresh, per-scoped-key caching producer such as {@link
 * org.apache.lucene.search.join.QueryBitSetProducer}. Successful materialization is measured once
 * per segment. Same-segment calls are serialized so a concurrent cache miss cannot be mistaken for
 * a hit. Failed attempts leave the segment eligible for measurement on retry. Different segments
 * can still evaluate concurrently.
 *
 * <p>Cardinality counts candidates for the entire segment, not children actually visited by the
 * reductions. Parent-query selectivity and repeated per-type reductions affect actual scan work.
 *
 * <p>Segments are tracked by core cache key, the same granularity the delegate caches at. A segment
 * that reports no cache helper is never cached by the delegate, so each call there is a real
 * evaluation and is recorded.
 */
public class InstrumentedChildBitSetProducer implements BitSetProducer {
  private final BitSetProducer delegate;
  private final Timer evaluationLatency;
  private final DistributionSummary matchingChildren;

  /**
   * Per-segment measurement state. Values do not retain keys or readers, allowing weak keys to be
   * collected when segments close.
   */
  @GuardedBy("measurements")
  private final Map<IndexReader.CacheKey, SegmentMeasurement> measurements = new WeakHashMap<>();

  private static final class SegmentMeasurement {
    @GuardedBy("this")
    private boolean recorded;
  }

  public InstrumentedChildBitSetProducer(
      BitSetProducer delegate, Timer evaluationLatency, DistributionSummary matchingChildren) {
    this.delegate = delegate;
    this.evaluationLatency = evaluationLatency;
    this.matchingChildren = matchingChildren;
  }

  @Override
  public BitSet getBitSet(LeafReaderContext context) throws IOException {
    IndexReader.CacheHelper cacheHelper = context.reader().getCoreCacheHelper();
    if (cacheHelper == null) {
      return evaluate(context);
    }

    SegmentMeasurement measurement;
    synchronized (this.measurements) {
      measurement =
          this.measurements.computeIfAbsent(
              cacheHelper.getKey(), ignored -> new SegmentMeasurement());
    }
    synchronized (measurement) {
      if (measurement.recorded) {
        return this.delegate.getBitSet(context);
      }
      BitSet result = evaluate(context);
      measurement.recorded = true;
      return result;
    }
  }

  private BitSet evaluate(LeafReaderContext context) throws IOException {
    long startNanos = System.nanoTime();
    BitSet bitSet = this.delegate.getBitSet(context);
    this.evaluationLatency.record(Duration.ofNanos(System.nanoTime() - startNanos));
    // A segment with no matching children yields null rather than an empty set: zero candidates.
    this.matchingChildren.record(bitSet == null ? 0 : bitSet.cardinality());
    return bitSet;
  }
}
