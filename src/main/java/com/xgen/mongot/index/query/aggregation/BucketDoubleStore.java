package com.xgen.mongot.index.query.aggregation;

import com.carrotsearch.hppc.LongDoubleHashMap;
import com.carrotsearch.hppc.cursors.LongDoubleCursor;
import java.util.BitSet;
import java.util.Objects;
import java.util.function.LongConsumer;
import org.jetbrains.annotations.Nullable;

/**
 * Per-bucket {@code double} state keyed by bucket ordinal, sparse until occupancy justifies dense.
 * The {@code double} counterpart of {@link BucketLongStore}.
 *
 * <p>The dense array zero-fills gaps, and {@code 0.0} is a legitimate stored sum, so a presence
 * bitset distinguishes a written bucket from one that was never touched.
 */
final class BucketDoubleStore implements AutoCloseable {

  /** One present (ordinal, value) entry. */
  interface OrdValueConsumer {
    void accept(long ord, double value);
  }

  private static final class Dense {
    final DoubleArray values;
    final BitSet present;
    int entries;

    Dense(int range, int entries, LongConsumer ramDeltaBytes) {
      this.values = new DoubleArray(range, ramDeltaBytes);
      this.present = new BitSet(range);
      this.entries = entries;
    }
  }

  private final int spillMinEntries;
  private final LongConsumer ramDeltaBytes;
  private final Runnable onSpill;

  /**
   * Sparse-map bytes already reported to {@link #ramDeltaBytes}. Replaced by the dense array on
   * spill.
   */
  private long chargedSparseBytes;

  @Nullable private LongDoubleHashMap sparse;
  @Nullable private Dense dense;
  private long maxOrd = -1;

  BucketDoubleStore() {
    this(BucketLongStore.DEFAULT_SPILL_MIN_ENTRIES, unused -> {}, () -> {});
  }

  BucketDoubleStore(int spillMinEntries) {
    this(spillMinEntries, unused -> {}, () -> {});
  }

  /**
   * {@code ramDeltaBytes} is charged for sparse-map growth. A spill drops that charge and charges
   * the dense array instead.
   */
  BucketDoubleStore(LongConsumer ramDeltaBytes) {
    this(BucketLongStore.DEFAULT_SPILL_MIN_ENTRIES, ramDeltaBytes, () -> {});
  }

  /** {@code onSpill} runs once, when this store migrates to the dense array. */
  BucketDoubleStore(Runnable onSpill) {
    this(BucketLongStore.DEFAULT_SPILL_MIN_ENTRIES, unused -> {}, onSpill);
  }

  BucketDoubleStore(int spillMinEntries, Runnable onSpill) {
    this(spillMinEntries, unused -> {}, onSpill);
  }

  BucketDoubleStore(LongConsumer ramDeltaBytes, Runnable onSpill) {
    this(BucketLongStore.DEFAULT_SPILL_MIN_ENTRIES, ramDeltaBytes, onSpill);
  }

  private BucketDoubleStore(int spillMinEntries, LongConsumer ramDeltaBytes, Runnable onSpill) {
    this.spillMinEntries = spillMinEntries;
    this.ramDeltaBytes = ramDeltaBytes;
    this.onSpill = onSpill;
    this.sparse = new LongDoubleHashMap();
  }

  /** The stored value for {@code ord}, or {@code orElse} if the bucket was never written. */
  double get(long ord, double orElse) {
    LongDoubleHashMap sparse = this.sparse;
    if (sparse != null) {
      return sparse.getOrDefault(ord, orElse);
    }
    Dense dense = Objects.requireNonNull(this.dense);
    int index = Math.toIntExact(ord);
    return dense.present.get(index) ? dense.values.get(index) : orElse;
  }

  /** Add {@code value} to {@code ord}. An absent bucket starts at 0 and becomes present. */
  void add(long ord, double value) {
    LongDoubleHashMap sparse = this.sparse;
    if (sparse != null) {
      sparse.putOrAdd(ord, value, value);
      trackAndMaybeSpill(sparse, ord);
      return;
    }
    Dense dense = Objects.requireNonNull(this.dense);
    int index = Math.toIntExact(ord);
    if (!dense.present.get(index)) {
      dense.present.set(index);
      dense.entries++;
    }
    dense.values.increment(index, value);
  }

  /** Number of present buckets. */
  int size() {
    LongDoubleHashMap sparse = this.sparse;
    return sparse != null ? sparse.size() : Objects.requireNonNull(this.dense).entries;
  }

  /**
   * Visit every present entry. Dense mode visits in ascending ordinal order. Sparse mode visits in
   * hash order.
   */
  void forEach(OrdValueConsumer consumer) {
    LongDoubleHashMap sparse = this.sparse;
    if (sparse != null) {
      for (LongDoubleCursor cursor : sparse) {
        consumer.accept(cursor.key, cursor.value);
      }
      return;
    }
    Dense dense = Objects.requireNonNull(this.dense);
    for (int ord = dense.present.nextSetBit(0); ord >= 0; ord = dense.present.nextSetBit(ord + 1)) {
      consumer.accept(ord, dense.values.get(ord));
    }
  }

  /** Add every entry of {@code other} into this store. Both stores share a bucket keyspace. */
  void addAll(BucketDoubleStore other) {
    other.forEach(this::add);
  }

  private void trackAndMaybeSpill(LongDoubleHashMap sparse, long ord) {
    if (ord > this.maxOrd) {
      this.maxOrd = ord;
    }
    chargeSparseGrowth(sparse.ramBytesAllocated());
    if (sparse.size() >= this.spillMinEntries && sparse.size() * 4L >= this.maxOrd + 1) {
      Dense dense = new Dense(Math.toIntExact(this.maxOrd + 1), sparse.size(), this.ramDeltaBytes);
      for (LongDoubleCursor cursor : sparse) {
        int index = Math.toIntExact(cursor.key);
        dense.values.set(index, cursor.value);
        dense.present.set(index);
      }
      this.dense = dense;
      sparse.release();
      this.sparse = null;
      unchargeSparse();
      this.onSpill.run();
    }
  }

  /** Charges newly allocated sparse-map bytes. A query that never spills still reports heap. */
  private void chargeSparseGrowth(long allocated) {
    if (allocated > this.chargedSparseBytes) {
      this.ramDeltaBytes.accept(allocated - this.chargedSparseBytes);
      this.chargedSparseBytes = allocated;
    }
  }

  /** Drops the sparse-map charge once the map has been released and the dense array is charged. */
  private void unchargeSparse() {
    if (this.chargedSparseBytes > 0) {
      this.ramDeltaBytes.accept(-this.chargedSparseBytes);
      this.chargedSparseBytes = 0;
    }
  }

  /** Drops the backing buffer. Safe to call more than once. Results must already have been read. */
  @Override
  public void close() {
    LongDoubleHashMap sparse = this.sparse;
    if (sparse != null) {
      sparse.release();
      this.sparse = null;
    }
    Dense dense = this.dense;
    if (dense != null) {
      dense.values.close();
      this.dense = null;
    }
  }
}
