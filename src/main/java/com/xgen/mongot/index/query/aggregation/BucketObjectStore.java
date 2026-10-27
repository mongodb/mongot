package com.xgen.mongot.index.query.aggregation;

import com.carrotsearch.hppc.LongObjectHashMap;
import com.carrotsearch.hppc.cursors.LongObjectCursor;
import java.util.BitSet;
import java.util.Objects;
import java.util.Optional;
import java.util.function.LongConsumer;
import org.jetbrains.annotations.Nullable;

/**
 * Per-bucket object state keyed by bucket ordinal, sparse until occupancy justifies dense.
 *
 * <p>Used for min/max extrema, where an unset slot must stay distinguishable from a stored value.
 * See {@link BucketLongStore} for the spill rule and the shared-ordinal merge contract.
 */
final class BucketObjectStore<T> implements AutoCloseable {

  /** One present (ordinal, value) entry. */
  interface OrdValueConsumer<T> {
    void accept(long ord, T value);
  }

  /** Dense representation. Presence is separate because an unset slot is not a stored extremum. */
  private static final class Dense<T> {
    final ObjectArray<T> values;
    final BitSet present;

    Dense(int range, LongConsumer ramDeltaBytes) {
      this.values = new ObjectArray<>(range, ramDeltaBytes);
      this.present = new BitSet(range);
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

  @Nullable private LongObjectHashMap<T> sparse;
  @Nullable private Dense<T> dense;
  private int entries;
  private long maxOrd = -1;

  BucketObjectStore() {
    this(BucketLongStore.DEFAULT_SPILL_MIN_ENTRIES, unused -> {}, () -> {});
  }

  BucketObjectStore(int spillMinEntries) {
    this(spillMinEntries, unused -> {}, () -> {});
  }

  /**
   * {@code ramDeltaBytes} is charged for sparse-map growth. A spill drops that charge and charges
   * the dense array instead.
   */
  BucketObjectStore(LongConsumer ramDeltaBytes) {
    this(BucketLongStore.DEFAULT_SPILL_MIN_ENTRIES, ramDeltaBytes, () -> {});
  }

  /** {@code onSpill} runs once, when this store migrates to the dense array. */
  BucketObjectStore(Runnable onSpill) {
    this(BucketLongStore.DEFAULT_SPILL_MIN_ENTRIES, unused -> {}, onSpill);
  }

  BucketObjectStore(int spillMinEntries, Runnable onSpill) {
    this(spillMinEntries, unused -> {}, onSpill);
  }

  BucketObjectStore(LongConsumer ramDeltaBytes, Runnable onSpill) {
    this(BucketLongStore.DEFAULT_SPILL_MIN_ENTRIES, ramDeltaBytes, onSpill);
  }

  private BucketObjectStore(int spillMinEntries, LongConsumer ramDeltaBytes, Runnable onSpill) {
    this.spillMinEntries = spillMinEntries;
    this.ramDeltaBytes = ramDeltaBytes;
    this.onSpill = onSpill;
    this.sparse = new LongObjectHashMap<>();
  }

  /** The stored value, or empty if the bucket was never written. */
  Optional<T> get(long ord) {
    LongObjectHashMap<T> sparse = this.sparse;
    if (sparse != null) {
      return Optional.ofNullable(sparse.get(ord));
    }
    Dense<T> dense = Objects.requireNonNull(this.dense);
    int index = Math.toIntExact(ord);
    return dense.present.get(index) ? dense.values.get(index) : Optional.empty();
  }

  /** Set {@code ord} to {@code value}. {@code value} must be non-null. */
  void put(long ord, T value) {
    LongObjectHashMap<T> sparse = this.sparse;
    if (sparse != null) {
      if (!sparse.containsKey(ord)) {
        this.entries++;
      }
      sparse.put(ord, value);
      trackAndMaybeSpill(sparse, ord);
      return;
    }
    Dense<T> dense = Objects.requireNonNull(this.dense);
    int index = Math.toIntExact(ord);
    if (!dense.present.get(index)) {
      dense.present.set(index);
      this.entries++;
    }
    dense.values.set(index, value);
  }

  /** Number of present buckets. */
  int size() {
    return this.entries;
  }

  /**
   * Visit every present entry. Dense mode visits in ascending ordinal order. Sparse mode visits in
   * hash order.
   */
  void forEach(OrdValueConsumer<T> consumer) {
    LongObjectHashMap<T> sparse = this.sparse;
    if (sparse != null) {
      for (LongObjectCursor<T> cursor : sparse) {
        consumer.accept(cursor.key, cursor.value);
      }
      return;
    }
    Dense<T> dense = Objects.requireNonNull(this.dense);
    for (int ord = dense.present.nextSetBit(0); ord >= 0; ord = dense.present.nextSetBit(ord + 1)) {
      consumer.accept(ord, dense.values.get(ord).orElseThrow());
    }
  }

  private void trackAndMaybeSpill(LongObjectHashMap<T> sparse, long ord) {
    if (ord > this.maxOrd) {
      this.maxOrd = ord;
    }
    chargeSparseGrowth(sparse.ramBytesAllocated());
    if (sparse.size() >= this.spillMinEntries && sparse.size() * 4L >= this.maxOrd + 1) {
      Dense<T> dense = new Dense<>(Math.toIntExact(this.maxOrd + 1), this.ramDeltaBytes);
      for (LongObjectCursor<T> cursor : sparse) {
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
    LongObjectHashMap<T> sparse = this.sparse;
    if (sparse != null) {
      sparse.release();
      this.sparse = null;
    }
    Dense<T> dense = this.dense;
    if (dense != null) {
      dense.values.close();
      this.dense = null;
    }
  }
}
