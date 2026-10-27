package com.xgen.mongot.index.query.aggregation;

import com.carrotsearch.hppc.LongLongHashMap;
import com.carrotsearch.hppc.cursors.LongLongCursor;
import java.util.BitSet;
import java.util.Objects;
import java.util.function.LongConsumer;
import org.jetbrains.annotations.Nullable;

/**
 * Per-bucket {@code long} state keyed by bucket ordinal, sparse until occupancy justifies dense.
 *
 * <p>At a dense root terms level the ordinal is the reader-wide global ordinal. A selective query
 * touches a tiny fraction of that space, and a growable array still pays for the whole range:
 * growth copies, zero-fill, and a scan from {@code 0} to {@code valueCount}. This store starts as
 * an ordinal-keyed hash map and migrates once to a dense array plus a presence bitset when the
 * entry count is at least {@value #DEFAULT_SPILL_MIN_ENTRIES} and occupancy is at least 25%. A
 * wrong guess costs one pass over the map.
 *
 * <p>Keys are the caller's ordinals verbatim, so slices built from the same spec share a keyspace
 * and fold with {@link #addAll}. Zero is a stored value; absence is a missing key, not a zero slot.
 * Not thread-safe.
 */
final class BucketLongStore implements AutoCloseable {
  /**
   * Minimum entries before dense migration is considered. Below this, hash overhead does not matter
   * even when the map is fully dense.
   */
  static final int DEFAULT_SPILL_MIN_ENTRIES = 1 << 16;

  /** One present (ordinal, value) entry. */
  interface OrdValueConsumer {
    void accept(long ord, long value);
  }

  /** Dense representation. Presence is separate because 0 is a legitimate stored value. */
  private static final class Dense {
    final LongArray values;
    final BitSet present;
    int entries;

    Dense(int range, int entries, LongConsumer ramDeltaBytes) {
      this.values = new LongArray(range, ramDeltaBytes);
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

  // Exactly one representation is live: sparse until migration, then dense forever after.
  @Nullable private LongLongHashMap sparse;
  @Nullable private Dense dense;
  private long maxOrd = -1;

  BucketLongStore() {
    this(DEFAULT_SPILL_MIN_ENTRIES, unused -> {}, () -> {});
  }

  BucketLongStore(int spillMinEntries) {
    this(spillMinEntries, unused -> {}, () -> {});
  }

  /**
   * {@code ramDeltaBytes} is charged for sparse-map growth. A spill drops that charge and charges
   * the dense array instead.
   */
  BucketLongStore(LongConsumer ramDeltaBytes) {
    this(DEFAULT_SPILL_MIN_ENTRIES, ramDeltaBytes, () -> {});
  }

  /** {@code onSpill} runs once, when this store migrates to the dense array. */
  BucketLongStore(Runnable onSpill) {
    this(DEFAULT_SPILL_MIN_ENTRIES, unused -> {}, onSpill);
  }

  BucketLongStore(int spillMinEntries, Runnable onSpill) {
    this(spillMinEntries, unused -> {}, onSpill);
  }

  BucketLongStore(LongConsumer ramDeltaBytes, Runnable onSpill) {
    this(DEFAULT_SPILL_MIN_ENTRIES, ramDeltaBytes, onSpill);
  }

  private BucketLongStore(int spillMinEntries, LongConsumer ramDeltaBytes, Runnable onSpill) {
    this.spillMinEntries = spillMinEntries;
    this.ramDeltaBytes = ramDeltaBytes;
    this.onSpill = onSpill;
    this.sparse = new LongLongHashMap();
  }

  /** The stored value for {@code ord}, or {@code orElse} if the bucket was never written. */
  long get(long ord, long orElse) {
    LongLongHashMap sparse = this.sparse;
    if (sparse != null) {
      return sparse.getOrDefault(ord, orElse);
    }
    Dense dense = Objects.requireNonNull(this.dense);
    int index = Math.toIntExact(ord);
    return dense.present.get(index) ? dense.values.get(index) : orElse;
  }

  /** Whether {@code ord} has ever been written. */
  boolean has(long ord) {
    LongLongHashMap sparse = this.sparse;
    if (sparse != null) {
      return sparse.containsKey(ord);
    }
    return Objects.requireNonNull(this.dense).present.get(Math.toIntExact(ord));
  }

  /** Set {@code ord} to {@code value}, marking it present. */
  void put(long ord, long value) {
    LongLongHashMap sparse = this.sparse;
    if (sparse != null) {
      sparse.put(ord, value);
      trackAndMaybeSpill(sparse, ord);
      return;
    }
    writeDense(Objects.requireNonNull(this.dense), ord, value, false);
  }

  /** Add {@code value} to {@code ord}. An absent bucket starts at 0 and becomes present. */
  void add(long ord, long value) {
    LongLongHashMap sparse = this.sparse;
    if (sparse != null) {
      sparse.putOrAdd(ord, value, value);
      trackAndMaybeSpill(sparse, ord);
      return;
    }
    writeDense(Objects.requireNonNull(this.dense), ord, value, true);
  }

  /**
   * Whether {@link #forEach} visits entries in ascending ordinal order. True after the store has
   * spilled to the dense array.
   */
  boolean iteratesInOrdinalOrder() {
    return this.sparse == null;
  }

  /** Number of present buckets. */
  int size() {
    LongLongHashMap sparse = this.sparse;
    return sparse != null ? sparse.size() : Objects.requireNonNull(this.dense).entries;
  }

  /**
   * Visit every present entry. Dense mode visits in ascending ordinal order. Sparse mode visits in
   * hash order, so callers that need ordinal order must sort.
   */
  void forEach(OrdValueConsumer consumer) {
    LongLongHashMap sparse = this.sparse;
    if (sparse != null) {
      for (LongLongCursor cursor : sparse) {
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
  void addAll(BucketLongStore other) {
    other.forEach(this::add);
  }

  private static void writeDense(Dense dense, long ord, long value, boolean increment) {
    int index = Math.toIntExact(ord);
    if (!dense.present.get(index)) {
      dense.present.set(index);
      dense.entries++;
    }
    if (increment) {
      dense.values.increment(index, value);
    } else {
      dense.values.set(index, value);
    }
  }

  private void trackAndMaybeSpill(LongLongHashMap sparse, long ord) {
    if (ord > this.maxOrd) {
      this.maxOrd = ord;
    }
    chargeSparseGrowth(sparse.ramBytesAllocated());
    // Occupancy >= 25% means the dense array is at most 4x the entries actually stored.
    if (sparse.size() >= this.spillMinEntries && sparse.size() * 4L >= this.maxOrd + 1) {
      Dense dense = new Dense(Math.toIntExact(this.maxOrd + 1), sparse.size(), this.ramDeltaBytes);
      for (LongLongCursor cursor : sparse) {
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
    LongLongHashMap sparse = this.sparse;
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
