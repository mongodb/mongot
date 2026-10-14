package com.xgen.mongot.index.query.aggregation;

import com.carrotsearch.hppc.LongArrayList;
import java.util.function.LongConsumer;

/**
 * Growable long array backed by HPPC's {@link LongArrayList}.
 *
 * <p>This is a thin adapter that preserves auto-grow-on-write semantics: setting or incrementing at
 * an index beyond the current size grows the underlying storage and zero-fills any new slots.
 */
public class LongArray implements AutoCloseable {
  private final LongArrayList list;
  private final LongConsumer ramDeltaBytes;
  private long lastObservedRamBytesAllocated;

  public LongArray(int initialCapacity) {
    this(initialCapacity, unused -> {});
  }

  /**
   * Same as {@link #LongArray(int)}, reporting HPPC {@link LongArrayList#ramBytesAllocated()}
   * growth to {@code ramDeltaBytes}. Charge allocated ram, not {@link #size}, so a sparse write at
   * a high ordinal includes the zero-filled slack and the list's own overhead.
   */
  public LongArray(int initialCapacity, LongConsumer ramDeltaBytes) {
    this.list = new LongArrayList(initialCapacity);
    this.ramDeltaBytes = ramDeltaBytes;
    this.lastObservedRamBytesAllocated = 0;
    accountRam();
  }

  private void accountRam() {
    long allocated = this.list.ramBytesAllocated();
    if (allocated > this.lastObservedRamBytesAllocated) {
      this.ramDeltaBytes.accept(allocated - this.lastObservedRamBytesAllocated);
      this.lastObservedRamBytesAllocated = allocated;
    }
  }

  public int size() {
    return this.list.size();
  }

  public long get(int index) {
    return this.list.get(index);
  }

  public void set(int index, long value) {
    if (index >= this.list.size()) {
      this.list.resize(index + 1);
      accountRam();
    }
    this.list.set(index, value);
  }

  public void increment(int index, long value) {
    if (index >= this.list.size()) {
      this.list.resize(index + 1);
      accountRam();
    }
    this.list.set(index, this.list.get(index) + value);
  }

  /** Element-wise add {@code other} into this array, growing to {@code other}'s size if needed. */
  public void add(LongArray other) {
    for (int i = 0; i < other.size(); i++) {
      this.increment(i, other.get(i));
    }
  }

  public static LongArray from(long... values) {
    LongArray array = new LongArray(values.length);
    for (int i = 0; i < values.length; i++) {
      array.set(i, values[i]);
    }
    return array;
  }

  @Override
  public void close() {
    this.list.release();
  }
}
