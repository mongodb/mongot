package com.xgen.mongot.index.query.aggregation;

import com.carrotsearch.hppc.ObjectArrayList;
import java.util.Optional;
import java.util.function.LongConsumer;

/**
 * Growable object array backed by HPPC's {@link ObjectArrayList}.
 *
 * <p>Thin adapter with the same auto-grow-on-write semantics as {@link LongArray} and {@link
 * DoubleArray}: setting at an index beyond the current size grows the underlying storage.
 *
 * <p>Unlike those two there is no zero to fill new slots with, so {@link #get} reports an unset
 * slot as empty rather than handing back the {@code null} HPPC leaves there.
 *
 * <p>Heap accounting charges {@link ObjectArrayList#ramBytesAllocated()} only: the list's own
 * buffer of references, not a walk of the pointed-to values. That is a cheap floor rather than a
 * deep size.
 */
public class ObjectArray<T> implements AutoCloseable {
  private final ObjectArrayList<T> list;
  private final LongConsumer ramDeltaBytes;
  private long lastObservedRamBytesAllocated;

  public ObjectArray(int initialCapacity) {
    this(initialCapacity, unused -> {});
  }

  /**
   * Same as {@link #ObjectArray(int)}, reporting HPPC {@link ObjectArrayList#ramBytesAllocated()}
   * growth to {@code ramDeltaBytes}.
   */
  public ObjectArray(int initialCapacity, LongConsumer ramDeltaBytes) {
    this.list = new ObjectArrayList<>(initialCapacity);
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

  /** The value at {@code index}, or empty if it was never set. */
  public Optional<T> get(int index) {
    return index < this.list.size() ? Optional.ofNullable(this.list.get(index)) : Optional.empty();
  }

  public void set(int index, T value) {
    if (index >= this.list.size()) {
      this.list.resize(index + 1);
      accountRam();
    }
    this.list.set(index, value);
  }

  @Override
  public void close() {
    this.list.release();
  }
}
