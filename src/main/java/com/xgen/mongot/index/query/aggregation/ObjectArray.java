package com.xgen.mongot.index.query.aggregation;

import com.carrotsearch.hppc.ObjectArrayList;
import java.util.Optional;

/**
 * Growable object array backed by HPPC's {@link ObjectArrayList}.
 *
 * <p>Thin adapter with the same auto-grow-on-write semantics as {@link LongArray} and {@link
 * DoubleArray}: setting at an index beyond the current size grows the underlying storage.
 *
 * <p>Unlike those two there is no zero to fill new slots with, so {@link #get} reports an unset
 * slot as empty rather than handing back the {@code null} HPPC leaves there.
 */
public class ObjectArray<T> implements AutoCloseable {
  private final ObjectArrayList<T> list;

  public ObjectArray(int initialCapacity) {
    this.list = new ObjectArrayList<>(initialCapacity);
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
    }
    this.list.set(index, value);
  }

  @Override
  public void close() {
    this.list.release();
  }
}
