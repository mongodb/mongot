package com.xgen.mongot.index.query.aggregation;

import com.carrotsearch.hppc.LongArrayList;

/**
 * Growable long array backed by HPPC's {@link LongArrayList}.
 *
 * <p>This is a thin adapter that preserves auto-grow-on-write semantics: setting or incrementing
 * at an index beyond the current size grows the underlying storage and zero-fills any new slots.
 */
public class LongArray implements AutoCloseable {
  private final LongArrayList list;

  public LongArray(int initialCapacity) {
    this.list = new LongArrayList(initialCapacity);
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
    }
    this.list.set(index, value);
  }

  public void increment(int index, long value) {
    if (index >= this.list.size()) {
      this.list.resize(index + 1);
    }
    this.list.set(index, this.list.get(index) + value);
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
