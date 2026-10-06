package com.xgen.mongot.index.query.aggregation;

import com.carrotsearch.hppc.DoubleArrayList;

/**
 * Growable double array backed by HPPC's {@link DoubleArrayList}.
 *
 * <p>This is a thin adapter that preserves auto-grow-on-write semantics: setting or incrementing at
 * an index beyond the current size grows the underlying storage and zero-fills any new slots.
 */
public class DoubleArray implements AutoCloseable {
  private final DoubleArrayList list;

  public DoubleArray(int initialCapacity) {
    this.list = new DoubleArrayList(initialCapacity);
  }

  public int size() {
    return this.list.size();
  }

  public double get(int index) {
    return this.list.get(index);
  }

  public void set(int index, double value) {
    if (index >= this.list.size()) {
      this.list.resize(index + 1);
    }
    this.list.set(index, value);
  }

  public void increment(int index, double value) {
    if (index >= this.list.size()) {
      this.list.resize(index + 1);
    }
    this.list.set(index, this.list.get(index) + value);
  }

  /** Element-wise add {@code other} into this array, growing to {@code other}'s size if needed. */
  public void add(DoubleArray other) {
    for (int i = 0; i < other.size(); i++) {
      this.increment(i, other.get(i));
    }
  }

  public static DoubleArray from(double... values) {
    DoubleArray array = new DoubleArray(values.length);
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
