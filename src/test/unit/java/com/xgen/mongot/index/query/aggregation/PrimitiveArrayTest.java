package com.xgen.mongot.index.query.aggregation;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import org.junit.Test;

/** Basic coverage for {@link LongArray} and {@link DoubleArray}. */
public class PrimitiveArrayTest {

  @Test
  public void longArrayGetSet() {
    LongArray arr = new LongArray(4);
    arr.set(0, 10L);
    arr.set(1, 20L);
    arr.set(2, 30L);
    assertEquals(3, arr.size());
    assertEquals(10L, arr.get(0));
    assertEquals(20L, arr.get(1));
    assertEquals(30L, arr.get(2));
  }

  @Test
  public void longArrayIncrement() {
    LongArray arr = new LongArray(4);
    arr.increment(0, 5L);
    arr.increment(0, 3L);
    arr.increment(2, 7L);
    assertEquals(3, arr.size());
    assertEquals(8L, arr.get(0));
    assertEquals(0L, arr.get(1));
    assertEquals(7L, arr.get(2));
  }

  @Test
  public void longArrayAutoGrows() {
    LongArray arr = new LongArray(2);
    arr.set(0, 1L);
    arr.set(100, 999L); // forces growth well beyond initial capacity
    assertEquals(101, arr.size());
    assertEquals(999L, arr.get(100));
    assertEquals(1L, arr.get(0));
    assertEquals(0L, arr.get(50));
  }

  @Test
  public void longArrayIncrementAutoGrows() {
    LongArray arr = new LongArray(1);
    arr.increment(50, 5L);
    assertEquals(51, arr.size());
    assertEquals(5L, arr.get(50));
  }

  @Test
  public void longArrayGetOutOfBoundsThrows() {
    LongArray arr = new LongArray(8);
    arr.set(0, 1L);
    // HPPC bounds-checks via Java assertions; test JVM runs with -ea.
    assertThrows(AssertionError.class, () -> arr.get(5));
    assertThrows(AssertionError.class, () -> arr.get(-1));
  }

  @Test
  public void longArrayRejectsNegativeIndexAndCapacity() {
    // HPPC's constructor uses Arrays.copyOf, which throws NegativeArraySizeException.
    assertThrows(NegativeArraySizeException.class, () -> new LongArray(-1));
    LongArray arr = new LongArray(4);
    assertThrows(AssertionError.class, () -> arr.set(-1, 1L));
    assertThrows(AssertionError.class, () -> arr.increment(-1, 1L));
  }

  @Test
  public void longArrayAddGrowsToTheOtherSide() {
    LongArray left = LongArray.from(1L, 2L);
    LongArray right = new LongArray(4);
    right.set(3, 5L);
    left.add(right);
    assertEquals(4, left.size());
    assertEquals(1L, left.get(0));
    assertEquals(2L, left.get(1));
    assertEquals(0L, left.get(2));
    assertEquals(5L, left.get(3));
  }

  @Test
  public void longArrayFromFactory() {
    LongArray arr = LongArray.from(1L, 2L, 3L, 4L);
    assertEquals(4, arr.size());
    assertEquals(1L, arr.get(0));
    assertEquals(4L, arr.get(3));
  }

  @Test
  public void longArrayCloseResets() {
    LongArray arr = new LongArray(4);
    arr.set(0, 42L);
    arr.close();
    assertEquals(0, arr.size());
  }

  @Test
  public void doubleArrayGetSet() {
    DoubleArray arr = new DoubleArray(4);
    arr.set(0, 1.5);
    arr.set(1, 2.5);
    assertEquals(2, arr.size());
    assertEquals(1.5, arr.get(0), 0.0);
    assertEquals(2.5, arr.get(1), 0.0);
  }

  @Test
  public void doubleArrayAddSumsByIndex() {
    DoubleArray left = DoubleArray.from(1.5, 2.0);
    DoubleArray right = DoubleArray.from(0.5, 1.0, 3.0);
    left.add(right);
    assertEquals(3, left.size());
    assertEquals(2.0, left.get(0), 0.0);
    assertEquals(3.0, left.get(1), 0.0);
    assertEquals(3.0, left.get(2), 0.0);
  }

  @Test
  public void doubleArrayIncrement() {
    DoubleArray arr = new DoubleArray(4);
    arr.increment(0, 1.5);
    arr.increment(0, 2.25);
    arr.increment(3, 0.5);
    assertEquals(4, arr.size());
    assertEquals(3.75, arr.get(0), 0.0001);
    assertEquals(0.0, arr.get(1), 0.0);
    assertEquals(0.0, arr.get(2), 0.0);
    assertEquals(0.5, arr.get(3), 0.0001);
  }

  @Test
  public void doubleArrayAutoGrows() {
    DoubleArray arr = new DoubleArray(2);
    arr.set(0, 1.1);
    arr.set(100, 99.9);
    assertEquals(101, arr.size());
    assertEquals(99.9, arr.get(100), 0.0);
    assertEquals(1.1, arr.get(0), 0.0);
  }

  @Test
  public void doubleArrayGetOutOfBoundsThrows() {
    DoubleArray arr = new DoubleArray(8);
    arr.set(0, 1.0);
    // HPPC bounds-checks via Java assertions; test JVM runs with -ea.
    assertThrows(AssertionError.class, () -> arr.get(5));
    assertThrows(AssertionError.class, () -> arr.get(-1));
  }

  @Test
  public void doubleArrayRejectsNegativeIndexAndCapacity() {
    // HPPC's constructor uses Arrays.copyOf, which throws NegativeArraySizeException.
    assertThrows(NegativeArraySizeException.class, () -> new DoubleArray(-1));
    DoubleArray arr = new DoubleArray(4);
    assertThrows(AssertionError.class, () -> arr.set(-1, 1.0));
    assertThrows(AssertionError.class, () -> arr.increment(-1, 1.0));
  }

  @Test
  public void doubleArrayFromFactory() {
    DoubleArray arr = DoubleArray.from(1.1, 2.2, 3.3);
    assertEquals(3, arr.size());
    assertEquals(1.1, arr.get(0), 0.0001);
    assertEquals(3.3, arr.get(2), 0.0001);
  }

  @Test
  public void doubleArrayCloseResets() {
    DoubleArray arr = new DoubleArray(4);
    arr.set(0, 1.0);
    arr.close();
    assertEquals(0, arr.size());
  }
}
