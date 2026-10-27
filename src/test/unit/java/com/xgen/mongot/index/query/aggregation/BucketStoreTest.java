package com.xgen.mongot.index.query.aggregation;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import org.bson.BsonInt32;
import org.bson.BsonValue;
import org.junit.Test;

/**
 * Exercises the bucket stores, in particular the sparse-to-dense migration: values, presence, size,
 * and iteration must be indistinguishable across the spill.
 */
public class BucketStoreTest {

  // Spill requires both size >= threshold and occupancy >= 25% (size * 4 >= maxOrd + 1).
  private static final int SPILL_MIN = 8;

  @Test
  public void longStore_getPutAddHas_sparseMode() {
    BucketLongStore store = new BucketLongStore();

    store.add(1_000_000L, 5);
    store.add(1_000_000L, 2);
    store.put(3L, 0);

    assertEquals(7, store.get(1_000_000L, -1));
    assertEquals(0, store.get(3L, -1));
    assertTrue(store.has(3L));
    assertFalse(store.has(4L));
    assertEquals(-1, store.get(4L, -1));
    assertEquals(2, store.size());
  }

  @Test
  public void longStore_denseOrdinals_spillsAndStaysConsistent() {
    BucketLongStore store = new BucketLongStore(SPILL_MIN);

    // 32 consecutive ordinals cross the entry threshold at full occupancy.
    for (long ord = 0; ord < 32; ord++) {
      store.add(ord, ord + 1);
    }

    assertEquals(32, store.size());
    assertTrue(store.iteratesInOrdinalOrder());
    for (long ord = 0; ord < 32; ord++) {
      assertEquals(ord + 1, store.get(ord, -1));
      assertTrue(store.has(ord));
    }
    assertFalse(store.has(32L));
    store.add(2L, 10);
    store.put(40L, 7);
    assertEquals(13, store.get(2L, -1));
    assertEquals(7, store.get(40L, -1));
    assertEquals(33, store.size());
  }

  @Test
  public void longStore_sparseOrdinals_neverSpills() {
    BucketLongStore store = new BucketLongStore(SPILL_MIN);

    // Entry count crosses the threshold but occupancy stays far below 25%.
    for (long ord = 0; ord < 64; ord++) {
      store.add(ord * 1_000_000L, ord);
    }

    assertEquals(64, store.size());
    assertFalse(store.iteratesInOrdinalOrder());
    assertEquals(63, store.get(63_000_000L, -1));
    assertFalse(store.has(1L));
  }

  @Test
  public void longStore_forEach_visitsEveryEntryOnceAcrossSpill() {
    BucketLongStore store = new BucketLongStore(SPILL_MIN);
    Random random = new Random(42);
    Map<Long, Long> expected = new HashMap<>();
    for (int i = 0; i < 1000; i++) {
      long ord = random.nextInt(2000);
      store.add(ord, 1);
      expected.merge(ord, 1L, Long::sum);
    }

    Map<Long, Long> seen = new HashMap<>();
    store.forEach((ord, value) -> seen.merge(ord, value, (a, b) -> Long.MIN_VALUE));

    // A second visit of the same ordinal would replace the sum with Long.MIN_VALUE.
    assertEquals(expected, seen);
  }

  @Test
  public void longStore_addAll_mergesAcrossMixedRepresentations() {
    BucketLongStore dense = new BucketLongStore(SPILL_MIN);
    for (long ord = 0; ord < 32; ord++) {
      dense.add(ord, 1);
    }
    BucketLongStore sparse = new BucketLongStore(SPILL_MIN);
    sparse.add(2L, 5);
    sparse.add(1_000L, 7);

    dense.addAll(sparse);

    assertEquals(6, dense.get(2L, -1));
    assertEquals(7, dense.get(1_000L, -1));
    assertEquals(33, dense.size());
  }

  @Test
  public void doubleStore_addGetSizeForEach_acrossSpill() {
    BucketDoubleStore store = new BucketDoubleStore(SPILL_MIN);

    for (long ord = 0; ord < 32; ord++) {
      store.add(ord, 0.5);
    }
    store.add(2L, 0.25);

    assertEquals(0.75, store.get(2L, -1), 1e-9);
    assertEquals(0.5, store.get(31L, -1), 1e-9);
    assertEquals(-1.0, store.get(32L, -1), 1e-9);
    assertEquals(32, store.size());

    double[] sum = new double[1];
    store.forEach((ord, value) -> sum[0] += value);
    assertEquals(32 * 0.5 + 0.25, sum[0], 1e-9);
  }

  @Test
  public void doubleStore_addAll_matchesElementWiseSum() {
    BucketDoubleStore left = new BucketDoubleStore();
    BucketDoubleStore right = new BucketDoubleStore();
    left.add(5L, 1.5);
    right.add(5L, 2.5);
    right.add(9L, 4.0);

    left.addAll(right);

    assertEquals(4.0, left.get(5L, 0), 1e-9);
    assertEquals(4.0, left.get(9L, 0), 1e-9);
    assertEquals(2, left.size());
  }

  @Test
  public void longStore_spillNotifiesOnceAndCloseIsIdempotent() {
    int[] spills = new int[1];
    BucketLongStore store = new BucketLongStore(SPILL_MIN, () -> spills[0]++);

    for (long ord = 0; ord < 32; ord++) {
      store.add(ord, 1);
    }
    store.add(2L, 1);

    assertEquals(1, spills[0]);
    assertTrue(store.iteratesInOrdinalOrder());
    store.close();
    store.close();
  }

  @Test
  public void longStore_zeroValueEntry_isPresentAfterSpill() {
    BucketLongStore store = new BucketLongStore(SPILL_MIN);
    store.put(0L, 0);
    for (long ord = 1; ord < 32; ord++) {
      store.add(ord, ord);
    }

    assertTrue(store.has(0L));
    assertEquals(0, store.get(0L, -1));
    assertFalse(store.has(100L));
  }

  @Test
  public void objectStore_putGet_acrossSpill() {
    BucketObjectStore<BsonValue> store = new BucketObjectStore<>(SPILL_MIN);
    for (int ord = 0; ord < 32; ord++) {
      store.put(ord, new BsonInt32(ord));
    }
    store.put(2L, new BsonInt32(99));

    assertEquals(Optional.of(new BsonInt32(99)), store.get(2L));
    assertEquals(Optional.of(new BsonInt32(31)), store.get(31L));
    assertEquals(Optional.empty(), store.get(32L));
    assertEquals(32, store.size());

    Map<Long, BsonValue> seen = new HashMap<>();
    store.forEach((ord, value) -> seen.put(ord, value));
    assertEquals(32, seen.size());
    assertEquals(new BsonInt32(99), seen.get(2L));
    assertEquals(new BsonInt32(0), seen.get(0L));
  }
}
