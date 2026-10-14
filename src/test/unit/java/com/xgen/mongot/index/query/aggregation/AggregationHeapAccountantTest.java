package com.xgen.mongot.index.query.aggregation;

import static com.google.common.truth.Truth.assertThat;

import com.carrotsearch.hppc.DoubleArrayList;
import com.carrotsearch.hppc.LongArrayList;
import com.carrotsearch.hppc.ObjectArrayList;
import org.junit.Test;

public class AggregationHeapAccountantTest {

  @Test
  public void currentBytes_noAllocation_isZero() {
    assertThat(new AggregationHeapAccountant().currentBytes()).isEqualTo(0);
  }

  @Test
  public void longArray_chargesHppcRamBytesAllocated() {
    var accountant = new AggregationHeapAccountant();

    new LongArray(8, accountant::add);

    assertThat(accountant.currentBytes()).isEqualTo(new LongArrayList(8).ramBytesAllocated());
  }

  @Test
  public void longArray_growth_chargesOnlyTheDelta() {
    var accountant = new AggregationHeapAccountant();
    var array = new LongArray(8, accountant::add);
    long initial = accountant.currentBytes();

    array.set(100, 1);

    long afterGrowth = accountant.currentBytes();
    assertThat(afterGrowth).isGreaterThan(initial);
    LongArrayList grown = new LongArrayList(8);
    grown.resize(101);
    assertThat(afterGrowth).isEqualTo(grown.ramBytesAllocated());
  }

  @Test
  public void longArray_writesWithinCapacity_chargeNothing() {
    var accountant = new AggregationHeapAccountant();
    var array = new LongArray(8, accountant::add);
    array.set(100, 1);
    long afterGrowth = accountant.currentBytes();

    array.increment(0, 1);
    array.increment(50, 1);
    array.increment(99, 1);

    assertThat(accountant.currentBytes()).isEqualTo(afterGrowth);
  }

  @Test
  public void longArray_sparseHighOrdinalWrite_chargesTheZeroFilledSlack() {
    var accountant = new AggregationHeapAccountant();
    var array = new LongArray(8, accountant::add);

    array.set(0, 1);
    array.set(50_000, 1);

    LongArrayList grown = new LongArrayList(8);
    grown.resize(50_001);
    assertThat(accountant.currentBytes()).isEqualTo(grown.ramBytesAllocated());
    assertThat(array.size()).isEqualTo(50_001);
  }

  @Test
  public void doubleArray_chargesHppcRamBytesAllocated() {
    var accountant = new AggregationHeapAccountant();

    new DoubleArray(16, accountant::add);

    assertThat(accountant.currentBytes()).isEqualTo(new DoubleArrayList(16).ramBytesAllocated());
  }

  @Test
  public void objectArray_chargesHppcRamBytesAllocated() {
    var accountant = new AggregationHeapAccountant();

    new ObjectArray<String>(8, accountant::add);

    assertThat(accountant.currentBytes())
        .isEqualTo(new ObjectArrayList<String>(8).ramBytesAllocated());
  }

  @Test
  public void objectArray_growth_chargesOnlyTheDelta() {
    var accountant = new AggregationHeapAccountant();
    var array = new ObjectArray<String>(8, accountant::add);
    long initial = accountant.currentBytes();

    array.set(100, "x");

    long afterGrowth = accountant.currentBytes();
    assertThat(afterGrowth).isGreaterThan(initial);
    ObjectArrayList<String> grown = new ObjectArrayList<>(8);
    grown.resize(101);
    assertThat(afterGrowth).isEqualTo(grown.ramBytesAllocated());
  }

  @Test
  public void arraysWithoutAnAccountant_chargeNothing() {
    var accountant = new AggregationHeapAccountant();

    new LongArray(8).set(1000, 1);
    LongArray.from(1, 2, 3);
    new ObjectArray<String>(8).set(100, "x");

    assertThat(accountant.currentBytes()).isEqualTo(0);
  }

  @Test
  public void disabled_isSharedAndAlwaysZero() {
    AggregationHeapAccountant.disabled().add(1024);

    assertThat(AggregationHeapAccountant.disabled().currentBytes()).isEqualTo(0);
  }
}
