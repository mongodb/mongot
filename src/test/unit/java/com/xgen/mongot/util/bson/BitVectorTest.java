package com.xgen.mongot.util.bson;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class BitVectorTest {
  @Test
  public void testConstructor() {
    byte[] testVector = new byte[] {(byte) 0x00, (byte) 0xFF, (byte) 0xA5, (byte) 0x04};
    BitVector vector = new BitVector(testVector);
    assertEquals(Vector.VectorType.BIT, vector.getVectorType());
    assertEquals(testVector.length * 8, vector.numDimensions());
    assertEquals(testVector.length, vector.size());
  }

  @Test
  public void numDimensions() {
    BitVector vector1 =
        new BitVector(new byte[] {(byte) 0x00, (byte) 0xFF, (byte) 0xA5, (byte) 0x04});
    assertEquals(32, vector1.numDimensions());
    assertEquals(4, vector1.size());
    BitVector vector2 = new BitVector(new byte[0]);
    assertEquals(0, vector2.numDimensions());
    assertEquals(0, vector2.size());
  }

  @Test
  public void isZeroVector_nonZeroVectors_returnsFalse() {
    BitVector vector1 =
        new BitVector(new byte[] {(byte) 0x00, (byte) 0xFF, (byte) 0xA5, (byte) 0x04});
    assertFalse(vector1.isZeroVector());
    BitVector vector2 =
        new BitVector(new byte[] {(byte) 0x00, (byte) 0x00, (byte) 0x01, (byte) 0x00});
    assertFalse(vector2.isZeroVector());
  }

  @Test
  public void isZeroVector_emptyVector_returnsTrue() {
    BitVector vector = new BitVector(new byte[0]);
    assertTrue(vector.isZeroVector());
  }

  @Test
  public void isZeroVector_zeroVector_returnsTrue() {
    BitVector vector =
        new BitVector(new byte[] {(byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00});
    assertTrue(vector.isZeroVector());
  }

  @Test
  public void isEmpty_nonEmptyVector_returnsFalse() {
    BitVector vector =
        new BitVector(new byte[] {(byte) 0x00, (byte) 0xFF, (byte) 0xA5, (byte) 0x04});
    assertEquals(32, vector.numDimensions());
    assertEquals(4, vector.size());
    assertFalse(vector.isEmpty());
  }

  @Test
  public void isEmpty_emptyVector_returnsTrue() {
    BitVector vector = new BitVector(new byte[0]);
    assertEquals(0, vector.numDimensions());
    assertEquals(0, vector.size());
    assertTrue(vector.isEmpty());
  }

  @Test
  public void equals_bitVector_comparesValues() {
    BitVector vector = new BitVector(new byte[] {(byte) 0x00, (byte) 0xFF, (byte) 0xA5});
    assertNotEquals(null, vector);
    assertNotEquals(new Object(), vector);
    assertNotEquals(new BitVector(new byte[0]), vector);
    assertNotEquals(new BitVector(new byte[] {(byte) 0x00}), vector);
    assertEquals(vector, new BitVector(new byte[] {(byte) 0x00, (byte) 0xFF, (byte) 0xA5}));
  }

  @Test
  public void toString_longVector_getsTruncated() {
    BitVector vector = new BitVector(new byte[] {(byte) 0x00, (byte) 0x0F, (byte) 0xA5});
    assertEquals("[0, 15,...]", vector.toString());
  }

  @Test
  public void toString_emptyVector_emptyResult() {
    BitVector vector = new BitVector(new byte[] {});
    assertEquals("[]", vector.toString());
  }

  @Test
  public void toString_singleElementVector_printsSingleElement() {
    BitVector vector = new BitVector(new byte[] {(byte) 0x00});
    assertEquals("[0]", vector.toString());
  }

  @Test
  public void toString_twoElementVector_printsTwoElements() {
    BitVector vector = new BitVector(new byte[] {(byte) 0x00, (byte) 0x0F});
    assertEquals("[0, 15]", vector.toString());
  }
}
