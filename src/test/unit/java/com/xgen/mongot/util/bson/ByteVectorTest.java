package com.xgen.mongot.util.bson;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class ByteVectorTest {
  @Test
  public void testConstructor() {
    byte[] testVector = new byte[] {(byte) 0x00, (byte) 0xFF, (byte) 0xA5, (byte) 0x04};
    ByteVector vector = new ByteVector(testVector);
    assertEquals(Vector.VectorType.BYTE, vector.getVectorType());
    assertEquals(testVector.length, vector.numDimensions());
  }

  @Test
  public void numDimensions() {
    ByteVector vector1 =
        new ByteVector(new byte[] {(byte) 0x00, (byte) 0xFF, (byte) 0xA5, (byte) 0x04});
    assertEquals(4, vector1.numDimensions());
    ByteVector vector2 = new ByteVector(new byte[0]);
    assertEquals(0, vector2.numDimensions());
  }

  @Test
  public void isZeroVector_nonZeroVectors_returnsFalse() {
    ByteVector vector1 =
        new ByteVector(new byte[] {(byte) 0x00, (byte) 0xFF, (byte) 0xA5, (byte) 0x04});
    assertFalse(vector1.isZeroVector());
    ByteVector vector2 =
        new ByteVector(new byte[] {(byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00});
    assertTrue(vector2.isZeroVector());
  }

  @Test
  public void isZeroVector_emptyVector_returnsTrue() {
    ByteVector vector = new ByteVector(new byte[0]);
    assertTrue(vector.isZeroVector());
  }

  @Test
  public void isZeroVector_zeroVector_returnsTrue() {
    ByteVector vector =
        new ByteVector(new byte[] {(byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00});
    assertTrue(vector.isZeroVector());
  }

  @Test
  public void isEmpty_nonEmptyVector_returnsFalse() {
    ByteVector vector1 =
        new ByteVector(new byte[] {(byte) 0x00, (byte) 0xFF, (byte) 0xA5, (byte) 0x04});
    assertEquals(4, vector1.numDimensions());
    assertFalse(vector1.isEmpty());
  }

  @Test
  public void isEmpty_emptyVector_returnsTrue() {
    ByteVector vector2 = new ByteVector(new byte[0]);
    assertEquals(0, vector2.numDimensions());
    assertTrue(vector2.isEmpty());
  }

  @Test
  public void equals_byteVector_comparesValues() {
    ByteVector vector = new ByteVector(new byte[] {(byte) 0x00, (byte) 0xFF, (byte) 0xA5});
    assertNotEquals(null, vector);
    assertNotEquals(new Object(), vector);
    assertNotEquals(new ByteVector(new byte[0]), vector);
    assertNotEquals(new ByteVector(new byte[] {(byte) 0x00}), vector);
    assertEquals(vector, new ByteVector(new byte[] {(byte) 0x00, (byte) 0xFF, (byte) 0xA5}));
  }

  @Test
  public void toString_longVector_getsTruncated() {
    ByteVector vector = new ByteVector(new byte[] {(byte) 0x00, (byte) 0x0F, (byte) 0xA5});
    assertEquals("[0, 15,...]", vector.toString());
  }

  @Test
  public void toString_emptyVector_emptyResult() {
    ByteVector vector = new ByteVector(new byte[] {});
    assertEquals("[]", vector.toString());
  }

  @Test
  public void toString_singleElementVector_printsSingleElement() {
    ByteVector vector = new ByteVector(new byte[] {(byte) 0x00});
    assertEquals("[0]", vector.toString());
  }

  @Test
  public void toString_twoElementVector_printsTwoElements() {
    ByteVector vector = new ByteVector(new byte[] {(byte) 0x00, (byte) 0x0F});
    assertEquals("[0, 15]", vector.toString());
  }
}
