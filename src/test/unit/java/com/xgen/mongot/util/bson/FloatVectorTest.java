package com.xgen.mongot.util.bson;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class FloatVectorTest {

  @Test
  public void testConstructor() {
    float[] testVector = new float[] {1.0f, 2.0f, 3.0f, 4.0f, 5.0f, 6.0f};
    FloatVector vector = new FloatVector(testVector, FloatVector.OriginalType.NATIVE);
    assertEquals(Vector.VectorType.FLOAT, vector.getVectorType());
    assertEquals(testVector.length, vector.numDimensions());
  }

  @Test
  public void numDimensions() {
    FloatVector vector1 =
        new FloatVector(
            new float[] {1.0f, 2.0f, 3.0f, 4.0f, 5.0f}, FloatVector.OriginalType.NATIVE);
    assertEquals(5, vector1.numDimensions());
    FloatVector vector2 = new FloatVector(new float[0], FloatVector.OriginalType.NATIVE);
    assertEquals(0, vector2.numDimensions());
  }

  @Test
  public void isZeroVector_nonZeroVectors_returnsFalse() {
    FloatVector vector1 =
        new FloatVector(
            new float[] {1.0f, 2.0f, 3.0f, 4.0f, 5.0f}, FloatVector.OriginalType.NATIVE);
    assertFalse(vector1.isZeroVector());
    FloatVector vector2 =
        new FloatVector(
            new float[] {0.00000001f, 0.0f, 0.0f, 0.0f, 0.0f}, FloatVector.OriginalType.NATIVE);
    assertFalse(vector2.isZeroVector());
  }

  @Test
  public void isZeroVector_emptyVector_returnsTrue() {
    FloatVector vector = new FloatVector(new float[0], FloatVector.OriginalType.NATIVE);
    assertTrue(vector.isZeroVector());
  }

  @Test
  public void isZeroVector_zeroVector_returnsTrue() {
    FloatVector vector =
        new FloatVector(
            new float[] {0.0f, 0.0f, 0.0f, 0.0f, 0.0f}, FloatVector.OriginalType.NATIVE);
    assertTrue(vector.isZeroVector());
  }

  @Test
  public void isEmpty_nonEmptyVector_returnsFalse() {
    FloatVector vector1 =
        new FloatVector(
            new float[] {1.0f, 2.0f, 3.0f, 4.0f, 5.0f}, FloatVector.OriginalType.NATIVE);
    assertEquals(5, vector1.numDimensions());
    assertFalse(vector1.isEmpty());
  }

  @Test
  public void isEmpty_emptyVector_returnsTrue() {
    FloatVector vector2 = new FloatVector(new float[0], FloatVector.OriginalType.NATIVE);
    assertEquals(0, vector2.numDimensions());
    assertTrue(vector2.isEmpty());
  }

  @Test
  public void equals_floatVector_comparesValues() {
    FloatVector vector =
        new FloatVector(
            new float[] {1.0f, 2.0f, 3.0f, 4.0f, 5.0f}, FloatVector.OriginalType.NATIVE);
    assertNotEquals(null, vector);
    assertNotEquals(new Object(), vector);
    assertNotEquals(new FloatVector(new float[0], FloatVector.OriginalType.NATIVE), vector);
    assertNotEquals(new FloatVector(new float[] {1.0f}, FloatVector.OriginalType.NATIVE), vector);
    assertEquals(
        vector,
        new FloatVector(
            new float[] {1.0f, 2.0f, 3.0f, 4.0f, 5.0f}, FloatVector.OriginalType.NATIVE));
  }

  @Test
  public void toString_longVector_getsTruncated() {
    FloatVector vector =
        new FloatVector(
            new float[] {1.0f, 2.0f, 3.0f, 4.0f, 5.0f}, FloatVector.OriginalType.NATIVE);
    assertEquals("[1.0, 2.0,...]", vector.toString());
  }

  @Test
  public void toString_emptyVector_emptyResult() {
    FloatVector vector = new FloatVector(new float[] {}, FloatVector.OriginalType.NATIVE);
    assertEquals("[]", vector.toString());
  }

  @Test
  public void toString_singleElementVector_printsSingleElement() {
    FloatVector vector = new FloatVector(new float[] {1.0f}, FloatVector.OriginalType.NATIVE);
    assertEquals("[1.0]", vector.toString());
  }

  @Test
  public void toString_twoElementVector_printsTwoElements() {
    FloatVector vector = new FloatVector(new float[] {1.0f, 2.0f}, FloatVector.OriginalType.NATIVE);
    assertEquals("[1.0, 2.0]", vector.toString());
  }
}
