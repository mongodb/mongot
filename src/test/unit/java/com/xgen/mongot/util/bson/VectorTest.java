package com.xgen.mongot.util.bson;

import static com.xgen.mongot.util.bson.FloatVector.OriginalType.NATIVE;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.fail;

import com.xgen.testing.TestUtils;
import org.junit.Test;

public class VectorTest {

  @Test
  public void testEmptyVectors() {
    try {
      Vector fromFloatVector = Vector.fromFloats(new float[] {}, NATIVE);
      assertNotNull(fromFloatVector);
      Vector fromByteVector = Vector.fromBytes(new byte[] {});
      assertNotNull(fromByteVector);
    } catch (Exception e) {
      fail("Exception thrown when trying to create a VectorWrapper");
    }
  }

  @Test
  public void testFloatVector() {
    float[] floats = new float[] {1.0f, 2.5f, -3.2f, 0.0f, 500f};
    Vector vector = Vector.fromFloats(floats, NATIVE);
    assertEquals(Vector.VectorType.FLOAT, vector.getVectorType());
    assertEquals(floats.length, vector.numDimensions());
    assertArrayEquals(floats, vector.asFloatVector().getFloatVector(), TestUtils.EPSILON);
    Exception e = assertThrows(UnsupportedOperationException.class, vector::asByteVector);
    assertEquals("Expected to be of type BYTE but found: FLOAT", e.getMessage());
  }

  @Test
  public void testByteVector() {
    byte[] bytes = new byte[] {(byte) 1, (byte) 2, (byte) 3, (byte) 4, (byte) 5, (byte) 6};
    Vector vector = Vector.fromBytes(bytes);
    assertEquals(Vector.VectorType.BYTE, vector.getVectorType());
    assertEquals(bytes.length, vector.numDimensions());
    assertArrayEquals(bytes, vector.asByteVector().getByteVector());
    Exception e = assertThrows(UnsupportedOperationException.class, vector::asFloatVector);
    assertEquals("Expected to be of type FLOAT but found: BYTE", e.getMessage());
  }

  @Test
  public void testBitVector() {
    byte[] bytes = new byte[] {(byte) 1, (byte) 2, (byte) 3, (byte) 4, (byte) 5, (byte) 6};
    Vector vector = Vector.fromBits(bytes);
    assertEquals(Vector.VectorType.BIT, vector.getVectorType());
    assertEquals(bytes.length * 8, vector.numDimensions());
    assertEquals(bytes.length, vector.asBitVector().size());
    assertArrayEquals(bytes, vector.asBitVector().getBitVector());
    Exception e = assertThrows(UnsupportedOperationException.class, vector::asFloatVector);
    assertEquals("Expected to be of type FLOAT but found: BIT", e.getMessage());
  }

  @Test
  public void testGetBytesForByteVector() {
    byte[] bytes = new byte[] {(byte) 1, (byte) 2, (byte) 3, (byte) 4, (byte) 5, (byte) 6};
    Vector vector = Vector.fromBytes(bytes);
    assertEquals(Vector.VectorType.BYTE, vector.getVectorType());
    assertEquals(bytes, vector.getBytes());
    assertEquals(vector.asByteVector().getByteVector(), vector.getBytes());
  }

  @Test
  public void testGetBytesForBitVector() {
    byte[] bytes = new byte[] {(byte) 1, (byte) 2, (byte) 3, (byte) 4, (byte) 5, (byte) 6};
    Vector vector = Vector.fromBits(bytes);
    assertEquals(Vector.VectorType.BIT, vector.getVectorType());
    assertEquals(bytes, vector.getBytes());
    assertEquals(vector.asBitVector().getBitVector(), vector.getBytes());
  }

  @Test
  @SuppressWarnings("DoNotCall")
  public void testGetBytesForFloatVector() {
    float[] floats = new float[] {1.0f, 2.5f, -3.2f, 0.0f, 500f};
    Vector vector = Vector.fromFloats(floats, NATIVE);
    Exception e = assertThrows(UnsupportedOperationException.class, vector::getBytes);
    assertEquals("Expected to be one of types [BYTE,BIT] but found: FLOAT", e.getMessage());
  }
}
