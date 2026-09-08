package com.xgen.mongot.util.bson;

import static com.xgen.mongot.util.bson.FloatVector.OriginalType.BSON;
import static com.xgen.mongot.util.bson.FloatVector.OriginalType.NATIVE;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.fail;

import com.xgen.mongot.util.bson.parser.BsonParseContext;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.testing.TestUtils;
import java.util.List;
import org.bson.BsonArray;
import org.bson.BsonBoolean;
import org.bson.BsonDecimal128;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.types.Decimal128;
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
  public void getBsonEncoding_mapsEachVectorFormToItsWireEncoding() {
    assertEquals(
        Vector.BsonEncoding.ARRAY, Vector.fromFloats(new float[] {1.0f}, NATIVE).getBsonEncoding());
    assertEquals(
        Vector.BsonEncoding.BIN_DATA_FLOAT32,
        Vector.fromFloats(new float[] {1.0f}, BSON).getBsonEncoding());
    assertEquals(
        Vector.BsonEncoding.BIN_DATA_FLOAT16,
        Vector.fromFloat16Bytes(new byte[] {0, 60}).getBsonEncoding());
    assertEquals(
        Vector.BsonEncoding.BIN_DATA_INT8, Vector.fromBytes(new byte[] {1}).getBsonEncoding());
    assertEquals(
        Vector.BsonEncoding.BIN_DATA_BIT, Vector.fromBits(new byte[] {1}).getBsonEncoding());
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

  @Test
  public void fromBson_mixedNativeArray_preservesOriginalType() throws BsonParseException {
    var value = new BsonArray(List.of(new BsonInt32(10), new BsonInt64(20), new BsonDouble(3.03)));

    var vector = Vector.fromBson(BsonParseContext.root(), value);

    assertEquals(NATIVE, vector.asFloatVector().getOriginalType());
    assertArrayEquals(new float[] {10f, 20f, 3.03f}, vector.asFloatVector().getFloatVector(), 0f);
  }

  @Test
  public void fromBson_invalidElement_throwsBsonParseException() {
    var value = new BsonArray(List.of(new BsonBoolean(false)));

    assertThrows(BsonParseException.class, () -> Vector.fromBson(BsonParseContext.root(), value));
  }

  @Test
  public void fromBson_decimal128Element_throwsBsonParseException() {
    var value = new BsonArray(List.of(new BsonDecimal128(new Decimal128(1))));

    assertThrows(BsonParseException.class, () -> Vector.fromBson(BsonParseContext.root(), value));
  }
}
