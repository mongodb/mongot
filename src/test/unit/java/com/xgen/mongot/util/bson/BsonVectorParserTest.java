package com.xgen.mongot.util.bson;

import static com.xgen.mongot.util.bson.BsonVectorParser.BSON_VECTOR_HEADER_SIZE;
import static com.xgen.mongot.util.bson.FloatVector.OriginalType.NATIVE;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.google.errorprone.annotations.Var;
import com.xgen.testing.TestUtils;
import com.xgen.testing.util.VectorTestUtils;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.FloatBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.Random;
import org.bson.BSONException;
import org.bson.BsonBinary;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses(
    value = {
      BsonVectorParserTest.ValidBsonVectorParserTest.class,
      BsonVectorParserTest.InvalidBsonVectorParserTest.class,
      BsonVectorParserTest.BsonFloatVectorEncodeDecodeTest.class,
      BsonVectorParserTest.BsonByteVectorEncodeDecodeTest.class,
      BsonVectorParserTest.BsonBitVectorEncodeDecodeTest.class,
      BsonVectorParserTest.PaddingTest.class,
    })
public class BsonVectorParserTest {

  @RunWith(Parameterized.class)
  public static class ValidBsonVectorParserTest {
    private final int dimension;
    private final int padding;

    public ValidBsonVectorParserTest(int dimension, int padding) {
      this.dimension = dimension;
      this.padding = padding;
    }

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
      return Arrays.asList(new Object[][] {{0, 0}, {1, 0}, {2, 0}, {10, 0}, {1536, 0}});
    }

    @Test
    public void testValidFloatVector() {
      int size = BSON_VECTOR_HEADER_SIZE + (4 * this.dimension);
      float[] randomFloats = VectorTestUtils.createFloatVector(this.dimension);

      ByteBuffer buffer = ByteBuffer.allocate(size).order(ByteOrder.LITTLE_ENDIAN);
      buffer.put(BsonVectorParser.VectorDataType.FLOAT32_VECTOR_DATA_TYPE.getValue());
      buffer.put((byte) this.padding);
      for (float f : randomFloats) {
        buffer.putFloat(f);
      }
      float[] parsed =
          BsonVectorParser.parse(new BsonBinary(BsonVectorParser.VECTOR_SUB_TYPE, buffer.array()))
              .asFloatVector()
              .getFloatVector();
      assertEquals(this.dimension - this.padding, parsed.length);
      assertArrayEquals(
          Arrays.copyOf(randomFloats, this.dimension - this.padding), parsed, TestUtils.EPSILON);
    }

    @Test
    public void testValidInt8Vector() {
      int size = this.dimension + BSON_VECTOR_HEADER_SIZE;
      byte[] randomBytes = new byte[this.dimension];
      Random random = new Random();
      random.nextBytes(randomBytes);
      ByteBuffer buffer = ByteBuffer.allocate(size).order(ByteOrder.LITTLE_ENDIAN);
      buffer.put(BsonVectorParser.VectorDataType.BYTE_VECTOR_DATA_TYPE.getValue());
      buffer.put((byte) this.padding);
      buffer.put(randomBytes);
      byte[] parsed =
          BsonVectorParser.parse(new BsonBinary(BsonVectorParser.VECTOR_SUB_TYPE, buffer.array()))
              .asByteVector()
              .getByteVector();
      assertEquals(this.dimension - this.padding, parsed.length);
      assertArrayEquals(Arrays.copyOf(randomBytes, this.dimension - this.padding), parsed);
    }

    @Test
    public void testValidBitVector() {
      int size = this.dimension + BSON_VECTOR_HEADER_SIZE;
      byte[] randomBytes = new byte[this.dimension];
      Random random = new Random();
      random.nextBytes(randomBytes);
      ByteBuffer buffer = ByteBuffer.allocate(size).order(ByteOrder.LITTLE_ENDIAN);
      buffer.put(BsonVectorParser.VectorDataType.BIT_VECTOR_DATA_TYPE.getValue());
      buffer.put((byte) this.padding);
      buffer.put(randomBytes);
      BitVector bitVector =
          BsonVectorParser.parse(new BsonBinary(BsonVectorParser.VECTOR_SUB_TYPE, buffer.array()))
              .asBitVector();
      byte[] parsed = bitVector.getBitVector();
      assertEquals(this.dimension - this.padding, parsed.length);
      assertArrayEquals(Arrays.copyOf(randomBytes, this.dimension - this.padding), parsed);
    }
  }

  public static class InvalidBsonVectorParserTest {
    @Test
    public void testInvalidVector() {
      String expectedMessage = "Expected at least 2 bytes in BSON vector";
      @Var
      Exception ex =
          assertThrows(
              BSONException.class,
              () ->
                  BsonVectorParser.parse(
                      new BsonBinary(BsonVectorParser.VECTOR_SUB_TYPE, new byte[0])));
      assertTrue(ex.getMessage().contains(expectedMessage));

      byte[] singleByteVector = new byte[] {(byte) 0x27};
      ex =
          assertThrows(
              BSONException.class,
              () ->
                  BsonVectorParser.parse(
                      new BsonBinary(BsonVectorParser.VECTOR_SUB_TYPE, singleByteVector)));
      assertTrue(ex.getMessage().contains(expectedMessage));

      byte[] emptyVector = new byte[0];
      ex =
          assertThrows(
              BSONException.class,
              () ->
                  BsonVectorParser.parse(
                      new BsonBinary(BsonVectorParser.VECTOR_SUB_TYPE, emptyVector)));
      assertTrue(ex.getMessage().contains(expectedMessage));
    }

    @Test
    public void testUnexpectedFirstByte() {
      byte[] invalidVector = new byte[] {(byte) 0x00, (byte) 0x00};
      Exception ex =
          assertThrows(
              BSONException.class,
              () ->
                  BsonVectorParser.parse(
                      new BsonBinary(BsonVectorParser.VECTOR_SUB_TYPE, invalidVector)));
      assertEquals("Unsupported vector subtype found", ex.getMessage());
    }

    @Test
    public void testCorruptedVectorData() {
      // not enough bytes for a valid float number after the header bytes
      byte[] invalidVector = new byte[] {(byte) 0x27, (byte) 0x00, (byte) 0x11, (byte) 0x22};
      float[] parsed =
          BsonVectorParser.parse(new BsonBinary(BsonVectorParser.VECTOR_SUB_TYPE, invalidVector))
              .asFloatVector()
              .getFloatVector();
      assertEquals(0, parsed.length);
    }
  }

  @RunWith(Parameterized.class)
  public static class BsonFloatVectorEncodeDecodeTest {
    private final float[] floatVector;

    public BsonFloatVectorEncodeDecodeTest(float[] floatVector) {
      this.floatVector = floatVector;
    }

    @Parameterized.Parameters
    public static Collection<float[]> data() {
      return Arrays.asList(
          new float[][] {
            {
              /* empty */
            },
            {1f, 2.5f, -0.8f}
          });
    }

    @Test
    public void testDecodeThenEncodeFloatVector() {
      int dimension = this.floatVector.length;
      int size = (4 * dimension) + BSON_VECTOR_HEADER_SIZE;
      ByteBuffer buffer = ByteBuffer.allocate(size).order(ByteOrder.LITTLE_ENDIAN);
      buffer.put(BsonVectorParser.VectorDataType.FLOAT32_VECTOR_DATA_TYPE.getValue());
      buffer.put((byte) 0x00);
      for (float f : this.floatVector) {
        buffer.putFloat(f);
      }
      BsonBinary bsonBinary = new BsonBinary(BsonVectorParser.VECTOR_SUB_TYPE, buffer.array());

      float[] converted = BsonVectorParser.parse(bsonBinary).asFloatVector().getFloatVector();
      assertEquals(converted.length, dimension);

      BsonBinary encoded = BsonVectorParser.encode(Vector.fromFloats(converted, NATIVE));
      assertEquals(size, encoded.getData().length);
      byte[] encodedArray = encoded.getData();
      assertEquals((byte) 0x27, encodedArray[0]);
      assertEquals((byte) 0x00, encodedArray[1]);
      ByteBuffer byteBuffer =
          ByteBuffer.wrap(encodedArray, 2, encodedArray.length - 2).order(ByteOrder.LITTLE_ENDIAN);
      FloatBuffer floatBuffer = byteBuffer.asFloatBuffer();
      for (float f : this.floatVector) {
        assertEquals(f, floatBuffer.get(), TestUtils.EPSILON);
      }
    }

    @Test
    public void testEncodeThenDecodeFloatVector() {
      BsonBinary encoded = BsonVectorParser.encode(Vector.fromFloats(this.floatVector, NATIVE));
      assertEquals(BsonVectorParser.VECTOR_SUB_TYPE, encoded.getType());
      // check first byte
      assertEquals(
          BsonVectorParser.VectorDataType.FLOAT32_VECTOR_DATA_TYPE.getValue(),
          encoded.getData()[0]);
      // check padding byte
      assertEquals(0x00, encoded.getData()[1]);

      Vector decoded = BsonVectorParser.parse(encoded);
      assertEquals(Vector.VectorType.FLOAT, decoded.getVectorType());
      float[] decodedVector = decoded.asFloatVector().getFloatVector();
      assertEquals(this.floatVector.length, decodedVector.length);
      assertArrayEquals(this.floatVector, decodedVector, TestUtils.EPSILON);
    }
  }

  @RunWith(Parameterized.class)
  public static class BsonByteVectorEncodeDecodeTest {
    private final byte[] byteVector;

    public BsonByteVectorEncodeDecodeTest(byte[] byteVector) {
      this.byteVector = byteVector;
    }

    @Parameterized.Parameters
    public static Collection<byte[]> data() {
      return Arrays.asList(
          new byte[][] {
            {
              /* empty */
            },
            {(byte) 0x00, (byte) 0xFF, (byte) 0xA0}
          });
    }

    @Test
    public void testEncodeThenDecodeByteVector() {
      BsonBinary encoded = BsonVectorParser.encode(Vector.fromBytes(this.byteVector));
      assertEquals(BsonVectorParser.VECTOR_SUB_TYPE, encoded.getType());
      // check first byte
      assertEquals(
          BsonVectorParser.VectorDataType.BYTE_VECTOR_DATA_TYPE.getValue(), encoded.getData()[0]);
      // check padding byte
      assertEquals(0x00, encoded.getData()[1]);

      Vector decoded = BsonVectorParser.parse(encoded);
      assertEquals(Vector.VectorType.BYTE, decoded.getVectorType());
      byte[] decodedVector = decoded.asByteVector().getByteVector();
      assertEquals(this.byteVector.length, decodedVector.length);
      assertArrayEquals(this.byteVector, decodedVector);
    }

    @Test
    public void testDecodeThenEncodeByteVector() {
      int dimension = this.byteVector.length;
      int size = dimension + BSON_VECTOR_HEADER_SIZE;
      ByteBuffer buffer = ByteBuffer.allocate(size).order(ByteOrder.LITTLE_ENDIAN);
      buffer.put(BsonVectorParser.VectorDataType.BYTE_VECTOR_DATA_TYPE.getValue());
      buffer.put((byte) 0x00);
      for (byte b : this.byteVector) {
        buffer.put(b);
      }
      BsonBinary bsonBinary = new BsonBinary(BsonVectorParser.VECTOR_SUB_TYPE, buffer.array());

      byte[] converted = BsonVectorParser.parse(bsonBinary).asByteVector().getByteVector();
      assertEquals(converted.length, dimension);

      BsonBinary encoded = BsonVectorParser.encode(Vector.fromBytes(converted));
      assertEquals(size, encoded.getData().length);
      byte[] encodedArray = encoded.getData();
      assertEquals(
          BsonVectorParser.VectorDataType.BYTE_VECTOR_DATA_TYPE.getValue(), encodedArray[0]);
      assertEquals((byte) 0x00, encodedArray[1]);
      @Var int encodedArrayIndex = 2;
      for (byte b : this.byteVector) {
        assertEquals(b, encodedArray[encodedArrayIndex]);
        encodedArrayIndex++;
      }
    }
  }

  @RunWith(Parameterized.class)
  public static class BsonBitVectorEncodeDecodeTest {
    private final byte[] bitVector;

    public BsonBitVectorEncodeDecodeTest(byte[] bitVector) {
      this.bitVector = bitVector;
    }

    @Parameterized.Parameters
    public static Collection<byte[]> data() {
      return Arrays.asList(
          new byte[][] {
            {
              /* empty */
            },
            {(byte) 0x00, (byte) 0xFF, (byte) 0xA0}
          });
    }

    @Test
    public void testEncodeThenDecodeBitVector() {
      BsonBinary encoded = BsonVectorParser.encode(Vector.fromBits(this.bitVector));
      assertEquals(BsonVectorParser.VECTOR_SUB_TYPE, encoded.getType());
      // check first byte
      assertEquals(
          BsonVectorParser.VectorDataType.BIT_VECTOR_DATA_TYPE.getValue(), encoded.getData()[0]);
      // check padding byte
      assertEquals(0x00, encoded.getData()[1]);

      Vector decoded = BsonVectorParser.parse(encoded);
      assertEquals(Vector.VectorType.BIT, decoded.getVectorType());
      byte[] decodedVector = decoded.asBitVector().getBitVector();
      assertEquals(this.bitVector.length, decodedVector.length);
      assertArrayEquals(this.bitVector, decodedVector);
    }

    @Test
    public void testDecodeThenEncodeBitVector() {
      int len = this.bitVector.length;
      int size = len + BSON_VECTOR_HEADER_SIZE;
      int padding = 0;
      ByteBuffer buffer = ByteBuffer.allocate(size).order(ByteOrder.LITTLE_ENDIAN);
      buffer.put(BsonVectorParser.VectorDataType.BIT_VECTOR_DATA_TYPE.getValue());
      buffer.put((byte) padding);
      for (byte b : this.bitVector) {
        buffer.put(b);
      }
      BsonBinary bsonBinary = new BsonBinary(BsonVectorParser.VECTOR_SUB_TYPE, buffer.array());

      byte[] converted = BsonVectorParser.parse(bsonBinary).asBitVector().getBitVector();
      assertEquals(converted.length, len);

      BsonBinary encoded = BsonVectorParser.encode(Vector.fromBits(converted));
      assertEquals(size, encoded.getData().length);
      byte[] encodedArray = encoded.getData();
      assertEquals(
          BsonVectorParser.VectorDataType.BIT_VECTOR_DATA_TYPE.getValue(), encodedArray[0]);
      assertEquals((byte) padding, encodedArray[1]);
      @Var int encodedArrayIndex = 2;
      for (byte b : this.bitVector) {
        assertEquals(b, encodedArray[encodedArrayIndex]);
        encodedArrayIndex++;
      }
    }
  }

  public static class PaddingTest {
    @Test
    public void testPaddingByte() {
      assertEquals(0, BsonVectorParser.getPadding((byte) 0x00));
      assertEquals(2, BsonVectorParser.getPadding((byte) 0x02));
      assertEquals(2, BsonVectorParser.getPadding((byte) 0xFA));
      assertEquals(7, BsonVectorParser.getPadding((byte) 0x07));
      assertEquals(7, BsonVectorParser.getPadding((byte) 0xFF));
    }

    @Test
    public void testNonZeroPaddingByteWithFloatVector() {
      byte[] testVector = new byte[] {(byte) 0x23, (byte) 0xFF, (byte) 0x11, (byte) 0x22};
      byte nonZeroPaddingByte = (byte) 0x01;
      ByteBuffer buffer = ByteBuffer.allocate(6).order(ByteOrder.LITTLE_ENDIAN);

      buffer.put(BsonVectorParser.VectorDataType.FLOAT32_VECTOR_DATA_TYPE.getValue());
      buffer.put(nonZeroPaddingByte);
      buffer.put(testVector);
      BsonBinary floatBsonBinaryVector =
          new BsonBinary(BsonVectorParser.VECTOR_SUB_TYPE, buffer.array());
      Exception ex =
          assertThrows(BSONException.class, () -> BsonVectorParser.parse(floatBsonBinaryVector));
      assertTrue(ex.getMessage().contains("Padding should be zero"));
    }

    @Test
    public void testNonZeroPaddingByteWithByteVector() {
      byte[] testVector = new byte[] {(byte) 0x23, (byte) 0xFF, (byte) 0x11, (byte) 0x22};
      byte nonZeroPaddingByte = (byte) 0x01;
      ByteBuffer buffer = ByteBuffer.allocate(6).order(ByteOrder.LITTLE_ENDIAN);

      buffer.put(BsonVectorParser.VectorDataType.BYTE_VECTOR_DATA_TYPE.getValue());
      buffer.put(nonZeroPaddingByte);
      buffer.put(testVector);
      BsonBinary byteBsonBinaryVector =
          new BsonBinary(BsonVectorParser.VECTOR_SUB_TYPE, buffer.array());
      Exception ex =
          assertThrows(BSONException.class, () -> BsonVectorParser.parse(byteBsonBinaryVector));
      assertTrue(ex.getMessage().contains("Padding should be zero"));
    }

    @Test
    public void testNonZeroPaddingByteWithBitVector() {
      byte[] testVector = new byte[] {(byte) 0x23, (byte) 0xFF, (byte) 0x11, (byte) 0x22};
      byte nonZeroPaddingByte = (byte) 0x01;
      ByteBuffer buffer = ByteBuffer.allocate(6).order(ByteOrder.LITTLE_ENDIAN);

      buffer.put(BsonVectorParser.VectorDataType.BIT_VECTOR_DATA_TYPE.getValue());
      buffer.put(nonZeroPaddingByte);
      buffer.put(testVector);
      BsonBinary bitBsonBinaryVector =
          new BsonBinary(BsonVectorParser.VECTOR_SUB_TYPE, buffer.array());
      Exception ex =
          assertThrows(BSONException.class, () -> BsonVectorParser.parse(bitBsonBinaryVector));
      assertTrue(
          ex.getMessage().contains("The dimensions of binary vector should be a multiple of 8"));
    }
  }
}
